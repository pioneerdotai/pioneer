use super::*;
use crate::history::HistoryReadFence;
use crate::task_output::{
    DeliveredTaskOutputCursor, DeliveredTaskOutputPage, DeliveredTaskOutputRef,
};

async fn commit_item_event(
    store: &CompactionStore,
    notification: pioneer_protocol::ItemCompletedNotification,
) {
    use pioneer_crud_conversations::events::TurnEventPayload;
    use pioneer_crud_execution::event_append::{PreparedTurnEvent, append_prepared_event};
    let at = pioneer_crud_execution::time::unix_to_datetime(chrono::Utc::now().timestamp());
    let payload = TurnEventPayload::ItemCompleted(notification);
    let prepared = PreparedTurnEvent::prepare_for_storage(payload).unwrap();
    let connection = store.database_connection();
    let tx = connection.begin().await.unwrap();
    append_prepared_event(&tx, prepared, at).await.unwrap();
    tx.commit().await.unwrap();
}

#[pioneer_postgres_test::postgres_test]
async fn canonical_event_metadata_uses_typed_identity_and_invalidates_on_edit() {
    let store = store().await;
    for item in [
        pioneer_protocol::TurnItem::Reasoning {
            id: "reasoning-source".into(),
            summary: vec![],
            content: vec!["equal text".into()],
        },
        pioneer_protocol::TurnItem::AgentMessage {
            id: "answer-source".into(),
            text: "equal text".into(),
            phase: Default::default(),
            markdown: None,
            markdown_version: None,
        },
    ] {
        commit_item_event(
            &store,
            pioneer_protocol::ItemCompletedNotification {
                workspace_id: "ws".into(),
                thread_id: "thread".into(),
                turn_id: "turn".into(),
                item,
            },
        )
        .await;
    }
    let metadata = store
        .compaction_source_metadata_page("ws", "thread", "turn", PagedSource::Event, 0)
        .await
        .unwrap();
    let reasoning = metadata
        .entries
        .iter()
        .find(|row| row.item_id.as_deref() == Some("reasoning-source"))
        .unwrap();
    let answer = metadata
        .entries
        .iter()
        .find(|row| row.item_id.as_deref() == Some("answer-source"))
        .unwrap();
    assert_eq!(reasoning.projection_kind.as_deref(), Some("reasoning"));
    assert_eq!(answer.projection_kind.as_deref(), Some("assistant"));
    assert_ne!(reasoning.reference, answer.reference);
    edit_event_bytes(&store, &reasoning.reference.id, |wire| {
        wire.replace("equal text", "edited content")
    })
    .await;
    let edited = store
        .compaction_source_metadata_page("ws", "thread", "turn", PagedSource::Event, 0)
        .await
        .unwrap();
    let invalidated = edited
        .entries
        .iter()
        .find(|row| row.reference.id == reasoning.reference.id)
        .unwrap();
    assert_eq!(invalidated.projection_kind, None);
    assert_eq!(invalidated.item_id, None);
    assert_ne!(invalidated.reference.version, reasoning.reference.version);
}

async fn delivered_output_fixture(store: &CompactionStore, delivery: &str, delivery_turn: &str) {
    let db = store.database_connection();
    let task = format!("task-{delivery}");
    let run = format!("run-{delivery}");
    let run_turn = format!("rt-{delivery}");
    let source_turn = format!("output-turn-{delivery}");
    for (sql, values) in [
        (
            "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ($1,'thread','completed','conversation','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
            vec![source_turn.clone().into()],
        ),
        (
            "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,created_by_turn_id,executor_kind,status,title,goal) VALUES ($1,'ws','thread','thread','thread','turn','agent','completed','Task','fixture')",
            vec![task.clone().into()],
        ),
        (
            "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES ($1,$2,$3,1,1,'succeeded','agent')",
            vec![run.clone().into(), task.clone().into(), run.clone().into()],
        ),
        (
            "INSERT INTO task_run_turn(id,task_id,run_id,thread_id,turn_id,kind,round,sequence,status,created_at) VALUES ($1,$2,$3,'thread',$4,'initial',0,1,'completed',CURRENT_TIMESTAMP)",
            vec![
                run_turn.clone().into(),
                task.clone().into(),
                run.clone().into(),
                source_turn.clone().into(),
            ],
        ),
        (
            "INSERT INTO task_result_candidate(id,task_id,run_id,task_run_turn_id,thread_id,turn_id,round,status,created_at,updated_at) VALUES ($1,$2,$3,$4,'thread',$5,0,'accepted',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
            vec![
                delivery.into(),
                task.clone().into(),
                run.clone().into(),
                run_turn.clone().into(),
                source_turn.clone().into(),
            ],
        ),
        (
            "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ($1,'thread','completed','conversation','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP) ON CONFLICT DO NOTHING",
            vec![delivery_turn.into()],
        ),
        (
            "INSERT INTO task_delivery(id,workspace_id,task_id,run_id,delivery_key,mode,thread_target,target_thread_id,status,attempt_count,max_attempts,delivered_turn_id) VALUES ($1,'ws',$2,$3,$4,'thread','origin_thread','thread','delivered',1,1,$5)",
            vec![
                delivery.into(),
                task.into(),
                run.into(),
                delivery.into(),
                delivery_turn.into(),
            ],
        ),
    ] {
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            sql,
            values,
        ))
        .await
        .unwrap();
    }
    let history = pioneer_compaction::frozen::FrozenHistoryRef {
        format: 1,
        manifest_id: format!("output-{delivery}"),
        messages: 0,
        identity_sha256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".into(),
    };
    store
        .compaction_begin_frozen_history("ws", "thread", &history)
        .await
        .unwrap();
    assert!(
        store
            .compaction_finish_frozen_history("ws", "thread", &history)
            .await
            .unwrap()
    );
    store
        .compaction_record_task_output("ws", &run_turn, &history)
        .await
        .unwrap();
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
        "INSERT INTO compaction_delivery_output(delivery_id,candidate_id,task_run_turn_id) VALUES ($1,$2,$3)",
        [delivery.into(),delivery.into(),run_turn.into()],
    )).await.unwrap();
}

async fn delivered_ack(store: &CompactionStore, delivery: &str, turn: &str) {
    delivered_ack_with_body(store, delivery, turn, false).await;
}

async fn delivered_ack_with_body(
    store: &CompactionStore,
    delivery: &str,
    turn: &str,
    compressed: bool,
) {
    commit_item_event(
        store,
        pioneer_protocol::ItemCompletedNotification {
            workspace_id: "ws".into(),
            thread_id: "thread".into(),
            turn_id: turn.into(),
            item: pioneer_protocol::TurnItem::AgentMessage {
                id: pioneer_protocol::task_delivery_result_item_id(delivery),
                text: if compressed {
                    format!("result-{delivery} 🌍 ").repeat(8_192)
                } else {
                    format!("result-{delivery}")
                },
                phase: Default::default(),
                markdown: None,
                markdown_version: None,
            },
        },
    )
    .await;
}

async fn event_bytes(store: &CompactionStore, id: &str) -> String {
    use pioneer_crud_payloads::storage::{Selection, capture};
    let reader = store.database_connection().begin_read().await.unwrap();
    let row = reader
        .query_one_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "SELECT payload::text AS inline_payload,payload_payload_id FROM turn_event WHERE id=$1",
            [id.into()],
        ))
        .await
        .unwrap()
        .unwrap();
    if let Some(locator) = row
        .try_get::<Option<String>>("", "payload_payload_id")
        .unwrap()
    {
        let captured = capture(&reader, &locator, Selection::Full)
            .await
            .unwrap()
            .unwrap();
        reader.commit().await.unwrap();
        captured.into_text().unwrap()
    } else {
        reader.commit().await.unwrap();
        row.try_get("", "inline_payload").unwrap()
    }
}

async fn edit_event_bytes(store: &CompactionStore, id: &str, edit: impl FnOnce(String) -> String) {
    let bytes = edit(event_bytes(store, id).await);
    let body = pioneer_crud_payloads::storage::PreparedPayload::new(&bytes).unwrap();
    let db = store.database_connection();
    let tx = db.begin().await.unwrap();
    body.insert(&tx).await.unwrap();
    tx.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "UPDATE turn_event SET payload=NULL,payload_payload_id=$1 WHERE id=$2",
        [body.id().into(), id.into()],
    ))
    .await
    .unwrap();
    tx.commit().await.unwrap();
}

async fn repeat_delivered_ack(
    store: &CompactionStore,
    delivery: &str,
    turn: &str,
    id: &str,
    sequence: i64,
) {
    use pioneer_crud_conversations::turn_event::{NewEvent, insert};
    let db = store.database_connection();
    let original = db
        .query_one_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "SELECT e.id,e.event_type FROM turn_event e
         JOIN compaction_event_revision r ON r.source_id=e.id
         WHERE e.turn_id=$1 AND r.item_id=$2 ORDER BY r.capture_order LIMIT 1",
            [
                turn.into(),
                pioneer_protocol::task_delivery_result_item_id(delivery).into(),
            ],
        ))
        .await
        .unwrap()
        .unwrap();
    let original_id: String = original.try_get("", "id").unwrap();
    let event_type: String = original.try_get("", "event_type").unwrap();
    let wire = event_bytes(store, &original_id).await;
    let body = pioneer_crud_payloads::storage::PreparedPayload::new(&wire).unwrap();
    let tx = db.begin().await.unwrap();
    insert(
        &tx,
        NewEvent {
            id,
            thread_id: "thread",
            turn_id: turn,
            sequence,
            event_type: &event_type,
            idempotency_key: id,
            created_at: chrono::Utc::now().fixed_offset(),
        },
        &body,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let source = SourceRef {
        scope: format!("event:{turn}"),
        id: id.into(),
        version: "event-revision:1".into(),
    };
    let event = serde_json::from_str(&wire).unwrap();
    assert!(
        store
            .compaction_record_event_projection("ws", "thread", &source, &event)
            .await
            .unwrap()
    );
}

async fn output_metadata_padding(
    store: &CompactionStore,
    turn: &str,
    tag: &str,
    start: i64,
    count: i64,
) {
    for sequence in start..start + count {
        store.database_connection().execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
            "INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) VALUES ($1,'thread',$2,$3,'fixture','{}',CURRENT_TIMESTAMP)",
            [format!("{tag}-{sequence}").into(),turn.into(),sequence.into()],
        )).await.unwrap();
    }
}

async fn refresh_output_page(store: &CompactionStore, page: &DeliveredTaskOutputPage) {
    for source in &page.unprojected_events {
        let payload = store
            .compaction_reference_payload("ws", "thread", source)
            .await
            .unwrap()
            .unwrap();
        let event = serde_json::from_str(&payload).unwrap();
        assert!(
            store
                .compaction_record_event_projection("ws", "thread", source, &event)
                .await
                .unwrap()
        );
    }
}

async fn foreign_output_history(store: &CompactionStore) {
    let db = store.database_connection();
    for sql in [
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES ('foreign-ws','foreign',TRUE,FALSE) ON CONFLICT DO NOTHING",
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('foreign-thread','foreign-ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP) ON CONFLICT DO NOTHING",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('foreign-turn','foreign-thread','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP) ON CONFLICT DO NOTHING",
        "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<4096) INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) SELECT 'foreign-'||x,'foreign-thread','foreign-turn',x,'item/completed','{}',CURRENT_TIMESTAMP FROM n",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
}

async fn discover_output_metadata(
    store: &CompactionStore,
    fence: &HistoryReadFence,
) -> (Vec<DeliveredTaskOutputRef>, usize, usize) {
    let mut cursor = DeliveredTaskOutputCursor::default();
    let mut entries = Vec::new();
    let mut pages = 0;
    let mut event_rows = 0;
    loop {
        let page = store
            .compaction_delivered_output_page("ws", "thread", &cursor, fence)
            .await
            .unwrap();
        assert!(page.selected_turn_rows + page.selected_event_rows <= 128);
        assert!(page.unprojected_events.is_empty());
        pages += 1;
        assert!(pages <= 20, "local keyset cursor must terminate");
        event_rows += page.selected_event_rows;
        entries.extend(page.entries);
        if page.done {
            break;
        }
        assert_ne!(page.next_cursor, cursor);
        cursor = page.next_cursor;
    }
    entries.sort_by_key(|entry| entry.capture_order);
    (entries, pages, event_rows)
}

// Inspect the actual bound repository statements. Returned row counts alone
// cannot detect a hidden global revision/delivery scan inside a SQL plan.
async fn assert_output_discovery_plans(store: &CompactionStore, statements: &RecordedStatements) {
    let statements = statements.lock().unwrap().clone();
    let discovery = statements
        .into_iter()
        .filter(|statement| {
            statement.sql.starts_with("SELECT d.delivered_turn_id")
                || statement.sql.contains("WITH quantum AS MATERIALIZED")
        })
        .collect::<Vec<_>>();
    assert!(!discovery.is_empty());
    // This checks index availability for every query family, including empty
    // and sparse fixture tables. The actual paged reads above use normal
    // planner settings and assert their row budgets and exact continuations.
    let tx = store.database_connection().begin().await.unwrap();
    tx.execute_unprepared("SET LOCAL enable_seqscan = off")
        .await
        .unwrap();
    for mut statement in discovery {
        let event_page = statement.sql.contains("WITH quantum AS MATERIALIZED");
        let exact_recheck = statement.sql.contains("FROM json_array_elements(");
        assert!(!statement.sql.contains("SELECT *"));
        assert!(!statement.sql.contains("compaction_event_revision WHERE"));
        assert!(!statement.sql.contains("rowid"));
        assert!(!statement.sql.contains("payload"));
        if event_page && !exact_recheck {
            assert!(statement.sql.contains("e.sequence > $4"));
            assert!(statement.sql.contains("e.sequence<=$5"));
            assert!(statement.sql.contains("ORDER BY e.sequence LIMIT 127"));
        }
        statement.sql = format!("EXPLAIN (COSTS OFF) {}", statement.sql);
        let plan = tx
            .query_all_raw(statement)
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.try_get::<String>("", "QUERY PLAN").unwrap())
            .collect::<Vec<_>>();
        let uses = |index: &str| {
            plan.iter()
                .any(|detail| detail.contains("Index") && detail.contains(index))
        };
        if event_page {
            if exact_recheck {
                assert!(uses("turn_event_pkey"), "{plan:#?}");
            } else {
                assert!(uses("idx_turn_events_turn_id_sequence"), "{plan:#?}");
            }
            assert!(uses("compaction_event_revision_pkey"), "{plan:#?}");
            assert!(uses("task_delivery_pkey"), "{plan:#?}");
        } else {
            assert!(
                uses("compaction_delivery_turn") || uses("idx_task_delivery_inbox_result"),
                "{plan:#?}"
            );
            assert!(uses("compaction_turn_creation_turn_id_key"), "{plan:#?}");
            assert!(uses("idx_turn_events_turn_id_sequence"), "{plan:#?}");
        }
        assert!(
            !uses("compaction_event_revision_capture_order"),
            "{plan:#?}"
        );
    }
    tx.rollback().await.unwrap();
}

#[pioneer_postgres_test::postgres_test]
async fn delivered_output_local_keysets_preserve_sparse_outputs_and_repeated_acks() {
    for compressed in [false, true] {
        let statements = RecordedStatements::default();
        let store = store_recording_statements(Some(statements.clone())).await;
        for (delivery, turn) in [
            ("first", "z-turn"),
            ("second", "a-turn"),
            ("shared", "a-turn"),
            ("third", "m-turn"),
        ] {
            delivered_output_fixture(&store, delivery, turn).await;
        }
        let mut canonical_outputs = std::collections::BTreeSet::new();
        for delivery in ["first", "second", "shared", "third"] {
            let snapshot = store
                .compaction_delivery_output("ws", delivery)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                snapshot.output.source_turn,
                format!("output-turn-{delivery}")
            );
            assert_eq!(snapshot.output.task_run_turn_id, format!("rt-{delivery}"));
            assert_eq!(snapshot.candidate_id, delivery);
            assert_eq!(
                snapshot.output.history.manifest_id,
                format!("output-{delivery}")
            );
            assert!(canonical_outputs.insert(snapshot.output.source_turn));
        }
        assert_eq!(canonical_outputs.len(), 4);
        let before = store.compaction_history_read_fence().await.unwrap();
        delivered_ack_with_body(&store, "first", "z-turn", compressed).await;
        foreign_output_history(&store).await;
        delivered_ack(&store, "second", "a-turn").await;
        delivered_ack(&store, "shared", "a-turn").await;
        delivered_ack(&store, "third", "m-turn").await;
        // Put a replay on a different local page; sequence order deliberately
        // disagrees with capture order so first-encountered is invalid.
        for n in 2..=130 {
            store.database_connection().execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
                "INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) VALUES ($1,'thread','z-turn',$2,'fixture','{}',CURRENT_TIMESTAMP)",
                [format!("padding-{n}").into(),n.into()],
            )).await.unwrap();
        }
        repeat_delivered_ack(&store, "first", "z-turn", "repeated-first", 132).await;
        repeat_delivered_ack(&store, "first", "z-turn", "repeated-first-late", 131).await;
        let fence = store.compaction_history_read_fence().await.unwrap();
        statements.lock().unwrap().clear();
        let (entries, pages, rows) = discover_output_metadata(&store, &fence).await;
        assert_eq!(pages, 5); // a, m, two z pages, local exhaustion
        assert_eq!(rows, 135);
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.delivery_id.as_str())
                .collect::<Vec<_>>(),
            ["first", "second", "shared", "third", "first", "first"]
        );
        assert!(
            entries
                .windows(2)
                .all(|pair| pair[0].capture_order < pair[1].capture_order)
        );
        assert_eq!(entries[0].acknowledgement.version, "event-revision:1");
        assert_eq!(entries[4].acknowledgement.id, "repeated-first");
        assert_ne!(entries[0].acknowledgement.id, "repeated-first");
        assert_output_discovery_plans(&store, &statements).await;
        store.database_connection().execute_unprepared(
            "WITH RECURSIVE n(x) AS (VALUES(4097) UNION ALL SELECT x+1 FROM n WHERE x<8192) INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) SELECT 'foreign-'||x,'foreign-thread','foreign-turn',x,'item/completed','{}',CURRENT_TIMESTAMP FROM n",
        ).await.unwrap();
        let larger_fence = store.compaction_history_read_fence().await.unwrap();
        statements.lock().unwrap().clear();
        let (same, same_pages, same_rows) = discover_output_metadata(&store, &larger_fence).await;
        assert_eq!(same, entries);
        assert_eq!((same_pages, same_rows), (pages, rows));
        // Three local Turn seeks plus exhaustion, and four event pages.
        assert_eq!(statements.lock().unwrap().len(), 8);

        // Append during an existing fenced request. Old discovery neither
        // includes the new acknowledgement nor traverses foreign revisions.
        let first_page = store
            .compaction_delivered_output_page(
                "ws",
                "thread",
                &DeliveredTaskOutputCursor::default(),
                &fence,
            )
            .await
            .unwrap();
        assert!(!first_page.done);
        repeat_delivered_ack(&store, "third", "m-turn", "late-third", 2).await;
        let mut during_append = first_page.entries;
        let mut cursor = first_page.next_cursor;
        for n in 0..20 {
            let page = store
                .compaction_delivered_output_page("ws", "thread", &cursor, &fence)
                .await
                .unwrap();
            assert!(page.unprojected_events.is_empty());
            during_append.extend(page.entries);
            if page.done {
                break;
            }
            assert!(n < 19, "append must not prevent local exhaustion");
            assert_ne!(page.next_cursor, cursor);
            cursor = page.next_cursor;
        }
        during_append.sort_by_key(|entry| entry.capture_order);
        assert_eq!(during_append, entries);
        assert_eq!(discover_output_metadata(&store, &fence).await.0, entries);
        assert!(discover_output_metadata(&store, &before).await.0.is_empty());
        let after = store.compaction_history_read_fence().await.unwrap();
        assert_eq!(discover_output_metadata(&store, &after).await.0.len(), 7);
        for (workspace, thread) in [("foreign-ws", "thread"), ("ws", "foreign-thread")] {
            let page = store
                .compaction_delivered_output_page(
                    workspace,
                    thread,
                    &DeliveredTaskOutputCursor::default(),
                    &after,
                )
                .await
                .unwrap();
            assert!(page.done && page.entries.is_empty());
            assert_eq!(page.selected_event_rows, 0);
        }
        assert_eq!(
            store.database_connection().read_class(),
            pioneer_sqlite::SqliteReadClass::Maintenance
        );
        assert_eq!(
            store.database_connection().write_class(),
            pioneer_sqlite::SqliteWriteClass::Maintenance
        );
    }
}

#[pioneer_postgres_test::postgres_test]
async fn delivered_output_exact_full_page_exhausts_without_a_revision_range_seek() {
    let store = store().await;
    delivered_output_fixture(&store, "delivery", "delivery-turn").await;
    delivered_ack(&store, "delivery", "delivery-turn").await;
    for sequence in 2..=127 {
        store.database_connection().execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
            "INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) VALUES ($1,'thread','delivery-turn',$2,'fixture','{}',CURRENT_TIMESTAMP)",
            [format!("exact-padding-{sequence}").into(),sequence.into()],
        )).await.unwrap();
    }
    let fence = store.compaction_history_read_fence().await.unwrap();
    let first = store
        .compaction_delivered_output_page(
            "ws",
            "thread",
            &DeliveredTaskOutputCursor::default(),
            &fence,
        )
        .await
        .unwrap();
    assert_eq!(first.selected_event_rows, 127);
    assert_eq!(first.entries.len(), 1);
    assert!(!first.done);
    assert_eq!(first.next_cursor.after_sequence, Some(127));
    let empty_tail = store
        .compaction_delivered_output_page("ws", "thread", &first.next_cursor, &fence)
        .await
        .unwrap();
    assert_eq!(
        empty_tail.selected_turn_rows + empty_tail.selected_event_rows,
        0
    );
    assert!(!empty_tail.done);
    assert_ne!(empty_tail.next_cursor, first.next_cursor);
    assert_eq!(empty_tail.next_cursor.after_turn, "delivery-turn");
    let last = store
        .compaction_delivered_output_page("ws", "thread", &empty_tail.next_cursor, &fence)
        .await
        .unwrap();
    assert!(last.done && last.entries.is_empty());
    assert_eq!(last.selected_turn_rows + last.selected_event_rows, 0);
}

#[pioneer_postgres_test::postgres_test]
async fn delivered_output_cold_metadata_uses_scoped_revision_cas_and_source_freshness() {
    let store = store().await;
    delivered_output_fixture(&store, "delivery", "delivery-turn").await;
    delivered_ack(&store, "delivery", "delivery-turn").await;
    let fence = store.compaction_history_read_fence().await.unwrap();
    let cursor = DeliveredTaskOutputCursor::default();
    let warm = store
        .compaction_delivered_output_page("ws", "thread", &cursor, &fence)
        .await
        .unwrap();
    assert_eq!(warm.entries.len(), 1);
    let acknowledgement = &warm.entries[0].acknowledgement;
    let payload = store
        .compaction_reference_payload("ws", "thread", acknowledgement)
        .await
        .unwrap()
        .unwrap();
    let event: pioneer_crud_conversations::events::CanonicalTurnEventPayload =
        serde_json::from_str(&payload).unwrap();
    for projection in ["NULL", "revision-1"] {
        store.database_connection().execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
            format!("UPDATE compaction_event_revision SET projection_revision={projection},item_id=NULL,projection_kind=NULL WHERE source_id=$1"),
            [acknowledgement.id.clone().into()],
        )).await.unwrap();
        let cold = store
            .compaction_delivered_output_page("ws", "thread", &cursor, &fence)
            .await
            .unwrap();
        assert!(cold.entries.is_empty());
        assert_eq!(cold.unprojected_events, vec![acknowledgement.clone()]);
        assert!(
            store
                .compaction_reference_payload("foreign-ws", "thread", acknowledgement)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .compaction_record_event_projection("ws", "thread", acknowledgement, &event)
                .await
                .unwrap()
        );
        assert_eq!(
            store
                .compaction_delivered_output_page("ws", "thread", &cursor, &fence)
                .await
                .unwrap(),
            warm
        );
    }
    foreign_output_history(&store).await;
    store
        .database_connection()
        .execute_unprepared(
            "UPDATE task_delivery SET delivered_turn_id='foreign-turn' WHERE id='delivery'",
        )
        .await
        .unwrap();
    let foreign_locator = store
        .compaction_delivered_output_page("ws", "thread", &cursor, &fence)
        .await
        .unwrap();
    assert!(foreign_locator.done && foreign_locator.entries.is_empty());
    assert_eq!(foreign_locator.selected_event_rows, 0);
    store
        .database_connection()
        .execute_unprepared(
            "UPDATE task_delivery SET delivered_turn_id='delivery-turn' WHERE id='delivery'",
        )
        .await
        .unwrap();
    let epoch = store
        .compaction_projection_version("ws", "thread")
        .await
        .unwrap();
    edit_event_bytes(&store, &acknowledgement.id, |wire| wire + " ").await;
    assert_ne!(
        store
            .compaction_projection_version("ws", "thread")
            .await
            .unwrap(),
        epoch
    );
    assert!(
        !store
            .compaction_record_event_projection("ws", "thread", acknowledgement, &event)
            .await
            .unwrap()
    );
    assert!(
        store
            .compaction_reference_payload("ws", "thread", acknowledgement)
            .await
            .unwrap()
            .is_none()
    );
    let stale = store
        .compaction_delivered_output_page("ws", "thread", &cursor, &fence)
        .await
        .unwrap();
    assert!(stale.entries.is_empty());
    assert_eq!(stale.unprojected_events.len(), 1);
    assert_eq!(stale.unprojected_events[0].version, "event-revision:2");
    store
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "DELETE FROM turn_event WHERE id=$1",
            [acknowledgement.id.clone().into()],
        ))
        .await
        .unwrap();
    assert!(
        !store
            .compaction_record_event_projection("ws", "thread", acknowledgement, &event)
            .await
            .unwrap()
    );
    let deleted = store
        .compaction_delivered_output_page("ws", "thread", &cursor, &fence)
        .await
        .unwrap();
    assert!(deleted.entries.is_empty() && deleted.unprojected_events.is_empty());
    assert_eq!(deleted.selected_event_rows, 0);
}

#[pioneer_postgres_test::postgres_test]
async fn delivered_output_fence_terminates_despite_continuous_append_and_new_turns() {
    for compressed in [false, true] {
        let statements = RecordedStatements::default();
        let store = store_recording_statements(Some(statements.clone())).await;
        delivered_output_fixture(&store, "first", "a-old").await;
        delivered_output_fixture(&store, "second", "z-old").await;
        delivered_ack_with_body(&store, "first", "a-old", compressed).await;
        delivered_ack(&store, "second", "z-old").await;
        // A valid old acknowledgement can have a sequence far above the
        // global capture order. An event_order-as-sequence cutoff would lose it.
        let mut second_source = store
            .compaction_source_metadata_page("ws", "thread", "z-old", PagedSource::Event, 0)
            .await
            .unwrap()
            .entries[0]
            .reference
            .clone();
        store
            .database_connection()
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "UPDATE turn_event SET sequence=10000 WHERE id=$1",
                [second_source.id.clone().into()],
            ))
            .await
            .unwrap();
        second_source.version = "event-revision:2".into();
        let payload = store
            .compaction_reference_payload("ws", "thread", &second_source)
            .await
            .unwrap()
            .unwrap();
        let event = serde_json::from_str(&payload).unwrap();
        assert!(
            store
                .compaction_record_event_projection("ws", "thread", &second_source, &event)
                .await
                .unwrap()
        );
        output_metadata_padding(&store, "a-old", "before", 1002, 259).await;
        let fence = store.compaction_history_read_fence().await.unwrap();
        assert!(fence.event_order < 10000);
        let (expected, expected_pages, expected_rows) =
            discover_output_metadata(&store, &fence).await;
        assert_eq!((expected_pages, expected_rows), (5, 261));
        let mut cursor = DeliveredTaskOutputCursor::default();
        let mut entries = Vec::new();
        let mut sources = std::collections::BTreeSet::new();
        let mut rows = 0;
        let mut pages = 0_usize;
        loop {
            // A producer appends a full quantum before EVERY continuation,
            // including the would-be terminating request. New Turn IDs are
            // inside the fence's lexical range, so creation_order is essential.
            if pages > 0 {
                if pages == 1 {
                    // Late rows BEFORE an unvisited old acknowledgement must
                    // not fill its quantum or move the admitted continuation.
                    output_metadata_padding(&store, "z-old", "late-interleaved", 1, 127).await;
                }
                output_metadata_padding(
                    &store,
                    "a-old",
                    &format!("late-{pages}"),
                    1261 + (pages as i64 - 1) * 127,
                    127,
                )
                .await;
                repeat_delivered_ack(
                    &store,
                    "first",
                    "a-old",
                    &format!("late-low-{pages}"),
                    1 + pages as i64,
                )
                .await;
                let delivery = format!("late-delivery-{pages}");
                let turn = format!("b-new-{pages:02}");
                delivered_output_fixture(&store, &delivery, &turn).await;
                delivered_ack(&store, &delivery, &turn).await;
            }
            statements.lock().unwrap().clear();
            let page = store
                .compaction_delivered_output_page("ws", "thread", &cursor, &fence)
                .await
                .unwrap();
            assert!(page.selected_turn_rows + page.selected_event_rows <= 128);
            for selected in &page.selected_events {
                assert!(selected.capture_order <= fence.event_order);
                assert!(
                    sources.insert(selected.source.clone()),
                    "no source is selected twice"
                );
            }
            rows += page.selected_event_rows;
            entries.extend(page.entries);
            pages += 1;
            assert!(
                pages <= expected_pages,
                "post-fence appends must not extend discovery"
            );
            assert_output_discovery_plans(&store, &statements).await;
            if page.done {
                break;
            }
            if let Some(upper) = cursor.event_high_water {
                if page.next_cursor.active_turn == cursor.active_turn {
                    assert_eq!(page.next_cursor.event_high_water, Some(upper));
                }
            }
            assert_ne!(page.next_cursor, cursor);
            cursor = page.next_cursor;
        }
        entries.sort_by_key(|entry| entry.capture_order);
        assert_eq!(entries, expected);
        assert_eq!((pages, rows), (expected_pages, expected_rows));
        assert_eq!(sources.len(), expected_rows);
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.delivery_id.as_str())
                .collect::<Vec<_>>(),
            ["first", "second"]
        );
    }
}

#[pioneer_postgres_test::postgres_test]
async fn delivered_output_cold_page_refresh_keeps_exact_sources_across_append_boundaries() {
    for compressed in [false, true] {
        for count in [126, 127, 128] {
            let statements = RecordedStatements::default();
            let store = store_recording_statements(Some(statements.clone())).await;
            delivered_output_fixture(&store, "delivery", "delivery-turn").await;
            delivered_ack_with_body(&store, "delivery", "delivery-turn", compressed).await;
            output_metadata_padding(&store, "delivery-turn", "before", 3, count - 1).await;
            let fence = store.compaction_history_read_fence().await.unwrap();
            let warm = store
                .compaction_delivered_output_page(
                    "ws",
                    "thread",
                    &DeliveredTaskOutputCursor::default(),
                    &fence,
                )
                .await
                .unwrap();
            let ack = warm.entries[0].acknowledgement.clone();
            store.database_connection().execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
                "UPDATE compaction_event_revision SET projection_revision=NULL,item_id=NULL,projection_kind=NULL WHERE source_id=$1", [ack.id.clone().into()],
            )).await.unwrap();
            let cold = store
                .compaction_delivered_output_page(
                    "ws",
                    "thread",
                    &DeliveredTaskOutputCursor::default(),
                    &fence,
                )
                .await
                .unwrap();
            assert_eq!(cold.selected_event_rows, std::cmp::min(count, 127) as usize);
            assert_eq!(cold.selected_events, warm.selected_events);
            assert_eq!(cold.unprojected_events, [ack.clone()]);
            // Append between selection and CAS, then again between CAS and
            // exact recheck. Both higher and out-of-order sequences are after the fence.
            output_metadata_padding(&store, "delivery-turn", "after-selection", count + 2, 127)
                .await;
            store.database_connection().execute_unprepared(
                "INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at)
                 VALUES ('out-of-order-late','thread','delivery-turn',2,'fixture','{}',CURRENT_TIMESTAMP)"
            ).await.unwrap();
            refresh_output_page(&store, &cold).await;
            output_metadata_padding(&store, "delivery-turn", "after-cas", count + 129, 127).await;
            delivered_output_fixture(&store, "late", "late-turn").await;
            delivered_ack(&store, "late", "late-turn").await;
            statements.lock().unwrap().clear();
            let refreshed = store
                .compaction_recheck_delivered_output_page("ws", "thread", &cold, &fence)
                .await
                .unwrap();
            assert_eq!(refreshed, warm);
            assert_eq!(
                statements.lock().unwrap().len(),
                1,
                "refresh uses one exact-key query"
            );
            assert_output_discovery_plans(&store, &statements).await;
            assert!(
                store
                    .compaction_recheck_delivered_output_page("foreign-ws", "thread", &cold, &fence)
                    .await
                    .is_err()
            );
            let mut selected = refreshed
                .selected_events
                .into_iter()
                .map(|event| event.source)
                .collect::<std::collections::BTreeSet<_>>();
            let mut entries = refreshed.entries;
            let mut cursor = refreshed.next_cursor;
            loop {
                let tail = store
                    .compaction_delivered_output_page("ws", "thread", &cursor, &fence)
                    .await
                    .unwrap();
                assert!(tail.selected_turn_rows + tail.selected_event_rows <= 128);
                for event in tail.selected_events {
                    assert!(selected.insert(event.source));
                }
                entries.extend(tail.entries);
                if tail.done {
                    break;
                }
                assert_ne!(tail.next_cursor, cursor);
                cursor = tail.next_cursor;
            }
            assert_eq!(selected.len(), count as usize);
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].acknowledgement, ack);
        }
    }
}

#[pioneer_postgres_test::postgres_test]
async fn delivered_output_exact_refresh_rejects_source_and_known_binding_changes() {
    for compressed in [false, true] {
        for mutation in ["edit", "delete", "scope", "binding"] {
            let store = store().await;
            delivered_output_fixture(&store, "first", "shared-turn").await;
            delivered_output_fixture(&store, "second", "shared-turn").await;
            delivered_ack_with_body(&store, "first", "shared-turn", compressed).await;
            delivered_ack_with_body(&store, "second", "shared-turn", compressed).await;
            let fence = store.compaction_history_read_fence().await.unwrap();
            let warm = store
                .compaction_delivered_output_page(
                    "ws",
                    "thread",
                    &DeliveredTaskOutputCursor::default(),
                    &fence,
                )
                .await
                .unwrap();
            let first = warm.entries[0].acknowledgement.clone();
            store.database_connection().execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
                "UPDATE compaction_event_revision SET projection_revision=NULL,item_id=NULL,projection_kind=NULL WHERE source_id=$1", [first.id.clone().into()],
            )).await.unwrap();
            let cold = store
                .compaction_delivered_output_page(
                    "ws",
                    "thread",
                    &DeliveredTaskOutputCursor::default(),
                    &fence,
                )
                .await
                .unwrap();
            assert_eq!(
                cold.entries.len(),
                1,
                "a warm binding is retained alongside the cold source"
            );
            refresh_output_page(&store, &cold).await;
            assert_eq!(
                store
                    .compaction_recheck_delivered_output_page("ws", "thread", &cold, &fence)
                    .await
                    .unwrap(),
                warm
            );
            let db = store.database_connection();
            if mutation == "binding" {
                db.execute_unprepared("UPDATE compaction_delivery_output SET candidate_id='first' WHERE delivery_id='second'").await.unwrap();
            } else if mutation == "edit" {
                edit_event_bytes(&store, &first.id, |wire| wire + " ").await;
            } else {
                let sql = match mutation {
                    "delete" => "DELETE FROM turn_event WHERE id=$1",
                    "scope" => "UPDATE turn_event SET thread_id='other-thread' WHERE id=$1",
                    _ => unreachable!(),
                };
                db.execute_raw(Statement::from_sql_and_values(
                    DbBackend::Postgres,
                    sql,
                    [first.id.clone().into()],
                ))
                .await
                .unwrap();
            }
            assert!(
                store
                    .compaction_recheck_delivered_output_page("ws", "thread", &cold, &fence)
                    .await
                    .is_err(),
                "{mutation} must invalidate the selected page"
            );
        }
    }
}

#[pioneer_postgres_test::postgres_test]
async fn delivered_output_discovery_preserves_routes_and_cancels_queued_reads() {
    use pioneer_sqlite::{
        SqliteReadClass, SqliteReadEvent, SqliteReadObserver, SqliteReadOutcome, SqliteWriteEvent,
        SqliteWriteObserver,
    };
    use sea_orm::StreamTrait;
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };
    #[derive(Default)]
    struct Observer {
        reads: Mutex<Vec<SqliteReadEvent>>,
        writes: Mutex<Vec<SqliteWriteEvent>>,
        queued: tokio::sync::Notify,
    }
    impl SqliteReadObserver for Observer {
        fn observe(&self, event: SqliteReadEvent) {
            self.reads.lock().unwrap().push(event);
            if matches!(
                event,
                SqliteReadEvent::AdmissionEnqueued {
                    queue_depth: 1,
                    active: 1,
                    ..
                }
            ) {
                self.queued.notify_one();
            }
        }
    }
    impl SqliteWriteObserver for Observer {
        fn observe(&self, event: SqliteWriteEvent) {
            self.writes.lock().unwrap().push(event);
        }
    }
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let writer = fixture.connect_with_limit(1).await.unwrap();
    for sql in [
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES ('ws','fixture',TRUE,FALSE)",
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('turn','thread','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
    ] {
        writer.execute_unprepared(sql).await.unwrap();
    }
    let reader = fixture.connect_read_only().await.unwrap();
    let reader_proof = reader.clone();
    let observer = Arc::new(Observer::default());
    let database = pioneer_sqlite::SqliteDatabase::from_executor_with_read_observer(
        reader,
        pioneer_sqlite::SqliteWriteExecutor::with_observer(writer, observer.clone()),
        observer.clone(),
    );
    let interactive = CompactionStore::for_test(database.clone());
    delivered_output_fixture(&interactive, "delivery", "delivery-turn").await;
    delivered_ack(&interactive, "delivery", "delivery-turn").await;
    let fence = interactive.compaction_history_read_fence().await.unwrap();
    let maintenance = interactive.with_maintenance_access();
    observer.reads.lock().unwrap().clear();
    observer.writes.lock().unwrap().clear();
    let held = maintenance
        .database_connection()
        .stream_raw(Statement::from_string(
            DbBackend::Postgres,
            "SELECT 1".to_owned(),
        ))
        .await
        .unwrap();
    let waiting = tokio::spawn({
        let maintenance = maintenance.clone();
        let fence = fence.clone();
        async move {
            maintenance
                .compaction_delivered_output_page(
                    "ws",
                    "thread",
                    &DeliveredTaskOutputCursor::default(),
                    &fence,
                )
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(2), observer.queued.notified())
        .await
        .unwrap();
    // Interactive discovery remains on its reader route while maintenance is
    // queued. Both locator and bounded event query inherit the scoped class.
    let page = tokio::time::timeout(
        Duration::from_secs(2),
        interactive.compaction_delivered_output_page(
            "ws",
            "thread",
            &DeliveredTaskOutputCursor::default(),
            &fence,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(page.entries.len(), 1);
    waiting.abort();
    assert!(waiting.await.unwrap_err().is_cancelled());
    assert!(observer.reads.lock().unwrap().iter().any(|event| matches!(
        event,
        SqliteReadEvent::AdmissionCancelled {
            queue_depth: 0,
            active: 1,
            ..
        }
    )));
    drop(held);
    let resumed = tokio::time::timeout(
        Duration::from_secs(2),
        maintenance.compaction_delivered_output_page(
            "ws",
            "thread",
            &DeliveredTaskOutputCursor::default(),
            &fence,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(resumed, page);
    let reads = observer.reads.lock().unwrap().clone();
    for class in [SqliteReadClass::Interactive, SqliteReadClass::Maintenance] {
        assert_eq!(reads.iter().filter(|event| matches!(event,
            SqliteReadEvent::OperationFinished { class: actual, outcome: SqliteReadOutcome::Ok, .. } if *actual == class
        )).count(), 2);
    }
    assert!(reads.iter().any(|event| matches!(
        event,
        SqliteReadEvent::AdmissionReleased {
            queue_depth: 0,
            active: 0,
            ..
        }
    )));
    assert!(
        observer.writes.lock().unwrap().is_empty(),
        "metadata discovery must stay on the physical reader"
    );
    assert!(
        reader_proof
            .execute_unprepared("UPDATE task_delivery SET status='queued'")
            .await
            .is_err(),
        "the independently opened discovery reader must reject physical writes"
    );
}

#[pioneer_postgres_test::postgres_test]
async fn legacy_task_snapshot_reference_is_bounded_scoped_and_revision_guarded() {
    let store = store().await;
    let db = store.database_connection();
    for sql in [
        "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,created_by_turn_id,executor_kind,status,title,goal) VALUES ('task','ws','thread','thread','thread','turn','agent','running','Task','fixture')",
        "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES ('run','task','run',1,1,'running','agent')",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    let body = serde_json::json!([{"role":"user","content":"память🦀".repeat(9000)}]).to_string();
    store
        .insert_task_run_conversation_snapshot_if_absent(
            pioneer_crud_tasks::conversation_snapshot::NewTaskRunConversationSnapshot {
                run_id: "run".into(),
                task_id: "task".into(),
                workspace_id: "ws".into(),
                conversation_thread_id: "thread".into(),
                source_turn_id: Some("turn".into()),
                history_json: body.clone(),
                created_at: chrono::Utc::now().fixed_offset(),
            },
        )
        .await
        .unwrap();
    let source = store
        .compaction_legacy_task_basis_source("ws", "thread", "run")
        .await
        .unwrap()
        .unwrap();
    assert!(
        store
            .compaction_legacy_task_basis_source("other", "thread", "run")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .compaction_reference_fragment("ws", "other", &source, 0)
            .await
            .unwrap()
            .is_none()
    );
    let mut restored = String::new();
    let mut offset = 0;
    loop {
        let fragment = store
            .compaction_reference_fragment("ws", "thread", &source, offset)
            .await
            .unwrap()
            .unwrap();
        assert!(fragment.text.len() <= 64 * 1024);
        restored.push_str(&fragment.text);
        match fragment.next_character {
            Some(next) => offset = next,
            None => break,
        }
    }
    assert_eq!(restored, body);
    let epoch = store
        .compaction_projection_version("ws", "thread")
        .await
        .unwrap();
    let replacement =
        pioneer_crud_payloads::storage::PreparedPayload::new(&format!("{body} ")).unwrap();
    let tx = db.begin().await.unwrap();
    replacement.insert(&tx).await.unwrap();
    tx.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "UPDATE task_run_conversation_snapshot SET history_json_payload_id=$1 WHERE run_id='run'",
        [replacement.id().into()],
    ))
    .await
    .unwrap();
    tx.commit().await.unwrap();
    assert!(
        store
            .compaction_reference_fragment("ws", "thread", &source, 0)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .compaction_projection_version("ws", "thread")
            .await
            .unwrap()
            > epoch
    );
    let current = store
        .compaction_legacy_task_basis_source("ws", "thread", "run")
        .await
        .unwrap()
        .unwrap();
    assert_ne!(current.version, source.version);
    db.execute_unprepared("DELETE FROM task_run_conversation_snapshot WHERE run_id='run'")
        .await
        .unwrap();
    assert!(
        store
            .compaction_reference_thread("ws", &current)
            .await
            .unwrap()
            .is_none()
    );
}

#[pioneer_postgres_test::postgres_test]
async fn causal_task_boundary_requires_identified_delivery_inside_capture_fence() {
    let store = store().await;
    let db = store.database_connection();
    for sql in [
        "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,created_by_turn_id,executor_kind,status,title,goal) VALUES ('task','ws','thread','thread','thread','turn','agent','running','Task','fixture')",
        "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES ('run','task','run',1,1,'running','agent')",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('run','thread','completed','conversation','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO task_delivery(id,workspace_id,task_id,run_id,delivery_key,mode,thread_target,target_thread_id,status,attempt_count,max_attempts,delivered_turn_id) VALUES ('delivery','ws','task','run','key','thread','origin_thread','thread','delivered',1,1,'run')",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    let empty = store.compaction_history_read_fence().await.unwrap();
    let boundary = store
        .compaction_history_causal_boundary("ws", "thread", "turn", &empty)
        .await
        .unwrap();
    assert!(boundary.delegated_command);
    assert!(
        !boundary.delivered_outcome,
        "delivery status without an acknowledged payload is not closure"
    );
    assert!(
        store
            .compaction_history_causal_boundary("ws", "thread", "run", &empty)
            .await
            .unwrap()
            .task_transport
    );
    commit_item_event(
        &store,
        pioneer_protocol::ItemCompletedNotification {
            workspace_id: "ws".into(),
            thread_id: "thread".into(),
            turn_id: "run".into(),
            item: pioneer_protocol::TurnItem::AgentMessage {
                id: "unrelated-response".into(),
                text: "not the Task result".into(),
                phase: Default::default(),
                markdown: None,
                markdown_version: None,
            },
        },
    )
    .await;
    let unrelated = store.compaction_history_read_fence().await.unwrap();
    assert!(
        !store
            .compaction_history_causal_boundary("ws", "thread", "turn", &unrelated)
            .await
            .unwrap()
            .delivered_outcome
    );
    commit_item_event(
        &store,
        pioneer_protocol::ItemCompletedNotification {
            workspace_id: "ws".into(),
            thread_id: "thread".into(),
            turn_id: "run".into(),
            item: pioneer_protocol::TurnItem::SystemEvent {
                id: pioneer_protocol::task_delivery_result_item_id("delivery"),
                level: pioneer_protocol::SystemEventLevel::Error,
                message: "acknowledged failed outcome".into(),
                code: Some("unavailable".into()),
                details: None,
            },
        },
    )
    .await;
    let completed = store.compaction_history_read_fence().await.unwrap();
    assert!(
        store
            .compaction_history_causal_boundary("ws", "thread", "turn", &completed)
            .await
            .unwrap()
            .delivered_outcome
    );
    assert!(
        !store
            .compaction_history_causal_boundary("ws", "thread", "turn", &unrelated)
            .await
            .unwrap()
            .delivered_outcome,
        "later delivery must not cross the accepted snapshot fence"
    );
    assert!(
        store
            .compaction_history_causal_boundary("other", "thread", "turn", &completed)
            .await
            .is_err()
    );
    let entries = store
        .compaction_source_page("ws", "thread", "run", PagedSource::Event, 0)
        .await
        .unwrap()
        .entries;
    let result = entries
        .iter()
        .find(|entry| {
            entry
                .payload
                .as_deref()
                .is_some_and(|p| p.contains("acknowledged failed outcome"))
        })
        .unwrap()
        .reference
        .clone();
    assert_eq!(
        store
            .compaction_task_delivery_command("ws", "thread", &result)
            .await
            .unwrap()
            .as_deref(),
        Some("turn")
    );
    let unrelated_source = entries
        .iter()
        .find(|entry| {
            entry
                .payload
                .as_deref()
                .is_some_and(|p| p.contains("unrelated-response"))
        })
        .unwrap()
        .reference
        .clone();
    assert!(
        store
            .compaction_task_delivery_command("ws", "thread", &unrelated_source)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .compaction_task_delivery_command("other", "thread", &result)
            .await
            .unwrap()
            .is_none()
    );
    let mut stale_result = result.clone();
    stale_result.version = "event-revision:999".into();
    assert!(
        store
            .compaction_task_delivery_command("ws", "thread", &stale_result)
            .await
            .unwrap()
            .is_none()
    );
    let event: pioneer_crud_conversations::events::CanonicalTurnEventPayload =
        serde_json::from_str(
            entries
                .iter()
                .find(|entry| entry.reference == result)
                .unwrap()
                .payload
                .as_deref()
                .unwrap(),
        )
        .unwrap();
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "UPDATE compaction_event_revision SET projection_revision=0 WHERE source_id=$1",
        [result.id.clone().into()],
    ))
    .await
    .unwrap();
    assert!(
        store
            .compaction_task_delivery_command("ws", "thread", &result)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        !store
            .compaction_record_event_projection("ws", "thread", &stale_result, &event)
            .await
            .unwrap()
    );
    assert!(
        store
            .compaction_record_event_projection("other", "thread", &result, &event)
            .await
            .is_err()
    );
    assert!(
        store
            .compaction_record_event_projection("ws", "thread", &result, &event)
            .await
            .unwrap()
    );
    assert_eq!(
        store
            .compaction_task_delivery_command("ws", "thread", &result)
            .await
            .unwrap()
            .as_deref(),
        Some("turn")
    );
    db.execute_unprepared("UPDATE task_delivery SET status='delivering' WHERE id='delivery'")
        .await
        .unwrap();
    assert!(
        !store
            .compaction_history_causal_boundary("ws", "thread", "turn", &completed)
            .await
            .unwrap()
            .delivered_outcome
    );
    assert!(
        store
            .compaction_task_delivery_command("ws", "thread", &result)
            .await
            .unwrap()
            .is_none()
    );
}
