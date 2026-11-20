use pioneer_crud::CrudStore;
use pioneer_crud_payloads::storage::PreparedPayload;
use pioneer_postgres_test::{TestDatabase, with_database};
use pioneer_sqlite::SqliteDatabase;
use sea_orm::{ConnectionTrait, DbBackend, Statement, TransactionTrait};

async fn open(fixture: &TestDatabase) -> CrudStore {
    CrudStore::new(SqliteDatabase::new(
        fixture.connect_read_only().await.unwrap(),
        fixture.connect().await.unwrap(),
    ))
    .with_maintenance_access()
}

async fn scalar(store: &CrudStore, sql: &str) -> i64 {
    store
        .database_connection()
        .query_one_raw(Statement::from_string(DbBackend::Postgres, sql))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "n")
        .unwrap()
}

async fn fixture(db: &TestDatabase) -> CrudStore {
    db.connection()
        .execute_unprepared(
            r#"
        INSERT INTO workspace(id,name) VALUES ('ws','fixture');
        INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status)
            VALUES ('thread','ws','','agent','m','p','active');
        INSERT INTO turn(id,thread_id,status,turn_kind,origin)
            VALUES ('turn','thread','completed','conversation','user');
        INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload)
            VALUES ('event','thread','turn',1,'fixture','{"text":"original"}');
        INSERT INTO turn_item(id,turn_id,item_id,item_type,status,payload)
            VALUES ('item','turn','item','command_execution','completed','{"text":"original"}');
        INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload)
            VALUES ('input','turn',0,'text','original','{"text":"original"}');
        INSERT INTO turn_llm_context(id,turn_id,sequence,source,payload,output_policy_snapshot)
            VALUES ('context','turn',1,'assistant_round','{"text":"original"}','{}');
    "#,
        )
        .await
        .unwrap();
    open(db).await
}

async fn replace_event(fixture: &TestDatabase, text: &str) -> String {
    let body = PreparedPayload::new(text).unwrap();
    let tx = fixture.connection().begin().await.unwrap();
    body.insert(&tx).await.unwrap();
    let result = tx
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "UPDATE turn_event SET payload=NULL, payload_payload_id=$1 WHERE id='event'",
            [body.id().into()],
        ))
        .await
        .unwrap();
    assert_eq!(result.rows_affected(), 1);
    tx.commit().await.unwrap();
    body.id().to_owned()
}

#[tokio::test]
async fn a_canonical_edit_invalidates_the_compaction_reference_for_a_stored_payload() {
    with_database(async |db| {
        let store = fixture(db).await;
        let text = serde_json::json!({"text": "сжатие 🦀".repeat(10_000)}).to_string();
        let value = replace_event(db, &text).await;
        let source = pioneer_compaction::SourceRef {
            scope: "event:turn".into(),
            id: "event".into(),
            version: "event-revision:2".into(),
        };
        let epoch = store.compaction_projection_version("ws", "thread").await?;
        let original = store
            .compaction_reference_fragment("ws", "thread", &source, 0)
            .await?
            .expect("current event reference");
        assert_eq!(original.text, text.chars().take(16_384).collect::<String>());
        assert!(original.next_character.is_some());
        assert_eq!(original.reference, source);
        assert!(
            store
                .compaction_references_current("ws", "thread", std::slice::from_ref(&source))
                .await?
        );
        assert_eq!(
            scalar(
                &store,
                "SELECT count(DISTINCT payload_id) n FROM portable_payload_part WHERE codec=1"
            )
            .await,
            1
        );

        store
            .database_connection()
            .execute_unprepared("UPDATE turn_event SET sequence=sequence+1 WHERE id='event'")
            .await?;
        let updated = pioneer_compaction::SourceRef {
            version: "event-revision:3".into(),
            ..source.clone()
        };
        assert!(store.compaction_projection_version("ws", "thread").await? > epoch);
        assert!(
            !store
                .compaction_references_current("ws", "thread", std::slice::from_ref(&source))
                .await?
        );
        assert!(
            store
                .compaction_reference_fragment("ws", "thread", &source, 0)
                .await
                .is_err()
        );
        assert_eq!(
            store
                .compaction_reference_payload("ws", "thread", &updated)
                .await?,
            Some(text)
        );
        assert!(
            pioneer_crud_payloads::storage::metadata(db.connection(), &value)
                .await?
                .is_some()
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn whole_and_batched_payload_loading_preserves_unicode_large_values_and_order() {
    with_database(async |db| {
    let store = fixture(db).await;
    let large = serde_json::json!({"text": "🧪漢字".repeat(90_000)}).to_string();
    replace_event(db, &large).await;
    let event = pioneer_compaction::SourceRef {
        scope: "event:turn".into(),
        id: "event".into(),
        version: "event-revision:2".into(),
    };
    assert_eq!(
        store
            .compaction_reference_payload("ws", "thread", &event)
            .await
            .unwrap()
            .as_deref(),
        Some(large.as_str())
    );

    let first = serde_json::to_string(&"a".repeat(130_000)).unwrap();
    let second = serde_json::to_string(&"б".repeat(65_000)).unwrap();
    let third = serde_json::to_string(&"c".repeat(130_000)).unwrap();
    for (index, (id, payload)) in [("input", &first), ("input-2", &second), ("input-3", &third)]
        .into_iter()
        .enumerate()
    {
        if index == 0 {
            store
                .database_connection()
                .execute_raw(Statement::from_sql_and_values(
                    DbBackend::Postgres,
                    "UPDATE turn_input SET payload=$1::text::json WHERE id=$2",
                    [payload.clone().into(), id.into()],
                ))
                .await
                .unwrap();
        } else {
            store.database_connection().execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) VALUES($1, 'turn', $2::bigint::int, 'text', '', $3::text::json, CURRENT_TIMESTAMP)",
                [id.into(), (index as i64).into(), payload.clone().into()],
            )).await.unwrap();
        }
    }
    let refs = [
        pioneer_compaction::SourceRef {
            scope: "input:turn".into(),
            id: "input".into(),
            version: "input-revision:2".into(),
        },
        pioneer_compaction::SourceRef {
            scope: "input:turn".into(),
            id: "input-2".into(),
            version: "input-revision:1".into(),
        },
        pioneer_compaction::SourceRef {
            scope: "input:turn".into(),
            id: "input-3".into(),
            version: "input-revision:1".into(),
        },
    ];
    let (consumed, payloads) = store
        .compaction_reference_payload_batch("ws", "thread", &refs)
        .await
        .unwrap();
    assert_eq!(consumed, 2);
    assert_eq!(payloads, [first, second]);
    let (consumed, payloads) = store
        .compaction_reference_payload_batch("ws", "thread", &refs[2..])
        .await
        .unwrap();
    assert_eq!(consumed, 1);
    assert_eq!(payloads, [third]);
    store
        .database_connection()
        .execute_unprepared("UPDATE turn_input SET payload='null' WHERE id='input-2'")
        .await
        .unwrap();
    assert!(
        store
            .compaction_reference_payload_batch("ws", "thread", &refs[..2])
            .await
            .is_err(),
        "a stale member cannot disappear from a successful batch"
    );
    store
        .database_connection()
        .execute_unprepared("DELETE FROM turn_input WHERE id='input-3'")
        .await
        .unwrap();
    assert!(
        store
            .compaction_reference_payload_batch("ws", "thread", &refs[2..])
            .await
            .is_err(),
        "a tombstoned member cannot disappear from a successful batch"
    );
        Ok(())
    }).await.unwrap();
}

/// Counts the operations and retained bytes for a mixed history page.

#[tokio::test]
#[ignore = "manual history preparation benchmark; run only in the dedicated benchmark stage"]
async fn benchmark_history_payload_loading_many_small_and_one_large_compressed_result() {
    with_database(async |db| {
    let store = fixture(db).await;
    for index in 1..=1_000_i64 {
        store.database_connection().execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) VALUES($1, 'turn', $2::bigint::int, 'text', '', $3::text::json, CURRENT_TIMESTAMP)",
            [format!("bench-{index}").into(), index.into(), serde_json::json!({"type":"text","text":format!("message-{index}")}).to_string().into()],
        )).await.unwrap();
    }
    let large = serde_json::json!({"result":"z".repeat(4 * 1024 * 1024)}).to_string();
    replace_event(db, &large).await;

    let mut refs = (1..=1_000_i64)
        .map(|index| pioneer_compaction::SourceRef {
            scope: "input:turn".into(),
            id: format!("bench-{index}"),
            version: "input-revision:1".into(),
        })
        .collect::<Vec<_>>();
    refs.push(pioneer_compaction::SourceRef {
        scope: "event:turn".into(),
        id: "event".into(),
        version: "event-revision:2".into(),
    });
    let started = std::time::Instant::now();
    let mut offset = 0usize;
    let mut batches = 0usize;
    let mut retained_bytes = 0usize;
    while offset < refs.len() {
        let (consumed, payloads) = store
            .compaction_reference_payload_batch("ws", "thread", &refs[offset..])
            .await
            .unwrap();
        retained_bytes = retained_bytes.max(payloads.iter().map(String::len).sum());
        offset += consumed;
        batches += 1;
    }
    eprintln!(
        "history_payload_crud_microbenchmark rows={} measured_payload_calls={} estimated_reader_queries={} measured_max_returned_bytes={} elapsed_ms={}",
        refs.len(),
        batches,
        batches * 2 - 1,
        retained_bytes,
        started.elapsed().as_millis()
    );
        Ok(())
    }).await.unwrap();
}

#[tokio::test]
async fn stale_compaction_references_remain_stale_after_compression_and_restart() {
    with_database(async |db| {
        let store = fixture(db).await;
        let reference = pioneer_compaction::SourceRef {
            scope: "event:turn".into(),
            id: "event".into(),
            version: "event-revision:1".into(),
        };
        store
            .compaction_reference_fragment("ws", "thread", &reference, 0)
            .await
            .unwrap()
            .unwrap();
        let old_value = replace_event(
            db,
            &serde_json::json!({"text":"edited".repeat(20_000)}).to_string(),
        )
        .await;
        assert!(
            store
                .compaction_reference_fragment("ws", "thread", &reference, 0)
                .await
                .unwrap_err()
                .to_string()
                .contains("stale source revision")
        );
        assert!(
            store
                .compaction_reference_payload("ws", "thread", &reference)
                .await
                .unwrap()
                .is_none(),
            "a stale exact revision must not return a payload"
        );
        assert!(
            store
                .compaction_reference_payload_batch(
                    "ws",
                    "thread",
                    std::slice::from_ref(&reference)
                )
                .await
                .is_err(),
            "a stale batch must not return a partial success"
        );
        assert_eq!(
            scalar(
                &store,
                "SELECT revision n FROM compaction_event_revision WHERE source_id='event'"
            )
            .await,
            2
        );
        drop(store);
        let store = open(db).await;
        assert!(
            store
                .compaction_reference_fragment("ws", "thread", &reference, 0)
                .await
                .unwrap_err()
                .to_string()
                .contains("stale source revision")
        );
        assert!(
            store
                .compaction_reference_payload("ws", "thread", &reference)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .compaction_reference_payload_batch(
                    "ws",
                    "thread",
                    std::slice::from_ref(&reference)
                )
                .await
                .is_err()
        );
        assert_eq!(
            scalar(
                &store,
                "SELECT revision n FROM compaction_event_revision WHERE source_id='event'"
            )
            .await,
            2
        );
        let current_text = serde_json::json!({"text":"edited again".repeat(15_000)}).to_string();
        let current_value = replace_event(db, &current_text).await;
        assert!(
            pioneer_crud_payloads::storage::metadata(db.connection(), &old_value)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            scalar(
                &store,
                "SELECT revision n FROM compaction_event_revision WHERE source_id='event'"
            )
            .await,
            3
        );
        let current = pioneer_compaction::SourceRef {
            scope: "event:turn".into(),
            id: "event".into(),
            version: "event-revision:3".into(),
        };
        assert_eq!(
            store
                .compaction_reference_payload("ws", "thread", &current)
                .await
                .unwrap()
                .as_deref(),
            Some(current_text.as_str()),
        );
        store
            .database_connection()
            .execute_unprepared("DELETE FROM turn_event WHERE id='event'")
            .await
            .unwrap();
        assert!(
            pioneer_crud_payloads::storage::metadata(db.connection(), &current_value)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .compaction_reference_payload("ws", "thread", &current)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .compaction_reference_payload_batch("ws", "thread", std::slice::from_ref(&current))
                .await
                .is_err()
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn whole_and_batch_reads_reject_noncanonical_revision_aliases() {
    with_database(async |db| {
        let store = fixture(db).await;
        for (scope, id, version) in [
            ("input:turn", "input", "input-revision:01"),
            ("context:turn", "context", "revision:+1"),
            ("event:turn", "event", "event-revision:01"),
            ("item:turn", "item", "item-revision:+1"),
        ] {
            let reference = pioneer_compaction::SourceRef {
                scope: scope.into(),
                id: id.into(),
                version: version.into(),
            };
            assert!(
                store
                    .compaction_reference_payload("ws", "thread", &reference)
                    .await
                    .is_err(),
                "whole read accepted noncanonical version {version}"
            );
            assert!(
                store
                    .compaction_reference_payload_batch(
                        "ws",
                        "thread",
                        std::slice::from_ref(&reference),
                    )
                    .await
                    .is_err(),
                "batch read accepted noncanonical version {version}"
            );
        }
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn portable_cli_mapping_preserves_sent_bytes_control_updates_and_upsert() {
    with_database(async |db| {
    let store = fixture(db).await;
    let at = chrono::Utc::now().fixed_offset();
    let history = serde_json::to_string(
        &serde_json::json!([{"role":"user","content":"контекст🦀".repeat(12000)}]),
    )
    .unwrap();
    let mut original = format!(
        "  {}\n",
        serde_json::json!({"pioneerContextBasis":{"historyJson":history,"executionThreadId":"thread","pendingTurn":{"turnId":"turn","messageRevision":0,"messageDeleted":false}},"input":"original sent bytes"})
    );
    let mut binding = pioneer_crud::NewCliRuntimeTurnBinding {
        turn_id: "turn".into(),
        thread_id: "thread".into(),
        continuation_thread_id: "thread".into(),
        workspace_id: "ws".into(),
        runtime_id: "codex".into(),
        runtime_kind: "codex".into(),
        native_thread_id: "native".into(),
        native_turn_id: None,
        request_id: Some("portable-request".into()),
        status: "starting".into(),
        model: None,
        cwd: None,
        sandbox_json: None,
        approval_policy: None,
        input_mapping_json: original.clone(),
        created_at: at,
        updated_at: at,
    };
    store
        .update_turn_status(
            "thread",
            "turn",
            pioneer_protocol::TurnStatus::InProgress,
            None,
            at.timestamp(),
        )
        .await
        .unwrap();
    store
        .prepare_cli_runtime_initial_turn_attempt(binding.clone(), "mapping-attempt".into(), 1)
        .await
        .unwrap();
    store
        .activate_cli_runtime_turn_attempt("turn", "mapping-attempt", "native-control-1", None, at)
        .await
        .unwrap();
    store
        .register_cli_runtime_control_execution_segment("turn", "native", "native-control-1", at)
        .await
        .unwrap();
    binding.status = "running".into();
    binding.native_turn_id = Some("native-control-1".into());
    // Keep the mapping binding active after the owning Turn is sealed.
    store
        .update_turn_status(
            "thread",
            "turn",
            pioneer_protocol::TurnStatus::Completed,
            None,
            at.timestamp(),
        )
        .await
        .unwrap();
    // Insignificant JSON whitespace is part of the original sent bytes.
    original.push_str(" \n");
    binding.input_mapping_json = original.clone();
    store.upsert_cli_runtime_turn_binding(binding.clone()).await.unwrap();
    assert_eq!(scalar(&store,
        "SELECT count(*) n FROM turn_cli_runtime_binding WHERE turn_id='turn' AND input_mapping_codec=1 AND input_mapping_bytes>131072 AND octet_length(input_mapping_json)<input_mapping_bytes AND input_mapping_has_inline_history").await, 1);
    assert_eq!(
        store
            .get_cli_runtime_turn_binding_by_request("portable-request")
            .await
            .unwrap()
            .unwrap()
            .input_mapping_json,
        original
    );
    assert_eq!(
        store
            .set_cli_runtime_turn_mcp_metadata("turn", None)
            .await
            .unwrap()
            .input_mapping_json,
        original
    );
    // Invalid frame bytes must fail content reads while metadata stays usable.
    store.database_connection().execute_unprepared(
        "UPDATE turn_cli_runtime_binding SET input_mapping_json=decode('ff','hex') WHERE turn_id='turn'"
    ).await.unwrap();
    let compressed_guard = store.cli_runtime_turn_terminal_control_guard_by_id("turn").await.unwrap().unwrap();
    let metadata = store
        .get_cli_runtime_turn_binding_control("turn")
        .await
        .unwrap()
        .unwrap();
    assert!(metadata.input_mapping_json.is_empty());
    let frontier = store
        .active_cli_runtime_turn_binding_frontier(
            pioneer_crud::CliRuntimeActiveTurnBindingStatus::Running,
        )
        .await
        .unwrap()
        .unwrap();
    let page = store
        .list_active_cli_runtime_turn_binding_control_page_through(
            pioneer_crud::CliRuntimeActiveTurnBindingStatus::Running,
            None,
            &frontier,
        )
        .await
        .unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0], metadata);
    assert_eq!(
        store
            .cli_runtime_turn_terminal_control_guard(&metadata)
            .await
            .unwrap()
            .unwrap(),
        compressed_guard
    );
    assert!(store.get_cli_runtime_turn_binding("turn").await.is_err());
    assert!(
        store
            .cli_runtime_turn_terminal_guard_by_id("turn")
            .await
            .is_err()
    );
    assert!(
        store
            .load_cli_runtime_control_mapping(&metadata)
            .await
            .is_err()
    );
    let owner = store
        .resolve_cli_runtime_native_turn_control_owner("codex", "native-control-1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(owner.attempt.id, "mapping-attempt");
    assert_eq!(owner.binding, metadata);
    assert!(
        store
            .resolve_cli_runtime_native_turn_owner("codex", "native-control-1")
            .await
            .is_err()
    );
    let terminal_segment = store
        .terminalize_cli_runtime_control_execution_segment(
            "codex",
            "native-control-1",
            pioneer_crud::CliRuntimeExecutionSegmentStatus::Completed,
            None,
            at,
        )
        .await
        .unwrap()
        .unwrap();
    assert!(terminal_segment.0.input_mapping_json.is_empty());
    assert_eq!(
        terminal_segment.2.status,
        pioneer_crud::CliRuntimeExecutionSegmentStatus::Completed
    );
    let next_segment = store
        .register_cli_runtime_control_execution_segment("turn", "native", "native-control-2", at)
        .await
        .unwrap();
    assert!(next_segment.0.input_mapping_json.is_empty());
    assert_eq!(
        next_segment.2.segment_index,
        terminal_segment.2.segment_index + 1
    );
    let selected = store
        .cli_runtime_turn_terminal_control_guard_by_id("turn")
        .await
        .unwrap()
        .unwrap();
    // A sealed Turn fences late Goal events even when the control path can read metadata.
    assert!(
        store
            .set_cli_runtime_turn_native_goal_control_state(
                "turn",
                Some("active".into()),
                Some("native-control-2".into()),
                at,
                Some(selected),
                "fixture-owner",
            )
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .get_cli_runtime_turn_binding_control("turn")
            .await
            .unwrap()
            .unwrap(),
        next_segment.0
    );
    let metadata = store
        .get_cli_runtime_turn_binding_control("turn")
        .await
        .unwrap()
        .unwrap();
    let listed = store
        .list_cli_runtime_turn_binding_controls(pioneer_crud::CliRuntimeTurnBindingListFilter {
            workspace_id: Some("ws".into()),
            runtime_id: Some("codex".into()),
            statuses: vec!["running".into()],
            limit: Some(1),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(listed, vec![metadata.clone()]);
    assert!(
        store
            .load_cli_runtime_control_mapping(&metadata)
            .await
            .is_err()
    );
    binding.input_mapping_json =
        r#"{ "pioneerContextBasis": {"historyJson":"[]","executionThreadId":"thread","pendingTurn":{"turnId":"turn","messageRevision":0,"messageDeleted":false}}, "input":"replacement" }"#.into();
    let replacement = binding.input_mapping_json.clone();
    assert_eq!(
        store
            .upsert_cli_runtime_turn_binding(binding)
            .await
            .unwrap()
            .input_mapping_json,
        replacement
    );
    assert!(
        store
            .cli_runtime_turn_terminal_control_guard(&metadata)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .load_cli_runtime_control_mapping(&metadata)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .get_cli_runtime_turn_binding("turn")
            .await
            .unwrap()
            .unwrap()
            .input_mapping_json,
        replacement
    );
    assert_eq!(scalar(&store, "SELECT count(*) n FROM turn_cli_runtime_binding WHERE turn_id='turn' AND NOT input_mapping_has_inline_history").await, 1);
        Ok(())
    }).await.unwrap();
}

fn stage_one_completion() -> pioneer_protocol::ItemCompletedNotification {
    pioneer_protocol::ItemCompletedNotification {
        workspace_id: "ws".into(),
        thread_id: "thread".into(),
        turn_id: "turn".into(),
        item: pioneer_protocol::TurnItem::SystemEvent {
            id: "stage-one-item".into(),
            level: pioneer_protocol::SystemEventLevel::Info,
            message: "точные portable bytes 🦀\n".repeat(12000),
            code: Some("stage.one.fixture".into()),
            details: None,
        },
    }
}

#[tokio::test]
async fn portable_turn_payload_real_producer_restart_retry_and_corruption() {
    with_database(async |db| {
    let store = fixture(db).await;
    // Remove the unrelated noncanonical fixture bodies. The real producer below
    // owns sequence allocation, item projection, receipts and watermark.
    store
        .database_connection()
        .execute_unprepared("DELETE FROM turn_event; DELETE FROM turn_item")
        .await
        .unwrap();
    let completion = stage_one_completion();
    store
        .materialize_item_snapshot_updated(
            pioneer_protocol::ItemUpdatedNotification {
                workspace_id: completion.workspace_id.clone(),
                thread_id: completion.thread_id.clone(),
                turn_id: completion.turn_id.clone(),
                item: completion.item.clone(),
            },
            1_700_000_099,
        )
        .await
        .unwrap();
    // A standalone snapshot persists an encoded item without a canonical event.
    assert_eq!(scalar(&store, "SELECT count(*) n FROM turn_event").await, 0);
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) n FROM turn_item WHERE payload_payload_id IS NOT NULL"
        )
        .await,
        1
    );
    assert_eq!(
        store.get_turn_item("turn", "stage-one-item").await.unwrap(),
        Some(completion.item.clone())
    );
    store
        .materialize_item_completed(completion.clone(), 1_700_000_100)
        .await
        .unwrap();
    store
        .materialize_item_completed(completion.clone(), 1_700_000_100)
        .await
        .unwrap();
    assert_eq!(
        store.get_turn_item("turn", "stage-one-item").await.unwrap(),
        Some(completion.item.clone())
    );
    drop(store);
    let reopened = open(db).await;
    assert_eq!(
        scalar(
            &reopened,
            "SELECT count(*) n FROM turn_event WHERE payload_payload_id IS NOT NULL"
        )
        .await,
        1
    );
    assert_eq!(
        scalar(
            &reopened,
            "SELECT count(*) n FROM turn_item WHERE payload_payload_id IS NOT NULL"
        )
        .await,
        1
    );
    assert_eq!(
        reopened
            .get_turn_item("turn", "stage-one-item")
            .await
            .unwrap(),
        Some(completion.item.clone())
    );
    // Replay uses the exact bytes of the persisted event.
    reopened
        .materialize_item_completed(completion.clone(), 1_700_000_100)
        .await
        .unwrap();
    assert_eq!(
        scalar(&reopened, "SELECT count(*) n FROM turn_event").await,
        1
    );
    assert_eq!(scalar(&reopened, "SELECT count(DISTINCT payload_id) n FROM portable_payload_part WHERE codec=1").await,2);
    reopened.database_connection().execute_unprepared(
        "DELETE FROM portable_payload_part WHERE payload_id=(SELECT payload_payload_id FROM turn_item WHERE item_id='stage-one-item') AND byte_offset=0"
    ).await.unwrap();
    let error = reopened
        .get_turn_item("turn", "stage-one-item")
        .await
        .unwrap_err();
    assert!(
        format!("{error:#}").contains("portable payload"),
        "{error:#}"
    );
    // Remove that independent fault before testing the event header. Otherwise
    // a later item preflight could make the next negative control pass falsely.
    reopened
        .database_connection()
        .execute_unprepared("DELETE FROM turn_item WHERE item_id='stage-one-item'")
        .await
        .unwrap();
    reopened
        .materialize_item_completed(completion.clone(), 1_700_000_100)
        .await
        .unwrap();
    reopened.database_connection().execute_unprepared("DELETE FROM portable_payload_part WHERE payload_id=(SELECT payload_payload_id FROM turn_event LIMIT 1) AND byte_offset=0").await.unwrap();
    let error = reopened
        .materialize_item_completed(completion, 1_700_000_100)
        .await
        .unwrap_err();
    assert!(
        format!("{error:#}").contains("portable"),
        "missing block lost its portable error: {error:#}"
    );
        Ok(())
    }).await.unwrap();
}
