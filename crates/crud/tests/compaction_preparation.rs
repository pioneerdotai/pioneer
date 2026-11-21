use pioneer_crud::CrudStore;
use pioneer_postgres_test::{TestDatabase, with_database};
use pioneer_sqlite::SqliteDatabase;
use sea_orm::{ConnectionTrait, DbBackend, Statement, TransactionTrait};

async fn scalar(store: &CrudStore, sql: &str) -> i64 {
    store
        .database_connection()
        .query_one_raw(Statement::from_string(
            store.database_connection().get_database_backend(),
            sql,
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "n")
        .unwrap()
}

async fn seed_metadata(db: &sea_orm::DatabaseConnection, compressed: bool) {
    for sql in [
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES('ws','fixture',TRUE,FALSE)",
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES('thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),('other','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES('turn','thread','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),('other-turn','other','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "WITH RECURSIVE nums(n) AS (VALUES(0) UNION ALL SELECT n+1 FROM nums WHERE n<299) INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) SELECT 'input-'||n,'turn',n,'text','legacy','{\"type\":\"text\",\"text\":\"legacy\"}',CURRENT_TIMESTAMP FROM nums",
        "WITH RECURSIVE nums(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM nums WHERE n<300) INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) SELECT 'event-'||n,'thread','turn',n,'fixture','{}',CURRENT_TIMESTAMP FROM nums",
        "WITH RECURSIVE nums(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM nums WHERE n<300) INSERT INTO turn_llm_context(id,turn_id,sequence,source,payload,output_policy_snapshot,created_at) SELECT 'context-'||n,'turn',n,'tool_result_v2','{}','{}',CURRENT_TIMESTAMP FROM nums",
        "INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) VALUES('unrelated','other-turn',0,'text','untouched','{}',CURRENT_TIMESTAMP)",
        "INSERT INTO turn_llm_context(id,turn_id,sequence,source,payload,output_policy_snapshot,created_at) VALUES('unrelated-context','other-turn',1,'tool_result_v2','{}','{}',CURRENT_TIMESTAMP)",
        "INSERT INTO turn_item(id,turn_id,item_id,item_type,status,active_attempt_number,payload,created_at,updated_at) VALUES('legacy-item','turn','legacy-item','command_execution','completed',0,'{\"text\":\"legacy 🧪\"}',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    if compressed {
        let wire = serde_json::json!({"source":"🔧".repeat(32_768)}).to_string();
        let payload = pioneer_crud_payloads::storage::PreparedPayload::new(&wire).unwrap();
        let transaction = db.begin().await.unwrap();
        payload.insert(&transaction).await.unwrap();
        transaction
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "UPDATE turn_event SET payload=NULL,payload_payload_id=$1 WHERE id='event-1'",
                [payload.id().into()],
            ))
            .await
            .unwrap();
        transaction.commit().await.unwrap();
    }
}

#[pioneer_postgres_test::postgres_test]
async fn canonical_revisions_preserve_independent_edits_deletes_and_compressed_sources() {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let writer = fixture.connect().await.unwrap();
    seed_metadata(fixture.connection(), true).await;
    let reader = fixture.connect_read_only().await.unwrap();
    let store = CrudStore::new(SqliteDatabase::new(reader, fixture.connection().clone()));
    writer
        .execute_unprepared(
            "UPDATE turn_input SET payload='{\"edited\":true}' WHERE id='input-250'",
        )
        .await
        .unwrap();
    writer
        .execute_unprepared("DELETE FROM turn_input WHERE id='input-251'")
        .await
        .unwrap();
    writer.execute_unprepared("INSERT INTO turn_input(id,turn_id,input_index,input_type,payload,created_at) VALUES('new','turn',300,'text','{}',CURRENT_TIMESTAMP)").await.unwrap();
    assert!(
        store
            .compaction_source_page(
                "ws",
                "thread",
                "turn",
                pioneer_crud::compaction::PagedSource::Event,
                0
            )
            .await
            .unwrap()
            .entries
            .iter()
            .any(|event| event.reference.id == "event-1")
    );
    assert_eq!(
        scalar(
            &store,
            "SELECT revision AS n FROM compaction_input_revision WHERE source_id='input-250'"
        )
        .await,
        2
    );
    assert_eq!(
        scalar(
            &store,
            "SELECT present AS n FROM compaction_input_revision WHERE source_id='input-251'"
        )
        .await,
        0
    );
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) AS n FROM compaction_input_revision WHERE source_id='new'"
        )
        .await,
        1
    );
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) AS n FROM compaction_event_revision WHERE present=1"
        )
        .await,
        300
    );
    assert_eq!(scalar(&store,"SELECT count(*) AS n FROM turn_input WHERE id='input-250' AND payload::text='{\"edited\":true}'").await,1);
}

#[pioneer_postgres_test::postgres_test]
async fn cancelled_frozen_conversion_and_output_writes_reopen_with_a_read_only_pool() {
    let mut frozen_conversion_progress = pioneer_crud::FrozenStorageLifetimeProgress::default();
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let writer = fixture.connect_with_limit(1).await.unwrap();
    seed_metadata(&writer, false).await;
    let reader = fixture.connect_read_only().await.unwrap();
    let database = SqliteDatabase::new(reader, writer);
    assert!(database.reader_query_only_enabled().await.unwrap());
    let store = CrudStore::new(database.clone()).with_maintenance_access();
    let output = pioneer_protocol::ItemDeltaNotification {
        workspace_id: "ws".into(),
        thread_id: "thread".into(),
        turn_id: "turn".into(),
        item_id: "persisted-shell".into(),
        delta: "before cancellation".into(),
        stream: Some(pioneer_protocol::ItemDeltaStream::Stdout),
        payload: None,
        markdown: None,
        markdown_version: None,
    };
    store
        .record_tool_output("physical-output", &output)
        .await
        .unwrap();
    // Complete bounded scope discovery while no frozen rows exist. The
    // cancellation below then waits for a real conversion writer, not a
    // metadata-only prerequisite read.
    for _ in 0..64 {
        store
            .compact_frozen_storage_quantum(&mut frozen_conversion_progress)
            .await
            .unwrap();
    }
    let reference: pioneer_compaction::frozen::FrozenMessageRef = serde_json::from_value(serde_json::json!({
        "logical_turn_id":"turn", "source_thread":"thread", "context_thread":null, "unit_id":"u",
        "sources":[{"scope":"event:turn","id":"event-1","version":"event-revision:1"}],
        "inherited":false,"complete":true,"protected_input":false,"wire_sha256":"a".repeat(64),
        "replay_source":null,"tool_call_id":null,"tool_name":null
    })).unwrap();
    for id in ["storage-a", "storage-b"] {
        let descriptor = pioneer_compaction::frozen::FrozenHistoryRef {
            format: 1,
            manifest_id: id.into(),
            messages: 1,
            identity_sha256: "b".repeat(64),
        };
        store
            .compaction_begin_frozen_history("ws", "thread", &descriptor)
            .await
            .unwrap();
        store
            .compaction_append_frozen_history(
                "ws",
                "thread",
                id,
                0,
                std::slice::from_ref(&reference),
            )
            .await
            .unwrap();
        assert!(
            store
                .compaction_finish_frozen_history("ws", "thread", &descriptor)
                .await
                .unwrap()
        );
    }
    let layouts_before = scalar(&store, "SELECT count(*) AS n FROM compaction_frozen_layout").await;
    let held = database.begin().await.unwrap();
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(50),
            CrudStore::new(database.clone()).record_tool_output("cancelled-output", &output)
        )
        .await
        .is_err()
    );

    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(50),
            store.compact_frozen_storage_quantum(&mut frozen_conversion_progress)
        )
        .await
        .is_err()
    );
    held.rollback().await.unwrap();
    let rows = store
        .tool_output_page("ws", "thread", "turn", "persisted-shell", 0)
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "cancelled queued write must not persist later"
    );
    store
        .record_tool_output("after-cancel", &output)
        .await
        .unwrap();

    assert_eq!(
        scalar(&store, "SELECT count(*) AS n FROM compaction_frozen_layout").await,
        layouts_before
    );
    for _ in 0..4 {
        assert!(
            store
                .compact_frozen_storage_quantum(&mut frozen_conversion_progress)
                .await
                .unwrap()
        );
    }
    drop(store);
    drop(database);
    let writer = fixture.connect_with_limit(1).await.unwrap();
    let reader = fixture.connect_read_only().await.unwrap();
    let reopened = CrudStore::new(SqliteDatabase::new(reader, writer)).with_maintenance_access();
    let mut frozen_conversion_progress = pioneer_crud::FrozenStorageLifetimeProgress::default();
    let rows = reopened
        .tool_output_page("ws", "thread", "turn", "persisted-shell", 0)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert!(
        rows.iter()
            .all(|(_, row)| row.text == "before cancellation")
    );
    for _ in 0..512 {
        reopened
            .compact_frozen_storage_quantum(&mut frozen_conversion_progress)
            .await
            .unwrap();
    }
    assert_eq!(
        scalar(
            &reopened,
            "SELECT count(*) AS n FROM compaction_frozen_message_data"
        )
        .await,
        1
    );
    for id in ["storage-a", "storage-b"] {
        assert_eq!(
            reopened
                .compaction_frozen_history_page("ws", "thread", id, 0)
                .await
                .unwrap(),
            vec![reference.clone()]
        );
    }
}

async fn postgres_store(database: &TestDatabase) -> CrudStore {
    let writer = database.connection();
    writer
        .execute_unprepared(
            "INSERT INTO workspace(id,name) VALUES ('ws','Compaction fixtures');
         INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,access_class)
         VALUES ('thread','ws','','agent','model','provider','active','workspace'),
                ('other','ws','','agent','model','provider','active','workspace');
         INSERT INTO turn(id,thread_id,status) VALUES ('turn','thread','completed'),
                ('other-turn','other','completed');",
        )
        .await
        .unwrap();
    let reader = database.connect_read_only().await.unwrap();
    CrudStore::new(SqliteDatabase::new(reader, writer.clone())).with_maintenance_access()
}

#[tokio::test]
async fn history_check_retry_survives_reopening_the_pool_and_a_cancelled_claim() {
    with_database(async |fixture| {
        use pioneer_crud::compaction::{HistoryCheckDiagnostic as D, HistoryCheckOutcome as O};
        use sea_orm::EntityTrait;
        use std::time::Duration;
        let store = postgres_store(fixture).await;
        let mut thread = store.get_thread_model("thread").await.unwrap().unwrap();
        thread.updated_at = 1;
        let (_, mut turn) = store.get_turn("thread", "turn").await.unwrap().unwrap();
        turn.id = "root-turn".into();
        turn.status = pioneer_protocol::TurnStatus::InProgress;
        store
            .materialize_turn_start(
                &thread,
                pioneer_protocol::SandboxMode::FullAccess,
                &turn,
                &[pioneer_protocol::UserInput::Text {
                    text: "new root question".into(),
                    text_elements: Vec::new(),
                }],
                pioneer_protocol::PersistedActorRef::Principal(
                    pioneer_protocol::PrincipalId::new("P00000000000000000001").unwrap(),
                ),
            )
            .await
            .unwrap();
        store
            .materialize_item_completed(
                pioneer_protocol::ItemCompletedNotification {
                    workspace_id: "ws".into(),
                    thread_id: "thread".into(),
                    turn_id: turn.id.clone(),
                    item: pioneer_protocol::TurnItem::AgentMessage {
                        id: "root-answer".into(),
                        text: "ready root answer".into(),
                        phase: Default::default(),
                        markdown: None,
                        markdown_version: None,
                    },
                },
                1,
            )
            .await
            .unwrap();
        turn.status = pioneer_protocol::TurnStatus::Completed;
        store
            .materialize_turn_completed(
                pioneer_protocol::TurnCompletedNotification {
                    workspace_id: "ws".into(),
                    thread_id: "thread".into(),
                    turn,
                },
                1,
            )
            .await
            .unwrap();
        let page = store.compaction_due_history_checks(1000).await.unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].turn_id, "root-turn");
        let revision = page[0].revision;
        let writer = fixture.connect().await.unwrap();
        let held = writer.begin().await.unwrap();
        let holder_pid: i32 = held
            .query_one_raw(Statement::from_string(
                DbBackend::Postgres,
                "SELECT pg_backend_pid()",
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get_by_index(0)
            .unwrap();
        held.query_one_raw(Statement::from_string(
            DbBackend::Postgres,
            "SELECT turn_id FROM compaction_history_check WHERE turn_id='root-turn' FOR UPDATE",
        ))
        .await
        .unwrap()
        .unwrap();
        let claim_store = store.clone();
        let contender = tokio::spawn(async move {
            claim_store
                .compaction_claim_history_check("root-turn", revision, 1000)
                .await
        });
        let reader = fixture.connect_read_only().await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let waiting: bool = reader.query_one_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))",
                [holder_pid.into()],
            )).await.unwrap().unwrap().try_get_by_index(0).unwrap();
                if waiting {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the competing claim must reach the PostgreSQL row lock");
        assert_eq!(scalar(&store, "SELECT count(*) AS n FROM turn").await, 3);
        contender.abort();
        assert!(contender.await.unwrap_err().is_cancelled());
        held.rollback().await.unwrap();
        assert_eq!(
            scalar(
                &store,
                "SELECT revision AS n FROM compaction_history_check WHERE turn_id='root-turn'"
            )
            .await,
            revision
        );
        let claim = store
            .compaction_claim_history_check("root-turn", revision, 1000)
            .await
            .unwrap()
            .unwrap();
        store
            .compaction_begin_history_attempt(
                "root-turn",
                claim.revision,
                "{}",
                "safe-digest",
                1000,
            )
            .await
            .unwrap();
        store
            .compaction_record_history_result(
                "root-turn",
                claim.revision,
                0,
                O::Retryable,
                &D::new("history_capture", "database_error", "Temporary failure"),
                1000,
            )
            .await
            .unwrap();
        drop(store);
        let reopened = CrudStore::new(SqliteDatabase::new(
            fixture.connect_read_only().await.unwrap(),
            fixture.connect().await.unwrap(),
        ))
        .with_maintenance_access();
        assert!(
            reopened
                .compaction_due_history_checks(60999)
                .await
                .unwrap()
                .is_empty()
        );
        let page = reopened.compaction_due_history_checks(61000).await.unwrap();
        assert_eq!(page[0].failures, 1);
        assert_eq!(page[0].config_hash.as_deref(), Some("safe-digest"));
        let row = pioneer_entity::compaction_history_check::Entity::find_by_id("root-turn")
            .one(&reopened.database_connection())
            .await
            .unwrap()
            .unwrap();
        assert!(
            row.diagnostic
                .unwrap()
                .to_string()
                .contains("database_error")
        );
        Ok(())
    })
    .await
    .unwrap();
}
