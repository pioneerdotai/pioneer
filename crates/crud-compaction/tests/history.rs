use pioneer_compaction::SourceRef;
use pioneer_crud_compaction::history::compaction_task_delivery_command;
use pioneer_crud_conversations::turn_event;
use pioneer_crud_payloads::storage::PreparedPayload;
use sea_orm::{ConnectionTrait, DbBackend, Statement, TransactionTrait};

#[pioneer_postgres_test::postgres_test]
async fn exact_delivery_source_uses_event_keys_instead_of_scanning_revision_history() {
    for portable in [false, true] {
        let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
        let mut db = fixture.connection().clone();
        let statements = std::sync::Arc::new(std::sync::Mutex::new(Vec::<Statement>::new()));
        let capture = statements.clone();
        db.set_metric_callback(move |info| capture.lock().unwrap().push(info.statement.clone()));
        db.execute_unprepared(
            "INSERT INTO workspace(id,name) VALUES ('ws','fixture');
             INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status)
             VALUES ('thread','ws','','agent','m','p','active');
             INSERT INTO turn(id,thread_id,status,turn_kind,origin)
             VALUES ('turn','thread','completed','conversation','user')",
        )
        .await
        .unwrap();
        let wire = format!(r#"{{"text":"{}"}}"#, "ordinary history".repeat(1024));
        if portable {
            let prepared = PreparedPayload::new(&wire).unwrap();
            let tx = db.begin().await.unwrap();
            turn_event::lock_stream(&tx, "turn").await.unwrap();
            turn_event::insert(
                &tx,
                turn_event::NewEvent {
                    id: "ordinary",
                    thread_id: "thread",
                    turn_id: "turn",
                    sequence: 1,
                    event_type: "fixture",
                    idempotency_key: "ordinary",
                    created_at: "2026-11-19T10:00:00Z".parse().unwrap(),
                },
                &prepared,
            )
            .await
            .unwrap();
            tx.commit().await.unwrap();
        } else {
            db.execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload)
                 VALUES ('ordinary','thread','turn',1,'fixture',$1::text::json)",
                [wire.into()],
            ))
            .await
            .unwrap();
        }
        statements.lock().unwrap().clear();
        assert!(
            compaction_task_delivery_command(
                &db,
                "ws",
                "thread",
                &SourceRef {
                    scope: "event:turn".into(),
                    id: "ordinary".into(),
                    version: "event-revision:1".into(),
                }
            )
            .await
            .unwrap()
            .is_none()
        );
        // Inspect the actual repository statement, including its bound values.
        // This guards query work independently of machine speed and row count.
        let mut statement = statements
            .lock()
            .unwrap()
            .iter()
            .find(|statement| {
                statement.sql.contains("EXISTS") && statement.sql.contains("\"outcome\"")
            })
            .expect("failed-delivery fallback must be exercised")
            .clone();
        statement.sql = format!("EXPLAIN (COSTS OFF) {}", statement.sql);
        let plan = db
            .query_all_raw(statement)
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.try_get::<String>("", "QUERY PLAN").unwrap())
            .collect::<Vec<_>>();
        assert!(
            plan.iter()
                .any(|detail| detail.contains("compaction_event_revision_pkey")),
            "{plan:#?}"
        );
        assert!(
            plan.iter().any(|detail| {
                detail.contains("Index Scan using") && detail.contains("on turn_event")
            }) && plan.iter().any(|detail| {
                detail.contains("Index Cond:") && detail.contains("id = 'ordinary'")
            }),
            "{plan:#?}"
        );
        assert!(
            !plan
                .iter()
                .any(|detail| detail.contains("compaction_event_revision_capture_order")),
            "{plan:#?}"
        );
    }
}
