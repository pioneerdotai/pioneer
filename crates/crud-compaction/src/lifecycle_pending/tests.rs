use super::*;
use pioneer_compaction::{ModelBudget, runner::RunnerState};
use pioneer_postgres_test::TestDatabase;
use sea_orm::{DatabaseConnection, DbBackend, QueryTrait, Statement};
use std::sync::Arc;

const NOW: i64 = 2_000_000_000_000;

async fn fixture() -> Arc<TestDatabase> {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    fixture.connection().execute_unprepared(
        "INSERT INTO workspace(id,name) VALUES ('ws','Lifecycle');
         INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class)
         VALUES ('thread','ws','','agent','m','p','active','user','workspace');
         INSERT INTO turn(id,thread_id,status,turn_kind,origin)
         VALUES ('turn','thread','completed','conversation','user');
         INSERT INTO compaction_context(owner,workspace_id,thread_id,format_version)
         VALUES ('owner','ws','thread',1);"
    ).await.unwrap();
    fixture
}

async fn operation(db: &DatabaseConnection, id: &str, status: &str, deadline: i64) {
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,execution_turn,deadline_ms)
         VALUES($1,'owner',$1,$2,'{}','turn',$3)",
        [id.into(),status.into(),deadline.into()],
    )).await.unwrap();
}

async fn state(db: &DatabaseConnection, id: &str) {
    let state = RunnerState::new(
        NOW as u64,
        &ModelBudget::new(Some(128_000), None, Some(16_384)),
        1_000,
        None,
    )
    .unwrap();
    assert!(
        crate::runner_state::insert_initial(db, id, serde_json::to_string(&state).unwrap())
            .await
            .unwrap()
    );
}

async fn row(db: &DatabaseConnection, id: &str) -> Option<pending::Model> {
    pending::Entity::find_by_id(id).one(db).await.unwrap()
}

async fn scalar(db: &DatabaseConnection, sql: &str) -> i64 {
    db.query_one_raw(Statement::from_string(DbBackend::Postgres, sql))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "n")
        .unwrap()
}

async fn consume(db: &DatabaseConnection, id: &str, now: i64) -> bool {
    let Some(claim) = claim(db, &row(db, id).await.unwrap(), &|| now)
        .await
        .unwrap()
    else {
        return false;
    };
    let tx = db.begin().await.unwrap();
    assert!(owns(&tx, &claim).await.unwrap());
    acknowledge(&tx, id, None).await.unwrap();
    tx.commit().await.unwrap();
    true
}

async fn expand_all(db: &DatabaseConnection, now: i64) {
    for _ in 0..100 {
        if expand(db, true, &|| now).await.unwrap() + expand(db, false, &|| now).await.unwrap() == 0
        {
            return;
        }
    }
    panic!("bounded scope queue did not drain");
}

#[pioneer_postgres_test::postgres_test]
async fn lifecycle_scope_page_rollback_keeps_lease_and_restart_progress() {
    let fixture = fixture().await;
    let db = fixture.connection();
    expand_all(db, NOW).await;
    for id in ["a", "b", "c", "d"] {
        operation(db, id, "running", NOW + 60_000).await;
        acknowledge(db, id, Some(NOW + 60_000)).await.unwrap();
    }
    db.execute_unprepared(
        "UPDATE turn SET status='interrupted' WHERE id='turn';
         CREATE FUNCTION reject_page() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN RAISE EXCEPTION 'scope page rollback'; END; $$;
         CREATE TRIGGER reject_page BEFORE INSERT ON compaction_lifecycle_pending
         FOR EACH ROW WHEN (NEW.operation_id='b') EXECUTE FUNCTION reject_page()",
    )
    .await
    .unwrap();
    let error = expand(db, false, &|| NOW).await.unwrap_err();
    assert!(error.to_string().contains("scope page rollback"));
    assert!(
        due(db, NOW).await.unwrap().is_empty(),
        "the first enqueue rolls back with the page"
    );
    let interrupted = scope::Entity::find()
        .filter(scope::Column::Kind.eq("turn"))
        .filter(scope::Column::TurnId.eq("turn"))
        .one(db)
        .await
        .unwrap()
        .unwrap();
    assert!(interrupted.claim_token.is_some());
    assert_eq!(interrupted.attempts, 1);
    assert_eq!(interrupted.due_at, NOW + 5_000);
    assert!(interrupted.cursor_id.is_none() && interrupted.upper_id.is_none());
    db.execute_unprepared("DROP TRIGGER reject_page ON compaction_lifecycle_pending")
        .await
        .unwrap();
    let reopened = fixture.connect().await.unwrap();
    assert_eq!(expand(&reopened, false, &|| NOW).await.unwrap(), 0);
    expand_all(&reopened, NOW + 5_000).await;
    let due = due(&reopened, NOW + 5_000).await.unwrap();
    assert_eq!(
        due.into_iter()
            .map(|row| row.operation_id)
            .collect::<Vec<_>>(),
        ["a", "b", "c", "d"]
    );
    assert_eq!(expand(&reopened, false, &|| NOW + 5_000).await.unwrap(), 0);
}

#[pioneer_postgres_test::postgres_test]
async fn lifecycle_claim_failure_exact_deferral_poison_progress_and_generation_exhaustion() {
    let fixture = fixture().await;
    let db = fixture.connection();
    for id in ["a-poison", "b-good"] {
        operation(db, id, "failed", NOW).await;
        state(db, id).await;
    }
    db.execute_unprepared("CREATE FUNCTION reject_claim() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'poison reservation'; END; $$;
        CREATE TRIGGER reject_claim BEFORE UPDATE OF claim_token ON compaction_lifecycle_pending FOR EACH ROW WHEN (OLD.operation_id='a-poison' AND NEW.claim_token IS NOT NULL) EXECUTE FUNCTION reject_claim()").await.unwrap();
    let batch = due(db, NOW).await.unwrap();
    assert_eq!(batch.len(), 2);
    for row in batch {
        if row.operation_id == "a-poison" {
            assert!(claim(db, &row, &|| NOW).await.is_err());
        } else {
            assert!(consume(db, &row.operation_id, NOW).await);
        }
    }
    let delayed = row(db, "a-poison").await.unwrap();
    assert_eq!(delayed.due_at, NOW + 5_000);
    assert_eq!(delayed.attempts, 1);
    assert!(delayed.claim_token.is_none());
    assert!(due(db, NOW).await.unwrap().is_empty());
    let stale = delayed.clone();
    db.execute_unprepared("UPDATE compaction_operation SET outcome='changed' WHERE id='a-poison'")
        .await
        .unwrap();
    assert!(claim(db, &stale, &|| NOW + 5_000).await.unwrap().is_none());
    let fresh = row(db, "a-poison").await.unwrap();
    assert_eq!(
        fresh.attempts, 1,
        "stale deferral cannot mutate a new generation"
    );
    db.execute_unprepared("CREATE FUNCTION reject_deferral() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'bookkeeping unavailable'; END; $$;
        CREATE TRIGGER reject_deferral BEFORE UPDATE OF attempts ON compaction_lifecycle_pending FOR EACH ROW WHEN (OLD.operation_id='a-poison') EXECUTE FUNCTION reject_deferral()").await.unwrap();
    let error = claim(db, &fresh, &|| NOW + 5_000).await.unwrap_err();
    assert!(
        error
            .downcast_ref::<CompactionLifecycleStorageError>()
            .is_some()
    );
    assert_eq!(row(db, "a-poison").await.unwrap(), fresh);
    db.execute_unprepared("DROP TRIGGER reject_deferral ON compaction_lifecycle_pending")
        .await
        .unwrap();
    db.execute_unprepared("DROP TRIGGER reject_claim ON compaction_lifecycle_pending")
        .await
        .unwrap();
    db.execute_unprepared(
        "UPDATE compaction_lifecycle_sequence SET generation=9223372036854775807",
    )
    .await
    .unwrap();
    assert!(
        db.execute_unprepared(
            "UPDATE compaction_operation SET outcome='overflow' WHERE id='a-poison'"
        )
        .await
        .is_err()
    );
    assert_eq!(
        scalar(
            db,
            "SELECT count(*) n FROM compaction_operation WHERE outcome='changed'"
        )
        .await,
        1
    );
    assert_eq!(row(db, "a-poison").await.unwrap(), fresh);
}

#[pioneer_postgres_test::postgres_test]
async fn lifecycle_idle_discovery_and_scope_indexes_have_no_historical_antijoin() {
    let fixture = fixture().await;
    let db = fixture.connection();
    expand_all(db, NOW).await;
    for index in 0..64 {
        operation(db, &format!("old-{index}"), "failed", NOW).await;
    }
    for index in 0..64 {
        assert!(consume(db, &format!("old-{index}"), NOW).await);
    }
    assert!(due(db, NOW).await.unwrap().is_empty());
    assert_eq!(expand(db, true, &|| NOW).await.unwrap(), 0);
    assert_eq!(expand(db, false, &|| NOW).await.unwrap(), 0);
    // Plans run only after acceptance. This code verifies exact index shapes
    // as well as the due prefix, rather than timing a small fixture.
    for (name, columns) in [
        (
            "idx_compaction_lifecycle_execution",
            vec!["execution_turn", "id"],
        ),
        (
            "idx_compaction_lifecycle_owner_execution",
            vec!["owner", "execution_turn", "id"],
        ),
        ("idx_compaction_lifecycle_owner", vec!["owner", "id"]),
        ("idx_compaction_lifecycle_running", vec!["status", "id"]),
        (
            "idx_compaction_lifecycle_due",
            vec!["due_at", "generation", "operation_id"],
        ),
    ] {
        let rows = db
            .query_all_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "SELECT pg_get_indexdef(indexrelid,key_no,true) AS name FROM pg_index CROSS JOIN LATERAL generate_series(1,indnkeyatts) AS key_no WHERE indexrelid=$1::text::regclass ORDER BY key_no",
                [name.into()],
            ))
            .await
            .unwrap();
        assert_eq!(
            rows.iter()
                .map(|r| r.try_get::<String>("", "name").unwrap())
                .collect::<Vec<_>>(),
            columns
        );
    }
    db.execute_unprepared(
        "INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,execution_turn,deadline_ms)
         SELECT 'future-'||n,'owner','future-'||n,'running','{}'::json,'turn',2000000060000
         FROM generate_series(1,1024) n;
         ANALYZE compaction_lifecycle_pending;",
    ).await.unwrap();
    assert!(due(db, NOW).await.unwrap().is_empty());
    let mut statement = due_query(NOW).build(DbBackend::Postgres);
    statement.sql.insert_str(0, "EXPLAIN (COSTS OFF) ");
    let plan = db.query_all_raw(statement).await.unwrap();
    let details = plan
        .iter()
        .map(|r| r.try_get::<String>("", "QUERY PLAN").unwrap())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        details.contains("Index Cond") && details.contains("idx_compaction_lifecycle_due"),
        "{details}"
    );
    assert!(
        !details.contains("compaction_operation") && !details.contains("Sort"),
        "{details}"
    );
    let triggers=db.query_all_raw(Statement::from_string(DbBackend::Postgres,"SELECT pg_get_functiondef(tgfoid) AS sql FROM pg_trigger WHERE NOT tgisinternal AND tgname LIKE 'compaction_lifecycle_%'".to_owned())).await.unwrap();
    for trigger in triggers {
        let sql = trigger.try_get::<String>("", "sql").unwrap();
        // Inline JSON may be compared as text for a replacement fence.
        // The trigger must neither parse it nor load portable payload bodies.
        for forbidden in [
            "json_extract",
            "jsonb_path_query",
            "->",
            "#>",
            "portable_payload_value",
            "portable_payload_part",
        ] {
            assert!(
                !sql.contains(forbidden),
                "tracking must not inspect payload bodies: {sql}"
            );
        }
    }
}
