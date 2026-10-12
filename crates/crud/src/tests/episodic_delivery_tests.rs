use super::*;
use crate::repositories::turn_event_delivery as deliveries;
use pioneer_entity::{
    turn_event_delivery as delivery, turn_event_projection_state as receipt,
    turn_event_projection_stream_state as stream,
};
use pioneer_sqlite::{SqliteDatabase, sqlite_read_only_connection_url};
use sea_orm::{ConnectOptions, DbBackend, DbErr, ExecResult, IntoActiveModel, QueryResult};
use std::sync::Mutex;

const NOW: i64 = 1_700_000_110;
const EPISODIC: &str = deliveries::CONSUMER_THREAD_EPISODIC;
const LIVE: &str = deliveries::CONSUMER_LIVE_NOTIFICATION;
const PREDECESSOR_INDEX: &str = "idx_turn_event_delivery_unfinished_predecessor";

async fn open_file_store(path: &std::path::Path, migrate: bool) -> CrudStore {
    let mut options = ConnectOptions::new(format!("sqlite://{}?mode=rwc", path.display()));
    options.max_connections(1);
    let writer = Database::connect(options).await.unwrap();
    if migrate {
        Migrator::up(&writer, None).await.unwrap();
    }
    writer
        .execute_unprepared("PRAGMA journal_mode=WAL")
        .await
        .unwrap();
    let mut options = ConnectOptions::new(sqlite_read_only_connection_url(path));
    options.max_connections(2);
    options.map_sqlx_sqlite_opts(|options| {
        options
            .read_only(true)
            .create_if_missing(false)
            .pragma("query_only", "ON")
    });
    let reader = Database::connect(options).await.unwrap();
    let database = SqliteDatabase::new(reader, writer);
    database.validate_reader().await.unwrap();
    CrudStore::new(database).with_maintenance_access()
}

#[tokio::test]
async fn episodic_pending_delivery_survives_real_receipt_cleanup_and_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("delivery.sqlite");
    let store = open_file_store(&path, true).await;
    pioneer_entity::workspace::Entity::insert(pioneer_entity::workspace::ActiveModel {
        id: Set("ws_cleanup_delivery".into()),
        name: Set("Cleanup delivery".into()),
        is_active: Set(true),
        is_current: Set(true),
        created_at: Set(unix_to_datetime(NOW)),
        updated_at: Set(unix_to_datetime(NOW)),
    })
    .exec(&store.connection)
    .await
    .unwrap();
    let (store, thread, turn) = start_test_turn(
        store,
        "ws_cleanup_delivery",
        "thr_cleanup_delivery",
        "turn_cleanup_delivery",
    )
    .await;
    append_optional_delivery_test_items(&store, &thread, &turn.id, 3).await;
    let pending = delivery::Entity::find()
        .filter(delivery::Column::Consumer.eq(EPISODIC))
        .order_by_asc(delivery::Column::Sequence)
        .all(&store.connection)
        .await
        .unwrap();
    assert_eq!(pending.len(), 3);
    assert!(
        pending
            .iter()
            .all(|row| row.status == deliveries::DELIVERY_STATUS_PENDING && row.attempt_count == 0)
    );
    crate::upsert_projection_meta(
        &store.connection,
        crate::ProjectionMetaRecord {
            projection_key: "turn_event_projection_stream_state_backfill".into(),
            projection_version: 3,
            status: crate::PROJECTION_META_STATUS_COMPLETE.into(),
            source_thread_count: 0,
            source_turn_count: 0,
            source_turn_item_count: 0,
            source_turn_event_count: 0,
            last_error: None,
            backfill_started_at: Some(unix_to_datetime(NOW)),
            backfilled_at: Some(unix_to_datetime(NOW)),
            created_at: unix_to_datetime(NOW),
            updated_at: unix_to_datetime(NOW),
        },
    )
    .await
    .unwrap();
    // Use the actual cleanup quantum, including its atomic durable boundary.
    let cleanup = store
        .cleanup_projection_receipts_quantum(None)
        .await
        .unwrap();
    assert_eq!(cleanup.rows_deleted, 4); // Turn start and three completed items.
    let state = stream::Entity::find_by_id(turn.id.clone())
        .one(&store.connection)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(state.projected_through_sequence, 4);
    assert_eq!(state.receipts_compacted_through_sequence, 4);
    assert_eq!(
        receipt::Entity::find()
            .count(&store.connection)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        delivery::Entity::find()
            .filter(delivery::Column::Consumer.eq(EPISODIC))
            .order_by_asc(delivery::Column::Sequence)
            .all(&store.connection)
            .await
            .unwrap(),
        pending
    );
    assert_eq!(
        pioneer_entity::turn_event::Entity::find()
            .count(&store.connection)
            .await
            .unwrap(),
        4
    );
    store.database_connection().close().await.unwrap();
    drop(store);

    let reopened = open_file_store(&path, false).await;
    for expected in pending {
        assert!(
            receipt::Entity::find_by_id(expected.event_id.clone())
                .one(&reopened.connection)
                .await
                .unwrap()
                .is_none()
        );
        let claims = reopened
            .claim_due_turn_event_deliveries(EPISODIC, NOW, 10)
            .await
            .unwrap();
        assert_eq!(claims.len(), 1, "only the causal head can be leased");
        assert_eq!(claims[0].id, expected.id);
        assert_eq!(claims[0].event.id, expected.event_id);
        assert_eq!(claims[0].event.sequence, expected.sequence);
        assert_eq!(claims[0].attempt_count, 0);
        assert!(
            reopened
                .complete_turn_event_delivery(&claims[0].id, &claims[0].claim_token, NOW)
                .await
                .unwrap()
        );
    }
    assert!(
        reopened
            .claim_due_turn_event_deliveries(EPISODIC, NOW, 10)
            .await
            .unwrap()
            .is_empty()
    );
    reopened.database_connection().close().await.unwrap();
}

#[tokio::test]
async fn episodic_delivery_requires_matching_stream_scope_and_positive_sequence() {
    let (store, thread, turn) =
        test_store_with_started_turn("ws_scope", "thr_scope", "turn_scope").await;
    append_optional_delivery_test_items(&store, &thread, &turn.id, 1).await;
    let db = store.database_connection();
    // Retained projected receipts must not override a missing/wrong stream.
    for mutation in [
        "UPDATE turn_event_projection_stream_state SET turn_id = 'other_turn' WHERE turn_id = ?",
        "UPDATE turn_event_projection_stream_state SET thread_id = 'other_thread' WHERE turn_id = ?",
        "DELETE FROM turn_event_projection_stream_state WHERE turn_id = ?",
    ] {
        let saved = stream::Entity::find_by_id(turn.id.clone())
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            mutation,
            [turn.id.clone().into()],
        ))
        .await
        .unwrap();
        assert!(
            store
                .claim_due_turn_event_deliveries(EPISODIC, NOW, 1)
                .await
                .unwrap()
                .is_empty()
        );
        stream::Entity::delete_many().exec(&db).await.unwrap();
        stream::Entity::insert(saved.into_active_model())
            .exec(&db)
            .await
            .unwrap();
    }
    delivery::Entity::update_many()
        .col_expr(delivery::Column::Sequence, Expr::value(0_i64))
        .filter(delivery::Column::Consumer.eq(EPISODIC))
        .exec(&db)
        .await
        .unwrap();
    assert!(
        store
            .claim_due_turn_event_deliveries(EPISODIC, NOW, 1)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store
            .claim_due_turn_event_deliveries(LIVE, NOW, 1)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn episodic_quarantined_prefix_remains_ready_and_unprojected_turn_does_not_starve_others() {
    let (store, thread, turn) =
        test_store_with_started_turn("ws_prefix", "thr_prefix", "turn_a_prefix").await;
    append_optional_delivery_test_items(&store, &thread, &turn.id, 2).await;
    let db = store.database_connection();
    stream::Entity::update_many()
        .filter(stream::Column::TurnId.eq(turn.id.clone()))
        .col_expr(stream::Column::Status, Expr::value("quarantined"))
        .col_expr(stream::Column::ProjectedThroughSequence, Expr::value(2_i64))
        .exec(&db)
        .await
        .unwrap();
    let prefix = store
        .claim_due_turn_event_deliveries(EPISODIC, NOW, 1)
        .await
        .unwrap();
    assert_eq!(prefix.len(), 1);
    assert_eq!(prefix[0].event.sequence, 2);
    assert!(
        store
            .complete_turn_event_delivery(&prefix[0].id, &prefix[0].claim_token, NOW)
            .await
            .unwrap()
    );
    // Sequence 3 still has a projected receipt, but exceeds the authority.
    assert!(
        store
            .claim_due_turn_event_deliveries(EPISODIC, NOW, 1)
            .await
            .unwrap()
            .is_empty()
    );
    let mut independent = turn.clone();
    independent.id = "turn_b_prefix".into();
    store
        .materialize_turn_start(
            &thread,
            SandboxMode::FullAccess,
            &independent,
            &[],
            pioneer_protocol::PersistedActorRef::System,
        )
        .await
        .unwrap();
    append_optional_delivery_test_items(&store, &thread, &independent.id, 1).await;
    let ready = store
        .claim_due_turn_event_deliveries(EPISODIC, NOW, 1)
        .await
        .unwrap();
    assert_eq!(
        ready.len(),
        1,
        "projection eligibility must be applied before LIMIT"
    );
    assert_eq!(ready[0].event.turn_id, independent.id);
    assert!(
        store
            .complete_turn_event_delivery(&ready[0].id, &ready[0].claim_token, NOW)
            .await
            .unwrap()
    );

    // Restore the replay fixture to the real unprojected crash window.
    stream::Entity::update_many()
        .filter(stream::Column::TurnId.eq(turn.id.clone()))
        .col_expr(stream::Column::Status, Expr::value("healthy"))
        .exec(&db)
        .await
        .unwrap();
    receipt::Entity::update_many()
        .filter(receipt::Column::TurnId.eq(turn.id.clone()))
        .filter(receipt::Column::Sequence.eq(3_i64))
        .col_expr(receipt::Column::Status, Expr::value("pending"))
        .exec(&db)
        .await
        .unwrap();
    assert_eq!(
        store
            .replay_due_turn_event_projections(NOW, 10)
            .await
            .unwrap()
            .projected,
        1
    );
    let projected = store
        .claim_due_turn_event_deliveries(EPISODIC, NOW, 1)
        .await
        .unwrap();
    assert_eq!(projected.len(), 1);
    assert_eq!(projected[0].event.turn_id, turn.id);
    assert_eq!(projected[0].event.sequence, 3);
}

// Test-only interception captures the exact statements issued by claim_due.
// All actual I/O still goes through the scoped SqliteDatabase and its routes.
struct ClaimConnection {
    db: SqliteDatabase,
    statements: Mutex<Vec<Statement>>,
    before_update: Mutex<Option<Statement>>,
}

impl ClaimConnection {
    fn new(db: SqliteDatabase, before_update: Option<Statement>) -> Self {
        Self {
            db,
            statements: Mutex::new(Vec::new()),
            before_update: Mutex::new(before_update),
        }
    }
}

#[sea_orm::prelude::async_trait::async_trait]
impl ConnectionTrait for ClaimConnection {
    fn get_database_backend(&self) -> DbBackend {
        self.db.get_database_backend()
    }

    async fn execute_raw(&self, statement: Statement) -> Result<ExecResult, DbErr> {
        self.statements.lock().unwrap().push(statement.clone());
        let mutation = self.before_update.lock().unwrap().take();
        if let Some(mutation) = mutation {
            self.db.execute_raw(mutation).await?;
        }
        self.db.execute_raw(statement).await
    }

    async fn execute_unprepared(&self, sql: &str) -> Result<ExecResult, DbErr> {
        self.db.execute_unprepared(sql).await
    }

    async fn query_one_raw(&self, statement: Statement) -> Result<Option<QueryResult>, DbErr> {
        self.statements.lock().unwrap().push(statement.clone());
        self.db.query_one_raw(statement).await
    }

    async fn query_all_raw(&self, statement: Statement) -> Result<Vec<QueryResult>, DbErr> {
        self.statements.lock().unwrap().push(statement.clone());
        self.db.query_all_raw(statement).await
    }
}

#[tokio::test]
async fn episodic_guarded_claim_rechecks_watermark_scope_and_unknown_predecessor() {
    for mutation in [
        "UPDATE turn_event_projection_stream_state SET projected_through_sequence = 2 WHERE turn_id = ?",
        "UPDATE turn_event_projection_stream_state SET thread_id = 'other_thread' WHERE turn_id = ?",
        "DELETE FROM turn_event_projection_stream_state WHERE turn_id = ?",
        "UPDATE turn_event_delivery SET status = 'future_nonterminal' WHERE turn_id = ? AND consumer = 'thread_episodic' AND sequence = 2",
    ] {
        let (store, thread, turn) =
            test_store_with_started_turn("ws_guard", "thr_guard", "turn_guard").await;
        append_optional_delivery_test_items(&store, &thread, &turn.id, 2).await;
        let first = store
            .claim_due_turn_event_deliveries(EPISODIC, NOW, 1)
            .await
            .unwrap();
        assert!(
            store
                .complete_turn_event_delivery(&first[0].id, &first[0].claim_token, NOW)
                .await
                .unwrap()
        );
        let connection = ClaimConnection::new(
            store.database_connection(),
            Some(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                mutation,
                [turn.id.clone().into()],
            )),
        );
        let claims = deliveries::claim_due(
            &connection,
            EPISODIC,
            unix_to_datetime(NOW),
            unix_to_datetime(NOW + 120),
            1,
        )
        .await
        .unwrap();
        assert!(
            claims.is_empty(),
            "discovery must not replace the atomic UPDATE guard"
        );
        assert!(
            connection
                .statements
                .lock()
                .unwrap()
                .iter()
                .any(|statement| statement.sql.starts_with("UPDATE "))
        );
        let unclaimed = delivery::Entity::find()
            .filter(delivery::Column::Consumer.eq(EPISODIC))
            .filter(delivery::Column::Sequence.eq(3_i64))
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(unclaimed.status, deliveries::DELIVERY_STATUS_PENDING);
        assert!(unclaimed.claim_token.is_none());
        assert!(unclaimed.claim_expires_at.is_none());
        assert!(
            store
                .claim_due_turn_event_deliveries(EPISODIC, NOW, 1)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            store
                .claim_due_turn_event_deliveries(LIVE, NOW, 1)
                .await
                .unwrap()
                .len(),
            1
        );
    }
}

#[tokio::test]
async fn episodic_claim_production_sql_uses_unfinished_index_with_large_terminal_history() {
    let store = test_store_with_workspace("ws_plan")
        .await
        .with_maintenance_access();
    let db = store.database_connection();
    // Bounded test fixture only: both consumers, mixed terminal statuses,
    // 20k completed events followed by three active events in the same Turn.
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "WITH RECURSIVE history(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM history WHERE n < ?) \
         INSERT INTO turn_event(id, thread_id, turn_id, sequence, event_type, payload, created_at) \
         SELECT 'history-' || n, 'thread', 'turn_a', n, 'test', '{}', CURRENT_TIMESTAMP FROM history",
        [20_003_i64.into()])).await.unwrap();
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "INSERT INTO turn_event_delivery(id, event_id, thread_id, turn_id, sequence, consumer, status, next_run_at, created_at, updated_at) \
         SELECT event.id || '-' || consumers.consumer, event.id, event.thread_id, event.turn_id, event.sequence, consumers.consumer, \
             CASE WHEN event.sequence > ? THEN 'pending' WHEN event.sequence % 2 = 0 THEN 'delivered' ELSE 'exhausted' END, ?, ?, ? \
         FROM turn_event AS event CROSS JOIN (SELECT 'thread_episodic' AS consumer UNION ALL SELECT 'live_notification') AS consumers",
        [20_000_i64.into(), unix_to_datetime(NOW).into(), unix_to_datetime(NOW).into(), unix_to_datetime(NOW).into()])).await.unwrap();
    db.execute_unprepared(
        "INSERT INTO turn_event(id, thread_id, turn_id, sequence, event_type, payload, created_at) VALUES ('independent', 'thread', 'turn_b', 1, 'test', '{}', CURRENT_TIMESTAMP)"
    ).await.unwrap();
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "INSERT INTO turn_event_delivery(id, event_id, thread_id, turn_id, sequence, consumer, status, next_run_at, created_at, updated_at) \
         VALUES ('independent-delivery', 'independent', 'thread', 'turn_b', 1, ?, 'pending', ?, ?, ?)",
        [EPISODIC.into(), unix_to_datetime(NOW).into(), unix_to_datetime(NOW).into(), unix_to_datetime(NOW).into()])).await.unwrap();
    db.execute_unprepared(
        "INSERT INTO turn_event_projection_stream_state(turn_id, thread_id, status, projected_through_sequence, created_at, updated_at) \
         VALUES ('turn_a', 'thread', 'healthy', 20003, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP), ('turn_b', 'thread', 'healthy', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
    ).await.unwrap();
    let connection = ClaimConnection::new(db.clone(), None);
    let claims = deliveries::claim_due(
        &connection,
        EPISODIC,
        unix_to_datetime(NOW),
        unix_to_datetime(NOW + 120),
        2,
    )
    .await
    .unwrap();
    assert_eq!(
        claims
            .iter()
            .map(|claim| (claim.row.turn_id.as_str(), claim.row.sequence))
            .collect::<Vec<_>>(),
        vec![("turn_a", 20_001), ("turn_b", 1)]
    );
    let statements = connection.statements.lock().unwrap().clone();
    let mut selects = 0;
    let mut updates = 0;
    for mut statement in statements
        .into_iter()
        .filter(|statement| statement.sql.contains("predecessor"))
    {
        if statement.sql.starts_with("SELECT ") {
            selects += 1;
        } else if statement.sql.starts_with("UPDATE ") {
            updates += 1;
        } else {
            panic!("unexpected claim statement: {}", statement.sql);
        }
        // Preserve SQL and bound values; only prepend EXPLAIN QUERY PLAN.
        statement.sql = format!("EXPLAIN QUERY PLAN {}", statement.sql);
        let plan = db
            .query_all_raw(statement)
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.try_get::<String>("", "detail").unwrap())
            .collect::<Vec<_>>();
        assert!(
            plan.iter()
                .any(|detail| detail.starts_with("SEARCH predecessor ")
                    && detail.contains(PREDECESSOR_INDEX)
                    && detail.contains("consumer=? AND turn_id=? AND sequence<?")),
            "predecessor must use the consumer/Turn active range: {plan:#?}"
        );
        assert!(
            !plan
                .iter()
                .any(|detail| detail.starts_with("SCAN predecessor")),
            "must not scan terminal history: {plan:#?}"
        );
    }
    assert_eq!(
        (selects, updates),
        (1, 2),
        "cover discovery and every guarded claim"
    );
    for claim in claims {
        assert!(
            deliveries::mark_delivered(
                &db,
                &claim.row.id,
                &claim.claim_token,
                unix_to_datetime(NOW)
            )
            .await
            .unwrap()
        );
    }
    let next = deliveries::claim_due(
        &db,
        EPISODIC,
        unix_to_datetime(NOW),
        unix_to_datetime(NOW + 120),
        2,
    )
    .await
    .unwrap();
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].row.sequence, 20_002);
    assert!(
        deliveries::mark_failed(
            &db,
            &next[0].row.id,
            &next[0].claim_token,
            "exhausted fixture".into(),
            unix_to_datetime(NOW),
            unix_to_datetime(NOW),
            true
        )
        .await
        .unwrap()
    );
    let last = deliveries::claim_due(
        &db,
        EPISODIC,
        unix_to_datetime(NOW),
        unix_to_datetime(NOW + 120),
        2,
    )
    .await
    .unwrap();
    assert_eq!(last.len(), 1);
    assert_eq!(last[0].row.sequence, 20_003);
    let live = deliveries::claim_due(
        &db,
        LIVE,
        unix_to_datetime(NOW),
        unix_to_datetime(NOW + 120),
        2,
    )
    .await
    .unwrap();
    assert_eq!(live.len(), 1);
    assert_eq!(
        live[0].row.sequence, 20_001,
        "consumers retain separate causal heads"
    );
}
