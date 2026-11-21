use super::*;
use crate::events::TurnEventPayload;
use crate::turn_event::{NewEvent, insert};
use crate::turn_event_delivery as deliveries;
use pioneer_crud_payloads::storage::PreparedPayload;
use pioneer_entity::{
    turn_event_delivery as delivery, turn_event_projection_stream_state as stream,
};
use pioneer_protocol::{ItemCompletedNotification, SystemEventLevel, TurnItem};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DbBackend, DbErr, EntityTrait, ExecResult, IntoActiveModel,
    QueryFilter, QueryResult, Statement, TransactionTrait, sea_query::Expr,
};
use std::sync::Mutex;

const NOW: i64 = 1_700_000_110;
const EPISODIC: &str = deliveries::CONSUMER_THREAD_EPISODIC;
const LIVE: &str = deliveries::CONSUMER_LIVE_NOTIFICATION;
const PREDECESSOR_INDEX: &str = "idx_turn_event_delivery_unfinished_predecessor";

async fn delivery_connection(workspace: &str) -> SqliteDatabase {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let writer = fixture.connection();
    writer.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES ($1,'Delivery fixture',TRUE,FALSE)",
        [workspace.into()],
    )).await.unwrap();
    SqliteDatabase::new(fixture.connect_read_only().await.unwrap(), writer.clone())
}

async fn delivery_fixture(
    workspace: &str,
    thread_id: &str,
    turn_id: &str,
    count: i64,
) -> (SqliteDatabase, String) {
    let db = delivery_connection(workspace).await;
    let tx = db.begin().await.unwrap();
    for statement in [
        Statement::from_sql_and_values(DbBackend::Postgres,
            "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class)
             VALUES ($1,$2,'','agent','model','provider','active','user','workspace')",
            [thread_id.into(), workspace.into()]),
        Statement::from_sql_and_values(DbBackend::Postgres,
            "INSERT INTO turn(id,thread_id,status,turn_kind,origin) VALUES ($1,$2,'in_progress','conversation','user')",
            [turn_id.into(), thread_id.into()]),
        Statement::from_sql_and_values(DbBackend::Postgres,
            "INSERT INTO turn_event_projection_stream_state(turn_id,thread_id,status,projected_through_sequence)
            VALUES ($1,$2,'healthy',$3)",
            [turn_id.into(), thread_id.into(), (count + 1).into()]),
    ] {
        tx.execute_raw(statement).await.unwrap();
    }
    for index in 1..=count {
        let payload = TurnEventPayload::ItemCompleted(ItemCompletedNotification {
            workspace_id: workspace.to_owned(),
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
            item: TurnItem::SystemEvent {
                id: format!("{turn_id}_item_{index}"),
                level: SystemEventLevel::Info,
                message: format!("committed {index}"),
                code: None,
                details: None,
            },
        });
        let id = format!("{turn_id}_event_{index}");
        let created_at = unix_to_datetime(1_700_000_100 + index);
        let sequence = index + 1;
        let body = PreparedPayload::new(&serde_json::to_string(&payload).unwrap()).unwrap();
        insert(
            &tx,
            NewEvent {
                id: &id,
                thread_id,
                turn_id,
                sequence,
                event_type: payload.event_type(),
                idempotency_key: &id,
                created_at,
            },
            &body,
        )
        .await
        .unwrap();
        deliveries::insert_pending_for_event(
            &tx,
            &AppendedTurnEvent {
                id,
                thread_id: thread_id.to_owned(),
                turn_id: turn_id.to_owned(),
                sequence,
                payload,
                idempotency_key: None,
                was_inserted: true,
                created_at,
            },
            created_at,
            true,
        )
        .await
        .unwrap();
    }
    tx.commit().await.unwrap();
    (db, turn_id.to_owned())
}

async fn claim_deliveries(
    db: &SqliteDatabase,
    consumer: &str,
    now: i64,
    limit: u64,
) -> Result<Vec<ClaimedTurnEventDeliveryRecord>> {
    Ok(
        claim_due_turn_event_deliveries_with_deadline(db, consumer, now, limit)
            .await?
            .deliveries,
    )
}

async fn claim_due(
    db: &impl sea_orm::ConnectionTrait,
    consumer: &str,
    now: sea_orm::entity::prelude::DateTimeWithTimeZone,
    claim_expires_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
    limit: u64,
) -> anyhow::Result<Vec<deliveries::ClaimedTurnEventDelivery>> {
    let candidates = deliveries::discover(db, consumer, limit)
        .await?
        .into_iter()
        .filter(|(_, eligible_at)| *eligible_at <= now)
        .map(|(id, _)| id)
        .collect();
    deliveries::claim_prepared(
        db,
        consumer,
        now,
        claim_expires_at,
        &deliveries::prepare_claims(candidates),
    )
    .await
}

#[pioneer_postgres_test::postgres_test]
async fn episodic_delivery_requires_matching_stream_scope_and_positive_sequence() {
    let (db, turn_id) = delivery_fixture("ws_scope", "thr_scope", "turn_scope", 1).await;
    // Retained projected receipts must not override a missing/wrong stream.
    for mutation in [
        "UPDATE turn_event_projection_stream_state SET turn_id = 'other_turn' WHERE turn_id = $1",
        "UPDATE turn_event_projection_stream_state SET thread_id = 'other_thread' WHERE turn_id = $1",
        "DELETE FROM turn_event_projection_stream_state WHERE turn_id = $1",
    ] {
        let saved = stream::Entity::find_by_id(turn_id.clone())
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            mutation,
            [turn_id.clone().into()],
        ))
        .await
        .unwrap();
        assert!(
            claim_deliveries(&db, EPISODIC, NOW, 1)
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
        claim_deliveries(&db, EPISODIC, NOW, 1)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(claim_deliveries(&db, LIVE, NOW, 1).await.unwrap().len(), 1);
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
        if statement.sql.starts_with("UPDATE ") {
            let mutation = self.before_update.lock().unwrap().take();
            if let Some(mutation) = mutation {
                self.db.execute_raw(mutation).await?;
            }
        }
        self.db.query_all_raw(statement).await
    }
}

#[pioneer_postgres_test::postgres_test]
async fn episodic_guarded_claim_rechecks_watermark_scope_and_unknown_predecessor() {
    for mutation in [
        "UPDATE turn_event_projection_stream_state SET projected_through_sequence = 2 WHERE turn_id = $1",
        "UPDATE turn_event_projection_stream_state SET thread_id = 'other_thread' WHERE turn_id = $1",
        "DELETE FROM turn_event_projection_stream_state WHERE turn_id = $1",
        "UPDATE turn_event_delivery SET status = 'future_nonterminal' WHERE turn_id = $1 AND consumer = 'thread_episodic' AND sequence = 2",
    ] {
        let (db, turn_id) = delivery_fixture("ws_guard", "thr_guard", "turn_guard", 2).await;
        let first = claim_deliveries(&db, EPISODIC, NOW, 1).await.unwrap();
        assert!(
            complete_turn_event_delivery(&db, &first[0].id, &first[0].claim_token, NOW)
                .await
                .unwrap()
        );
        let connection = ClaimConnection::new(
            db.clone(),
            Some(Statement::from_sql_and_values(
                DbBackend::Postgres,
                mutation,
                [turn_id.clone().into()],
            )),
        );
        let claims = claim_due(
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
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(unclaimed.status, deliveries::DELIVERY_STATUS_PENDING);
        assert!(unclaimed.claim_token.is_none());
        assert!(unclaimed.claim_expires_at.is_none());
        assert!(
            claim_deliveries(&db, EPISODIC, NOW, 1)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(claim_deliveries(&db, LIVE, NOW, 1).await.unwrap().len(), 1);
    }
}

#[pioneer_postgres_test::postgres_test]
async fn episodic_claim_production_sql_uses_unfinished_index_with_large_terminal_history() {
    let db = delivery_connection("ws_plan").await.maintenance();
    db.execute_unprepared(
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status)
         VALUES ('thread','ws_plan','','agent','model','provider','active');
         INSERT INTO turn(id,thread_id,status) VALUES ('turn_a','thread','completed'), ('turn_b','thread','completed');",
    ).await.unwrap();
    // Bounded test fixture only: both consumers, mixed terminal statuses,
    // 20k completed events followed by three active events in the same Turn.
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
        "WITH history(n) AS (SELECT generate_series(1::bigint,$1)) \
         INSERT INTO turn_event(id, thread_id, turn_id, sequence, event_type, payload, created_at) \
         SELECT 'history-' || n, 'thread', 'turn_a', n, 'test', '{}', CURRENT_TIMESTAMP FROM history",
        [20_003_i64.into()])).await.unwrap();
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
        "INSERT INTO turn_event_delivery(id, event_id, thread_id, turn_id, sequence, consumer, status, next_run_at, created_at, updated_at) \
         SELECT event.id || '-' || consumers.consumer, event.id, event.thread_id, event.turn_id, event.sequence, consumers.consumer, \
             CASE WHEN event.sequence > $1 THEN 'pending' WHEN event.sequence % 2 = 0 THEN 'delivered' ELSE 'exhausted' END, $2, $3, $4 \
         FROM turn_event AS event CROSS JOIN (SELECT 'thread_episodic' AS consumer UNION ALL SELECT 'live_notification') AS consumers",
        [20_000_i64.into(), unix_to_datetime(NOW).into(), unix_to_datetime(NOW).into(), unix_to_datetime(NOW).into()])).await.unwrap();
    db.execute_unprepared(
        "INSERT INTO turn_event(id, thread_id, turn_id, sequence, event_type, payload, created_at) VALUES ('independent', 'thread', 'turn_b', 1, 'test', '{}', CURRENT_TIMESTAMP)"
    ).await.unwrap();
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
        "INSERT INTO turn_event_delivery(id, event_id, thread_id, turn_id, sequence, consumer, status, next_run_at, created_at, updated_at) \
         VALUES ('independent-delivery', 'independent', 'thread', 'turn_b', 1, $1, 'pending', $2, $3, $4)",
        [EPISODIC.into(), unix_to_datetime(NOW).into(), unix_to_datetime(NOW).into(), unix_to_datetime(NOW).into()])).await.unwrap();
    db.execute_unprepared(
        "INSERT INTO turn_event_projection_stream_state(turn_id, thread_id, status, projected_through_sequence, created_at, updated_at) \
         VALUES ('turn_a', 'thread', 'healthy', 20003, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP), ('turn_b', 'thread', 'healthy', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
    ).await.unwrap();
    let connection = ClaimConnection::new(db.clone(), None);
    let claims = claim_due(
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
        // Preserve SQL and bound values; only prepend EXPLAIN (COSTS OFF).
        statement.sql = format!("EXPLAIN (COSTS OFF) {}", statement.sql);
        let plan = db
            .query_all_raw(statement)
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.try_get::<String>("", "QUERY PLAN").unwrap())
            .collect::<Vec<_>>();
        assert!(
            plan.iter().any(|detail| detail.contains(PREDECESSOR_INDEX))
                && plan.iter().any(|detail| {
                    detail.contains("Index Cond:")
                        && detail.contains("turn_id")
                        && detail.contains("sequence")
                        && detail.contains('<')
                }),
            "predecessor must use the consumer/Turn active range: {plan:#?}"
        );
        assert!(
            !plan
                .iter()
                .any(|detail| detail.contains("Seq Scan") && detail.contains("predecessor")),
            "must not scan terminal history: {plan:#?}"
        );
    }
    assert_eq!(
        (selects, updates),
        (1, 1),
        "one discovery and one guarded batch claim; both use the causal index"
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
    let next = claim_due(
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
    let last = claim_due(
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
    let live = claim_due(
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
