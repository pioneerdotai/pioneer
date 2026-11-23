use pioneer_crud_conversations::projection_receipts::{
    RECEIPT_CLEANUP_MAX_SOURCE_BYTES, cleanup_quantum,
};
use pioneer_crud_conversations::{event_projection, projection_stream};
use pioneer_entity::{
    turn_event_projection_state as receipt, turn_event_projection_stream_state as stream,
};
use pioneer_postgres_pools::PostgresDatabase;
use pioneer_postgres_test::{TestDatabase, with_database};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DbBackend, EntityTrait, PaginatorTrait, QueryFilter, Statement,
    TransactionTrait, sea_query::Expr,
};

async fn scoped_store(fixture: &TestDatabase) -> PostgresDatabase {
    let writer = fixture.connection();
    writer
        .execute_unprepared(
            "INSERT INTO workspace (id, name) VALUES ('ws', 'Projection receipts');
             INSERT INTO thread (id, workspace_id, preview, mode, model, model_provider, status)
               VALUES ('thread', 'ws', '', 'agent', 'm', 'p', 'active');
             INSERT INTO turn (id, thread_id, status) VALUES ('turn', 'thread', 'completed');",
        )
        .await
        .unwrap();
    PostgresDatabase::new(fixture.connect_read_only().await.unwrap(), writer.clone()).maintenance()
}

async fn seed(db: &PostgresDatabase, count: i64) {
    for sql in [
        "INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload)
         SELECT 'turn-'||n,'thread','turn',n,'test','{}'::json
         FROM generate_series(1::bigint,$1) AS sequences(n)",
        "INSERT INTO turn_event_projection_state(event_id,thread_id,turn_id,sequence,status,next_run_at)
         SELECT 'turn-'||n,'thread','turn',n,'projected',CURRENT_TIMESTAMP
         FROM generate_series(1::bigint,$1) AS sequences(n)",
        "INSERT INTO turn_event_projection_stream_state(turn_id,thread_id,status,projected_through_sequence)
         VALUES ('turn','thread','healthy',$1)",
    ] {
        db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres, sql, [count.into()]))
            .await
            .unwrap();
    }
}

async fn boundary(db: &PostgresDatabase) -> i64 {
    stream::Entity::find_by_id("turn")
        .one(db)
        .await
        .unwrap()
        .unwrap()
        .receipts_compacted_through_sequence
}

#[tokio::test]
async fn byte_budget_can_end_a_quantum_before_the_row_limit() {
    with_database(async |fixture| {
        let db = scoped_store(fixture).await;
        seed(&db, 3).await;
        receipt::Entity::update_many()
            .col_expr(
                receipt::Column::ProjectionContextJson,
                Expr::value(serde_json::to_string(&"x".repeat(128 * 1024 - 2))?),
            )
            .exec(&db)
            .await
            .unwrap();
        let first = cleanup_quantum(&db, None, None, None).await.unwrap();
        assert_eq!(first.rows_deleted, 1);
        assert!(first.source_bytes <= RECEIPT_CLEANUP_MAX_SOURCE_BYTES as u64);
        assert_eq!(boundary(&db).await, 1);
        assert_eq!(
            cleanup_quantum(&db, None, None, None)
                .await
                .unwrap()
                .rows_deleted,
            1
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn requires_maintenance_reads_and_writes() {
    with_database(async |fixture| {
        let db = scoped_store(fixture).await;
        seed(&db, 2).await;
        let wrong_scope = db.with_critical_writes();
        assert!(
            cleanup_quantum(&wrong_scope, None, None, None)
                .await
                .is_err()
        );
        assert_eq!(receipt::Entity::find().count(&db).await.unwrap(), 2);
        assert_eq!(
            cleanup_quantum(&db, None, None, None)
                .await
                .unwrap()
                .rows_deleted,
            2
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn projection_replay_claims_only_the_immediate_watermark_successor() {
    pioneer_postgres_test::with_database(async |fixture| {
        let db = scoped_store(fixture).await;
        seed(&db, 1).await;
        let thread_id = "thread";
        let turn_id = "turn";
        let timestamp = 1_700_000_000;
        let time = |seconds| {
            chrono::DateTime::from_timestamp_secs(seconds)
                .unwrap()
                .fixed_offset()
        };

        receipt::Entity::update_many()
            .col_expr(
                receipt::Column::Status,
                Expr::value(event_projection::PROJECTION_STATUS_FAILED),
            )
            .filter(receipt::Column::TurnId.eq(turn_id))
            .filter(receipt::Column::Sequence.eq(1))
            .exec(&db)
            .await
            .expect("retained predecessor receipt should become stale");

        for (event_id, sequence, claim_token) in [
            ("evt_wm_claim_2", 2, "claim_wm_2"),
            ("evt_wm_claim_3", 3, "claim_wm_3"),
        ] {
            let seed = db.begin().await?;
            event_projection::insert_claimed(
                &seed,
                event_projection::NewTurnEventProjectionState {
                    event_id: event_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    sequence,
                    projection_context_json: "{}".to_owned(),
                    claim_token: claim_token.to_owned(),
                    claim_expires_at: time(timestamp + 60),
                    created_at: time(timestamp + sequence),
                },
            )
            .await
            .expect("projection receipt fixture should insert");
            assert!(
                event_projection::release_claim_as_pending(
                    &seed,
                    event_id,
                    claim_token,
                    time(timestamp + 1),
                    time(timestamp + 1),
                )
                .await
                .expect("projection receipt fixture should become pending")
            );
            seed.commit().await?;
        }

        assert_eq!(
            event_projection::next_deadline(&db)
                .await?
                .map(|deadline| deadline.timestamp()),
            Some(timestamp + 1),
            "a stale receipt behind the watermark must not affect the next deadline",
        );
        let first_claim =
            event_projection::claim_due(&db, time(timestamp + 10), time(timestamp + 70), 10)
                .await
                .expect("first watermark successor should claim");
        assert_eq!(first_claim.len(), 1);
        assert_eq!(first_claim[0].state.sequence, 2);
        assert_eq!(
            event_projection::next_deadline(&db)
                .await?
                .map(|deadline| deadline.timestamp()),
            Some(timestamp + 70),
            "the past pending successor must not supersede its predecessor's running lease",
        );

        let transaction = db
            .begin()
            .await
            .expect("projection transaction should begin");
        assert!(
            event_projection::mark_claim_projected(
                &transaction,
                first_claim[0].state.event_id.as_str(),
                turn_id,
                2,
                first_claim[0].claim_token.as_str(),
                time(timestamp + 11),
            )
            .await
            .expect("first watermark successor should complete")
        );
        assert!(
            projection_stream::advance_projected_through(
                &transaction,
                turn_id,
                1,
                2,
                time(timestamp + 11)
            )
            .await?
            .is_some(),
            "the receipt and its watermark must be published in the same transaction",
        );
        transaction
            .commit()
            .await
            .expect("first watermark successor should commit");

        let second_claim =
            event_projection::claim_due(&db, time(timestamp + 12), time(timestamp + 72), 10)
                .await
                .expect("second watermark successor should claim");
        assert_eq!(second_claim.len(), 1);
        assert_eq!(second_claim[0].state.sequence, 3);
        assert_eq!(
            receipt::Entity::find()
                .filter(receipt::Column::TurnId.eq(turn_id),)
                .count(&db)
                .await
                .expect("retained projection receipts should count"),
            3
        );
        Ok(())
    })
    .await
    .unwrap();
}
