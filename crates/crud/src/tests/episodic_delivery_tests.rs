use super::*;
use pioneer_crud_conversations::turn_event_delivery as deliveries;
use pioneer_entity::{
    turn_event_delivery as delivery, turn_event_projection_state as receipt,
    turn_event_projection_stream_state as stream,
};
use pioneer_sqlite::SqliteDatabase;

const NOW: i64 = 1_700_000_110;
const EPISODIC: &str = deliveries::CONSUMER_THREAD_EPISODIC;
const LIVE: &str = deliveries::CONSUMER_LIVE_NOTIFICATION;

async fn open_store(fixture: &pioneer_postgres_test::TestDatabase) -> CrudStore {
    let writer = fixture.connect_with_limit(1).await.unwrap();
    let reader = fixture.connect_read_only().await.unwrap();
    let database = SqliteDatabase::new(reader, writer);
    database.validate_reader().await.unwrap();
    CrudStore::new(database).with_maintenance_access()
}

#[pioneer_postgres_test::postgres_test]
async fn episodic_pending_delivery_survives_real_receipt_cleanup_and_reopen() {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let store = open_store(&fixture).await;
    pioneer_entity::workspace::Entity::insert(pioneer_entity::workspace::ActiveModel {
        id: Set("ws_cleanup_delivery".into()),
        name: Set("Cleanup delivery".into()),
        is_active: Set(true),
        is_current: Set(false),
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
    let mut completed = turn.clone();
    completed.status = TurnStatus::Completed;
    store
        .materialize_turn_completed(
            pioneer_protocol::TurnCompletedNotification {
                workspace_id: thread.workspace_id.clone(),
                thread_id: thread.id.clone(),
                turn: completed,
            },
            NOW,
        )
        .await
        .unwrap();
    // Use the actual cleanup quantum, including its atomic durable boundary.
    let cleanup = store
        .cleanup_projection_receipts_quantum(None, None)
        .await
        .unwrap();
    assert_eq!(cleanup.rows_deleted, 5); // Start, three items, terminal event.
    let state = stream::Entity::find_by_id(turn.id.clone())
        .one(&store.connection)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(state.projected_through_sequence, 5);
    assert_eq!(state.receipts_compacted_through_sequence, 5);
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
        5
    );
    store.database_connection().close().await.unwrap();
    drop(store);

    let reopened = open_store(&fixture).await;
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
async fn episodic_quarantined_prefix_remains_ready_and_unprojected_turn_does_not_starve_others() {
    pioneer_postgres_test::with_database(async |fixture| {
        let (store, thread, turn) =
            postgres_store_with_started_turn(fixture, "ws_prefix", "thr_prefix", "turn_a_prefix")
                .await;
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
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn mixed_delivery_batch_preserves_individual_retry_deferral_and_ack_state() {
    pioneer_postgres_test::with_database(async |fixture| {
        let (store, thread, turn) =
            postgres_store_with_started_turn(fixture, "ws_batch", "thr_batch", "batch_a").await;
        let turns = ["batch_a", "batch_b", "batch_c", "batch_d"];
        for (index, id) in turns.iter().enumerate() {
            if index != 0 {
                let mut next = turn.clone();
                next.id = (*id).into();
                store
                    .materialize_turn_start(
                        &thread,
                        SandboxMode::FullAccess,
                        &next,
                        &[],
                        pioneer_protocol::PersistedActorRef::System,
                    )
                    .await
                    .unwrap();
            }
            append_optional_delivery_test_items(&store, &thread, id, 2).await;
        }
        // Existing retry history is real persisted state, read back into each claim.
        for (id, count, error) in [
            ("batch_b", 2, "earlier error"),
            ("batch_d", 9, "last retry"),
        ] {
            delivery::Entity::update_many()
                .col_expr(delivery::Column::AttemptCount, Expr::value(count))
                .col_expr(delivery::Column::LastError, Expr::value(error))
                .filter(delivery::Column::TurnId.eq(id))
                .filter(delivery::Column::Consumer.eq(LIVE))
                .exec(&store.connection)
                .await
                .unwrap();
        }
        let claims = store
            .with_maintenance_access()
            .claim_due_turn_event_deliveries(LIVE, NOW, 64)
            .await
            .unwrap();
        assert_eq!(
            claims
                .iter()
                .map(|c| c.event.turn_id.as_str())
                .collect::<Vec<_>>(),
            turns
        );
        let outcomes = claims
            .iter()
            .enumerate()
            .map(|(i, c)| crate::TurnEventDeliverySettlement {
                id: c.id.clone(),
                claim_token: c.claim_token.clone(),
                at_unix: NOW,
                outcome: match i {
                    0 => crate::TurnEventDeliveryOutcome::Delivered,
                    1 => crate::TurnEventDeliveryOutcome::ForegroundDeferred,
                    _ => crate::TurnEventDeliveryOutcome::Failed {
                        attempt_count: c.attempt_count,
                        error: format!("failure-{i}"),
                    },
                },
            })
            .collect::<Vec<_>>();
        assert_eq!(
            store
                .with_maintenance_access()
                .settle_turn_event_deliveries(&outcomes)
                .await
                .unwrap(),
            4
        );
        assert_eq!(
            store.settle_turn_event_deliveries(&outcomes).await.unwrap(),
            0
        );
        for (i, claim) in claims.iter().enumerate() {
            let row = delivery::Entity::find_by_id(claim.id.clone())
                .one(&store.connection)
                .await
                .unwrap()
                .unwrap();
            assert!(row.claim_token.is_none() && row.claim_expires_at.is_none());
            match i {
                0 => {
                    assert_eq!(row.status, "delivered");
                    assert_eq!(row.delivered_at, Some(unix_to_datetime(NOW)));
                    assert_eq!(row.attempt_count, 0);
                }
                1 => {
                    assert_eq!(row.status, "pending");
                    assert_eq!(row.next_run_at, unix_to_datetime(NOW + 1));
                    assert_eq!(row.attempt_count, 2);
                    assert_eq!(row.last_error.as_deref(), Some("earlier error"));
                }
                2 => {
                    assert_eq!(row.status, "failed");
                    assert_eq!(row.next_run_at, unix_to_datetime(NOW + 1));
                    assert_eq!(row.attempt_count, 1);
                    assert_eq!(row.last_error.as_deref(), Some("failure-2"));
                }
                _ => {
                    assert_eq!(row.status, "exhausted");
                    assert_eq!(row.attempt_count, 10);
                    assert_eq!(row.last_error.as_deref(), Some("failure-3"));
                }
            }
        }
        let next = store
            .claim_due_turn_event_deliveries(LIVE, NOW, 64)
            .await
            .unwrap();
        assert_eq!(
            next.iter()
                .map(|c| c.event.turn_id.as_str())
                .collect::<Vec<_>>(),
            ["batch_a", "batch_d"],
            "only delivered/exhausted heads unblock their own successors"
        );
        Ok(())
    })
    .await
    .unwrap();
}
