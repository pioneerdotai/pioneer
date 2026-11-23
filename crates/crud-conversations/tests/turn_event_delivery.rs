use anyhow::Result;
use pioneer_crud_conversations::{
    events::{AppendedTurnEvent, TurnEventPayload},
    projection_stream, turn_event,
    turn_event_delivery::{
        CONSUMER_LIVE_NOTIFICATION, CONSUMER_THREAD_EPISODIC, ClaimedTurnEventDelivery,
        DeliveryOutcome, DeliverySettlement, claim_prepared, discover, insert_pending_for_event,
        mark_delivered, mark_failed, prepare_claims, settle_batch,
    },
};
use pioneer_crud_payloads::storage::PreparedPayload;
use pioneer_postgres_test::with_database;
use pioneer_protocol::{ItemCompletedNotification, SystemEventLevel, TurnItem};
use sea_orm::{
    ConnectionTrait, EntityTrait, TransactionTrait, entity::prelude::DateTimeWithTimeZone,
};

fn at(second: i64) -> DateTimeWithTimeZone {
    chrono::DateTime::from_timestamp(second, 0)
        .unwrap()
        .fixed_offset()
}

async fn seed_thread(db: &impl ConnectionTrait) -> Result<()> {
    db.execute_unprepared(
        "INSERT INTO workspace(id,name) VALUES ('w','Workspace');
         INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status)
            VALUES ('thread','w','','agent','model','provider','active');",
    )
    .await?;
    Ok(())
}

async fn enqueue_items(db: &sea_orm::DatabaseConnection, turn_id: &str, count: i64) -> Result<()> {
    let tx = db.begin().await?;
    for sequence in 1..=count {
        let event_id = format!("{turn_id}-event-{sequence}");
        let payload = TurnEventPayload::ItemCompleted(ItemCompletedNotification {
            workspace_id: "w".into(),
            thread_id: "thread".into(),
            turn_id: turn_id.to_owned(),
            item: TurnItem::SystemEvent {
                id: format!("{turn_id}_item_{sequence}"),
                level: SystemEventLevel::Info,
                message: format!("committed {sequence}"),
                code: None,
                details: None,
            },
        });
        let body = PreparedPayload::new(&serde_json::to_string(&payload)?)?;
        let created_at = at(1_700_000_100 + sequence);
        turn_event::insert(
            &tx,
            turn_event::NewEvent {
                id: &event_id,
                thread_id: "thread",
                turn_id,
                sequence,
                event_type: pioneer_protocol::constants::events::ITEM_COMPLETED,
                idempotency_key: &event_id,
                created_at,
            },
            &body,
        )
        .await?;
        insert_pending_for_event(
            &tx,
            &AppendedTurnEvent {
                id: event_id,
                thread_id: "thread".into(),
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
        .await?;
    }
    projection_stream::ensure_healthy(&tx, "thread", turn_id, at(1_700_000_100)).await?;
    for sequence in 1..=count {
        projection_stream::advance_projected_through(
            &tx,
            turn_id,
            sequence - 1,
            sequence,
            at(1_700_000_100 + sequence),
        )
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

async fn claim(
    db: &impl ConnectionTrait,
    consumer: &str,
    now: i64,
    limit: u64,
) -> Result<Vec<ClaimedTurnEventDelivery>> {
    let due = discover(db, consumer, limit)
        .await?
        .into_iter()
        .filter(|(_, clock)| *clock <= at(now))
        .map(|(id, _)| id)
        .collect();
    claim_prepared(db, consumer, at(now), at(now + 120), &prepare_claims(due)).await
}

#[tokio::test]
async fn native_optional_delivery_reclaims_expired_heads_and_fences_stale_claims() -> Result<()> {
    with_database(async |fixture| {
        let db = fixture.connection();
        seed_thread(db).await?;
        db.execute_unprepared(
            "INSERT INTO turn(id,thread_id,status) VALUES ('target','thread','in_progress')",
        )
        .await?;
        enqueue_items(db, "target", 2).await?;

        let now = 1_700_000_110;
        let consumer = CONSUMER_LIVE_NOTIFICATION;
        let first = claim(db, consumer, now, 10).await?;
        assert_eq!(first.len(), 1);
        assert!(claim(db, consumer, now + 119, 10).await?.is_empty());

        let restored = fixture.connect().await?;
        let reclaimed = claim(&restored, consumer, now + 120, 10).await?;
        assert_eq!(reclaimed.len(), 1);
        assert_eq!(reclaimed[0].row.id, first[0].row.id);
        assert_ne!(reclaimed[0].claim_token, first[0].claim_token);
        assert_eq!(
            settle_batch(
                &restored,
                &[DeliverySettlement {
                    id: first[0].row.id.clone(),
                    claim_token: first[0].claim_token.clone(),
                    at_unix: now + 120,
                    outcome: DeliveryOutcome::Delivered,
                }]
            )
            .await?,
            0,
            "expired owner cannot complete a reclaimed head"
        );
        assert!(
            mark_delivered(
                &restored,
                &reclaimed[0].row.id,
                &reclaimed[0].claim_token,
                at(now + 120)
            )
            .await?
        );
        let next = claim(&restored, consumer, now + 120, 10).await?;
        assert_eq!(next.len(), 1);
        assert!(next[0].row.sequence > reclaimed[0].row.sequence);
        restored.close().await?;
        Ok(())
    })
    .await
}

#[tokio::test]
async fn native_optional_delivery_exhausted_head_unblocks_only_its_consumer() -> Result<()> {
    with_database(async |fixture| {
        let db = fixture.connection();
        seed_thread(db).await?;
        db.execute_unprepared(
            "INSERT INTO turn(id,thread_id,status) VALUES ('target','thread','in_progress')",
        )
        .await?;
        enqueue_items(db, "target", 2).await?;

        let mut now = 1_700_000_110;
        let mut head = None;
        for attempt in 0..10 {
            let claims = claim(db, CONSUMER_LIVE_NOTIFICATION, now, 2).await?;
            assert_eq!(claims.len(), 1);
            assert_eq!(claims[0].row.attempt_count, attempt);
            assert_eq!(
                *head.get_or_insert_with(|| claims[0].row.id.clone()),
                claims[0].row.id
            );
            assert_eq!(
                settle_batch(
                    db,
                    &[DeliverySettlement {
                        id: claims[0].row.id.clone(),
                        claim_token: claims[0].claim_token.clone(),
                        at_unix: now,
                        outcome: DeliveryOutcome::Failed {
                            attempt_count: claims[0].row.attempt_count,
                            error: "poison delivery".to_owned(),
                        },
                    }]
                )
                .await?,
                1
            );
            now += 2_i64.pow(attempt.min(8) as u32);
        }
        let exhausted = pioneer_entity::turn_event_delivery::Entity::find_by_id(head.unwrap())
            .one(db)
            .await?
            .unwrap();
        assert_eq!(exhausted.status, "exhausted");
        let live = claim(db, CONSUMER_LIVE_NOTIFICATION, now, 2).await?;
        let episodic = claim(db, CONSUMER_THREAD_EPISODIC, now, 2).await?;
        assert_eq!(live.len(), 1);
        assert_eq!(episodic.len(), 1);
        assert_eq!(live[0].row.sequence, exhausted.sequence + 1);
        assert_eq!(episodic[0].row.sequence, exhausted.sequence);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn native_optional_delivery_filters_blocked_turns_before_the_batch_limit() -> Result<()> {
    with_database(async |fixture| {
        let db = fixture.connection();
        seed_thread(db).await?;
        db.execute_unprepared(
            "INSERT INTO turn(id,thread_id,status) VALUES ('a','thread','in_progress'),
               ('b','thread','in_progress'), ('c','thread','in_progress')",
        )
        .await?;
        enqueue_items(db, "a", 4).await?;
        let healthy_turns = ["b", "c"];
        for turn in healthy_turns {
            enqueue_items(db, turn, 1).await?;
        }

        let now = 1_700_000_110;
        let consumers = [CONSUMER_LIVE_NOTIFICATION, CONSUMER_THREAD_EPISODIC];
        let mut heads = Vec::new();
        for consumer in consumers {
            let mut head = claim(db, consumer, now, 1).await?;
            assert_eq!(head.len(), 1);
            assert_eq!(head[0].row.turn_id, "a");
            heads.push(head.remove(0));
        }
        assert!(
            mark_failed(
                db,
                &heads[0].row.id,
                &heads[0].claim_token,
                "injected backoff".into(),
                at(now + 1),
                at(now),
                false
            )
            .await?
        );
        for consumer in consumers {
            let ready = claim(db, consumer, now, 2).await?;
            assert_eq!(ready.len(), 2, "blocked turn must not starve other turns");
            assert_eq!(
                ready
                    .iter()
                    .map(|claim| claim.row.turn_id.as_str())
                    .collect::<Vec<_>>(),
                healthy_turns,
            );
            let outcomes = ready
                .iter()
                .map(|claim| DeliverySettlement {
                    id: claim.row.id.clone(),
                    claim_token: claim.claim_token.clone(),
                    at_unix: now,
                    outcome: DeliveryOutcome::Delivered,
                })
                .collect::<Vec<_>>();
            assert_eq!(settle_batch(db, &outcomes).await?, 2);
            assert_eq!(
                settle_batch(db, &outcomes).await?,
                0,
                "replayed batch cannot settle a claim twice"
            );
            assert!(
                claim(db, consumer, now, 2).await?.is_empty(),
                "successors must wait for their own predecessor"
            );
        }

        let mut retry = claim(db, consumers[0], now + 1, 2).await?;
        assert_eq!(retry.len(), 1);
        assert_eq!(retry[0].row.id, heads[0].row.id);
        heads[0] = retry.remove(0);
        let restored = fixture.connect().await?;
        for head in heads {
            assert!(mark_delivered(&restored, &head.row.id, &head.claim_token, at(now + 1)).await?);
            for offset in 1..=3 {
                let next = claim(&restored, &head.row.consumer, now + 1, 2).await?;
                assert_eq!(next.len(), 1, "only one event per turn can be in flight");
                assert_eq!(next[0].row.turn_id, "a");
                assert_eq!(next[0].row.sequence, head.row.sequence + offset);
                assert!(
                    mark_delivered(
                        &restored,
                        &next[0].row.id,
                        &next[0].claim_token,
                        at(now + 1)
                    )
                    .await?
                );
            }
            assert!(
                claim(&restored, &head.row.consumer, now + 1, 2)
                    .await?
                    .is_empty()
            );
        }
        restored.close().await?;
        Ok(())
    })
    .await
}

#[tokio::test]
async fn corrupt_body_in_claim_batch_does_not_suppress_healthy_turn() -> Result<()> {
    with_database(async |fixture| {
        use pioneer_crud_conversations::delivery_service::{
            claim_due_turn_event_deliveries_with_deadline, settle_turn_event_deliveries,
        };
        use pioneer_entity::turn_event_delivery as delivery;
        use sea_orm::sea_query::Expr;
        use sea_orm::{ColumnTrait, QueryFilter};
        const BAD: &str = "a_corrupt_batch";
        const GOOD: &str = "z_healthy_batch";
        const NOW: i64 = 1_700_000_200;
        let db = fixture.connection();
        seed_thread(db).await?;
        db.execute_unprepared(
            "INSERT INTO turn (id,thread_id,status) VALUES
             ('a_corrupt_batch','thread','in_progress'),('z_healthy_batch','thread','in_progress')",
        )
        .await?;
        enqueue_items(db, BAD, 1).await?;
        enqueue_items(db, GOOD, 1).await?;
        let storage = pioneer_postgres_pools::PostgresDatabase::new(
            fixture.connect_read_only().await?,
            db.clone(),
        );
        let bad = delivery::Entity::find()
            .filter(delivery::Column::TurnId.eq(BAD))
            .filter(delivery::Column::Consumer.eq(CONSUMER_LIVE_NOTIFICATION))
            .one(db)
            .await
            .unwrap()
            .unwrap();
        pioneer_entity::turn_event::Entity::update_many()
            .col_expr(
                pioneer_entity::turn_event::Column::Payload,
                Expr::value(serde_json::json!({"invalid_canonical_event": true})),
            )
            .col_expr(
                pioneer_entity::turn_event::Column::PayloadPayloadId,
                Expr::value(Option::<String>::None),
            )
            .filter(pioneer_entity::turn_event::Column::Id.eq(bad.event_id))
            .exec(db)
            .await
            .unwrap();
        let claims = claim_due_turn_event_deliveries_with_deadline(
            &storage,
            CONSUMER_LIVE_NOTIFICATION,
            NOW,
            64,
        )
        .await?
        .deliveries;
        assert_eq!(claims.len(), 1);
        assert_eq!(claims[0].event.turn_id, GOOD);
        let bad = delivery::Entity::find_by_id(bad.id)
            .one(db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(bad.status, "failed");
        assert_eq!(bad.attempt_count, 1);
        assert!(bad.claim_token.is_none());
        assert_eq!(bad.next_run_at, at(NOW + 1));
        assert_eq!(
            settle_turn_event_deliveries(
                &storage,
                &[DeliverySettlement {
                    id: claims[0].id.clone(),
                    claim_token: claims[0].claim_token.clone(),
                    at_unix: NOW,
                    outcome: DeliveryOutcome::Delivered,
                }]
            )
            .await
            .unwrap(),
            1
        );
        Ok(())
    })
    .await
}
