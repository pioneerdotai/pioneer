use super::*;
use crate::message::agent_runtime::NativeTurnEventDeliveryWake;
use futures_util::FutureExt;
use pioneer_entity::turn_event_delivery::{self, Column, Entity};
use sea_orm::{ActiveModelTrait, ColumnTrait, EntityTrait, QueryFilter, QueryOrder, Set};

const LIVE: &str = pioneer_crud::NATIVE_TURN_EVENT_LIVE_CONSUMER;
const EPISODIC: &str = pioneer_crud::NATIVE_TURN_EVENT_EPISODIC_CONSUMER;
const THREAD: &str = "delivery-worker-thread";
const TURN: &str = "delivery-worker-turn";

async fn fixture() -> (MessageProcessor, Arc<CrudStore>, String) {
    let (workspaces, store, workspace) = setup_workspace_manager().await;
    let processor = MessageProcessor::with_agent_manager(
        Arc::new(ThreadManager::new("test-model", "openai")),
        Arc::new(AgentManager::new(test_provider(), test_tool_loop_config())),
        Arc::new(SessionManager::new()),
        workspaces,
        store.clone(),
    );
    processor
        .set_thread_episodic_ingestor_for_test(Arc::new(RecordingThreadEpisodicIngestor::default()))
        .await;
    materialize_thread_episodic_ingest_turn(&store, &workspace, THREAD, TURN).await;
    (processor, store, workspace)
}

async fn commit(store: &CrudStore, workspace: &str, item: &str) {
    store
        .materialize_native_agent_turn_event(
            pioneer_crud::CanonicalTurnEventPayload::ItemCompleted(ItemCompletedNotification {
                workspace_id: workspace.into(),
                thread_id: THREAD.into(),
                turn_id: TURN.into(),
                item: TurnItem::AgentMessage {
                    id: item.into(),
                    text: item.into(),
                    phase: pioneer_protocol::AgentMessagePhase::FinalAnswer,
                    markdown: None,
                    markdown_version: None,
                },
            }),
            now_timestamp_secs(),
            None,
        )
        .await
        .unwrap();
}

fn wake(processor: &MessageProcessor, consumer: &str) -> Arc<NativeTurnEventDeliveryWake> {
    match consumer {
        LIVE => processor.native_turn_event_delivery_workers.live.clone(),
        EPISODIC => processor
            .native_turn_event_delivery_workers
            .episodic
            .clone(),
        _ => unreachable!(),
    }
}

async fn reached(
    barrier: &super::super::compaction_background::CompletedHistoryPreparationBarrier,
) {
    timeout(Duration::from_secs(10), barrier.wait_until_reached())
        .await
        .expect("production delivery worker must reach the requested boundary");
}

async fn idle(processor: &MessageProcessor) {
    timeout(Duration::from_secs(10), async {
        while !processor
            .native_turn_event_delivery_workers
            .tasks
            .is_empty()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("delivery workers must drain without another kick or recovery tick");
    for consumer in [LIVE, EPISODIC] {
        assert!(!wake(processor, consumer).running.load(Ordering::SeqCst));
    }
}

async fn rows(store: &CrudStore, consumer: &str) -> Vec<turn_event_delivery::Model> {
    Entity::find()
        .filter(Column::TurnId.eq(TURN))
        .filter(Column::Consumer.eq(consumer))
        .order_by_asc(Column::Sequence)
        .all(&store.database_connection())
        .await
        .unwrap()
}

#[tokio::test]
async fn commit_after_empty_claim_retains_wake_for_each_consumer() {
    for consumer in [LIVE, EPISODIC] {
        let (processor, store, workspace) = fixture().await;
        let owner = wake(&processor, consumer);
        owner.claim_barrier.arm("empty");
        processor.kick_native_turn_event_deliveries();
        reached(&owner.claim_barrier).await;
        // The final empty claim has returned, but running still belongs to the
        // original worker. No periodic worker is started in this fixture.
        // Closing only the CLI manager is not delivery teardown.
        processor.shutdown_cli_runtime_manager().await;
        commit(&store, &workspace, "after-empty").await;
        let background = processor.scoped_for_background_reconciliation();
        for _ in 0..32 {
            background.kick_native_turn_event_deliveries();
        }
        owner.claim_barrier.release();
        idle(&processor).await;
        let delivered = rows(&store, consumer).await;
        assert_eq!(delivered.len(), 1);
        assert_eq!(delivered[0].status, "delivered");
        assert!(delivered[0].claim_token.is_none());
        processor.shutdown_resilience_workers().await;
    }
}

#[tokio::test]
async fn released_owner_neither_steals_replacement_wake_nor_resets_its_running_bit() {
    for consumer in [LIVE, EPISODIC] {
        let (processor, store, workspace) = fixture().await;
        let owner = wake(&processor, consumer);
        owner.handoff_barrier.arm("released");
        processor.kick_native_turn_event_deliveries();
        reached(&owner.handoff_barrier).await;
        // Owner A has disarmed its guard and published running=false. Owner B
        // acquires that same state and then stops after its own empty claim.
        owner.claim_barrier.arm("empty");
        processor.kick_native_turn_event_deliveries();
        reached(&owner.claim_barrier).await;
        commit(&store, &workspace, "replacement-wake").await;
        processor.kick_native_turn_event_deliveries();
        owner.handoff_barrier.release();
        // B stays behind a barrier. Wait for A's future (and old Drop guard)
        // and the other consumer to finish, rather than sleeping for a guess.
        timeout(Duration::from_secs(10), async {
            while processor.native_turn_event_delivery_workers.tasks.len() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            owner.running.load(Ordering::SeqCst),
            "A's Drop must not release B"
        );
        owner.claim_barrier.release();
        idle(&processor).await;
        let delivered = rows(&store, consumer).await;
        assert_eq!(delivered.len(), 1);
        assert_eq!(
            delivered[0].status, "delivered",
            "A must not consume B's pending wake"
        );
        processor.shutdown_resilience_workers().await;
    }
}

#[derive(Default)]
struct BlockingIngestor {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    calls: TokioMutex<Vec<String>>,
}

#[async_trait]
impl ThreadEpisodicIngestor for BlockingIngestor {
    async fn ingest_committed_item(
        &self,
        item: ThreadEpisodicCommittedItem,
    ) -> anyhow::Result<ThreadEpisodicIngestionOutcome> {
        let first = {
            let mut calls = self.calls.lock().await;
            calls.push(item.item_id);
            calls.len() == 1
        };
        if first {
            self.entered.notify_one();
            self.release.notified().await;
        }
        Ok(ThreadEpisodicIngestionOutcome::Accepted)
    }
}

#[tokio::test]
async fn blocked_episodic_effect_does_not_hold_next_live_batch() {
    let (processor, store, workspace) = fixture().await;
    let ingestor = Arc::new(BlockingIngestor::default());
    processor
        .set_thread_episodic_ingestor_for_test(ingestor.clone())
        .await;
    commit(&store, &workspace, "first").await;
    processor.kick_native_turn_event_deliveries();
    timeout(Duration::from_secs(10), ingestor.entered.notified())
        .await
        .unwrap();
    let live = wake(&processor, LIVE);
    // Finish the first LIVE batch before making a second batch runnable.
    timeout(Duration::from_secs(10), async {
        while live.running.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    live.claim_barrier.arm("empty");
    commit(&store, &workspace, "second").await;
    processor.kick_native_turn_event_deliveries();
    reached(&live.claim_barrier).await;
    let live_rows = rows(&store, LIVE).await;
    let episodic_rows = rows(&store, EPISODIC).await;
    assert_eq!(live_rows.len(), 2);
    assert!(live_rows.iter().all(|row| row.status == "delivered"));
    assert_eq!(
        episodic_rows
            .iter()
            .map(|row| row.status.as_str())
            .collect::<Vec<_>>(),
        ["delivering", "pending"]
    );
    assert_eq!(*ingestor.calls.lock().await, ["first"]);
    live.claim_barrier.release();
    ingestor.release.notify_one();
    idle(&processor).await;
    assert_eq!(*ingestor.calls.lock().await, ["first", "second"]);
    let episodic_rows = rows(&store, EPISODIC).await;
    assert!(episodic_rows.iter().all(|row| row.status == "delivered"));
    assert_eq!(
        live_rows
            .iter()
            .map(|row| &row.event_id)
            .collect::<Vec<_>>(),
        episodic_rows
            .iter()
            .map(|row| &row.event_id)
            .collect::<Vec<_>>()
    );
    processor.shutdown_resilience_workers().await;
}

#[tokio::test]
async fn exhausted_quantum_hands_causal_remainder_to_each_consumer_without_a_new_kick() {
    let (processor, store, workspace) = fixture().await;
    for index in 0..129 {
        commit(&store, &workspace, &format!("quantum-{index}")).await;
    }
    for consumer in [LIVE, EPISODIC] {
        wake(&processor, consumer).claim_barrier.arm("quantum");
    }
    processor.kick_native_turn_event_deliveries();
    for consumer in [LIVE, EPISODIC] {
        let owner = wake(&processor, consumer);
        reached(&owner.claim_barrier).await;
        let before = rows(&store, consumer).await;
        // Causal eligibility admits one head per turn/consumer per batch.
        assert_eq!(
            before
                .iter()
                .filter(|row| row.status == "delivered")
                .count(),
            128
        );
        assert_eq!(before.last().unwrap().status, "pending");
    }
    for consumer in [LIVE, EPISODIC] {
        wake(&processor, consumer).claim_barrier.release();
    }
    idle(&processor).await;
    for consumer in [LIVE, EPISODIC] {
        let delivered = rows(&store, consumer).await;
        assert_eq!(delivered.len(), 129);
        assert!(
            delivered
                .iter()
                .all(|row| row.status == "delivered" && row.attempt_count == 0)
        );
    }
    processor.shutdown_resilience_workers().await;
}

#[tokio::test]
async fn periodic_and_commit_kicks_share_consumer_owners() {
    let (processor, store, workspace) = fixture().await;
    let processor = Arc::new(processor);
    processor.arm_completed_history_preparation_barrier("__event_delivery_recovery_before_kick__");
    let periodic = tokio::spawn(MessageProcessor::run_projection_delivery_resilience_worker(
        Arc::downgrade(&processor),
    ));
    timeout(
        Duration::from_secs(10),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    for consumer in [LIVE, EPISODIC] {
        wake(&processor, consumer).claim_barrier.arm("empty");
    }
    processor.kick_native_turn_event_deliveries();
    for consumer in [LIVE, EPISODIC] {
        reached(&wake(&processor, consumer).claim_barrier).await;
    }
    commit(&store, &workspace, "periodic-and-commit").await;
    processor.kick_native_turn_event_deliveries();
    processor.release_completed_history_preparation_barrier();
    // Wait until the real periodic loop gets back to the same gate on its next
    // pass. Its first kick has then definitely coalesced with the commit kick.
    processor.arm_completed_history_preparation_barrier("__event_delivery_recovery_before_kick__");
    timeout(
        Duration::from_secs(10),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    assert_eq!(processor.native_turn_event_delivery_workers.tasks.len(), 2);
    for consumer in [LIVE, EPISODIC] {
        wake(&processor, consumer).claim_barrier.release();
    }
    idle(&processor).await;
    for consumer in [LIVE, EPISODIC] {
        assert_eq!(rows(&store, consumer).await[0].status, "delivered");
    }
    periodic.abort();
    let _ = periodic.await;
    processor.shutdown_resilience_workers().await;
}

#[tokio::test]
async fn coalesced_storage_failures_wait_and_shutdown_releases_both_owners() {
    let (processor, store, _) = fixture().await;
    store.database_connection().close().await.unwrap();
    for consumer in [LIVE, EPISODIC] {
        wake(&processor, consumer)
            .claim_barrier
            .arm("storage_failed");
    }
    processor.kick_native_turn_event_deliveries();
    for consumer in [LIVE, EPISODIC] {
        reached(&wake(&processor, consumer).claim_barrier).await;
    }
    tokio::time::pause();
    for _ in 0..32 {
        processor.kick_native_turn_event_deliveries();
    }
    for consumer in [LIVE, EPISODIC] {
        let owner = wake(&processor, consumer);
        owner.claim_barrier.arm("storage_failed");
        owner.claim_barrier.release();
    }
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(4)).await;
    for consumer in [LIVE, EPISODIC] {
        let owner = wake(&processor, consumer);
        assert!(
            owner
                .claim_barrier
                .wait_until_reached()
                .now_or_never()
                .is_none()
        );
        assert!(owner.running.load(Ordering::SeqCst));
    }
    assert_eq!(processor.native_turn_event_delivery_workers.tasks.len(), 2);
    tokio::time::advance(Duration::from_secs(1)).await;
    for consumer in [LIVE, EPISODIC] {
        reached(&wake(&processor, consumer).claim_barrier).await;
    }
    processor.shutdown_resilience_workers().await;
    idle(&processor).await;
    processor.kick_native_turn_event_deliveries();
    assert!(
        processor
            .native_turn_event_delivery_workers
            .tasks
            .is_empty()
    );
}

#[tokio::test]
async fn shutdown_retains_claim_and_restored_processor_recovers_with_a_new_token() {
    let (processor, store, workspace) = fixture().await;
    let ingestor = Arc::new(BlockingIngestor::default());
    processor
        .set_thread_episodic_ingestor_for_test(ingestor.clone())
        .await;
    commit(&store, &workspace, "restore-first").await;
    commit(&store, &workspace, "restore-second").await;
    processor.kick_native_turn_event_deliveries();
    timeout(Duration::from_secs(10), ingestor.entered.notified())
        .await
        .unwrap();
    let original = rows(&store, EPISODIC).await.remove(0);
    let stale_token = original.claim_token.clone().unwrap();
    processor.shutdown_resilience_workers().await;
    idle(&processor).await;
    let after_shutdown = rows(&store, EPISODIC).await;
    assert_eq!(after_shutdown[0].status, "delivering");
    assert_eq!(after_shutdown[1].status, "pending");
    // Emulate elapsed durable lease time, without sleeping or changing the
    // claim protocol. All access still goes through the fixture's CrudStore DB.
    // Shutdown can also interrupt LIVE after claim and before ACK. Advance
    // every retained lease so restored workers do not wait for real lease time.
    for consumer in [LIVE, EPISODIC] {
        for row in rows(&store, consumer).await {
            if row.status == "delivering" {
                let mut expired: turn_event_delivery::ActiveModel = row.into();
                expired.claim_expires_at = Set(Some(
                    chrono::Utc::now().fixed_offset() - chrono::Duration::seconds(1),
                ));
                expired.update(&store.database_connection()).await.unwrap();
            }
        }
    }
    let workspace_manager = processor.workspace_manager.clone();
    // A restart releases the closed physical delivery runtime before its
    // replacement is constructed; another facade must not revive a closed owner.
    drop(processor);
    let restored = MessageProcessor::with_agent_manager(
        Arc::new(ThreadManager::new("test-model", "openai")),
        Arc::new(AgentManager::new(test_provider(), test_tool_loop_config())),
        Arc::new(SessionManager::new()),
        workspace_manager,
        store.clone(),
    );
    let restored_ingestor = Arc::new(BlockingIngestor::default());
    restored
        .set_thread_episodic_ingestor_for_test(restored_ingestor.clone())
        .await;
    restored.kick_native_turn_event_deliveries();
    timeout(
        Duration::from_secs(10),
        restored_ingestor.entered.notified(),
    )
    .await
    .unwrap();
    let reclaimed = rows(&store, EPISODIC).await.remove(0);
    assert_eq!(reclaimed.id, original.id);
    assert_ne!(reclaimed.claim_token.as_deref(), Some(stale_token.as_str()));
    assert!(
        !store
            .complete_turn_event_delivery(&original.id, &stale_token, now_timestamp_secs())
            .await
            .unwrap()
    );
    assert_eq!(rows(&store, EPISODIC).await[0].status, "delivering");
    restored_ingestor.release.notify_one();
    idle(&restored).await;
    assert_eq!(
        *restored_ingestor.calls.lock().await,
        ["restore-first", "restore-second"]
    );
    assert!(
        rows(&store, EPISODIC)
            .await
            .iter()
            .all(|row| row.status == "delivered")
    );
    restored.shutdown_resilience_workers().await;
}

#[tokio::test]
async fn acknowledgement_and_deferral_storage_errors_leave_leases_for_recovery() {
    for (consumer, status) in [(LIVE, "delivered"), (EPISODIC, "failed")] {
        let (processor, store, workspace) = fixture().await;
        if consumer == EPISODIC {
            processor
                .set_thread_episodic_ingestor_for_test(Arc::new(RecordingThreadEpisodicIngestor {
                    fail: true,
                    ..Default::default()
                }))
                .await;
        }
        commit(&store, &workspace, "storage-write-failure").await;
        // Failure at the real acknowledgement/deferral boundary, rather than
        // a mock result from a copied drain algorithm.
        store.database_connection().execute_unprepared(&format!(
            "CREATE TRIGGER reject_delivery_status BEFORE UPDATE OF status ON turn_event_delivery WHEN NEW.consumer = '{consumer}' AND NEW.status = '{status}' BEGIN SELECT RAISE(ABORT, 'fixture delivery storage failure'); END"
        )).await.unwrap();
        let owner = wake(&processor, consumer);
        owner.claim_barrier.arm("storage_failed");
        processor.kick_native_turn_event_deliveries();
        reached(&owner.claim_barrier).await;
        let leased = rows(&store, consumer).await;
        assert_eq!(leased.len(), 1);
        assert_eq!(leased[0].status, "delivering");
        assert!(leased[0].claim_token.is_some());
        assert!(leased[0].delivered_at.is_none());
        assert_eq!(leased[0].attempt_count, 0);
        // Cancellation of the paused error path releases memory ownership,
        // while the outbox lease still owns the durable retry obligation.
        processor.shutdown_resilience_workers().await;
        idle(&processor).await;
        assert_eq!(rows(&store, consumer).await[0].status, "delivering");
    }
}

#[tokio::test]
async fn future_due_retry_does_not_spin_or_unblock_its_causal_successor() {
    let (processor, store, workspace) = fixture().await;
    commit(&store, &workspace, "retry-head").await;
    commit(&store, &workspace, "retry-successor").await;
    let head = store
        .claim_due_turn_event_deliveries(EPISODIC, now_timestamp_secs(), 64)
        .await
        .unwrap()
        .remove(0);
    // Use the actual retry API with a future wall-clock observation so the
    // deadline stays unambiguously in the future even on a slow test machine.
    assert!(
        store
            .fail_turn_event_delivery(
                &head.id,
                &head.claim_token,
                head.attempt_count,
                "fixture retry".into(),
                now_timestamp_secs() + 3600,
            )
            .await
            .unwrap()
    );
    let before = rows(&store, EPISODIC).await;
    let owner = wake(&processor, EPISODIC);
    owner.claim_barrier.arm("empty");
    processor.kick_native_turn_event_deliveries();
    reached(&owner.claim_barrier).await;
    for _ in 0..32 {
        processor.kick_native_turn_event_deliveries();
    }
    owner.claim_barrier.arm("timer");
    owner.claim_barrier.release();
    reached(&owner.claim_barrier).await;
    let after = rows(&store, EPISODIC).await;
    assert_eq!(after[0].status, "failed");
    assert_eq!(after[0].attempt_count, 1);
    assert_eq!(after[0].next_run_at, before[0].next_run_at);
    assert_eq!(after[1].status, "pending");
    // EPISODIC remains parked on its future timer. Its barrier does not
    // synchronize the independent LIVE owner, and the shared tracker cannot
    // become idle while that timer is retained.
    timeout(Duration::from_secs(10), async {
        while wake(&processor, LIVE).running.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("LIVE owner must finish independently of the EPISODIC timer");
    assert!(
        rows(&store, LIVE)
            .await
            .iter()
            .all(|row| row.status == "delivered")
    );
    processor.shutdown_resilience_workers().await;
}

#[tokio::test]
async fn eligible_retry_timer_drains_without_a_new_commit_or_kick() {
    let (processor, store, workspace) = fixture().await;
    commit(&store, &workspace, "timed-retry").await;
    let deadline = now_timestamp_secs() + 2;
    Entity::update_many()
        .col_expr(
            Column::NextRunAt,
            sea_orm::sea_query::Expr::value(
                chrono::DateTime::from_timestamp(deadline, 0)
                    .unwrap()
                    .fixed_offset(),
            ),
        )
        .filter(Column::TurnId.eq(TURN))
        .exec(&store.database_connection())
        .await
        .unwrap();
    processor.kick_native_turn_event_deliveries();
    idle(&processor).await;
    for consumer in [LIVE, EPISODIC] {
        assert!(
            rows(&store, consumer)
                .await
                .iter()
                .all(|row| row.status == "delivered")
        );
    }
}

#[tokio::test]
async fn deadline_ignores_past_successors_behind_future_causal_head() {
    let (_processor, store, workspace) = fixture().await;
    commit(&store, &workspace, "head").await;
    commit(&store, &workspace, "blocked-successor").await;
    let deadline = now_timestamp_secs() + 120;
    for consumer in [LIVE, EPISODIC] {
        let head = rows(&store, consumer).await.remove(0);
        Entity::update_many()
            .col_expr(
                Column::NextRunAt,
                sea_orm::sea_query::Expr::value(
                    chrono::DateTime::from_timestamp(deadline, 0)
                        .unwrap()
                        .fixed_offset(),
                ),
            )
            .filter(Column::Id.eq(head.id.clone()))
            .exec(&store.database_connection())
            .await
            .unwrap();
        assert_eq!(
            store
                .next_turn_event_delivery_deadline(consumer)
                .await
                .unwrap(),
            Some(deadline)
        );
        let waiting = store
            .claim_due_turn_event_deliveries_with_deadline(consumer, deadline - 1, 64)
            .await
            .unwrap();
        assert!(waiting.deliveries.is_empty());
        assert_eq!(waiting.next_eligible_at_unix, Some(deadline));
        let claimed = store
            .claim_due_turn_event_deliveries_with_deadline(consumer, deadline, 64)
            .await
            .unwrap();
        assert_eq!(claimed.deliveries.len(), 1);
        let old_token = claimed.deliveries[0].claim_token.clone();
        assert_eq!(
            store
                .next_turn_event_delivery_deadline(consumer)
                .await
                .unwrap(),
            Some(deadline + 120)
        );
        let reclaimed = store
            .claim_due_turn_event_deliveries_with_deadline(consumer, deadline + 120, 64)
            .await
            .unwrap();
        assert_eq!(reclaimed.deliveries.len(), 1);
        assert_ne!(reclaimed.deliveries[0].claim_token, old_token);
        assert!(
            !store
                .complete_turn_event_delivery(&head.id, &old_token, deadline + 121)
                .await
                .unwrap()
        );
        Entity::update_many()
            .col_expr(
                Column::ClaimExpiresAt,
                sea_orm::sea_query::Expr::value(
                    Option::<chrono::DateTime<chrono::FixedOffset>>::None,
                ),
            )
            .filter(Column::Id.eq(head.id))
            .exec(&store.database_connection())
            .await
            .unwrap();
        let no_lease = store
            .claim_due_turn_event_deliveries_with_deadline(consumer, deadline + 1000, 64)
            .await
            .unwrap();
        assert!(no_lease.deliveries.is_empty());
        assert_eq!(
            no_lease.next_eligible_at_unix, None,
            "NULL lease cannot fall back to past retry clock or unblock successor"
        );
    }
}

#[tokio::test]
async fn projection_deadline_tracks_healthy_watermark_successor_and_quarantine() {
    use pioneer_entity::{
        turn_event_projection_state as projection, turn_event_projection_stream_state as stream,
    };
    let (_processor, store, workspace) = fixture().await;
    commit(&store, &workspace, "projection-timer").await;
    let connection = store.database_connection();
    let head = projection::Entity::find()
        .filter(projection::Column::TurnId.eq(TURN))
        .order_by_desc(projection::Column::Sequence)
        .one(&connection)
        .await
        .unwrap()
        .unwrap();
    let deadline = now_timestamp_secs() + 120;
    projection::Entity::update_many()
        .col_expr(
            projection::Column::Status,
            sea_orm::sea_query::Expr::value("failed"),
        )
        .col_expr(
            projection::Column::NextRunAt,
            sea_orm::sea_query::Expr::value(
                chrono::DateTime::from_timestamp(deadline, 0)
                    .unwrap()
                    .fixed_offset(),
            ),
        )
        .filter(projection::Column::EventId.eq(head.event_id))
        .exec(&connection)
        .await
        .unwrap();
    // Fixture an unprojected immediate successor; earlier projected receipts
    // and any old next_run_at values must have no influence on this clock.
    stream::Entity::update_many()
        .col_expr(
            stream::Column::ProjectedThroughSequence,
            sea_orm::sea_query::Expr::value(head.sequence - 1),
        )
        .filter(stream::Column::TurnId.eq(TURN))
        .exec(&connection)
        .await
        .unwrap();
    assert_eq!(
        store.next_turn_event_projection_deadline().await.unwrap(),
        Some(deadline)
    );
    let replay = store
        .replay_due_turn_event_projections(deadline - 1, 64)
        .await
        .unwrap();
    assert_eq!(replay.claimed, 0);
    assert_eq!(replay.next_eligible_at_unix, Some(deadline));
    stream::Entity::update_many()
        .col_expr(
            stream::Column::Status,
            sea_orm::sea_query::Expr::value("quarantined"),
        )
        .filter(stream::Column::TurnId.eq(TURN))
        .exec(&connection)
        .await
        .unwrap();
    assert_eq!(
        store.next_turn_event_projection_deadline().await.unwrap(),
        None
    );
    assert_eq!(
        store
            .replay_due_turn_event_projections(deadline - 1, 64)
            .await
            .unwrap()
            .next_eligible_at_unix,
        None
    );
    stream::Entity::update_many()
        .col_expr(
            stream::Column::Status,
            sea_orm::sea_query::Expr::value("healthy"),
        )
        .filter(stream::Column::TurnId.eq(TURN))
        .exec(&connection)
        .await
        .unwrap();
    assert_eq!(
        store.next_turn_event_projection_deadline().await.unwrap(),
        Some(deadline)
    );
    let replay = store
        .replay_due_turn_event_projections(deadline - 1, 64)
        .await
        .unwrap();
    assert_eq!(replay.claimed, 0);
    assert_eq!(replay.next_eligible_at_unix, Some(deadline));
}

#[tokio::test]
async fn projection_recovery_deadline_storage_errors_remain_errors() {
    let (_processor, store, _) = fixture().await;
    store.database_connection().close().await.unwrap();
    assert!(
        store
            .replay_due_turn_event_projections(now_timestamp_secs(), 64)
            .await
            .is_err()
    );
    assert!(store.next_turn_event_delivery_deadline(LIVE).await.is_err());
}

#[tokio::test]
async fn empty_delivery_owners_and_projection_cycles_do_not_repeat_deadline_queries() {
    use pioneer_sqlite::{
        SqliteDatabase, SqliteReadClass, SqliteReadEvent, SqliteReadOutcome, SqliteWriteExecutor,
    };
    use sea_orm::ConnectOptions;
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("empty-delivery-owners.sqlite");
    let mut options = ConnectOptions::new(pioneer_sqlite::sqlite_connection_url(&path));
    options.max_connections(1).sqlx_logging(false);
    let writer = Database::connect(options).await.unwrap();
    let (_, _, workspace) = setup_workspace_manager_with_connection(writer.clone()).await;
    writer
        .execute_unprepared("PRAGMA journal_mode=WAL")
        .await
        .unwrap();
    let mut options = ConnectOptions::new(pioneer_sqlite::sqlite_read_only_connection_url(&path));
    options
        .max_connections(2)
        .map_sqlx_sqlite_opts(|opts| opts.pragma("query_only", "ON"));
    let reader = Database::connect(options).await.unwrap();
    let observer = Arc::new(NativeSchedulingObserver::default());
    let database = SqliteDatabase::from_executor_with_read_observer(
        reader,
        SqliteWriteExecutor::with_observer(writer, observer.clone()),
        observer.clone(),
    );
    let processor = Arc::new(MessageProcessor::with_agent_manager(
        Arc::new(ThreadManager::new("test-model", "openai")),
        Arc::new(AgentManager::new(test_provider(), test_tool_loop_config())),
        Arc::new(SessionManager::new()),
        Arc::new(WorkspaceManager::new(database.clone())),
        Arc::new(CrudStore::new(database)),
    ));
    materialize_thread_episodic_ingest_turn(&processor.crud_store, &workspace, THREAD, TURN).await;
    let read_count = || {
        observer
            .reads
            .lock()
            .unwrap()
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    SqliteReadEvent::OperationFinished {
                        class: SqliteReadClass::Maintenance,
                        outcome: SqliteReadOutcome::Ok,
                        ..
                    }
                )
            })
            .count()
    };
    observer.reads.lock().unwrap().clear();
    observer.writes.lock().unwrap().clear();
    // Exercise the actual independent owner futures twice, not a deadline DAO.
    for expected in [2, 4] {
        processor.kick_native_turn_event_deliveries();
        idle(&processor).await;
        assert_eq!(
            read_count(),
            expected,
            "one discovery per consumer; no empty MIN or extra drain"
        );
        assert!(observer.writes.lock().unwrap().is_empty());
    }
    observer.reads.lock().unwrap().clear();
    processor.arm_completed_history_preparation_barrier("__event_delivery_recovery_before_kick__");
    let worker = tokio::spawn(MessageProcessor::run_projection_delivery_resilience_worker(
        Arc::downgrade(&processor),
    ));
    reached(&processor.completed_history_preparation_barrier).await;
    processor.release_completed_history_preparation_barrier();
    timeout(Duration::from_secs(10), async {
        while read_count() < 4 {
            tokio::task::yield_now().await;
        }
        idle(&processor).await;
    })
    .await
    .unwrap();
    assert_eq!(
        read_count(),
        4,
        "one bootstrap discovery for each independent projection/finalization/delivery owner"
    );
    assert!(observer.writes.lock().unwrap().is_empty());
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(300)).await;
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        read_count(),
        4,
        "successful Empty has no periodic domain discovery"
    );
    assert!(observer.writes.lock().unwrap().is_empty());
    tokio::time::resume();

    // A separate supported CrudStore commits without a MessageProcessor kick.
    // Shared physical-runtime producer invalidation must reach both consumers,
    // even after their completed Empty and without a fallback poll.
    let ingestor = Arc::new(BlockingIngestor::default());
    processor
        .set_thread_episodic_ingestor_for_test(ingestor.clone())
        .await;
    let lower = CrudStore::new(processor.crud_store.database_connection());
    commit(&lower, &workspace, "committed-after-empty").await;
    timeout(Duration::from_secs(10), ingestor.entered.notified())
        .await
        .unwrap();
    timeout(Duration::from_secs(10), async {
        loop {
            let delivered = rows(&processor.crud_store, LIVE).await;
            if delivered.len() == 1 && delivered[0].status == "delivered" {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("foreign work must reach LIVE while its episodic sibling is blocked");
    assert_eq!(
        rows(&processor.crud_store, EPISODIC).await[0].status,
        "delivering"
    );
    ingestor.release.notify_one();
    idle(&processor).await;
    assert_eq!(
        rows(&processor.crud_store, EPISODIC).await[0].status,
        "delivered"
    );
    worker.abort();
    let _ = worker.await;
    processor.shutdown_resilience_workers().await;
}
