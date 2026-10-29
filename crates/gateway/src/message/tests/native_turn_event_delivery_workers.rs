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
    let mut expired: turn_event_delivery::ActiveModel = after_shutdown[0].clone().into();
    expired.claim_expires_at = Set(Some(
        chrono::Utc::now().fixed_offset() - chrono::Duration::seconds(1),
    ));
    expired.update(&store.database_connection()).await.unwrap();
    let restored = MessageProcessor::with_agent_manager(
        Arc::new(ThreadManager::new("test-model", "openai")),
        Arc::new(AgentManager::new(test_provider(), test_tool_loop_config())),
        Arc::new(SessionManager::new()),
        processor.workspace_manager.clone(),
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
    owner.claim_barrier.release();
    idle(&processor).await;
    let after = rows(&store, EPISODIC).await;
    assert_eq!(after[0].status, "failed");
    assert_eq!(after[0].attempt_count, 1);
    assert_eq!(after[0].next_run_at, before[0].next_run_at);
    assert_eq!(after[1].status, "pending");
    assert!(
        rows(&store, LIVE)
            .await
            .iter()
            .all(|row| row.status == "delivered")
    );
    processor.shutdown_resilience_workers().await;
}
