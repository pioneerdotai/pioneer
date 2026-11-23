use super::*;
use futures_util::FutureExt;
use pioneer_postgres_pools::{
    PostgresDatabase, PostgresReadEvent, PostgresReadObserver, PostgresWriteEvent,
    PostgresWriteExecutor, PostgresWriteObserver, WorkSubscription,
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

// Observe notification ordering at the first real reader admission after the
// finalization writer releases. No database operations or waits in callbacks.
#[derive(Default)]
struct FinalizationWakeObserver {
    armed: AtomicBool,
    after_commit: AtomicBool,
    subscriptions: Mutex<Vec<WorkSubscription>>,
    observed: Mutex<Option<Vec<bool>>>,
}
impl PostgresWriteObserver for FinalizationWakeObserver {
    fn observe(&self, event: PostgresWriteEvent) {
        if matches!(event, PostgresWriteEvent::Released { .. })
            && self.armed.swap(false, Ordering::SeqCst)
        {
            self.after_commit.store(true, Ordering::SeqCst);
        }
    }
}
impl PostgresReadObserver for FinalizationWakeObserver {
    fn observe(&self, event: PostgresReadEvent) {
        if matches!(event, PostgresReadEvent::AdmissionEnqueued { .. })
            && self.after_commit.swap(false, Ordering::SeqCst)
        {
            let states = self
                .subscriptions
                .lock()
                .unwrap()
                .iter_mut()
                .map(|subscription| subscription.changed().now_or_never().is_some())
                .collect();
            *self.observed.lock().unwrap() = Some(states);
        }
    }
}

#[pioneer_postgres_test::postgres_test]
async fn committed_finalization_publishes_wakes_before_any_post_commit_reader_wait() {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let writer = fixture.connect_with_limit(1).await.unwrap();
    let reader = fixture.connect_read_only().await.unwrap();
    let observer = Arc::new(FinalizationWakeObserver::default());
    let database = PostgresDatabase::from_executor_with_read_observer(
        reader,
        PostgresWriteExecutor::with_observer(writer, observer.clone()),
        observer.clone(),
    );
    let store = CrudStore::new(database.clone()).with_maintenance_access();
    let at = 1_700_000_100;
    pioneer_entity::workspace::Entity::insert(pioneer_entity::workspace::ActiveModel {
        id: Set("ws_finalization_wakes".into()),
        name: Set("Finalization wake fixture".into()),
        is_active: Set(true),
        is_current: Set(false),
        created_at: Set(unix_to_datetime(at)),
        updated_at: Set(unix_to_datetime(at)),
    })
    .exec(&database)
    .await
    .unwrap();
    let (store, thread, turn) = start_test_turn(
        store,
        "ws_finalization_wakes",
        "thread_finalization_wakes",
        "turn_finalization_wakes",
    )
    .await;
    store
        .prepare_turn_finalization(
            &ItemCompletedNotification {
                workspace_id: thread.workspace_id.clone(),
                thread_id: thread.id.clone(),
                turn_id: turn.id.clone(),
                item: TurnItem::AgentMessage {
                    id: "final_wake_answer".into(),
                    text: "final answer".into(),
                    phase: AgentMessagePhase::FinalAnswer,
                    markdown: None,
                    markdown_version: None,
                },
            },
            1,
            None,
            at,
        )
        .await
        .unwrap();

    // These consumers all require the committed terminal state. The first
    // post-commit reader belongs to further lifecycle work, not to publishing
    // these notifications. A slow reader must not hide an already durable turn.
    let mut subscriptions = Vec::new();
    for domain in [
        "gateway.control",
        "gateway.cli",
        "projection",
        "maintenance.receipts",
        "maintenance.zstd",
    ] {
        let mut subscription = database.work_signal(domain).subscribe();
        subscription.begin_pass(&database).await.unwrap();
        subscriptions.push(subscription);
    }
    *observer.subscriptions.lock().unwrap() = subscriptions;
    observer.armed.store(true, Ordering::SeqCst);
    let outcome = store
        .commit_prepared_turn_finalization(&turn.id, at + 1)
        .await
        .unwrap();
    assert!(matches!(
        outcome,
        TurnFinalizationCommitOutcome::Committed(_)
    ));
    assert_eq!(
        observer.observed.lock().unwrap().as_deref(),
        Some([true; 5].as_slice()),
        "all committed notifications must precede the first post-commit reader admission"
    );

    let mut subscriptions = std::mem::take(&mut *observer.subscriptions.lock().unwrap());
    for subscription in &mut subscriptions {
        subscription.begin_pass(&database).await.unwrap();
    }
    let replay = store
        .commit_prepared_turn_finalization(&turn.id, at + 2)
        .await
        .unwrap();
    assert!(
        matches!(replay, TurnFinalizationCommitOutcome::Committed(committed) if committed.already_committed)
    );
    assert!(
        subscriptions
            .iter_mut()
            .all(|subscription| subscription.changed().now_or_never().is_none()),
        "an unchanged finalization ACK must not schedule another control or cleanup pass"
    );
}
