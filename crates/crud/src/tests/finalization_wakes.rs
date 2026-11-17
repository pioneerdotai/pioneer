use super::*;
use futures_util::FutureExt;
use pioneer_sqlite::{
    SqliteDatabase, SqliteReadEvent, SqliteReadObserver, SqliteWriteEvent, SqliteWriteExecutor,
    SqliteWriteObserver, WorkSubscription,
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
impl SqliteWriteObserver for FinalizationWakeObserver {
    fn observe(&self, event: SqliteWriteEvent) {
        if matches!(event, SqliteWriteEvent::Released { .. })
            && self.armed.swap(false, Ordering::SeqCst)
        {
            self.after_commit.store(true, Ordering::SeqCst);
        }
    }
}
impl SqliteReadObserver for FinalizationWakeObserver {
    fn observe(&self, event: SqliteReadEvent) {
        if matches!(event, SqliteReadEvent::AdmissionEnqueued { .. })
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

#[tokio::test]
async fn committed_finalization_publishes_wakes_before_any_post_commit_reader_wait() {
    use sea_orm::{ConnectOptions, Set};
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("finalization-wakes.sqlite");
    let mut options = ConnectOptions::new(pioneer_sqlite::sqlite_connection_url(&path));
    options.max_connections(1).sqlx_logging(false);
    let writer = Database::connect(options).await.unwrap();
    Migrator::up(&writer, None).await.unwrap();
    writer
        .execute_unprepared("PRAGMA journal_mode=WAL")
        .await
        .unwrap();
    let mut options = ConnectOptions::new(pioneer_sqlite::sqlite_read_only_connection_url(&path));
    options
        .max_connections(1)
        .map_sqlx_sqlite_opts(|options| options.pragma("query_only", "ON"));
    let reader = Database::connect(options).await.unwrap();
    let observer = Arc::new(FinalizationWakeObserver::default());
    let database = SqliteDatabase::from_executor_with_read_observer(
        reader,
        SqliteWriteExecutor::with_observer(writer, observer.clone()),
        observer.clone(),
    );
    let store = CrudStore::new(database.clone()).with_maintenance_access();
    let at = 1_700_000_100;
    pioneer_entity::workspace::Entity::insert(pioneer_entity::workspace::ActiveModel {
        id: Set("ws_finalization_wakes".into()),
        name: Set("Finalization wake fixture".into()),
        is_active: Set(true),
        is_current: Set(true),
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
