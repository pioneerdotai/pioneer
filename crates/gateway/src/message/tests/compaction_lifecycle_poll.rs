//! Gateway orchestration regressions; compile only until review acceptance.
use super::*;
use pioneer_entity::{compaction_lifecycle_pending as pending, compaction_operation as operation};
use pioneer_sqlite::SqliteWriteClass;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, TransactionTrait};

async fn fixture() -> (
    tempfile::TempDir,
    Arc<MessageProcessor>,
    Arc<CrudStore>,
    Arc<NativeSchedulingObserver>,
) {
    let observer = Arc::new(NativeSchedulingObserver::default());
    let (directory, manager, store, workspace) =
        setup_pooled_file_workspace_manager_with_observer(Some(observer.clone())).await;
    let processor = Arc::new(MessageProcessor::new(
        Arc::new(ThreadManager::new("test-model", "openai")),
        test_provider(),
        Arc::new(SessionManager::new()),
        manager,
        store.clone(),
        test_gateway_secrets(),
        phase_13_summary_config(),
        test_tool_loop_config(),
    ));
    processor
        .agent_manager
        .set_context_controller(Some(Arc::new(
            crate::compaction::GatewayNativeContextController::new(Arc::downgrade(&processor)),
        )))
        .await;
    let db = store.with_maintenance_access().database_connection();
    db.execute_raw(sea_orm::Statement::from_sql_and_values(sea_orm::DbBackend::Sqlite,
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('poll-thread',?,'','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)", [workspace.clone().into()])).await.unwrap();
    db.execute_unprepared("INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES('poll-turn','poll-thread','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    db.execute_raw(sea_orm::Statement::from_sql_and_values(sea_orm::DbBackend::Sqlite,
        "INSERT INTO compaction_context(owner,workspace_id,thread_id,format_version) VALUES('poll-owner',?,'poll-thread',1)", [workspace.into()])).await.unwrap();
    for id in
        std::iter::once("a-poison".to_owned()).chain((0..11).map(|i| format!("b-healthy-{i:02}")))
    {
        db.execute_raw(sea_orm::Statement::from_sql_and_values(sea_orm::DbBackend::Sqlite,
            "INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,execution_turn,deadline_ms) VALUES (?,'poll-owner',?,'running','{}','poll-turn',0)", [id.clone().into(), id.into()])).await.unwrap();
    }
    (directory, processor, store, observer)
}

async fn failed_count(store: &CrudStore) -> u64 {
    use sea_orm::PaginatorTrait;
    operation::Entity::find()
        .filter(operation::Column::Status.eq("failed"))
        .count(&store.with_maintenance_access().database_connection())
        .await
        .unwrap()
}

#[tokio::test]
async fn lifecycle_claim_and_deferral_failure_keeps_batch_progress_history_poll_and_cooldown() {
    let (_directory, processor, store, observer) = fixture().await;
    let db = store.with_maintenance_access().database_connection();
    db.execute_unprepared("CREATE TRIGGER reject_poison_bookkeeping BEFORE UPDATE ON compaction_lifecycle_pending WHEN OLD.operation_id='a-poison' AND NEW.attempts>OLD.attempts BEGIN SELECT RAISE(ABORT,'fixture bookkeeping unavailable'); END").await.unwrap();
    // Complete expansion before selecting this batch so the first point
    // locator keeps its original generation/order. No seed rediscovery.
    db.execute_unprepared("DELETE FROM compaction_lifecycle_scope; UPDATE compaction_lifecycle_sequence SET seed_complete=1 WHERE singleton=1").await.unwrap();
    let before = pending::Entity::find_by_id("a-poison")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    // An independent history check must still be owned after lifecycle failure.
    db.execute_unprepared("INSERT INTO compaction_history_check(turn_id,state,descriptor,managed) VALUES('poll-turn','pending','{}',1)").await.unwrap();
    let started = std::time::Instant::now();
    let write_start = observer.writes.lock().unwrap().len();
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        processor.poll_completed_history_checks(),
    )
    .await
    .expect("lifecycle storage failure must not sleep five seconds in the common poll");
    assert!(result.is_err(), "storage failure must remain visible");
    assert!(started.elapsed() < Duration::from_secs(2));
    // Exactly eight pending inputs: poison plus seven healthy. No rediscovery.
    assert_eq!(failed_count(&store).await, 7);
    assert_eq!(
        store
            .compaction_operation("b-healthy-00")
            .await
            .unwrap()
            .unwrap()
            .status,
        "failed"
    );
    assert_eq!(
        store
            .compaction_operation("a-poison")
            .await
            .unwrap()
            .unwrap()
            .status,
        "running"
    );
    let poison = pending::Entity::find_by_id("a-poison")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(poison.attempts, before.attempts);
    assert_eq!(poison.retry_not_before, before.retry_not_before);
    assert!(poison.claim_token.is_none());
    assert!(
        processor
            .completed_history_checks
            .lock()
            .await
            .keys()
            .any(|(_, thread)| thread == "poll-thread")
    );
    // The common resilience loop's next action can proceed without the cooldown.
    let now = now_timestamp_secs();
    processor
        .crud_store
        .with_critical_writes()
        .heartbeat_turn_executions_owned_by(
            processor.turn_execution_owner_id.as_ref(),
            now,
            now + 30,
        )
        .await
        .unwrap();
    assert!(
        observer.writes.lock().unwrap()[write_start..]
            .iter()
            .any(|event| matches!(
                event,
                pioneer_sqlite::SqliteWriteEvent::Acquired {
                    class: SqliteWriteClass::Critical,
                    ..
                }
            ))
    );
    let scheduled = processor
        .compaction_lifecycle_not_before
        .read()
        .unwrap()
        .unwrap();
    assert!(scheduled > tokio::time::Instant::now());
    // Removing the fault does not bypass the next lifecycle poll's cooldown.
    db.execute_unprepared("DROP TRIGGER reject_poison_bookkeeping")
        .await
        .unwrap();
    processor.poll_completed_history_checks().await.unwrap();
    assert_eq!(failed_count(&store).await, 7);
    assert_eq!(
        pending::Entity::find_by_id("a-poison")
            .one(&db)
            .await
            .unwrap()
            .unwrap(),
        poison
    );
    // Move only this existing scheduling deadline to its expiry, without sleep.
    *processor.compaction_lifecycle_not_before.write().unwrap() = Some(tokio::time::Instant::now());
    processor.poll_completed_history_checks().await.unwrap();
    assert!(failed_count(&store).await > 7);
    processor.suspend_completed_history_checks().await;
    assert!(db.reader_query_only_enabled().await.unwrap());
}

#[tokio::test]
async fn lifecycle_scope_bookkeeping_failure_preserves_independent_pending_quota() {
    let (_directory, processor, store, _) = fixture().await;
    let db = store.with_maintenance_access().database_connection();
    db.execute_unprepared("CREATE TRIGGER reject_scope_bookkeeping BEFORE UPDATE ON compaction_lifecycle_scope WHEN NEW.attempts>OLD.attempts BEGIN SELECT RAISE(ABORT,'fixture scope unavailable'); END").await.unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        processor.poll_completed_history_checks(),
    )
    .await
    .unwrap();
    assert!(result.is_err());
    assert_eq!(failed_count(&store).await, 8);
    assert!(
        pioneer_entity::compaction_lifecycle_scope::Entity::find_by_id((
            "seed".to_owned(),
            String::new(),
            String::new()
        ))
        .one(&db)
        .await
        .unwrap()
        .is_some()
    );
    assert!(
        processor
            .compaction_lifecycle_not_before
            .read()
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn lifecycle_gateway_poll_cancellation_releases_maintenance_reservation() {
    let (_directory, processor, store, observer) = fixture().await;
    let db = store.with_maintenance_access().database_connection();
    let held = db.begin().await.unwrap();
    let start = observer.writes.lock().unwrap().len();
    let owner = processor.clone();
    let poll = tokio::spawn(async move { owner.poll_completed_history_checks().await });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if observer.writes.lock().unwrap()[start..]
                .iter()
                .any(|event| {
                    matches!(
                        event,
                        pioneer_sqlite::SqliteWriteEvent::Enqueued {
                            class: SqliteWriteClass::Maintenance,
                            ..
                        }
                    )
                })
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    poll.abort();
    assert!(poll.await.unwrap_err().is_cancelled());
    held.rollback().await.unwrap();
    assert_eq!(failed_count(&store).await, 0);
    assert!(
        pending::Entity::find_by_id("a-poison")
            .one(&db)
            .await
            .unwrap()
            .is_some()
    );
    assert!(observer.writes.lock().unwrap()[start..].iter().any(|event| matches!(event,
        pioneer_sqlite::SqliteWriteEvent::Cancelled { class: SqliteWriteClass::Maintenance, queue, .. } if queue.maintenance == 0)));
    processor.poll_completed_history_checks().await.unwrap();
    assert_eq!(failed_count(&store).await, 8);
    assert!(db.reader_query_only_enabled().await.unwrap());
}
