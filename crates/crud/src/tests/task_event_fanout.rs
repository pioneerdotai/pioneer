//! Regression code for review; do not execute before orchestrator acceptance.
use super::*;
use crate::repositories::{task_event as events, task_event_fanout as queue};
use crate::{
    TASK_EVENT_FANOUT_BYTE_BUDGET as BYTES, TaskEventFanoutClaim, TaskEventFanoutOutcome,
    TaskEventFanoutPage,
};
use pioneer_entity::{
    task_event, task_event_fanout_cursor as cursor, task_event_fanout_pending as pending,
    task_event_fanout_sequence as sequence,
};
use sea_orm::TransactionTrait;
use sea_orm::sea_query::Expr;
const NOW: i64 = 4_000_000_000;
const MIGRATION: &str = "m20261004_000008_task_event_fanout_pending";
async fn store() -> CrudStore {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    Migrator::up(&db, None).await.unwrap();
    CrudStore::new(db)
}
async fn row(store: &CrudStore, id: &str) -> Option<pending::Model> {
    pending::Entity::find_by_id(id.to_owned())
        .one(&store.connection)
        .await
        .unwrap()
}
async fn created(store: &CrudStore, id: &str) -> Task {
    let mut task = sample_task(1_700_000_000);
    task.id = id.to_owned();
    store
        .append_task_event(
            TaskEventPayload::TaskCreated { task: task.clone() },
            task.created_at,
        )
        .await
        .unwrap();
    task
}
async fn progress(store: &CrudStore, task_id: &str, message: &str) -> crate::AppendedTaskEvent {
    store
        .append_task_event(
            TaskEventPayload::Progress {
                task_id: task_id.to_owned(),
                run_id: None,
                message: message.to_owned(),
                details: None,
            },
            1_700_000_001,
        )
        .await
        .unwrap()
}
async fn claim(store: &CrudStore, id: &str, token: &str, now: i64) -> TaskEventFanoutClaim {
    queue::claim(
        &store.with_maintenance_access().connection,
        &row(store, id).await.unwrap(),
        token.to_owned(),
        &|| now,
    )
    .await
    .unwrap()
    .unwrap()
}

#[tokio::test]
async fn initial_append_precedes_task_projection_and_rollback_restores_frontier_and_cursor() {
    let store = store().await;
    let task = sample_task(1_700_000_000);
    // Preparation outside writer, INSERT before Task projection, no pending FK.
    let prepared =
        events::PreparedTaskEvent::prepare(TaskEventPayload::TaskCreated { task: task.clone() })
            .unwrap();
    let tx = store.connection.begin().await.unwrap();
    events::append_prepared_event(&tx, prepared, unix_to_datetime(task.created_at))
        .await
        .unwrap();
    assert!(
        pending::Entity::find_by_id(task.id.clone())
            .one(&tx)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        pioneer_entity::task::Entity::find_by_id(task.id.clone())
            .one(&tx)
            .await
            .unwrap()
            .is_none()
    );
    tx.rollback().await.unwrap();
    assert!(row(&store, &task.id).await.is_none());
    assert!(
        task_event::Entity::find()
            .all(&store.connection)
            .await
            .unwrap()
            .is_empty()
    );
    created(&store, &task.id).await;
    let before = row(&store, &task.id).await.unwrap();
    let tx = store.connection.begin().await.unwrap();
    events::advance_fanout_cursor(&tx, &task.id, 1, unix_to_datetime(task.created_at))
        .await
        .unwrap();
    assert!(
        pending::Entity::find_by_id(task.id.clone())
            .one(&tx)
            .await
            .unwrap()
            .is_none()
    );
    tx.rollback().await.unwrap();
    assert_eq!(row(&store, &task.id).await.unwrap(), before);
    assert_eq!(
        store.get_task_event_fanout_cursor(&task.id).await.unwrap(),
        Some(0)
    );
}

#[tokio::test]
async fn duplicate_append_does_not_refresh_or_reset_a_poison_reservation() {
    let store = store().await;
    let task = created(&store, "fanout_duplicate").await;
    claim(&store, &task.id, "holder", NOW).await;
    let before = row(&store, &task.id).await.unwrap();
    let event = store
        .append_task_event(
            TaskEventPayload::TaskCreated { task: task.clone() },
            task.created_at,
        )
        .await
        .unwrap();
    assert!(!event.append_status.is_inserted());
    assert_eq!(row(&store, &task.id).await.unwrap(), before);
}

#[tokio::test]
async fn append_during_emit_keeps_lane_and_allows_prefix_ack_without_stale_error() {
    let store = store().await;
    let task = created(&store, "fanout_concurrent").await;
    let claim = claim(&store, &task.id, "holder", NOW).await;
    let later = progress(&store, &task.id, "N+1").await;
    let refreshed = row(&store, &task.id).await.unwrap();
    assert!(refreshed.generation > claim.generation);
    assert_eq!(refreshed.claim_token.as_deref(), Some("holder"));
    assert_eq!(refreshed.due_at, claim.retry_at);
    assert!(
        queue::renew(&store.with_maintenance_access().connection, &claim, &|| {
            chrono::Utc::now().timestamp()
        })
        .await
        .unwrap()
    );
    queue::ack(
        &store
            .with_maintenance_reads_and_critical_writes()
            .connection,
        &claim,
        1,
        &|| chrono::Utc::now().timestamp(),
    )
    .await
    .unwrap();
    assert_eq!(
        store.get_task_event_fanout_cursor(&task.id).await.unwrap(),
        Some(1)
    );
    let after = row(&store, &task.id).await.unwrap();
    assert_eq!(after.newest_sequence, later.sequence);
    assert_eq!(after.attempts, 0);
    queue::release(
        &store.with_maintenance_access().connection,
        &claim,
        TaskEventFanoutOutcome::Failed,
        &|| chrono::Utc::now().timestamp(),
    )
    .await
    .unwrap();
    assert_eq!(
        row(&store, &task.id).await.unwrap(),
        after,
        "old generation cannot record error"
    );
    queue::release(
        &store.with_maintenance_access().connection,
        &claim,
        TaskEventFanoutOutcome::Delivered,
        &|| chrono::Utc::now().timestamp(),
    )
    .await
    .unwrap();
    assert!(row(&store, &task.id).await.unwrap().claim_token.is_none());
}

#[tokio::test]
async fn lease_expiry_crash_before_ack_and_stale_holder_do_not_clear_new_error() {
    let store = store().await;
    let task = created(&store, "fanout_restart").await;
    progress(&store, &task.id, "next").await;
    let old = claim(&store, &task.id, "old", NOW).await;
    // An emitted but unACKed prefix must be selected again after lease expiry.
    let next = claim(&store, &task.id, "next", old.retry_at).await;
    assert_eq!(
        store.get_task_event_fanout_cursor(&task.id).await.unwrap(),
        Some(0)
    );
    let TaskEventFanoutPage::Prefix { events: page, .. } = store
        .task_event_fanout_page(&task.id, 0, 128, &mut { BYTES }, true)
        .await
        .unwrap()
    else {
        panic!("expected a delivered prefix")
    };
    assert_eq!(page[0].as_ref().unwrap().sequence, 1);
    let before = row(&store, &task.id).await.unwrap();
    assert!(
        !queue::renew(&store.with_maintenance_access().connection, &old, &|| {
            chrono::Utc::now().timestamp()
        })
        .await
        .unwrap()
    );
    queue::release(
        &store.with_maintenance_access().connection,
        &old,
        TaskEventFanoutOutcome::Failed,
        &|| chrono::Utc::now().timestamp(),
    )
    .await
    .unwrap();
    assert_eq!(row(&store, &task.id).await.unwrap(), before);
    // Successful immutable prefix ACK is allowed even with a different holder;
    // its token must not reset the new holder's penalty or due time.
    queue::ack(
        &store
            .with_maintenance_reads_and_critical_writes()
            .connection,
        &old,
        1,
        &|| chrono::Utc::now().timestamp(),
    )
    .await
    .unwrap();
    assert_eq!(
        store.get_task_event_fanout_cursor(&task.id).await.unwrap(),
        Some(1)
    );
    assert_eq!(row(&store, &task.id).await.unwrap(), before);
    queue::release(
        &store.with_maintenance_access().connection,
        &next,
        TaskEventFanoutOutcome::Failed,
        &|| chrono::Utc::now().timestamp(),
    )
    .await
    .unwrap();
    let failed = row(&store, &task.id).await.unwrap();
    assert_eq!(failed.attempts, 2);
    progress(&store, &task.id, "append cannot reset retry").await;
    let refreshed = row(&store, &task.id).await.unwrap();
    assert_eq!(refreshed.due_at, failed.due_at);
    assert_eq!(refreshed.attempts, 2);
}

#[tokio::test]
async fn healthy_prefixes_reset_retry_instead_of_exponentially_delaying_backlog() {
    let store = store().await;
    let task = created(&store, "fanout_healthy").await;
    for _ in 0..140 {
        progress(&store, &task.id, "backlog").await;
    }
    for sequence in 1..=140 {
        let claim = claim(&store, &task.id, &format!("token_{sequence}"), NOW).await;
        assert_eq!(row(&store, &task.id).await.unwrap().attempts, 1);
        queue::ack(
            &store
                .with_maintenance_reads_and_critical_writes()
                .connection,
            &claim,
            sequence,
            &|| chrono::Utc::now().timestamp(),
        )
        .await
        .unwrap();
        queue::release(
            &store.with_maintenance_access().connection,
            &claim,
            TaskEventFanoutOutcome::Delivered,
            &|| chrono::Utc::now().timestamp(),
        )
        .await
        .unwrap();
        assert_eq!(row(&store, &task.id).await.unwrap().attempts, 0);
    }
}

#[tokio::test]
async fn cursor_rewind_delete_late_initialization_and_task_delete_keep_source_coverage() {
    let store = store().await;
    let task = created(&store, "fanout_cursor").await;
    progress(&store, &task.id, "second").await;
    store
        .advance_task_event_fanout_cursor(&task.id, 2)
        .await
        .unwrap();
    assert!(row(&store, &task.id).await.is_none());
    cursor::Entity::update_many()
        .col_expr(cursor::Column::LastSequence, Expr::val(0_i64))
        .filter(cursor::Column::TaskId.eq(task.id.clone()))
        .exec(&store.connection)
        .await
        .unwrap();
    let old = claim(&store, &task.id, "old", NOW).await;
    cursor::Entity::delete_by_id(task.id.clone())
        .exec(&store.connection)
        .await
        .unwrap();
    assert!(row(&store, &task.id).await.unwrap().generation > old.generation);
    queue::ack(
        &store
            .with_maintenance_reads_and_critical_writes()
            .connection,
        &old,
        1,
        &|| chrono::Utc::now().timestamp(),
    )
    .await
    .unwrap();
    assert_eq!(
        store.get_task_event_fanout_cursor(&task.id).await.unwrap(),
        None,
        "no synthetic cursor after delete"
    );
    let new = progress(&store, &task.id, "late initialized event").await;
    assert_eq!(
        store.get_task_event_fanout_cursor(&task.id).await.unwrap(),
        Some(new.sequence - 1)
    );
    assert!(row(&store, &task.id).await.is_some());
    pioneer_entity::task::Entity::delete_by_id(task.id.clone())
        .exec(&store.connection)
        .await
        .unwrap();
    assert!(row(&store, &task.id).await.is_none());
    assert_eq!(
        store.get_task_event_fanout_cursor(&task.id).await.unwrap(),
        None
    );
}

#[tokio::test]
async fn discovery_and_pages_are_bounded_with_oversized_supported_separately() {
    let store = store().await;
    for n in 0..70 {
        created(&store, &format!("fanout_{n:03}")).await;
    }
    assert_eq!(
        store.due_task_event_fanout(NOW, 999).await.unwrap().len(),
        64
    );
    let id = "fanout_000";
    store.advance_task_event_fanout_cursor(id, 1).await.unwrap();
    let first = progress(&store, id, &"界".repeat(BYTES)).await;
    progress(&store, id, "following oversized").await;
    assert!(matches!(
        store
            .task_event_fanout_page(id, 1, 128, &mut { BYTES }, false)
            .await
            .unwrap(),
        TaskEventFanoutPage::BudgetDeferred
    ));
    let TaskEventFanoutPage::Prefix {
        events: page,
        bytes,
    } = store
        .task_event_fanout_page(id, 1, 128, &mut { BYTES }, true)
        .await
        .unwrap()
    else {
        panic!("expected a delivered prefix")
    };
    assert_eq!(page.len(), 1);
    assert!(bytes > BYTES);
    assert_eq!(page[0].as_ref().unwrap().sequence, first.sequence);
    let TaskEventFanoutPage::Prefix {
        events: page,
        bytes,
    } = store
        .task_event_fanout_page(id, first.sequence, 128, &mut { BYTES }, true)
        .await
        .unwrap()
    else {
        panic!("expected a delivered prefix")
    };
    assert_eq!(page.len(), 1);
    assert!(bytes < BYTES);
    for _ in 0..140 {
        progress(&store, id, "small").await;
    }
    let TaskEventFanoutPage::Prefix { events: page, .. } = store
        .task_event_fanout_page(id, first.sequence, 999, &mut { BYTES }, true)
        .await
        .unwrap()
    else {
        panic!("expected a delivered prefix")
    };
    assert_eq!(page.len(), 128);
}

#[tokio::test]
async fn bootstrap_fixed_bound_pages_restart_and_behind_cursor_changes_are_atomic() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    let before = Migrator::migrations()
        .iter()
        .position(|m| m.name() == MIGRATION)
        .unwrap();
    Migrator::up(&db, Some(before as u32)).await.unwrap();
    let executor = pioneer_sqlite::SqliteWriteExecutor::new(db.clone());
    let store = CrudStore::new(pioneer_sqlite::SqliteDatabase::from_executor(
        db,
        executor.clone(),
    ));
    for n in 0..70 {
        let task = created(&store, &format!("bootstrap_{n:03}")).await;
        if n % 2 == 0 {
            store
                .advance_task_event_fanout_cursor(&task.id, 1)
                .await
                .unwrap();
        }
    }
    executor
        .run_migrations::<Migrator>(pioneer_sqlite::SqliteWriteClass::Maintenance, None)
        .await
        .unwrap();
    assert!(!store.has_pending_task_event_fanout().await.unwrap());
    assert_eq!(
        store.bootstrap_task_event_fanout(32).await.unwrap(),
        (32, false)
    );
    assert_eq!(
        pending::Entity::find()
            .all(&store.connection)
            .await
            .unwrap()
            .len(),
        16
    );
    let cp = read_model_repair::load_checkpoint(&store.connection, "task_event_fanout_frontier")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cp.full_scan_cursor_id.as_deref(), Some("bootstrap_031"));
    assert_eq!(
        cp.full_scan_high_watermark_id.as_deref(),
        Some("bootstrap_069")
    );
    // Install-before-bootstrap catches changes behind and above the fixed bound.
    progress(&store, "bootstrap_000", "behind").await;
    created(&store, "zz_above_bound").await;
    let restarted = store.with_maintenance_access();
    assert_eq!(
        restarted.bootstrap_task_event_fanout(32).await.unwrap(),
        (32, false)
    );
    assert_eq!(
        restarted.bootstrap_task_event_fanout(32).await.unwrap(),
        (6, true)
    );
    assert_eq!(
        restarted.bootstrap_task_event_fanout(64).await.unwrap(),
        (0, true)
    );
    assert!(row(&store, "bootstrap_000").await.is_some());
    assert!(row(&store, "zz_above_bound").await.is_some());
    assert!(
        row(&store, "bootstrap_002").await.is_none(),
        "ACKed history is never enqueued"
    );
    assert_eq!(
        store
            .get_task_event_fanout_cursor("bootstrap_001")
            .await
            .unwrap(),
        Some(0),
        "metadata bootstrap does not emit/ACK"
    );
}

#[tokio::test]
async fn bootstrap_interrupted_page_rolls_back_checkpoint_and_frontier_then_retries() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    let before = Migrator::migrations()
        .iter()
        .position(|m| m.name() == MIGRATION)
        .unwrap();
    Migrator::up(&db, Some(before as u32)).await.unwrap();
    let executor = pioneer_sqlite::SqliteWriteExecutor::new(db.clone());
    let store = CrudStore::new(pioneer_sqlite::SqliteDatabase::from_executor(
        db,
        executor.clone(),
    ));
    created(&store, "bootstrap_a").await;
    created(&store, "bootstrap_b").await;
    executor
        .run_migrations::<Migrator>(pioneer_sqlite::SqliteWriteClass::Maintenance, None)
        .await
        .unwrap();
    store.connection.execute_unprepared("CREATE TRIGGER fanout_bootstrap_fail BEFORE INSERT ON task_event_fanout_pending WHEN NEW.task_id='bootstrap_b' BEGIN SELECT RAISE(ABORT,'fixture'); END").await.unwrap();
    assert!(store.bootstrap_task_event_fanout(64).await.is_err());
    assert!(!store.has_pending_task_event_fanout().await.unwrap());
    assert!(
        read_model_repair::load_checkpoint(&store.connection, "task_event_fanout_frontier")
            .await
            .unwrap()
            .is_none()
    );
    store
        .connection
        .execute_unprepared("DROP TRIGGER fanout_bootstrap_fail")
        .await
        .unwrap();
    assert_eq!(
        store.bootstrap_task_event_fanout(64).await.unwrap(),
        (2, true)
    );
}

#[tokio::test]
async fn empty_bootstrap_completion_and_generation_overflow_are_durable() {
    let store = store().await;
    assert_eq!(
        store.bootstrap_task_event_fanout(64).await.unwrap(),
        (0, true)
    );
    let task = created(&store, "fanout_after_empty").await;
    assert_eq!(
        store.bootstrap_task_event_fanout(64).await.unwrap(),
        (0, true)
    );
    let before = row(&store, &task.id).await.unwrap();
    sequence::Entity::update_many()
        .col_expr(sequence::Column::Generation, Expr::val(i64::MAX))
        .exec(&store.connection)
        .await
        .unwrap();
    let count = task_event::Entity::find()
        .all(&store.connection)
        .await
        .unwrap()
        .len();
    assert!(
        store
            .append_task_event(
                TaskEventPayload::Progress {
                    task_id: task.id.clone(),
                    run_id: None,
                    message: "overflow".into(),
                    details: None
                },
                1_700_000_002
            )
            .await
            .is_err()
    );
    assert_eq!(
        task_event::Entity::find()
            .all(&store.connection)
            .await
            .unwrap()
            .len(),
        count
    );
    assert_eq!(row(&store, &task.id).await.unwrap(), before);
    pioneer_entity::task::Entity::delete_by_id(task.id.clone())
        .exec(&store.connection)
        .await
        .unwrap();
    assert!(
        row(&store, &task.id).await.is_none(),
        "delete must not allocate a generation"
    );
}

#[tokio::test]
async fn point_context_selects_exact_event_run_trigger_and_preserves_latest_anchor() {
    let store = store().await;
    let mut task = sample_task(1_700_000_000);
    task.lifecycle_policy = Some(TaskLifecyclePolicy {
        attachment: TaskAttachmentMode::Attached,
        on_parent_cancel: TaskParentTerminalAction::Cancel,
        on_parent_failure: TaskParentTerminalAction::Cancel,
        completion: TaskCompletionBehavior::CompleteOnTerminalRun,
    });
    store
        .append_task_event(
            TaskEventPayload::TaskCreated { task: task.clone() },
            task.created_at,
        )
        .await
        .unwrap();
    let mut trigger = sample_task_trigger(task.created_at);
    trigger.spec = TaskTriggerSpec::Immediate;
    store
        .append_task_event(
            TaskEventPayload::TriggerCreated {
                trigger: trigger.clone(),
            },
            task.created_at,
        )
        .await
        .unwrap();
    let run = sample_task_run(task.created_at);
    let spec = sample_task_agent_spec(task.created_at);
    store
        .append_task_event(
            TaskEventPayload::RunCreated {
                run: run.clone(),
                agent_spec: Some(spec),
            },
            task.created_at,
        )
        .await
        .unwrap();
    let mut next = run.clone();
    next.id = "next_context_run".into();
    next.run_number = 2;
    next.run_group_id = next.id.clone();
    next.trigger_id = None;
    store
        .append_task_event(
            TaskEventPayload::RunCreated {
                run: next.clone(),
                agent_spec: None,
            },
            task.created_at + 1,
        )
        .await
        .unwrap();
    let payload = TaskEventPayload::RunStarted {
        task_id: task.id.clone(),
        run_id: run.id.clone(),
        started_at: task.created_at + 2,
    };
    let context = store
        .get_task_event_context(&payload)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(context.run.as_ref().unwrap().id, run.id);
    assert!(context.run_uses_creation_anchor(&run.id));
    assert!(
        store
            .get_task_creation_anchor_context(&context.task)
            .await
            .unwrap()
            .run
            .is_none(),
        "latest non-immediate run cannot overwrite the creation anchor"
    );
    // Notification branches must use the event's earlier run and its latest
    // persisted snapshot, even after a different run became the task's latest.
    let payloads = vec![
        TaskEventPayload::TaskQueued {
            task_id: task.id.clone(),
            run_id: Some(run.id.clone()),
        },
        TaskEventPayload::Progress {
            task_id: task.id.clone(),
            run_id: Some(run.id.clone()),
            message: "snapshot".into(),
            details: None,
        },
        TaskEventPayload::RunCompleted {
            task_id: task.id.clone(),
            run_id: run.id.clone(),
            result: None,
            completed_at: task.created_at + 3,
        },
        TaskEventPayload::RunFailed {
            task_id: task.id.clone(),
            run_id: run.id.clone(),
            error: None,
            completed_at: task.created_at + 3,
        },
        TaskEventPayload::RunBlocked {
            task_id: task.id.clone(),
            run_id: run.id.clone(),
            error: None,
            blocked_at: task.created_at + 3,
        },
        TaskEventPayload::RunCancelled {
            task_id: task.id.clone(),
            run_id: run.id.clone(),
            reason: None,
            cancelled_at: task.created_at + 3,
        },
    ];
    let expected = store.get_task_run(&run.id).await.unwrap().unwrap();
    for payload in payloads {
        let exact = store
            .get_task_event_context(&payload)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::to_value(exact.run.unwrap()).unwrap(),
            serde_json::to_value(&expected).unwrap()
        );
        assert_eq!(exact.run_trigger.unwrap().id, trigger.id);
    }
    let scheduled = store
        .get_task_event_context(&TaskEventPayload::TaskScheduled {
            task_id: task.id.clone(),
            trigger_id: trigger.id.clone(),
            next_fire_at: None,
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(scheduled.scheduled_trigger.unwrap().id, trigger.id);
    assert!(scheduled.run.is_none());
}

#[tokio::test]
async fn failed_reservation_defers_exact_snapshot_including_null_token() {
    let store = store().await;
    let task = created(&store, "claim_failure").await;
    let before = row(&store, &task.id).await.unwrap();
    assert!(before.claim_token.is_none());
    store.connection.execute_unprepared("CREATE TRIGGER fanout_reject_token BEFORE UPDATE OF claim_token ON task_event_fanout_pending WHEN NEW.claim_token IS NOT NULL BEGIN SELECT RAISE(ABORT,'fixture'); END").await.unwrap();
    assert!(
        queue::claim(
            &store.with_maintenance_access().connection,
            &before,
            "rejected".into(),
            &|| NOW
        )
        .await
        .is_err()
    );
    let after = row(&store, &task.id).await.unwrap();
    assert!(after.claim_token.is_none());
    assert_eq!(after.attempts, 1);
    assert_eq!(after.due_at, NOW + 5);
    store
        .connection
        .execute_unprepared("DROP TRIGGER fanout_reject_token")
        .await
        .unwrap();
    assert!(
        queue::claim(
            &store.with_maintenance_access().connection,
            &before,
            "stale".into(),
            &|| NOW + 5
        )
        .await
        .unwrap()
        .is_none()
    );
    assert_eq!(row(&store, &task.id).await.unwrap(), after);
}

#[tokio::test]
async fn malformed_event_page_keeps_good_prefix_and_does_not_discard_poison() {
    let store = store().await;
    let task = created(&store, "fanout_poison").await;
    let bad = progress(&store, &task.id, "bad").await;
    let good = progress(&store, &task.id, "later").await;
    task_event::Entity::update_many()
        .col_expr(task_event::Column::PayloadJson, Expr::val("{bad"))
        .filter(task_event::Column::Id.eq(bad.id.clone()))
        .exec(&store.connection)
        .await
        .unwrap();
    let TaskEventFanoutPage::Prefix { events: page, .. } = store
        .task_event_fanout_page(&task.id, 0, 128, &mut { BYTES }, true)
        .await
        .unwrap()
    else {
        panic!("expected a delivered prefix")
    };
    assert!(page[0].is_ok());
    assert!(page[1].is_err());
    assert_eq!(page[2].as_ref().unwrap().sequence, good.sequence);
    let claim = claim(&store, &task.id, "poison", NOW).await;
    queue::ack(
        &store
            .with_maintenance_reads_and_critical_writes()
            .connection,
        &claim,
        1,
        &|| chrono::Utc::now().timestamp(),
    )
    .await
    .unwrap();
    queue::release(
        &store.with_maintenance_access().connection,
        &claim,
        TaskEventFanoutOutcome::Failed,
        &|| chrono::Utc::now().timestamp(),
    )
    .await
    .unwrap();
    assert_eq!(
        store.get_task_event_fanout_cursor(&task.id).await.unwrap(),
        Some(1)
    );
    assert!(row(&store, &task.id).await.unwrap().due_at > chrono::Utc::now().timestamp());
}

#[derive(Default)]
struct Routes {
    reads: std::sync::Mutex<Vec<pioneer_sqlite::SqliteReadClass>>,
    writes: std::sync::Mutex<Vec<pioneer_sqlite::SqliteWriteEvent>>,
    queued: tokio::sync::Notify,
    watch: std::sync::atomic::AtomicBool,
}
impl pioneer_sqlite::SqliteReadObserver for Routes {
    fn observe(&self, event: pioneer_sqlite::SqliteReadEvent) {
        if let pioneer_sqlite::SqliteReadEvent::OperationFinished { class, .. } = event {
            self.reads.lock().unwrap().push(class);
        }
    }
}
impl pioneer_sqlite::SqliteWriteObserver for Routes {
    fn observe(&self, event: pioneer_sqlite::SqliteWriteEvent) {
        self.writes.lock().unwrap().push(event);
        if self.watch.load(std::sync::atomic::Ordering::SeqCst)
            && matches!(
                event,
                pioneer_sqlite::SqliteWriteEvent::Enqueued {
                    class: pioneer_sqlite::SqliteWriteClass::Maintenance,
                    ..
                }
            )
        {
            self.queued.notify_one();
        }
    }
}
async fn disk_store(path: &std::path::Path, routes: Arc<Routes>) -> CrudStore {
    let mut options = sea_orm::ConnectOptions::new(format!("sqlite://{}?mode=rwc", path.display()));
    options
        .max_connections(1)
        .min_connections(1)
        .map_sqlx_sqlite_opts(|o| o.pragma("journal_mode", "WAL"));
    let writer = Database::connect(options).await.unwrap();
    let executor = pioneer_sqlite::SqliteWriteExecutor::with_observer(writer, routes.clone());
    executor
        .run_migrations::<Migrator>(pioneer_sqlite::SqliteWriteClass::Maintenance, None)
        .await
        .unwrap();
    let mut options =
        sea_orm::ConnectOptions::new(pioneer_sqlite::sqlite_read_only_connection_url(path));
    options
        .max_connections(1)
        .min_connections(1)
        .map_sqlx_sqlite_opts(|o| {
            o.read_only(true)
                .create_if_missing(false)
                .pragma("query_only", "ON")
        });
    let reader = Database::connect(options).await.unwrap();
    assert!(
        reader
            .execute_unprepared("UPDATE task_event_fanout_sequence SET generation=0")
            .await
            .is_err(),
        "physical reader remains query_only"
    );
    CrudStore::new(
        pioneer_sqlite::SqliteDatabase::from_executor_with_read_observer(reader, executor, routes),
    )
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn maintenance_discovery_claims_context_and_cancellation_use_physical_routes() {
    use pioneer_sqlite::{SqliteReadClass, SqliteWriteClass, SqliteWriteEvent};
    let path = std::env::temp_dir().join(format!("pioneer-fanout-{}.sqlite", uuid::Uuid::new_v4()));
    let routes = Arc::new(Routes::default());
    let store = disk_store(&path, routes.clone()).await;
    let task = created(&store, "route_task").await;
    routes.reads.lock().unwrap().clear();
    routes.writes.lock().unwrap().clear();
    let candidates = store.due_task_event_fanout(NOW, 64).await.unwrap();
    assert_eq!(candidates.len(), 1);
    assert!(
        store
            .with_maintenance_access()
            .get_task_event_context(&TaskEventPayload::TaskCreated { task: task.clone() })
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        routes
            .reads
            .lock()
            .unwrap()
            .iter()
            .all(|c| *c == SqliteReadClass::Maintenance)
    );
    assert!(
        routes.writes.lock().unwrap().is_empty(),
        "point context and discovery do not reserve the writer"
    );
    let before = row(&store, &task.id).await.unwrap();
    let hold = store.connection.begin().await.unwrap();
    routes
        .watch
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let worker = store.clone();
    let candidate = candidates[0].clone();
    let waiting = tokio::spawn(async move {
        worker
            .claim_task_event_fanout(&candidate, &|| chrono::Utc::now().timestamp())
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), routes.queued.notified())
        .await
        .unwrap();
    assert!(
        store.get_task_record(&task.id).await.unwrap().is_some(),
        "interactive reader is independent of queued maintenance writer"
    );
    waiting.abort();
    assert!(waiting.await.unwrap_err().is_cancelled());
    hold.rollback().await.unwrap();
    assert_eq!(row(&store, &task.id).await.unwrap(), before);
    assert!(routes.writes.lock().unwrap().iter().any(|event|matches!(event,SqliteWriteEvent::Cancelled{class:SqliteWriteClass::Maintenance,queue,..} if queue.maintenance==0)));
    routes
        .watch
        .store(false, std::sync::atomic::Ordering::SeqCst);
    routes.writes.lock().unwrap().clear();
    let claimed = store
        .claim_task_event_fanout(&row(&store, &task.id).await.unwrap(), &|| {
            chrono::Utc::now().timestamp()
        })
        .await
        .unwrap()
        .unwrap();
    assert!(routes.writes.lock().unwrap().iter().all(|event|!matches!(event,SqliteWriteEvent::Enqueued{class,..} if *class!=SqliteWriteClass::Maintenance)));
    routes.writes.lock().unwrap().clear();
    store
        .with_maintenance_reads_and_critical_writes()
        .ack_task_event_fanout(&claimed, 1, &|| chrono::Utc::now().timestamp())
        .await
        .unwrap();
    assert!(routes.writes.lock().unwrap().iter().any(|event| matches!(
        event,
        SqliteWriteEvent::Enqueued {
            class: SqliteWriteClass::Critical,
            ..
        }
    )));
    store.connection.close().await.unwrap();
    std::fs::remove_file(&path).unwrap();
    for suffix in ["-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
}

#[tokio::test]
async fn delivery_context_preserves_latest_window_without_attempts_or_payloads() {
    use pioneer_entity::task_delivery as delivery;
    let store = store().await;
    let task = created(&store, "delivery-context").await;
    for n in 0..101 {
        delivery::Entity::insert(delivery::ActiveModel {
            id: Set(format!("delivery-{n}")),
            workspace_id: Set(task.workspace_id.clone()),
            task_id: Set(task.id.clone()),
            run_id: Set("delivery-run".into()),
            delivery_key: Set(format!("key-{n}")),
            mode: Set("thread".into()),
            status: Set("pending".into()),
            target_thread_id: Set(Some(if n == 0 { "occurrence" } else { "other" }.into())),
            // Notification context must never deserialize unrelated snapshots.
            result_snapshot_json: Set(Some("invalid-json".into())),
            created_at: Set(crate::util::unix_to_datetime(1_700_000_000 + n)),
            updated_at: Set(crate::util::unix_to_datetime(1_700_000_000 + n)),
            ..Default::default()
        })
        .exec(&store.connection)
        .await
        .unwrap();
    }
    assert!(
        !store
            .task_run_has_pending_thread_delivery(
                &task.workspace_id,
                &task.id,
                "delivery-run",
                Some("occurrence")
            )
            .await
            .unwrap()
    );
    delivery::Entity::update_many()
        .col_expr(
            delivery::Column::UpdatedAt,
            Expr::value(crate::util::unix_to_datetime(1_700_001_000)),
        )
        .filter(delivery::Column::Id.eq("delivery-0"))
        .exec(&store.connection)
        .await
        .unwrap();
    assert!(
        store
            .task_run_has_pending_thread_delivery(
                &task.workspace_id,
                &task.id,
                "delivery-run",
                Some("occurrence")
            )
            .await
            .unwrap()
    );
    assert!(
        !store
            .task_run_has_pending_thread_delivery(
                &task.workspace_id,
                &task.id,
                "other-run",
                Some("occurrence")
            )
            .await
            .unwrap()
    );
    delivery::Entity::update_many()
        .col_expr(delivery::Column::Status, Expr::value("delivered"))
        .filter(delivery::Column::Id.eq("delivery-0"))
        .exec(&store.connection)
        .await
        .unwrap();
    assert!(
        !store
            .task_run_has_pending_thread_delivery(
                &task.workspace_id,
                &task.id,
                "delivery-run",
                Some("occurrence")
            )
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn empty_budget_page_restores_waiting_priority_and_error_history() {
    let store = store().await;
    let task = created(&store, "empty_byte_page").await;
    let prior = row(&store, &task.id).await.unwrap();
    let holder = claim(&store, &task.id, "empty-page", NOW).await;
    assert!(matches!(
        store
            .task_event_fanout_page(&task.id, 0, 2, &mut 1, false)
            .await
            .unwrap(),
        TaskEventFanoutPage::BudgetDeferred
    ));
    queue::release(
        &store.with_maintenance_access().connection,
        &holder,
        TaskEventFanoutOutcome::BudgetDeferred,
        &|| NOW,
    )
    .await
    .unwrap();
    let released = row(&store, &task.id).await.unwrap();
    assert_eq!(released.attempts, prior.attempts);
    assert_eq!(released.due_at, prior.due_at);
    assert!(released.claim_token.is_none());
}

#[tokio::test]
async fn utf8_metadata_bytecode_does_not_fetch_overflow_payload_before_admission() {
    use sea_orm::QueryTrait;
    let store = store().await;
    let task = created(&store, "metadata-bytecode").await;
    let encoding = store
        .connection
        .query_one_raw(Statement::from_string(
            DatabaseBackend::Sqlite,
            "PRAGMA encoding".to_owned(),
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(encoding.try_get::<String>("", "encoding").unwrap(), "UTF-8");
    let small = progress(&store, &task.id, "界🙂").await;
    let expected = serde_json::to_string(&small.payload).unwrap().len();
    assert!(
        expected
            > serde_json::to_string(&small.payload)
                .unwrap()
                .chars()
                .count()
    );
    for _ in 0..3 {
        progress(&store, &task.id, &"界".repeat(BYTES)).await;
    }
    assert!(matches!(
        store
            .task_event_fanout_page(&task.id, 1, 128, &mut (expected - 1), false)
            .await
            .unwrap(),
        TaskEventFanoutPage::BudgetDeferred
    ));
    let mut bytes_left = expected;
    let TaskEventFanoutPage::Prefix { events, bytes } = store
        .task_event_fanout_page(&task.id, 1, 128, &mut bytes_left, false)
        .await
        .unwrap()
    else {
        panic!("UTF-8 prefix fits exact byte budget")
    };
    assert_eq!(events.len(), 1);
    assert_eq!(bytes, expected);
    assert_eq!(bytes_left, 0, "payload charged before full read/decode");
    let TaskEventFanoutPage::Prefix { events, bytes } = store
        .task_event_fanout_page(&task.id, small.sequence, 128, &mut { BYTES }, true)
        .await
        .unwrap()
    else {
        panic!("one oversized event is supported")
    };
    assert_eq!(
        events.len(),
        1,
        "three large inputs must fetch only one payload"
    );
    assert!(bytes > BYTES);
    let statement = events::fanout_metadata_query(&task.id, 1, 128).build(DatabaseBackend::Sqlite);
    assert!(statement.sql.contains("octet_length(payload_json)"));
    assert!(!statement.sql.contains("CAST"));
    // EXPLAIN (not QUERY PLAN) checks OP_Column's metadata-only flag on the
    // function's argument register. The negative control uses the old CAST.
    for (sql, function, metadata_only) in [
        (statement.sql.clone(), "octet_length", true),
        (
            statement.sql.replace(
                "octet_length(payload_json)",
                "length(CAST(payload_json AS BLOB))",
            ),
            "length",
            false,
        ),
    ] {
        let mut explained = statement.clone();
        explained.sql = format!("EXPLAIN {sql}");
        let bytecode = store
            .with_maintenance_access()
            .connection
            .query_all_raw(explained)
            .await
            .unwrap();
        let function_row = bytecode
            .iter()
            .find(|r| {
                let op: String = r.try_get("", "opcode").unwrap();
                op == "Function"
                    && r.try_get::<Option<String>>("", "p4")
                        .unwrap()
                        .is_some_and(|p| p.starts_with(function))
            })
            .expect("byte-length function in bytecode");
        let argument: i64 = function_row.try_get("", "p2").unwrap();
        let column = bytecode
            .iter()
            .find(|r| {
                r.try_get::<String>("", "opcode").unwrap() == "Column"
                    && r.try_get::<i64>("", "p3").unwrap() == argument
            })
            .expect("direct column argument");
        let flags: i64 = column.try_get("", "p5").unwrap();
        // OPFLAG_BYTELENARG in bundled SQLite 3.51.3 (libsqlite3-sys 0.37).
        assert_eq!(flags & 0xc0 == 0xc0, metadata_only);
    }
}

#[tokio::test]
async fn budget_deferral_fences_new_holders_resets_and_keeps_appended_work() {
    let store = store().await;
    let task = created(&store, "budget-fences").await;
    let original = row(&store, &task.id).await.unwrap();
    let old = claim(&store, &task.id, "budget-old", NOW).await;
    progress(&store, &task.id, "append retains this lane").await;
    let generation = row(&store, &task.id).await.unwrap().generation;
    queue::release(
        &store.with_maintenance_access().connection,
        &old,
        TaskEventFanoutOutcome::BudgetDeferred,
        &|| NOW,
    )
    .await
    .unwrap();
    let restored = row(&store, &task.id).await.unwrap();
    assert_eq!(restored.due_at, original.due_at);
    assert_eq!(restored.attempts, original.attempts);
    assert_eq!(restored.generation, generation);
    let old = claim(&store, &task.id, "expired", NOW).await;
    let new = claim(&store, &task.id, "replacement", old.retry_at).await;
    let held = row(&store, &task.id).await.unwrap();
    queue::release(
        &store.with_maintenance_access().connection,
        &old,
        TaskEventFanoutOutcome::BudgetDeferred,
        &|| NOW,
    )
    .await
    .unwrap();
    assert_eq!(row(&store, &task.id).await.unwrap(), held);
    cursor::Entity::delete_by_id(task.id.clone())
        .exec(&store.connection)
        .await
        .unwrap();
    let reset = row(&store, &task.id).await.unwrap();
    queue::release(
        &store.with_maintenance_access().connection,
        &new,
        TaskEventFanoutOutcome::BudgetDeferred,
        &|| NOW,
    )
    .await
    .unwrap();
    assert_eq!(row(&store, &task.id).await.unwrap(), reset);
}

fn fanout_rollback_steps() -> u32 {
    let migrations = Migrator::migrations();
    let target = migrations
        .iter()
        .position(|m| m.name() == MIGRATION)
        .expect("fanout migration registered");
    (migrations.len() - target) as u32
}
async fn named_schema_objects(store: &CrudStore) -> Vec<String> {
    store.connection.query_all_raw(Statement::from_string(DatabaseBackend::Sqlite,
        "SELECT name FROM sqlite_master WHERE name LIKE 'task_event_fanout_%' OR name LIKE '%task_event_fanout%' OR name IN ('idx_task_trigger_task_created','idx_task_agent_spec_task_created','idx_task_agent_spec_run_created','idx_task_delivery_fanout_pending') ORDER BY name".to_owned()
    )).await.unwrap().into_iter().map(|r| r.try_get("", "name").unwrap()).collect()
}
#[tokio::test]
async fn fanout_partial_ddl_failure_rollback_reinstall_down_and_restart_preserve_coverage() {
    let store = store().await;
    let maintenance = store.with_maintenance_access();
    let tx = maintenance.connection.begin().await.unwrap();
    Migrator::down(&*tx, Some(fanout_rollback_steps()))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let before = named_schema_objects(&store).await;
    // Cursor table/index predate this package and must survive down.
    assert!(before.iter().any(|n| n == "task_event_fanout_cursor"));
    assert!(!before.iter().any(|n| n == "task_event_fanout_pending"));
    crate::repositories::read_model_repair::reset_full_scan(
        &maintenance.connection,
        "unrelated-repair",
        1,
        None,
    )
    .await
    .unwrap();
    let foreign = crate::repositories::read_model_repair::load_checkpoint(
        &store.connection,
        "unrelated-repair",
    )
    .await
    .unwrap()
    .unwrap();
    // Force failure after pending/sequence/due objects were created.
    maintenance
        .connection
        .execute_unprepared("CREATE INDEX idx_task_delivery_fanout_pending ON task_delivery(id)")
        .await
        .unwrap();
    let tx = maintenance.connection.begin().await.unwrap();
    assert!(Migrator::up(&*tx, None).await.is_err());
    tx.rollback().await.unwrap();
    let objects = named_schema_objects(&store).await;
    let mut expected = before.clone();
    expected.push("idx_task_delivery_fanout_pending".into());
    expected.sort();
    assert_eq!(
        objects, expected,
        "all partial DDL rolled back; only the fixture blocker remains"
    );
    let applied = store
        .connection
        .query_one_raw(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "SELECT count(*) AS n FROM seaql_migrations WHERE version=?",
            [MIGRATION.into()],
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(applied.try_get::<i64>("", "n").unwrap(), 0);
    assert!(
        read_model_repair::load_checkpoint(&store.connection, "task_event_fanout_frontier")
            .await
            .unwrap()
            .is_none()
    );
    maintenance
        .connection
        .execute_unprepared("DROP INDEX idx_task_delivery_fanout_pending")
        .await
        .unwrap();
    let tx = maintenance.connection.begin().await.unwrap();
    Migrator::up(&*tx, None).await.unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        store.bootstrap_task_event_fanout(64).await.unwrap(),
        (0, true)
    );
    let complete =
        read_model_repair::load_checkpoint(&store.connection, "task_event_fanout_frontier")
            .await
            .unwrap()
            .unwrap();
    let task = created(&store, "reinstalled-tracking").await;
    assert!(row(&store, &task.id).await.is_some());
    let restarted = CrudStore::new(store.connection.clone());
    assert_eq!(
        restarted.bootstrap_task_event_fanout(64).await.unwrap(),
        (0, true)
    );
    assert_eq!(
        read_model_repair::load_checkpoint(&store.connection, "task_event_fanout_frontier")
            .await
            .unwrap()
            .unwrap(),
        complete
    );
    let tx = maintenance.connection.begin().await.unwrap();
    Migrator::down(&*tx, Some(fanout_rollback_steps()))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(named_schema_objects(&store).await, before);
    assert!(
        read_model_repair::load_checkpoint(&store.connection, "task_event_fanout_frontier")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        read_model_repair::load_checkpoint(&store.connection, "unrelated-repair")
            .await
            .unwrap()
            .unwrap(),
        foreign
    );
    let tx = maintenance.connection.begin().await.unwrap();
    Migrator::up(&*tx, None).await.unwrap();
    tx.commit().await.unwrap();
    progress(&store, &task.id, "new append after reinstall").await;
    assert!(row(&store, &task.id).await.is_some());
}

#[tokio::test]
async fn completed_bootstrap_survives_physical_close_and_reopen() {
    let path = std::env::temp_dir().join(format!(
        "pioneer-fanout-bootstrap-{}.sqlite",
        uuid::Uuid::new_v4()
    ));
    let store = disk_store(&path, Arc::new(Routes::default())).await;
    assert_eq!(
        store.bootstrap_task_event_fanout(64).await.unwrap(),
        (0, true)
    );
    let checkpoint =
        read_model_repair::load_checkpoint(&store.connection, "task_event_fanout_frontier")
            .await
            .unwrap()
            .unwrap();
    created(&store, "post-completion-append").await;
    store.connection.clone().close().await.unwrap();
    drop(store);
    let reopened = disk_store(&path, Arc::new(Routes::default())).await;
    assert_eq!(
        reopened.bootstrap_task_event_fanout(64).await.unwrap(),
        (0, true)
    );
    assert_eq!(
        read_model_repair::load_checkpoint(&reopened.connection, "task_event_fanout_frontier")
            .await
            .unwrap()
            .unwrap(),
        checkpoint
    );
    assert!(row(&reopened, "post-completion-append").await.is_some());
    reopened.connection.clone().close().await.unwrap();
    drop(reopened);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
    let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
}

#[tokio::test]
async fn late_budget_result_cannot_undo_prefix_progress_or_real_error_bookkeeping() {
    let store = store().await;
    for (id, outcome) in [
        ("budget-after-prefix", TaskEventFanoutOutcome::Delivered),
        ("budget-after-error", TaskEventFanoutOutcome::Failed),
    ] {
        let task = created(&store, id).await;
        progress(&store, id, "backlog keeps the frontier present").await;
        let holder = claim(&store, id, id, NOW).await;
        if outcome == TaskEventFanoutOutcome::Delivered {
            queue::ack(
                &store
                    .with_maintenance_reads_and_critical_writes()
                    .connection,
                &holder,
                1,
                &|| NOW,
            )
            .await
            .unwrap();
        } else {
            queue::release(
                &store.with_maintenance_access().connection,
                &holder,
                outcome,
                &|| NOW + 1,
            )
            .await
            .unwrap();
        }
        let before = row(&store, &task.id).await.unwrap();
        queue::release(
            &store.with_maintenance_access().connection,
            &holder,
            TaskEventFanoutOutcome::BudgetDeferred,
            &|| NOW + 1,
        )
        .await
        .unwrap();
        assert_eq!(row(&store, &task.id).await.unwrap(), before);
        assert_eq!(
            store.get_task_event_fanout_cursor(id).await.unwrap(),
            Some(if outcome == TaskEventFanoutOutcome::Delivered {
                1
            } else {
                0
            })
        );
    }
}
