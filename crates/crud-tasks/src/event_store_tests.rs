use super::*;
use crate::event_fanout as queue;
use crate::event_fanout::TASK_EVENT_FANOUT_BYTE_BUDGET as BYTES;
use crate::task_event as events;
use crate::task_event_context::TaskEventContextRepository;
use crate::test_support::execution::drain_custom_publisher_fanout;
use crate::test_support::task_projection::{
    NoFrozenOutput, sample_task, sample_task_agent_spec, sample_task_run, sample_task_trigger,
};
use crate::values::unix_to_datetime;
use pioneer_entity::{
    task_event, task_event_fanout_cursor as cursor, task_event_fanout_pending as pending,
    task_event_fanout_sequence as sequence,
};
use pioneer_protocol::{
    Task, TaskAttachmentMode, TaskCompletionBehavior, TaskError, TaskErrorClass,
    TaskLifecyclePolicy, TaskParentTerminalAction, TaskResult, TaskRunStatus, TaskStatus,
    TaskTriggerSpec, TaskTriggerStatus, TaskValue,
};
use sea_orm::sea_query::Expr;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, DatabaseBackend, DbBackend, EntityTrait,
    PaginatorTrait, QueryFilter, QueryOrder, QuerySelect, Set, Statement, TransactionTrait,
};
use sha2::{Digest, Sha256};
use std::sync::Arc;

fn task_event_store(connection: SqliteDatabase) -> TaskEventStore<NoFrozenOutput> {
    TaskEventStore::new(connection, NoFrozenOutput)
}

impl TaskEventStore<NoFrozenOutput> {
    fn context(&self) -> TaskEventContextRepository<'_> {
        TaskEventContextRepository::new(&self.connection)
    }

    fn with_maintenance_reads_and_critical_writes(&self) -> Self {
        Self::new(
            self.connection.maintenance().with_critical_writes(),
            NoFrozenOutput,
        )
    }
}

const NOW: i64 = 4_000_000_000;
async fn store(fixture: &pioneer_postgres_test::TestDatabase) -> TaskEventStore<NoFrozenOutput> {
    let writer = fixture.connect().await.unwrap();
    writer
        .execute_unprepared(
            "INSERT INTO workspace(id,name) VALUES ('ws_task','Tasks');
         INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status)
            VALUES ('thr_task','ws_task','','agent','model','provider','active');
         INSERT INTO turn(id,thread_id,status) VALUES ('turn_task','thr_task','completed')",
        )
        .await
        .unwrap();
    let reader = fixture.connect_read_only().await.unwrap();
    task_event_store(pioneer_sqlite::SqliteDatabase::new(reader, writer))
}
async fn row(store: &TaskEventStore<NoFrozenOutput>, id: &str) -> Option<pending::Model> {
    pending::Entity::find_by_id(id.to_owned())
        .one(&store.connection)
        .await
        .unwrap()
}
async fn created(store: &TaskEventStore<NoFrozenOutput>, id: &str) -> Task {
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
async fn progress(
    store: &TaskEventStore<NoFrozenOutput>,
    task_id: &str,
    message: &str,
) -> AppendedTaskEvent {
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
async fn claim(
    store: &TaskEventStore<NoFrozenOutput>,
    id: &str,
    token: &str,
    now: i64,
) -> TaskEventFanoutClaim {
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
    pioneer_postgres_test::with_database(async |fixture| {
        let store = store(fixture).await;
        let task = sample_task(1_700_000_000);
        // Preparation outside writer, INSERT before Task projection, no pending FK.
        let prepared = events::PreparedTaskEvent::prepare(TaskEventPayload::TaskCreated {
            task: task.clone(),
        })
        .unwrap();
        let tx = store.connection.begin().await.unwrap();
        events::append_prepared_event(&tx, prepared, unix_to_datetime(task.created_at))
            .await
            .unwrap();
        let pending = pending::Entity::find_by_id(task.id.clone())
            .one(&tx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!((pending.first_sequence, pending.newest_sequence), (1, 1));
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
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn compressed_duplicate_append_preserves_winner_reservation_and_later_insert() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = store(fixture).await;
        let mut task = sample_task(1_700_000_000);
        task.id = "fanout_compressed_duplicate".into();
        task.goal = "Повторное создание 界🙂\n".repeat(4096);
        let payload = TaskEventPayload::TaskCreated { task: task.clone() };
        let original = store
            .append_task_event(payload.clone(), task.created_at)
            .await
            .unwrap();
        claim(&store, &task.id, "holder", NOW).await;
        let before = row(&store, &task.id).await.unwrap();
        let compressed = task_event::Entity::find_by_id(original.id.clone())
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            compressed.payload_json_codec, 1,
            "fixture must actually be compressed"
        );
        assert!(compressed.payload_json.len() < compressed.payload_json_bytes as usize);
        // Both persisted replays share proof payloads during writer admission.
        let replays = store
            .append_task_events(vec![payload.clone(), payload.clone()], task.created_at + 1)
            .await
            .unwrap();
        assert_eq!(replays.len(), 2);
        for event in replays {
            assert!(!event.append_status.is_inserted());
            assert_eq!(event.id, original.id);
            assert_eq!(event.sequence, original.sequence);
            assert_eq!(event.created_at, original.created_at);
            assert_eq!(event.payload, payload);
        }
        assert_eq!(row(&store, &task.id).await.unwrap(), before);

        let later_message = "new event after the duplicate 界🙂".repeat(4096);
        let later = progress(&store, &task.id, &later_message).await;
        assert!(later.append_status.is_inserted());
        assert_eq!(later.sequence, original.sequence + 1);
        let appended = row(&store, &task.id).await.unwrap();
        assert!(appended.generation > before.generation);
        assert_eq!(appended.first_sequence, before.first_sequence);
        assert_eq!(appended.newest_sequence, later.sequence);
        assert_eq!(appended.claim_token, before.claim_token);
        assert_eq!(appended.attempts, before.attempts);
        assert_eq!(appended.due_at, before.due_at);
        let published = task_event::Entity::find_by_id(later.id.clone())
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(published.payload_json_codec, 1);
        assert!(published.payload_json.len() < published.payload_json_bytes as usize);
        let decoded = store.list_task_events_after(&task.id, 0).await.unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(
            (decoded[0].id.as_str(), decoded[0].sequence),
            (original.id.as_str(), original.sequence)
        );
        assert_eq!(decoded[0].payload, payload);
        assert_eq!(
            (decoded[1].id.as_str(), decoded[1].sequence),
            (later.id.as_str(), later.sequence)
        );
        assert_eq!(
            decoded[1].payload,
            TaskEventPayload::Progress {
                task_id: task.id.clone(),
                run_id: None,
                message: later_message,
                details: None,
            }
        );
        assert_eq!(row(&store, &task.id).await.unwrap(), appended);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn atomic_cancelled_replays_keep_first_uncommitted_payload_and_sequence() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = store(fixture).await;
        let task = created(&store, "fanout_cancelled_batch").await;
        let before = row(&store, &task.id).await.unwrap();
        let original = TaskEventPayload::TaskCancelled {
            task_id: task.id.clone(),
            reason: Some("first cancellation 界🙂\n".repeat(16384)),
            completed_at: task.created_at + 1,
        };
        let retry = TaskEventPayload::TaskCancelled {
            task_id: task.id.clone(),
            reason: Some("replayed cancellation".into()),
            completed_at: task.created_at + 2,
        };
        // No cancellation exists during reader preflight. Three equivalent inputs
        // exercise a shared large candidate, retaining the first semantic winner.
        let events = store
            .append_task_events(
                vec![original.clone(), retry.clone(), retry],
                task.created_at + 2,
            )
            .await
            .unwrap();
        assert_eq!(events.len(), 3);
        assert!(events[0].append_status.is_inserted());
        assert!(!events[1].append_status.is_inserted());
        assert!(!events[2].append_status.is_inserted());
        assert_eq!(events[0].id, events[1].id);
        assert_eq!(events[0].id, events[2].id);
        assert_eq!(events[0].sequence, before.newest_sequence + 1);
        assert_eq!(events[1].sequence, events[0].sequence);
        assert_eq!(events[2].sequence, events[0].sequence);
        assert_eq!(events[0].payload, original);
        assert_eq!(events[1].payload, original);
        assert_eq!(events[2].payload, original);
        let pending = row(&store, &task.id).await.unwrap();
        assert_eq!(pending.generation, before.generation + 1);
        assert_eq!(pending.first_sequence, before.first_sequence);
        assert_eq!(pending.newest_sequence, events[0].sequence);
        let decoded = store
            .list_task_events_after(&task.id, before.newest_sequence)
            .await
            .unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].payload, original);
        let projected = store
            .context()
            .get_task_record(&task.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(projected.status, TaskStatus::Cancelled);
        assert_eq!(projected.completed_at, Some(task.created_at + 1));
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn append_during_emit_keeps_lane_and_allows_prefix_ack_without_stale_error() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = store(fixture).await;
        let task = created(&store, "fanout_concurrent").await;
        let claim = claim(&store, &task.id, "holder", NOW).await;
        let later = progress(&store, &task.id, "N+1").await;
        let refreshed = row(&store, &task.id).await.unwrap();
        assert!(refreshed.generation > claim.generation);
        assert_eq!(refreshed.claim_token.as_deref(), Some("holder"));
        assert_eq!(refreshed.due_at, claim.retry_at);
        assert_eq!(refreshed.first_sequence, claim.first_sequence);
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
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn lease_expiry_crash_before_ack_and_stale_holder_do_not_clear_new_error() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = store(fixture).await;
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
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn healthy_prefixes_reset_retry_instead_of_exponentially_delaying_backlog() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = store(fixture).await;
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
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn cursor_reset_delete_recreate_only_reconcile_existing_new_work() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = store(fixture).await;
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
        assert!(
            row(&store, &task.id).await.is_none(),
            "reset alone does not queue history"
        );
        cursor::Entity::delete_by_id(task.id.clone())
            .exec(&store.connection)
            .await
            .unwrap();
        events::initialize_fanout_cursor(&store.connection, &task.id, 0, unix_to_datetime(NOW))
            .await
            .unwrap();
        assert!(
            row(&store, &task.id).await.is_none(),
            "late cursor alone does not queue history"
        );
        let new = progress(&store, &task.id, "first newly tracked event").await;
        let old = claim(&store, &task.id, "old", NOW).await;
        assert_eq!(old.first_sequence, new.sequence);
        cursor::Entity::delete_by_id(task.id.clone())
            .exec(&store.connection)
            .await
            .unwrap();
        let fenced = row(&store, &task.id).await.unwrap();
        assert!(fenced.generation > old.generation);
        assert_eq!(fenced.first_sequence, new.sequence);
        assert!(fenced.claim_token.is_none());
        assert_eq!(fenced.due_at, old.retry_at);
        queue::ack(
            &store
                .with_maintenance_reads_and_critical_writes()
                .connection,
            &old,
            new.sequence,
            &|| NOW,
        )
        .await
        .unwrap();
        assert_eq!(
            store.get_task_event_fanout_cursor(&task.id).await.unwrap(),
            None
        );
        let restored = claim(&store, &task.id, "restored", old.retry_at).await;
        assert_eq!(
            store.get_task_event_fanout_cursor(&task.id).await.unwrap(),
            Some(0)
        );
        assert_eq!(
            row(&store, &task.id).await.unwrap().claim_token.as_deref(),
            Some("restored")
        );
        store
            .advance_task_event_fanout_cursor(&task.id, new.sequence - 1)
            .await
            .unwrap();
        cursor::Entity::update_many()
            .col_expr(cursor::Column::LastSequence, Expr::val(0_i64))
            .filter(cursor::Column::TaskId.eq(task.id.clone()))
            .exec(&store.connection)
            .await
            .unwrap();
        let reset = row(&store, &task.id).await.unwrap();
        assert_eq!(reset.first_sequence, new.sequence);
        assert!(reset.generation > restored.generation);
        assert!(reset.claim_token.is_none());
        queue::release(
            &store.with_maintenance_access().connection,
            &restored,
            TaskEventFanoutOutcome::Failed,
            &|| NOW,
        )
        .await
        .unwrap();
        queue::release(
            &store.with_maintenance_access().connection,
            &restored,
            TaskEventFanoutOutcome::BudgetDeferred,
            &|| NOW,
        )
        .await
        .unwrap();
        assert_eq!(row(&store, &task.id).await.unwrap(), reset);
        assert!(
            !queue::renew(
                &store.with_maintenance_access().connection,
                &restored,
                &|| NOW
            )
            .await
            .unwrap()
        );
        pioneer_entity::task::Entity::delete_by_id(task.id.clone())
            .exec(&store.connection)
            .await
            .unwrap();
        assert!(row(&store, &task.id).await.is_none());
        assert_eq!(
            store.get_task_event_fanout_cursor(&task.id).await.unwrap(),
            None
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn discovery_and_pages_are_bounded_with_oversized_supported_separately() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = store(fixture).await;
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
        Ok(())
    })
    .await
    .unwrap();
}

async fn old_history(store: &TaskEventStore<NoFrozenOutput>, id: &str) -> Task {
    let task = created(store, id).await;
    for n in 2..=100 {
        progress(store, id, &format!("old {n}")).await;
    }
    store
        .advance_task_event_fanout_cursor(id, 100)
        .await
        .unwrap();
    task
}
#[tokio::test]
async fn late_cursor_creation_retains_first_tracked_event_and_claim_ownership() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = store(fixture).await;
        let task = created(&store, "late-cursor").await;
        progress(&store, &task.id, "second").await;
        cursor::Entity::delete_by_id(task.id.clone())
            .exec(&store.connection)
            .await
            .unwrap();
        let holder = claim(&store, &task.id, "late", NOW).await;
        assert_eq!(holder.first_sequence, 1);
        assert_eq!(
            store.get_task_event_fanout_cursor(&task.id).await.unwrap(),
            Some(0)
        );
        assert_eq!(
            row(&store, &task.id).await.unwrap().claim_token.as_deref(),
            Some("late")
        );
        assert!(
            queue::renew(
                &store.with_maintenance_access().connection,
                &holder,
                &|| NOW
            )
            .await
            .unwrap()
        );
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
        assert_eq!(row(&store, &task.id).await.unwrap().first_sequence, 1);
        queue::ack(
            &store
                .with_maintenance_reads_and_critical_writes()
                .connection,
            &holder,
            2,
            &|| NOW,
        )
        .await
        .unwrap();
        assert!(row(&store, &task.id).await.is_none());
        Ok(())
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn generation_overflow_rolls_back_new_event_and_tracking() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = store(fixture).await;
        let task = created(&store, "fanout_overflow").await;
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
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn point_context_selects_exact_event_run_trigger_and_preserves_latest_anchor() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = store(fixture).await;
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
            .context()
            .get_task_event_context(&payload)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(context.run.as_ref().unwrap().id, run.id);
        assert!(context.run_uses_creation_anchor(&run.id));
        assert!(
            store
                .context()
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
        let expected = store
            .context()
            .get_task_run(&run.id)
            .await
            .unwrap()
            .unwrap();
        for payload in payloads {
            let exact = store
                .context()
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
            .context()
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
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn failed_reservation_defers_exact_snapshot_including_null_token() {
    pioneer_postgres_test::with_database(async |fixture| {
    let store = store(fixture).await;
    let task = created(&store, "claim_failure").await;
    let before = row(&store, &task.id).await.unwrap();
    assert!(before.claim_token.is_none());
    store.connection.execute_unprepared("CREATE FUNCTION fanout_reject_token() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture'; END; $$; CREATE TRIGGER fanout_reject_token BEFORE UPDATE OF claim_token ON task_event_fanout_pending FOR EACH ROW WHEN (NEW.claim_token IS NOT NULL) EXECUTE FUNCTION fanout_reject_token()").await.unwrap();
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
        .execute_unprepared("DROP TRIGGER fanout_reject_token ON task_event_fanout_pending")
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
        Ok(())
    }).await.unwrap();
}

#[tokio::test]
async fn malformed_event_page_keeps_good_prefix_and_does_not_discard_poison() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = store(fixture).await;
        let task = created(&store, "fanout_poison").await;
        let bad = progress(&store, &task.id, "bad").await;
        let good = progress(&store, &task.id, "later").await;
        task_event::Entity::update_many()
            .col_expr(
                task_event::Column::PayloadJsonSha256,
                Expr::val(vec![0_u8; 32]),
            )
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
        Ok(())
    })
    .await
    .unwrap();
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
async fn observed_store(
    fixture: &pioneer_postgres_test::TestDatabase,
    routes: Arc<Routes>,
) -> TaskEventStore<NoFrozenOutput> {
    let writer = fixture.connect_with_limit(1).await.unwrap();
    let reader = fixture.connect_read_only().await.unwrap();
    let executor = pioneer_sqlite::SqliteWriteExecutor::with_observer(writer, routes.clone());
    task_event_store(
        pioneer_sqlite::SqliteDatabase::from_executor_with_read_observer(reader, executor, routes),
    )
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn maintenance_discovery_claims_context_and_cancellation_use_physical_routes() {
    pioneer_postgres_test::with_database(async |fixture| {
    use pioneer_sqlite::{SqliteReadClass, SqliteWriteClass, SqliteWriteEvent};
    store(fixture).await.connection.close().await.unwrap();
    let routes = Arc::new(Routes::default());
    let store = observed_store(fixture, routes.clone()).await;
    let task = created(&store, "route_task").await;
    routes.reads.lock().unwrap().clear();
    routes.writes.lock().unwrap().clear();
    let candidates = store.due_task_event_fanout(NOW, 64).await.unwrap();
    assert_eq!(candidates.len(), 1);
    assert!(
        store
            .with_maintenance_access()
            .context().get_task_event_context(&TaskEventPayload::TaskCreated { task: task.clone() })
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
        store.context().get_task_record(&task.id).await.unwrap().is_some(),
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

        Ok(())
    }).await.unwrap();
}

#[tokio::test]
async fn delivery_context_preserves_latest_window_without_attempts_or_payloads() {
    pioneer_postgres_test::with_database(async |fixture| {
        use pioneer_entity::task_delivery as delivery;
        let store = store(fixture).await;
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
                created_at: Set(unix_to_datetime(1_700_000_000 + n)),
                updated_at: Set(unix_to_datetime(1_700_000_000 + n)),
                ..Default::default()
            })
            .exec(&store.connection)
            .await
            .unwrap();
        }
        assert!(
            !store
                .context()
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
                Expr::value(unix_to_datetime(1_700_001_000)),
            )
            .filter(delivery::Column::Id.eq("delivery-0"))
            .exec(&store.connection)
            .await
            .unwrap();
        assert!(
            store
                .context()
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
                .context()
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
                .context()
                .task_run_has_pending_thread_delivery(
                    &task.workspace_id,
                    &task.id,
                    "delivery-run",
                    Some("occurrence")
                )
                .await
                .unwrap()
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn utf8_metadata_does_not_fetch_payload_before_admission() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = store(fixture).await;
        let task = created(&store, "metadata-bytecode").await;
        let small_payload = TaskEventPayload::Progress {
            task_id: task.id.clone(),
            run_id: None,
            message: "界🙂".into(),
            details: None,
        };
        let small = progress(&store, &task.id, "界🙂").await;
        let small_json = serde_json::to_string(&small_payload).unwrap();
        let expected = small_json.len();
        assert!(expected > small_json.chars().count());
        let mut originals = vec![(small.id.clone(), small_json)];
        let large_message = "界".repeat(BYTES);
        let large_json = serde_json::to_string(&TaskEventPayload::Progress {
            task_id: task.id.clone(),
            run_id: None,
            message: large_message.clone(),
            details: None,
        })
        .unwrap();
        for _ in 0..3 {
            let event = progress(&store, &task.id, &large_message).await;
            originals.push((event.id, large_json.clone()));
        }
        for (index, (id, original)) in originals.iter().enumerate() {
            let mut stored = task_event::Entity::find_by_id(id.clone())
                .one(&store.connection)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(stored.payload_json_bytes, original.len() as i64);
            assert_eq!(stored.payload_json_chars, original.chars().count() as i64);
            assert_eq!(
                stored.payload_json_sha256,
                Sha256::digest(original.as_bytes()).to_vec()
            );
            if index != 0 {
                assert_eq!(
                    stored.payload_json_codec, 1,
                    "oversized fixture must actually compress"
                );
            }
            assert_eq!(
                pioneer_crud_tasks::event_log::EventPayload::read(&mut stored).unwrap(),
                *original,
            );
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
        assert_eq!(events[0].as_ref().unwrap().payload, small_payload);
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
        assert_eq!(
            bytes,
            originals[1].1.len(),
            "admission charges original bytes"
        );
        assert_eq!(events[0].as_ref().unwrap().id, originals[1].0);
        assert_eq!(
            serde_json::to_string(&events[0].as_ref().unwrap().payload).unwrap(),
            originals[1].1
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn budget_deferral_fences_new_holders_resets_and_keeps_appended_work() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = store(fixture).await;
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
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn floor_survives_physical_restart_before_claim_after_claim_partial_ack_and_error() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = store(fixture).await;
        old_history(&store, "restart-old-task").await;
        progress(&store, "restart-old-task", "101").await;
        progress(&store, "restart-old-task", "102").await;
        let mut store = store;
        let mut now = NOW;
        for phase in 0..4 {
            assert_eq!(
                row(&store, "restart-old-task")
                    .await
                    .unwrap()
                    .first_sequence,
                101
            );
            if phase > 0 {
                let holder =
                    claim(&store, "restart-old-task", &format!("phase-{phase}"), now).await;
                if phase == 2 {
                    queue::ack(
                        &store
                            .with_maintenance_reads_and_critical_writes()
                            .connection,
                        &holder,
                        101,
                        &|| now,
                    )
                    .await
                    .unwrap();
                    queue::release(
                        &store.with_maintenance_access().connection,
                        &holder,
                        TaskEventFanoutOutcome::Delivered,
                        &|| now,
                    )
                    .await
                    .unwrap();
                } else if phase == 3 {
                    queue::release(
                        &store.with_maintenance_access().connection,
                        &holder,
                        TaskEventFanoutOutcome::Failed,
                        &|| now,
                    )
                    .await
                    .unwrap();
                }
                now = row(&store, "restart-old-task").await.unwrap().due_at;
            }
            let before = row(&store, "restart-old-task").await.unwrap();
            store.connection.clone().close().await.unwrap();
            drop(store);
            store = observed_store(fixture, Arc::new(Routes::default())).await;
            assert_eq!(row(&store, "restart-old-task").await.unwrap(), before);
            let after = store
                .get_task_event_fanout_cursor("restart-old-task")
                .await
                .unwrap()
                .unwrap()
                .max(before.first_sequence - 1);
            let TaskEventFanoutPage::Prefix { events: page, .. } = store
                .task_event_fanout_page("restart-old-task", after, 128, &mut { BYTES }, true)
                .await
                .unwrap()
            else {
                panic!("new prefix expected")
            };
            assert_eq!(
                page.iter()
                    .map(|e| e.as_ref().unwrap().sequence)
                    .collect::<Vec<_>>(),
                if phase >= 2 {
                    vec![102]
                } else {
                    vec![101, 102]
                }
            );
        }
        store.connection.clone().close().await.unwrap();
        drop(store);

        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn late_budget_result_cannot_undo_prefix_progress_or_real_error_bookkeeping() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = store(fixture).await;
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
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn nonexecuting_trigger_reconciliation_wakes_fanout_and_noop_stays_parked() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = store(fixture).await;
        store
            .connection
            .execute_unprepared(
                "UPDATE workspace SET is_current = FALSE WHERE is_current;
                 INSERT INTO workspace (id,name,is_active,is_current)
                 VALUES ('ws_trigger_custom_publisher','Scheduler fixture',TRUE,TRUE)",
            )
            .await?;
        let now = 1_700_000_000;
        let mut task = sample_task(now);
        task.workspace_id = "ws_trigger_custom_publisher".into();
        task.status = TaskStatus::Blocked;
        let trigger = sample_task_trigger(now);
        store
            .append_task_events(
                vec![
                    TaskEventPayload::TaskCreated { task: task.clone() },
                    TaskEventPayload::TriggerCreated {
                        trigger: trigger.clone(),
                    },
                ],
                now,
            )
            .await
            .unwrap();
        drain_custom_publisher_fanout(&store).await;
        let database = store.connection.clone();
        let mut fanout = database.work_signal("tasks.fanout").subscribe();
        fanout.begin_pass(&database).await.unwrap();
        assert!(futures_util::FutureExt::now_or_never(fanout.wait(None)).is_none());
        // A stale expectation is an ordinary no-op, not an unknown write.
        assert!(
            store
                .reconcile_due_trigger_for_nonexecuting_task(&trigger.id, now, now + 3600)
                .await
                .unwrap()
                .is_none()
        );
        assert!(futures_util::FutureExt::now_or_never(fanout.wait(None)).is_none());
        let outcome = store
            .reconcile_due_trigger_for_nonexecuting_task(&trigger.id, now + 3600, now + 3600)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(outcome.trigger_status, TaskTriggerStatus::Paused);
        assert!(futures_util::FutureExt::now_or_never(fanout.wait(None)).is_some());
        let delivered = drain_custom_publisher_fanout(&store).await;
        assert_eq!(delivered.len(), 1);
        assert!(
            matches!(&delivered[0].payload, TaskEventPayload::TaskRescheduled { trigger, .. }
            if trigger.status == TaskTriggerStatus::Paused && trigger.next_fire_at.is_none())
        );
        fanout.begin_pass(&database).await.unwrap();
        assert!(
            store
                .reconcile_due_trigger_for_nonexecuting_task(&trigger.id, now + 3600, now + 3601)
                .await
                .unwrap()
                .is_none()
        );
        assert!(futures_util::FutureExt::now_or_never(fanout.wait(None)).is_none());
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn repeated_run_started_with_new_timestamp_is_noop_after_run_is_running() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = store(fixture).await;
        let timestamp = 1_700_000_000;
        let task = sample_task(timestamp);
        let run = sample_task_run(timestamp);
        store
            .append_task_events(
                vec![
                    TaskEventPayload::TaskCreated { task: task.clone() },
                    TaskEventPayload::RunCreated {
                        run: run.clone(),
                        agent_spec: None,
                    },
                ],
                timestamp,
            )
            .await
            .expect("task and run should persist");

        assert!(
            store
                .append_task_run_started_once(task.id.clone(), run.id.clone(), timestamp + 1,)
                .await
                .expect("first start should succeed")
                .is_some()
        );
        assert!(
            store
                .append_task_run_started_once(task.id.clone(), run.id.clone(), timestamp + 2)
                .await
                .expect("recovery start after the run is already running should be a no-op")
                .is_none()
        );

        let persisted = store
            .context()
            .get_task_run(run.id.as_str())
            .await
            .expect("run should load")
            .expect("run should exist");
        assert_eq!(persisted.status, TaskRunStatus::Running);
        assert_eq!(persisted.started_at, Some(timestamp + 1));
        let run_started_events = store
            .get_task_events(task.id.as_str(), None)
            .await
            .expect("task events should list")
            .events
            .into_iter()
            .filter(|event| matches!(event.payload, TaskEventPayload::RunStarted { .. }))
            .count();
        assert_eq!(run_started_events, 1);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn dispatch_claim_commits_starting_activity_event_once_and_rolls_back_together() {
    pioneer_postgres_test::with_database(async |fixture| {
    let store = store(fixture).await;
    let task = created(&store, "activity-task").await;
    let mut run = sample_task_run(task.created_at);
    run.task_id = task.id.clone();
    run.trigger_id = None;
    store
        .append_task_event(
            TaskEventPayload::RunCreated {
                run: run.clone(),
                agent_spec: None,
            },
            task.created_at,
        )
        .await
        .unwrap();
    // Inject failure into the durable event write: the preceding Starting claim
    // must roll back with it, retaining a claimable Queued run.
    store
        .connection
        .execute_unprepared(
            "CREATE FUNCTION reject_starting() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected activity event failure'; END; $$;
         CREATE TRIGGER reject_starting BEFORE INSERT ON task_event FOR EACH ROW
         WHEN (NEW.event_type='task/tree/changed' AND NEW.idempotency_key LIKE 'run:%:starting') EXECUTE FUNCTION reject_starting()",
        )
        .await
        .unwrap();
    assert!(
        store
            .claim_task_run_for_dispatch_with_event(&run.id, task.created_at + 1)
            .await
            .is_err()
    );
    assert_eq!(
        store.context().get_task_run(&run.id).await.unwrap().unwrap().status,
        TaskRunStatus::Queued
    );
    store
        .connection
        .execute_unprepared("DROP TRIGGER reject_starting ON task_event")
        .await
        .unwrap();
    let (claimed, event) = store
        .claim_task_run_for_dispatch_with_event(&run.id, task.created_at + 2)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed.status, TaskRunStatus::Starting);
    assert!(matches!(
        event.payload,
        TaskEventPayload::RunStarting { .. }
    ));
    assert_eq!(
        event.workspace_id.as_deref(),
        Some(task.workspace_id.as_str())
    );
    assert_eq!(
        row(&store, &task.id).await.unwrap().newest_sequence,
        event.sequence
    );
    assert!(
        store
            .claim_task_run_for_dispatch_with_event(&run.id, task.created_at + 3)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        row(&store, &task.id).await.unwrap().newest_sequence,
        event.sequence
    );
        Ok(())
    }).await.unwrap();
}

#[tokio::test]
async fn list_tasks_created_by_turn_is_exact_and_unbounded_by_guard_page_size() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = store(fixture).await;
        store
            .connection
            .execute_unprepared(
                "INSERT INTO workspace(id,name,is_active,is_current)
                 VALUES ('ws_creator_turn_query','Task creator fixture',true,false)",
            )
            .await?;
        let thread_id = "thread_creator_turn_query";
        let turn_id = "turn_creator_turn_query";
        let other_turn_id = "turn_creator_turn_other";
        let timestamp = 1_700_020_000;
        let mut events = Vec::with_capacity(503);
        for index in 0..501 {
            let mut task = sample_task(timestamp + index);
            task.id = format!("task_creator_query_{index:03}");
            task.workspace_id = "ws_creator_turn_query".to_owned();
            task.created_by_thread_id = Some(thread_id.to_owned());
            task.created_by_turn_id = Some(turn_id.to_owned());
            events.push(TaskEventPayload::TaskCreated { task });
        }
        for index in 0..2 {
            let mut task = sample_task(timestamp + 501 + index);
            task.id = format!("task_creator_other_{index:03}");
            task.workspace_id = "ws_creator_turn_query".to_owned();
            task.created_by_thread_id = Some(thread_id.to_owned());
            task.created_by_turn_id = Some(other_turn_id.to_owned());
            events.push(TaskEventPayload::TaskCreated { task });
        }
        store
            .append_task_events(events, timestamp)
            .await
            .expect("creator-turn task events should project");

        let tasks = crate::view_store::TaskViewStore::new(&store.connection)
            .list_tasks_created_by_turn("ws_creator_turn_query", thread_id, turn_id)
            .await
            .expect("creator-turn query should succeed");
        assert_eq!(tasks.len(), 501);
        assert!(
            tasks
                .iter()
                .all(|task| task.created_by_turn_id.as_deref() == Some(turn_id))
        );
        assert!(tasks.iter().any(|task| task.id == "task_creator_query_500"));
        assert!(
            !tasks
                .iter()
                .any(|task| task.created_by_turn_id.as_deref() == Some(other_turn_id))
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn task_event_fanout_cursor_is_durable_and_monotonic() {
    pioneer_postgres_test::with_database(async |fixture| {
            let store = store(fixture).await;
            let timestamp = 1_700_000_000;
            let task = sample_task(timestamp);
            let created = store
                .append_task_event(
                    TaskEventPayload::TaskCreated { task: task.clone() },
                    timestamp,
                )
                .await
                .expect("task created event should append");
            assert_eq!(
                store
                    .get_task_event_fanout_cursor(task.id.as_str())
                    .await
                    .expect("new task cursor should load"),
                Some(0),
                "the event transaction must establish a zero cursor before its first wake"
            );
            assert_eq!(
                store
                    .due_task_event_fanout(i64::MAX, 64)
                    .await
                    .expect("durable fanout backlog should list")
                    .into_iter()
                    .map(|row| row.task_id)
                    .collect::<Vec<_>>(),
                vec![task.id.clone()],
                "an unacknowledged terminal/progress event must remain discoverable without a wake"
            );

            store
                .advance_task_event_fanout_cursor(task.id.as_str(), created.sequence)
                .await
                .expect("fanout cursor should persist");
            assert_eq!(
                store
                    .get_task_event_fanout_cursor(task.id.as_str())
                    .await
                    .expect("persisted cursor should load"),
                Some(created.sequence)
            );
            assert!(
                store
                    .due_task_event_fanout(i64::MAX, 64)
                    .await
                    .expect("acknowledged fanout backlog should list")
                    .is_empty(),
                "advancing the durable cursor must remove the task from replay backlog"
            );

            store
                .advance_task_event_fanout_cursor(task.id.as_str(), 0)
                .await
                .expect("a stale writer should be harmless");
            assert_eq!(
                store
                    .get_task_event_fanout_cursor(task.id.as_str())
                    .await
                    .expect("cursor should remain monotonic"),
                Some(created.sequence),
                "an older concurrent writer must not move the durable cursor backwards"
            );

            pioneer_entity::task_event_fanout_cursor::Entity::delete_by_id(task.id.clone())
                .exec(&store.connection)
                .await
                .expect("legacy cursor fixture should delete");
            let mut updated_task = task.clone();
            updated_task.title = "Updated after cursor migration".to_owned();
            updated_task.updated_at = timestamp + 1;
            let updated = store
                .append_task_event(
                    TaskEventPayload::TaskUpdated {
                        task: updated_task,
                        trigger: None,
                        agent_spec: None,
                        changed_fields: vec!["title".to_owned()],
                        updated_at: timestamp + 1,
                    },
                    timestamp + 1,
                )
                .await
                .expect("a new event for a legacy task should append");
            assert_eq!(
                store
                    .get_task_event_fanout_cursor(task.id.as_str())
                    .await
                    .expect("legacy task cursor should initialize atomically"),
                Some(0),
                "the first post-upgrade event must remain pending for fanout while older events stay skipped"
            );
            store
                .advance_task_event_fanout_cursor(task.id.as_str(), updated.sequence)
                .await
                .expect("new legacy task event should be acknowledged");
            Ok(())
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn task_events_append_project_and_read_back_lifecycle_state() {
    pioneer_postgres_test::with_database(async |fixture| {
        let connection = fixture.connection().clone();

        let store = store(fixture).await;
        let timestamp = 1_700_000_000;
        let task = sample_task(timestamp);
        let trigger = sample_task_trigger(timestamp);
        let run = sample_task_run(timestamp);
        let agent_spec = sample_task_agent_spec(timestamp);

        let created = store
            .append_task_event(
                TaskEventPayload::TaskCreated { task: task.clone() },
                timestamp,
            )
            .await
            .expect("task created event should append");
        assert_eq!(created.sequence, 1);

        let scheduled = store
            .append_task_event(
                TaskEventPayload::TriggerCreated {
                    trigger: trigger.clone(),
                },
                timestamp + 1,
            )
            .await
            .expect("trigger event should append");
        assert_eq!(scheduled.sequence, 2);

        let run_created = store
            .append_task_event(
                TaskEventPayload::RunCreated {
                    run: run.clone(),
                    agent_spec: Some(agent_spec),
                },
                timestamp + 2,
            )
            .await
            .expect("run created event should append");
        assert_eq!(run_created.sequence, 3);

        store
            .append_task_event(
                TaskEventPayload::RunStarted {
                    task_id: task.id.clone(),
                    run_id: run.id.clone(),
                    started_at: timestamp + 3,
                },
                timestamp + 3,
            )
            .await
            .expect("run started event should append");

        store
            .append_task_event(
                TaskEventPayload::RunCompleted {
                    task_id: task.id.clone(),
                    run_id: run.id.clone(),
                    result: Some(TaskResult {
                        summary: Some("Clear".to_owned()),
                        data: None,
                        artifacts: Vec::new(),
                        completed_by_run_id: Some(run.id.clone()),
                    }),
                    completed_at: timestamp + 4,
                },
                timestamp + 4,
            )
            .await
            .expect("run completed event should append");

        let completed = store
            .append_task_event(
                TaskEventPayload::TaskCompleted {
                    task_id: task.id.clone(),
                    result: Some(TaskResult {
                        summary: Some("Clear".to_owned()),
                        data: None,
                        artifacts: Vec::new(),
                        completed_by_run_id: Some(run.id.clone()),
                    }),
                    completed_at: timestamp + 5,
                },
                timestamp + 5,
            )
            .await
            .expect("task completed event should append");
        assert_eq!(completed.sequence, 6);

        let response = crate::view_store::TaskViewStore::new(&store.connection)
            .get_task(task.id.as_str())
            .await
            .expect("task read should succeed")
            .expect("task should exist");
        assert_eq!(response.task.status, TaskStatus::Completed);
        assert_eq!(
            response
                .task
                .result
                .as_ref()
                .and_then(|result| result.summary.as_deref()),
            Some("Clear")
        );
        assert_eq!(response.triggers.len(), 1);
        assert_eq!(response.runs.len(), 1);
        assert_eq!(response.runs[0].status, TaskRunStatus::Succeeded);
        assert_eq!(response.agent_specs.len(), 1);

        let events = store
            .get_task_events(task.id.as_str(), None)
            .await
            .expect("task events read should succeed");
        assert_eq!(events.last_sequence, 6);
        assert_eq!(events.events.len(), 6);

        let duplicate = connection
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "INSERT INTO task_event
                    (id, task_id, sequence, event_type, payload_json, payload_json_codec,
                     payload_json_bytes, payload_json_chars, payload_json_sha256)
                 SELECT 'evt_duplicate_sequence', task_id, sequence, event_type, payload_json,
                        payload_json_codec, payload_json_bytes, payload_json_chars, payload_json_sha256
                 FROM task_event WHERE id = $1",
                [created.id.into()],
            ))
            .await
            .expect_err("duplicate (task_id, sequence) must be rejected");
        assert!(matches!(
            duplicate.sql_err(),
            Some(sea_orm::SqlErr::UniqueConstraintViolation(_))
        ));

        let duplicate_run_number = connection
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "INSERT INTO task_run
                    (id, task_id, run_group_id, run_number, status, executor_kind)
                 VALUES ('run_duplicate_number', $1, 'run_duplicate_number', $2, 'queued', 'agent')",
                [task.id.into(), run.run_number.into()],
            ))
            .await
            .expect_err("duplicate (task_id, run_number) must be rejected");
        assert!(matches!(
            duplicate_run_number.sql_err(),
            Some(sea_orm::SqlErr::UniqueConstraintViolation(_))
        ));
        Ok(())
    })
    .await
    .unwrap();
}
