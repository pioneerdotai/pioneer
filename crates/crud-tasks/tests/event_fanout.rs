use anyhow::{Context, Result};
use pioneer_crud_tasks::event_fanout::TaskEventFanoutOutcome;
use pioneer_crud_tasks::{event_fanout as queue, event_log};
use pioneer_entity::{task_event, task_event_fanout_pending as pending};
use pioneer_postgres_test::with_database;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseConnection, EntityTrait, PaginatorTrait, QueryFilter,
    TransactionTrait,
};

#[tokio::test]
async fn append_preserves_the_fanout_holder_and_a_stale_error_cannot_overwrite_ack() -> Result<()> {
    with_database(async |fixture| {
        let publisher = fixture.connect().await?;
        let subscriber = fixture.connection();
        subscriber
            .execute_unprepared(
                "INSERT INTO workspace(id,name) VALUES ('workspace','Workspace');
             INSERT INTO task(id,workspace_id,owner_kind,executor_kind,status,title,goal)
                VALUES ('task','workspace','workspace','system','running','Task','Test')",
            )
            .await?;
        publish(&publisher, "event-1", 1).await?;
        let clock = || 4_000_000_000;
        let candidate = queue::due(subscriber, clock(), 1)
            .await?
            .pop()
            .context("initial event frontier")?;
        let claim = queue::claim(subscriber, &candidate, "holder".into(), &clock)
            .await?
            .context("reserve first event")?;
        publish(&publisher, "event-2", 2).await?;
        let current = pending::Entity::find_by_id("task")
            .one(subscriber)
            .await?
            .context("extended frontier")?;
        assert!(current.generation > claim.generation);
        assert_eq!(current.claim_token.as_deref(), Some("holder"));
        assert_eq!(current.first_sequence, claim.first_sequence);
        assert_eq!(current.due_at, claim.retry_at);
        assert!(queue::renew(subscriber, &claim, &|| claim.retry_at + 1).await?);
        assert_eq!(
            queue::next_deadline(subscriber).await?,
            Some(claim.retry_at + 6)
        );

        queue::ack(subscriber, &claim, 1, &clock).await?;
        assert_eq!(
            queue::find_fanout_cursor(subscriber, "task").await?,
            Some(1)
        );
        let acknowledged = pending::Entity::find_by_id("task")
            .one(subscriber)
            .await?
            .context("new pending event")?;
        assert_eq!(acknowledged.newest_sequence, 2);
        assert_eq!(acknowledged.attempts, 0);
        queue::release(subscriber, &claim, TaskEventFanoutOutcome::Failed, &clock).await?;
        assert_eq!(
            pending::Entity::find_by_id("task").one(subscriber).await?,
            Some(acknowledged)
        );
        queue::release(
            subscriber,
            &claim,
            TaskEventFanoutOutcome::Delivered,
            &clock,
        )
        .await?;
        let next = queue::due(subscriber, clock(), 1)
            .await?
            .pop()
            .context("next unacknowledged event")?;
        assert_eq!(next.first_sequence, claim.first_sequence);
        assert_eq!(next.newest_sequence, 2);
        assert!(next.claim_token.is_none());
        publisher.close().await?;
        Ok(())
    })
    .await
}

async fn publish(db: &DatabaseConnection, id: &str, sequence: i64) -> Result<()> {
    let tx = db.begin().await?;
    event_log::lock_task(&tx, "task").await?;
    assert_eq!(event_log::next_sequence(&tx, "task").await?, sequence);
    append(&tx, id, sequence).await?;
    tx.commit().await?;
    Ok(())
}

async fn append(db: &impl ConnectionTrait, id: &str, sequence: i64) -> Result<()> {
    let payload = event_log::EventPayload::prepare("{}")?;
    event_log::append(
        db,
        event_log::NewEvent {
            id,
            task_id: "task",
            run_id: None,
            thread_id: None,
            turn_id: None,
            event_type: "task/progress",
            idempotency_key: None,
            sequence,
            created_at: "2026-11-20T01:00:00Z".parse()?,
        },
        payload,
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn atomic_new_batch_keeps_first_sequence_and_rollback_removes_tracking() -> Result<()> {
    with_database(async |fixture| {
        let writer = fixture.connect().await?;
        let reader = fixture.connection();
        reader
            .execute_unprepared(
                "INSERT INTO workspace(id,name) VALUES ('workspace','Workspace');
                 INSERT INTO task(id,workspace_id,owner_kind,executor_kind,status,title,goal)
                 VALUES ('task','workspace','workspace','system','running','Task','Test')",
            )
            .await?;

        let history = writer.begin().await?;
        event_log::lock_task(&history, "task").await?;
        for sequence in 1..=100 {
            append(&history, &format!("history-{sequence}"), sequence).await?;
        }
        history.commit().await?;
        let clock = || 4_000_000_000;
        let candidate = queue::due(reader, clock(), 1)
            .await?
            .pop()
            .context("historical event frontier")?;
        let claim = queue::claim(reader, &candidate, "history".into(), &clock)
            .await?
            .context("claim historical events")?;
        queue::ack(reader, &claim, 100, &clock).await?;
        assert!(
            pending::Entity::find_by_id("task")
                .one(reader)
                .await?
                .is_none()
        );

        for commit in [false, true] {
            let batch = writer.begin().await?;
            event_log::lock_task(&batch, "task").await?;
            for sequence in [101, 102] {
                assert_eq!(event_log::next_sequence(&batch, "task").await?, sequence);
                append(&batch, &format!("batch-{sequence}"), sequence).await?;
            }
            let frontier = pending::Entity::find_by_id("task")
                .one(&batch)
                .await?
                .context("new event frontier in writer transaction")?;
            assert_eq!(
                (frontier.first_sequence, frontier.newest_sequence),
                (101, 102)
            );
            assert!(
                pending::Entity::find_by_id("task")
                    .one(reader)
                    .await?
                    .is_none()
            );

            if commit {
                batch.commit().await?;
            } else {
                batch.rollback().await?;
                assert!(
                    pending::Entity::find_by_id("task")
                        .one(reader)
                        .await?
                        .is_none()
                );
                assert_eq!(
                    task_event::Entity::find()
                        .filter(task_event::Column::TaskId.eq("task"))
                        .count(reader)
                        .await?,
                    100
                );
            }
        }
        let committed = pending::Entity::find_by_id("task")
            .one(reader)
            .await?
            .context("committed event frontier")?;
        assert_eq!(
            (committed.first_sequence, committed.newest_sequence),
            (101, 102)
        );
        assert_eq!(
            task_event::Entity::find()
                .filter(task_event::Column::TaskId.eq("task"))
                .count(reader)
                .await?,
            102
        );
        writer.close().await?;
        Ok(())
    })
    .await
}
