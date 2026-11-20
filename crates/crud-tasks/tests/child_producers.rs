use anyhow::{Context, Result};
use pioneer_crud_tasks::{child_work, task_child_producers};
use pioneer_entity::{
    task, task_child_reconcile_pending as point, task_child_reconcile_scope as scope,
};
use pioneer_postgres_test::with_database;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DbBackend, EntityTrait, QueryFilter, Select, Statement,
    TransactionTrait,
};
use std::time::Duration;

fn child_scope(kind: &str, key: &str) -> Select<scope::Entity> {
    scope::Entity::find()
        .filter(scope::Column::ScopeKind.eq(kind))
        .filter(scope::Column::ScopeKey.eq(key))
}

#[tokio::test]
async fn child_task_obligation_rolls_back_with_business_projection() -> Result<()> {
    with_database(async |database| {
        let db = database.connection();
        let id = "child-obligation";
        assert!(scope::Entity::find().one(db).await?.is_none());

        let write = db.begin().await?;
        write.execute_unprepared(
            "INSERT INTO task (id, workspace_id, owner_kind, executor_kind, status, title, goal)
             VALUES ('child-obligation', '000000000000000000000', 'user', 'agent', 'draft', 'Title', 'Goal')",
        ).await?;
        let task = task::Entity::find_by_id(id)
            .one(&write).await?.context("task prep did not insert")?;
        task_child_producers::task_change(&write, None, Some(&task)).await?;
        assert!(child_scope("task", id).one(&write).await?.is_some());
        let observer = database.connect().await?;
        assert!(child_scope("task", id).one(&observer).await?.is_none());
        write.rollback().await?;
        assert!(task::Entity::find_by_id(id).one(&observer).await?.is_none());
        assert!(child_scope("task", id).one(&observer).await?.is_none());
        observer.close().await?;
        Ok(())
    }).await
}

#[tokio::test]
async fn claimed_scope_keeps_its_cursor_and_lease_during_publication() -> Result<()> {
    with_database(async |database| {
        let db = database.connection();
        child_work::enqueue_scope(db, "run", "run", 100).await?;
        let claimant = db.begin().await?;
        claimant
            .execute_unprepared(
                "UPDATE task_child_reconcile_scope
             SET claim_token = 'claim', next_attempt_at = 160, pass_revision = requested_revision,
                 cursor = 'cursor', upper_watermark = 'tail'
             WHERE scope_kind = 'run' AND scope_key = 'run'",
            )
            .await?;
        let key = ("run".to_owned(), "run".to_owned());
        let before = scope::Entity::find_by_id(key.clone())
            .one(&claimant)
            .await?
            .context("claimed scope must exist")?;
        let second = database.connect().await?;
        let producer = second.begin().await?;
        let producer_pid: i32 = producer
            .query_one_raw(Statement::from_string(
                DbBackend::Postgres,
                "SELECT pg_backend_pid() AS pid",
            ))
            .await?
            .context("producer PID")?
            .try_get("", "pid")?;
        let publish = tokio::spawn(async move {
            child_work::enqueue_scope(&producer, "run", "run", 101).await?;
            producer.commit().await?;
            Ok::<_, anyhow::Error>(())
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let blocked: bool = db
                    .query_one_raw(Statement::from_sql_and_values(
                        DbBackend::Postgres,
                        "SELECT cardinality(pg_blocking_pids($1)) > 0 AS blocked",
                        [producer_pid.into()],
                    ))
                    .await?
                    .context("producer lock state")?
                    .try_get("", "blocked")?;
                if blocked {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await??;
        claimant.commit().await?;
        publish.await??;
        let current = scope::Entity::find_by_id(key)
            .one(db)
            .await?
            .context("published scope must exist")?;
        assert_ne!(current.requested_revision, before.requested_revision);
        assert_eq!(current.incarnation_token, before.incarnation_token);
        assert_eq!(current.claim_token, before.claim_token);
        assert_eq!(current.pass_revision, before.pass_revision);
        assert_eq!(current.cursor, before.cursor);
        assert_eq!(current.upper_watermark, before.upper_watermark);
        assert_eq!(current.next_attempt_at, before.next_attempt_at);
        second.close().await?;
        Ok(())
    })
    .await
}

#[tokio::test]
async fn point_replacement_publishes_new_locator_and_clears_old_claim() -> Result<()> {
    with_database(async |database| {
        let db = database.connection();
        child_work::enqueue_point(db, "z-old-thread", "turn", 100).await?;
        db.execute_unprepared(
            "UPDATE task_child_reconcile_pending
             SET claim_token = 'old-claim', attempt_count = 2 WHERE turn_id = 'turn'",
        )
        .await?;
        let previous = point::Entity::find_by_id("turn")
            .one(db)
            .await?
            .context("old point must exist")?;

        let producer = database.connect().await?;
        let transaction = producer.begin().await?;
        child_work::enqueue_point(&transaction, "a-new-thread", "turn", 101).await?;
        assert_eq!(
            point::Entity::find_by_id("turn").one(db).await?,
            Some(previous.clone())
        );
        transaction.commit().await?;

        let replacement = point::Entity::find_by_id("turn")
            .one(db)
            .await?
            .context("new point must exist")?;
        assert_eq!(replacement.thread_id, "a-new-thread");
        assert_ne!(replacement.revision, previous.revision);
        assert_eq!(replacement.next_attempt_at, Some(101));
        assert_eq!(replacement.attempt_count, 0);
        assert!(replacement.claim_token.is_none());
        producer.close().await?;
        Ok(())
    })
    .await
}
