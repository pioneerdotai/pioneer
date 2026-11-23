use anyhow::{Context, Result};
use pioneer_crud_tasks::occurrence_reconcile::{claim, discover, owns};
use pioneer_postgres_test::with_database;
use sea_orm::{ConnectionTrait, DbBackend, Statement, TransactionTrait};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

#[tokio::test]
async fn competing_claims_use_the_time_after_the_postgres_row_lock() -> Result<()> {
    with_database(async |fixture| {
        let db = fixture.connection();
        db.execute_unprepared(
            "INSERT INTO task_run_occurrence_reconcile_pending
             (run_id, generation, next_attempt_at) VALUES ('run', 1, 0)",
        )
        .await?;
        let candidate = discover(db, 100, 1)
            .await?
            .pop()
            .context("missing candidate")?;
        assert_eq!(candidate.run_id, "run");
        let blocker = db.begin().await?;
        blocker
            .execute_unprepared(
                "SELECT run_id FROM task_run_occurrence_reconcile_pending
                 WHERE run_id='run' FOR UPDATE",
            )
            .await?;

        let now = Arc::new(AtomicI64::new(100));
        let mut pids = Vec::new();
        let mut tasks = Vec::new();
        for token in ["left", "right"] {
            let session = fixture.connect_with_limit(1).await?;
            let pid = session
                .query_one_raw(Statement::from_string(
                    DbBackend::Postgres,
                    "SELECT pg_backend_pid() AS pid",
                ))
                .await?
                .context("missing session pid")?
                .try_get::<i32>("", "pid")?;
            pids.push(pid);
            let candidate = candidate.clone();
            let clock = now.clone();
            tasks.push(tokio::spawn(async move {
                claim(&session, &candidate, token.into(), &|| {
                    clock.load(Ordering::SeqCst)
                })
                .await
            }));
        }
        assert_ne!(pids[0], pids[1]);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let row = db
                    .query_one_raw(Statement::from_sql_and_values(
                        DbBackend::Postgres,
                        "SELECT cardinality(pg_blocking_pids($1)) > 0 AND
                            cardinality(pg_blocking_pids($2)) > 0 AS both_waiting",
                        pids.iter().copied().map(Into::into),
                    ))
                    .await?
                    .context("missing lock state")?;
                if row.try_get::<bool>("", "both_waiting")? {
                    return Ok::<(), anyhow::Error>(());
                }
            }
        })
        .await
        .context("both writers must reach the row lock")??;
        now.store(200, Ordering::SeqCst);
        blocker.commit().await?;

        let mut acquired = Vec::new();
        for task in tasks {
            if let Some(held) = task.await?.map_err(|failure| failure.error)? {
                acquired.push(held);
            }
        }
        assert_eq!(acquired.len(), 1);
        assert_eq!(acquired[0].attempt_count, 1);
        assert_eq!(acquired[0].next_attempt_at, 205);
        assert!(owns(db, &acquired[0]).await?);
        assert!(
            claim(db, &candidate, "too-early".into(), &|| 204)
                .await
                .map_err(|failure| failure.error)?
                .is_none()
        );

        let reopened = fixture.connect().await?;
        let next = claim(&reopened, &candidate, "new-owner".into(), &|| 205)
            .await
            .map_err(|failure| failure.error)?
            .context("due claim was not found")?;
        assert_eq!(next.attempt_count, 2);
        assert_eq!(next.next_attempt_at, 215);
        assert!(!owns(db, &acquired[0]).await?);
        assert!(owns(db, &next).await?);
        Ok(())
    })
    .await
}
