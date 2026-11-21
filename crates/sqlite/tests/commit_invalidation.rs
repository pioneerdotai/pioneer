use anyhow::{Context, Result};
use pioneer_postgres_test::with_database;
use pioneer_sqlite::SqliteDatabase;
use sea_orm::{ConnectionTrait, DbBackend, Statement, TransactionTrait};
use std::time::Duration;

#[tokio::test]
async fn unknown_commit_is_visible_before_background_discovery() -> Result<()> {
    with_database(async |fixture| {
        let control = fixture.connection();
        control
            .execute_unprepared(
                "INSERT INTO workspace (id, name) VALUES ('commit-wake', 'before commit');
                 CREATE FUNCTION pause_commit() RETURNS trigger LANGUAGE plpgsql AS $trigger$
                 BEGIN
                     PERFORM pg_advisory_xact_lock(62812377);
                     RETURN NEW;
                 END
                 $trigger$;
                 CREATE CONSTRAINT TRIGGER pause_commit AFTER UPDATE ON workspace
                 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
                 WHEN (NEW.id = 'commit-wake') EXECUTE FUNCTION pause_commit()",
            )
            .await?;
        let gate = control.begin().await?;
        gate.execute_unprepared("SELECT pg_advisory_xact_lock(62812377)")
            .await?;

        let writer = fixture.connect_with_limit(2).await?;
        let scoped = SqliteDatabase::new(fixture.connect_read_only().await?, writer.clone());
        let change = scoped.work_signal("commit-wake");
        let mut wake = change.subscribe();
        wake.begin_pass(&scoped).await?;
        let (publish_pid, receive_pid) = tokio::sync::oneshot::channel();
        let task = tokio::spawn({
            let scoped = scoped.clone();
            async move {
                let change = change.changing();
                let write = scoped.begin().await?;
                let pid = write
                    .query_one_raw(Statement::from_string(
                        DbBackend::Postgres,
                        "SELECT pg_backend_pid() AS pid",
                    ))
                    .await?
                    .context("missing writer pid")?
                    .try_get::<i32>("", "pid")?;
                write
                    .execute_unprepared(
                        "UPDATE workspace SET name='after commit' WHERE id='commit-wake'",
                    )
                    .await?;
                publish_pid.send(pid).expect("commit observer");
                write.commit().await?;
                change.completed(true);
                Ok::<(), anyhow::Error>(())
            }
        });
        let pid = receive_pid.await?;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let blocked = control
                    .query_one_raw(Statement::from_sql_and_values(
                        DbBackend::Postgres,
                        "SELECT cardinality(pg_blocking_pids($1)) > 0 AS blocked",
                        [pid.into()],
                    ))
                    .await?
                    .context("missing writer lock state")?
                    .try_get::<bool>("", "blocked")?;
                if blocked {
                    return Ok::<(), anyhow::Error>(());
                }
            }
        })
        .await
        .context("COMMIT did not reach the deferred trigger")??;

        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        let next_pass = wake.begin_pass(&scoped);
        tokio::pin!(next_pass);
        assert!(
            tokio::time::timeout(Duration::from_millis(150), &mut next_pass)
                .await
                .is_err(),
            "discovery crossed an unacknowledged commit"
        );
        gate.commit().await?;
        tokio::time::timeout(Duration::from_secs(5), next_pass)
            .await
            .context("discovery did not resume after commit")??;
        let workspace = scoped
            .query_one_raw(Statement::from_string(
                DbBackend::Postgres,
                "SELECT name FROM workspace WHERE id='commit-wake'",
            ))
            .await?
            .context("missing committed workspace")?;
        assert_eq!(workspace.try_get::<String>("", "name")?, "after commit");
        Ok(())
    })
    .await
}
