use anyhow::{Context, Result};
use pioneer_crud_tasks::dispatch::{has_dispatch_capacity, lock_task_for_dispatch};
use pioneer_entity::task_run;
use pioneer_postgres_test::with_database;
use sea_orm::{ConnectionTrait, DbBackend, EntityTrait, Statement, TransactionTrait};

#[tokio::test]
async fn task_dispatch_rechecks_capacity_after_competing_commit_and_rollback() -> Result<()> {
    with_database(async |fixture| {
        let db = fixture.connection();
        db.execute_unprepared(
            "INSERT INTO workspace (id, name) VALUES ('workspace', 'Workspace');
             INSERT INTO task (id, workspace_id, owner_kind, executor_kind, status, title, goal)
             VALUES ('task', 'workspace', 'workspace', 'tool', 'running', 'Task', '');
             INSERT INTO task_run (id, task_id, run_group_id, run_number, status, executor_kind)
             VALUES ('first', 'task', 'first', 1, 'queued', 'tool'),
                    ('second', 'task', 'second', 2, 'queued', 'tool');",
        )
        .await?;
        let left = fixture.connect().await?;
        let right = fixture.connect().await?;
        let now = chrono::Utc::now().fixed_offset();

        for commit in [false, true] {
            let first = left.begin().await?;
            let second = right.begin().await?;
            let second_pid: i32 = second
                .query_one_raw(Statement::from_string(
                    DbBackend::Postgres,
                    "SELECT pg_backend_pid() AS pid",
                ))
                .await?
                .context("no test backend")?
                .try_get("", "pid")?;
            assert!(lock_task_for_dispatch(&first, "task").await?.is_some());
            let run = task_run::Entity::find_by_id("first")
                .one(&first)
                .await?
                .context("missing first run")?;
            assert!(has_dispatch_capacity(&first, &run, 1, now).await?);
            first
                .execute_unprepared("UPDATE task_run SET status='starting' WHERE id='first'")
                .await?;
            let other = tokio::spawn(async move {
                lock_task_for_dispatch(&second, "task").await?;
                let run = task_run::Entity::find_by_id("first")
                    .one(&second)
                    .await?
                    .context("missing first run")?;
                let capacity = has_dispatch_capacity(&second, &run, 1, now).await?;
                second.rollback().await?;
                Ok::<_, anyhow::Error>(capacity)
            });
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                loop {
                    let waiting: bool = db
                        .query_one_raw(Statement::from_sql_and_values(
                            DbBackend::Postgres,
                            "SELECT cardinality(pg_blocking_pids($1)) > 0 AS waiting",
                            [second_pid.into()],
                        ))
                        .await?
                        .context("blocking probe returned no result")?
                        .try_get("", "waiting")?;
                    if waiting {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
                Ok::<_, anyhow::Error>(())
            })
            .await??;
            if commit {
                first.commit().await?;
            } else {
                first.rollback().await?;
            }
            assert_eq!(other.await??, !commit);
        }
        left.close().await?;
        right.close().await?;
        Ok(())
    })
    .await
}
