use anyhow::{Context, Result};
use pioneer_crud_packages::plugins::{begin_mutation, find, finish_mutation};
use pioneer_postgres_test::with_database;
use sea_orm::{ConnectionTrait, DbBackend, Statement, TransactionTrait};
use std::time::Duration;

#[tokio::test]
async fn concurrent_package_mutations_cannot_reuse_the_same_revision() -> Result<()> {
    with_database(async |fixture| {
        let db = fixture.connection();
        db.execute_unprepared(r#"
            INSERT INTO workspace(id, name) VALUES ('w','Workspace');
            INSERT INTO package_installation
                (id, workspace_id, source_upload_id, package_path, data_path,
                 package_fingerprint, state, revision, created_at, updated_at)
            VALUES ('package','w','upload','package','data','fingerprint','installed',1,now(),now());
            INSERT INTO plugin_installation (id, name) VALUES ('package', 'Plugin');
        "#).await?;

        let tx = db.begin().await?;
        let pending = r#"{ "path": "проба" }"#;
        let first = begin_mutation(&tx, "w", "package", 1, "updating", true, pending.into(), None).await?;
        assert_eq!(first.revision, 2);
        assert_eq!(first.pending_json.as_deref(), Some(pending));

        let second = db.begin().await?;
        let pid: i32 = second.query_one_raw(Statement::from_string(
            DbBackend::Postgres, "SELECT pg_backend_pid() AS pid",
        )).await?.context("missing second backend PID")?.try_get("", "pid")?;
        let waiter = tokio::spawn(async move {
            let result = begin_mutation(&second, "w", "package", 1, "removing", false, "{}".into(), None).await;
            second.rollback().await?;
            Ok::<_, anyhow::Error>(result)
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let blocked: bool = db.query_one_raw(Statement::from_sql_and_values(
                    DbBackend::Postgres,
                    "SELECT cardinality(pg_blocking_pids($1)) > 0 AS blocked",
                    [pid.into()],
                )).await?.context("no lock observation")?.try_get("", "blocked")?;
                if blocked { return Ok::<_, anyhow::Error>(()); }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.context("second mutation did not wait for the package lock")??;
        tx.commit().await?;
        assert_eq!(waiter.await??.expect_err("stale mutation was admitted").to_string(), "plugins.stale");

        let tx = db.begin().await?;
        finish_mutation(&tx, "package", 2, "installed", None).await?;
        tx.commit().await?;
        let saved = find(db, "package").await?.context("package missing after commit")?;
        assert_eq!(saved.revision, 2);
        assert_eq!(saved.state, "installed");
        assert_eq!(saved.pending_json, None);
        Ok(())
    }).await
}
