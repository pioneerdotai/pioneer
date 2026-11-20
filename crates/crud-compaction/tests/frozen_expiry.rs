use anyhow::{Context, Result};
use pioneer_crud_compaction::frozen_expiry::expire;
use pioneer_entity::compaction_frozen_history as history;
use pioneer_postgres_test::with_database;
use sea_orm::{
    ConnectionTrait, DatabaseTransaction, DbBackend, EntityTrait, Statement, TransactionTrait,
};
use std::time::Duration;

const ROOT: &str = "INSERT INTO turn_runtime_snapshot
    (turn_id,thread_id,workspace_id,mode,model,provider_name,hook_runtime_context_json,
     workspace_skill_policies_json,input_json,capabilities_json,resolved_artifacts_json,
     runtime_environment_json,history_json,frozen_manifest_id)
    VALUES ('turn','thread','workspace','agent','model','provider','{}','[]',
            '[]','{}','[]','{}','[]','manifest')";

#[tokio::test]
async fn expiry_and_root_publication_follow_the_header_lock_in_both_commit_orders() -> Result<()> {
    for publish_first in [true, false] {
        with_database(async |fixture| {
            let pool = fixture.connection();
            pool.execute_unprepared(
                "INSERT INTO workspace(id,name) VALUES ('workspace','Workspace');
                 INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status)
                 VALUES ('thread','workspace','','agent','model','provider','active');
                 INSERT INTO turn(id,thread_id,status) VALUES ('turn','thread','completed');
                 INSERT INTO compaction_frozen_history
                    (id,workspace_id,owner_thread,identity_sha256,message_count,import_count,imports_sha256,ready)
                 VALUES ('manifest','workspace','thread',repeat('a',64),0,0,repeat('b',64),1)",
            ).await?;
            let header = history::Entity::find_by_id("manifest").one(pool).await?.context("manifest fixture")?;
            let first = pool.begin().await?;
            if publish_first {
                first.execute_unprepared(ROOT).await?;
            } else {
                assert!(expire(&first, &header, |_| false).await?);
                assert_eq!(
                    history::Entity::find_by_id("manifest").one(pool).await?.context("uncommitted expiry")?.expired, 0
                );
            }

            let independent = fixture.connect().await?;
            let second = independent.begin().await?;
            let second_pid = backend_pid(&second).await?;
            let saved_header = header.clone();
            let attempt = tokio::spawn(async move {
                if publish_first {
                    assert!(!expire(&second, &saved_header, |_| false).await?);
                    second.commit().await?;
                } else {
                    let error = second.execute_unprepared(ROOT).await.expect_err("cannot hold an expired manifest");
                    assert!(error.to_string().contains("unverified or unavailable frozen root"));
                    second.rollback().await?;
                }
                Ok::<_, anyhow::Error>(())
            });
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let blocked: bool = pool.query_one_raw(Statement::from_sql_and_values(
                        DbBackend::Postgres,
                        "SELECT cardinality(pg_blocking_pids($1)) > 0",
                        [second_pid.into()],
                    )).await?.context("read root publication lock state")?.try_get_by_index(0)?;
                    if blocked {
                        return Ok::<_, anyhow::Error>(());
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }).await.context("second writer did not wait on the header")??;
            first.commit().await?;
            attempt.await??;
            let saved = history::Entity::find_by_id("manifest").one(pool).await?.context("saved expiry marker")?;
            assert_eq!(saved.expired, i64::from(!publish_first));
            if publish_first {
                pool.execute_unprepared("DELETE FROM turn_runtime_snapshot WHERE turn_id='turn'").await?;
                let retry = independent.begin().await?;
                assert!(expire(&retry, &header, |_| false).await?);
                retry.rollback().await?;
                assert_eq!(history::Entity::find_by_id("manifest").one(pool).await?.context("rolled back marker")?.expired, 0);
                let retry = independent.begin().await?;
                assert!(expire(&retry, &header, |_| false).await?);
                retry.commit().await?;
                assert_eq!(history::Entity::find_by_id("manifest").one(pool).await?.context("committed retry")?.expired, 1);
            }
            independent.close().await?;
            Ok(())
        }).await?;
    }
    Ok(())
}

async fn backend_pid(tx: &DatabaseTransaction) -> Result<i32> {
    Ok(tx
        .query_one_raw(Statement::from_string(
            DbBackend::Postgres,
            "SELECT pg_backend_pid()",
        ))
        .await?
        .context("competing transaction PID")?
        .try_get_by_index(0)?)
}
