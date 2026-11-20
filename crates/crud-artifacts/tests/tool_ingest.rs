use anyhow::{Context, Result};
use pioneer_crud_artifacts::artifact::{ArtifactBindingTargetRecord, ArtifactRepository};
use pioneer_postgres_test::with_database;
use pioneer_protocol::{ArtifactBindingDirection, ArtifactBindingKind, ArtifactRole};
use sea_orm::{ConnectionTrait, DbBackend, Statement, TransactionTrait};
use std::time::Duration;

#[tokio::test]
async fn turn_completion_and_tool_publication_observe_the_owner_in_commit_order() -> Result<()> {
    for publication_first in [true, false] {
        with_database(async |fixture| {
            let db = fixture.connection();
            db.execute_unprepared(
                "INSERT INTO workspace(id,name) VALUES ('workspace','Workspace');
                 INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status)
                 VALUES ('thread','workspace','','agent','model','provider','active');
                 INSERT INTO turn(id,thread_id,status) VALUES ('turn','thread','in_progress')",
            )
            .await?;
            let target = ArtifactBindingTargetRecord {
                thread_id: Some("thread".into()),
                turn_id: Some("turn".into()),
                message_id: None,
                turn_item_id: None,
                tool_call_id: Some("call".into()),
                task_id: None,
                task_run_id: None,
                binding_kind: ArtifactBindingKind::ToolOutput,
                direction: ArtifactBindingDirection::Output,
                role: Some(ArtifactRole::Tool),
                item_index: None,
            };
            let finish = "UPDATE turn SET status='completed' WHERE id='turn'";
            let first = db.begin().await?;
            if publication_first {
                ArtifactRepository::new()
                    .lock_tool_ingest_owner(&first, "workspace", &target)
                    .await?;
            } else {
                first.execute_unprepared(finish).await?;
            }
            let writer = fixture.connect().await?;
            let second = writer.begin().await?;
            let second_pid: i32 = second
                .query_one_raw(Statement::from_string(
                    DbBackend::Postgres,
                    "SELECT pg_backend_pid()",
                ))
                .await?
                .context("second connection PID")?
                .try_get_by_index(0)?;
            let contender = tokio::spawn(async move {
                if publication_first {
                    second.execute_unprepared(finish).await?;
                    second.commit().await?;
                    return Ok::<_, anyhow::Error>(());
                }
                let rejection = ArtifactRepository::new()
                    .lock_tool_ingest_owner(&second, "workspace", &target)
                    .await
                    .expect_err("the committed terminal turn rejects a later publication");
                assert!(rejection.to_string().contains("owner is no longer live"));
                second.rollback().await?;
                Ok(())
            });
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if db
                        .query_one_raw(Statement::from_sql_and_values(
                            DbBackend::Postgres,
                            "SELECT cardinality(pg_blocking_pids($1)) > 0",
                            [second_pid.into()],
                        ))
                        .await?
                        .context("second connection lock state")?
                        .try_get_by_index(0)?
                    {
                        return Ok::<_, anyhow::Error>(());
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .context("competing transaction did not wait for the owner")??;
            first.commit().await?;
            contender.await??;
            Ok(())
        })
        .await?;
    }
    Ok(())
}
