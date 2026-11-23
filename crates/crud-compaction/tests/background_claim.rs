use std::time::Duration;

use anyhow::{Context, Result};
use pioneer_crud_compaction::background::{claim_history_check, record_history_result};
use pioneer_crud_compaction::history_check_result::HistoryCheckOutcome;
use pioneer_entity::compaction_history_check;
use sea_orm::{ConnectionTrait, DatabaseBackend, EntityTrait, Statement, TransactionTrait};

#[pioneer_postgres_test::postgres_test]
async fn competing_history_claim_rechecks_revision_after_commit_or_rollback() -> Result<()> {
    for commit_first in [true, false] {
        let fixture = pioneer_postgres_test::scoped_database().await?;
        let db = fixture.connection();
        db.execute_unprepared(
            "INSERT INTO workspace(id,name) VALUES ('ws','History fixture');
             INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status)
             VALUES ('thread','ws','','agent','m','p','active');
             INSERT INTO turn(id,thread_id,status) VALUES ('turn','thread','completed');
             INSERT INTO compaction_root_activity(thread_id,user_turn_id,last_user_activity_ms)
             VALUES ('thread','turn',1) ON CONFLICT (thread_id) DO NOTHING;
             INSERT INTO compaction_history_check(turn_id,purpose)
             VALUES ('turn','root_conversation') ON CONFLICT (turn_id) DO UPDATE
             SET purpose=EXCLUDED.purpose",
        )
        .await?;

        let first = db.begin().await?;
        let pid = first
            .query_one_raw(Statement::from_string(
                DatabaseBackend::Postgres,
                "SELECT pg_backend_pid()",
            ))
            .await?
            .context("missing claim backend")?
            .try_get_by_index::<i32>(0)?;
        let claimed = claim_history_check(&first, "turn", 0, 100)
            .await?
            .context("first worker should own revision 0")?;
        assert_eq!(claimed.revision, 1);

        let competitor = fixture.connect().await?;
        let second = competitor.begin().await?;
        let waiting = tokio::spawn(async move {
            let claimed = claim_history_check(&second, "turn", 0, 100).await?;
            second.commit().await?;
            Ok::<_, anyhow::Error>(claimed)
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let blocked = db
                    .query_one_raw(Statement::from_sql_and_values(
                        DatabaseBackend::Postgres,
                        "SELECT EXISTS(SELECT 1 FROM pg_stat_activity
                         WHERE datname=current_database() AND $1=ANY(pg_blocking_pids(pid)))",
                        [pid.into()],
                    ))
                    .await?
                    .context("missing contention observation")?
                    .try_get_by_index::<bool>(0)?;
                if blocked {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await??;

        if commit_first {
            first.commit().await?;
        } else {
            first.rollback().await?;
        }
        assert_eq!(waiting.await??.is_some(), !commit_first);

        let finish = db.begin().await?;
        assert!(
            record_history_result(
                &finish,
                "turn",
                1,
                0,
                HistoryCheckOutcome::Compacted,
                serde_json::json!({"stage": "completion"}),
                101,
            )
            .await?
        );
        finish.commit().await?;
        let row = compaction_history_check::Entity::find_by_id("turn")
            .one(db)
            .await?
            .context("missing committed history check")?;
        assert_eq!(row.revision, 2);
        assert_eq!(row.outcome.as_deref(), Some("compacted"));
        competitor.close().await?;
    }
    Ok(())
}
