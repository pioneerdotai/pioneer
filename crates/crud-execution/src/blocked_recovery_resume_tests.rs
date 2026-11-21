use super::*;
use crate::{recovery_store::RecoveryStore, time::unix_to_datetime};
use pioneer_protocol::{RecoveryAction, RecoveryJobStatus, RecoveryTrigger};
use pioneer_sqlite::SqliteDatabase;
use sea_orm::{ConnectionTrait, EntityTrait, Set, TransactionTrait};

#[tokio::test]
async fn blocked_tool_recovery_resume_without_snapshot_leaves_turn_blocked() {
    pioneer_postgres_test::with_database(async |fixture| {
        let db = SqliteDatabase::new(
            fixture.connect_read_only().await?,
            fixture.connection().clone(),
        );
        db.execute_unprepared(
            "INSERT INTO workspace(id,name) VALUES ('ws_resume_missing_snapshot','Recovery');
             INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status)
             VALUES ('thread_resume_missing_snapshot','ws_resume_missing_snapshot','','agent','m','p','active');",
        ).await?;
        let wake = Default::default();
        let store = RecoveryStore::new(&db, &wake);
        let now = unix_to_datetime(1_700_000_000);
        let thread_id = "thread_resume_missing_snapshot";
        let turn_id = "turn_resume_missing_snapshot";
        pioneer_entity::turn::Entity::insert(pioneer_entity::turn::ActiveModel {
            id: Set(turn_id.to_owned()),
            thread_id: Set(thread_id.to_owned()),
            status: Set("blocked".to_owned()),
            turn_kind: Set("conversation".to_owned()),
            origin: Set("user".to_owned()),
            error: Set(Some("waiting for runtime snapshot".to_owned())),
            prompt_manifest_json: Set("{}".to_owned()),
            created_at: Set(now),
            updated_at: Set(now),
            ..Default::default()
        })
        .exec(&db)
        .await
        .expect("turn insert should succeed");

        let job = store
            .enqueue_recovery_job(
                turn_id.to_owned(),
                "tool_item_missing_snapshot".to_owned(),
                TurnItemType::WebFetch,
                None,
                RecoveryTrigger::Timeout,
                RecoveryAction::BlockResumable,
                Some("tool recovery blocked".to_owned()),
                None,
                None,
                None,
                0,
                0,
                serde_json::json!({}),
                serde_json::json!({
                    "base_backoff_secs": 0,
                    "max_wall_clock_secs": 60,
                    "no_progress_limit": 3,
                }),
                1_700_000_001,
            )
            .await
            .expect("recovery job should enqueue");
        store
            .mark_recovery_job_terminal(
                job.id.as_str(),
                RecoveryJobStatus::Blocked,
                Some("waiting for operator resume".to_owned()),
                1_700_000_002,
            )
            .await
            .expect("recovery job should be marked blocked");

        let tx = db.begin().await?;
        let admission = admit_blocked_recovery_resume(
            &tx, thread_id, turn_id, Some(job.id.as_str()),
        )
        .await
        .expect("blocked resume should evaluate");
        let BlockedResumeAdmission::Unchanged { outcome, .. } = admission else {
            panic!("blocked recovery without a snapshot must not be admitted");
        };
        tx.rollback().await?;

        match outcome {
            BlockedTurnRecoveryResumeOutcome::MissingRuntimeSnapshot { recovery_job_id } => {
                assert_eq!(recovery_job_id, job.id);
            }
            other => panic!("expected missing runtime snapshot, got {other:?}"),
        }
        let turn = pioneer_entity::turn::Entity::find_by_id(turn_id.to_owned())
            .one(&db)
            .await
            .expect("turn read should succeed")
            .expect("turn should exist");
        assert_eq!(turn.status, "blocked");
        let blocked_job = store
            .get_recovery_job(job.id.as_str())
            .await
            .expect("job read should succeed")
            .expect("job should exist");
        assert_eq!(blocked_job.status, RecoveryJobStatus::Blocked);
        Ok(())
    })
    .await
    .unwrap();
}
