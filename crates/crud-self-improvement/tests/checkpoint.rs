use anyhow::{Context, Result};
use chrono::{Duration, Utc};
use pioneer_crud_self_improvement::{
    NewSelfImprovementRun, SelfImprovementRunMutationResult,
    self_improvement_run::{
        claim_available, find_by_id, insert_pending, record_from_model, save_checkpoint,
    },
    self_improvement_workspace_state::{activate_if_inactive, ensure},
};
use pioneer_postgres_test::with_database;
use sea_orm::{ConnectionTrait, TransactionTrait};

#[tokio::test]
async fn checkpoint_keeps_exact_bytes_and_rejects_the_previous_lease() -> Result<()> {
    with_database(async |fixture| {
        let db = fixture.connection();
        db.execute_unprepared("INSERT INTO workspace (id,name) VALUES ('w','Workspace')")
            .await?;
        let now = Utc::now().fixed_offset();
        ensure(db, "w", now).await?;
        assert!(activate_if_inactive(db, "w", now).await?);
        let input = NewSelfImprovementRun {
            workspace_id: "w".into(),
            activation_epoch: 1,
            scheduled_date_utc: "2026-11-20".into(),
            source_lower_exclusive: 0,
            source_upper_inclusive: 19,
            learner_provider: "provider".into(),
            learner_model: "learner".into(),
            learner_reasoning_effort: None,
            reviewer_provider: "provider".into(),
            reviewer_model: "reviewer".into(),
            reviewer_reasoning_effort: None,
            pipeline_contract_version: "v1".into(),
        };
        insert_pending(db, "r", &input, now).await?;
        let saved_now = now + Duration::seconds(1);
        let lease = now + Duration::seconds(2);
        assert!(claim_available(db, "w", "r", 1, "first", "worker-one", now, lease).await?);
        let read = record_from_model(find_by_id(db, "w", "r").await?.context("missing run")?);
        let fence = read.fence().context("missing fence")?;

        let cursor = r#"{"after": 17, "source": "\u0000"}"#;
        let digest = r#"{ "status": "готово" }"#;
        let tx = db.begin().await?;
        assert_eq!(
            save_checkpoint(&tx, &fence, cursor, digest, saved_now).await?,
            SelfImprovementRunMutationResult::Applied
        );
        assert_eq!(
            find_by_id(db, "w", "r")
                .await?
                .context("missing run")?
                .analysis_cursor_json,
            None
        );
        tx.commit().await?;
        let stored = find_by_id(db, "w", "r").await?.context("missing run")?;
        assert_eq!(stored.analysis_cursor_json.as_deref(), Some(cursor));
        assert_eq!(stored.analysis_digest_json.as_deref(), Some(digest));

        let later = lease + Duration::seconds(1);
        assert!(
            claim_available(
                db,
                "w",
                "r",
                1,
                "second",
                "worker-two",
                later,
                later + Duration::seconds(2)
            )
            .await?
        );
        assert_eq!(
            save_checkpoint(db, &fence, "{}", "{}", later).await?,
            SelfImprovementRunMutationResult::LostAuthority
        );
        let stored = find_by_id(db, "w", "r")
            .await?
            .context("missing run after reclaim")?;
        assert_eq!(stored.analysis_cursor_json.as_deref(), Some(cursor));
        assert_eq!(stored.analysis_digest_json.as_deref(), Some(digest));
        Ok(())
    })
    .await
}
