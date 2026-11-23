use anyhow::{Context, Result};
use pioneer_crud_execution::agent_execution::{
    heartbeat_agent_execution, load_agent_execution, record_agent_execution_progress,
};
use pioneer_entity::agent_execution_resource_state as resource;
use pioneer_postgres_test::with_database;
use sea_orm::{ConnectionTrait, EntityTrait, TransactionTrait};

#[tokio::test]
async fn heartbeat_and_progress_commit_together_and_respect_the_attempt_fence() -> Result<()> {
    with_database(async |fixture| {
        let db = fixture.connection();
        seed_agent_execution(db).await?;
        let execution = "Execution000000000001";
        let after_creation = "2026-11-19T10:00:01Z".parse()?;
        let progress = "2026-11-19T10:00:05Z".parse()?;
        let hard_deadline = "2026-11-19T10:00:10Z".parse()?;
        let beyond_hard = "2026-11-19T10:00:15Z".parse()?;
        let frontier = r#"{ "step" : 1 }"#;

        let tx = db.begin().await?;
        assert!(!heartbeat_agent_execution(&tx, execution, 2, after_creation, None).await?);
        assert!(
            !record_agent_execution_progress(&tx, execution, 2, frontier, progress, None).await?
        );
        assert!(heartbeat_agent_execution(&tx, execution, 1, after_creation, None).await?);
        let row = resource::Entity::find_by_id("resource")
            .one(&tx)
            .await?
            .context("missing attempt")?;
        assert_eq!(row.last_heartbeat_at, Some(after_creation));
        assert_eq!(row.progress_sequence, 0);
        assert!(row.last_progress_at.is_none());
        assert_eq!(row.hard_deadline, Some(hard_deadline));
        assert!(
            record_agent_execution_progress(
                &tx,
                execution,
                1,
                frontier,
                progress,
                Some(beyond_hard)
            )
            .await?
        );
        assert_eq!(
            load_agent_execution(&tx, execution)
                .await?
                .context("missing execution")?
                .updated_at,
            progress
        );
        assert_eq!(
            resource::Entity::find_by_id("resource")
                .one(db)
                .await?
                .context("missing attempt")?
                .progress_sequence,
            0
        );
        tx.rollback().await?;
        assert!(
            resource::Entity::find_by_id("resource")
                .one(db)
                .await?
                .context("missing attempt")?
                .last_heartbeat_at
                .is_none()
        );

        let tx = db.begin().await?;
        assert!(
            record_agent_execution_progress(
                &tx,
                execution,
                1,
                frontier,
                progress,
                Some(beyond_hard)
            )
            .await?
        );
        tx.commit().await?;
        let row = resource::Entity::find_by_id("resource")
            .one(db)
            .await?
            .context("missing attempt")?;
        assert_eq!(row.progress_sequence, 1);
        assert_eq!(row.progress_frontier_json, frontier);
        assert_eq!(row.idle_deadline, Some(hard_deadline));
        assert_eq!(row.last_progress_at, Some(progress));
        assert_eq!(
            load_agent_execution(db, execution)
                .await?
                .context("missing execution")?
                .updated_at,
            progress
        );
        Ok(())
    })
    .await
}

async fn seed_agent_execution(db: &impl ConnectionTrait) -> Result<()> {
    db.execute_unprepared(
        r#"
        INSERT INTO workspace (id, name) VALUES ('w', 'Workspace');
        INSERT INTO agent_identity (id, workspace_id, source_kind, source_id, source_fingerprint)
            VALUES ('Identity0000000000000', 'w', 'ephemeral', 'identity', 'fingerprint');
        INSERT INTO agent_execution
            (id, workspace_id, agent_identity_id, identity_source_revision, identity_source_fingerprint,
             home_root_thread_id, work_graph_root_execution_id, requested_identity_selection_json,
             requested_profile_selection_json, authorization_context_fingerprint, status, updated_at)
            VALUES ('Execution000000000001', 'w', 'Identity0000000000000', 1, 'fingerprint',
                    'thread', 'Execution000000000001', '{}', '{}', 'authorization', 'running',
                    '2026-11-19T10:00:00Z');
        INSERT INTO agent_execution_resource_state
            (id, execution_id, progress_frontier_json, local_usage_json, branch_key, fair_order,
             status, hard_deadline, updated_at)
            VALUES ('resource', 'Execution000000000001', '{}', '{}', 'a', 1, 'running',
                    '2026-11-19T10:00:10Z', '2026-11-19T10:00:00Z');
        "#,
    ).await?;
    Ok(())
}

#[tokio::test]
async fn finalize_and_retry_preserve_the_closed_attempt_and_scope_in_one_commit() -> Result<()> {
    use pioneer_crud_execution::agent_execution::{
        acquire_agent_running_permit, finalize_agent_execution_status,
        release_finalized_agent_execution, reopen_agent_execution_for_retry,
    };
    use pioneer_entity::{agent_running_permit as permits, agent_work_resource_scope as scopes};

    with_database(async |fixture| {
        let db = fixture.connection();
        seed_agent_execution(db).await?;
        let id = "Execution000000000001";
        let now = "2026-11-19T10:00:05Z".parse()?;
        let retry_at = "2026-11-19T10:00:09Z".parse()?;
        db.execute_unprepared(
            "INSERT INTO agent_work_resource_scope(root_execution_id,aggregate_usage_json)
                 VALUES('Execution000000000001','{}');",
        )
        .await?;
        acquire_agent_running_permit(db, "permit", id, id, 1, now).await?;
        db.execute_unprepared(
            "UPDATE agent_execution_resource_state SET permit_id='permit' WHERE id='resource'",
        )
        .await?;

        for commit in [false, true] {
            let tx = db.begin().await?;
            let change = finalize_agent_execution_status(&tx, id, "failed", now).await?;
            assert_eq!(change.previous.status, "running");
            assert_eq!(change.current.status, "failed");
            release_finalized_agent_execution(&tx, &change, now).await?;
            assert_eq!(
                scopes::Entity::find_by_id(id)
                    .one(&tx)
                    .await?
                    .context("scope missing")?
                    .status,
                "closed"
            );
            assert_eq!(
                resource::Entity::find_by_id("resource")
                    .one(&tx)
                    .await?
                    .context("attempt missing")?
                    .status,
                "failed"
            );
            assert_eq!(
                permits::Entity::find_by_id("permit")
                    .one(&tx)
                    .await?
                    .context("permit missing")?
                    .status,
                "released"
            );
            assert_eq!(
                load_agent_execution(db, id)
                    .await?
                    .context("execution missing")?
                    .status,
                "running"
            );
            if commit {
                tx.commit().await?;
            } else {
                tx.rollback().await?;
                assert_eq!(
                    permits::Entity::find_by_id("permit")
                        .one(db)
                        .await?
                        .context("permit missing")?
                        .status,
                    "held"
                );
            }
        }

        let tx = db.begin().await?;
        let (previous, current, changed) =
            reopen_agent_execution_for_retry(&tx, id, id, retry_at).await?;
        assert!(changed);
        assert_eq!(previous.finished_at, Some(now));
        assert_eq!(current.status, "recovering");
        assert!(current.finished_at.is_none());
        tx.commit().await?;
        assert_eq!(
            scopes::Entity::find_by_id(id)
                .one(db)
                .await?
                .context("scope missing")?
                .status,
            "active"
        );
        assert_eq!(
            resource::Entity::find_by_id("resource")
                .one(db)
                .await?
                .context("attempt missing")?
                .status,
            "failed"
        );
        assert_eq!(
            permits::Entity::find_by_id("permit")
                .one(db)
                .await?
                .context("permit missing")?
                .status,
            "released"
        );
        Ok(())
    })
    .await
}
