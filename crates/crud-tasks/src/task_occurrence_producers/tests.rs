use super::*;
use crate::test_support::occurrence::seed_occurrence_pair;
use pioneer_entity::{
    task_occurrence_reconcile_pending as pending, task_occurrence_reconcile_sequence as sequence,
};
use pioneer_postgres_test::with_database;
use pioneer_protocol::TaskRunStatus;
use sea_orm::TransactionTrait;

#[tokio::test]
async fn cutover_preserves_rollback_and_invalidates_claim_without_resetting_retry() {
    with_database(async |database| {
        let db = database.connection();
        seed_occurrence_pair(db).await;
        let tx = db.begin().await.unwrap();
        crate::task_run::update_run_status(
            &tx,
            "occurrence-run",
            TaskRunStatus::Waiting,
            chrono::Utc::now().fixed_offset(),
        )
        .await
        .unwrap();
        assert!(
            pending::Entity::find_by_id("occurrence-run")
                .one(&tx)
                .await
                .unwrap()
                .is_some()
        );
        tx.rollback().await.unwrap();
        assert_eq!(
            task_run::Entity::find_by_id("occurrence-run")
                .one(db)
                .await
                .unwrap()
                .unwrap()
                .status,
            "running"
        );
        assert!(
            pending::Entity::find_by_id("occurrence-run")
                .one(db)
                .await
                .unwrap()
                .is_none()
        );

        let tx = db.begin().await.unwrap();
        crate::task_run::update_run_status(
            &tx,
            "occurrence-run",
            TaskRunStatus::Waiting,
            chrono::Utc::now().fixed_offset(),
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        let before = pending::Entity::find_by_id("occurrence-run")
            .one(db)
            .await
            .unwrap()
            .unwrap();
        pending::Entity::update_many()
            .col_expr(pending::Column::ClaimToken, Expr::val("claim"))
            .col_expr(pending::Column::NextAttemptAt, Expr::val(9_000_000_000_i64))
            .col_expr(pending::Column::AttemptCount, Expr::val(4_i64))
            .filter(pending::Column::RunId.eq("occurrence-run"))
            .exec(db)
            .await
            .unwrap();
        let tx = db.begin().await.unwrap();
        crate::task_run::update_run_status(
            &tx,
            "occurrence-run",
            TaskRunStatus::WaitingReview,
            chrono::Utc::now().fixed_offset(),
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        let after = pending::Entity::find_by_id("occurrence-run")
            .one(db)
            .await
            .unwrap()
            .unwrap();
        assert!(after.generation > before.generation);
        assert_eq!(
            (
                after.next_attempt_at,
                after.attempt_count,
                after.claim_token
            ),
            (9_000_000_000, 4, None)
        );
        let stale = pending::Entity::delete_many()
            .filter(pending::Column::RunId.eq("occurrence-run"))
            .filter(pending::Column::Generation.eq(before.generation))
            .exec(db)
            .await
            .unwrap();
        assert_eq!(stale.rows_affected, 0);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn enqueue_failure_rolls_back_source_transition() {
    with_database(async |database| {
        let db = database.connection();
        seed_occurrence_pair(db).await;
        sequence::Entity::update_many()
            .col_expr(sequence::Column::Generation, Expr::val(i64::MAX))
            .filter(sequence::Column::Singleton.eq(1))
            .exec(db)
            .await
            .unwrap();
        let tx = db.begin().await.unwrap();
        let result = crate::task_run::update_run_status(
            &tx,
            "occurrence-run",
            TaskRunStatus::Waiting,
            chrono::Utc::now().fixed_offset(),
        )
        .await;
        assert!(
            result.is_err(),
            "exhausted generation must abort the atomic write set"
        );
        tx.rollback().await.unwrap();
        assert_eq!(
            task_run::Entity::find_by_id("occurrence-run")
                .one(db)
                .await
                .unwrap()
                .unwrap()
                .status,
            "running"
        );
        assert!(
            pending::Entity::find_by_id("occurrence-run")
                .one(db)
                .await
                .unwrap()
                .is_none()
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn missing_parent_retains_obligation_until_consistent_birth() {
    with_database(async |database| {
        use pioneer_entity::task_run_occurrence_reconcile_pending as parents;
        let db = database.connection();
        seed_occurrence_pair(db).await;
        let now = chrono::Utc::now().fixed_offset();
        let tx = db.begin().await.unwrap();
        crate::task_run::update_run_status(&tx, "occurrence-run", TaskRunStatus::Succeeded, now)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert!(
            parents::Entity::find_by_id("occurrence-run")
                .one(db)
                .await
                .unwrap()
                .is_some()
        );
        let tx = db.begin().await.unwrap();
        let parent = turn::ActiveModel {
            id: Set("occurrence-run".into()),
            thread_id: Set("parent-thread".into()),
            status: Set("completed".into()),
            turn_kind: Set("task_run".into()),
            origin: Set("system".into()),
            prompt_manifest_json: Set("{}".into()),
            created_at: Set(now),
            updated_at: Set(now),
            ..Default::default()
        };
        turn::Entity::insert(parent)
            .exec_without_returning(&tx)
            .await
            .unwrap();
        let parent = turn::Entity::find_by_id("occurrence-run")
            .one(&tx)
            .await
            .unwrap()
            .unwrap();
        turn_change(&tx, None, Some(&parent)).await.unwrap();
        tx.commit().await.unwrap();
        assert!(
            parents::Entity::find_by_id("occurrence-run")
                .one(db)
                .await
                .unwrap()
                .is_none()
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn known_ineligible_births_do_not_fill_parent_queue_and_deletes_keep_generation() {
    with_database(async |database| {
        use pioneer_entity::{
            task_run_occurrence_reconcile_pending as parents,
            task_run_occurrence_reconcile_sequence as parent_sequence,
        };
        let db = database.connection();
        seed_occurrence_pair(db).await;
        let now = chrono::Utc::now().fixed_offset();
        let before = parent_sequence::Entity::find_by_id(1)
            .one(db)
            .await
            .unwrap()
            .unwrap()
            .generation;
        let tx = db.begin().await.unwrap();
        // A real source adapter publishes the contract owner's birth, while a
        // nonterminal Run cannot require a terminal parent projection repair.
        let template = task_run::Entity::find_by_id("occurrence-run")
            .one(&tx)
            .await
            .unwrap()
            .unwrap();
        let mut queued = crate::task_run::task_run_from_db_model(template).unwrap();
        queued.id = "ordinary-queued".into();
        queued.run_group_id = queued.id.clone();
        queued.run_number = 2;
        queued.status = TaskRunStatus::Queued;
        crate::task_run::upsert_run(&tx, &queued).await.unwrap();
        assert!(
            pending::Entity::find_by_id(&queued.id)
                .one(&tx)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            parents::Entity::find_by_id(&queued.id)
                .one(&tx)
                .await
                .unwrap()
                .is_none()
        );
        turn::Entity::insert(turn::ActiveModel {
            id: Set("ordinary-conversation".into()),
            thread_id: Set("parent-thread".into()),
            status: Set("in_progress".into()),
            turn_kind: Set("conversation".into()),
            origin: Set("system".into()),
            prompt_manifest_json: Set("{}".into()),
            created_at: Set(now),
            updated_at: Set(now),
            ..Default::default()
        })
        .exec_without_returning(&tx)
        .await
        .unwrap();
        let old = turn::Entity::find_by_id("ordinary-conversation")
            .one(&tx)
            .await
            .unwrap()
            .unwrap();
        turn_change(&tx, None, Some(&old)).await.unwrap();
        turn::Entity::update_many()
            .col_expr(turn::Column::Status, Expr::val("completed"))
            .filter(turn::Column::Id.eq(&old.id))
            .exec(&tx)
            .await
            .unwrap();
        let new = turn::Entity::find_by_id(&old.id)
            .one(&tx)
            .await
            .unwrap()
            .unwrap();
        turn_change(&tx, Some(&old), Some(&new)).await.unwrap();
        assert!(
            parents::Entity::find_by_id(&old.id)
                .one(&tx)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            parent_sequence::Entity::find_by_id(1)
                .one(&tx)
                .await
                .unwrap()
                .unwrap()
                .generation,
            before
        );
        tx.commit().await.unwrap();

        // An explicit source deletion has authoritative absence. It must remove
        // old work without allocating a conservative birth generation first.
        let tx = db.begin().await.unwrap();
        crate::task_run::update_run_status(&tx, &queued.id, TaskRunStatus::Succeeded, now)
            .await
            .unwrap();
        let old = task_run::Entity::find_by_id(&queued.id)
            .one(&tx)
            .await
            .unwrap()
            .unwrap();
        assert!(
            parents::Entity::find_by_id(&queued.id)
                .one(&tx)
                .await
                .unwrap()
                .is_some()
        );
        let generation = parent_sequence::Entity::find_by_id(1)
            .one(&tx)
            .await
            .unwrap()
            .unwrap()
            .generation;
        task_run::Entity::delete_by_id(&queued.id)
            .exec(&tx)
            .await
            .unwrap();
        run_change(&tx, Some(&old), None).await.unwrap();
        assert!(
            parents::Entity::find_by_id(&queued.id)
                .one(&tx)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            parent_sequence::Entity::find_by_id(1)
                .one(&tx)
                .await
                .unwrap()
                .unwrap()
                .generation,
            generation
        );
        tx.commit().await.unwrap();
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn recovery_completion_timestamp_publishes_occurrence_work_in_the_same_transaction()
-> anyhow::Result<()> {
    use pioneer_entity::task_run_occurrence_reconcile_pending as parent_pending;

    with_database(async |fixture| {
        let db = fixture.connection();
        let id = "occurrence-run";
        seed_occurrence_pair(db).await;
        task_run::Entity::update_many()
            .filter(task_run::Column::Id.eq(id))
            .col_expr(task_run::Column::Status, Expr::value("succeeded"))
            .exec(db)
            .await?;

        let high = crate::task_run::missing_completion_high_watermark(db)
            .await?
            .expect("terminal run with missing completion timestamp");
        assert_eq!(high, id);
        let ids = crate::task_run::missing_completion_page(db, &high, None, 32).await?;
        assert_eq!(ids, [id]);
        assert!(
            crate::task_run::missing_completion_page(db, &high, Some(id), 32)
                .await?
                .is_empty()
        );
        let completed_at = chrono::DateTime::from_timestamp(1_700_000_001, 0)
            .unwrap()
            .fixed_offset();

        let tx = db.begin().await?;
        assert_eq!(
            crate::task_run::repair_missing_completion(&tx, ids.clone(), completed_at).await?,
            1
        );
        assert!(
            parent_pending::Entity::find_by_id(id)
                .one(&tx)
                .await?
                .is_some()
        );
        assert!(pending::Entity::find_by_id(id).one(&tx).await?.is_some());
        assert!(
            parent_pending::Entity::find_by_id(id)
                .one(db)
                .await?
                .is_none()
        );
        tx.rollback().await?;
        assert!(
            parent_pending::Entity::find_by_id(id)
                .one(db)
                .await?
                .is_none()
        );
        assert!(pending::Entity::find_by_id(id).one(db).await?.is_none());
        assert!(
            task_run::Entity::find_by_id(id)
                .one(db)
                .await?
                .unwrap()
                .completed_at
                .is_none()
        );

        let tx = db.begin().await?;
        assert_eq!(
            crate::task_run::repair_missing_completion(&tx, ids.clone(), completed_at).await?,
            1
        );
        tx.commit().await?;
        assert_eq!(
            task_run::Entity::find_by_id(id)
                .one(db)
                .await?
                .unwrap()
                .completed_at,
            Some(completed_at)
        );
        let current = parent_pending::Entity::find_by_id(id)
            .one(db)
            .await?
            .unwrap();
        let tx = db.begin().await?;
        assert_eq!(
            crate::task_run::repair_missing_completion(&tx, ids, completed_at).await?,
            0
        );
        tx.commit().await?;
        assert_eq!(
            parent_pending::Entity::find_by_id(id).one(db).await?,
            Some(current)
        );
        assert!(
            crate::task_run::missing_completion_high_watermark(db)
                .await?
                .is_none()
        );
        Ok(())
    })
    .await
}
