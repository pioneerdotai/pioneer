use anyhow::{Context, Result};
use pioneer_compaction::ModelBudget;
use pioneer_compaction::runner::{FailureKind, RunnerPhase, RunnerState};
use pioneer_crud_compaction::runner_repository::{
    compaction_bind_execution_turn, compaction_execution_cancelled, compaction_finish_execution,
    compaction_manifest_page, compaction_prepare_runner, compaction_reconcile_runner_state,
    compaction_runner_plan,
};
use pioneer_crud_compaction::runner_state::insert_initial;
use pioneer_postgres_test::with_database;
use sea_orm::{ConnectionTrait, TransactionTrait};

#[tokio::test]
async fn runner_admission_and_manifest_are_visible_only_after_commit() -> Result<()> {
    with_database(async |database| {
        let db = database.connection();
        seed_runner(db).await?;
        let budget = ModelBudget::new(None, None, None);
        let tx = db.begin().await?;
        assert!(compaction_prepare_runner(&tx, "operation", &budget, 2, 1).await?);
        assert!(!compaction_prepare_runner(&tx, "operation", &budget, 2, 1).await?);
        assert!(
            compaction_prepare_runner(&tx, "operation", &budget, 3, 1)
                .await
                .is_err()
        );
        let plan = compaction_runner_plan(&tx, "operation")
            .await?
            .context("missing plan")?;
        assert_eq!(
            (plan.source_count, plan.reference_count, plan.ready),
            (2, 1, false)
        );
        assert_eq!(
            serde_json::to_value(plan.budget)?,
            serde_json::to_value(budget)?
        );
        assert!(compaction_runner_plan(db, "operation").await?.is_none());
        tx.commit().await?;
        assert!(compaction_runner_plan(db, "operation").await?.is_some());

        db.execute_unprepared(
            r#"
            INSERT INTO compaction_manifest
                (operation_id, ordinal, unit_ordinal, reference_only, source_thread,
                 source_scope, source_id, source_version)
                VALUES ('operation', 3, 1, 0, 'thread', 'context:turn', 'three', 'v3'),
                       ('operation', 2, 1, 0, 'thread', 'context:turn', 'two', 'v2'),
                       ('operation', 1, 0, 1, 'thread', 'context:turn', 'reference', 'v1');
        "#,
        )
        .await?;
        let owned = compaction_manifest_page(db, "operation", false, 1, 0).await?;
        assert_eq!(
            owned
                .iter()
                .map(|entry| (entry.ordinal, entry.unit))
                .collect::<Vec<_>>(),
            [(2, 1), (3, 1)]
        );
        assert_eq!(
            compaction_manifest_page(db, "operation", false, 1, 1).await?[0]
                .source
                .id,
            "three"
        );
        let references = compaction_manifest_page(db, "operation", true, 0, 0).await?;
        assert_eq!(references.len(), 1);
        assert_eq!(references[0].source.id, "reference");
        assert!(references[0].reference_only);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn runner_terminal_fence_rejects_stale_cleanup_and_reconciliation_replays() -> Result<()> {
    with_database(async |database| {
        let db = database.connection();
        seed_runner(db).await?;
        let initial = RunnerState::new(1234, &ModelBudget::new(None, None, None), 100, None)?;
        assert!(insert_initial(db, "operation", serde_json::to_string(&initial)?).await?);
        assert!(compaction_bind_execution_turn(db, "operation", "turn").await?);
        assert!(!compaction_bind_execution_turn(db, "operation", "turn").await?);
        assert!(
            compaction_bind_execution_turn(db, "operation", "other")
                .await
                .is_err()
        );
        assert!(!compaction_execution_cancelled(db, "operation").await?);
        let tx = db.begin().await?;
        tx.execute_unprepared(
            "INSERT INTO compaction_execution_stop (owner,turn_id) VALUES ('owner','turn')",
        )
        .await?;
        assert!(compaction_execution_cancelled(&tx, "operation").await?);
        assert!(!compaction_execution_cancelled(db, "operation").await?);
        tx.commit().await?;
        assert!(compaction_execution_cancelled(db, "operation").await?);
        assert!(
            !compaction_finish_execution(
                db,
                "operation",
                Some("other"),
                1234,
                "cancelled",
                "cancelled"
            )
            .await?
        );
        assert!(
            !compaction_finish_execution(
                db,
                "operation",
                Some("turn"),
                1233,
                "cancelled",
                "cancelled"
            )
            .await?
        );

        let tx = db.begin().await?;
        assert!(
            compaction_finish_execution(
                &tx,
                "operation",
                Some("turn"),
                1234,
                "cancelled",
                "cancelled"
            )
            .await?
        );
        let (pending, won) = compaction_reconcile_runner_state(&tx, "operation").await?;
        assert!(won);
        let pending = pending.context("missing terminal state")?;
        assert_eq!(
            pending.phase,
            RunnerPhase::Failed {
                kind: FailureKind::Cancelled
            }
        );
        assert_eq!(pending.generation, initial.generation + 1);
        assert_eq!(
            compaction_reconcile_runner_state(db, "operation").await?,
            (Some(initial.clone()), false)
        );
        tx.rollback().await?;
        assert_eq!(
            compaction_reconcile_runner_state(db, "operation").await?,
            (Some(initial), false)
        );

        assert!(
            compaction_finish_execution(db, "operation", None, 1234, "cancelled", "cancelled")
                .await?
        );
        assert!(
            !compaction_finish_execution(db, "operation", None, 1234, "cancelled", "cancelled")
                .await?
        );
        let (a, b) = tokio::try_join!(
            compaction_reconcile_runner_state(db, "operation"),
            compaction_reconcile_runner_state(db, "operation"),
        )?;
        assert_ne!(a.1, b.1);
        assert_eq!(a.0, Some(pending.clone()));
        assert_eq!(b.0, Some(pending));
        Ok(())
    })
    .await
}

async fn seed_runner(db: &impl ConnectionTrait) -> Result<()> {
    db.execute_unprepared(
        r#"
        INSERT INTO workspace (id, name) VALUES ('w', 'Workspace');
        INSERT INTO thread (id, workspace_id, preview, mode, model, model_provider, status)
            VALUES ('thread', 'w', '', 'chat', 'model', 'provider', 'active');
        INSERT INTO turn (id, thread_id, status)
            VALUES ('turn', 'thread', 'in_progress'), ('other', 'thread', 'completed');
        INSERT INTO compaction_context (owner, workspace_id, thread_id)
            VALUES ('owner', 'w', 'thread');
        INSERT INTO compaction_operation (id, owner, fingerprint, status, snapshot, deadline_ms)
            VALUES ('operation', 'owner', 'fingerprint', 'running', '{}', 1234);
    "#,
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn durable_retry_budget_survives_pool_reopen() -> Result<()> {
    use pioneer_crud_compaction::operations::{compaction_claim_attempt, compaction_operation};

    with_database(async |fixture| {
        let first = fixture.connect().await?;
        seed_runner(&first).await?;
        assert!(compaction_claim_attempt(&first, "operation", 0, 20, false, false).await?);
        assert!(!compaction_claim_attempt(&first, "operation", 0, 20, false, false).await?);
        assert!(compaction_claim_attempt(&first, "operation", 1, 20, true, false).await?);
        first.close().await?;

        let restarted = fixture.connect().await?;
        assert!(compaction_claim_attempt(&restarted, "operation", 2, 20, true, true).await?);
        assert!(!compaction_claim_attempt(&restarted, "operation", 3, 20, true, false).await?);
        assert!(!compaction_claim_attempt(&restarted, "operation", 3, 20, false, true).await?);
        let operation = compaction_operation(&restarted, "operation")
            .await?
            .context("admitted compaction operation")?;
        assert!(
            !compaction_claim_attempt(
                &restarted,
                "operation",
                3,
                operation.deadline_ms,
                false,
                false,
            )
            .await?
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn runner_activation_is_idempotent_and_freezes_the_complete_manifest() -> Result<()> {
    use pioneer_compaction::SourceRef;
    use pioneer_crud_compaction::runner_repository::{ManifestEntry, compaction_runner_state};
    use pioneer_crud_compaction::runner_writes::{
        compaction_activate_runner, compaction_append_manifest, compaction_runner_transition,
    };
    use pioneer_sqlite::SqliteDatabase;

    with_database(async |fixture| {
        let writer = fixture.connection();
        seed_runner(writer).await?;
        let db = SqliteDatabase::new(fixture.connect_read_only().await?, writer.clone());
        let other =
            SqliteDatabase::new(fixture.connect_read_only().await?, fixture.connect().await?);
        let budget = ModelBudget::new(None, None, None);
        let state = RunnerState::new(1234, &budget, 100, None)?;
        assert!(compaction_prepare_runner(writer, "operation", &budget, 1, 0).await?);
        assert!(
            compaction_activate_runner(&db, "operation", &state)
                .await
                .is_err()
        );

        let entry = ManifestEntry {
            ordinal: 0,
            unit: 0,
            reference_only: false,
            thread_id: "thread".into(),
            source: SourceRef {
                scope: "checkpoint:owner".into(),
                id: "previous".into(),
                version: "previous-identity".into(),
            },
        };
        compaction_append_manifest(&db, "operation", std::slice::from_ref(&entry)).await?;
        compaction_append_manifest(&db, "operation", std::slice::from_ref(&entry)).await?;
        let mut conflict = entry.clone();
        conflict.source.version = "conflicting".into();
        assert!(
            compaction_append_manifest(&db, "operation", &[conflict])
                .await
                .is_err()
        );

        let (first, second) = tokio::try_join!(
            compaction_activate_runner(&db, "operation", &state),
            compaction_activate_runner(&other, "operation", &state),
        )?;
        assert_ne!(first, second);
        assert!(
            compaction_runner_plan(writer, "operation")
                .await?
                .unwrap()
                .ready
        );
        assert_eq!(
            compaction_manifest_page(writer, "operation", false, 0, 0).await?[0].source,
            entry.source,
        );
        assert!(
            compaction_append_manifest(&db, "operation", &[entry])
                .await
                .is_err()
        );
        let mut next = state.clone();
        next.generation = 1;
        next.phase = RunnerPhase::Failed {
            kind: FailureKind::Deadline,
        };
        assert!(compaction_runner_transition(&db, "operation", 0, &next, None).await?);
        assert!(!compaction_runner_transition(&other, "operation", 0, &next, None).await?);
        assert_eq!(
            compaction_runner_state(writer, "operation").await?,
            Some(next)
        );
        Ok(())
    })
    .await
}
