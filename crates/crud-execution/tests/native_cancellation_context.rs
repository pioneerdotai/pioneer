use anyhow::{Context, Result};
use pioneer_crud_execution::native_cancellation_context as cancellation;
use pioneer_postgres_test::with_database;
use pioneer_protocol::NativeTerminalEffectPreparation;
use sea_orm::{ConnectionTrait, TransactionTrait};

#[tokio::test]
async fn registration_digest_and_owned_receipt_follow_the_same_commit() -> Result<()> {
    with_database(async |fixture| {
        let db = fixture.connection();
        db.execute_unprepared(
            "INSERT INTO workspace (id, name) VALUES ('workspace', 'Workspace');
             INSERT INTO thread (id, workspace_id, preview, mode, model, model_provider, status)
             VALUES ('thread', 'workspace', '', 'agent', 'model', 'provider', 'active');
             INSERT INTO turn (id, thread_id, status) VALUES ('turn', 'thread', 'in_progress');",
        )
        .await?;
        let writer = fixture.connect().await?;
        let plan = NativeTerminalEffectPreparation {
            batch_id: "batch".into(),
            workspace_id: "workspace".into(),
            thread_id: "thread".into(),
            turn_id: "turn".into(),
            runtime_generation: 1,
            effects: vec![],
        };
        let insert = writer.begin().await?;
        let candidate =
            cancellation::prepare_registration(&plan, "owner", chrono::Utc::now().fixed_offset())?;
        cancellation::insert_once(&insert, &plan, candidate, true, "owner").await?;
        assert!(cancellation::is_registered(&insert, "turn").await?);
        assert!(!cancellation::is_registered(db, "turn").await?);
        insert.commit().await?;

        let context = cancellation::load(db, "turn")
            .await?
            .context("cancellation intent not committed")?;
        assert_eq!(context.preparation, plan);
        assert!(context.accepted_event_id.is_none());
        let fence = cancellation::prepare_revalidation(&context, "owner");
        let mut tx = writer.begin().await?;
        cancellation::revalidate(&tx, fence.clone()).await?;
        cancellation::mark_accepted(&tx, "turn", "receipt").await?;
        assert!(cancellation::has_accepted_owned(&tx, "turn", "owner").await?);
        assert!(!cancellation::has_accepted_owned(db, "turn", "owner").await?);
        tx.rollback().await?;
        assert!(!cancellation::has_accepted(db, "turn").await?);

        tx = writer.begin().await?;
        cancellation::revalidate(&tx, fence.clone()).await?;
        cancellation::mark_accepted(&tx, "turn", "receipt").await?;
        tx.commit().await?;
        assert!(cancellation::revalidate(db, fence).await.is_err());
        assert!(
            cancellation::receipt_owned(db, "turn", "other-owner")
                .await?
                .is_none()
        );
        let receipt = cancellation::receipt_owned(db, "turn", "owner")
            .await?
            .context("receipt missing")?;
        assert_eq!(receipt.canonical_event_id, "receipt");
        db.execute_unprepared(
            "UPDATE native_cancellation_context SET context_json = '{}' WHERE turn_id = 'turn'",
        )
        .await?;
        assert!(cancellation::load(db, "turn").await.is_err());
        writer.close().await?;
        Ok(())
    })
    .await
}
