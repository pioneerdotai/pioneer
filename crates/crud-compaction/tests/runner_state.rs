use anyhow::{Context, Result};
use pioneer_postgres_test::with_database;
use sea_orm::{ConnectionTrait, DbBackend, Statement, Value};

async fn scalar<T: sea_orm::TryGetable>(
    db: &impl ConnectionTrait,
    sql: &str,
    values: impl IntoIterator<Item = Value>,
) -> Result<T> {
    db.query_one_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        sql,
        values,
    ))
    .await?
    .context("expected one row")?
    .try_get("", "value")
    .map_err(Into::into)
}

#[tokio::test]
async fn compaction_runner_state_update_requeues_lifecycle_and_preserves_json_bytes() -> Result<()>
{
    with_database(async |database| {
        let db = database.connection();
        db.execute_unprepared(r#"
            INSERT INTO workspace (id, name) VALUES ('w', 'Workspace');
            INSERT INTO thread (id, workspace_id, preview, mode, model, model_provider, status)
                VALUES ('thread', 'w', '', 'chat', 'model', 'provider', 'active');
            INSERT INTO compaction_context (workspace_id, thread_id, owner)
                VALUES ('w', 'thread', 'owner');
            INSERT INTO compaction_operation (id, owner, fingerprint, status, snapshot, deadline_ms)
                VALUES ('operation', 'owner', 'fingerprint', 'running', '{}', 15);
            INSERT INTO compaction_runner_state (operation_id, generation, state)
                VALUES ('operation', 1, '{ "phase": "Running" }');
        "#).await?;
        let generation_sql = "SELECT generation AS value FROM compaction_lifecycle_pending WHERE operation_id = 'operation'";
        let previous = scalar::<i64>(db, generation_sql, []).await?;
        db.execute_unprepared("UPDATE compaction_runner_state SET state = state WHERE operation_id = 'operation'").await?;
        assert_eq!(scalar::<i64>(db, generation_sql, []).await?, previous);
        let updated = r#"{ "phase": {"Failed": {"kind": "InvalidCompletion"}} }"#;
        use pioneer_crud_compaction::runner_state::{observation, update_runner_state};
        use pioneer_entity::compaction_runner_state as state;
        use sea_orm::{ColumnTrait, QueryFilter};
        update_runner_state(1, updated.to_owned())
            .filter(state::Column::OperationId.eq("operation"))
            .exec(db).await?;
        assert_eq!(observation(db, "operation").await?.context("missing state")?.state, updated);
        assert!(scalar::<i64>(db, generation_sql, []).await? > previous);
        assert_eq!(scalar::<String>(db, "SELECT state::text AS value FROM compaction_runner_state WHERE operation_id = 'operation'", []).await?, updated);
        db.execute_unprepared("DELETE FROM compaction_runner_state WHERE operation_id = 'operation'").await?;
        Ok(())
    }).await
}

#[tokio::test]
async fn runner_reads_committed_state_and_can_retry_rolled_back_update() -> Result<()> {
    use pioneer_compaction::ModelBudget;
    use pioneer_compaction::runner::{FailureKind, RunnerState};
    use pioneer_crud_compaction::runner_state::{insert_initial, observation, update_runner_state};
    use pioneer_entity::compaction_runner_state as state;
    use sea_orm::{ColumnTrait, QueryFilter, TransactionTrait};

    with_database(async |database| {
        let db = database.connection();
        db.execute_unprepared(
            r#"
            INSERT INTO workspace (id, name) VALUES ('w', 'Workspace');
            INSERT INTO thread (id, workspace_id, preview, mode, model, model_provider, status)
                VALUES ('thread', 'w', '', 'chat', 'model', 'provider', 'active');
            INSERT INTO compaction_context (workspace_id, thread_id, owner)
                VALUES ('w', 'thread', 'owner');
            INSERT INTO compaction_operation (id, owner, fingerprint, status, snapshot, deadline_ms)
                VALUES ('operation', 'owner', 'fingerprint', 'running', '{}', 1234);
        "#,
        )
        .await?;

        let initial = RunnerState::new(1234, &ModelBudget::new(None, None, None), 100, None)?;
        let initial_text = serde_json::to_string_pretty(&initial)?;
        assert!(insert_initial(db, "operation", initial_text.clone()).await?);
        assert!(!insert_initial(db, "operation", initial_text.clone()).await?);
        let read = observation(db, "operation")
            .await?
            .context("missing initial state")?;
        assert_eq!(read.state, initial_text);
        assert_eq!(read.decode()?, initial);

        let next = initial.terminate(FailureKind::Cancelled)?;
        let generation = i64::try_from(next.generation)?;
        let update = update_runner_state(generation, serde_json::to_string_pretty(&next)?)
            .filter(state::Column::OperationId.eq("operation"))
            .filter(state::Column::Generation.eq(0_i64));
        let tx = db.begin().await?;
        assert_eq!(update.clone().exec(&tx).await?.rows_affected, 1);
        assert_eq!(
            observation(&tx, "operation")
                .await?
                .context("missing next state")?
                .decode()?,
            next
        );
        assert_eq!(observation(db, "operation").await?, Some(read.clone()));
        tx.rollback().await?;
        assert_eq!(observation(db, "operation").await?, Some(read));

        let tx = db.begin().await?;
        assert_eq!(update.clone().exec(&tx).await?.rows_affected, 1);
        tx.commit().await?;
        assert_eq!(update.exec(db).await?.rows_affected, 0);
        assert_eq!(
            observation(db, "operation")
                .await?
                .context("missing committed state")?
                .decode()?,
            next
        );
        Ok(())
    })
    .await
}
