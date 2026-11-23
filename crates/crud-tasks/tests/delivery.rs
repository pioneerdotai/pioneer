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
async fn delivery_status_change_invalidates_recovery_with_nullable_and_filled_snapshots()
-> Result<()> {
    with_database(async |database| {
        let db = database.connection();
        for (id, result, error) in [
            ("missing", None, None),
            ("json-null", Some("null"), Some("null")),
            ("object", Some(r#"{ "result": 42 }"#), Some("{}")),
        ] {
            db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
                "INSERT INTO task_delivery (id, workspace_id, task_id, run_id, delivery_key, mode, status, result_snapshot_json, error_snapshot_json) VALUES ($1, 'w', 'task', 'run', $1, 'thread', 'delivering', $2::text::json, $3::text::json)",
                [id.into(), result.into(), error.into()]
            )).await?;
            db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
                "INSERT INTO task_delivery_recovery_retry SELECT id, NULL, attempt_count, updated_at, 'retry', 0, 1 FROM task_delivery WHERE id = $1",
                [id.into()],
            )).await?;
            db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
                "UPDATE task_delivery SET status = status WHERE id = $1",
                [id.into()],
            )).await?;
            assert!(scalar::<bool>(db, "SELECT EXISTS (SELECT FROM task_delivery_recovery_retry WHERE delivery_id = $1) AS value", [id.into()]).await?);
            db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
                "UPDATE task_delivery SET status = 'delivered' WHERE id = $1",
                [id.into()],
            )).await?;
            assert!(!scalar::<bool>(db, "SELECT EXISTS (SELECT FROM task_delivery_recovery_retry WHERE delivery_id = $1) AS value", [id.into()]).await?);
            assert_eq!(scalar::<Option<String>>(db, "SELECT result_snapshot_json::text AS value FROM task_delivery WHERE id = $1", [id.into()]).await?.as_deref(), result);
            db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
                "DELETE FROM task_delivery WHERE id = $1",
                [id.into()],
            )).await?;
        }
        Ok(())
    }).await
}

#[tokio::test]
async fn delivery_projection_changes_status_atomically_with_recovery_invalidation() -> Result<()> {
    use pioneer_crud_tasks::delivery::{
        find_delivery_by_id, prepare_delivery_projection, task_delivery_from_db_model,
        upsert_prepared_delivery,
    };
    use pioneer_entity::task_delivery_recovery_retry as retry;
    use pioneer_protocol::{
        TaskDelivery, TaskDeliveryMode, TaskDeliveryStatus, TaskError, TaskErrorClass, TaskResult,
        TaskValue,
    };
    use sea_orm::{EntityTrait, Set, TransactionTrait};

    with_database(async |database| {
        let db = database.connection();
        for (id, result_snapshot, error_snapshot) in [
            ("empty", None, None),
            (
                "result",
                Some(TaskResult {
                    summary: Some("строка\0".into()),
                    data: Some(TaskValue::Object([("null".into(), TaskValue::Null)].into())),
                    artifacts: Vec::new(),
                    completed_by_run_id: None,
                }),
                None,
            ),
            (
                "error",
                None,
                Some(TaskError {
                    code: "test_error".into(),
                    message: "Job failed".into(),
                    class: TaskErrorClass::Internal,
                    recovery_diagnostic: None,
                    details: None,
                    failed_run_id: None,
                }),
            ),
        ] {
            let mut delivery = TaskDelivery {
                id: id.into(),
                workspace_id: "workspace".into(),
                task_id: "task".into(),
                run_id: "run".into(),
                delivery_key: id.into(),
                mode: TaskDeliveryMode::Thread,
                thread_target: None,
                target_thread_id: Some("thread".into()),
                target_user_id: None,
                webhook_url: None,
                webhook_url_fingerprint: None,
                status: TaskDeliveryStatus::Delivering,
                next_attempt_at: None,
                attempt_count: 1,
                max_attempts: 2,
                result_snapshot,
                error_snapshot,
                delivered_turn_id: None,
                delivered_notification_id: None,
                delivered_at: None,
                last_error: None,
                created_at: 1000,
                updated_at: 1000,
            };
            upsert_prepared_delivery(db, prepare_delivery_projection(&delivery)?).await?;
            let stored = find_delivery_by_id(db, id)
                .await?
                .context("missing delivery")?;
            assert_eq!(task_delivery_from_db_model(stored.clone())?, delivery);
            retry::Entity::insert(retry::ActiveModel {
                delivery_id: Set(id.into()),
                expected_attempt_id: Set(None),
                expected_attempt_count: Set(1),
                expected_updated_at: Set(stored.updated_at),
                retry_token: Set("retry".into()),
                next_probe_at: Set(1100),
                attempts: Set(1),
            })
            .exec(db)
            .await?;

            delivery.status = TaskDeliveryStatus::Delivered;
            delivery.delivered_turn_id = Some("delivered-turn".into());
            delivery.delivered_at = Some(1001);
            delivery.updated_at = 1001;
            let prepared = prepare_delivery_projection(&delivery)?;

            let tx = db.begin().await?;
            upsert_prepared_delivery(&tx, prepared.clone()).await?;
            assert!(retry::Entity::find_by_id(id).one(&tx).await?.is_none());
            assert!(retry::Entity::find_by_id(id).one(db).await?.is_some());
            tx.rollback().await?;
            assert_eq!(find_delivery_by_id(db, id).await?, Some(stored));

            let tx = db.begin().await?;
            upsert_prepared_delivery(&tx, prepared).await?;
            tx.commit().await?;
            assert!(retry::Entity::find_by_id(id).one(db).await?.is_none());
            let stored = find_delivery_by_id(db, id)
                .await?
                .context("missing delivery")?;
            assert_eq!(task_delivery_from_db_model(stored)?, delivery);
        }
        Ok(())
    })
    .await
}

#[tokio::test]
async fn recovery_fence_uses_exact_snapshot_bytes() -> Result<()> {
    use pioneer_crud_tasks::delivery_recovery::{defer, matches, snapshot};
    use sea_orm::TransactionTrait;

    with_database(async |fixture| {
        let db = fixture.connection();
        db.execute_unprepared(
            r#"INSERT INTO task_delivery
               (id, workspace_id, task_id, run_id, delivery_key, mode, status, result_snapshot_json)
               VALUES ('delivery', 'workspace', 'task', 'run', 'delivery', 'thread', 'delivering',
                       '{ "summary": "done" }')"#,
        )
        .await?;
        let initial = snapshot(db, "delivery")
            .await?
            .context("missing delivery")?;
        let tx = db.begin().await?;
        assert!(matches(&tx, &initial).await?);
        assert!(defer(&tx, &initial, 100, "defer-once").await?);
        tx.rollback().await?;

        db.execute_unprepared(
            r#"UPDATE task_delivery SET result_snapshot_json = '{"summary":"done"}'
               WHERE id = 'delivery'"#,
        )
        .await?;
        let committed = db.begin().await?;
        assert!(!matches(&committed, &initial).await?);
        let current = snapshot(&committed, "delivery")
            .await?
            .context("missing updated delivery")?;
        assert!(defer(&committed, &current, 100, "valid-retry").await?);
        committed.commit().await?;
        assert_eq!(
            snapshot(db, "delivery")
                .await?
                .and_then(|value| value.retry)
                .context("missing retry")?
                .retry_token,
            "valid-retry",
        );
        Ok(())
    })
    .await
}
