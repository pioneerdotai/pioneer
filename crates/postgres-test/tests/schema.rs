use anyhow::{Context, Result};
use migration::{Migrator, MigratorTrait};
use pioneer_postgres_test::{with_database, with_empty_database};
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
async fn baseline_install_reset_and_reapply() -> Result<()> {
    with_empty_database(async |database| {
        let db = database.connection();
        Migrator::up(db, None).await?;
        Migrator::up(db, None).await?;
        assert!(Migrator::get_pending_migrations(db).await?.is_empty());
        assert!(
            scalar::<bool>(
                db,
                "SELECT '[1,2]'::vector <-> '[1,2]'::vector = 0 AS value",
                []
            )
            .await?
        );

        Migrator::reset(db).await?;
        assert!(
            !scalar::<bool>(
                db,
                "SELECT to_regclass('public.turn') IS NOT NULL AS value",
                []
            )
            .await?
        );
        assert!(
            !scalar::<bool>(
                db,
                "SELECT to_regprocedure('public.thread_episodic_search_keys(text)') IS NOT NULL AS value",
                []
            )
            .await?
        );

        Migrator::up(db, None).await?;
        assert!(Migrator::get_pending_migrations(db).await?.is_empty());
        Ok(())
    })
    .await
}

#[tokio::test]
async fn exhausted_start_candidate_handles_missing_kind_and_nested_nul() -> Result<()> {
    with_database(async |database| {
        let db = database.connection();
        for (payload, candidate) in [
            (r#"{"input":{"text":"\u0000"}}"#, true),
            (
                r#"{"input":{"kind":"child","text":"\u0000"},"kind":"start_agent"}"#,
                true,
            ),
            (
                r#"{"kind":"\u0073tart_agent","input":{"text":"\u0000"}}"#,
                true,
            ),
            (
                r#"{"kind":"stop_agent","input":{"kind":"start_agent","text":"\u0000"}}"#,
                false,
            ),
            (r#"{"kind":null}"#, true),
        ] {
            assert_eq!(
                scalar::<bool>(
                    db,
                    "SELECT agent_action_outbox_start_candidate($1) AS value",
                    [payload.into()]
                )
                .await?,
                candidate
            );
        }
        Ok(())
    })
    .await
}

#[tokio::test]
async fn conflicting_trigger_rolls_back_the_rule_migration_and_leaves_the_database_retryable()
-> Result<()> {
    with_empty_database(async |fixture| {
        let db = fixture.connection();
        let migration_count = u32::try_from(Migrator::migrations().len())?;
        Migrator::up(db, Some(migration_count - 1)).await?;
        db.execute_unprepared(
            "INSERT INTO workspace(id,name) VALUES ('rollback-fixture','Retained');
             CREATE FUNCTION fixture_task_event() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN RETURN NEW; END; $$;
             CREATE TRIGGER task_event_fanout_append AFTER INSERT ON task_event
             FOR EACH ROW EXECUTE FUNCTION fixture_task_event();",
        )
        .await?;
        let error = Migrator::up(db, None)
            .await
            .expect_err("repeated trigger name must fail");
        assert!(error.to_string().contains("task_event_fanout_append"));
        assert_eq!(Migrator::get_pending_migrations(db).await?.len(), 1);
        assert!(
            !scalar::<bool>(
                db,
                "SELECT to_regclass('public.compaction_live_sources') IS NOT NULL AS value",
                [],
            )
            .await?
        );

        db.execute_unprepared("DROP TRIGGER task_event_fanout_append ON task_event")
            .await?;
        Migrator::up(db, None).await?;
        assert!(Migrator::get_pending_migrations(db).await?.is_empty());
        assert!(
            scalar::<bool>(
                db,
                "SELECT to_regclass('public.compaction_live_sources') IS NOT NULL AS value",
                [],
            )
            .await?
        );
        assert_eq!(
            scalar::<String>(
                db,
                "SELECT name AS value FROM workspace WHERE id=$1",
                ["rollback-fixture".into()],
            )
            .await?,
            "Retained"
        );
        Ok(())
    })
    .await
}
