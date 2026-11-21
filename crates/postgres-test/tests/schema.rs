use anyhow::{Context, Result};
use migration::{Migrator, MigratorTrait};
use pioneer_postgres_test::{
    scoped_database, with_database, with_empty_database, with_test_databases,
};
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
async fn scoped_fixtures_clone_independently_and_close_after_panic() -> Result<()> {
    with_database(async |observer| {
        let (send, receive) = tokio::sync::oneshot::channel();
        let result = tokio::spawn(with_test_databases(async move {
            let first = scoped_database().await.unwrap();
            let second = scoped_database().await.unwrap();
            let reader = first.connect_read_only().await.unwrap();
            first
                .connection()
                .execute_unprepared("INSERT INTO workspace(id,name) VALUES ('scope-check','Test')")
                .await
                .unwrap();
            let query = "SELECT count(*) AS value FROM workspace WHERE id='scope-check'";
            assert_eq!(scalar::<i64>(&reader, query, []).await.unwrap(), 1);
            assert_eq!(
                scalar::<i64>(second.connection(), query, []).await.unwrap(),
                0
            );
            let names = [first.url().path(), second.url().path()]
                .map(|path| path.trim_start_matches('/').to_owned());
            send.send((names, reader)).unwrap();
            panic!("test fixture panic");
        }))
        .await;
        assert!(result.unwrap_err().is_panic());
        let (names, reader) = receive.await?;
        assert!(reader.ping().await.is_err());
        for name in names {
            assert!(
                !scalar::<bool>(
                    observer.connection(),
                    "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname=$1) AS value",
                    [name.into()],
                )
                .await?
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

#[tokio::test]
async fn installs_and_rolls_back_conversation_execution_and_package_schema() -> Result<()> {
    use sea_orm::EntityTrait;

    with_empty_database(async |fixture| {
        let db = fixture.connection();
        Migrator::up(db, None).await?;

        for (table, columns, indexes) in [
            ("turn_event", &["idempotency_key"][..], &[][..]),
            (
                "turn_llm_context",
                &[
                    "id",
                    "turn_id",
                    "item_id",
                    "attempt_id",
                    "sequence",
                    "source",
                    "tool_name",
                    "payload",
                    "output_policy_snapshot",
                    "delivery_key",
                    "created_at",
                    "expires_at",
                ][..],
                &[
                    "idx_turn_llm_context_turn_id",
                    "uq_turn_llm_context_turn_id_sequence",
                    "idx_turn_llm_context_turn_item",
                    "idx_turn_llm_context_expires_at",
                    "uq_turn_llm_context_turn_id_delivery_key",
                ][..],
            ),
            (
                "turn_execution_window",
                &[
                    "id",
                    "workspace_id",
                    "thread_id",
                    "turn_id",
                    "window_index",
                    "status",
                    "exhaustion_reason",
                    "agent_round_count",
                    "tool_call_count",
                    "provider_token_count",
                    "metadata_json",
                    "started_at",
                    "completed_at",
                    "created_at",
                    "updated_at",
                ][..],
                &[
                    "uidx_turn_execution_window_turn_index",
                    "idx_turn_execution_window_turn_id",
                    "idx_turn_execution_window_thread_turn",
                    "idx_turn_execution_window_status",
                ][..],
            ),
            (
                "turn_execution_checkpoint",
                &[
                    "id",
                    "window_id",
                    "workspace_id",
                    "thread_id",
                    "turn_id",
                    "checkpoint_kind",
                    "payload_json",
                    "created_at",
                ][..],
                &[
                    "idx_turn_execution_checkpoint_window",
                    "idx_turn_execution_checkpoint_turn",
                    "idx_turn_execution_checkpoint_thread_turn",
                    "idx_turn_execution_checkpoint_kind",
                ][..],
            ),
        ] {
            let actual_columns = scalar::<Vec<String>>(
                db,
                "SELECT ARRAY(SELECT column_name::text FROM information_schema.columns
                    WHERE table_schema='public' AND table_name=$1) AS value",
                [table.into()],
            )
            .await?;
            let actual_indexes = scalar::<Vec<String>>(
                db,
                "SELECT ARRAY(SELECT indexname::text FROM pg_indexes
                    WHERE schemaname='public' AND tablename=$1) AS value",
                [table.into()],
            )
            .await?;
            for column in columns {
                assert!(
                    actual_columns.iter().any(|found| found == column),
                    "missing {table}.{column}"
                );
            }
            for index in indexes {
                assert!(
                    actual_indexes.iter().any(|found| found == index),
                    "missing {table} index {index}"
                );
            }
        }
        for index in [
            "uq_turn_llm_context_turn_id_sequence",
            "uidx_turn_execution_window_turn_index",
            "uq_mcp_server_installation_scope_name",
        ] {
            assert!(
                scalar::<bool>(
                    db,
                    "SELECT indisunique AS value FROM pg_index WHERE indexrelid=to_regclass($1)",
                    [index.into()],
                )
                .await?
            );
        }
        assert!(
            pioneer_entity::turn_llm_context::Entity::find()
                .all(db)
                .await?
                .is_empty()
        );
        assert!(
            pioneer_entity::turn_execution_window::Entity::find()
                .all(db)
                .await?
                .is_empty()
        );
        assert!(
            pioneer_entity::turn_execution_checkpoint::Entity::find()
                .all(db)
                .await?
                .is_empty()
        );

        db.execute_unprepared(
            "INSERT INTO mcp_server_installation (
                id, scope_kind, scope_key, name, source_kind, source_ref, transport_kind,
                transport_json, auth_json, secret_refs_json, enabled, allow_implicit_invocation,
                required, fingerprint
             ) VALUES ('mcp-first','workspace','default','fixture','config','{}','stdio',
                '{}','{}','[]',TRUE,TRUE,FALSE,'first')",
        )
        .await?;
        let error = db
            .execute_unprepared(
                "INSERT INTO mcp_server_installation (
                id, scope_kind, scope_key, name, source_kind, source_ref, transport_kind,
                transport_json, auth_json, secret_refs_json, enabled, allow_implicit_invocation,
                required, fingerprint
             ) VALUES ('mcp-second','workspace','default','fixture','config','{}','stdio',
                '{}','{}','[]',TRUE,TRUE,FALSE,'second')",
            )
            .await
            .expect_err("duplicate MCP installation scope/name must be rejected");
        assert!(
            error
                .to_string()
                .contains("uq_mcp_server_installation_scope_name")
        );

        let relations = [
            "turn_llm_context",
            "turn_event_delivery",
            "turn_execution_window",
            "turn_execution_checkpoint",
            "turn_admission",
            "turn_finalization",
            "recovery_terminalization_outbox",
            "mcp_server_installation",
            "mcp_server_catalog_snapshot",
            "mcp_audit_event",
            "turn_mcp_binding",
        ];
        for table in relations {
            assert!(
                scalar::<bool>(
                    db,
                    "SELECT to_regclass($1) IS NOT NULL AS value",
                    [table.into()]
                )
                .await?,
                "{table} must be installed",
            );
        }
        Migrator::reset(db).await?;
        for table in relations {
            assert!(
                scalar::<bool>(
                    db,
                    "SELECT to_regclass($1) IS NULL AS value",
                    [table.into()]
                )
                .await?,
                "{table} must be removed by reset",
            );
        }
        Ok(())
    })
    .await
}

#[tokio::test]
async fn installed_terminal_effect_and_recovery_guards_enforce_live_invariants() -> Result<()> {
    with_database(async |fixture| {
        let db = fixture.connection();
        for index in ["idx_native_terminal_effect_due", "idx_native_terminal_effect_completed"] {
            assert!(scalar::<bool>(db, "SELECT to_regclass($1) IS NOT NULL AS value", [index.into()]).await?);
        }
        assert!(scalar::<bool>(db,
            "SELECT
                EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid='native_terminal_effect_outbox'::regclass
                    AND attname='payload_identity_sha256' AND attnotnull) AND
                (SELECT atttypid FROM pg_attribute WHERE attrelid='native_terminal_effect_outbox'::regclass
                    AND attname='accepted_candidate_id') =
                (SELECT atttypid FROM pg_attribute WHERE attrelid='task_result_candidate'::regclass
                    AND attname='id') AS value",
            [],
        ).await?);

        db.execute_unprepared(
            "INSERT INTO workspace(id,name) VALUES ('effect-ws','Effect fixture');
             INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status)
             VALUES ('effect-thread','effect-ws','','agent','model','provider','active');
             INSERT INTO turn(id,thread_id,status) VALUES ('effect-turn','effect-thread','in_progress');
             INSERT INTO native_terminal_effect_outbox(
               effect_id,batch_id,workspace_id,thread_id,turn_id,runtime_generation,effect_kind,
               gate_kind,payload_json,payload_sha256,payload_identity_sha256,max_attempts,prepared_at
             ) VALUES ('effect','batch','effect-ws','effect-thread','effect-turn',1,
               'attached_task_cleanup','terminal_commit','{}',repeat('0',64),repeat('0',64),
               3,CURRENT_TIMESTAMP);
             INSERT INTO recovery_job(id,turn_id,item_id,item_type,status,trigger,action,resolution_pending)
             VALUES ('recovery','effect-turn','runtime:current','reasoning','pending',
               'runtime_failure','mark_failed',TRUE);",
        ).await?;
        for (sql, constraint) in [
            ("UPDATE native_terminal_effect_outbox SET status='ready', terminal_committed_at=CURRENT_TIMESTAMP WHERE effect_id='effect'", "native_terminal_effect_outbox_check2"),
            ("UPDATE native_terminal_effect_outbox SET attempt_count=4 WHERE effect_id='effect'", "native_terminal_effect_outbox_check"),
            ("INSERT INTO recovery_job(id,turn_id,item_id,item_type,status,trigger,action,resolution_pending) VALUES ('duplicate','effect-turn','runtime:duplicate','reasoning','pending','runtime_failure','mark_failed',TRUE)", "uq_recovery_job_unresolved_turn"),
        ] {
            let error = db.execute_unprepared(sql).await.expect_err("invalid persisted state must fail");
            assert!(error.to_string().contains(&format!("\"{constraint}\"")), "{error}");
        }
        assert_eq!(
            scalar::<String>(db, "SELECT status AS value FROM native_terminal_effect_outbox WHERE effect_id='effect'", []).await?,
            "prepared",
        );
        db.execute_unprepared("DELETE FROM turn WHERE id='effect-turn'").await?;
        assert_eq!(
            scalar::<i64>(db, "SELECT COUNT(*) AS value FROM native_terminal_effect_outbox WHERE effect_id='effect'", []).await?,
            0,
        );
        Ok(())
    }).await
}

#[tokio::test]
async fn task_occurrence_schema_and_migration_marker_roll_back_together() -> Result<()> {
    with_empty_database(async |fixture| {
        let db = fixture.connection();
        let version = "m20261119_000006_tasks";
        let preceding = Migrator::migrations()
            .iter()
            .position(|migration| migration.name() == version)
            .context("task migration is registered")?;
        Migrator::up(db, Some(u32::try_from(preceding)?)).await?;

        // This index is installed after both occurrence queues and their
        // sequences, so a name collision exercises rollback of those writes.
        db.execute_unprepared(
            "CREATE INDEX idx_task_occurrence_reconcile_scope ON workspace (id)",
        )
        .await?;
        let error = Migrator::up(db, Some(1)).await.expect_err("colliding index");
        assert!(error.to_string().contains("idx_task_occurrence_reconcile_scope"));
        assert_eq!(
            Migrator::get_pending_migrations(db).await?[0].name(),
            version
        );
        for relation in [
            "task_occurrence_reconcile_pending",
            "task_occurrence_reconcile_sequence",
            "task_occurrence_reconcile_scope",
            "task_run_occurrence_reconcile_pending",
            "task_run_occurrence_reconcile_sequence",
            "idx_task_occurrence_reconcile_due",
            "idx_task_run_occurrence_reconcile_due",
            "idx_task_run_task_id",
            "idx_task_run_status_id",
        ] {
            assert!(
                !scalar::<bool>(db, "SELECT to_regclass($1) IS NOT NULL AS value", [relation.into()]).await?,
                "{relation} survived a failed migration"
            );
        }
        db.execute_unprepared("DROP INDEX idx_task_occurrence_reconcile_scope").await?;
        Migrator::up(db, Some(1)).await?;
        assert_ne!(
            Migrator::get_pending_migrations(db).await?[0].name(),
            version
        );
        for query in [
            "SELECT generation AS value FROM task_occurrence_reconcile_sequence WHERE singleton=1",
            "SELECT generation AS value FROM task_run_occurrence_reconcile_sequence WHERE singleton=1",
            "SELECT count(*) AS value FROM task_occurrence_reconcile_pending",
            "SELECT count(*) AS value FROM task_run_occurrence_reconcile_pending",
        ] {
            assert_eq!(scalar::<i64>(db, query, []).await?, 0);
        }
        Migrator::down(db, Some(1)).await?;
        assert_eq!(
            Migrator::get_pending_migrations(db).await?[0].name(),
            version
        );
        assert!(
            !scalar::<bool>(
                db, "SELECT to_regclass('task_occurrence_reconcile_pending') IS NOT NULL AS value", []
            ).await?
        );
        assert!(
            !scalar::<bool>(
                db, "SELECT to_regclass('task_run_occurrence_reconcile_pending') IS NOT NULL AS value", []
            ).await?
        );
        Ok(())
    })
    .await
}
