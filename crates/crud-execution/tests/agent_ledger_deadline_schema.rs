use anyhow::Result;
use migration::{Migrator, MigratorTrait};
use pioneer_entity::agent_action_outbox;
use pioneer_postgres_test::with_empty_database;
use sea_orm::{ActiveModelTrait, ConnectionTrait, DbBackend, EntityTrait, Set, Statement};

const INDEX: &str = "idx_agent_action_outbox_status_created_id";

#[tokio::test]
async fn ledger_deadline_index_is_reproducible_and_repeat_up_preserves_ledger_rows() -> Result<()> {
    with_empty_database(async |fixture| {
        let db = fixture.connection();
        assert!(index_columns(db).await?.is_empty());
        let execution_migration = "m20261119_000005_execution";
        let preceding = Migrator::migrations()
            .iter()
            .position(|migration| migration.name() == execution_migration)
            .expect("execution migration is registered");
        Migrator::up(db, Some(u32::try_from(preceding)?)).await?;
        db.execute_unprepared(
            "CREATE INDEX idx_agent_action_outbox_immediate ON workspace (id)",
        )
        .await?;
        let error = Migrator::up(db, Some(1)).await.expect_err("colliding index");
        assert!(error.to_string().contains("idx_agent_action_outbox_immediate"));
        assert_eq!(
            Migrator::get_pending_migrations(db).await?[0].name(),
            execution_migration,
        );
        let rollback = db
            .query_one_raw(Statement::from_string(
                DbBackend::Postgres,
                "SELECT to_regclass('agent_action_outbox') IS NULL
                    AND to_regclass('idx_agent_action_outbox_timed') IS NULL
                    AND to_regclass('idx_agent_action_outbox_exhausted') IS NULL
                    AND to_regprocedure('agent_action_outbox_start_candidate(text)') IS NULL
                    AS rolled_back",
            ))
            .await?
            .expect("catalog query returns one row");
        assert!(rollback.try_get::<bool>("", "rolled_back")?);
        db.execute_unprepared("DROP INDEX idx_agent_action_outbox_immediate").await?;
        Migrator::up(db, None).await?;
        let expected = ["status", "created_at", "id"];
        assert_eq!(index_columns(db).await?, expected);

        db.execute_unprepared(
            r#"
            INSERT INTO workspace (id, name) VALUES ('w', 'Workspace');
            INSERT INTO agent_identity (id, workspace_id, source_kind, source_id, source_fingerprint)
                VALUES ('Identity0000000000000', 'w', 'ephemeral', 'identity', 'fingerprint');
            INSERT INTO agent_execution (
                id, workspace_id, agent_identity_id, identity_source_revision, identity_source_fingerprint,
                home_root_thread_id, work_graph_root_execution_id, requested_identity_selection_json,
                requested_profile_selection_json, authorization_context_fingerprint
            ) VALUES (
                'Execution000000000000', 'w', 'Identity0000000000000', 1, 'fingerprint',
                'thread', 'Execution000000000000', '{}', '{}', 'authorization'
            );
            INSERT INTO agent_action (id, execution_id, action_kind, idempotency_key, request_fingerprint)
                VALUES ('action', 'Execution000000000000', 'start_agent', 'ledger-key', 'fingerprint');
            "#,
        )
        .await?;
        let created = chrono::DateTime::parse_from_rfc3339("2026-11-19T08:00:00.123Z")?;
        let original = agent_action_outbox::ActiveModel {
            id: Set("preserved-ledger-row".into()),
            action_id: Set("action".into()),
            owner_execution_id: Set("Execution000000000000".into()),
            payload_json: Set(r#"{ "kind": "start_agent" }"#.into()),
            status: Set("delivered".into()),
            attempts: Set(1),
            next_attempt_at: Set(None),
            delivered_at: Set(Some(created)),
            last_error: Set(None),
            created_at: Set(created),
        }
        .insert(db)
        .await?;
        Migrator::up(db, None).await?;
        assert_eq!(
            agent_action_outbox::Entity::find_by_id(&original.id).one(db).await?,
            Some(original),
        );
        assert_eq!(index_columns(db).await?, expected);

        // Pioneer 1.0 creates the index with the outbox table. Reset is the
        // schema lifecycle boundary, while repeating up preserves live data.
        Migrator::reset(db).await?;
        assert!(index_columns(db).await?.is_empty());
        Migrator::up(db, None).await?;
        assert_eq!(index_columns(db).await?, expected);
        Ok(())
    })
    .await
}

async fn index_columns(db: &impl ConnectionTrait) -> Result<Vec<String>> {
    db.query_all_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        r#"
        SELECT a.attname::text AS name
        FROM pg_index i
        CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, position)
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
        WHERE i.indexrelid = to_regclass($1)
        ORDER BY k.position
        "#,
        [INDEX.into()],
    ))
    .await?
    .into_iter()
    .map(|row| row.try_get("", "name").map_err(Into::into))
    .collect()
}
