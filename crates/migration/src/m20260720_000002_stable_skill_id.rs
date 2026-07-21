use crate::stable_skill_id::LEGACY_RELATION_TABLES;
use sea_orm_migration::{
    prelude::*,
    schema::{boolean, string, text, timestamp_with_time_zone},
};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        tracing::info!(
            migration = "stable_skill_id",
            "stable SkillId schema migration started"
        );

        drop_conflicting_legacy_indexes(manager).await?;
        for (canonical, legacy) in LEGACY_RELATION_TABLES {
            manager
                .rename_table(
                    Table::rename()
                        .table(Alias::new(canonical), Alias::new(legacy))
                        .to_owned(),
                )
                .await?;
        }
        create_stable_tables(manager).await?;
        create_stable_indexes(manager).await?;

        tracing::info!(
            migration = "stable_skill_id",
            "stable SkillId schema migration completed; data backfill is deferred"
        );
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for (canonical, _) in LEGACY_RELATION_TABLES.into_iter().rev() {
            manager
                .drop_table(
                    Table::drop()
                        .table(Alias::new(canonical))
                        .if_exists()
                        .to_owned(),
                )
                .await?;
        }
        for (_, legacy) in LEGACY_RELATION_TABLES.into_iter().rev() {
            manager
                .drop_table(
                    Table::drop()
                        .table(Alias::new(legacy))
                        .if_exists()
                        .to_owned(),
                )
                .await?;
        }

        create_legacy_tables(manager).await?;
        create_legacy_indexes(manager).await?;
        Ok(())
    }

    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }
}

async fn drop_conflicting_legacy_indexes(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    for name in [
        "idx_turn_skill_binding_turn_id",
        "idx_skill_workspace_policy_workspace",
    ] {
        manager
            .drop_index(Index::drop().name(name).if_exists().to_owned())
            .await?;
    }
    Ok(())
}

async fn create_stable_tables(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table("skill_installation")
                .col(string("id").string_len(21).primary_key())
                .col(string("owner").string_len(255).null())
                .col(string("slug").string_len(255))
                .col(string("version").string_len(64).null())
                .col(string("source_kind").string_len(32))
                .col(string("scope_key").string_len(255))
                .col(text("source_ref"))
                .col(text("install_path"))
                .col(string("trust_level").string_len(32))
                .col(string("fingerprint").string_len(128))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                .to_owned(),
        )
        .await?;
    manager
        .create_table(
            Table::create()
                .table("skill_workspace_policy")
                .col(string("id").string_len(21).primary_key())
                .col(string("workspace_id").string_len(21))
                .col(string("skill_id").string_len(21))
                .col(boolean("enabled").null())
                .col(boolean("allow_implicit_invocation").null())
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                .to_owned(),
        )
        .await?;
    manager
        .create_table(
            Table::create()
                .table("turn_skill_binding")
                .col(string("id").string_len(21).primary_key())
                .col(string("turn_id").string_len(21))
                .col(string("skill_id").string_len(21))
                .col(string("skill_owner").string_len(255).null())
                .col(string("skill_slug").string_len(255))
                .col(string("skill_version").string_len(64).null())
                .col(string("fingerprint").string_len(128))
                .col(string("source_kind").string_len(32))
                .col(string("resolved_reason").string_len(32))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .to_owned(),
        )
        .await?;
    manager
        .create_table(
            Table::create()
                .table("skill_audit_event")
                .col(string("id").string_len(21).primary_key())
                .col(string("turn_id").string_len(21).null())
                .col(string("skill_id").string_len(21))
                .col(string("skill_owner").string_len(255).null())
                .col(string("skill_slug").string_len(255))
                .col(string("source_kind").string_len(32))
                .col(string("action").string_len(64))
                .col(string("decision").string_len(32))
                .col(string("reason_code").string_len(128).null())
                .col(text("details_json").default("{}"))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .to_owned(),
        )
        .await?;
    manager
        .create_table(
            Table::create()
                .table("skill_dependency_snapshot")
                .col(string("id").string_len(21).primary_key())
                .col(string("turn_id").string_len(21).null())
                .col(string("skill_id").string_len(21))
                .col(string("skill_owner").string_len(255).null())
                .col(string("skill_slug").string_len(255))
                .col(string("source_kind").string_len(32))
                .col(text("diagnostics_json").default("[]"))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .to_owned(),
        )
        .await?;
    Ok(())
}

async fn create_stable_indexes(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    for statement in [
        Index::create()
            .name("idx_turn_skill_binding_turn_id")
            .table("turn_skill_binding")
            .col("turn_id")
            .to_owned(),
        Index::create()
            .name("uq_turn_skill_binding_turn_id_skill_id")
            .table("turn_skill_binding")
            .col("turn_id")
            .col("skill_id")
            .unique()
            .to_owned(),
        Index::create()
            .name("idx_skill_audit_event_skill_id_created_at")
            .table("skill_audit_event")
            .col("skill_id")
            .col("created_at")
            .to_owned(),
        Index::create()
            .name("idx_skill_audit_event_turn_id")
            .table("skill_audit_event")
            .col("turn_id")
            .to_owned(),
        Index::create()
            .name("idx_skill_dependency_snapshot_skill_id_created_at")
            .table("skill_dependency_snapshot")
            .col("skill_id")
            .col("created_at")
            .to_owned(),
        Index::create()
            .name("idx_skill_dependency_snapshot_turn_id")
            .table("skill_dependency_snapshot")
            .col("turn_id")
            .to_owned(),
        Index::create()
            .name("idx_skill_installation_scope_source")
            .table("skill_installation")
            .col("scope_key")
            .col("source_kind")
            .to_owned(),
        Index::create()
            .name("idx_skill_installation_source_ref")
            .table("skill_installation")
            .col("source_ref")
            .to_owned(),
        Index::create()
            .name("idx_skill_workspace_policy_workspace")
            .table("skill_workspace_policy")
            .col("workspace_id")
            .to_owned(),
        Index::create()
            .name("uq_skill_workspace_policy_workspace_skill_id")
            .table("skill_workspace_policy")
            .col("workspace_id")
            .col("skill_id")
            .unique()
            .to_owned(),
    ] {
        manager.create_index(statement).await?;
    }
    Ok(())
}

async fn create_legacy_tables(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table("turn_skill_binding")
                .col(string("id").string_len(21).primary_key())
                .col(string("turn_id").string_len(21))
                .col(string("skill_slug").string_len(255))
                .col(string("skill_version").string_len(64).null())
                .col(string("fingerprint").string_len(128))
                .col(string("source_kind").string_len(32))
                .col(string("resolved_reason").string_len(32))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .to_owned(),
        )
        .await?;
    manager
        .create_table(
            Table::create()
                .table("skill_installation")
                .col(string("id").string_len(21).primary_key())
                .col(string("slug").string_len(255))
                .col(string("version").string_len(64).null())
                .col(string("source_kind").string_len(32))
                .col(string("scope_key").string_len(255))
                .col(text("source_ref"))
                .col(text("install_path"))
                .col(string("trust_level").string_len(32))
                .col(string("fingerprint").string_len(128))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                .to_owned(),
        )
        .await?;
    manager
        .create_table(
            Table::create()
                .table("skill_audit_event")
                .col(string("id").string_len(21).primary_key())
                .col(string("turn_id").string_len(21).null())
                .col(string("skill_slug").string_len(255))
                .col(string("source_kind").string_len(32))
                .col(string("action").string_len(64))
                .col(string("decision").string_len(32))
                .col(string("reason_code").string_len(128).null())
                .col(text("details_json").default("{}"))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .to_owned(),
        )
        .await?;
    manager
        .create_table(
            Table::create()
                .table("skill_dependency_snapshot")
                .col(string("id").string_len(21).primary_key())
                .col(string("turn_id").string_len(21).null())
                .col(string("skill_slug").string_len(255))
                .col(string("source_kind").string_len(32))
                .col(text("diagnostics_json").default("[]"))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .to_owned(),
        )
        .await?;
    manager
        .create_table(
            Table::create()
                .table("skill_workspace_policy")
                .col(string("id").string_len(21).primary_key())
                .col(string("workspace_id").string_len(21))
                .col(string("skill_slug").string_len(255))
                .col(string("source_kind").string_len(32))
                .col(boolean("enabled").null())
                .col(boolean("allow_implicit_invocation").null())
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                .to_owned(),
        )
        .await?;
    Ok(())
}

async fn create_legacy_indexes(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    for statement in [
        Index::create()
            .name("idx_turn_skill_binding_turn_id")
            .table("turn_skill_binding")
            .col("turn_id")
            .to_owned(),
        Index::create()
            .name("uq_turn_skill_binding_turn_id_skill_source")
            .table("turn_skill_binding")
            .col("turn_id")
            .col("skill_slug")
            .col("source_kind")
            .unique()
            .to_owned(),
        Index::create()
            .name("uq_skill_installation_slug_source_scope")
            .table("skill_installation")
            .col("slug")
            .col("source_kind")
            .col("scope_key")
            .unique()
            .to_owned(),
        Index::create()
            .name("idx_skill_installation_scope")
            .table("skill_installation")
            .col("scope_key")
            .to_owned(),
        Index::create()
            .name("idx_skill_audit_event_slug_created_at")
            .table("skill_audit_event")
            .col("skill_slug")
            .col("created_at")
            .to_owned(),
        Index::create()
            .name("idx_skill_audit_event_turn_skill_source")
            .table("skill_audit_event")
            .col("turn_id")
            .col("skill_slug")
            .col("source_kind")
            .to_owned(),
        Index::create()
            .name("idx_skill_dependency_snapshot_turn_skill_source")
            .table("skill_dependency_snapshot")
            .col("turn_id")
            .col("skill_slug")
            .col("source_kind")
            .to_owned(),
        Index::create()
            .name("idx_skill_workspace_policy_workspace")
            .table("skill_workspace_policy")
            .col("workspace_id")
            .to_owned(),
        Index::create()
            .name("uq_skill_workspace_policy_workspace_skill_source")
            .table("skill_workspace_policy")
            .col("workspace_id")
            .col("skill_slug")
            .col("source_kind")
            .unique()
            .to_owned(),
    ] {
        manager.create_index(statement).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Migrator, MigratorTrait, stable_skill_id::LEGACY_SKILL_INSTALLATION_TABLE};
    use sea_orm_migration::sea_orm::{ConnectionTrait, Database, Statement};

    async fn apply_pre_stable_migrations(db: &sea_orm::DatabaseConnection) {
        let migration_count = Migrator::migrations().len();
        Migrator::up(db, Some((migration_count - 1) as u32))
            .await
            .expect("pre-Stable-SkillId migrations should apply");
    }

    async fn columns(db: &sea_orm::DatabaseConnection, table: &str) -> Vec<String> {
        db.query_all_raw(Statement::from_string(
            db.get_database_backend(),
            format!("PRAGMA table_info('{table}')"),
        ))
        .await
        .expect("table columns should query")
        .into_iter()
        .map(|row| row.try_get("", "name").expect("column name should decode"))
        .collect()
    }

    async fn count(db: &sea_orm::DatabaseConnection, table: &str) -> i64 {
        db.query_one_raw(Statement::from_string(
            db.get_database_backend(),
            format!("SELECT COUNT(*) AS count FROM {table}"),
        ))
        .await
        .expect("table count should query")
        .expect("table count row should exist")
        .try_get("", "count")
        .expect("table count should decode")
    }

    async fn skill_schema_signature(
        db: &sea_orm::DatabaseConnection,
    ) -> Vec<(String, String, String)> {
        let tables = [
            "skill_installation",
            "skill_workspace_policy",
            "turn_skill_binding",
            "skill_audit_event",
            "skill_dependency_snapshot",
        ];
        let mut signature = Vec::new();
        for table in tables {
            for row in db
                .query_all_raw(Statement::from_string(
                    db.get_database_backend(),
                    format!("PRAGMA table_info('{table}')"),
                ))
                .await
                .expect("table signature should query")
            {
                signature.push((
                    table.to_owned(),
                    row.try_get::<String>("", "name").unwrap(),
                    format!(
                        "{}:{}:{}:{}",
                        row.try_get::<String>("", "type").unwrap(),
                        row.try_get::<i64>("", "notnull").unwrap(),
                        row.try_get::<i64>("", "pk").unwrap(),
                        row.try_get::<Option<String>>("", "dflt_value")
                            .unwrap()
                            .unwrap_or_default()
                    ),
                ));
            }
        }
        for row in db
            .query_all_raw(Statement::from_string(
                db.get_database_backend(),
                "SELECT tbl_name, name, COALESCE(sql, '') AS sql FROM sqlite_master \
                 WHERE type = 'index' AND tbl_name IN (\
                    'skill_installation', 'skill_workspace_policy', 'turn_skill_binding', \
                    'skill_audit_event', 'skill_dependency_snapshot'\
                 ) ORDER BY tbl_name, name"
                    .to_owned(),
            ))
            .await
            .expect("index signature should query")
        {
            signature.push((
                row.try_get("", "tbl_name").unwrap(),
                row.try_get("", "name").unwrap(),
                row.try_get("", "sql").unwrap(),
            ));
        }
        signature
    }

    #[tokio::test]
    async fn up_changes_only_schema_and_leaves_legacy_rows_for_background_backfill() {
        let db = Database::connect("sqlite::memory:")
            .await
            .expect("sqlite memory database should open");
        apply_pre_stable_migrations(&db).await;
        let deferred_payload = r#"{"type":"userMessage","attachments":[{"type":"skill","capability":{"slug":"owner/example","sourceKind":"user"}}"#;
        db.execute_unprepared(
            r#"
            INSERT INTO skill_installation (
                id, slug, source_kind, scope_key, source_ref, install_path,
                trust_level, fingerprint
            ) VALUES (
                'AAAAAAAAAAAAAAAAAAAAA', 'owner/example', 'user',
                'WWWWWWWWWWWWWWWWWWWWW', 'source', '/legacy/example',
                'community', 'fingerprint'
            );
            "#,
        )
        .await
        .expect("legacy installation should insert");
        db.execute_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            "INSERT INTO turn_item (id, turn_id, item_id, item_type, payload) VALUES ('IIIIIIIIIIIIIIIIIIIII', 'TTTTTTTTTTTTTTTTTTTTT', 'message-1', 'user_message', ?)",
            [deferred_payload.to_owned().into()],
        ))
        .await
        .expect("deferred malformed identity payload should insert as text");

        Migrator::up(&db, None)
            .await
            .expect("Stable SkillId schema migration should apply");

        assert_eq!(count(&db, "skill_installation").await, 0);
        assert_eq!(count(&db, LEGACY_SKILL_INSTALLATION_TABLE).await, 1);
        assert_eq!(
            columns(&db, "skill_installation").await,
            vec![
                "id",
                "owner",
                "slug",
                "version",
                "source_kind",
                "scope_key",
                "source_ref",
                "install_path",
                "trust_level",
                "fingerprint",
                "created_at",
                "updated_at",
            ]
        );
        assert_eq!(
            columns(&db, LEGACY_SKILL_INSTALLATION_TABLE).await,
            vec![
                "id",
                "slug",
                "version",
                "source_kind",
                "scope_key",
                "source_ref",
                "install_path",
                "trust_level",
                "fingerprint",
                "created_at",
                "updated_at",
            ]
        );
        let persisted_payload = db
            .query_one_raw(Statement::from_string(
                db.get_database_backend(),
                "SELECT payload FROM turn_item WHERE id = 'IIIIIIIIIIIIIIIIIIIII'".to_owned(),
            ))
            .await
            .expect("deferred payload should query")
            .expect("deferred payload should remain present")
            .try_get::<String>("", "payload")
            .expect("deferred payload should decode");
        assert_eq!(persisted_payload, deferred_payload);
    }

    #[tokio::test]
    async fn down_recreates_only_the_legacy_schema() {
        let expected = Database::connect("sqlite::memory:")
            .await
            .expect("expected sqlite memory database should open");
        apply_pre_stable_migrations(&expected).await;
        let expected_signature = skill_schema_signature(&expected).await;

        let db = Database::connect("sqlite::memory:")
            .await
            .expect("sqlite memory database should open");
        Migrator::up(&db, None)
            .await
            .expect("all migrations should apply");
        db.execute_unprepared(
            r#"
            INSERT INTO skill_installation (
                id, owner, slug, source_kind, scope_key, source_ref, install_path,
                trust_level, fingerprint
            ) VALUES (
                'AAAAAAAAAAAAAAAAAAAAA', 'owner', 'example', 'user',
                'WWWWWWWWWWWWWWWWWWWWW', 'source', '/new/example',
                'community', 'fingerprint'
            );
            "#,
        )
        .await
        .expect("stable installation should insert");
        for (_, legacy) in LEGACY_RELATION_TABLES {
            db.execute_unprepared(format!("DROP TABLE {legacy}").as_str())
                .await
                .expect("completed background backfill should have dropped legacy table");
        }

        Migrator::down(&db, Some(1))
            .await
            .expect("Stable SkillId schema down migration should apply");

        let columns = columns(&db, "skill_installation").await;
        assert!(columns.contains(&"slug".to_owned()));
        assert!(!columns.contains(&"owner".to_owned()));
        assert_eq!(count(&db, "skill_installation").await, 0);
        assert_eq!(skill_schema_signature(&db).await, expected_signature);
        let legacy_table = db
            .query_one_raw(Statement::from_sql_and_values(
                db.get_database_backend(),
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
                [LEGACY_SKILL_INSTALLATION_TABLE.into()],
            ))
            .await
            .expect("sqlite schema should query");
        assert!(legacy_table.is_none());
    }

    #[tokio::test]
    async fn fresh_and_staged_upgrades_have_the_same_stable_schema() {
        let fresh = Database::connect("sqlite::memory:")
            .await
            .expect("fresh database should open");
        Migrator::up(&fresh, None)
            .await
            .expect("fresh database should migrate");

        let upgraded = Database::connect("sqlite::memory:")
            .await
            .expect("upgrade database should open");
        apply_pre_stable_migrations(&upgraded).await;
        Migrator::up(&upgraded, None)
            .await
            .expect("upgrade database should migrate");

        assert_eq!(
            skill_schema_signature(&fresh).await,
            skill_schema_signature(&upgraded).await
        );
    }
}
