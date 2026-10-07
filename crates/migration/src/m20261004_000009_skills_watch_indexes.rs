use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // One-time index builds read existing tables. No data/entity shape changes.
        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_skill_installation_source_scope_id")
                    .table(Alias::new("skill_installation"))
                    .col(Alias::new("source_kind"))
                    .col(Alias::new("scope_key"))
                    .col(Alias::new("id"))
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_skill_installation_import_provenance")
                    .table(Alias::new("skill_installation"))
                    .col(Alias::new("source_kind"))
                    .col(Alias::new("scope_key"))
                    .col(Alias::new("source_ref"))
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_workspace_active_id")
                    .table(Alias::new("workspace"))
                    .col(Alias::new("is_active"))
                    .col(Alias::new("id"))
                    .to_owned(),
            )
            .await
    }
    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for (name, table) in [
            (
                "idx_skill_installation_source_scope_id",
                "skill_installation",
            ),
            (
                "idx_skill_installation_import_provenance",
                "skill_installation",
            ),
            ("idx_workspace_active_id", "workspace"),
        ] {
            manager
                .drop_index(Index::drop().name(name).table(Alias::new(table)).to_owned())
                .await?;
        }
        Ok(())
    }
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_orm::{ConnectionTrait, Database, DbBackend, Statement};

    #[tokio::test]
    async fn installed_indexes_have_the_scoped_keyset_prefix_and_retry_down_up() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        crate::Migrator::up(&db, None).await.unwrap();
        let manager = SchemaManager::new(&db);
        Migration.up(&manager).await.unwrap(); // retry is idempotent
        for (name, columns) in [
            (
                "idx_skill_installation_source_scope_id",
                vec!["source_kind", "scope_key", "id"],
            ),
            (
                "idx_skill_installation_import_provenance",
                vec!["source_kind", "scope_key", "source_ref"],
            ),
            ("idx_workspace_active_id", vec!["is_active", "id"]),
        ] {
            let rows = db
                .query_all_raw(Statement::from_sql_and_values(
                    DbBackend::Sqlite,
                    "SELECT name FROM pragma_index_info(?) ORDER BY seqno",
                    [name.into()],
                ))
                .await
                .unwrap();
            let actual = rows
                .iter()
                .map(|row| row.try_get::<String>("", "name").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(actual, columns);
        }
        Migration.down(&manager).await.unwrap();
        assert!(
            !manager
                .has_index("workspace", "idx_workspace_active_id")
                .await
                .unwrap()
        );
        Migration.up(&manager).await.unwrap();
        assert!(
            manager
                .has_index("workspace", "idx_workspace_active_id")
                .await
                .unwrap()
        );
    }
    #[tokio::test]
    async fn partial_ddl_and_completion_marker_rollback_preserve_foreign_objects_and_sources() {
        use sea_orm::TransactionTrait;
        let db = Database::connect("sqlite::memory:").await.unwrap();
        let name = Migration.name();
        let before = crate::Migrator::migrations()
            .iter()
            .position(|m| m.name() == name)
            .unwrap() as u32;
        crate::Migrator::up(&db, Some(before)).await.unwrap();
        db.execute_unprepared("CREATE TABLE foreign_skills_object(value TEXT); CREATE INDEX foreign_skills_index ON foreign_skills_object(value); INSERT INTO foreign_skills_object VALUES('keep'); INSERT INTO workspace(id,name,is_active,is_current) VALUES('skills-migration','Keep',1,0); CREATE TABLE idx_workspace_active_id(value TEXT)").await.unwrap();
        db.execute_unprepared("INSERT INTO skill_installation(id,slug,source_kind,scope_key,source_ref,install_path,trust_level,fingerprint) VALUES('MMMMMMMMMMMMMMMMMMMMM','keep','user','skills-migration','original-source','/original/path','community','original-fingerprint')").await.unwrap();
        // The third DDL statement collides with a table, after both skill indexes
        // have been installed. The same transaction owns the completion marker.
        let transaction = db.begin().await.unwrap();
        assert!(crate::Migrator::up(&transaction, None).await.is_err());
        transaction.rollback().await.unwrap();
        for index in [
            "idx_skill_installation_source_scope_id",
            "idx_skill_installation_import_provenance",
        ] {
            assert!(
                !SchemaManager::new(&db)
                    .has_index("skill_installation", index)
                    .await
                    .unwrap()
            );
        }
        let marker = db
            .query_one_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "SELECT version FROM seaql_migrations WHERE version=?",
                [name.into()],
            ))
            .await
            .unwrap();
        assert!(marker.is_none());
        assert!(
            SchemaManager::new(&db)
                .has_table("idx_workspace_active_id")
                .await
                .unwrap()
        );
        db.execute_unprepared("DROP TABLE idx_workspace_active_id")
            .await
            .unwrap();
        crate::Migrator::up(&db, None).await.unwrap();
        crate::Migrator::up(&db, None).await.unwrap();
        let suffix = (crate::Migrator::migrations().len() - before as usize) as u32;
        crate::Migrator::down(&db, Some(suffix)).await.unwrap();
        crate::Migrator::up(&db, None).await.unwrap();
        assert!(
            SchemaManager::new(&db)
                .has_index("workspace", "idx_workspace_active_id")
                .await
                .unwrap()
        );
        assert!(
            SchemaManager::new(&db)
                .has_index("foreign_skills_object", "foreign_skills_index")
                .await
                .unwrap()
        );
        let source = db
            .query_one_raw(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT name FROM workspace WHERE id='skills-migration'".to_owned(),
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(source.try_get::<String>("", "name").unwrap(), "Keep");
        let skill = db.query_one_raw(Statement::from_string(DbBackend::Sqlite, "SELECT source_ref,fingerprint FROM skill_installation WHERE id='MMMMMMMMMMMMMMMMMMMMM'".to_owned())).await.unwrap().unwrap();
        assert_eq!(
            skill.try_get::<String>("", "source_ref").unwrap(),
            "original-source"
        );
        assert_eq!(
            skill.try_get::<String>("", "fingerprint").unwrap(),
            "original-fingerprint"
        );
        let foreign = db
            .query_one_raw(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT value FROM foreign_skills_object".to_owned(),
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(foreign.try_get::<String>("", "value").unwrap(), "keep");
    }
}
