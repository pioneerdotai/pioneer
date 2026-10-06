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
}
