use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Row-value seek, abandoned Running settlement and the next-run timer
        // use this same partial index.
        // ID bounds timestamp ties; ready/canceled history is outside its range.
        // Each index build reads existing jobs once; no table/data rewrite.
        manager
            .create_index(
                Index::create()
                    .name("idx_thread_episodic_jobs_runnable_seek")
                    .table("thread_episodic_index_jobs")
                    .col("next_run_at")
                    .col("created_at")
                    .col("id")
                    .cond_where(Expr::cust("status IN ('queued','failed','running')"))
                    .to_owned(),
            )
            .await
    }
    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_index(
                Index::drop()
                    .name("idx_thread_episodic_jobs_runnable_seek")
                    .table("thread_episodic_index_jobs")
                    .to_owned(),
            )
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Migrator;
    use sea_orm::{ConnectionTrait, Database, Statement};
    #[tokio::test]
    async fn runnable_seek_has_full_cursor_and_partial_predicate_and_rolls_back_named_suffix() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        Migrator::up(&db, None).await.unwrap();
        let columns = db
            .query_all_raw(Statement::from_string(
                db.get_database_backend(),
                "PRAGMA index_info('idx_thread_episodic_jobs_runnable_seek')".to_owned(),
            ))
            .await
            .unwrap();
        assert_eq!(
            columns
                .into_iter()
                .map(|row| row.try_get::<String>("", "name").unwrap())
                .collect::<Vec<_>>(),
            vec!["next_run_at", "created_at", "id"]
        );
        let definition = db.query_one_raw(Statement::from_string(db.get_database_backend(), "SELECT sql FROM sqlite_master WHERE name = 'idx_thread_episodic_jobs_runnable_seek'".to_owned())).await.unwrap().unwrap();
        assert!(
            definition
                .try_get::<String>("", "sql")
                .unwrap()
                .contains("status IN ('queued','failed','running')")
        );
        let migrations = Migrator::migrations();
        let position = migrations
            .iter()
            .position(|migration| {
                migration.name() == "m20261006_000002_thread_episodic_runnable_seek"
            })
            .unwrap();
        Migrator::down(&db, Some((migrations.len() - position) as u32))
            .await
            .unwrap();
        assert!(db.query_one_raw(Statement::from_string(db.get_database_backend(), "SELECT name FROM sqlite_master WHERE name = 'idx_thread_episodic_jobs_runnable_seek'".to_owned())).await.unwrap().is_none());
        let columns = db
            .query_all_raw(Statement::from_string(
                db.get_database_backend(),
                "PRAGMA index_info('idx_thread_episodic_index_jobs_due')".to_owned(),
            ))
            .await
            .unwrap();
        assert_eq!(
            columns
                .into_iter()
                .map(|row| row.try_get::<String>("", "name").unwrap())
                .collect::<Vec<_>>(),
            vec!["status", "next_run_at"]
        );
    }
}
