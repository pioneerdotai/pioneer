use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

// One-time index builds read existing jobs. They add no rows or obligations.
#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for (name, columns) in [
            (
                "idx_thread_episodic_jobs_workspace_due",
                &["workspace_id", "status", "next_run_at", "created_at"][..],
            ),
            (
                "idx_thread_episodic_jobs_workspace_recovery",
                &["workspace_id", "status", "updated_at", "id"][..],
            ),
        ] {
            let mut index = Index::create();
            index.name(name).table("thread_episodic_index_jobs");
            for column in columns {
                index.col(*column);
            }
            manager.create_index(index.to_owned()).await?;
        }
        // The readiness guard must not scan canceled superseded/deleted history.
        // Keep this predicate identical to the repository existence probe.
        manager.create_index(Index::create()
            .name("idx_thread_episodic_jobs_workspace_terminal")
            .table("thread_episodic_index_jobs").col("workspace_id")
            .cond_where(Expr::cust("status = 'canceled' AND (last_error IS NULL OR last_error NOT IN ('thread episodic source version superseded during reconciliation', 'thread episodic source deleted by user', 'thread episodic source excluded by user'))"))
            .to_owned()).await?;
        Ok(())
    }
    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for name in [
            "idx_thread_episodic_jobs_workspace_due",
            "idx_thread_episodic_jobs_workspace_recovery",
            "idx_thread_episodic_jobs_workspace_terminal",
        ] {
            manager
                .drop_index(
                    Index::drop()
                        .name(name)
                        .table("thread_episodic_index_jobs")
                        .to_owned(),
                )
                .await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Migrator;
    use sea_orm::{ConnectionTrait, Database, Statement};

    #[tokio::test]
    async fn episodic_job_access_indexes_cover_due_and_recovery_and_rollback_named_suffix() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        Migrator::up(&db, None).await.unwrap();
        for (name, columns) in [
            (
                "idx_thread_episodic_index_jobs_due",
                vec!["status", "next_run_at"],
            ),
            (
                "idx_thread_episodic_jobs_workspace_terminal",
                vec!["workspace_id"],
            ),
            (
                "idx_thread_episodic_jobs_workspace_due",
                vec!["workspace_id", "status", "next_run_at", "created_at"],
            ),
            (
                "idx_thread_episodic_jobs_workspace_recovery",
                vec!["workspace_id", "status", "updated_at", "id"],
            ),
        ] {
            let info = db
                .query_all_raw(Statement::from_string(
                    db.get_database_backend(),
                    format!("PRAGMA index_info('{name}')"),
                ))
                .await
                .unwrap();
            assert_eq!(
                info.into_iter()
                    .map(|row| row.try_get::<String>("", "name").unwrap())
                    .collect::<Vec<_>>(),
                columns
            );
        }
        let migrations = Migrator::migrations();
        let position = migrations
            .iter()
            .position(|migration| migration.name() == "m20261006_000001_thread_episodic_job_access")
            .unwrap();
        // Future migrations may follow this one. Remove the suffix by name.
        Migrator::down(&db, Some((migrations.len() - position) as u32))
            .await
            .unwrap();
        assert!(db.query_all_raw(Statement::from_string(db.get_database_backend(), "SELECT name FROM sqlite_master WHERE type='index' AND name IN ('idx_thread_episodic_jobs_workspace_due','idx_thread_episodic_jobs_workspace_recovery','idx_thread_episodic_jobs_workspace_terminal')".to_owned())).await.unwrap().is_empty());
        assert!(db.query_one_raw(Statement::from_string(db.get_database_backend(), "SELECT name FROM sqlite_master WHERE type='table' AND name='thread_episodic_index_jobs'".to_owned())).await.unwrap().is_some());
        assert!(db.query_one_raw(Statement::from_string(db.get_database_backend(), "SELECT name FROM sqlite_master WHERE name='idx_thread_episodic_jobs_runnable_seek'".to_owned())).await.unwrap().is_none());
        let global_columns = db
            .query_all_raw(Statement::from_string(
                db.get_database_backend(),
                "PRAGMA index_info('idx_thread_episodic_index_jobs_due')".to_owned(),
            ))
            .await
            .unwrap();
        assert_eq!(
            global_columns
                .into_iter()
                .map(|row| row.try_get::<String>("", "name").unwrap())
                .collect::<Vec<_>>(),
            vec!["status", "next_run_at"]
        );
    }
}
