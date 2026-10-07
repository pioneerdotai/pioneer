use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Defaults expose existing waiting obligations without a payload backfill.
        for column in [
            big_integer("gate_probe_at").default(0).to_owned(),
            big_integer("gate_probe_attempts").default(0).to_owned(),
            string("gate_probe_token").string_len(21).null().to_owned(),
        ] {
            manager
                .alter_table(
                    Table::alter()
                        .table(Alias::new("native_terminal_effect_outbox"))
                        .add_column(column)
                        .to_owned(),
                )
                .await?;
        }
        // Fixed predicates are shared verbatim with the repository's due seek.
        // Each build reads the existing table once and adds durable index storage.
        for sql in [
            "CREATE INDEX idx_task_result_candidate_gate ON task_result_candidate(thread_id, turn_id, status, updated_at DESC, id DESC)",
            "CREATE INDEX idx_native_terminal_effect_gate_due ON native_terminal_effect_outbox(gate_probe_at, prepared_at, effect_id) WHERE status = 'waiting_acceptance'",
            "CREATE UNIQUE INDEX uidx_native_terminal_effect_probe_token ON native_terminal_effect_outbox(gate_probe_token) WHERE gate_probe_token IS NOT NULL",
            "DROP INDEX idx_native_terminal_effect_due",
            "CREATE INDEX idx_native_terminal_effect_due ON native_terminal_effect_outbox(status, next_run_at, prepared_at, effect_id)",
            "CREATE INDEX idx_native_terminal_effect_expired ON native_terminal_effect_outbox(status, claim_expires_at, prepared_at, effect_id)",
        ] {
            manager.get_connection().execute_unprepared(sql).await?;
        }
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for sql in [
            "DROP INDEX idx_task_result_candidate_gate",
            "DROP INDEX idx_native_terminal_effect_gate_due",
            "DROP INDEX uidx_native_terminal_effect_probe_token",
            "DROP INDEX idx_native_terminal_effect_expired",
            "DROP INDEX idx_native_terminal_effect_due",
            "CREATE INDEX idx_native_terminal_effect_due ON native_terminal_effect_outbox(status, next_run_at)",
        ] {
            manager.get_connection().execute_unprepared(sql).await?;
        }
        for column in ["gate_probe_token", "gate_probe_attempts", "gate_probe_at"] {
            manager
                .alter_table(
                    Table::alter()
                        .table(Alias::new("native_terminal_effect_outbox"))
                        .drop_column(Alias::new(column))
                        .to_owned(),
                )
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
    use crate::Migrator;
    use sea_orm::{ConnectionTrait, Database, DbBackend, Statement};

    #[tokio::test]
    async fn schema_and_marker_rollback_on_index_failure_then_retry_and_down() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        let name = Migration.name();
        let migrations = Migrator::migrations();
        let position = migrations
            .iter()
            .position(|migration| migration.name() == name)
            .expect("terminal-effect gate migration must remain registered");
        let before = <u32 as TryFrom<usize>>::try_from(position).unwrap();
        // Only this migration is under test. Later migrations may be irreversible.
        Migrator::up(&db, Some(before)).await.unwrap();
        db.execute_unprepared(
            "CREATE INDEX idx_task_result_candidate_gate ON task_result_candidate(id)",
        )
        .await
        .unwrap();
        assert!(Migrator::up(&db, Some(1)).await.is_err());
        let columns = db
            .query_all_raw(Statement::from_string(
                DbBackend::Sqlite,
                "PRAGMA table_info(native_terminal_effect_outbox)",
            ))
            .await
            .unwrap();
        for column in ["gate_probe_at", "gate_probe_attempts", "gate_probe_token"] {
            assert!(
                !columns
                    .iter()
                    .any(|row| row.try_get::<String>("", "name").unwrap() == column)
            );
        }
        for index in [
            "idx_native_terminal_effect_gate_due",
            "uidx_native_terminal_effect_probe_token",
            "idx_native_terminal_effect_expired",
        ] {
            assert!(
                !SchemaManager::new(&db)
                    .has_index("native_terminal_effect_outbox", index)
                    .await
                    .unwrap()
            );
        }
        let applied = Migrator::get_applied_migrations(&db).await.unwrap();
        assert_eq!(applied.len(), before as usize);
        assert!(!applied.iter().any(|migration| migration.name() == name));
        db.execute_unprepared("DROP INDEX idx_task_result_candidate_gate")
            .await
            .unwrap();
        Migrator::up(&db, Some(1)).await.unwrap();
        let columns = db
            .query_all_raw(Statement::from_string(
                DbBackend::Sqlite,
                "PRAGMA table_info(native_terminal_effect_outbox)",
            ))
            .await
            .unwrap();
        for name in ["gate_probe_at", "gate_probe_attempts"] {
            let row = columns
                .iter()
                .find(|r| r.try_get::<String>("", "name").unwrap() == name)
                .unwrap();
            assert_eq!(row.try_get::<i64>("", "notnull").unwrap(), 1);
            assert_eq!(row.try_get::<String>("", "dflt_value").unwrap(), "0");
        }
        assert!(
            Migrator::get_applied_migrations(&db)
                .await
                .unwrap()
                .iter()
                .any(|migration| migration.name() == name)
        );
        let added_indexes = [
            ("task_result_candidate", "idx_task_result_candidate_gate"),
            (
                "native_terminal_effect_outbox",
                "idx_native_terminal_effect_gate_due",
            ),
            (
                "native_terminal_effect_outbox",
                "uidx_native_terminal_effect_probe_token",
            ),
            (
                "native_terminal_effect_outbox",
                "idx_native_terminal_effect_expired",
            ),
        ];
        for (table, index) in added_indexes {
            assert!(
                SchemaManager::new(&db)
                    .has_index(table, index)
                    .await
                    .unwrap()
            );
        }
        Migrator::down(&db, Some(1)).await.unwrap();
        assert!(
            !Migrator::get_applied_migrations(&db)
                .await
                .unwrap()
                .iter()
                .any(|migration| migration.name() == name)
        );
        for (table, index) in added_indexes {
            assert!(
                !SchemaManager::new(&db)
                    .has_index(table, index)
                    .await
                    .unwrap()
            );
        }
        let due_columns = db
            .query_all_raw(Statement::from_string(
                DbBackend::Sqlite,
                "PRAGMA index_info(idx_native_terminal_effect_due)",
            ))
            .await
            .unwrap();
        assert_eq!(
            due_columns
                .iter()
                .map(|row| row.try_get::<String>("", "name").unwrap())
                .collect::<Vec<_>>(),
            vec!["status", "next_run_at"]
        );
        let columns = db
            .query_all_raw(Statement::from_string(
                DbBackend::Sqlite,
                "PRAGMA table_info(native_terminal_effect_outbox)",
            ))
            .await
            .unwrap();
        for name in ["gate_probe_at", "gate_probe_attempts", "gate_probe_token"] {
            assert!(
                !columns
                    .iter()
                    .any(|row| row.try_get::<String>("", "name").unwrap() == name)
            );
        }
        Migrator::up(&db, Some(1)).await.unwrap();
        let applied = Migrator::get_applied_migrations(&db).await.unwrap();
        assert_eq!(applied.len(), before as usize + 1);
        assert!(applied.iter().any(|migration| migration.name() == name));
    }
}
