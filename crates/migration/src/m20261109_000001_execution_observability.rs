//! Empty indexed tables only: installation never scans historical transcripts.
//! Facts are immutable revisions; one attempt is one charge identity regardless
//! of the number of revisions. Tariffs are deduplicated within a workspace.
use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;
#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table("execution_tariff")
                    .col(string("workspace_id"))
                    .col(string("content_hash"))
                    .primary_key(Index::create().col("workspace_id").col("content_hash"))
                    .col(text("evidence_json"))
                    .col(big_integer("captured_at_ms"))
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                Table::create()
                    .table("execution_observation")
                    .col(big_integer("sequence").primary_key().auto_increment())
                    .col(string("workspace_id"))
                    .col(string("operation_id"))
                    .col(big_integer("revision"))
                    .col(big_integer("source_revision"))
                    .col(string("resolution_origin"))
                    .col(string_null("thread_id"))
                    .col(string_null("root_thread_id"))
                    .col(string_null("turn_id"))
                    .col(string_null("task_id"))
                    .col(string_null("task_run_id"))
                    .col(string_null("task_execution_id"))
                    .col(string_null("execution_id"))
                    .col(string_null("window_id"))
                    .col(string("owner_kind"))
                    .col(string("owner_id"))
                    .col(string_null("parent_operation_id"))
                    .col(string("kind"))
                    .col(string("purpose"))
                    .col(string_null("provider"))
                    .col(string_null("model"))
                    .col(string("status"))
                    .col(big_integer("recorded_at_ms"))
                    .col(big_integer_null("start_wall_ms"))
                    .col(big_integer_null("end_wall_ms"))
                    .col(big_integer_null("start_mono_ns"))
                    .col(big_integer_null("end_mono_ns"))
                    .col(big_integer_null("elapsed_ns"))
                    .col(string_null("process_run_id"))
                    .col(string_null("start_process_run_id"))
                    .col(string_null("tariff_hash"))
                    .col(text("facts_json"))
                    .to_owned(),
            )
            .await?;
        for (name, columns, unique) in [
            (
                "idx_execution_observation_parent",
                vec!["workspace_id", "parent_operation_id", "sequence"],
                false,
            ),
            (
                "idx_execution_observation_workspace_frontier",
                vec!["workspace_id", "sequence"],
                false,
            ),
            (
                "idx_execution_observation_identity",
                vec!["workspace_id", "operation_id", "revision"],
                true,
            ),
            (
                "idx_execution_observation_thread",
                vec!["workspace_id", "thread_id", "sequence"],
                false,
            ),
            (
                "idx_execution_observation_turn",
                vec!["turn_id", "sequence"],
                false,
            ),
            (
                "idx_execution_observation_task",
                vec!["workspace_id", "task_id", "sequence"],
                false,
            ),
            (
                "idx_execution_observation_execution",
                vec!["workspace_id", "execution_id", "sequence"],
                false,
            ),
            (
                "idx_execution_observation_owner",
                vec!["workspace_id", "owner_kind", "owner_id", "sequence"],
                false,
            ),
        ] {
            let mut index = Index::create();
            index.name(name).table("execution_observation");
            for col in columns {
                index.col(col);
            }
            if unique {
                index.unique();
            }
            manager.create_index(index.to_owned()).await?;
        }
        // SeaQuery has no trigger DDL builder. These fixed schema-only triggers
        // provide atomic cleanup with SQLite foreign_keys=OFF, including callers
        // deleting through existing generic repositories.
        manager.get_connection().execute_unprepared("CREATE TRIGGER execution_observation_thread_delete AFTER DELETE ON thread BEGIN DELETE FROM provider_usage_observation WHERE workspace_id=OLD.workspace_id AND (owner_id=OLD.id OR id IN (SELECT substr(operation_id,18) FROM execution_observation WHERE workspace_id=OLD.workspace_id AND thread_id=OLD.id AND operation_id LIKE 'provider:journal:%')); DELETE FROM execution_observation WHERE workspace_id=OLD.workspace_id AND thread_id=OLD.id; END").await?;
        manager.get_connection().execute_unprepared("CREATE TRIGGER execution_observation_turn_delete AFTER DELETE ON turn BEGIN DELETE FROM provider_usage_observation WHERE id IN (SELECT CASE WHEN operation_id LIKE 'provider:journal:%' THEN substr(operation_id,18) ELSE substr(operation_id,length('provider:' || OLD.id || ':')+1) END FROM execution_observation WHERE turn_id=OLD.id AND kind IN ('provider_attempt','external_usage_snapshot') AND operation_id LIKE 'provider:%'); DELETE FROM execution_observation WHERE turn_id=OLD.id; END").await?;
        manager.get_connection().execute_unprepared("CREATE TRIGGER execution_observation_workspace_delete AFTER DELETE ON workspace BEGIN DELETE FROM execution_observation WHERE workspace_id=OLD.id; DELETE FROM execution_tariff WHERE workspace_id=OLD.id; DELETE FROM provider_usage_observation WHERE workspace_id=OLD.id; END").await?;
        Ok(())
    }
    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared("DROP TRIGGER IF EXISTS execution_observation_turn_delete")
            .await?;
        manager
            .get_connection()
            .execute_unprepared("DROP TRIGGER IF EXISTS execution_observation_thread_delete")
            .await?;
        manager
            .get_connection()
            .execute_unprepared("DROP TRIGGER IF EXISTS execution_observation_workspace_delete")
            .await?;
        manager
            .drop_table(Table::drop().table("execution_observation").to_owned())
            .await?;
        manager
            .drop_table(Table::drop().table("execution_tariff").to_owned())
            .await
    }
}
