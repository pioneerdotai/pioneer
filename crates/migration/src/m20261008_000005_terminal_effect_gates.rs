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
