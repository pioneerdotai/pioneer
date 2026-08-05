use sea_orm_migration::{prelude::*, schema::*};

const TURN_FINALIZATION: &str = "turn_finalization";
const TURN_ADMISSION: &str = "turn_admission";
const RECOVERY_TERMINALIZATION_OUTBOX: &str = "recovery_terminalization_outbox";

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(Alias::new(TURN_ADMISSION))
                    .if_not_exists()
                    .col(string("turn_id").string_len(21).primary_key())
                    .col(string("thread_id").string_len(21))
                    .col(string("workspace_id").string_len(64))
                    .col(string("request_digest").string_len(64))
                    .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_turn_admission_turn")
                            .from(Alias::new(TURN_ADMISSION), Alias::new("turn_id"))
                            .to(Alias::new("turn"), Alias::new("id"))
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("idx_turn_admission_scope")
                    .table(Alias::new(TURN_ADMISSION))
                    .col(Alias::new("workspace_id"))
                    .col(Alias::new("thread_id"))
                    .col(Alias::new("turn_id"))
                    .unique()
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(Alias::new(TURN_FINALIZATION))
                    .if_not_exists()
                    .col(string("turn_id").string_len(21).primary_key())
                    .col(string("thread_id").string_len(21))
                    .col(string("workspace_id").string_len(64))
                    .col(big_integer("generation"))
                    .col(string("item_id").string_len(21).unique_key())
                    .col(text("item_json"))
                    .col(string("item_digest").string_len(64))
                    .col(string("status").string_len(16).default("prepared"))
                    .col(timestamp_with_time_zone("prepared_at"))
                    .col(timestamp_with_time_zone("committed_at").null())
                    .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                    .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_turn_finalization_turn")
                            .from(Alias::new(TURN_FINALIZATION), Alias::new("turn_id"))
                            .to(Alias::new("turn"), Alias::new("id"))
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .check((
                        "ck_turn_finalization_generation",
                        Expr::cust("generation > 0"),
                    ))
                    .check((
                        "ck_turn_finalization_status",
                        Expr::cust("status IN ('prepared', 'committed')"),
                    ))
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("idx_turn_finalization_status")
                    .table(Alias::new(TURN_FINALIZATION))
                    .col(Alias::new("status"))
                    .col(Alias::new("prepared_at"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(Alias::new(RECOVERY_TERMINALIZATION_OUTBOX))
                    .if_not_exists()
                    .col(string("recovery_job_id").string_len(21).primary_key())
                    .col(string("turn_id").string_len(21))
                    .col(string("item_id").string_len(21))
                    .col(string("item_type").string_len(64))
                    .col(string("recovery_status").string_len(32))
                    .col(big_integer("attempt_number"))
                    .col(text("error_message"))
                    .col(string("status").string_len(16).default("pending"))
                    .col(big_integer("attempt_count").default(0))
                    .col(text("last_error").null())
                    .col(timestamp_with_time_zone("next_run_at"))
                    .col(string("claim_token").string_len(21).null())
                    .col(timestamp_with_time_zone("claim_expires_at").null())
                    .col(timestamp_with_time_zone("delivered_at").null())
                    .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                    .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_recovery_terminalization_job")
                            .from(
                                Alias::new(RECOVERY_TERMINALIZATION_OUTBOX),
                                Alias::new("recovery_job_id"),
                            )
                            .to(Alias::new("recovery_job"), Alias::new("id"))
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .check((
                        "ck_recovery_terminalization_attempt_number",
                        Expr::cust("attempt_number > 0"),
                    ))
                    .check((
                        "ck_recovery_terminalization_attempt_count",
                        Expr::cust("attempt_count >= 0"),
                    ))
                    .check((
                        "ck_recovery_terminalization_status",
                        Expr::cust(
                            "status IN ('pending', 'delivering', 'failed', 'delivered', 'cancelled')",
                        ),
                    ))
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("idx_recovery_terminalization_due")
                    .table(Alias::new(RECOVERY_TERMINALIZATION_OUTBOX))
                    .col(Alias::new("status"))
                    .col(Alias::new("next_run_at"))
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("idx_recovery_terminalization_turn")
                    .table(Alias::new(RECOVERY_TERMINALIZATION_OUTBOX))
                    .col(Alias::new("turn_id"))
                    .to_owned(),
            )
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(
                Table::drop()
                    .table(Alias::new(RECOVERY_TERMINALIZATION_OUTBOX))
                    .if_exists()
                    .to_owned(),
            )
            .await?;
        manager
            .drop_table(
                Table::drop()
                    .table(Alias::new(TURN_FINALIZATION))
                    .if_exists()
                    .to_owned(),
            )
            .await?;
        manager
            .drop_table(
                Table::drop()
                    .table(Alias::new(TURN_ADMISSION))
                    .if_exists()
                    .to_owned(),
            )
            .await?;
        Ok(())
    }
}
