use sea_orm_migration::{prelude::*, schema::*};

const OUTBOX: &str = "native_terminal_effect_outbox";

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(Alias::new(OUTBOX))
                    .if_not_exists()
                    .col(string("effect_id").string_len(128).primary_key())
                    .col(string("batch_id").string_len(128))
                    .col(string("workspace_id").string_len(64))
                    .col(string("thread_id").string_len(64))
                    .col(string("turn_id").string_len(64))
                    .col(big_integer("runtime_generation"))
                    .col(string("effect_kind").string_len(32))
                    .col(string("gate_kind").string_len(32))
                    .col(text("payload_json"))
                    .col(string("payload_sha256").string_len(64))
                    .col(string("payload_identity_sha256").string_len(64))
                    .col(text("handler_checkpoint_json").null())
                    .col(
                        string("handler_checkpoint_sha256")
                            .string_len(64)
                            .null(),
                    )
                    .col(string("status").string_len(32).default("prepared"))
                    // Keep this aligned with task_result_candidate.id. A valid
                    // review candidate must never fail the acceptance fence
                    // because the outbox reference is narrower than its owner.
                    .col(string("accepted_candidate_id").string_len(96).null())
                    .col(big_integer("attempt_count").default(0))
                    .col(big_integer("max_attempts"))
                    .col(string("last_error_code").string_len(64).null())
                    .col(text("last_error_message").null())
                    .col(timestamp_with_time_zone("next_run_at").null())
                    .col(string("claim_token").string_len(21).null())
                    .col(timestamp_with_time_zone("claim_expires_at").null())
                    .col(timestamp_with_time_zone("terminal_committed_at").null())
                    .col(timestamp_with_time_zone("completed_at").null())
                    .col(timestamp_with_time_zone("prepared_at"))
                    .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                    .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_native_terminal_effect_turn")
                            .from(Alias::new(OUTBOX), Alias::new("turn_id"))
                            .to(Alias::new("turn"), Alias::new("id"))
                            // Explicit deletion of a Turn/workspace remains a
                            // tenant-data deletion boundary. Ordinary lifecycle
                            // cleanup never deletes the parent Turn and instead
                            // uses the bounded resolved-row retention worker.
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .check((
                        "ck_native_terminal_effect_generation",
                        Expr::cust("runtime_generation > 0"),
                    ))
                    .check((
                        "ck_native_terminal_effect_attempts",
                        Expr::cust(
                            "attempt_count >= 0 AND attempt_count <= max_attempts AND max_attempts BETWEEN 1 AND 20",
                        ),
                    ))
                    .check((
                        "ck_native_terminal_effect_kind",
                        Expr::cust("effect_kind IN ('post_turn_hook', 'attached_task_cleanup')"),
                    ))
                    .check((
                        "ck_native_terminal_effect_gate",
                        Expr::cust("gate_kind IN ('terminal_commit', 'accepted_task_result')"),
                    ))
                    .check((
                        "ck_native_terminal_effect_status",
                        Expr::cust(
                            "status IN ('prepared', 'waiting_acceptance', 'ready', 'running', 'retry_wait', 'succeeded', 'unresolved', 'discarded', 'superseded')",
                        ),
                    ))
                    .check((
                        "ck_native_terminal_effect_payload_sha256",
                        Expr::cust("length(payload_sha256) = 64"),
                    ))
                    .check((
                        "ck_native_terminal_effect_payload_identity_sha256",
                        Expr::cust("length(payload_identity_sha256) = 64"),
                    ))
                    .check((
                        "ck_native_terminal_effect_checkpoint_pair",
                        Expr::cust(
                            "(handler_checkpoint_json IS NULL AND handler_checkpoint_sha256 IS NULL) OR (handler_checkpoint_json IS NOT NULL AND handler_checkpoint_sha256 IS NOT NULL AND length(handler_checkpoint_sha256) = 64)",
                        ),
                    ))
                    .check((
                        "ck_native_terminal_effect_state",
                        Expr::cust(
                            "(status = 'prepared' AND terminal_committed_at IS NULL AND completed_at IS NULL AND next_run_at IS NULL AND claim_token IS NULL AND claim_expires_at IS NULL) OR \
                             (status = 'waiting_acceptance' AND terminal_committed_at IS NOT NULL AND completed_at IS NULL AND next_run_at IS NULL AND claim_token IS NULL AND claim_expires_at IS NULL) OR \
                             (status IN ('ready', 'retry_wait') AND terminal_committed_at IS NOT NULL AND completed_at IS NULL AND next_run_at IS NOT NULL AND claim_token IS NULL AND claim_expires_at IS NULL) OR \
                             (status = 'running' AND terminal_committed_at IS NOT NULL AND completed_at IS NULL AND next_run_at IS NOT NULL AND claim_token IS NOT NULL AND claim_expires_at IS NOT NULL) OR \
                             (status IN ('succeeded', 'unresolved', 'discarded') AND terminal_committed_at IS NOT NULL AND completed_at IS NOT NULL AND next_run_at IS NULL AND claim_token IS NULL AND claim_expires_at IS NULL) OR \
                             (status = 'superseded' AND terminal_committed_at IS NULL AND completed_at IS NOT NULL AND next_run_at IS NULL AND claim_token IS NULL AND claim_expires_at IS NULL)",
                        ),
                    ))
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("uidx_native_terminal_effect_turn_kind")
                    .table(Alias::new(OUTBOX))
                    .if_not_exists()
                    .col(Alias::new("turn_id"))
                    .col(Alias::new("effect_kind"))
                    .unique()
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("idx_native_terminal_effect_due")
                    .table(Alias::new(OUTBOX))
                    .if_not_exists()
                    .col(Alias::new("status"))
                    .col(Alias::new("next_run_at"))
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("idx_native_terminal_effect_turn")
                    .table(Alias::new(OUTBOX))
                    .if_not_exists()
                    .col(Alias::new("turn_id"))
                    .col(Alias::new("batch_id"))
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("idx_native_terminal_effect_completed")
                    .table(Alias::new(OUTBOX))
                    .if_not_exists()
                    .col(Alias::new("status"))
                    .col(Alias::new("completed_at"))
                    .col(Alias::new("effect_id"))
                    .to_owned(),
            )
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let query = Query::select()
            .expr_as(Func::count(Expr::col(Asterisk)), Alias::new("count"))
            .from(Alias::new(OUTBOX))
            .and_where(Expr::col(Alias::new("status")).is_not_in([
                "succeeded",
                "discarded",
                "superseded",
            ]))
            .to_owned();
        let pending = manager
            .get_connection()
            .query_one(&query)
            .await?
            .map(|row| row.try_get::<i64>("", "count"))
            .transpose()?
            .unwrap_or(0);
        if pending > 0 {
            return Err(DbErr::Migration(format!(
                "cannot roll back native terminal-effect outbox while {pending} unresolved obligation(s) remain"
            )));
        }
        manager
            .drop_table(
                Table::drop()
                    .table(Alias::new(OUTBOX))
                    .if_exists()
                    .to_owned(),
            )
            .await
    }
}
