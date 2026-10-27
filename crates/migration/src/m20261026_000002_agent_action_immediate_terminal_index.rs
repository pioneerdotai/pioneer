use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }

    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Replacing the partial index scans existing outbox rows once. Its
        // narrower working set avoids rescanning closed refusals on every poll.
        // The replacement is atomic and matches the immediate CRUD predicate.
        manager
            .drop_index(
                Index::drop()
                    .if_exists()
                    .name("idx_agent_action_outbox_immediate")
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                immediate_index()
                    .cond_where(
                        Condition::any()
                            .add(Expr::col(Alias::new("status")).eq("pending"))
                            .add(Expr::col(Alias::new("last_error")).is_null())
                            .add(Expr::col(Alias::new("last_error")).is_not_in([
                                "outbox_start_integrity_rejected",
                                "outbox_start_authority_denied",
                                "outbox_start_runtime_unavailable",
                                "outbox_foreground_conflict",
                                "outbox_start_outcome_unknown",
                            ])),
                    )
                    .to_owned(),
            )
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_index(
                Index::drop()
                    .if_exists()
                    .name("idx_agent_action_outbox_immediate")
                    .to_owned(),
            )
            .await?;
        manager.create_index(immediate_index()).await?;
        Ok(())
    }
}

fn immediate_index() -> IndexCreateStatement {
    Index::create()
        .name("idx_agent_action_outbox_immediate")
        .table(Alias::new("agent_action_outbox"))
        .col(Alias::new("created_at"))
        .col(Alias::new("id"))
        .and_where(Expr::col(Alias::new("next_attempt_at")).is_null())
        .and_where(Expr::col(Alias::new("status")).is_in(["pending", "failed"]))
        .and_where(Expr::col(Alias::new("attempts")).lt(8))
        .to_owned()
}
