use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // One installation scan builds only the exhausted StartAgent range;
        // subsequent NULL/timed discovery searches this expression index.
        // Keep the CASE/COALESCE predicate aligned with the CRUD selector.
        let valid_json: Expr = Func::cust(Alias::new("json_valid"))
            .arg(Expr::col(Alias::new("payload_json")))
            .into();
        let payload_kind: Expr = Expr::case(
            valid_json,
            Func::cust(Alias::new("json_extract"))
                .arg(Expr::col(Alias::new("payload_json")))
                .arg("$.kind"),
        )
        .into();
        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_agent_action_outbox_exhausted")
                    .table(Alias::new("agent_action_outbox"))
                    .col(Func::coalesce([
                        Expr::col(Alias::new("next_attempt_at")),
                        Expr::val(""),
                    ]))
                    .col(Alias::new("created_at"))
                    .col(Alias::new("id"))
                    .and_where(Expr::col(Alias::new("status")).is_in(["pending", "failed"]))
                    .and_where(Expr::col(Alias::new("attempts")).eq(8))
                    .and_where(
                        Func::coalesce([payload_kind, Expr::val("start_agent")]).eq("start_agent"),
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
                    .name("idx_agent_action_outbox_exhausted")
                    .to_owned(),
            )
            .await?;
        Ok(())
    }
}
