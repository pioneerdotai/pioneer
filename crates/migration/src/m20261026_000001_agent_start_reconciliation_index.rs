use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // One installation scan builds only the exhausted StartAgent range;
        // subsequent NULL/timed discovery searches this expression index.
        // SeaQuery 1.0.2 converts Func/Expr index columns to IndexColumn::Expr.
        // Its SQLite builder inherits prepare_index_columns, which panics for
        // that variant. Static DDL is required for this expression index;
        // Expr::cust would enter the same unsupported branch.
        // Keep the expression and partial predicate aligned with CRUD discovery.
        manager
            .get_connection()
            .execute_unprepared(
                "CREATE INDEX IF NOT EXISTS idx_agent_action_outbox_exhausted \
                 ON agent_action_outbox(COALESCE(next_attempt_at,''),created_at,id) \
                 WHERE status IN ('pending','failed') AND attempts=8 \
                 AND COALESCE(CASE WHEN json_valid(payload_json) THEN json_extract(payload_json,'$.kind') END,'start_agent')='start_agent'",
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
