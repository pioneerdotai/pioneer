use sea_orm_migration::{prelude::*, schema::string};

const TURN: &str = "turn";
const WORK_OWNER: &str = "work_owner";

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        if manager.has_column(TURN, WORK_OWNER).await? {
            return Ok(());
        }
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new(TURN))
                    .add_column(
                        string(WORK_OWNER)
                            .string_len(32)
                            .not_null()
                            .default("turn")
                            .check((
                                "ck_turn_work_owner",
                                Expr::cust("work_owner IN ('turn', 'detached_task')"),
                            )),
                    )
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        if !manager.has_column(TURN, WORK_OWNER).await? {
            return Ok(());
        }
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new(TURN))
                    .drop_column(Alias::new(WORK_OWNER))
                    .to_owned(),
            )
            .await
    }
}
