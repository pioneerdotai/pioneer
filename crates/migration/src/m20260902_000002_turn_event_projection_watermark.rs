use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table("turn_event_projection_stream_state")
                    .add_column(big_integer("projected_through_sequence").default(0))
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table("turn_event_projection_stream_state")
                    .drop_column(Alias::new("projected_through_sequence"))
                    .to_owned(),
            )
            .await
    }
}
