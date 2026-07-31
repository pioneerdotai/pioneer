use sea_orm_migration::{prelude::*, schema::text};

const TURN: &str = "turn";
const CLI_RUNTIME_PENDING_REQUEST: &str = "cli_runtime_pending_request";
const EXECUTION_AUTHORIZATION_CONTEXT_JSON: &str = "execution_authorization_context_json";
const INITIATING_PRINCIPAL_ID: &str = "initiating_principal_id";
const INITIATING_SESSION_ID: &str = "initiating_session_id";
const INITIATING_SESSION_GENERATION: &str = "initiating_session_generation";
const AUTHORIZATION_CONTEXT_FINGERPRINT: &str = "authorization_context_fingerprint";

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new(TURN))
                    .add_column(text(EXECUTION_AUTHORIZATION_CONTEXT_JSON).null())
                    .to_owned(),
            )
            .await?;
        for column in [
            text(INITIATING_PRINCIPAL_ID).null(),
            text(INITIATING_SESSION_ID).null(),
            ColumnDef::new(Alias::new(INITIATING_SESSION_GENERATION))
                .big_integer()
                .null(),
            text(AUTHORIZATION_CONTEXT_FINGERPRINT).null(),
        ] {
            manager
                .alter_table(
                    Table::alter()
                        .table(Alias::new(CLI_RUNTIME_PENDING_REQUEST))
                        .add_column(column)
                        .to_owned(),
                )
                .await?;
        }
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for column in [
            AUTHORIZATION_CONTEXT_FINGERPRINT,
            INITIATING_SESSION_GENERATION,
            INITIATING_SESSION_ID,
            INITIATING_PRINCIPAL_ID,
        ] {
            manager
                .alter_table(
                    Table::alter()
                        .table(Alias::new(CLI_RUNTIME_PENDING_REQUEST))
                        .drop_column(Alias::new(column))
                        .to_owned(),
                )
                .await?;
        }
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new(TURN))
                    .drop_column(Alias::new(EXECUTION_AUTHORIZATION_CONTEXT_JSON))
                    .to_owned(),
            )
            .await
    }

    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }
}
