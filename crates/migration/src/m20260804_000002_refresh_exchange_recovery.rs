use sea_orm_migration::{prelude::*, schema::string};

const GATEWAY_IDENTITY: &str = "gateway_identity";
const AUTH_REFRESH_CREDENTIAL: &str = "auth_refresh_credential";
const EXCHANGE_REQUEST_ID: &str = "exchange_request_id";

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new(AUTH_REFRESH_CREDENTIAL))
                    .add_column(string(EXCHANGE_REQUEST_ID).string_len(21).null().check((
                        "ck_auth_refresh_exchange_request_id",
                        Expr::cust(
                            "exchange_request_id IS NULL OR (\
                                 length(exchange_request_id) = 21 AND \
                                 exchange_request_id NOT GLOB '*[^A-Za-z0-9]*')",
                        ),
                    )))
                    .to_owned(),
            )
            .await?;
        mark_auth_not_ready(manager).await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new(AUTH_REFRESH_CREDENTIAL))
                    .drop_column(Alias::new(EXCHANGE_REQUEST_ID))
                    .to_owned(),
            )
            .await?;
        mark_auth_not_ready(manager).await
    }

    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }
}

async fn mark_auth_not_ready(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .execute(
            Query::update()
                .table(Alias::new(GATEWAY_IDENTITY))
                .values([
                    (Alias::new("auth_schema_version"), 0.into()),
                    (Alias::new("auth_ready_at"), Option::<String>::None.into()),
                ])
                .to_owned(),
        )
        .await
}
