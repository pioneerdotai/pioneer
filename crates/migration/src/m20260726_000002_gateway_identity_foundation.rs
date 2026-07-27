use sea_orm_migration::{
    prelude::*,
    schema::{integer, string, timestamp_with_time_zone},
};

const GATEWAY_IDENTITY: &str = "gateway_identity";
const GATEWAY_PRINCIPAL: &str = "gateway_principal";
const THREAD: &str = "thread";
const TURN: &str = "turn";

const THREAD_ACTOR_KIND: &str = "created_by_actor_kind";
const THREAD_ACTOR_ID: &str = "created_by_actor_id";
const TURN_ACTOR_KIND: &str = "initiated_by_actor_kind";
const TURN_ACTOR_ID: &str = "initiated_by_actor_id";

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        create_gateway_identity(manager).await?;
        create_gateway_principal(manager).await?;
        create_principal_indexes(manager).await?;
        add_actor_columns(manager, THREAD, THREAD_ACTOR_KIND, THREAD_ACTOR_ID).await?;
        add_actor_columns(manager, TURN, TURN_ACTOR_KIND, TURN_ACTOR_ID).await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        drop_actor_columns(manager, TURN, TURN_ACTOR_KIND, TURN_ACTOR_ID).await?;
        drop_actor_columns(manager, THREAD, THREAD_ACTOR_KIND, THREAD_ACTOR_ID).await?;
        drop_table(manager, GATEWAY_PRINCIPAL).await?;
        drop_table(manager, GATEWAY_IDENTITY).await?;
        Ok(())
    }

    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }
}

async fn create_gateway_identity(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(GATEWAY_IDENTITY))
                .col(string("id").string_len(21).primary_key())
                .col(integer("singleton_key").unique_key())
                .col(integer("identity_bootstrap_version").default(0))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                .check((
                    "ck_gateway_identity_singleton",
                    Expr::cust("singleton_key = 1"),
                ))
                .check((
                    "ck_gateway_identity_bootstrap_version",
                    Expr::cust("identity_bootstrap_version >= 0"),
                ))
                .check((
                    "ck_gateway_identity_id",
                    Expr::cust("length(id) = 21 AND id NOT GLOB '*[^A-Za-z0-9]*'"),
                ))
                .to_owned(),
        )
        .await
}

async fn create_gateway_principal(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(GATEWAY_PRINCIPAL))
                .col(string("id").string_len(21).primary_key())
                .col(string("gateway_id").string_len(21))
                .col(string("kind").string_len(32))
                .col(string("role_key").string_len(64).null())
                .col(string("status").string_len(32))
                .col(string("display_name").string_len(255))
                .col(string("nickname").string_len(64))
                .col(string("nickname_key").string_len(64))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("removed_at").null())
                .check((
                    "ck_gateway_principal_id",
                    Expr::cust(
                        "length(id) = 21 AND id NOT GLOB '*[^A-Za-z0-9]*' \
                         AND length(gateway_id) = 21 \
                         AND gateway_id NOT GLOB '*[^A-Za-z0-9]*'",
                    ),
                ))
                .check((
                    "ck_gateway_principal_kind",
                    Expr::cust("kind IN ('superuser', 'user')"),
                ))
                .check((
                    "ck_gateway_principal_status",
                    Expr::cust("status IN ('active', 'suspended', 'removed')"),
                ))
                .check((
                    "ck_gateway_principal_removal",
                    Expr::cust(
                        "(status = 'removed' AND removed_at IS NOT NULL) \
                         OR (status != 'removed' AND removed_at IS NULL)",
                    ),
                ))
                .check((
                    "ck_gateway_principal_superuser",
                    Expr::cust(
                        "kind != 'superuser' \
                         OR (role_key IS NULL AND status = 'active' AND removed_at IS NULL)",
                    ),
                ))
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_gateway_principal_gateway")
                        .from(Alias::new(GATEWAY_PRINCIPAL), Alias::new("gateway_id"))
                        .to(Alias::new(GATEWAY_IDENTITY), Alias::new("id"))
                        .on_update(ForeignKeyAction::NoAction)
                        .on_delete(ForeignKeyAction::Restrict),
                )
                .to_owned(),
        )
        .await
}

async fn create_principal_indexes(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .get_connection()
        .execute_unprepared(
            "CREATE UNIQUE INDEX idx_gateway_principal_one_superuser \
             ON gateway_principal (gateway_id) WHERE kind = 'superuser'",
        )
        .await?;

    manager
        .create_index(
            Index::create()
                .name("idx_gateway_principal_nickname")
                .table(Alias::new(GATEWAY_PRINCIPAL))
                .col(Alias::new("gateway_id"))
                .col(Alias::new("nickname_key"))
                .unique()
                .to_owned(),
        )
        .await?;

    manager
        .create_index(
            Index::create()
                .name("idx_gateway_principal_status")
                .table(Alias::new(GATEWAY_PRINCIPAL))
                .col(Alias::new("gateway_id"))
                .col(Alias::new("status"))
                .to_owned(),
        )
        .await
}

async fn add_actor_columns(
    manager: &SchemaManager<'_>,
    table: &'static str,
    actor_kind: &'static str,
    actor_id: &'static str,
) -> Result<(), DbErr> {
    if !manager.has_column(table, actor_id).await? {
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new(table))
                    .add_column(string(actor_id).string_len(21).null())
                    .to_owned(),
            )
            .await?;
    }

    if !manager.has_column(table, actor_kind).await? {
        let constraint_name = format!("ck_{table}_{actor_kind}");
        let check = format!(
            "({actor_kind} IS NULL AND {actor_id} IS NULL) \
             OR ({actor_kind} = 'system' AND {actor_id} IS NULL) \
             OR ({actor_kind} = 'principal' AND length({actor_id}) = 21 \
                 AND {actor_id} NOT GLOB '*[^A-Za-z0-9]*')"
        );
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new(table))
                    .add_column(
                        string(actor_kind)
                            .string_len(32)
                            .null()
                            .check((constraint_name, Expr::cust(check))),
                    )
                    .to_owned(),
            )
            .await?;
    }

    Ok(())
}

async fn drop_actor_columns(
    manager: &SchemaManager<'_>,
    table: &str,
    actor_kind: &str,
    actor_id: &str,
) -> Result<(), DbErr> {
    if manager.has_column(table, actor_kind).await? {
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new(table))
                    .drop_column(Alias::new(actor_kind))
                    .to_owned(),
            )
            .await?;
    }
    if manager.has_column(table, actor_id).await? {
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new(table))
                    .drop_column(Alias::new(actor_id))
                    .to_owned(),
            )
            .await?;
    }
    Ok(())
}

async fn drop_table(manager: &SchemaManager<'_>, table: &str) -> Result<(), DbErr> {
    manager
        .drop_table(
            Table::drop()
                .if_exists()
                .table(Alias::new(table))
                .to_owned(),
        )
        .await
}
