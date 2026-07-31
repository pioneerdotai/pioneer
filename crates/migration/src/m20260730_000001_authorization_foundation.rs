use sea_orm_migration::{
    prelude::*,
    schema::{integer, string, timestamp_with_time_zone},
};

const GATEWAY_PRINCIPAL: &str = "gateway_principal";
const WORKSPACE: &str = "workspace";
const THREAD: &str = "thread";
const WORKSPACE_MEMBERSHIP: &str = "workspace_membership";
const THREAD_MEMBERSHIP: &str = "thread_membership";
const THREAD_ACCESS_CLASS: &str = "access_class";
const PRINCIPAL_AUTHORIZATION_GUARD: &str = "authorization_guard";

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        validate_existing_principals(manager).await?;
        add_principal_authorization_guard(manager).await?;
        create_principal_authorization_index(manager).await?;
        add_thread_access_class(manager).await?;
        create_thread_access_index(manager).await?;
        create_workspace_membership(manager).await?;
        create_workspace_membership_indexes(manager).await?;
        create_thread_membership(manager).await?;
        create_thread_membership_indexes(manager).await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        ensure_no_ordinary_principals(manager).await?;
        ensure_membership_table_empty(manager, THREAD_MEMBERSHIP).await?;
        ensure_membership_table_empty(manager, WORKSPACE_MEMBERSHIP).await?;
        drop_table(manager, THREAD_MEMBERSHIP).await?;
        drop_table(manager, WORKSPACE_MEMBERSHIP).await?;
        drop_index(manager, "idx_thread_workspace_access").await?;
        drop_thread_access_class(manager).await?;
        drop_index(manager, "idx_gateway_principal_authorization").await?;
        drop_principal_authorization_guard(manager).await
    }

    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }
}

async fn ensure_no_ordinary_principals(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    let query = Query::select()
        .expr_as(Func::count(Expr::col(Asterisk)), Alias::new("count"))
        .from(Alias::new(GATEWAY_PRINCIPAL))
        .and_where(Expr::col(Alias::new("kind")).eq("user"))
        .to_owned();
    ensure_zero(
        manager,
        &query,
        "cannot roll back Epic 4 while ordinary principals contain Member-dependent data",
    )
    .await
}

async fn ensure_membership_table_empty(
    manager: &SchemaManager<'_>,
    table: &str,
) -> Result<(), DbErr> {
    let query = Query::select()
        .expr_as(Func::count(Expr::col(Asterisk)), Alias::new("count"))
        .from(Alias::new(table))
        .to_owned();
    let message = format!("cannot roll back Epic 4 while {table} contains ACL data");
    ensure_zero(manager, &query, message.as_str()).await
}

async fn validate_existing_principals(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    let query = Query::select()
        .expr_as(Func::count(Expr::col(Asterisk)), Alias::new("count"))
        .from(Alias::new(GATEWAY_PRINCIPAL))
        .and_where(Expr::cust(
            "NOT (\
                (kind = 'superuser' AND role_key IS NULL \
                    AND status = 'active' AND removed_at IS NULL) \
                OR (kind = 'user' AND role_key IS NOT NULL AND role_key = 'member' AND (\
                    (status IN ('active', 'suspended') AND removed_at IS NULL) \
                    OR (status = 'removed' AND removed_at IS NOT NULL)\
                ))\
            )",
        ))
        .to_owned();
    ensure_zero(
        manager,
        &query,
        "gateway_principal contains rows outside the Epic 4 role/status contract",
    )
    .await
}

async fn add_principal_authorization_guard(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .alter_table(
            Table::alter()
                .table(Alias::new(GATEWAY_PRINCIPAL))
                .add_column(integer(PRINCIPAL_AUTHORIZATION_GUARD).default(1).check((
                    "ck_gateway_principal_authorization",
                    Expr::cust(
                        "authorization_guard = 1 AND (\
                                    (kind = 'superuser' AND role_key IS NULL \
                                        AND status = 'active' AND removed_at IS NULL) \
                                    OR (kind = 'user' AND role_key IS NOT NULL \
                                        AND role_key = 'member' AND (\
                                        (status IN ('active', 'suspended') \
                                            AND removed_at IS NULL) \
                                        OR (status = 'removed' AND removed_at IS NOT NULL)\
                                    ))\
                                )",
                    ),
                )))
                .to_owned(),
        )
        .await
}

async fn create_principal_authorization_index(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_index(
            Index::create()
                .name("idx_gateway_principal_authorization")
                .table(Alias::new(GATEWAY_PRINCIPAL))
                .col(Alias::new("gateway_id"))
                .col(Alias::new("kind"))
                .col(Alias::new("status"))
                .col(Alias::new("role_key"))
                .to_owned(),
        )
        .await
}

async fn add_thread_access_class(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .alter_table(
            Table::alter()
                .table(Alias::new(THREAD))
                .add_column(
                    string(THREAD_ACCESS_CLASS)
                        .string_len(32)
                        .default("private")
                        .check((
                            "ck_thread_access_class",
                            Expr::cust("access_class IN ('private', 'workspace', 'internal')"),
                        )),
                )
                .to_owned(),
        )
        .await
}

async fn create_thread_access_index(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_index(
            Index::create()
                .name("idx_thread_workspace_access")
                .table(Alias::new(THREAD))
                .col(Alias::new("workspace_id"))
                .col(Alias::new(THREAD_ACCESS_CLASS))
                .col(Alias::new("id"))
                .to_owned(),
        )
        .await
}

async fn create_workspace_membership(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(WORKSPACE_MEMBERSHIP))
                .col(string("principal_id").string_len(21))
                .col(string("workspace_id").string_len(21))
                .col(string("granted_by_actor_kind").string_len(32))
                .col(string("granted_by_actor_id").string_len(21).null())
                .col(timestamp_with_time_zone("created_at"))
                .col(timestamp_with_time_zone("updated_at"))
                .primary_key(
                    Index::create()
                        .name("pk_workspace_membership")
                        .col(Alias::new("principal_id"))
                        .col(Alias::new("workspace_id"))
                        .primary(),
                )
                .check((
                    "ck_workspace_membership_ids",
                    Expr::cust(
                        "length(principal_id) = 21 \
                         AND principal_id NOT GLOB '*[^A-Za-z0-9]*' \
                         AND length(workspace_id) = 21 \
                         AND workspace_id NOT GLOB '*[^A-Za-z0-9]*'",
                    ),
                ))
                .check((
                    "ck_workspace_membership_actor",
                    Expr::cust(
                        "(granted_by_actor_kind = 'system' AND granted_by_actor_id IS NULL) \
                         OR (granted_by_actor_kind = 'principal' \
                             AND length(granted_by_actor_id) = 21 \
                             AND granted_by_actor_id NOT GLOB '*[^A-Za-z0-9]*')",
                    ),
                ))
                .check((
                    "ck_workspace_membership_timestamps",
                    Expr::cust("updated_at >= created_at"),
                ))
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_workspace_membership_principal")
                        .from(Alias::new(WORKSPACE_MEMBERSHIP), Alias::new("principal_id"))
                        .to(Alias::new(GATEWAY_PRINCIPAL), Alias::new("id"))
                        .on_update(ForeignKeyAction::NoAction)
                        .on_delete(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_workspace_membership_workspace")
                        .from(Alias::new(WORKSPACE_MEMBERSHIP), Alias::new("workspace_id"))
                        .to(Alias::new(WORKSPACE), Alias::new("id"))
                        .on_update(ForeignKeyAction::NoAction)
                        .on_delete(ForeignKeyAction::Cascade),
                )
                .to_owned(),
        )
        .await
}

async fn create_workspace_membership_indexes(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    for index in [
        Index::create()
            .name("idx_workspace_membership_principal")
            .table(Alias::new(WORKSPACE_MEMBERSHIP))
            .col(Alias::new("principal_id"))
            .to_owned(),
        Index::create()
            .name("idx_workspace_membership_workspace")
            .table(Alias::new(WORKSPACE_MEMBERSHIP))
            .col(Alias::new("workspace_id"))
            .col(Alias::new("principal_id"))
            .to_owned(),
    ] {
        manager.create_index(index).await?;
    }
    Ok(())
}

async fn create_thread_membership(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(THREAD_MEMBERSHIP))
                .col(string("thread_id").string_len(21))
                .col(string("principal_id").string_len(21))
                .col(string("added_by_actor_kind").string_len(32))
                .col(string("added_by_actor_id").string_len(21).null())
                .col(timestamp_with_time_zone("created_at"))
                .col(timestamp_with_time_zone("updated_at"))
                .primary_key(
                    Index::create()
                        .name("pk_thread_membership")
                        .col(Alias::new("thread_id"))
                        .col(Alias::new("principal_id"))
                        .primary(),
                )
                .check((
                    "ck_thread_membership_ids",
                    Expr::cust(
                        "length(thread_id) = 21 \
                         AND thread_id NOT GLOB '*[^A-Za-z0-9]*' \
                         AND length(principal_id) = 21 \
                         AND principal_id NOT GLOB '*[^A-Za-z0-9]*'",
                    ),
                ))
                .check((
                    "ck_thread_membership_actor",
                    Expr::cust(
                        "(added_by_actor_kind = 'system' AND added_by_actor_id IS NULL) \
                         OR (added_by_actor_kind = 'principal' \
                             AND length(added_by_actor_id) = 21 \
                             AND added_by_actor_id NOT GLOB '*[^A-Za-z0-9]*')",
                    ),
                ))
                .check((
                    "ck_thread_membership_timestamps",
                    Expr::cust("updated_at >= created_at"),
                ))
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_thread_membership_thread")
                        .from(Alias::new(THREAD_MEMBERSHIP), Alias::new("thread_id"))
                        .to(Alias::new(THREAD), Alias::new("id"))
                        .on_update(ForeignKeyAction::NoAction)
                        .on_delete(ForeignKeyAction::Cascade),
                )
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_thread_membership_principal")
                        .from(Alias::new(THREAD_MEMBERSHIP), Alias::new("principal_id"))
                        .to(Alias::new(GATEWAY_PRINCIPAL), Alias::new("id"))
                        .on_update(ForeignKeyAction::NoAction)
                        .on_delete(ForeignKeyAction::Restrict),
                )
                .to_owned(),
        )
        .await
}

async fn create_thread_membership_indexes(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    for index in [
        Index::create()
            .name("idx_thread_membership_principal")
            .table(Alias::new(THREAD_MEMBERSHIP))
            .col(Alias::new("principal_id"))
            .col(Alias::new("thread_id"))
            .to_owned(),
        Index::create()
            .name("idx_thread_membership_thread")
            .table(Alias::new(THREAD_MEMBERSHIP))
            .col(Alias::new("thread_id"))
            .to_owned(),
    ] {
        manager.create_index(index).await?;
    }
    Ok(())
}

async fn drop_thread_access_class(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .alter_table(
            Table::alter()
                .table(Alias::new(THREAD))
                .drop_column(Alias::new(THREAD_ACCESS_CLASS))
                .to_owned(),
        )
        .await
}

async fn drop_principal_authorization_guard(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .alter_table(
            Table::alter()
                .table(Alias::new(GATEWAY_PRINCIPAL))
                .drop_column(Alias::new(PRINCIPAL_AUTHORIZATION_GUARD))
                .to_owned(),
        )
        .await
}

async fn drop_index(manager: &SchemaManager<'_>, index: &str) -> Result<(), DbErr> {
    manager
        .drop_index(Index::drop().if_exists().name(index).to_owned())
        .await
}

async fn ensure_zero(
    manager: &SchemaManager<'_>,
    query: &SelectStatement,
    message: &str,
) -> Result<(), DbErr> {
    let row = manager.get_connection().query_one(query).await?;
    let count = match row {
        Some(row) => row.try_get::<i64>("", "count")?,
        None => 0,
    };
    if count == 0 {
        Ok(())
    } else {
        Err(DbErr::Custom(format!("{message}: {count} row(s)")))
    }
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
