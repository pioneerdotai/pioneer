use sea_orm_migration::{prelude::*, schema::*};

const GATEWAY_PRINCIPAL: &str = "gateway_principal";
const PRINCIPAL_AUTHORIZATION_GUARD: &str = "authorization_guard";
const POLICY_STATE: &str = "authorization_policy_state";
const CHANGE_FEED: &str = "authorization_change_feed";
const TASK_EXECUTION_ADMISSION: &str = "task_execution_admission";
const CLI_RUNTIME_PENDING_REQUEST: &str = "cli_runtime_pending_request";
const USER_NOTIFICATION_OUTBOX: &str = "user_notification_outbox";
const EXECUTION_ADMISSION_LEASE: &str = "execution_admission_lease";
const LEGACY_POLICY_FINGERPRINT: &str =
    "0000000000000000000000000000000000000000000000000000000000000000";
const LEGACY_PRINCIPAL_CONTRACT: &str = "(kind = 'superuser' AND role_key IS NULL \
        AND status = 'active' AND removed_at IS NULL) \
     OR (kind = 'user' AND role_key IS NOT NULL AND role_key = 'member' AND (\
        (status IN ('active', 'suspended') AND removed_at IS NULL) \
        OR (status = 'removed' AND removed_at IS NOT NULL)\
     ))";
const ROLE_REGISTRY_PRINCIPAL_CONTRACT: &str = "(kind = 'superuser' AND role_key IS NULL \
        AND status = 'active' AND removed_at IS NULL) \
     OR (kind = 'user' AND role_key IS NOT NULL \
        AND length(role_key) BETWEEN 1 AND 32 \
        AND substr(role_key, 1, 1) GLOB '[a-z]' \
        AND role_key NOT GLOB '*[^a-z0-9_-]*' AND (\
            (status IN ('active', 'suspended') AND removed_at IS NULL) \
            OR (status = 'removed' AND removed_at IS NOT NULL)\
        ))";

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        replace_principal_authorization_guard(manager, ROLE_REGISTRY_PRINCIPAL_CONTRACT).await?;
        add_authorization_provenance(manager).await?;
        create_policy_generation_tables(manager).await?;
        create_task_execution_admission(manager).await?;
        add_human_interaction_delivery(manager).await?;
        create_user_notification_outbox(manager).await?;
        create_execution_admission_lease(manager).await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        validate_principals(manager, LEGACY_PRINCIPAL_CONTRACT).await?;
        replace_principal_authorization_guard(manager, LEGACY_PRINCIPAL_CONTRACT).await?;
        drop_table(manager, EXECUTION_ADMISSION_LEASE).await?;
        drop_table(manager, USER_NOTIFICATION_OUTBOX).await?;
        drop_human_interaction_delivery(manager).await?;
        drop_table(manager, TASK_EXECUTION_ADMISSION).await?;
        drop_table(manager, CHANGE_FEED).await?;
        drop_table(manager, POLICY_STATE).await?;
        drop_authorization_provenance(manager).await
    }

    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }
}

async fn validate_principals(manager: &SchemaManager<'_>, contract: &str) -> Result<(), DbErr> {
    let query = Query::select()
        .expr_as(Func::count(Expr::col(Asterisk)), Alias::new("count"))
        .from(Alias::new(GATEWAY_PRINCIPAL))
        .and_where(Expr::cust(format!("NOT ({contract})")))
        .to_owned();
    let row = manager.get_connection().query_one(&query).await?;
    let count = match row {
        Some(row) => row.try_get::<i64>("", "count")?,
        None => 0,
    };
    if count == 0 {
        Ok(())
    } else {
        Err(DbErr::Custom(format!(
            "gateway_principal contains {count} row(s) outside the authorization contract"
        )))
    }
}

async fn replace_principal_authorization_guard(
    manager: &SchemaManager<'_>,
    contract: &str,
) -> Result<(), DbErr> {
    validate_principals(manager, contract).await?;
    manager
        .alter_table(
            Table::alter()
                .table(Alias::new(GATEWAY_PRINCIPAL))
                .drop_column(Alias::new(PRINCIPAL_AUTHORIZATION_GUARD))
                .to_owned(),
        )
        .await?;
    manager
        .alter_table(
            Table::alter()
                .table(Alias::new(GATEWAY_PRINCIPAL))
                .add_column(integer(PRINCIPAL_AUTHORIZATION_GUARD).default(1).check((
                    "ck_gateway_principal_authorization",
                    Expr::cust(format!(
                        "{PRINCIPAL_AUTHORIZATION_GUARD} = 1 AND ({contract})"
                    )),
                )))
                .to_owned(),
        )
        .await
}

async fn add_authorization_provenance(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    for (table, name, column) in [
        (
            "turn_admission",
            "policy_generation",
            big_integer("policy_generation").null(),
        ),
        (
            "turn_admission",
            "role_key",
            string("role_key").string_len(32).null(),
        ),
        (
            "turn_admission",
            "policy_fingerprint",
            string("policy_fingerprint").string_len(64).null(),
        ),
        (
            "invitation",
            "target_role_key",
            string("target_role_key").string_len(32).default("member"),
        ),
        (
            "audit_event",
            "policy_generation",
            big_integer("policy_generation").default(1),
        ),
        (
            "audit_event",
            "policy_role_key",
            string("policy_role_key").string_len(32).null(),
        ),
        (
            "audit_event",
            "policy_fingerprint",
            string("policy_fingerprint")
                .string_len(64)
                .default(LEGACY_POLICY_FINGERPRINT)
                .check((
                    "ck_audit_event_policy_provenance",
                    Expr::cust(
                        "policy_generation > 0 \
                         AND length(policy_fingerprint) = 64 \
                         AND policy_fingerprint NOT GLOB '*[^0-9a-f]*' \
                         AND (policy_role_key IS NULL OR (\
                            length(policy_role_key) BETWEEN 1 AND 32 \
                            AND substr(policy_role_key, 1, 1) GLOB '[a-z]' \
                            AND policy_role_key NOT GLOB '*[^a-z0-9_-]*'\
                         ))",
                    ),
                )),
        ),
    ] {
        add_column_if_missing(manager, table, name, column.to_owned()).await?;
    }
    Ok(())
}

async fn drop_authorization_provenance(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    for (table, column) in [
        ("audit_event", "policy_fingerprint"),
        ("audit_event", "policy_role_key"),
        ("audit_event", "policy_generation"),
        ("invitation", "target_role_key"),
        ("turn_admission", "policy_fingerprint"),
        ("turn_admission", "role_key"),
        ("turn_admission", "policy_generation"),
    ] {
        drop_column_if_present(manager, table, column).await?;
    }
    Ok(())
}

async fn create_policy_generation_tables(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(POLICY_STATE))
                .col(integer("singleton_id").primary_key())
                .col(big_integer("generation"))
                .col(string("code_policy_fingerprint").string_len(64).default(""))
                .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                .check((
                    "ck_authorization_policy_state_singleton",
                    Expr::cust("singleton_id = 1 AND generation > 0"),
                ))
                .to_owned(),
        )
        .await?;
    manager
        .exec_stmt(
            Query::insert()
                .into_table(Alias::new(POLICY_STATE))
                .columns([
                    Alias::new("singleton_id"),
                    Alias::new("generation"),
                    Alias::new("code_policy_fingerprint"),
                ])
                .values_panic([1.into(), 1.into(), "".into()])
                .to_owned(),
        )
        .await?;
    manager
        .create_table(
            Table::create()
                .table(Alias::new(CHANGE_FEED))
                .col(big_integer("generation").primary_key())
                .col(string("change_kind").string_len(32))
                .col(text("affected_scope_json"))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .to_owned(),
        )
        .await
}

async fn create_task_execution_admission(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(TASK_EXECUTION_ADMISSION))
                .col(string("task_id").string_len(21).primary_key())
                .col(string("workspace_id").string_len(21))
                .col(string("root_thread_id").string_len(21))
                .col(string("initiating_principal_id").string_len(21))
                .col(text("authorization_context_json"))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_task_execution_admission_task")
                        .from(Alias::new(TASK_EXECUTION_ADMISSION), Alias::new("task_id"))
                        .to(Alias::new("task"), Alias::new("id"))
                        .on_delete(ForeignKeyAction::Cascade),
                )
                .to_owned(),
        )
        .await
}

async fn add_human_interaction_delivery(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    let mut responding_principal_id = string_null("responding_principal_id");
    responding_principal_id.string_len(21);
    let mut responding_session_id = string_null("responding_session_id");
    responding_session_id.string_len(21);
    let mut response_authorization_revision = big_integer("response_authorization_revision");
    response_authorization_revision.null();
    let mut delivery_attempts = big_integer("delivery_attempts");
    delivery_attempts.default(0);
    let delivery_error = text_null("delivery_error");
    let mut response_contains_secret = boolean("response_contains_secret");
    response_contains_secret.default(false);

    for (name, column) in [
        ("responding_principal_id", responding_principal_id),
        ("responding_session_id", responding_session_id),
        (
            "response_authorization_revision",
            response_authorization_revision,
        ),
        ("delivery_attempts", delivery_attempts),
        ("delivery_error", delivery_error),
        ("response_contains_secret", response_contains_secret),
    ] {
        add_column_if_missing(manager, CLI_RUNTIME_PENDING_REQUEST, name, column).await?;
    }
    Ok(())
}

async fn drop_human_interaction_delivery(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    for column in [
        "response_contains_secret",
        "delivery_error",
        "delivery_attempts",
        "response_authorization_revision",
        "responding_session_id",
        "responding_principal_id",
    ] {
        drop_column_if_present(manager, CLI_RUNTIME_PENDING_REQUEST, column).await?;
    }
    Ok(())
}

async fn create_user_notification_outbox(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(USER_NOTIFICATION_OUTBOX))
                .col(string("id").string_len(64).primary_key())
                .col(string("task_delivery_id").string_len(64))
                .col(string("workspace_id").string_len(64))
                .col(string("recipient_principal_id").string_len(64))
                .col(string("task_id").string_len(64))
                .col(string("run_id").string_len(64))
                .col(text("payload_json"))
                .col(string("status").string_len(32))
                .col(timestamp_with_time_zone("created_at"))
                .col(timestamp_with_time_zone("delivered_at"))
                .col(timestamp_with_time_zone_null("acknowledged_at"))
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_user_notification_task_delivery")
                        .from(
                            Alias::new(USER_NOTIFICATION_OUTBOX),
                            Alias::new("task_delivery_id"),
                        )
                        .to(Alias::new("task_delivery"), Alias::new("id"))
                        .on_delete(ForeignKeyAction::Cascade),
                )
                .to_owned(),
        )
        .await?;
    manager
        .create_index(
            Index::create()
                .name("uq_user_notification_task_delivery")
                .table(Alias::new(USER_NOTIFICATION_OUTBOX))
                .col(Alias::new("task_delivery_id"))
                .unique()
                .to_owned(),
        )
        .await?;
    manager
        .create_index(
            Index::create()
                .name("idx_user_notification_recipient_inbox")
                .table(Alias::new(USER_NOTIFICATION_OUTBOX))
                .col(Alias::new("recipient_principal_id"))
                .col(Alias::new("workspace_id"))
                .col(Alias::new("created_at"))
                .col(Alias::new("id"))
                .to_owned(),
        )
        .await
}

async fn create_execution_admission_lease(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(EXECUTION_ADMISSION_LEASE))
                .col(string("id").string_len(128).primary_key())
                .col(string("subject_kind").string_len(32))
                .col(string("subject_id").string_len(128))
                .col(string("operation_class").string_len(32))
                .col(string("quota_bucket").string_len(16))
                .col(string("principal_id").string_len(64))
                .col(string("role_key").string_len(64))
                .col(string("workspace_id").string_len(64))
                .col(string("policy_fingerprint").string_len(128))
                .col(string("status").string_len(16))
                .col(timestamp_with_time_zone("created_at"))
                .col(timestamp_with_time_zone_null("released_at"))
                .to_owned(),
        )
        .await?;
    manager
        .create_index(
            Index::create()
                .name("uq_execution_admission_lease_subject")
                .table(Alias::new(EXECUTION_ADMISSION_LEASE))
                .col(Alias::new("subject_kind"))
                .col(Alias::new("subject_id"))
                .unique()
                .to_owned(),
        )
        .await?;
    manager
        .create_index(
            Index::create()
                .name("idx_execution_admission_lease_quota")
                .table(Alias::new(EXECUTION_ADMISSION_LEASE))
                .col(Alias::new("status"))
                .col(Alias::new("quota_bucket"))
                .col(Alias::new("workspace_id"))
                .col(Alias::new("role_key"))
                .col(Alias::new("principal_id"))
                .to_owned(),
        )
        .await
}

async fn add_column_if_missing(
    manager: &SchemaManager<'_>,
    table: &str,
    name: &str,
    column: ColumnDef,
) -> Result<(), DbErr> {
    if manager.has_column(table, name).await? {
        return Ok(());
    }
    manager
        .alter_table(
            Table::alter()
                .table(Alias::new(table))
                .add_column(column)
                .to_owned(),
        )
        .await
}

async fn drop_column_if_present(
    manager: &SchemaManager<'_>,
    table: &str,
    column: &str,
) -> Result<(), DbErr> {
    if !manager.has_column(table, column).await? {
        return Ok(());
    }
    manager
        .alter_table(
            Table::alter()
                .table(Alias::new(table))
                .drop_column(Alias::new(column))
                .to_owned(),
        )
        .await
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
