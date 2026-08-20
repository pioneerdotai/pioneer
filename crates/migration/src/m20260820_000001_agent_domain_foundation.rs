use sea_orm::{ConnectionTrait, Statement};
use sea_orm_migration::{prelude::*, schema::*};
use sha2::{Digest, Sha256};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        create_identity_schema(manager).await?;
        create_execution_schema(manager).await?;
        create_resource_schema(manager).await?;
        create_domain_commit_schema(manager).await?;
        create_task_actor_contract_schema(manager).await?;
        Ok(())
    }

    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        Err(DbErr::Migration(
            "the Agent domain upgrade is irreversible".to_owned(),
        ))
    }

    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }
}

// --- identity persistence ---

const NATIVE_AGENT_CONFIG: &str = "native_agent_config";
const AGENT_IDENTITY: &str = "agent_identity";
const ACTOR_NICKNAME_INDEX: &str = "actor_nickname_index";
const AGENT_PRESENTATION_SNAPSHOT: &str = "agent_presentation_snapshot";

async fn create_identity_schema(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    create_native_agent_config(manager).await?;
    create_agent_identity(manager).await?;
    create_actor_nickname_index(manager).await?;
    create_agent_presentation_snapshot(manager).await?;
    seed_pioneer_sources(manager).await?;
    Ok(())
}

async fn create_native_agent_config(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(NATIVE_AGENT_CONFIG))
                .if_not_exists()
                .col(string("id").string_len(21).primary_key())
                .col(string("workspace_id").string_len(21))
                .col(string("system_key").string_len(128).null())
                .col(string("display_name").string_len(128))
                .col(string("nickname").string_len(32))
                .col(boolean("enabled").default(true))
                .col(string("avatar_revision").string_len(128).null())
                .col(big_integer("config_revision").default(1))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_native_agent_config_workspace")
                        .from(Alias::new(NATIVE_AGENT_CONFIG), Alias::new("workspace_id"))
                        .to(Alias::new("workspace"), Alias::new("id"))
                        .on_update(ForeignKeyAction::NoAction)
                        .on_delete(ForeignKeyAction::Cascade),
                )
                .check((
                    "ck_native_agent_config_id",
                    Expr::cust("length(id) = 21 AND id NOT GLOB '*[^A-Za-z0-9]*'"),
                ))
                .check((
                    "ck_native_agent_config_revision",
                    Expr::cust("config_revision >= 1"),
                ))
                .to_owned(),
        )
        .await?;
    manager
        .create_index(
            Index::create()
                .if_not_exists()
                .name("uq_native_agent_config_workspace_system_key")
                .table(Alias::new(NATIVE_AGENT_CONFIG))
                .col(Alias::new("workspace_id"))
                .col(Alias::new("system_key"))
                .unique()
                .to_owned(),
        )
        .await
}

async fn create_agent_identity(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(AGENT_IDENTITY))
                .if_not_exists()
                .col(string("id").string_len(21).primary_key())
                .col(string("workspace_id").string_len(21))
                .col(string("source_kind").string_len(32))
                .col(string("source_id").string_len(255))
                .col(big_integer("source_revision").default(1))
                .col(string("source_fingerprint").string_len(512))
                .col(string("status").string_len(32).default("active"))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("retired_at").null())
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_agent_identity_workspace")
                        .from(Alias::new(AGENT_IDENTITY), Alias::new("workspace_id"))
                        .to(Alias::new("workspace"), Alias::new("id"))
                        .on_update(ForeignKeyAction::NoAction)
                        .on_delete(ForeignKeyAction::Cascade),
                )
                .check((
                    "ck_agent_identity_id",
                    Expr::cust("length(id) = 21 AND id NOT GLOB '*[^A-Za-z0-9]*'"),
                ))
                .check((
                    "ck_agent_identity_source_kind",
                    Expr::cust(
                        "source_kind IN ('native_agent', 'cli_runtime_instance', 'ephemeral')",
                    ),
                ))
                .check((
                    "ck_agent_identity_source_revision",
                    Expr::cust("source_revision >= 1"),
                ))
                .check((
                    "ck_agent_identity_status",
                    Expr::cust("status IN ('active', 'retired')"),
                ))
                .check((
                    "ck_agent_identity_retirement",
                    Expr::cust(
                        "(status = 'retired' AND retired_at IS NOT NULL) \
                         OR (status = 'active' AND retired_at IS NULL)",
                    ),
                ))
                .to_owned(),
        )
        .await?;
    manager
        .create_index(
            Index::create()
                .if_not_exists()
                .name("uq_agent_identity_workspace_source")
                .table(Alias::new(AGENT_IDENTITY))
                .col(Alias::new("workspace_id"))
                .col(Alias::new("source_kind"))
                .col(Alias::new("source_id"))
                .unique()
                .to_owned(),
        )
        .await
}

async fn create_actor_nickname_index(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(ACTOR_NICKNAME_INDEX))
                .if_not_exists()
                .col(string("workspace_id").string_len(21))
                .col(string("nickname_key").string_len(32))
                .col(string("owner_kind").string_len(32))
                .col(string("owner_id").string_len(255))
                .col(string("status").string_len(32).default("active"))
                .col(timestamp_with_time_zone("claimed_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("tombstoned_at").null())
                .check((
                    "ck_actor_nickname_status",
                    Expr::cust(
                        "(status = 'active' AND tombstoned_at IS NULL) \
                         OR (status = 'tombstoned' AND tombstoned_at IS NOT NULL)",
                    ),
                ))
                .check((
                    "ck_actor_nickname_owner_kind",
                    Expr::cust("owner_kind IN ('principal', 'agent', 'reserved')"),
                ))
                .check((
                    "ck_actor_nickname_key",
                    Expr::cust(
                        "length(nickname_key) BETWEEN 2 AND 32 \
                         AND nickname_key NOT GLOB '*[^a-z0-9._-]*'",
                    ),
                ))
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_actor_nickname_workspace")
                        .from(Alias::new(ACTOR_NICKNAME_INDEX), Alias::new("workspace_id"))
                        .to(Alias::new("workspace"), Alias::new("id"))
                        .on_update(ForeignKeyAction::NoAction)
                        .on_delete(ForeignKeyAction::Cascade),
                )
                .primary_key(
                    Index::create()
                        .name("pk_actor_nickname_index")
                        .col(Alias::new("workspace_id"))
                        .col(Alias::new("nickname_key")),
                )
                .to_owned(),
        )
        .await?;
    // Nickname rows are historical claims, not a one-row-per-owner
    // table.  A rename tombstones the old key and inserts a new active
    // key for the same owner, so ownership uniqueness must be enforced by
    // the active-claim transaction rather than by a global unique index.
    Ok(())
}

async fn create_agent_presentation_snapshot(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(AGENT_PRESENTATION_SNAPSHOT))
                .if_not_exists()
                .col(string("id").string_len(21).primary_key())
                .col(string("agent_identity_id").string_len(21))
                .col(big_integer("source_revision"))
                .col(string("source_fingerprint").string_len(512))
                .col(string("display_name").string_len(128))
                .col(string("nickname").string_len(32))
                .col(string("avatar_revision").string_len(128).null())
                .col(string("role_label").string_len(64).null())
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_agent_presentation_snapshot_identity")
                        .from(
                            Alias::new(AGENT_PRESENTATION_SNAPSHOT),
                            Alias::new("agent_identity_id"),
                        )
                        .to(Alias::new(AGENT_IDENTITY), Alias::new("id"))
                        .on_update(ForeignKeyAction::NoAction)
                        .on_delete(ForeignKeyAction::Restrict),
                )
                .check((
                    "ck_agent_presentation_snapshot_id",
                    Expr::cust("length(id) = 21 AND id NOT GLOB '*[^A-Za-z0-9]*'"),
                ))
                .to_owned(),
        )
        .await?;
    manager
        .create_index(
            Index::create()
                .if_not_exists()
                .name("uq_agent_presentation_snapshot_source")
                .table(Alias::new(AGENT_PRESENTATION_SNAPSHOT))
                .col(Alias::new("agent_identity_id"))
                .col(Alias::new("source_revision"))
                .col(Alias::new("source_fingerprint"))
                .unique()
                .to_owned(),
        )
        .await
}

async fn seed_pioneer_sources(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    let backend = manager.get_database_backend();
    let rows = manager
        .get_connection()
        .query_all_raw(Statement::from_string(
            backend,
            "SELECT id FROM workspace ORDER BY id".to_owned(),
        ))
        .await?;

    for row in rows {
        let workspace_id = row.try_get::<String>("", "id")?;
        let config_id = deterministic_id('N', &format!("pioneer-config\0{workspace_id}"));
        let identity_id = deterministic_id('A', &format!("pioneer-identity\0{workspace_id}"));
        let snapshot_id = deterministic_id('S', &format!("pioneer-snapshot\0{workspace_id}"));
        let values = vec![
            config_id.clone().into(),
            workspace_id.clone().into(),
            "pioneer".into(),
            "Pioneer".into(),
            "pioneer".into(),
            true.into(),
            1_i64.into(),
        ];
        manager
            .get_connection()
            .execute_raw(Statement::from_sql_and_values(
                backend,
                "INSERT INTO native_agent_config \
                 (id, workspace_id, system_key, display_name, nickname, enabled, config_revision) \
                 VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT(id) DO NOTHING",
                values,
            ))
            .await?;

        manager
            .get_connection()
            .execute_raw(Statement::from_sql_and_values(
                backend,
                "INSERT INTO agent_identity \
                 (id, workspace_id, source_kind, source_id, source_revision, source_fingerprint) \
                 VALUES (?, ?, 'native_agent', ?, 1, 'seed:pioneer:v1') \
                 ON CONFLICT(id) DO NOTHING",
                vec![
                    identity_id.clone().into(),
                    workspace_id.clone().into(),
                    config_id.into(),
                ],
            ))
            .await?;

        manager
            .get_connection()
            .execute_raw(Statement::from_sql_and_values(
                backend,
                "INSERT INTO actor_nickname_index \
                 (workspace_id, nickname_key, owner_kind, owner_id, status) \
                 VALUES (?, 'pioneer', 'reserved', 'pioneer', 'active') \
                 ON CONFLICT(workspace_id, nickname_key) DO NOTHING",
                vec![workspace_id.clone().into()],
            ))
            .await?;

        manager
            .get_connection()
            .execute_raw(Statement::from_sql_and_values(
                backend,
                "INSERT INTO agent_presentation_snapshot \
                 (id, agent_identity_id, source_revision, source_fingerprint, display_name, nickname, role_label) \
                 VALUES (?, ?, 1, 'seed:pioneer:v1', 'Pioneer', 'pioneer', NULL) \
                 ON CONFLICT(id) DO NOTHING",
                vec![snapshot_id.into(), identity_id.into()],
            ))
            .await?;
    }
    Ok(())
}

/// Must stay byte-for-byte aligned with the identity catalog writer in
/// `pioneer-crud`. Domain-separated SHA-256 inputs avoid truncation collisions
/// between all-numeric and normal 21-character workspace IDs.
fn deterministic_id(prefix: char, key: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(key.as_bytes());
    format!("{prefix}{}", &hex::encode(digest.finalize())[..20])
}

// --- execution persistence ---

const AGENT_EXECUTION: &str = "agent_execution";
const AGENT_ACTION: &str = "agent_action";
const AGENT_ACTION_RECEIPT: &str = "agent_action_receipt";
const AGENT_DELEGATION_ROUTE: &str = "agent_delegation_route";
const AGENT_DELEGATION_ROUTE_EVENT: &str = "agent_delegation_route_event";
const AGENT_EXECUTION_GRANT: &str = "agent_execution_grant";
const AGENT_TURN_RESPONSE_EXECUTION: &str = "agent_turn_response_execution";
const AGENT_ACTION_OUTBOX: &str = "agent_action_outbox";

async fn create_execution_schema(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    create_execution(manager).await?;
    create_action(manager).await?;
    create_receipt(manager).await?;
    create_route(manager).await?;
    create_grant(manager).await?;
    create_turn_response(manager).await?;
    create_outbox(manager).await?;
    Ok(())
}

async fn create_execution(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(AGENT_EXECUTION))
                .if_not_exists()
                .col(string("id").string_len(21).primary_key())
                .col(string("workspace_id").string_len(21))
                .col(string("agent_identity_id").string_len(21))
                .col(big_integer("identity_source_revision"))
                .col(string("identity_source_fingerprint").string_len(512))
                .col(string("parent_execution_id").string_len(21).null())
                .col(string("parent_task_id").string_len(21).null())
                .col(string("parent_thread_id").string_len(21).null())
                .col(string("home_root_thread_id").string_len(21))
                .col(string("work_graph_root_execution_id").string_len(21))
                .col(text("requested_identity_selection_json"))
                .col(text("requested_profile_selection_json"))
                .col(string("resolved_profile_id").string_len(21).null())
                .col(
                    string("resolved_profile_fingerprint")
                        .string_len(512)
                        .null(),
                )
                .col(string("presentation_snapshot_id").string_len(21).null())
                .col(string("authorization_context_fingerprint").string_len(512))
                .col(big_integer("execution_generation").default(1))
                .col(string("status").string_len(32).default("created"))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("finished_at").null())
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_agent_execution_workspace")
                        .from(Alias::new(AGENT_EXECUTION), Alias::new("workspace_id"))
                        .to(Alias::new("workspace"), Alias::new("id"))
                        .on_delete(ForeignKeyAction::Cascade),
                )
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_agent_execution_identity")
                        .from(Alias::new(AGENT_EXECUTION), Alias::new("agent_identity_id"))
                        .to(Alias::new("agent_identity"), Alias::new("id"))
                        .on_delete(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_agent_execution_snapshot")
                        .from(
                            Alias::new(AGENT_EXECUTION),
                            Alias::new("presentation_snapshot_id"),
                        )
                        .to(Alias::new("agent_presentation_snapshot"), Alias::new("id"))
                        .on_delete(ForeignKeyAction::Restrict),
                )
                .check((
                    "ck_agent_execution_ids",
                    Expr::cust(
                        "length(id) = 21 AND id NOT GLOB '*[^A-Za-z0-9]*' \
                         AND length(work_graph_root_execution_id) = 21",
                    ),
                ))
                .check((
                    "ck_agent_execution_generation",
                    Expr::cust("execution_generation >= 1"),
                ))
                .to_owned(),
        )
        .await?;
    for (name, columns) in [
        (
            "idx_agent_execution_graph_status",
            ["work_graph_root_execution_id", "status", "id"],
        ),
        (
            "idx_agent_execution_identity_status",
            ["agent_identity_id", "status", "id"],
        ),
    ] {
        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name(name)
                    .table(Alias::new(AGENT_EXECUTION))
                    .col(Alias::new(columns[0]))
                    .col(Alias::new(columns[1]))
                    .col(Alias::new(columns[2]))
                    .to_owned(),
            )
            .await?;
    }
    manager
        .create_index(
            Index::create()
                .if_not_exists()
                .name("idx_agent_execution_parent")
                .table(Alias::new(AGENT_EXECUTION))
                .col(Alias::new("parent_execution_id"))
                .to_owned(),
        )
        .await
}

async fn create_action(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(AGENT_ACTION))
                .if_not_exists()
                .col(string("id").string_len(21).primary_key())
                .col(string("execution_id").string_len(21))
                .col(string("action_kind").string_len(64))
                .col(string("idempotency_key").string_len(255))
                .col(string("request_fingerprint").string_len(512))
                .col(string("status").string_len(32).default("prepared"))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("committed_at").null())
                .col(text("response_json").null())
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_agent_action_execution")
                        .from(Alias::new(AGENT_ACTION), Alias::new("execution_id"))
                        .to(Alias::new(AGENT_EXECUTION), Alias::new("id"))
                        .on_delete(ForeignKeyAction::Cascade),
                )
                .check((
                    "ck_agent_action_status",
                    Expr::cust("status IN ('prepared', 'committed', 'failed')"),
                ))
                .to_owned(),
        )
        .await?;
    manager
        .create_index(
            Index::create()
                .if_not_exists()
                .name("uq_agent_action_idempotency")
                .table(Alias::new(AGENT_ACTION))
                .col(Alias::new("execution_id"))
                .col(Alias::new("idempotency_key"))
                .unique()
                .to_owned(),
        )
        .await
}

async fn create_receipt(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(AGENT_ACTION_RECEIPT))
                .if_not_exists()
                .col(string("id").string_len(21).primary_key())
                .col(string("action_id").string_len(21))
                .col(string("actor_kind").string_len(32))
                .col(string("actor_id").string_len(255).null())
                .col(string("decision").string_len(32))
                .col(string("policy_fingerprint").string_len(512))
                .col(string("execution_grant_fingerprint").string_len(64).null())
                .col(big_integer("execution_grant_policy_generation").null())
                .col(string("source_scope_id").string_len(21).null())
                .col(string("destination_scope_id").string_len(21).null())
                .col(string("action_kind").string_len(64).null())
                .col(string("authorized_resource_action").string_len(64).null())
                .col(string("subject_role_key").string_len(64).null())
                .col(big_integer("execution_generation").null())
                .col(big_integer("source_policy_generation").null())
                .col(big_integer("destination_policy_generation").null())
                .col(big_integer("route_generation").null())
                .col(string("disclosure_class").string_len(32).null())
                .col(string("decision_fingerprint").string_len(64).null())
                .col(timestamp_with_time_zone("committed_at").default(Expr::current_timestamp()))
                .col(text("response_json").null())
                .col(text("route_receipt_json").null())
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_agent_action_receipt_action")
                        .from(Alias::new(AGENT_ACTION_RECEIPT), Alias::new("action_id"))
                        .to(Alias::new(AGENT_ACTION), Alias::new("id"))
                        .on_delete(ForeignKeyAction::Cascade),
                )
                .check((
                    "ck_agent_action_receipt_decision",
                    Expr::cust("decision IN ('allowed', 'denied', 'failed')"),
                ))
                .to_owned(),
        )
        .await?;
    manager
        .create_index(
            Index::create()
                .if_not_exists()
                .name("uq_agent_action_receipt_action")
                .table(Alias::new(AGENT_ACTION_RECEIPT))
                .col(Alias::new("action_id"))
                .unique()
                .to_owned(),
        )
        .await
}

async fn create_route(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(AGENT_DELEGATION_ROUTE))
                .if_not_exists()
                .col(string("id").string_len(21).primary_key())
                .col(string("source_execution_id").string_len(21))
                .col(string("destination_thread_id").string_len(21))
                .col(
                    string("destination_agent_identity_id")
                        .string_len(21)
                        .null(),
                )
                .col(string("home_capsule_id").string_len(21).null())
                .col(string("route_kind").string_len(64))
                .col(string("grant_fingerprint").string_len(512))
                .col(string("source_capsule_id").string_len(21).null())
                .col(string("destination_capsule_id").string_len(21).null())
                .col(string("source_workspace_id").string_len(21).null())
                .col(string("destination_workspace_id").string_len(21).null())
                .col(string("source_gateway_id").string_len(21).null())
                .col(string("destination_gateway_id").string_len(21).null())
                .col(string("source_identity_id").string_len(21).null())
                .col(string("destination_profile_id").string_len(21).null())
                .col(text("allowed_actions_json").default("[]"))
                .col(text("disclosure_json").default("{}"))
                .col(big_integer("route_generation").default(1))
                .col(big_integer("source_policy_generation").default(1))
                .col(big_integer("destination_policy_generation").default(1))
                .col(integer("hop_count").default(0))
                .col(integer("max_hops").default(8))
                .col(string("return_route_id").string_len(21).null())
                .col(text("authority_actor_json").null())
                .col(string("authority_fingerprint").string_len(512).null())
                .col(string("status").string_len(32).default("prepared"))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("expires_at").null())
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_agent_route_execution")
                        .from(
                            Alias::new(AGENT_DELEGATION_ROUTE),
                            Alias::new("source_execution_id"),
                        )
                        .to(Alias::new(AGENT_EXECUTION), Alias::new("id"))
                        .on_delete(ForeignKeyAction::Cascade),
                )
                .check((
                    "ck_agent_route_status",
                    Expr::cust("status IN ('prepared', 'active', 'expired', 'revoked')"),
                ))
                .check((
                    "ck_agent_route_kind",
                    Expr::cust("route_kind IN ('execution_bound', 'identity_bound')"),
                ))
                .check((
                    "ck_agent_route_generations",
                    Expr::cust(
                        "route_generation >= 1 AND source_policy_generation >= 1 \
                         AND destination_policy_generation >= 1",
                    ),
                ))
                .check((
                    "ck_agent_route_hops",
                    Expr::cust(
                        "hop_count >= 0 AND max_hops BETWEEN 1 AND 8 AND hop_count <= max_hops",
                    ),
                ))
                .to_owned(),
        )
        .await?;
    for (name, columns) in [
        (
            "idx_agent_route_source_page",
            vec!["source_execution_id", "created_at", "id"],
        ),
        (
            "idx_agent_route_identity_active",
            vec!["route_kind", "source_identity_id", "status", "expires_at"],
        ),
        ("idx_agent_route_expiry", vec!["status", "expires_at", "id"]),
        (
            "idx_agent_route_graph_edges",
            vec![
                "source_workspace_id",
                "destination_workspace_id",
                "source_gateway_id",
                "destination_gateway_id",
                "status",
                "expires_at",
            ],
        ),
    ] {
        let mut index = Index::create();
        index
            .if_not_exists()
            .name(name)
            .table(Alias::new(AGENT_DELEGATION_ROUTE));
        for column in columns {
            index.col(Alias::new(column));
        }
        manager.create_index(index.to_owned()).await?;
    }
    create_route_event(manager).await
}

async fn create_route_event(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(AGENT_DELEGATION_ROUTE_EVENT))
                .if_not_exists()
                .col(string("id").string_len(21).primary_key())
                .col(string("route_id").string_len(21))
                .col(string("event_kind").string_len(32))
                .col(big_integer("route_generation"))
                .col(text("authority_actor_json"))
                .col(string("authority_fingerprint").string_len(512))
                .col(timestamp_with_time_zone("occurred_at"))
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_agent_route_event_route")
                        .from(
                            Alias::new(AGENT_DELEGATION_ROUTE_EVENT),
                            Alias::new("route_id"),
                        )
                        .to(Alias::new(AGENT_DELEGATION_ROUTE), Alias::new("id"))
                        .on_delete(ForeignKeyAction::Cascade),
                )
                .check((
                    "ck_agent_route_event_kind",
                    Expr::cust("event_kind IN ('created', 'revoked', 'expired')"),
                ))
                .to_owned(),
        )
        .await?;
    manager
        .create_index(
            Index::create()
                .if_not_exists()
                .name("uq_agent_route_event_generation")
                .table(Alias::new(AGENT_DELEGATION_ROUTE_EVENT))
                .col(Alias::new("route_id"))
                .col(Alias::new("route_generation"))
                .unique()
                .to_owned(),
        )
        .await
}

async fn create_grant(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(AGENT_EXECUTION_GRANT))
                .if_not_exists()
                .col(string("id").string_len(21).primary_key())
                .col(string("execution_id").string_len(21))
                .col(string("parent_execution_id").string_len(21).null())
                .col(string("child_identity_id").string_len(21))
                .col(string("grant_fingerprint").string_len(512))
                .col(text("grant_json"))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_agent_grant_execution")
                        .from(
                            Alias::new(AGENT_EXECUTION_GRANT),
                            Alias::new("execution_id"),
                        )
                        .to(Alias::new(AGENT_EXECUTION), Alias::new("id"))
                        .on_delete(ForeignKeyAction::Cascade),
                )
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_agent_grant_identity")
                        .from(
                            Alias::new(AGENT_EXECUTION_GRANT),
                            Alias::new("child_identity_id"),
                        )
                        .to(Alias::new("agent_identity"), Alias::new("id"))
                        .on_delete(ForeignKeyAction::Restrict),
                )
                .to_owned(),
        )
        .await
}

async fn create_turn_response(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(AGENT_TURN_RESPONSE_EXECUTION))
                .if_not_exists()
                .col(string("turn_id").string_len(21).primary_key())
                .col(string("execution_id").string_len(21))
                .col(string("presentation_snapshot_id").string_len(21))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_agent_turn_response_turn")
                        .from(
                            Alias::new(AGENT_TURN_RESPONSE_EXECUTION),
                            Alias::new("turn_id"),
                        )
                        .to(Alias::new("turn"), Alias::new("id"))
                        .on_delete(ForeignKeyAction::Restrict),
                )
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_agent_turn_response_execution")
                        .from(
                            Alias::new(AGENT_TURN_RESPONSE_EXECUTION),
                            Alias::new("execution_id"),
                        )
                        .to(Alias::new(AGENT_EXECUTION), Alias::new("id"))
                        .on_delete(ForeignKeyAction::Cascade),
                )
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_agent_turn_response_presentation")
                        .from(
                            Alias::new(AGENT_TURN_RESPONSE_EXECUTION),
                            Alias::new("presentation_snapshot_id"),
                        )
                        .to(Alias::new("agent_presentation_snapshot"), Alias::new("id"))
                        .on_delete(ForeignKeyAction::Restrict),
                )
                .to_owned(),
        )
        .await?;
    manager
        .create_index(
            Index::create()
                .if_not_exists()
                .name("ix_agent_turn_response_execution_id")
                .table(Alias::new(AGENT_TURN_RESPONSE_EXECUTION))
                .col(Alias::new("execution_id"))
                .to_owned(),
        )
        .await
}

async fn create_outbox(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(AGENT_ACTION_OUTBOX))
                .if_not_exists()
                .col(string("id").string_len(21).primary_key())
                .col(string("action_id").string_len(21))
                .col(string("owner_execution_id").string_len(21))
                .col(text("payload_json"))
                .col(string("status").string_len(32).default("pending"))
                .col(integer("attempts").default(0))
                .col(timestamp_with_time_zone("next_attempt_at").null())
                .col(timestamp_with_time_zone("delivered_at").null())
                .col(text("last_error").null())
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_agent_outbox_action")
                        .from(Alias::new(AGENT_ACTION_OUTBOX), Alias::new("action_id"))
                        .to(Alias::new(AGENT_ACTION), Alias::new("id"))
                        .on_delete(ForeignKeyAction::Cascade),
                )
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_agent_outbox_execution")
                        .from(
                            Alias::new(AGENT_ACTION_OUTBOX),
                            Alias::new("owner_execution_id"),
                        )
                        .to(Alias::new(AGENT_EXECUTION), Alias::new("id"))
                        .on_delete(ForeignKeyAction::Cascade),
                )
                .check((
                    "ck_agent_outbox_status",
                    Expr::cust("status IN ('pending', 'delivered', 'failed')"),
                ))
                .check(("ck_agent_outbox_attempts", Expr::cust("attempts >= 0")))
                .to_owned(),
        )
        .await?;
    manager
        .create_index(
            Index::create()
                .if_not_exists()
                .name("uq_agent_outbox_action")
                .table(Alias::new(AGENT_ACTION_OUTBOX))
                .col(Alias::new("action_id"))
                .unique()
                .to_owned(),
        )
        .await
}

// --- resource persistence ---

const WORK_RESOURCE_SCOPE: &str = "agent_work_resource_scope";
const EXECUTION_RESOURCE_STATE: &str = "agent_execution_resource_state";
const WORK_QUEUE: &str = "agent_work_queue";
const RUNNING_PERMIT: &str = "agent_running_permit";

async fn create_resource_schema(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    create_scope(manager).await?;
    create_state(manager).await?;
    create_queue(manager).await?;
    create_permit(manager).await?;
    Ok(())
}

async fn create_scope(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(WORK_RESOURCE_SCOPE))
                .if_not_exists()
                .col(string("root_execution_id").string_len(21).primary_key())
                .col(big_integer("scope_generation").default(1))
                .col(integer("max_concurrency").default(1))
                .col(integer("max_queue_depth").default(2_048))
                .col(integer("max_depth").default(64))
                .col(integer("max_fan_out").default(128))
                .col(integer("max_total_nodes").default(4_096))
                .col(text("aggregate_usage_json"))
                .col(big_integer("queue_generation").default(0))
                .col(big_integer("last_scheduled_sequence").default(0))
                .col(timestamp_with_time_zone("last_scheduled_at").null())
                .col(string("status").string_len(32).default("active"))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_agent_resource_scope_root")
                        .from(
                            Alias::new(WORK_RESOURCE_SCOPE),
                            Alias::new("root_execution_id"),
                        )
                        .to(Alias::new("agent_execution"), Alias::new("id"))
                        .on_delete(ForeignKeyAction::Cascade),
                )
                .check((
                    "ck_agent_resource_scope_generation",
                    Expr::cust("scope_generation >= 1"),
                ))
                .check((
                    "ck_agent_resource_scope_concurrency",
                    Expr::cust("max_concurrency >= 1"),
                ))
                .check((
                    "ck_agent_resource_scope_queue_generation",
                    Expr::cust("queue_generation >= 0"),
                ))
                .check((
                    "ck_agent_resource_scope_limits",
                    Expr::cust(
                        "max_queue_depth BETWEEN 1 AND 2048 \
                         AND max_depth BETWEEN 1 AND 64 \
                         AND max_fan_out BETWEEN 1 AND 128 \
                         AND max_total_nodes BETWEEN 1 AND 4096",
                    ),
                ))
                .check((
                    "ck_agent_resource_scope_schedule_sequence",
                    Expr::cust("last_scheduled_sequence >= 0"),
                ))
                .check((
                    "ck_agent_resource_scope_status",
                    Expr::cust("status IN ('active', 'queued', 'recovering', 'closed')"),
                ))
                .to_owned(),
        )
        .await
}

async fn create_state(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(EXECUTION_RESOURCE_STATE))
                .if_not_exists()
                .col(string("id").string_len(21).primary_key())
                .col(string("execution_id").string_len(21))
                .col(big_integer("attempt_generation").default(1))
                .col(big_integer("progress_sequence").default(0))
                .col(text("progress_frontier_json"))
                .col(timestamp_with_time_zone("last_progress_at").null())
                .col(timestamp_with_time_zone("last_heartbeat_at").null())
                .col(timestamp_with_time_zone("idle_deadline").null())
                .col(timestamp_with_time_zone("hard_deadline").null())
                .col(text("local_usage_json"))
                .col(string("permit_id").string_len(21).null())
                .col(string("branch_key").string_len(255))
                .col(big_integer("fair_order"))
                .col(string("status").string_len(32).default("queued"))
                .col(big_integer("fencing_generation").default(1))
                .col(timestamp_with_time_zone("fenced_at").null())
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_agent_resource_state_execution")
                        .from(Alias::new(EXECUTION_RESOURCE_STATE), Alias::new("execution_id"))
                        .to(Alias::new("agent_execution"), Alias::new("id"))
                        .on_delete(ForeignKeyAction::Cascade),
                )
                .check(("ck_agent_resource_state_attempt", Expr::cust("attempt_generation >= 1")))
                .check(("ck_agent_resource_state_progress", Expr::cust("progress_sequence >= 0")))
                .check(("ck_agent_resource_state_fencing", Expr::cust("fencing_generation >= 1")))
                .check((
                    "ck_agent_resource_state_status",
                    Expr::cust("status IN ('queued', 'running', 'paused', 'completed', 'failed', 'cancelled', 'fenced')"),
                ))
                .to_owned(),
        )
        .await?;
    manager
        .create_index(
            Index::create()
                .if_not_exists()
                .name("uq_agent_resource_state_attempt")
                .table(Alias::new(EXECUTION_RESOURCE_STATE))
                .col(Alias::new("execution_id"))
                .col(Alias::new("attempt_generation"))
                .unique()
                .to_owned(),
        )
        .await
}

async fn create_queue(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(WORK_QUEUE))
                .if_not_exists()
                .col(string("id").string_len(21).primary_key())
                .col(string("root_execution_id").string_len(21))
                .col(string("execution_id").string_len(21))
                .col(big_integer("attempt_generation").default(1))
                .col(string("branch_key").string_len(255))
                .col(big_integer("enqueue_sequence"))
                .col(string("state").string_len(32).default("queued"))
                .col(timestamp_with_time_zone("eligible_at").null())
                .col(string("claim_token").string_len(128).null())
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_agent_work_queue_root")
                        .from(Alias::new(WORK_QUEUE), Alias::new("root_execution_id"))
                        .to(Alias::new("agent_execution"), Alias::new("id"))
                        .on_delete(ForeignKeyAction::Cascade),
                )
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_agent_work_queue_execution")
                        .from(Alias::new(WORK_QUEUE), Alias::new("execution_id"))
                        .to(Alias::new("agent_execution"), Alias::new("id"))
                        .on_delete(ForeignKeyAction::Cascade),
                )
                .check((
                    "ck_agent_work_queue_attempt",
                    Expr::cust("attempt_generation >= 1"),
                ))
                .check((
                    "ck_agent_work_queue_state",
                    Expr::cust(
                        "state IN ('queued', 'claimed', 'running', 'recovering', 'released', 'cancelled')",
                    ),
                ))
                .to_owned(),
        )
        .await?;
    manager
        .create_index(
            Index::create()
                .if_not_exists()
                .name("uq_agent_work_queue_attempt")
                .table(Alias::new(WORK_QUEUE))
                .col(Alias::new("root_execution_id"))
                .col(Alias::new("execution_id"))
                .col(Alias::new("attempt_generation"))
                .unique()
                .to_owned(),
        )
        .await?;
    manager
        .create_index(
            Index::create()
                .if_not_exists()
                .name("idx_agent_work_queue_root_state")
                .table(Alias::new(WORK_QUEUE))
                .col(Alias::new("root_execution_id"))
                .col(Alias::new("state"))
                .col(Alias::new("branch_key"))
                .col(Alias::new("enqueue_sequence"))
                .to_owned(),
        )
        .await?;
    manager
        .create_index(
            Index::create()
                .if_not_exists()
                .name("idx_agent_work_queue_ready")
                .table(Alias::new(WORK_QUEUE))
                .col(Alias::new("state"))
                .col(Alias::new("eligible_at"))
                .col(Alias::new("enqueue_sequence"))
                .col(Alias::new("root_execution_id"))
                .to_owned(),
        )
        .await
}

async fn create_permit(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(RUNNING_PERMIT))
                .if_not_exists()
                .col(string("id").string_len(21).primary_key())
                .col(string("root_execution_id").string_len(21))
                .col(string("execution_id").string_len(21))
                .col(big_integer("attempt_generation").default(1))
                .col(big_integer("lease_generation").default(1))
                .col(string("status").string_len(32).default("held"))
                .col(timestamp_with_time_zone("acquired_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("released_at").null())
                .col(timestamp_with_time_zone("fenced_at").null())
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_agent_running_permit_root")
                        .from(Alias::new(RUNNING_PERMIT), Alias::new("root_execution_id"))
                        .to(Alias::new("agent_execution"), Alias::new("id"))
                        .on_delete(ForeignKeyAction::Cascade),
                )
                .foreign_key(
                    ForeignKey::create()
                        .name("fk_agent_running_permit_execution")
                        .from(Alias::new(RUNNING_PERMIT), Alias::new("execution_id"))
                        .to(Alias::new("agent_execution"), Alias::new("id"))
                        .on_delete(ForeignKeyAction::Cascade),
                )
                .check(("ck_agent_running_permit_attempt", Expr::cust("attempt_generation >= 1")))
                .check(("ck_agent_running_permit_lease", Expr::cust("lease_generation >= 1")))
                .check((
                    "ck_agent_running_permit_status",
                    Expr::cust("status IN ('held', 'released', 'fenced')"),
                ))
                .check((
                    "ck_agent_running_permit_terminal_times",
                    Expr::cust("(status = 'held' AND released_at IS NULL AND fenced_at IS NULL) OR (status != 'held' AND (released_at IS NOT NULL OR fenced_at IS NOT NULL))"),
                ))
                .to_owned(),
        )
        .await?;
    manager
        .create_index(
            Index::create()
                .if_not_exists()
                .name("uq_agent_running_permit_attempt")
                .table(Alias::new(RUNNING_PERMIT))
                .col(Alias::new("root_execution_id"))
                .col(Alias::new("execution_id"))
                .col(Alias::new("attempt_generation"))
                .unique()
                .to_owned(),
        )
        .await
}

// --- atomic domain commit ---

const DOMAIN_COMMIT: &str = "agent_domain_commit";

async fn create_domain_commit_schema(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(DOMAIN_COMMIT))
                .if_not_exists()
                .col(string("id").string_len(21).primary_key())
                .col(string("mutation_kind").string_len(64))
                .col(string("idempotency_key").string_len(255))
                .col(string("request_fingerprint").string_len(512))
                .col(string("execution_id").string_len(21))
                .col(string("actor_identity_id").string_len(21))
                .col(string("receipt_id").string_len(21))
                .col(string("outbox_id").string_len(21))
                .col(string("status").string_len(32).default("committed"))
                .col(timestamp_with_time_zone("committed_at").default(Expr::current_timestamp()))
                .check((
                    "ck_agent_domain_commit_status",
                    Expr::cust("status IN ('committed', 'replayed', 'failed')"),
                ))
                .to_owned(),
        )
        .await?;

    manager
        .create_index(
            Index::create()
                .if_not_exists()
                .name("uq_agent_domain_commit_execution_idempotency")
                .table(Alias::new(DOMAIN_COMMIT))
                .col(Alias::new("execution_id"))
                .col(Alias::new("idempotency_key"))
                .unique()
                .to_owned(),
        )
        .await?;
    for (name, column) in [
        ("uq_agent_domain_commit_receipt", "receipt_id"),
        ("uq_agent_domain_commit_outbox", "outbox_id"),
    ] {
        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name(name)
                    .table(Alias::new(DOMAIN_COMMIT))
                    .col(Alias::new(column))
                    .unique()
                    .to_owned(),
            )
            .await?;
    }
    Ok(())
}

// --- task actor contracts ---

const TASK_ACTOR_CONTRACT: &str = "task_actor_contract";
const TASK_OCCURRENCE_CONTRACT: &str = "task_occurrence_contract";
const TASK_DELIVERY_AUTHORITY: &str = "task_delivery_authority";

async fn create_task_actor_contract_schema(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Alias::new(TASK_ACTOR_CONTRACT))
                .if_not_exists()
                .col(string("task_id").string_len(21).primary_key())
                .col(string("workspace_id").string_len(21))
                .col(text("creator_json"))
                .col(text_null("creator_snapshot_json"))
                .col(text("reviewer_json"))
                .col(text("delivery_json"))
                .col(text_null("launch_selection_json"))
                .col(text_null("requested_identity_json"))
                .col(string_null("resolved_identity_id").string_len(21))
                .col(string_null("resolved_profile_id").string_len(21))
                .col(string_null("source_config_fingerprint").string_len(512))
                .col(text_null("derived_child_launch_grant_json"))
                .col(string_null("execution_destination_thread_id").string_len(21))
                .col(string_null("execution_route_id").string_len(21))
                .col(text_null("execution_route_receipt_json"))
                .col(big_integer_null("execution_route_expires_at_millis"))
                .col(string_null("creator_work_graph_root_execution_id").string_len(21))
                .col(string_null("work_graph_root_execution_id").string_len(21))
                .col(string_null("root_resource_scope_id").string_len(21))
                .col(text_null("accounting_attribution_json"))
                .col(string_null("controller_principal_id").string_len(21))
                .col(big_integer("revision").default(1))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                .to_owned(),
        )
        .await?;
    manager
        .create_table(
            Table::create()
                .table(Alias::new(TASK_OCCURRENCE_CONTRACT))
                .if_not_exists()
                .col(string("occurrence_id").string_len(21).primary_key())
                .col(string("task_id").string_len(21))
                .col(string("run_id").string_len(21).unique_key())
                .col(string_null("trigger_id").string_len(21))
                .col(string("occurrence_key").string_len(255))
                .col(big_integer("execution_generation").default(1))
                .col(string_null("agent_execution_id").string_len(21))
                .col(string_null("work_graph_root_execution_id").string_len(21))
                .col(string_null("root_resource_scope_id").string_len(21))
                .col(string("status").string_len(32).default("queued"))
                .col(big_integer("queue_position").null())
                .col(big_integer("retry_attempt").default(0))
                .col(
                    string("action_idempotency_key")
                        .string_len(255)
                        .unique_key(),
                )
                .col(string_null("route_id").string_len(21))
                .col(string_null("result_return_route_id").string_len(21))
                .col(string_null("terminal_reason").string_len(512))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                .to_owned(),
        )
        .await?;
    manager
        .create_table(
            Table::create()
                .table(Alias::new(TASK_DELIVERY_AUTHORITY))
                .if_not_exists()
                .col(string("delivery_id").string_len(21).primary_key())
                .col(string("task_id").string_len(21))
                .col(string("run_id").string_len(21))
                .col(text("author_json"))
                .col(text_null("reviewer_json"))
                .col(string_null("destination_route_id").string_len(21))
                .col(text_null("route_receipt_json"))
                .col(big_integer("disclosure_generation").default(1))
                .col(string("idempotency_key").string_len(255).unique_key())
                .col(string("status").string_len(32).default("pending"))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                .to_owned(),
        )
        .await?;
    manager
        .create_index(
            Index::create()
                .if_not_exists()
                .name("idx_task_actor_contract_workspace")
                .table(Alias::new(TASK_ACTOR_CONTRACT))
                .col(Alias::new("workspace_id"))
                .to_owned(),
        )
        .await?;
    manager
        .create_index(
            Index::create()
                .if_not_exists()
                .name("idx_task_occurrence_generation")
                .table(Alias::new(TASK_OCCURRENCE_CONTRACT))
                .col(Alias::new("task_id"))
                .col(Alias::new("execution_generation"))
                .unique()
                .to_owned(),
        )
        .await?;
    manager
        .create_index(
            Index::create()
                .if_not_exists()
                .name("idx_task_occurrence_execution_owner")
                .table(Alias::new(TASK_OCCURRENCE_CONTRACT))
                .col(Alias::new("task_id"))
                .col(Alias::new("agent_execution_id"))
                .to_owned(),
        )
        .await?;
    manager
        .create_index(
            Index::create()
                .if_not_exists()
                .name("idx_task_occurrence_graph_owner")
                .table(Alias::new(TASK_OCCURRENCE_CONTRACT))
                .col(Alias::new("task_id"))
                .col(Alias::new("work_graph_root_execution_id"))
                .to_owned(),
        )
        .await?;
    manager
        .create_index(
            Index::create()
                .if_not_exists()
                .name("idx_task_occurrence_task_status")
                .table(Alias::new(TASK_OCCURRENCE_CONTRACT))
                .col(Alias::new("task_id"))
                .col(Alias::new("status"))
                .to_owned(),
        )
        .await?;
    manager
        .create_index(
            Index::create()
                .if_not_exists()
                .name("idx_task_occurrence_ready")
                .table(Alias::new(TASK_OCCURRENCE_CONTRACT))
                .col(Alias::new("status"))
                .col(Alias::new("queue_position"))
                .to_owned(),
        )
        .await?;
    manager
        .create_index(
            Index::create()
                .if_not_exists()
                .name("idx_task_delivery_authority_task")
                .table(Alias::new(TASK_DELIVERY_AUTHORITY))
                .col(Alias::new("task_id"))
                .col(Alias::new("run_id"))
                .to_owned(),
        )
        .await?;
    Ok(())
}
