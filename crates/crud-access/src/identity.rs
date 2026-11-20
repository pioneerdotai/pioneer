use anyhow::{Context, Result, bail};
use pioneer_entity::{gateway_identity, gateway_principal, thread, turn};
use pioneer_protocol::{
    AgentExecutionId, GatewayId, PersistedActorRef, PrincipalId, PrincipalKind, PrincipalStatus,
    RoleKey,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder,
    QuerySelect, Set,
    sea_query::{Expr, OnConflict},
};

pub const GATEWAY_SINGLETON_KEY: i64 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayIdentityRecord {
    pub id: GatewayId,
    pub singleton_key: i64,
    pub identity_bootstrap_version: i64,
    pub auth_schema_version: i64,
    pub auth_ready_at: Option<DateTimeWithTimeZone>,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayPrincipalRecord {
    pub id: PrincipalId,
    pub gateway_id: GatewayId,
    pub kind: PrincipalKind,
    pub role_key: Option<String>,
    pub status: PrincipalStatus,
    pub display_name: String,
    pub nickname: String,
    pub nickname_key: String,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
    pub removed_at: Option<DateTimeWithTimeZone>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewMemberPrincipalRow {
    pub id: PrincipalId,
    pub gateway_id: GatewayId,
    pub role_key: RoleKey,
    pub display_name: String,
    pub nickname: String,
    pub nickname_key: String,
    pub now: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActorResourceKind {
    Thread,
    Turn,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActorReferenceRow {
    pub resource_kind: ActorResourceKind,
    pub resource_id: String,
    pub actor_kind: Option<String>,
    pub actor_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentityInvariantRows {
    pub gateways: Vec<gateway_identity::Model>,
    pub principals: Vec<gateway_principal::Model>,
    pub actor_references: Vec<ActorReferenceRow>,
}

pub fn principal_kind_to_db(kind: PrincipalKind) -> &'static str {
    match kind {
        PrincipalKind::Superuser => "superuser",
        PrincipalKind::User => "user",
    }
}

pub fn principal_kind_from_db(value: &str) -> Result<PrincipalKind> {
    match value {
        "superuser" => Ok(PrincipalKind::Superuser),
        "user" => Ok(PrincipalKind::User),
        unknown => bail!("unknown persisted principal kind `{unknown}`"),
    }
}

pub fn principal_status_to_db(status: PrincipalStatus) -> &'static str {
    match status {
        PrincipalStatus::Active => "active",
        PrincipalStatus::Suspended => "suspended",
        PrincipalStatus::Removed => "removed",
    }
}

pub fn principal_status_from_db(value: &str) -> Result<PrincipalStatus> {
    match value {
        "active" => Ok(PrincipalStatus::Active),
        "suspended" => Ok(PrincipalStatus::Suspended),
        "removed" => Ok(PrincipalStatus::Removed),
        unknown => bail!("unknown persisted principal status `{unknown}`"),
    }
}

pub fn actor_ref_to_db(actor: &PersistedActorRef) -> (Option<String>, Option<String>) {
    match actor {
        PersistedActorRef::Principal(id) => (Some("principal".to_owned()), Some(id.to_string())),
        PersistedActorRef::AgentExecution(id) => {
            (Some("agent_execution".to_owned()), Some(id.to_string()))
        }
        PersistedActorRef::System => (Some("system".to_owned()), None),
    }
}

pub fn actor_ref_from_db(
    actor_kind: Option<&str>,
    actor_id: Option<&str>,
) -> Result<Option<PersistedActorRef>> {
    match (actor_kind, actor_id) {
        (None, None) => Ok(None),
        (Some("system"), None) => Ok(Some(PersistedActorRef::System)),
        (Some("principal"), Some(id)) => Ok(Some(PersistedActorRef::Principal(
            PrincipalId::new(id).context("invalid persisted principal actor id")?,
        ))),
        (Some("agent_execution"), Some(id)) => Ok(Some(PersistedActorRef::AgentExecution(
            AgentExecutionId::new(id).context("invalid persisted agent actor id")?,
        ))),
        (None, Some(_)) => bail!("persisted actor has an id without a kind"),
        (Some("system"), Some(_)) => bail!("persisted System actor must not have an id"),
        (Some("principal"), None) => bail!("persisted principal actor is missing its id"),
        (Some("agent_execution"), None) => {
            bail!("persisted agent actor is missing its id")
        }
        (Some(unknown), _) => bail!("unknown persisted actor kind `{unknown}`"),
    }
}

pub async fn list_gateway_identities<C: ConnectionTrait>(
    db: &C,
) -> Result<Vec<gateway_identity::Model>> {
    gateway_identity::Entity::find()
        .order_by_asc(gateway_identity::Column::Id)
        .all(db)
        .await
        .context("failed to list Gateway identities")
}

pub async fn load_gateway_singleton<C: ConnectionTrait>(
    db: &C,
) -> Result<Option<GatewayIdentityRecord>> {
    gateway_identity::Entity::find()
        .filter(gateway_identity::Column::SingletonKey.eq(GATEWAY_SINGLETON_KEY))
        .one(db)
        .await
        .context("failed to load Gateway identity singleton")?
        .map(gateway_identity_record_from_model)
        .transpose()
}

pub async fn create_gateway_singleton<C: ConnectionTrait>(
    db: &C,
    id: &GatewayId,
    identity_bootstrap_version: i64,
    now: DateTimeWithTimeZone,
) -> Result<GatewayIdentityRecord> {
    let model = new_gateway_identity(id, identity_bootstrap_version, now)
        .insert(db)
        .await
        .context("failed to create Gateway identity singleton")?;
    gateway_identity_record_from_model(model)
}

pub async fn ensure_gateway_singleton<C: ConnectionTrait>(
    transaction: &C,
    id: &GatewayId,
    now: DateTimeWithTimeZone,
) -> Result<(GatewayIdentityRecord, bool)> {
    let inserted = gateway_identity::Entity::insert(new_gateway_identity(id, 0, now))
        .on_conflict(
            OnConflict::column(gateway_identity::Column::SingletonKey)
                .do_nothing()
                .to_owned(),
        )
        .exec_without_returning(transaction)
        .await?
        == 1;
    let gateway = lock_gateway_singleton(transaction)
        .await?
        .context("Gateway identity disappeared during bootstrap")?;
    Ok((gateway, inserted))
}

pub async fn lock_gateway_singleton<C: ConnectionTrait>(
    transaction: &C,
) -> Result<Option<GatewayIdentityRecord>> {
    gateway_identity::Entity::find()
        .filter(gateway_identity::Column::SingletonKey.eq(GATEWAY_SINGLETON_KEY))
        .lock_exclusive()
        .one(transaction)
        .await
        .context("failed to lock Gateway identity singleton")?
        .map(gateway_identity_record_from_model)
        .transpose()
}

fn new_gateway_identity(
    id: &GatewayId,
    identity_bootstrap_version: i64,
    now: DateTimeWithTimeZone,
) -> gateway_identity::ActiveModel {
    gateway_identity::ActiveModel {
        unread_started_at: Set(now.timestamp()),
        id: Set(id.to_string()),
        singleton_key: Set(GATEWAY_SINGLETON_KEY),
        identity_bootstrap_version: Set(identity_bootstrap_version),
        auth_schema_version: Set(0),
        auth_ready_at: Set(None),
        created_at: Set(now),
        updated_at: Set(now),
    }
}

pub async fn set_identity_bootstrap_version<C: ConnectionTrait>(
    db: &C,
    id: &GatewayId,
    identity_bootstrap_version: i64,
    now: DateTimeWithTimeZone,
) -> Result<GatewayIdentityRecord> {
    let model = gateway_identity::Entity::find_by_id(id.to_string())
        .one(db)
        .await
        .context("failed to load Gateway identity for bootstrap marker update")?
        .context("Gateway identity disappeared before bootstrap marker update")?;
    let mut active: gateway_identity::ActiveModel = model.into();
    active.identity_bootstrap_version = Set(identity_bootstrap_version);
    active.updated_at = Set(now);
    gateway_identity_record_from_model(
        active
            .update(db)
            .await
            .context("failed to update identity bootstrap marker")?,
    )
}

pub async fn mark_gateway_auth_ready<C: ConnectionTrait>(
    db: &C,
    id: &GatewayId,
    auth_schema_version: i64,
    now: DateTimeWithTimeZone,
) -> Result<GatewayIdentityRecord> {
    let model = gateway_identity::Entity::find_by_id(id.to_string())
        .one(db)
        .await
        .context("failed to load Gateway identity for auth readiness")?
        .context("Gateway identity disappeared before auth readiness")?;
    if model.auth_schema_version == auth_schema_version {
        if model.auth_ready_at.is_none() {
            bail!("Gateway auth readiness marker is incomplete");
        }
        return gateway_identity_record_from_model(model);
    }
    if model.auth_schema_version != 0 {
        bail!(
            "unsupported Gateway auth schema version {}",
            model.auth_schema_version
        );
    }
    let mut active: gateway_identity::ActiveModel = model.into();
    active.auth_schema_version = Set(auth_schema_version);
    active.auth_ready_at = Set(Some(now));
    active.updated_at = Set(now);
    gateway_identity_record_from_model(
        active
            .update(db)
            .await
            .context("failed to mark Gateway auth ready")?,
    )
}

pub async fn list_gateway_principals<C: ConnectionTrait>(
    db: &C,
) -> Result<Vec<gateway_principal::Model>> {
    gateway_principal::Entity::find()
        .order_by_asc(gateway_principal::Column::Id)
        .all(db)
        .await
        .context("failed to list Gateway principals")
}

pub async fn load_superusers_for_gateway<C: ConnectionTrait>(
    db: &C,
    gateway_id: &GatewayId,
) -> Result<Vec<GatewayPrincipalRecord>> {
    gateway_principal::Entity::find()
        .filter(gateway_principal::Column::GatewayId.eq(gateway_id.to_string()))
        .filter(gateway_principal::Column::Kind.eq(principal_kind_to_db(PrincipalKind::Superuser)))
        .order_by_asc(gateway_principal::Column::Id)
        .all(db)
        .await
        .context("failed to load Gateway Superuser principals")?
        .into_iter()
        .map(gateway_principal_record_from_model)
        .collect()
}

pub async fn load_principal_by_id<C: ConnectionTrait>(
    db: &C,
    id: &PrincipalId,
) -> Result<Option<GatewayPrincipalRecord>> {
    gateway_principal::Entity::find_by_id(id.to_string())
        .one(db)
        .await
        .context("failed to load Gateway principal by id")?
        .map(gateway_principal_record_from_model)
        .transpose()
}

pub async fn transition_member_principal_status<C: ConnectionTrait>(
    db: &C,
    gateway_id: &GatewayId,
    principal_id: &PrincipalId,
    from: PrincipalStatus,
    to: PrincipalStatus,
    removed_at: Option<DateTimeWithTimeZone>,
    now: DateTimeWithTimeZone,
) -> Result<bool> {
    let result = gateway_principal::Entity::update_many()
        .col_expr(
            gateway_principal::Column::Status,
            Expr::value(principal_status_to_db(to)),
        )
        .col_expr(gateway_principal::Column::UpdatedAt, Expr::value(now))
        .col_expr(
            gateway_principal::Column::RemovedAt,
            Expr::value(removed_at),
        )
        .filter(gateway_principal::Column::Id.eq(principal_id.to_string()))
        .filter(gateway_principal::Column::GatewayId.eq(gateway_id.to_string()))
        .filter(gateway_principal::Column::Kind.eq(principal_kind_to_db(PrincipalKind::User)))
        .filter(gateway_principal::Column::Status.eq(principal_status_to_db(from)))
        .exec(db)
        .await
        .context("failed to transition Member principal status")?;
    Ok(result.rows_affected == 1)
}

pub async fn nickname_key_exists<C: ConnectionTrait>(
    db: &C,
    gateway_id: &GatewayId,
    nickname_key: &str,
) -> Result<bool> {
    Ok(gateway_principal::Entity::find()
        .filter(gateway_principal::Column::GatewayId.eq(gateway_id.to_string()))
        .filter(gateway_principal::Column::NicknameKey.eq(nickname_key))
        .one(db)
        .await
        .context("failed to check Gateway principal nickname availability")?
        .is_some())
}

pub async fn nickname_key_exists_for_other_principal<C: ConnectionTrait>(
    db: &C,
    gateway_id: &GatewayId,
    nickname_key: &str,
    principal_id: &PrincipalId,
) -> Result<bool> {
    Ok(gateway_principal::Entity::find()
        .filter(gateway_principal::Column::GatewayId.eq(gateway_id.to_string()))
        .filter(gateway_principal::Column::NicknameKey.eq(nickname_key))
        .filter(gateway_principal::Column::Id.ne(principal_id.to_string()))
        .one(db)
        .await
        .context("failed to check Gateway principal nickname availability")?
        .is_some())
}

#[allow(clippy::too_many_arguments)]
pub async fn update_principal_profile<C: ConnectionTrait>(
    db: &C,
    gateway_id: &GatewayId,
    principal_id: &PrincipalId,
    display_name: String,
    nickname: String,
    nickname_key: String,
    now: DateTimeWithTimeZone,
) -> Result<Option<GatewayPrincipalRecord>> {
    let result = gateway_principal::Entity::update_many()
        .col_expr(
            gateway_principal::Column::DisplayName,
            Expr::value(display_name),
        )
        .col_expr(gateway_principal::Column::Nickname, Expr::value(nickname))
        .col_expr(
            gateway_principal::Column::NicknameKey,
            Expr::value(nickname_key),
        )
        .col_expr(gateway_principal::Column::UpdatedAt, Expr::value(now))
        .filter(gateway_principal::Column::Id.eq(principal_id.to_string()))
        .filter(gateway_principal::Column::GatewayId.eq(gateway_id.to_string()))
        .exec(db)
        .await
        .context("failed to update Gateway principal profile")?;
    if result.rows_affected != 1 {
        return Ok(None);
    }
    load_principal_by_id(db, principal_id).await
}

pub async fn create_member_principal<C: ConnectionTrait>(
    db: &C,
    row: NewMemberPrincipalRow,
) -> Result<GatewayPrincipalRecord> {
    let model = gateway_principal::ActiveModel {
        id: Set(row.id.to_string()),
        gateway_id: Set(row.gateway_id.to_string()),
        kind: Set(principal_kind_to_db(PrincipalKind::User).to_owned()),
        role_key: Set(Some(row.role_key.to_string())),
        status: Set(principal_status_to_db(PrincipalStatus::Active).to_owned()),
        display_name: Set(row.display_name),
        nickname: Set(row.nickname),
        nickname_key: Set(row.nickname_key),
        created_at: Set(row.now),
        updated_at: Set(row.now),
        removed_at: Set(None),
        authorization_guard: Set(1),
        inbox_after_sequence: Set(None),
    }
    .insert(db)
    .await
    .context("failed to create Gateway Member principal")?;
    gateway_principal_record_from_model(model)
}

#[allow(clippy::too_many_arguments)]
pub async fn create_superuser<C: ConnectionTrait>(
    db: &C,
    id: &PrincipalId,
    gateway_id: &GatewayId,
    display_name: &str,
    nickname: &str,
    nickname_key: &str,
    now: DateTimeWithTimeZone,
    inbox_after_sequence: i64,
) -> Result<GatewayPrincipalRecord> {
    let model = gateway_principal::ActiveModel {
        id: Set(id.to_string()),
        gateway_id: Set(gateway_id.to_string()),
        kind: Set(principal_kind_to_db(PrincipalKind::Superuser).to_owned()),
        role_key: Set(None),
        status: Set(principal_status_to_db(PrincipalStatus::Active).to_owned()),
        display_name: Set(display_name.to_owned()),
        nickname: Set(nickname.to_owned()),
        nickname_key: Set(nickname_key.to_owned()),
        created_at: Set(now),
        updated_at: Set(now),
        removed_at: Set(None),
        authorization_guard: Set(1),
        inbox_after_sequence: Set(Some(inbox_after_sequence)),
    }
    .insert(db)
    .await
    .context("failed to create Gateway Superuser principal")?;
    gateway_principal_record_from_model(model)
}

pub async fn load_identity_invariant_rows<C: ConnectionTrait>(
    db: &C,
) -> Result<IdentityInvariantRows> {
    let gateways = list_gateway_identities(db).await?;
    let principals = list_gateway_principals(db).await?;
    let mut actor_references = Vec::new();

    let thread_actors = thread::Entity::find()
        .select_only()
        .columns([
            thread::Column::Id,
            thread::Column::CreatedByActorKind,
            thread::Column::CreatedByActorId,
        ])
        .order_by_asc(thread::Column::Id)
        .into_tuple::<(String, Option<String>, Option<String>)>()
        .all(db)
        .await
        .context("failed to list thread actor references")?;
    actor_references.extend(thread_actors.into_iter().map(
        |(resource_id, actor_kind, actor_id)| ActorReferenceRow {
            resource_kind: ActorResourceKind::Thread,
            resource_id,
            actor_kind,
            actor_id,
        },
    ));

    let turn_actors = turn::Entity::find()
        .select_only()
        .columns([
            turn::Column::Id,
            turn::Column::InitiatedByActorKind,
            turn::Column::InitiatedByActorId,
        ])
        .order_by_asc(turn::Column::Id)
        .into_tuple::<(String, Option<String>, Option<String>)>()
        .all(db)
        .await
        .context("failed to list turn actor references")?;
    actor_references.extend(
        turn_actors
            .into_iter()
            .map(|(resource_id, actor_kind, actor_id)| ActorReferenceRow {
                resource_kind: ActorResourceKind::Turn,
                resource_id,
                actor_kind,
                actor_id,
            }),
    );

    Ok(IdentityInvariantRows {
        gateways,
        principals,
        actor_references,
    })
}

pub fn gateway_identity_record_from_model(
    model: gateway_identity::Model,
) -> Result<GatewayIdentityRecord> {
    Ok(GatewayIdentityRecord {
        id: GatewayId::new(model.id).context("invalid persisted Gateway id")?,
        singleton_key: model.singleton_key,
        identity_bootstrap_version: model.identity_bootstrap_version,
        auth_schema_version: model.auth_schema_version,
        auth_ready_at: model.auth_ready_at,
        created_at: model.created_at,
        updated_at: model.updated_at,
    })
}

pub fn gateway_principal_record_from_model(
    model: gateway_principal::Model,
) -> Result<GatewayPrincipalRecord> {
    Ok(GatewayPrincipalRecord {
        id: PrincipalId::new(model.id).context("invalid persisted principal id")?,
        gateway_id: GatewayId::new(model.gateway_id)
            .context("invalid persisted principal Gateway id")?,
        kind: principal_kind_from_db(&model.kind)?,
        role_key: model.role_key,
        status: principal_status_from_db(&model.status)?,
        display_name: model.display_name,
        nickname: model.nickname,
        nickname_key: model.nickname_key,
        created_at: model.created_at,
        updated_at: model.updated_at,
        removed_at: model.removed_at,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        GATEWAY_SINGLETON_KEY, actor_ref_from_db, actor_ref_to_db, create_gateway_singleton,
        create_superuser, list_gateway_identities, load_gateway_singleton,
        load_identity_invariant_rows, load_principal_by_id, load_superusers_for_gateway,
        principal_kind_from_db, principal_kind_to_db, principal_status_from_db,
        principal_status_to_db,
    };
    use chrono::Utc;
    use pioneer_postgres_test::with_database;
    use pioneer_protocol::AgentExecutionId;
    use pioneer_protocol::{
        GatewayId, PersistedActorRef, PrincipalId, PrincipalKind, PrincipalStatus,
    };
    use sea_orm::TransactionTrait;

    fn gateway_id() -> GatewayId {
        GatewayId::new("G00000000000000000001").expect("valid Gateway id")
    }

    fn principal_id() -> PrincipalId {
        PrincipalId::new("P00000000000000000001").expect("valid principal id")
    }

    #[test]
    fn enum_and_actor_conversions_are_strict() {
        for kind in [PrincipalKind::Superuser, PrincipalKind::User] {
            assert_eq!(
                principal_kind_from_db(principal_kind_to_db(kind)).unwrap(),
                kind
            );
        }
        for status in [
            PrincipalStatus::Active,
            PrincipalStatus::Suspended,
            PrincipalStatus::Removed,
        ] {
            assert_eq!(
                principal_status_from_db(principal_status_to_db(status)).unwrap(),
                status
            );
        }
        assert!(principal_kind_from_db("owner").is_err());
        assert!(principal_status_from_db("enabled").is_err());

        let principal = PersistedActorRef::Principal(principal_id());
        let principal_pair = actor_ref_to_db(&principal);
        assert_eq!(
            actor_ref_from_db(principal_pair.0.as_deref(), principal_pair.1.as_deref()).unwrap(),
            Some(principal)
        );
        assert_eq!(
            actor_ref_from_db(Some("system"), None).unwrap(),
            Some(PersistedActorRef::System)
        );
        let execution = PersistedActorRef::AgentExecution(
            AgentExecutionId::new("E00000000000000000001").unwrap(),
        );
        let execution_pair = actor_ref_to_db(&execution);
        assert_eq!(
            actor_ref_from_db(execution_pair.0.as_deref(), execution_pair.1.as_deref()).unwrap(),
            Some(execution)
        );
        assert_eq!(actor_ref_from_db(None, None).unwrap(), None);
        assert!(actor_ref_from_db(None, Some(principal_id().as_str())).is_err());
        assert!(actor_ref_from_db(Some("system"), Some(principal_id().as_str())).is_err());
        assert!(actor_ref_from_db(Some("principal"), None).is_err());
        assert!(actor_ref_from_db(Some("owner"), None).is_err());
        assert!(actor_ref_from_db(Some("principal"), Some("superuser")).is_err());
    }

    #[tokio::test]
    async fn identity_primitives_share_a_caller_owned_transaction() {
        with_database(async |fixture| {
            let database = fixture.connection();
            let now = Utc::now().fixed_offset();

            let transaction = database.begin().await.expect("begin transaction");
            let gateway = create_gateway_singleton(&transaction, &gateway_id(), 0, now)
                .await
                .expect("create Gateway");
            let principal = create_superuser(
                &transaction,
                &principal_id(),
                &gateway.id,
                "Superuser",
                "superuser",
                "superuser",
                now,
                0,
            )
            .await
            .expect("create Superuser");

            assert_eq!(gateway.singleton_key, GATEWAY_SINGLETON_KEY);
            assert_eq!(principal.kind, PrincipalKind::Superuser);
            assert_eq!(principal.status, PrincipalStatus::Active);
            assert_eq!(
                load_superusers_for_gateway(&transaction, &gateway.id)
                    .await
                    .unwrap(),
                vec![principal.clone()]
            );
            assert_eq!(
                load_principal_by_id(&transaction, &principal.id)
                    .await
                    .unwrap(),
                Some(principal)
            );
            assert_eq!(
                load_identity_invariant_rows(&transaction)
                    .await
                    .unwrap()
                    .gateways
                    .len(),
                1
            );
            transaction.rollback().await.expect("roll back");

            assert!(load_gateway_singleton(database).await.unwrap().is_none());
            assert!(list_gateway_identities(database).await.unwrap().is_empty());

            let transaction = database.begin().await.expect("begin transaction");
            create_gateway_singleton(&transaction, &gateway_id(), 0, now)
                .await
                .expect("create Gateway");
            create_superuser(
                &transaction,
                &principal_id(),
                &gateway_id(),
                "Superuser",
                "superuser",
                "superuser",
                now,
                0,
            )
            .await
            .expect("create Superuser");
            transaction.commit().await.expect("commit");

            assert_eq!(
                load_gateway_singleton(database).await.unwrap().unwrap().id,
                gateway_id()
            );
            assert_eq!(
                load_superusers_for_gateway(database, &gateway_id())
                    .await
                    .unwrap()
                    .len(),
                1
            );
            Ok(())
        })
        .await
        .unwrap();
    }
}
