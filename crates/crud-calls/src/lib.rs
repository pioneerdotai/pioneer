use anyhow::{Context, Result};
use pioneer_calls::Call;
use pioneer_crud_access::auth_session::load_session_lease;
use pioneer_entity::{
    gateway_principal, thread, thread_call, thread_membership, workspace, workspace_membership,
};
use pioneer_postgres_pools::PostgresDatabase;
use pioneer_protocol::AuthSessionId;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, Condition, ConnectionTrait, EntityTrait, FromQueryResult,
    JoinType, QueryFilter, QueryOrder, QuerySelect, Select, Set, TransactionTrait,
    sea_query::{Expr, Query, extension::postgres::PgExpr},
};

pub struct CallRepository<'a>(pub &'a PostgresDatabase);

pub struct Scope<'a> {
    pub gateway: &'a str,
    pub workspace: &'a str,
    pub thread: &'a str,
}

impl<'a> From<&'a Call> for Scope<'a> {
    fn from(call: &'a Call) -> Self {
        Self {
            gateway: &call.gateway_id,
            workspace: &call.snapshot.workspace_id,
            thread: &call.snapshot.thread_id,
        }
    }
}

impl CallRepository<'_> {
    pub async fn contains(&self, scope: &Scope<'_>, actor: &str) -> Result<bool> {
        Ok(roster(self.0, scope, Some(actor), 1).await?.len() == 1)
    }

    pub async fn media_access(&self, call: &Call, principal: &str, now: i64) -> Result<bool> {
        let Some(session) = call.media_sessions.get(principal) else {
            return Ok(false);
        };
        let id = AuthSessionId::new(session.clone()).context("invalid stored call session")?;
        let Some(row) = load_session_lease(self.0, &id).await? else {
            return Ok(false);
        };
        let lease_valid = row.session_status == "active"
            && row.session_gateway_id == call.gateway_id
            && row.session_principal_id == principal
            && row.session_device_id == row.device_id
            && row
                .refresh_expires_at
                .is_some_and(|expiry| expiry.timestamp() > now)
            && row.device_status == "active"
            && row.device_gateway_id == call.gateway_id
            && row.device_principal_id == principal
            && row.principal_status == "active"
            && row.principal_gateway_id == call.gateway_id
            && row.principal_id == principal;
        Ok(lease_valid && self.contains(&Scope::from(call), principal).await?)
    }

    pub async fn classify(&self, scope: &Scope<'_>) -> Result<Vec<String>> {
        roster(self.0, scope, None, 3).await
    }

    pub async fn get(&self, call_id: &str) -> Result<Option<Call>> {
        decode(thread_call::Entity::find_by_id(call_id).one(self.0).await?)
    }

    pub async fn latest(&self, scope: &Scope<'_>, actor: &str) -> Result<Option<Call>> {
        decode(
            history_query(scope, actor)
                .order_by_desc(thread_call::Column::CreatedAt)
                .order_by_desc(thread_call::Column::Id)
                .one(self.0)
                .await?,
        )
    }

    pub async fn history(
        &self,
        scope: &Scope<'_>,
        before: Option<&Call>,
        actor: &str,
    ) -> Result<(Vec<Call>, Option<String>)> {
        const PAGE_SIZE: usize = 20;
        let mut query = history_query(scope, actor);
        if let Some(before) = before {
            query = query.filter(
                Condition::any()
                    .add(thread_call::Column::CreatedAt.lt(before.created_at))
                    .add(
                        Condition::all()
                            .add(thread_call::Column::CreatedAt.eq(before.created_at))
                            .add(thread_call::Column::Id.lt(before.snapshot.call_id.as_str())),
                    ),
            );
        }
        let mut rows = query
            .order_by_desc(thread_call::Column::CreatedAt)
            .order_by_desc(thread_call::Column::Id)
            .limit((PAGE_SIZE + 1) as u64)
            .all(self.0)
            .await?;
        let next = if rows.len() > PAGE_SIZE {
            rows.pop();
            rows.last().map(|row| row.id.clone())
        } else {
            None
        };
        let calls = rows
            .into_iter()
            .map(|row| serde_json::from_value(row.state).context("invalid stored call"))
            .collect::<Result<Vec<_>>>()?;
        Ok((calls, next))
    }

    pub async fn active(&self, thread_id: &str) -> Result<Option<Call>> {
        decode(
            thread_call::Entity::find()
                .filter(thread_call::Column::ActiveSlot.eq(thread_id))
                .one(self.0)
                .await?,
        )
    }

    pub async fn active_for_principal(&self, principal: &str) -> Result<Vec<Call>> {
        let participants =
            serde_json::json!({"snapshot": {"participants": [{"principal_id": principal}]}});
        thread_call::Entity::find()
            .filter(thread_call::Column::ActiveSlot.is_not_null())
            .filter(Expr::col(thread_call::Column::State).contains(participants))
            .all(self.0)
            .await?
            .into_iter()
            .map(|row| serde_json::from_value(row.state).context("invalid stored call"))
            .collect()
    }

    pub async fn active_page(&self, after: Option<&str>) -> Result<Vec<Call>> {
        let mut query =
            thread_call::Entity::find().filter(thread_call::Column::ActiveSlot.is_not_null());
        if let Some(after) = after {
            query = query.filter(thread_call::Column::Id.gt(after));
        }
        query
            .order_by_asc(thread_call::Column::Id)
            .limit(32)
            .all(self.0)
            .await?
            .into_iter()
            .map(|row| serde_json::from_value(row.state).context("invalid stored call"))
            .collect()
    }

    /// The sidecar operation occurs before this short transaction. We compare
    /// the exact state observed by the operation and recheck the caller's ACL.
    pub async fn save(
        &self,
        call: &Call,
        previous_revision: Option<i64>,
        actor: Option<&str>,
    ) -> Result<bool> {
        let scope = Scope::from(call);
        let state = serde_json::to_value(call)?;
        let session = self.0.begin().await?;
        let mut target_thread = thread::Entity::find_by_id(scope.thread);
        if call.is_active() || actor.is_some() {
            target_thread = target_thread.filter(thread::Column::WorkspaceId.eq(scope.workspace));
        }
        let thread_exists = target_thread
            .lock_exclusive()
            .one(&session)
            .await?
            .is_some();
        if !thread_exists
            || (actor.is_some() && roster(&session, &scope, actor, 1).await?.is_empty())
        {
            session.rollback().await?;
            return Ok(false);
        }
        let active_slot = call.is_active().then(|| scope.thread.to_owned());
        if let Some(previous_revision) = previous_revision {
            let row = thread_call::Entity::find_by_id(call.snapshot.call_id.as_str())
                .filter(thread_call::Column::ThreadId.eq(scope.thread))
                .lock_exclusive()
                .one(&session)
                .await?;
            let matches_version = row.as_ref().and_then(|row| {
                row.state
                    .pointer("/snapshot/revision")
                    .and_then(|value| value.as_i64())
            }) == Some(previous_revision);
            if !matches_version {
                session.rollback().await?;
                return Ok(false);
            }
            let mut row: thread_call::ActiveModel = row.expect("matching row").into();
            row.active_slot = Set(active_slot);
            row.state = Set(state);
            row.update(&session).await?;
        } else {
            let slot_exists = thread_call::Entity::find()
                .filter(thread_call::Column::ActiveSlot.eq(scope.thread))
                .one(&session)
                .await?
                .is_some();
            if slot_exists {
                session.rollback().await?;
                return Ok(false);
            }
            // Revalidate classification after obtaining the thread lock.
            let occupants = roster(&session, &scope, None, 3).await?;
            let invited = match occupants.as_slice() {
                [first, second] if first == &call.snapshot.initiator_id => Some(second),
                [first, second] if second == &call.snapshot.initiator_id => Some(first),
                [_, _, _] => None,
                _ => {
                    session.rollback().await?;
                    return Ok(false);
                }
            };
            if invited != call.snapshot.invitee_id.as_ref() {
                session.rollback().await?;
                return Ok(false);
            }
            thread_call::ActiveModel {
                id: Set(call.snapshot.call_id.clone()),
                thread_id: Set(scope.thread.to_owned()),
                active_slot: Set(active_slot),
                created_at: Set(call.created_at),
                state: Set(state),
            }
            .insert(&session)
            .await?;
        }
        session.commit().await?;
        Ok(true)
    }
}

fn history_query(scope: &Scope<'_>, actor: &str) -> Select<thread_call::Entity> {
    let state = || Expr::col(thread_call::Column::State);
    let visibility = Condition::any()
        .add(state().contains(serde_json::json!({"snapshot": {"invitee_id": null}})))
        .add(state().contains(serde_json::json!({"snapshot": {"invitee_id": actor}})))
        .add(state().contains(serde_json::json!({"snapshot": {"initiator_id": actor}})));
    thread_call::Entity::find()
        .filter(thread_call::Column::ThreadId.eq(scope.thread))
        .filter(state().contains(serde_json::json!({
            "gateway_id": scope.gateway,
            "snapshot": {"workspace_id": scope.workspace}
        })))
        .filter(visibility)
}

fn decode(row: Option<thread_call::Model>) -> Result<Option<Call>> {
    row.map(|row| serde_json::from_value(row.state).context("invalid stored call"))
        .transpose()
}

#[derive(FromQueryResult)]
struct ThreadAccess {
    access_class: String,
    created_by_actor_kind: Option<String>,
    created_by_actor_id: Option<String>,
}

async fn roster<C: ConnectionTrait>(
    db: &C,
    scope: &Scope<'_>,
    actor: Option<&str>,
    limit: u64,
) -> Result<Vec<String>> {
    let row = thread::Entity::find_by_id(scope.thread)
        .filter(thread::Column::WorkspaceId.eq(scope.workspace))
        .filter(thread::Column::SidebarVisibility.eq("visible"))
        .filter(thread::Column::AccessClass.is_in(["workspace", "private"]))
        .join(
            JoinType::InnerJoin,
            thread::Entity::belongs_to(workspace::Entity)
                .from(thread::Column::WorkspaceId)
                .to(workspace::Column::Id)
                .into(),
        )
        .filter(workspace::Column::IsActive.eq(true))
        .select_only()
        .columns([
            thread::Column::AccessClass,
            thread::Column::CreatedByActorKind,
            thread::Column::CreatedByActorId,
        ])
        .into_model::<ThreadAccess>()
        .one(db)
        .await?;
    let Some(row) = row else {
        return Ok(Vec::new());
    };

    let workspace_members = Query::select()
        .column(workspace_membership::Column::PrincipalId)
        .from(workspace_membership::Entity)
        .and_where(workspace_membership::Column::WorkspaceId.eq(scope.workspace))
        .to_owned();
    let mut user = Condition::all()
        .add(gateway_principal::Column::Kind.eq("user"))
        .add(gateway_principal::Column::RoleKey.is_not_null())
        .add(gateway_principal::Column::Id.in_subquery(workspace_members));
    if row.access_class == "private" {
        let private_members = Query::select()
            .column(thread_membership::Column::PrincipalId)
            .from(thread_membership::Entity)
            .and_where(thread_membership::Column::ThreadId.eq(scope.thread))
            .to_owned();
        user = user.add(gateway_principal::Column::Id.in_subquery(private_members));
    }
    // The Superuser's administrative override is not a chat membership. A
    // Superuser who created this conversation counts as a human participant.
    let mut eligible = Condition::any().add(user);
    if row.created_by_actor_kind.as_deref() == Some("principal")
        && let Some(creator) = row.created_by_actor_id
    {
        eligible = eligible.add(
            Condition::all()
                .add(gateway_principal::Column::Kind.eq("superuser"))
                .add(gateway_principal::Column::RoleKey.is_null())
                .add(gateway_principal::Column::Id.eq(creator)),
        );
    }
    let mut query = gateway_principal::Entity::find()
        .filter(gateway_principal::Column::GatewayId.eq(scope.gateway))
        .filter(gateway_principal::Column::Status.eq("active"))
        .filter(eligible);
    if let Some(actor) = actor {
        query = query.filter(gateway_principal::Column::Id.eq(actor));
    }
    Ok(query
        .select_only()
        .column(gateway_principal::Column::Id)
        .order_by_asc(gateway_principal::Column::Id)
        .limit(limit)
        .into_tuple::<String>()
        .all(db)
        .await?)
}
