use anyhow::{Context, Result, bail};
use pioneer_entity::{gateway_principal, invitation, invitation_workspace_grant, workspace};
use pioneer_protocol::{
    AuthSessionId, DeviceId, GatewayId, InvitationId, InvitationRevokeReason, InvitationStatus,
    PrincipalId, RoleKey, WorkspaceId,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::Expr;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, Condition, ConnectionTrait, DatabaseTransaction, EntityTrait,
    PaginatorTrait, QueryFilter, QueryOrder, QuerySelect, Set,
};

#[derive(Clone)]
pub struct NewInvitationRow {
    pub invitation_id: InvitationId,
    pub gateway_id: GatewayId,
    pub created_by_principal_id: PrincipalId,
    pub created_by_session_id: AuthSessionId,
    pub target_role_key: RoleKey,
    pub token_hash: [u8; 32],
    pub expires_at: DateTimeWithTimeZone,
    pub now: DateTimeWithTimeZone,
}

impl std::fmt::Debug for NewInvitationRow {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NewInvitationRow")
            .field("invitation_id", &self.invitation_id)
            .field("gateway_id", &self.gateway_id)
            .field("created_by_principal_id", &self.created_by_principal_id)
            .field("created_by_session_id", &self.created_by_session_id)
            .field("target_role_key", &self.target_role_key)
            .field("token_hash", &"[redacted]")
            .field("expires_at", &self.expires_at)
            .field("now", &self.now)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvitationListCursor {
    pub created_at: DateTimeWithTimeZone,
    pub invitation_id: InvitationId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvitationListPage {
    pub invitations: Vec<invitation::Model>,
    pub next_cursor: Option<InvitationListCursor>,
    pub materialized_expirations: Vec<invitation::Model>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvitationWithGrants {
    pub invitation: invitation::Model,
    pub grants: Vec<invitation_workspace_grant::Model>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvitationProjectionRows {
    pub invitation: invitation::Model,
    pub inviter: gateway_principal::Model,
    pub workspaces: Vec<InvitationWorkspaceProjectionRow>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvitationWorkspaceProjectionRow {
    pub workspace_id: String,
    pub name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvitationTransitionOutcome {
    Applied(invitation::Model),
    Expired(invitation::Model),
    NotApplied(invitation::Model),
    NotFound,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PendingInvitationLookup {
    Available(InvitationWithGrants),
    Expired(invitation::Model),
    Unavailable,
}

pub const fn invitation_status_to_db(status: InvitationStatus) -> &'static str {
    match status {
        InvitationStatus::Pending => "pending",
        InvitationStatus::Accepted => "accepted",
        InvitationStatus::Revoked => "revoked",
        InvitationStatus::Expired => "expired",
    }
}

pub fn invitation_status_from_db(value: &str) -> Result<InvitationStatus> {
    match value {
        "pending" => Ok(InvitationStatus::Pending),
        "accepted" => Ok(InvitationStatus::Accepted),
        "revoked" => Ok(InvitationStatus::Revoked),
        "expired" => Ok(InvitationStatus::Expired),
        unknown => bail!("unknown persisted invitation status `{unknown}`"),
    }
}

pub const fn invitation_revoke_reason_to_db(reason: InvitationRevokeReason) -> &'static str {
    match reason {
        InvitationRevokeReason::InviterRevoked => "inviter_revoked",
        InvitationRevokeReason::InviterUnavailable => "inviter_unavailable",
        InvitationRevokeReason::GrantAuthorityLost => "grant_authority_lost",
        InvitationRevokeReason::WorkspaceUnavailable => "workspace_unavailable",
    }
}

pub fn invitation_revoke_reason_from_db(value: &str) -> Result<InvitationRevokeReason> {
    match value {
        "inviter_revoked" => Ok(InvitationRevokeReason::InviterRevoked),
        "inviter_unavailable" => Ok(InvitationRevokeReason::InviterUnavailable),
        "grant_authority_lost" => Ok(InvitationRevokeReason::GrantAuthorityLost),
        "workspace_unavailable" => Ok(InvitationRevokeReason::WorkspaceUnavailable),
        unknown => bail!("unknown persisted invitation revoke reason `{unknown}`"),
    }
}

pub async fn insert_invitation(
    transaction: &DatabaseTransaction,
    row: NewInvitationRow,
) -> Result<invitation::Model> {
    invitation::ActiveModel {
        id: Set(row.invitation_id.to_string()),
        gateway_id: Set(row.gateway_id.to_string()),
        created_by_principal_id: Set(row.created_by_principal_id.to_string()),
        created_by_session_id: Set(row.created_by_session_id.to_string()),
        target_role_key: Set(row.target_role_key.to_string()),
        status: Set(invitation_status_to_db(InvitationStatus::Pending).to_owned()),
        token_hash: Set(Some(row.token_hash.to_vec())),
        token_format_version: Set(1),
        expires_at: Set(row.expires_at),
        accepted_at: Set(None),
        revoked_at: Set(None),
        expired_at: Set(None),
        accepted_principal_id: Set(None),
        accepted_device_id: Set(None),
        accepted_session_id: Set(None),
        revoke_reason: Set(None),
        created_at: Set(row.now),
        updated_at: Set(row.now),
    }
    .insert(transaction)
    .await
    .context("failed to insert invitation")
}

pub async fn insert_invitation_grants(
    transaction: &DatabaseTransaction,
    invitation_id: &InvitationId,
    workspace_ids: &[WorkspaceId],
    now: DateTimeWithTimeZone,
) -> Result<Vec<invitation_workspace_grant::Model>> {
    let mut canonical = workspace_ids.to_vec();
    canonical.sort();
    canonical.dedup();
    if canonical.is_empty() || canonical.len() != workspace_ids.len() {
        bail!("invitation workspace grants must be non-empty and unique");
    }
    let models = canonical
        .into_iter()
        .map(|workspace_id| invitation_workspace_grant::ActiveModel {
            invitation_id: Set(invitation_id.to_string()),
            workspace_id: Set(workspace_id.to_string()),
            created_at: Set(now),
        })
        .collect::<Vec<_>>();
    invitation_workspace_grant::Entity::insert_many(models)
        .exec(transaction)
        .await
        .context("failed to insert immutable invitation workspace grants")?;
    load_invitation_grants(transaction, invitation_id).await
}

pub async fn load_invitation<C: ConnectionTrait>(
    db: &C,
    invitation_id: &InvitationId,
) -> Result<Option<invitation::Model>> {
    invitation::Entity::find_by_id(invitation_id.to_string())
        .one(db)
        .await
        .context("failed to load invitation by stable id")
}

pub async fn load_invitation_with_grants<C: ConnectionTrait>(
    db: &C,
    invitation_id: &InvitationId,
) -> Result<Option<InvitationWithGrants>> {
    let Some(invitation) = load_invitation(db, invitation_id).await? else {
        return Ok(None);
    };
    let grants = load_invitation_grants(db, invitation_id).await?;
    Ok(Some(InvitationWithGrants { invitation, grants }))
}

pub async fn load_invitation_grants<C: ConnectionTrait>(
    db: &C,
    invitation_id: &InvitationId,
) -> Result<Vec<invitation_workspace_grant::Model>> {
    invitation_workspace_grant::Entity::find()
        .filter(invitation_workspace_grant::Column::InvitationId.eq(invitation_id.to_string()))
        .order_by_asc(invitation_workspace_grant::Column::WorkspaceId)
        .all(db)
        .await
        .context("failed to load immutable invitation workspace grants")
}

pub async fn load_invitation_projection<C: ConnectionTrait>(
    db: &C,
    invitation: invitation::Model,
) -> Result<InvitationProjectionRows> {
    let invitation_id =
        InvitationId::new(invitation.id.clone()).context("persisted invitation id is invalid")?;
    let inviter = gateway_principal::Entity::find_by_id(invitation.created_by_principal_id.clone())
        .one(db)
        .await
        .context("failed to load invitation creator projection")?
        .context("invitation creator projection is missing")?;
    let grants = load_invitation_grants(db, &invitation_id).await?;
    let workspace_ids = grants
        .iter()
        .map(|grant| grant.workspace_id.clone())
        .collect::<Vec<_>>();
    let workspaces_by_id = workspace::Entity::find()
        .filter(workspace::Column::Id.is_in(workspace_ids))
        .all(db)
        .await
        .context("failed to load invitation workspace projection")?
        .into_iter()
        .map(|workspace| (workspace.id.clone(), workspace))
        .collect::<std::collections::HashMap<_, _>>();
    let workspaces = grants
        .into_iter()
        .map(|grant| InvitationWorkspaceProjectionRow {
            name: workspaces_by_id
                .get(grant.workspace_id.as_str())
                .map(|workspace| workspace.name.clone()),
            workspace_id: grant.workspace_id,
        })
        .collect();
    Ok(InvitationProjectionRows {
        invitation,
        inviter,
        workspaces,
    })
}

pub async fn load_effective_pending_invitation_by_token_hash(
    transaction: &DatabaseTransaction,
    token_hash: &[u8; 32],
    now: DateTimeWithTimeZone,
) -> Result<PendingInvitationLookup> {
    let candidate = invitation::Entity::find()
        .filter(invitation::Column::Status.eq(invitation_status_to_db(InvitationStatus::Pending)))
        .filter(invitation::Column::TokenHash.eq(token_hash.to_vec()))
        .one(transaction)
        .await
        .context("failed to load pending invitation credential candidate")?;
    let Some(candidate) = candidate else {
        return Ok(PendingInvitationLookup::Unavailable);
    };
    let invitation_id =
        InvitationId::new(candidate.id.clone()).context("persisted invitation id is invalid")?;
    if candidate.expires_at <= now {
        return match transition_pending_to_expired(transaction, &invitation_id, now).await? {
            InvitationTransitionOutcome::Applied(expired) => {
                Ok(PendingInvitationLookup::Expired(expired))
            }
            InvitationTransitionOutcome::NotApplied(_)
            | InvitationTransitionOutcome::Expired(_)
            | InvitationTransitionOutcome::NotFound => Ok(PendingInvitationLookup::Unavailable),
        };
    }
    let grants = load_invitation_grants(transaction, &invitation_id).await?;
    Ok(PendingInvitationLookup::Available(InvitationWithGrants {
        invitation: candidate,
        grants,
    }))
}

pub fn effective_invitation_status(
    model: &invitation::Model,
    now: DateTimeWithTimeZone,
) -> Result<InvitationStatus> {
    let persisted = invitation_status_from_db(model.status.as_str())?;
    if persisted == InvitationStatus::Pending && model.expires_at <= now {
        Ok(InvitationStatus::Expired)
    } else {
        Ok(persisted)
    }
}

pub async fn list_pending_invitations_for_creator(
    transaction: &DatabaseTransaction,
    gateway_id: &GatewayId,
    creator_principal_id: &PrincipalId,
    now: DateTimeWithTimeZone,
    cursor: Option<&InvitationListCursor>,
    limit: u64,
) -> Result<InvitationListPage> {
    let materialized_expirations = materialize_due_pending_invitations_for_creator(
        transaction,
        gateway_id,
        creator_principal_id,
        now,
        limit,
    )
    .await?;
    let query = invitation::Entity::find()
        .filter(invitation::Column::GatewayId.eq(gateway_id.to_string()))
        .filter(invitation::Column::CreatedByPrincipalId.eq(creator_principal_id.to_string()))
        .filter(invitation::Column::Status.eq(invitation_status_to_db(InvitationStatus::Pending)))
        .filter(invitation::Column::ExpiresAt.gt(now));
    let mut page = execute_invitation_page(query, cursor, limit, now, transaction).await?;
    page.materialized_expirations
        .extend(materialized_expirations);
    Ok(page)
}

async fn materialize_due_pending_invitations_for_creator(
    transaction: &DatabaseTransaction,
    gateway_id: &GatewayId,
    creator_principal_id: &PrincipalId,
    now: DateTimeWithTimeZone,
    limit: u64,
) -> Result<Vec<invitation::Model>> {
    materialize_due_pending_invitations(
        transaction,
        gateway_id,
        Some(creator_principal_id),
        now,
        limit,
    )
    .await
}

async fn materialize_due_pending_invitations(
    transaction: &DatabaseTransaction,
    gateway_id: &GatewayId,
    creator_principal_id: Option<&PrincipalId>,
    now: DateTimeWithTimeZone,
    limit: u64,
) -> Result<Vec<invitation::Model>> {
    if limit == 0 {
        bail!("invitation page limit must be positive");
    }
    let mut query = invitation::Entity::find()
        .filter(invitation::Column::GatewayId.eq(gateway_id.to_string()))
        .filter(invitation::Column::Status.eq(invitation_status_to_db(InvitationStatus::Pending)))
        .filter(invitation::Column::ExpiresAt.lte(now));
    if let Some(creator_principal_id) = creator_principal_id {
        query = query
            .filter(invitation::Column::CreatedByPrincipalId.eq(creator_principal_id.to_string()));
    }
    let mut due = query
        .order_by_desc(invitation::Column::CreatedAt)
        .order_by_desc(invitation::Column::Id)
        .limit(limit)
        .all(transaction)
        .await
        .context("failed to list due creator invitations")?;
    if due.is_empty() {
        return Ok(due);
    }

    let due_ids = due.iter().map(|model| model.id.clone()).collect::<Vec<_>>();
    let updated = invitation::Entity::update_many()
        .col_expr(
            invitation::Column::Status,
            Expr::value(invitation_status_to_db(InvitationStatus::Expired)),
        )
        .col_expr(
            invitation::Column::TokenHash,
            Expr::value(Option::<Vec<u8>>::None),
        )
        .col_expr(invitation::Column::ExpiredAt, Expr::value(Some(now)))
        .col_expr(invitation::Column::UpdatedAt, Expr::value(now))
        .filter(invitation::Column::Id.is_in(due_ids))
        .filter(invitation::Column::Status.eq(invitation_status_to_db(InvitationStatus::Pending)))
        .filter(invitation::Column::ExpiresAt.lte(now))
        .exec(transaction)
        .await
        .context("failed to materialize due creator invitations")?;
    if updated.rows_affected != due.len() as u64 {
        bail!(
            "materializing due creator invitations affected {} rows instead of {}",
            updated.rows_affected,
            due.len()
        );
    }
    for model in &mut due {
        model.status = invitation_status_to_db(InvitationStatus::Expired).to_owned();
        model.token_hash = None;
        model.expired_at = Some(now);
        model.updated_at = now;
    }
    Ok(due)
}

/// Counts only currently usable pending invitations for one creator.
///
/// This query is intentionally executed inside the invitation-create
/// transaction so concurrent creates cannot bypass the per-creator bound.
pub async fn count_live_pending_invitations_for_creator(
    transaction: &DatabaseTransaction,
    gateway_id: &GatewayId,
    creator_principal_id: &PrincipalId,
    now: DateTimeWithTimeZone,
) -> Result<u64> {
    invitation::Entity::find()
        .filter(invitation::Column::GatewayId.eq(gateway_id.to_string()))
        .filter(invitation::Column::CreatedByPrincipalId.eq(creator_principal_id.to_string()))
        .filter(invitation::Column::Status.eq(invitation_status_to_db(InvitationStatus::Pending)))
        .filter(invitation::Column::ExpiresAt.gt(now))
        .count(transaction)
        .await
        .context("failed to count live pending invitations")
}

pub async fn list_invitations_for_superuser(
    transaction: &DatabaseTransaction,
    gateway_id: &GatewayId,
    status: Option<InvitationStatus>,
    creator_principal_id: Option<&PrincipalId>,
    now: DateTimeWithTimeZone,
    cursor: Option<&InvitationListCursor>,
    limit: u64,
) -> Result<InvitationListPage> {
    let materialized_expirations = materialize_due_pending_invitations(
        transaction,
        gateway_id,
        creator_principal_id,
        now,
        limit,
    )
    .await?;
    let mut query =
        invitation::Entity::find().filter(invitation::Column::GatewayId.eq(gateway_id.to_string()));
    if let Some(status) = status {
        query = match status {
            InvitationStatus::Pending => query
                .filter(invitation::Column::Status.eq(invitation_status_to_db(status)))
                .filter(invitation::Column::ExpiresAt.gt(now)),
            InvitationStatus::Expired => query.filter(
                Condition::any()
                    .add(invitation::Column::Status.eq(invitation_status_to_db(status)))
                    .add(
                        Condition::all()
                            .add(
                                invitation::Column::Status
                                    .eq(invitation_status_to_db(InvitationStatus::Pending)),
                            )
                            .add(invitation::Column::ExpiresAt.lte(now)),
                    ),
            ),
            InvitationStatus::Accepted | InvitationStatus::Revoked => {
                query.filter(invitation::Column::Status.eq(invitation_status_to_db(status)))
            }
        };
    }
    if let Some(creator_principal_id) = creator_principal_id {
        query = query
            .filter(invitation::Column::CreatedByPrincipalId.eq(creator_principal_id.to_string()));
    }
    let mut page = execute_invitation_page(query, cursor, limit, now, transaction).await?;
    page.materialized_expirations
        .extend(materialized_expirations);
    Ok(page)
}

async fn execute_invitation_page(
    mut query: sea_orm::Select<invitation::Entity>,
    cursor: Option<&InvitationListCursor>,
    limit: u64,
    now: DateTimeWithTimeZone,
    db: &DatabaseTransaction,
) -> Result<InvitationListPage> {
    if limit == 0 {
        bail!("invitation page limit must be positive");
    }
    if let Some(cursor) = cursor {
        query = query.filter(
            Condition::any()
                .add(invitation::Column::CreatedAt.lt(cursor.created_at))
                .add(
                    Condition::all()
                        .add(invitation::Column::CreatedAt.eq(cursor.created_at))
                        .add(invitation::Column::Id.lt(cursor.invitation_id.to_string())),
                ),
        );
    }
    let fetch_limit = limit.saturating_add(1).min(i64::MAX as u64);
    let mut invitations = query
        .order_by_desc(invitation::Column::CreatedAt)
        .order_by_desc(invitation::Column::Id)
        .limit(fetch_limit)
        .all(db)
        .await
        .context("failed to list scoped invitations")?;
    let has_more = invitations.len() > usize::try_from(limit).unwrap_or(usize::MAX);
    if has_more {
        invitations.pop();
    }
    let due_ids = invitations
        .iter()
        .filter(|model| model.status == invitation_status_to_db(InvitationStatus::Pending))
        .filter(|model| model.expires_at <= now)
        .map(|model| model.id.clone())
        .collect::<Vec<_>>();
    let mut materialized_expirations = Vec::with_capacity(due_ids.len());
    if !due_ids.is_empty() {
        let updated = invitation::Entity::update_many()
            .col_expr(
                invitation::Column::Status,
                Expr::value(invitation_status_to_db(InvitationStatus::Expired)),
            )
            .col_expr(
                invitation::Column::TokenHash,
                Expr::value(Option::<Vec<u8>>::None),
            )
            .col_expr(invitation::Column::ExpiredAt, Expr::value(Some(now)))
            .col_expr(invitation::Column::UpdatedAt, Expr::value(now))
            .filter(invitation::Column::Id.is_in(due_ids.clone()))
            .filter(
                invitation::Column::Status.eq(invitation_status_to_db(InvitationStatus::Pending)),
            )
            .filter(invitation::Column::ExpiresAt.lte(now))
            .exec(db)
            .await
            .context("failed to materialize returned invitation expirations")?;
        if updated.rows_affected != due_ids.len() as u64 {
            bail!(
                "materializing returned invitation expirations affected {} rows instead of {}",
                updated.rows_affected,
                due_ids.len()
            );
        }
        for model in &mut invitations {
            if due_ids.contains(&model.id) {
                model.status = invitation_status_to_db(InvitationStatus::Expired).to_owned();
                model.token_hash = None;
                model.expired_at = Some(now);
                model.updated_at = now;
                materialized_expirations.push(model.clone());
            }
        }
    }
    let next_cursor = has_more
        .then(|| invitations.last())
        .flatten()
        .map(|last| -> Result<InvitationListCursor> {
            Ok(InvitationListCursor {
                created_at: last.created_at,
                invitation_id: InvitationId::new(last.id.clone())?,
            })
        })
        .transpose()
        .context("persisted invitation cursor id is invalid")?;
    Ok(InvitationListPage {
        invitations,
        next_cursor,
        materialized_expirations,
    })
}

pub async fn transition_pending_to_accepted(
    transaction: &DatabaseTransaction,
    invitation_id: &InvitationId,
    expected_token_hash: &[u8; 32],
    accepted_principal_id: &PrincipalId,
    accepted_device_id: &DeviceId,
    accepted_session_id: &AuthSessionId,
    now: DateTimeWithTimeZone,
) -> Result<InvitationTransitionOutcome> {
    if let InvitationTransitionOutcome::Applied(expired) =
        transition_pending_to_expired(transaction, invitation_id, now).await?
    {
        return Ok(InvitationTransitionOutcome::Expired(expired));
    }
    let result = invitation::Entity::update_many()
        .col_expr(invitation::Column::Status, Expr::value("accepted"))
        .col_expr(
            invitation::Column::TokenHash,
            Expr::value(Option::<Vec<u8>>::None),
        )
        .col_expr(invitation::Column::AcceptedAt, Expr::value(Some(now)))
        .col_expr(
            invitation::Column::AcceptedPrincipalId,
            Expr::value(Some(accepted_principal_id.to_string())),
        )
        .col_expr(
            invitation::Column::AcceptedDeviceId,
            Expr::value(Some(accepted_device_id.to_string())),
        )
        .col_expr(
            invitation::Column::AcceptedSessionId,
            Expr::value(Some(accepted_session_id.to_string())),
        )
        .col_expr(invitation::Column::UpdatedAt, Expr::value(now))
        .filter(invitation::Column::Id.eq(invitation_id.to_string()))
        .filter(invitation::Column::Status.eq("pending"))
        .filter(invitation::Column::ExpiresAt.gt(now))
        .filter(invitation::Column::TokenHash.eq(expected_token_hash.to_vec()))
        .exec(transaction)
        .await
        .context("failed to conditionally accept invitation")?;
    transition_outcome(transaction, invitation_id, result.rows_affected).await
}

pub async fn transition_pending_to_revoked(
    transaction: &DatabaseTransaction,
    invitation_id: &InvitationId,
    reason: InvitationRevokeReason,
    now: DateTimeWithTimeZone,
) -> Result<InvitationTransitionOutcome> {
    if let InvitationTransitionOutcome::Applied(expired) =
        transition_pending_to_expired(transaction, invitation_id, now).await?
    {
        return Ok(InvitationTransitionOutcome::Expired(expired));
    }
    let result = invitation::Entity::update_many()
        .col_expr(invitation::Column::Status, Expr::value("revoked"))
        .col_expr(
            invitation::Column::TokenHash,
            Expr::value(Option::<Vec<u8>>::None),
        )
        .col_expr(invitation::Column::RevokedAt, Expr::value(Some(now)))
        .col_expr(
            invitation::Column::RevokeReason,
            Expr::value(Some(invitation_revoke_reason_to_db(reason).to_owned())),
        )
        .col_expr(invitation::Column::UpdatedAt, Expr::value(now))
        .filter(invitation::Column::Id.eq(invitation_id.to_string()))
        .filter(invitation::Column::Status.eq("pending"))
        .filter(invitation::Column::ExpiresAt.gt(now))
        .exec(transaction)
        .await
        .context("failed to conditionally revoke invitation")?;
    transition_outcome(transaction, invitation_id, result.rows_affected).await
}

pub async fn revoke_pending_invitations_for_creator(
    transaction: &DatabaseTransaction,
    gateway_id: &GatewayId,
    creator_principal_id: &PrincipalId,
    reason: InvitationRevokeReason,
    now: DateTimeWithTimeZone,
) -> Result<Vec<InvitationId>> {
    let pending_ids = invitation::Entity::find()
        .select_only()
        .column(invitation::Column::Id)
        .filter(invitation::Column::GatewayId.eq(gateway_id.to_string()))
        .filter(invitation::Column::CreatedByPrincipalId.eq(creator_principal_id.to_string()))
        .filter(invitation::Column::Status.eq("pending"))
        .filter(invitation::Column::ExpiresAt.gt(now))
        .order_by_asc(invitation::Column::Id)
        .into_tuple::<String>()
        .all(transaction)
        .await
        .context("failed to load pending invitations for removed Member")?;
    let mut changed = Vec::with_capacity(pending_ids.len());
    for id in pending_ids {
        let id = InvitationId::new(id).context("persisted invitation id is invalid")?;
        if matches!(
            transition_pending_to_revoked(transaction, &id, reason, now).await?,
            InvitationTransitionOutcome::Applied(_) | InvitationTransitionOutcome::Expired(_)
        ) {
            changed.push(id);
        }
    }
    Ok(changed)
}

pub async fn transition_pending_to_expired(
    transaction: &DatabaseTransaction,
    invitation_id: &InvitationId,
    now: DateTimeWithTimeZone,
) -> Result<InvitationTransitionOutcome> {
    let result = invitation::Entity::update_many()
        .col_expr(invitation::Column::Status, Expr::value("expired"))
        .col_expr(
            invitation::Column::TokenHash,
            Expr::value(Option::<Vec<u8>>::None),
        )
        .col_expr(invitation::Column::ExpiredAt, Expr::value(Some(now)))
        .col_expr(invitation::Column::UpdatedAt, Expr::value(now))
        .filter(invitation::Column::Id.eq(invitation_id.to_string()))
        .filter(invitation::Column::Status.eq("pending"))
        .filter(invitation::Column::ExpiresAt.lte(now))
        .exec(transaction)
        .await
        .context("failed to conditionally expire invitation")?;
    transition_outcome(transaction, invitation_id, result.rows_affected).await
}

async fn transition_outcome(
    transaction: &DatabaseTransaction,
    invitation_id: &InvitationId,
    rows_affected: u64,
) -> Result<InvitationTransitionOutcome> {
    let current = load_invitation(transaction, invitation_id).await?;
    match (rows_affected, current) {
        (1, Some(current)) => Ok(InvitationTransitionOutcome::Applied(current)),
        (0, Some(current)) => Ok(InvitationTransitionOutcome::NotApplied(current)),
        (0, None) => Ok(InvitationTransitionOutcome::NotFound),
        (unexpected, _) => bail!("invitation transition affected {unexpected} rows"),
    }
}

#[cfg(test)]
mod tests {
    use migration::{Migrator, MigratorTrait};
    use sea_orm::{ConnectionTrait, Database, DatabaseConnection, TransactionTrait};

    use super::*;

    fn timestamp(offset: i64) -> DateTimeWithTimeZone {
        crate::util::unix_to_datetime(1_800_000_000 + offset)
    }

    fn invitation_id(suffix: u8) -> InvitationId {
        InvitationId::new(format!("I{suffix:020}")).unwrap()
    }

    fn principal_id(suffix: u8) -> PrincipalId {
        PrincipalId::new(format!("P{suffix:020}")).unwrap()
    }

    fn session_id(suffix: u8) -> AuthSessionId {
        AuthSessionId::new(format!("S{suffix:020}")).unwrap()
    }

    async fn fixture() -> DatabaseConnection {
        let database = Database::connect("sqlite::memory:").await.unwrap();
        Migrator::up(&database, None).await.unwrap();
        database
            .execute_unprepared(
                "INSERT INTO gateway_identity(\
                    id,singleton_key,identity_bootstrap_version,auth_schema_version,\
                    auth_ready_at,created_at,updated_at\
                 ) VALUES(\
                    'G00000000000000000001',1,1,1,CURRENT_TIMESTAMP,\
                    CURRENT_TIMESTAMP,CURRENT_TIMESTAMP\
                 );\
                 INSERT INTO gateway_principal(\
                    id,gateway_id,kind,role_key,status,display_name,nickname,nickname_key,\
                    created_at,updated_at,removed_at,authorization_guard\
                 ) VALUES\
                    ('P00000000000000000001','G00000000000000000001','superuser',NULL,\
                     'active','Superuser','superuser','superuser',CURRENT_TIMESTAMP,\
                     CURRENT_TIMESTAMP,NULL,1),\
                    ('P00000000000000000002','G00000000000000000001','user','member',\
                     'active','Member','member','member',CURRENT_TIMESTAMP,\
                     CURRENT_TIMESTAMP,NULL,1);\
                 INSERT INTO device(\
                    id,gateway_id,principal_id,status,created_at,updated_at\
                 ) VALUES\
                    ('D00000000000000000001','G00000000000000000001',\
                     'P00000000000000000001','pending',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),\
                    ('D00000000000000000002','G00000000000000000001',\
                     'P00000000000000000002','pending',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP);\
                 INSERT INTO auth_session(\
                    id,gateway_id,principal_id,device_id,token_family_id,\
                    activation_token_hash,activation_locator_hash,activation_failed_attempts,\
                    activation_expires_at,status,refresh_generation,created_at,updated_at\
                 ) VALUES\
                    ('S00000000000000000001','G00000000000000000001',\
                     'P00000000000000000001','D00000000000000000001',\
                     'F00000000000000000001',randomblob(32),randomblob(32),0,\
                     datetime('now','+10 minutes'),'pending',0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),\
                    ('S00000000000000000002','G00000000000000000001',\
                     'P00000000000000000002','D00000000000000000002',\
                     'F00000000000000000002',randomblob(32),randomblob(32),0,\
                     datetime('now','+10 minutes'),'pending',0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
            )
            .await
            .unwrap();
        database
    }

    async fn create_invitation(
        database: &DatabaseConnection,
        id: u8,
        creator: u8,
        hash_byte: u8,
        created_offset: i64,
        expires_offset: i64,
        grants: &[u8],
    ) {
        let transaction = database.begin().await.unwrap();
        insert_invitation(
            &transaction,
            NewInvitationRow {
                invitation_id: invitation_id(id),
                gateway_id: GatewayId::new("G00000000000000000001").unwrap(),
                created_by_principal_id: principal_id(creator),
                created_by_session_id: session_id(creator),
                target_role_key: RoleKey::member(),
                token_hash: [hash_byte; 32],
                expires_at: timestamp(expires_offset),
                now: timestamp(created_offset),
            },
        )
        .await
        .unwrap();
        let workspace_ids = grants
            .iter()
            .map(|suffix| WorkspaceId::new(format!("W{suffix:020}")).unwrap())
            .collect::<Vec<_>>();
        insert_invitation_grants(
            &transaction,
            &invitation_id(id),
            workspace_ids.as_slice(),
            timestamp(created_offset),
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();
    }

    #[tokio::test]
    async fn grants_are_insert_only_canonical_and_invitation_debug_is_redacted() {
        let database = fixture().await;
        create_invitation(&database, 1, 1, 222, 1, 100, &[2, 1]).await;
        let loaded = load_invitation_with_grants(&database, &invitation_id(1))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            loaded
                .grants
                .iter()
                .map(|grant| grant.workspace_id.as_str())
                .collect::<Vec<_>>(),
            ["W00000000000000000001", "W00000000000000000002"]
        );
        let rendered = format!(
            "{:?}",
            NewInvitationRow {
                invitation_id: invitation_id(9),
                gateway_id: GatewayId::new("G00000000000000000001").unwrap(),
                created_by_principal_id: principal_id(1),
                created_by_session_id: session_id(1),
                target_role_key: RoleKey::member(),
                token_hash: [222; 32],
                expires_at: timestamp(100),
                now: timestamp(1),
            }
        );
        assert!(rendered.contains("[redacted]"));
        assert!(!rendered.contains("222"));

        let transaction = database.begin().await.unwrap();
        assert!(
            insert_invitation_grants(
                &transaction,
                &invitation_id(1),
                &[
                    WorkspaceId::new("W00000000000000000001").unwrap(),
                    WorkspaceId::new("W00000000000000000001").unwrap(),
                ],
                timestamp(2),
            )
            .await
            .is_err()
        );
        transaction.rollback().await.unwrap();
    }

    #[tokio::test]
    async fn historical_projection_preserves_soft_workspace_reference_after_deletion() {
        let database = fixture().await;
        database
            .execute_unprepared(
                "INSERT INTO workspace(id,name,is_active,is_current)\
                 VALUES('W00000000000000000001','Historical workspace',1,0)",
            )
            .await
            .unwrap();
        create_invitation(&database, 1, 1, 1, 1, 200, &[1]).await;
        let invitation = load_invitation(&database, &invitation_id(1))
            .await
            .unwrap()
            .unwrap();
        let current = load_invitation_projection(&database, invitation.clone())
            .await
            .unwrap();
        assert_eq!(
            current.workspaces,
            vec![InvitationWorkspaceProjectionRow {
                workspace_id: "W00000000000000000001".to_owned(),
                name: Some("Historical workspace".to_owned()),
            }]
        );

        database
            .execute_unprepared("DELETE FROM workspace WHERE id='W00000000000000000001'")
            .await
            .unwrap();
        let historical = load_invitation_projection(&database, invitation)
            .await
            .unwrap();
        assert_eq!(
            historical.workspaces,
            vec![InvitationWorkspaceProjectionRow {
                workspace_id: "W00000000000000000001".to_owned(),
                name: None,
            }]
        );
    }

    #[tokio::test]
    async fn scoped_listing_filters_before_cursor_and_materializes_due_rows_in_bounded_batches() {
        let database = fixture().await;
        create_invitation(&database, 1, 1, 1, 30, 200, &[1]).await;
        create_invitation(&database, 2, 1, 2, 20, 90, &[1]).await;
        create_invitation(&database, 3, 1, 3, 10, 200, &[1]).await;
        create_invitation(&database, 4, 2, 4, 40, 200, &[1]).await;
        create_invitation(&database, 5, 2, 5, 50, 90, &[1]).await;

        let transaction = database.begin().await.unwrap();
        let first = list_pending_invitations_for_creator(
            &transaction,
            &GatewayId::new("G00000000000000000001").unwrap(),
            &principal_id(1),
            timestamp(100),
            None,
            1,
        )
        .await
        .unwrap();
        assert_eq!(first.invitations[0].id, invitation_id(1).to_string());
        assert_eq!(
            first
                .materialized_expirations
                .iter()
                .map(|invitation| invitation.id.clone())
                .collect::<Vec<_>>(),
            [invitation_id(2).to_string()]
        );
        let second = list_pending_invitations_for_creator(
            &transaction,
            &GatewayId::new("G00000000000000000001").unwrap(),
            &principal_id(1),
            timestamp(100),
            first.next_cursor.as_ref(),
            1,
        )
        .await
        .unwrap();
        assert_eq!(second.invitations[0].id, invitation_id(3).to_string());
        assert_eq!(
            count_live_pending_invitations_for_creator(
                &transaction,
                &GatewayId::new("G00000000000000000001").unwrap(),
                &principal_id(1),
                timestamp(100),
            )
            .await
            .unwrap(),
            2
        );
        assert!(second.next_cursor.is_none());
        assert!(second.materialized_expirations.is_empty());

        let pending = list_invitations_for_superuser(
            &transaction,
            &GatewayId::new("G00000000000000000001").unwrap(),
            Some(InvitationStatus::Pending),
            None,
            timestamp(100),
            None,
            1,
        )
        .await
        .unwrap();
        assert_eq!(pending.invitations[0].id, invitation_id(4).to_string());
        assert_eq!(
            pending
                .materialized_expirations
                .iter()
                .map(|invitation| invitation.id.clone())
                .collect::<Vec<_>>(),
            [invitation_id(5).to_string()]
        );

        let first_expired = list_invitations_for_superuser(
            &transaction,
            &GatewayId::new("G00000000000000000001").unwrap(),
            Some(InvitationStatus::Expired),
            None,
            timestamp(100),
            None,
            1,
        )
        .await
        .unwrap();
        assert_eq!(
            first_expired.invitations[0].id,
            invitation_id(5).to_string()
        );
        assert!(first_expired.materialized_expirations.is_empty());
        assert_eq!(
            load_invitation(&transaction, &invitation_id(2))
                .await
                .unwrap()
                .unwrap()
                .status,
            "expired"
        );
        let second_expired = list_invitations_for_superuser(
            &transaction,
            &GatewayId::new("G00000000000000000001").unwrap(),
            Some(InvitationStatus::Expired),
            None,
            timestamp(100),
            first_expired.next_cursor.as_ref(),
            1,
        )
        .await
        .unwrap();
        assert_eq!(
            second_expired.invitations[0].id,
            invitation_id(2).to_string()
        );
        assert!(second_expired.materialized_expirations.is_empty());
        assert!(second_expired.next_cursor.is_none());
        transaction.commit().await.unwrap();

        let expired = load_invitation(&database, &invitation_id(2))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(expired.status, "expired");
        assert!(expired.token_hash.is_none());
        assert_eq!(
            effective_invitation_status(
                &load_invitation(&database, &invitation_id(4))
                    .await
                    .unwrap()
                    .unwrap(),
                timestamp(250),
            )
            .unwrap(),
            InvitationStatus::Expired
        );
    }

    #[tokio::test]
    async fn superuser_listing_reports_every_expiration_materialized_by_the_touch() {
        let database = fixture().await;
        create_invitation(&database, 1, 1, 1, 30, 90, &[1]).await;
        create_invitation(&database, 2, 1, 2, 20, 90, &[1]).await;

        let transaction = database.begin().await.unwrap();
        let page = list_invitations_for_superuser(
            &transaction,
            &GatewayId::new("G00000000000000000001").unwrap(),
            Some(InvitationStatus::Expired),
            None,
            timestamp(100),
            Some(&InvitationListCursor {
                created_at: timestamp(25),
                invitation_id: invitation_id(9),
            }),
            1,
        )
        .await
        .unwrap();

        assert_eq!(page.invitations[0].id, invitation_id(2).to_string());
        let materialized = page
            .materialized_expirations
            .iter()
            .map(|invitation| invitation.id.clone())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            materialized,
            [invitation_id(1).to_string(), invitation_id(2).to_string()]
                .into_iter()
                .collect()
        );
        transaction.rollback().await.unwrap();
    }

    #[tokio::test]
    async fn token_lookup_materializes_expiration_and_never_returns_expired_credentials() {
        let database = fixture().await;
        create_invitation(&database, 1, 1, 7, 1, 50, &[1]).await;
        let transaction = database.begin().await.unwrap();
        assert!(matches!(
            load_effective_pending_invitation_by_token_hash(&transaction, &[7; 32], timestamp(20))
                .await
                .unwrap(),
            PendingInvitationLookup::Available(_)
        ));
        assert!(matches!(
            load_effective_pending_invitation_by_token_hash(&transaction, &[7; 32], timestamp(50))
                .await
                .unwrap(),
            PendingInvitationLookup::Expired(_)
        ));
        assert!(matches!(
            load_effective_pending_invitation_by_token_hash(&transaction, &[7; 32], timestamp(51))
                .await
                .unwrap(),
            PendingInvitationLookup::Unavailable
        ));
        transaction.commit().await.unwrap();
    }

    #[tokio::test]
    async fn exactly_one_pending_terminal_transition_wins() {
        let database = fixture().await;
        create_invitation(&database, 1, 1, 1, 1, 200, &[1]).await;
        let first_database = database.clone();
        let second_database = database.clone();
        let first = async move {
            let transaction = first_database.begin().await.unwrap();
            let outcome = transition_pending_to_revoked(
                &transaction,
                &invitation_id(1),
                InvitationRevokeReason::InviterRevoked,
                timestamp(100),
            )
            .await
            .unwrap();
            transaction.commit().await.unwrap();
            outcome
        };
        let second = async move {
            let transaction = second_database.begin().await.unwrap();
            let outcome = transition_pending_to_revoked(
                &transaction,
                &invitation_id(1),
                InvitationRevokeReason::InviterRevoked,
                timestamp(100),
            )
            .await
            .unwrap();
            transaction.commit().await.unwrap();
            outcome
        };
        let (first, second) = tokio::join!(first, second);
        assert_eq!(
            [first, second]
                .into_iter()
                .filter(|outcome| matches!(outcome, InvitationTransitionOutcome::Applied(_)))
                .count(),
            1
        );

        let transaction = database.begin().await.unwrap();
        let terminal = transition_pending_to_accepted(
            &transaction,
            &invitation_id(1),
            &[1; 32],
            &principal_id(1),
            &DeviceId::new("D00000000000000000001").unwrap(),
            &session_id(1),
            timestamp(101),
        )
        .await
        .unwrap();
        assert!(
            matches!(terminal, InvitationTransitionOutcome::NotApplied(model) if model.status == "revoked")
        );
        transaction.rollback().await.unwrap();
    }
}
