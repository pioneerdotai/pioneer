use std::{collections::HashMap, sync::Arc};

use anyhow::{Context, Error};
use pioneer_artifacts::mime::detect_mime_from_bytes;
use pioneer_crud::CrudStore;
use pioneer_protocol::{
    AuthSessionId, AuthSessionRevokeReason, DeviceId, GatewayId, InvitationId,
    InvitationRevokeReason, MemberListParams, MemberListResponse, MemberManagementErrorReason,
    MemberMutationResponse, MemberRemoveParams, MemberRestoreParams, MemberSummary,
    MemberSuspendParams, PROFILE_AVATAR_MAX_DECODED_BYTES, PROFILE_AVATAR_MAX_DIMENSION,
    PersistedActorRef, PrincipalId, PrincipalKind, PrincipalStatus, ProfileAvatarMediaType,
    RoleKey, WorkspaceId, WorkspaceMemberAddParams, WorkspaceMemberListParams,
    WorkspaceMemberListResponse, WorkspaceMemberMutationResponse, WorkspaceMemberRemoveParams,
};
use sea_orm::{DatabaseTransaction, SqliteTransactionMode, TransactionOptions, TransactionTrait};
use sha2::Digest as _;

use crate::administrative_audit::AdministrativeAuditWriter;
use crate::auth::AuthenticatedSessionPrincipal;
use crate::authorization::{
    AuthorizationDecision, AuthorizationResolver, AuthorizationService, AuthorizedMemberDirectory,
    AuthorizedMemberPrincipal, AuthorizedWorkspace, DenyReason, DisclosurePolicy, ProofResolution,
    ResourceAction, RuntimePrincipalPolicy, persisted_actor_is_current,
};
use crate::epic5_observability::Epic5RateLimits;
use crate::secrets::GatewaySecrets;

use super::cursor::MemberCursorCodec;

const MEMBER_CURSOR_KEY_MIN_BYTES: usize = 32;

#[derive(Clone)]
pub(crate) struct MemberService {
    store: CrudStore,
    secrets: Arc<GatewaySecrets>,
    rate_limits: Arc<Epic5RateLimits>,
}

#[derive(Debug)]
pub(crate) enum MemberServiceError {
    InvalidParams,
    InvalidTarget,
    TargetUnavailable,
    RateLimited,
    Conflict(MemberManagementErrorReason),
    Authorization(AuthorizationDecision),
    Unavailable(Error),
}

#[derive(Clone)]
pub(crate) struct MemberAvatarSnapshot {
    media_type: ProfileAvatarMediaType,
    revision: String,
    content: Vec<u8>,
}

impl MemberAvatarSnapshot {
    pub(crate) fn new(
        media_type: ProfileAvatarMediaType,
        revision: String,
        content: Vec<u8>,
    ) -> Self {
        Self {
            media_type,
            revision,
            content,
        }
    }

    pub(crate) const fn media_type(&self) -> ProfileAvatarMediaType {
        self.media_type
    }

    pub(crate) fn revision(&self) -> &str {
        self.revision.as_str()
    }

    pub(crate) fn content(&self) -> &[u8] {
        self.content.as_slice()
    }
}

impl std::fmt::Debug for MemberAvatarSnapshot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MemberAvatarSnapshot")
            .field("media_type", &self.media_type)
            .field("revision", &self.revision)
            .field("content", &"[redacted]")
            .finish()
    }
}

#[derive(Debug)]
pub(crate) struct MemberLifecycleCommitted {
    pub(crate) response: MemberMutationResponse,
    pub(crate) revoked_session_ids: Vec<AuthSessionId>,
    pub(crate) revoked_device_ids: Vec<DeviceId>,
    pub(crate) affected_workspace_ids: Vec<WorkspaceId>,
}

#[derive(Debug)]
pub(crate) struct MemberRemovalCommitted {
    pub(crate) response: MemberMutationResponse,
    pub(crate) revoked_session_ids: Vec<AuthSessionId>,
    pub(crate) revoked_device_ids: Vec<DeviceId>,
    pub(crate) removed_workspace_ids: Vec<WorkspaceId>,
    pub(crate) removed_private_thread_ids: Vec<String>,
    pub(crate) changed_invitation_ids: Vec<InvitationId>,
}

#[derive(Debug)]
pub(crate) struct WorkspaceMemberRemovalCommitted {
    pub(crate) response: WorkspaceMemberMutationResponse,
    pub(crate) removed_private_thread_ids: Vec<String>,
}

impl MemberService {
    #[cfg(test)]
    pub(crate) fn new(store: CrudStore, secrets: Arc<GatewaySecrets>) -> Self {
        Self::with_rate_limits(store, secrets, Arc::new(Epic5RateLimits::default()))
    }

    pub(crate) fn with_rate_limits(
        store: CrudStore,
        secrets: Arc<GatewaySecrets>,
        rate_limits: Arc<Epic5RateLimits>,
    ) -> Self {
        Self {
            store,
            secrets,
            rate_limits,
        }
    }

    pub(crate) async fn suspend(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        authorization: &AuthorizedMemberPrincipal,
        params: MemberSuspendParams,
    ) -> Result<MemberLifecycleCommitted, MemberServiceError> {
        if authorization.principal_id() != &principal.principal_id
            || authorization.action() != ResourceAction::MemberSuspend
            || authorization.target_principal_id() != &params.principal_id
        {
            return Err(scope_mismatch());
        }
        let database = self.store.database_connection();
        let _write_admission = self.store.write_coordinator().acquire_foreground().await;
        let transaction = database
            .begin_with_options(TransactionOptions {
                sqlite_transaction_mode: Some(SqliteTransactionMode::Immediate),
                ..Default::default()
            })
            .await
            .context("failed to begin Member suspension transaction")
            .map_err(MemberServiceError::Unavailable)?;
        let result = async {
            ensure_current_actor(&transaction, principal).await?;
            let mut target =
                load_lifecycle_target(&transaction, &principal.gateway_id, &params.principal_id)
                    .await?;
            let avatar_revision =
                pioneer_crud::load_principal_avatar(&transaction, &params.principal_id)
                    .await?
                    .map(|avatar| hex::encode(avatar.content_hash));
            if params
                .expected_status
                .is_some_and(|expected| expected != target.status)
            {
                return Err(MemberServiceError::Conflict(
                    MemberManagementErrorReason::Conflict,
                ));
            }
            if target.status == PrincipalStatus::Suspended {
                return Ok(MemberLifecycleCommitted {
                    response: MemberMutationResponse {
                        member: member_summary(target, avatar_revision.clone())?,
                        changed: false,
                    },
                    revoked_session_ids: Vec::new(),
                    revoked_device_ids: Vec::new(),
                    affected_workspace_ids: Vec::new(),
                });
            }
            if target.status != PrincipalStatus::Active {
                return Err(MemberServiceError::InvalidTarget);
            }
            let now = chrono::Utc::now().fixed_offset();
            if !pioneer_crud::transition_member_principal_status(
                &transaction,
                &principal.gateway_id,
                &params.principal_id,
                PrincipalStatus::Active,
                PrincipalStatus::Suspended,
                None,
                now,
            )
            .await?
            {
                return Err(MemberServiceError::Conflict(
                    MemberManagementErrorReason::Conflict,
                ));
            }
            let revoked = pioneer_crud::revoke_principal_credentials(
                &transaction,
                &principal.gateway_id,
                &params.principal_id,
                AuthSessionRevokeReason::PrincipalSuspended,
                now,
            )
            .await?;
            let affected_workspace_ids = pioneer_crud::list_workspace_memberships_for_principal(
                &transaction,
                &params.principal_id,
            )
            .await?
            .into_iter()
            .map(|membership| WorkspaceId::new(membership.workspace_id))
            .collect::<Result<Vec<_>, _>>()
            .context("persisted Member workspace membership is invalid")?;
            AdministrativeAuditWriter
                .member_suspended(
                    &transaction,
                    &principal.gateway_id,
                    &principal.principal_id,
                    &principal.session_id,
                    &params.principal_id,
                    now,
                )
                .await?;
            target.status = PrincipalStatus::Suspended;
            Ok(MemberLifecycleCommitted {
                response: MemberMutationResponse {
                    member: member_summary(target, avatar_revision)?,
                    changed: true,
                },
                revoked_session_ids: revoked.session_ids,
                revoked_device_ids: revoked.device_ids,
                affected_workspace_ids,
            })
        }
        .await;
        match result {
            Ok(committed) => {
                transaction
                    .commit()
                    .await
                    .context("failed to commit Member suspension transaction")
                    .map_err(MemberServiceError::Unavailable)?;
                Ok(committed)
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    pub(crate) async fn restore(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        authorization: &AuthorizedMemberPrincipal,
        params: MemberRestoreParams,
    ) -> Result<MemberLifecycleCommitted, MemberServiceError> {
        if authorization.principal_id() != &principal.principal_id
            || authorization.action() != ResourceAction::MemberRestore
            || authorization.target_principal_id() != &params.principal_id
        {
            return Err(scope_mismatch());
        }
        let database = self.store.database_connection();
        let _write_admission = self.store.write_coordinator().acquire_foreground().await;
        let transaction = database
            .begin_with_options(TransactionOptions {
                sqlite_transaction_mode: Some(SqliteTransactionMode::Immediate),
                ..Default::default()
            })
            .await
            .context("failed to begin Member restore transaction")
            .map_err(MemberServiceError::Unavailable)?;
        let result = async {
            ensure_current_actor(&transaction, principal).await?;
            let mut target =
                load_lifecycle_target(&transaction, &principal.gateway_id, &params.principal_id)
                    .await?;
            let avatar_revision =
                pioneer_crud::load_principal_avatar(&transaction, &params.principal_id)
                    .await?
                    .map(|avatar| hex::encode(avatar.content_hash));
            if params
                .expected_status
                .is_some_and(|expected| expected != target.status)
            {
                return Err(MemberServiceError::Conflict(
                    MemberManagementErrorReason::Conflict,
                ));
            }
            if target.status == PrincipalStatus::Active {
                return Ok(MemberLifecycleCommitted {
                    response: MemberMutationResponse {
                        member: member_summary(target, avatar_revision.clone())?,
                        changed: false,
                    },
                    revoked_session_ids: Vec::new(),
                    revoked_device_ids: Vec::new(),
                    affected_workspace_ids: Vec::new(),
                });
            }
            if target.status != PrincipalStatus::Suspended {
                return Err(MemberServiceError::InvalidTarget);
            }
            let now = chrono::Utc::now().fixed_offset();
            if !pioneer_crud::transition_member_principal_status(
                &transaction,
                &principal.gateway_id,
                &params.principal_id,
                PrincipalStatus::Suspended,
                PrincipalStatus::Active,
                None,
                now,
            )
            .await?
            {
                return Err(MemberServiceError::Conflict(
                    MemberManagementErrorReason::Conflict,
                ));
            }
            AdministrativeAuditWriter
                .member_restored(
                    &transaction,
                    &principal.gateway_id,
                    &principal.principal_id,
                    &principal.session_id,
                    &params.principal_id,
                    now,
                )
                .await?;
            let affected_workspace_ids = pioneer_crud::list_workspace_memberships_for_principal(
                &transaction,
                &params.principal_id,
            )
            .await?
            .into_iter()
            .map(|membership| WorkspaceId::new(membership.workspace_id))
            .collect::<Result<Vec<_>, _>>()
            .context("persisted Member workspace membership is invalid")?;
            target.status = PrincipalStatus::Active;
            Ok(MemberLifecycleCommitted {
                response: MemberMutationResponse {
                    member: member_summary(target, avatar_revision)?,
                    changed: true,
                },
                revoked_session_ids: Vec::new(),
                revoked_device_ids: Vec::new(),
                affected_workspace_ids,
            })
        }
        .await;
        match result {
            Ok(committed) => {
                transaction
                    .commit()
                    .await
                    .context("failed to commit Member restore transaction")
                    .map_err(MemberServiceError::Unavailable)?;
                Ok(committed)
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    pub(crate) async fn remove(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        authorization: &AuthorizedMemberPrincipal,
        params: MemberRemoveParams,
    ) -> Result<MemberRemovalCommitted, MemberServiceError> {
        if authorization.principal_id() != &principal.principal_id
            || authorization.action() != ResourceAction::MemberRemove
            || authorization.target_principal_id() != &params.principal_id
        {
            return Err(scope_mismatch());
        }
        let database = self.store.database_connection();
        let _write_admission = self.store.write_coordinator().acquire_foreground().await;
        let transaction = database
            .begin_with_options(TransactionOptions {
                sqlite_transaction_mode: Some(SqliteTransactionMode::Immediate),
                ..Default::default()
            })
            .await
            .context("failed to begin Member removal transaction")
            .map_err(MemberServiceError::Unavailable)?;
        let result = async {
            ensure_current_actor(&transaction, principal).await?;
            let mut target =
                load_lifecycle_target(&transaction, &principal.gateway_id, &params.principal_id)
                    .await?;
            let avatar_revision =
                pioneer_crud::load_principal_avatar(&transaction, &params.principal_id)
                    .await?
                    .map(|avatar| hex::encode(avatar.content_hash));
            if params
                .expected_status
                .is_some_and(|expected| expected != target.status)
            {
                return Err(MemberServiceError::Conflict(
                    MemberManagementErrorReason::Conflict,
                ));
            }
            if target.status == PrincipalStatus::Removed {
                return Ok(MemberRemovalCommitted {
                    response: MemberMutationResponse {
                        member: member_summary(target, avatar_revision.clone())?,
                        changed: false,
                    },
                    revoked_session_ids: Vec::new(),
                    revoked_device_ids: Vec::new(),
                    removed_workspace_ids: Vec::new(),
                    removed_private_thread_ids: Vec::new(),
                    changed_invitation_ids: Vec::new(),
                });
            }
            if !matches!(
                target.status,
                PrincipalStatus::Active | PrincipalStatus::Suspended
            ) {
                return Err(MemberServiceError::InvalidTarget);
            }
            let now = chrono::Utc::now().fixed_offset();
            if !pioneer_crud::transition_member_principal_status(
                &transaction,
                &principal.gateway_id,
                &params.principal_id,
                target.status,
                PrincipalStatus::Removed,
                Some(now),
                now,
            )
            .await?
            {
                return Err(MemberServiceError::Conflict(
                    MemberManagementErrorReason::Conflict,
                ));
            }
            let revoked = pioneer_crud::revoke_principal_credentials(
                &transaction,
                &principal.gateway_id,
                &params.principal_id,
                AuthSessionRevokeReason::PrincipalRemoved,
                now,
            )
            .await?;
            let memberships = pioneer_crud::delete_all_memberships_for_principal(
                &transaction,
                &principal.gateway_id,
                &params.principal_id,
            )
            .await?;
            let removed_workspace_ids = memberships
                .workspace_ids
                .into_iter()
                .map(WorkspaceId::new)
                .collect::<Result<Vec<_>, _>>()
                .context("persisted removed workspace membership is invalid")?;
            let changed_invitation_ids = pioneer_crud::revoke_pending_invitations_for_creator(
                &transaction,
                &principal.gateway_id,
                &params.principal_id,
                InvitationRevokeReason::InviterUnavailable,
                now,
            )
            .await?;
            AdministrativeAuditWriter
                .member_removed(
                    &transaction,
                    &principal.gateway_id,
                    &principal.principal_id,
                    &principal.session_id,
                    &params.principal_id,
                    now,
                )
                .await?;
            target.status = PrincipalStatus::Removed;
            target.removed_at = Some(now);
            Ok::<_, MemberServiceError>(MemberRemovalCommitted {
                response: MemberMutationResponse {
                    member: member_summary(target, avatar_revision)?,
                    changed: true,
                },
                revoked_session_ids: revoked.session_ids,
                revoked_device_ids: revoked.device_ids,
                removed_workspace_ids,
                removed_private_thread_ids: memberships.private_thread_ids,
                changed_invitation_ids,
            })
        }
        .await;
        match result {
            Ok(committed) => {
                transaction
                    .commit()
                    .await
                    .context("failed to commit Member removal transaction")
                    .map_err(MemberServiceError::Unavailable)?;
                Ok(committed)
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    pub(crate) async fn list(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        authorization: &AuthorizedMemberDirectory,
        params: MemberListParams,
    ) -> Result<MemberListResponse, MemberServiceError> {
        if authorization.principal_id() != &principal.principal_id
            || authorization.action() != ResourceAction::MemberDirectoryList
        {
            return Err(scope_mismatch());
        }
        let limit = params
            .validate()
            .map_err(|_| MemberServiceError::InvalidParams)?;
        let key = self
            .secrets
            .load_or_create_auth_credential_hmac_key(MEMBER_CURSOR_KEY_MIN_BYTES)
            .map_err(MemberServiceError::Unavailable)?;
        let codec = MemberCursorCodec::new(&key).map_err(MemberServiceError::Unavailable)?;
        let policy_role = AuthorizationService::new()
            .resolved_role_key(principal.kind, principal.role_key.as_ref())
            .ok_or_else(|| MemberServiceError::Authorization(unsupported_role()))?;
        let scope = format!("{policy_role}:{}", principal.principal_id);
        let cursor = params
            .cursor
            .as_deref()
            .map(|cursor| codec.decode(cursor, scope.as_str()))
            .transpose()
            .map_err(|_| MemberServiceError::InvalidParams)?;
        let database = self.store.database_connection();
        let transaction = database
            .begin()
            .await
            .context("failed to begin Member directory read transaction")
            .map_err(MemberServiceError::Unavailable)?;
        let result = async {
            ensure_current_actor(&transaction, principal).await?;
            let page = pioneer_crud::list_member_directory_page(
                &transaction,
                &principal.gateway_id,
                &principal.principal_id,
                principal.kind,
                cursor.as_ref(),
                u64::from(limit),
            )
            .await?;
            let principal_ids = page
                .principals
                .iter()
                .map(|row| row.id.clone())
                .collect::<Vec<_>>();
            let avatar_revisions =
                pioneer_crud::list_principal_avatar_revisions(&transaction, &principal_ids)
                    .await?
                    .into_iter()
                    .map(|avatar| (avatar.principal_id, hex::encode(avatar.content_hash)))
                    .collect::<HashMap<_, _>>();
            let members = page
                .principals
                .into_iter()
                .map(|row| {
                    let avatar_revision = avatar_revisions.get(row.id.as_str()).cloned();
                    member_summary(row, avatar_revision)
                })
                .collect::<Result<Vec<_>, Error>>()?;
            Ok::<_, MemberServiceError>(MemberListResponse {
                members,
                next_cursor: page
                    .next_cursor
                    .as_ref()
                    .map(|cursor| codec.encode(cursor, scope.as_str())),
            })
        }
        .await;
        finish_read_transaction(transaction, result).await
    }

    pub(crate) async fn avatar_snapshot(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        target_principal_id: &PrincipalId,
        expected_revision: Option<&str>,
    ) -> Result<MemberAvatarSnapshot, MemberServiceError> {
        let database = self.store.database_connection();
        let transaction = database
            .begin()
            .await
            .context("failed to begin Member avatar snapshot transaction")
            .map_err(MemberServiceError::Unavailable)?;
        let result = async {
            ensure_current_actor(&transaction, principal).await?;
            let gate = AuthorizationService::new().authorize_action(
                principal.kind,
                principal.role_key.as_ref(),
                ResourceAction::MemberAvatarRead,
            );
            match AuthorizationResolver::new(self.store.clone())
                .authorize_member_avatar(&transaction, principal, &gate, target_principal_id)
                .await
                .map_err(MemberServiceError::Unavailable)?
            {
                ProofResolution::Authorized(_) => {}
                ProofResolution::Denied(decision) => {
                    return Err(MemberServiceError::Authorization(decision));
                }
            }
            let avatar = match expected_revision {
                Some(expected_revision) => {
                    let content_hash = hex::decode(expected_revision)
                        .map_err(|_| MemberServiceError::Authorization(missing_resource()))?;
                    pioneer_crud::load_principal_avatar_revision(
                        &transaction,
                        target_principal_id,
                        content_hash.as_slice(),
                    )
                    .await?
                }
                None => {
                    pioneer_crud::load_principal_avatar(&transaction, target_principal_id).await?
                }
            }
            .ok_or_else(|| MemberServiceError::Authorization(missing_resource()))?;
            if avatar.content.is_empty()
                || avatar.content.len() > PROFILE_AVATAR_MAX_DECODED_BYTES
                || avatar.width <= 0
                || avatar.height <= 0
                || avatar.width > i64::from(PROFILE_AVATAR_MAX_DIMENSION)
                || avatar.height > i64::from(PROFILE_AVATAR_MAX_DIMENSION)
                || avatar.content_hash.len() != 32
            {
                return Err(MemberServiceError::Unavailable(Error::msg(
                    "persisted principal avatar violates bounded contract",
                )));
            }
            let media_type = match avatar.media_type.as_str() {
                "image/png" => ProfileAvatarMediaType::Png,
                "image/jpeg" => ProfileAvatarMediaType::Jpeg,
                "image/webp" => ProfileAvatarMediaType::Webp,
                _ => {
                    return Err(MemberServiceError::Unavailable(Error::msg(
                        "persisted principal avatar media type is invalid",
                    )));
                }
            };
            let actual_content_hash: [u8; 32] = sha2::Sha256::digest(&avatar.content).into();
            if avatar.content_hash.as_slice() != actual_content_hash {
                return Err(MemberServiceError::Unavailable(Error::msg(
                    "persisted principal avatar content hash is invalid",
                )));
            }
            if detect_mime_from_bytes(avatar.content.as_slice(), None) != media_type.as_str() {
                return Err(MemberServiceError::Unavailable(Error::msg(
                    "persisted principal avatar media type does not match its content",
                )));
            }
            let revision = hex::encode(avatar.content_hash);
            Ok::<_, MemberServiceError>(MemberAvatarSnapshot::new(
                media_type,
                revision,
                avatar.content,
            ))
        }
        .await;
        finish_read_transaction(transaction, result).await
    }

    pub(crate) async fn workspace_list(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        authorization: &AuthorizedWorkspace,
        params: WorkspaceMemberListParams,
    ) -> Result<WorkspaceMemberListResponse, MemberServiceError> {
        if authorization.principal_id() != &principal.principal_id
            || authorization.action() != ResourceAction::WorkspaceMemberList
            || authorization.workspace_id() != params.workspace_id.as_str()
        {
            return Err(scope_mismatch());
        }
        let limit = params
            .validate()
            .map_err(|_| MemberServiceError::InvalidParams)?;
        let key = self
            .secrets
            .load_or_create_auth_credential_hmac_key(MEMBER_CURSOR_KEY_MIN_BYTES)
            .map_err(MemberServiceError::Unavailable)?;
        let codec = MemberCursorCodec::new(&key).map_err(MemberServiceError::Unavailable)?;
        let scope = format!(
            "workspace:{}:{}",
            params.workspace_id, principal.principal_id
        );
        let cursor = params
            .cursor
            .as_deref()
            .map(|cursor| codec.decode(cursor, scope.as_str()))
            .transpose()
            .map_err(|_| MemberServiceError::InvalidParams)?;
        let database = self.store.database_connection();
        let transaction = database
            .begin()
            .await
            .context("failed to begin workspace member list transaction")
            .map_err(MemberServiceError::Unavailable)?;
        let result = async {
            ensure_current_actor(&transaction, principal).await?;
            let workspace_active =
                principal_has_active_workspace(&transaction, principal, &params.workspace_id)
                    .await?;
            if !workspace_active {
                return Err(MemberServiceError::Authorization(missing_resource()));
            }
            let page = pioneer_crud::list_workspace_member_principals_page(
                &transaction,
                &principal.gateway_id,
                params.workspace_id.as_str(),
                cursor.as_ref(),
                u64::from(limit),
            )
            .await?;
            let principal_ids = page
                .principals
                .iter()
                .map(|row| row.id.clone())
                .collect::<Vec<_>>();
            let avatar_revisions =
                pioneer_crud::list_principal_avatar_revisions(&transaction, &principal_ids)
                    .await?
                    .into_iter()
                    .map(|avatar| (avatar.principal_id, hex::encode(avatar.content_hash)))
                    .collect::<HashMap<_, _>>();
            let members = page
                .principals
                .into_iter()
                .map(|row| {
                    let avatar_revision = avatar_revisions.get(row.id.as_str()).cloned();
                    member_summary(row, avatar_revision)
                })
                .collect::<Result<Vec<_>, Error>>()?;
            Ok::<_, MemberServiceError>(WorkspaceMemberListResponse {
                workspace_id: params.workspace_id,
                members,
                next_cursor: page
                    .next_cursor
                    .as_ref()
                    .map(|cursor| codec.encode(cursor, scope.as_str())),
            })
        }
        .await;
        finish_read_transaction(transaction, result).await
    }

    pub(crate) async fn workspace_add(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        authorization: &AuthorizedWorkspace,
        params: WorkspaceMemberAddParams,
    ) -> Result<WorkspaceMemberMutationResponse, MemberServiceError> {
        if authorization.principal_id() != &principal.principal_id
            || authorization.action() != ResourceAction::WorkspaceMemberAdd
            || authorization.workspace_id() != params.workspace_id.as_str()
        {
            return Err(scope_mismatch());
        }
        if !self
            .rate_limits
            .allow_direct_add(&principal.gateway_id, &principal.principal_id)
        {
            tracing::warn!(
                event = "epic5_rate_limited",
                operation = "workspace_member_add",
                actor_principal_id = %principal.principal_id,
                outcome = "rate_limited",
            );
            return Err(MemberServiceError::RateLimited);
        }
        let database = self.store.database_connection();
        let _write_admission = self.store.write_coordinator().acquire_foreground().await;
        let transaction = database
            .begin_with_options(TransactionOptions {
                sqlite_transaction_mode: Some(SqliteTransactionMode::Immediate),
                ..Default::default()
            })
            .await
            .context("failed to begin workspace member add transaction")
            .map_err(MemberServiceError::Unavailable)?;
        let result = async {
            ensure_current_actor(&transaction, principal).await?;
            let workspace_active =
                principal_has_active_workspace(&transaction, principal, &params.workspace_id)
                    .await?;
            if !workspace_active {
                return Err(MemberServiceError::Authorization(missing_resource()));
            }
            let Some(target) =
                pioneer_crud::load_principal_by_id(&transaction, &params.principal_id).await?
            else {
                return Err(MemberServiceError::TargetUnavailable);
            };
            if target.gateway_id != principal.gateway_id {
                return Err(MemberServiceError::TargetUnavailable);
            }
            if target.kind != PrincipalKind::User
                || target.status != PrincipalStatus::Active
                || !is_lifecycle_managed_user_role(target.role_key.as_deref())
            {
                return Err(MemberServiceError::InvalidTarget);
            }
            let changed = pioneer_crud::find_workspace_membership(
                &transaction,
                &params.principal_id,
                params.workspace_id.as_str(),
            )
            .await?
            .is_none();
            if changed {
                let now = chrono::Utc::now().fixed_offset();
                pioneer_crud::insert_workspace_membership(
                    &transaction,
                    &pioneer_crud::NewWorkspaceMembership {
                        gateway_id: principal.gateway_id.clone(),
                        principal_id: params.principal_id.clone(),
                        workspace_id: params.workspace_id.to_string(),
                        granted_by: PersistedActorRef::Principal(principal.principal_id.clone()),
                        now,
                    },
                )
                .await?;
                AdministrativeAuditWriter
                    .workspace_member_added(
                        &transaction,
                        &principal.gateway_id,
                        &principal.principal_id,
                        &principal.session_id,
                        &params.principal_id,
                        &params.workspace_id,
                        now,
                    )
                    .await?;
            }
            let avatar_revision =
                pioneer_crud::load_principal_avatar(&transaction, &params.principal_id)
                    .await?
                    .map(|avatar| hex::encode(avatar.content_hash));
            Ok::<_, MemberServiceError>(WorkspaceMemberMutationResponse {
                workspace_id: params.workspace_id,
                member: member_summary(target, avatar_revision)?,
                changed,
            })
        }
        .await;
        match result {
            Ok(response) => {
                transaction
                    .commit()
                    .await
                    .context("failed to commit workspace member add transaction")
                    .map_err(MemberServiceError::Unavailable)?;
                Ok(response)
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    pub(crate) async fn workspace_remove(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        authorization: &AuthorizedWorkspace,
        params: WorkspaceMemberRemoveParams,
    ) -> Result<WorkspaceMemberRemovalCommitted, MemberServiceError> {
        if authorization.principal_id() != &principal.principal_id
            || authorization.action() != ResourceAction::WorkspaceMemberRemove
            || authorization.workspace_id() != params.workspace_id.as_str()
        {
            return Err(scope_mismatch());
        }
        let database = self.store.database_connection();
        let _write_admission = self.store.write_coordinator().acquire_foreground().await;
        let transaction = database
            .begin_with_options(TransactionOptions {
                sqlite_transaction_mode: Some(SqliteTransactionMode::Immediate),
                ..Default::default()
            })
            .await
            .context("failed to begin workspace member removal transaction")
            .map_err(MemberServiceError::Unavailable)?;
        let result = async {
            ensure_current_actor(&transaction, principal).await?;
            let workspace_active = pioneer_crud::resolve_workspace_authorization_scope(
                &transaction,
                params.workspace_id.as_str(),
            )
            .await?
            .is_some_and(|workspace| workspace.is_active);
            if !workspace_active {
                return Err(MemberServiceError::Authorization(missing_resource()));
            }
            let target = pioneer_crud::load_principal_by_id(&transaction, &params.principal_id)
                .await?
                .ok_or_else(|| MemberServiceError::Authorization(missing_resource()))?;
            if target.gateway_id != principal.gateway_id {
                return Err(MemberServiceError::Authorization(missing_resource()));
            }
            if target.kind != PrincipalKind::User
                || !matches!(
                    target.status,
                    PrincipalStatus::Active | PrincipalStatus::Suspended
                )
                || !is_lifecycle_managed_user_role(target.role_key.as_deref())
            {
                return Err(MemberServiceError::InvalidTarget);
            }
            let deletion = pioneer_crud::delete_workspace_membership(
                &transaction,
                &principal.gateway_id,
                &params.principal_id,
                params.workspace_id.as_str(),
            )
            .await?;
            if deletion.changed {
                AdministrativeAuditWriter
                    .workspace_member_removed(
                        &transaction,
                        &principal.gateway_id,
                        &principal.principal_id,
                        &principal.session_id,
                        &params.principal_id,
                        &params.workspace_id,
                        chrono::Utc::now().fixed_offset(),
                    )
                    .await?;
            }
            let avatar_revision =
                pioneer_crud::load_principal_avatar(&transaction, &params.principal_id)
                    .await?
                    .map(|avatar| hex::encode(avatar.content_hash));
            Ok::<_, MemberServiceError>(WorkspaceMemberRemovalCommitted {
                response: WorkspaceMemberMutationResponse {
                    workspace_id: params.workspace_id,
                    member: member_summary(target, avatar_revision)?,
                    changed: deletion.changed,
                },
                removed_private_thread_ids: deletion.removed_private_thread_ids,
            })
        }
        .await;
        match result {
            Ok(committed) => {
                transaction
                    .commit()
                    .await
                    .context("failed to commit workspace member removal transaction")
                    .map_err(MemberServiceError::Unavailable)?;
                Ok(committed)
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }
}

impl From<Error> for MemberServiceError {
    fn from(error: Error) -> Self {
        Self::Unavailable(error)
    }
}

async fn ensure_current_actor(
    transaction: &DatabaseTransaction,
    principal: &AuthenticatedSessionPrincipal,
) -> Result<(), MemberServiceError> {
    if persisted_actor_is_current(transaction, principal).await? {
        Ok(())
    } else {
        Err(MemberServiceError::Authorization(
            AuthorizationDecision::Deny {
                reason: DenyReason::InactivePrincipal,
                disclosure: DisclosurePolicy::AuthenticationTerminal,
            },
        ))
    }
}

async fn finish_read_transaction<T>(
    transaction: DatabaseTransaction,
    result: Result<T, MemberServiceError>,
) -> Result<T, MemberServiceError> {
    match result {
        Ok(value) => {
            transaction
                .commit()
                .await
                .context("failed to finish Member administration read transaction")
                .map_err(MemberServiceError::Unavailable)?;
            Ok(value)
        }
        Err(error) => {
            let _ = transaction.rollback().await;
            Err(error)
        }
    }
}

async fn load_lifecycle_target(
    transaction: &sea_orm::DatabaseTransaction,
    gateway_id: &GatewayId,
    principal_id: &PrincipalId,
) -> Result<pioneer_crud::GatewayPrincipalRecord, MemberServiceError> {
    let target = pioneer_crud::load_principal_by_id(transaction, principal_id)
        .await?
        .ok_or_else(|| MemberServiceError::Authorization(missing_resource()))?;
    if &target.gateway_id != gateway_id {
        return Err(MemberServiceError::Authorization(missing_resource()));
    }
    if target.kind != PrincipalKind::User
        || !is_lifecycle_managed_user_role(target.role_key.as_deref())
    {
        return Err(MemberServiceError::InvalidTarget);
    }
    Ok(target)
}

async fn principal_has_active_workspace(
    transaction: &DatabaseTransaction,
    principal: &AuthenticatedSessionPrincipal,
    workspace_id: &WorkspaceId,
) -> Result<bool, Error> {
    match AuthorizationService::new()
        .runtime_principal_policy(principal.kind, principal.role_key.as_ref())
    {
        Some(RuntimePrincipalPolicy::Absolute) => Ok(
            pioneer_crud::resolve_workspace_authorization_scope(transaction, workspace_id.as_str())
                .await?
                .is_some_and(|workspace| workspace.is_active),
        ),
        Some(RuntimePrincipalPolicy::ScopedCollaboration) => {
            Ok(pioneer_crud::find_active_workspace_for_principal(
                transaction,
                &principal.principal_id,
                workspace_id.as_str(),
            )
            .await?
            .is_some())
        }
        None => Ok(false),
    }
}

fn is_lifecycle_managed_user_role(role_key: Option<&str>) -> bool {
    role_key.is_some_and(|role_key| {
        RoleKey::new(role_key).is_ok_and(|role_key| {
            AuthorizationService::new()
                .role_is_lifecycle_managed(PrincipalKind::User, Some(&role_key))
        })
    })
}

fn member_summary(
    row: pioneer_crud::GatewayPrincipalRecord,
    avatar_revision: Option<String>,
) -> Result<MemberSummary, Error> {
    let role_key = row
        .role_key
        .map(RoleKey::new)
        .transpose()
        .context("persisted Member role is invalid")?;
    let authorization = AuthorizationService::new();
    let role = authorization
        .role_presentation(row.kind, role_key.as_ref())
        .context("persisted Member role is not registered")?;
    let lifecycle_managed = authorization.role_is_lifecycle_managed(row.kind, role_key.as_ref());
    Ok(MemberSummary {
        principal_id: row.id,
        kind: row.kind,
        display_name: row.display_name,
        nickname: row.nickname,
        role_key,
        role,
        lifecycle_managed,
        status: row.status,
        avatar_revision,
    })
}

fn scope_mismatch() -> MemberServiceError {
    MemberServiceError::Authorization(AuthorizationDecision::Deny {
        reason: DenyReason::ResourceScopeMismatch,
        disclosure: DisclosurePolicy::NotFound,
    })
}

fn missing_resource() -> AuthorizationDecision {
    AuthorizationDecision::Deny {
        reason: DenyReason::MissingAuthoritativeResource,
        disclosure: DisclosurePolicy::NotFound,
    }
}

fn unsupported_role() -> AuthorizationDecision {
    AuthorizationDecision::Deny {
        reason: DenyReason::UnsupportedRole,
        disclosure: DisclosurePolicy::AuthenticationTerminal,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use pioneer_crud::{CrudStore, NewInvitationRow, NewPrincipalAvatarRow};
    use pioneer_entity::{
        audit_event, auth_refresh_credential, auth_session, device, gateway_principal, invitation,
        thread_membership, workspace_membership,
    };
    use pioneer_keystore::MemorySecretStore;
    use pioneer_protocol::{
        ADMINISTRATION_DOMAIN_ID_LEN, AUTH_DOMAIN_ID_LEN, AuthSessionId, DeviceId, GatewayId,
        InvitationId, MemberRemoveParams, MemberRestoreParams, MemberSuspendParams, PrincipalId,
        PrincipalStatus, RoleKey, WorkspaceId, WorkspaceMemberAddParams, WorkspaceMemberListParams,
        WorkspaceMemberRemoveParams, generate_id,
    };
    use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, TransactionTrait};
    use sha2::{Digest, Sha256};

    use crate::authorization::{AuthorizationResolver, AuthorizationService, ProofResolution};
    use crate::secrets::GatewaySecrets;
    use crate::tests::authorization::{IsolatedEpic4Harness, MEMBER_A_ID, MEMBER_B_ID};

    use super::*;

    fn member_a() -> AuthenticatedSessionPrincipal {
        AuthenticatedSessionPrincipal {
            gateway_id: GatewayId::new("G00000000000000000001").unwrap(),
            principal_id: PrincipalId::new(MEMBER_A_ID).unwrap(),
            kind: PrincipalKind::User,
            role_key: Some(RoleKey::member()),
            device_id: DeviceId::new("D0000000000000000000A").unwrap(),
            session_id: AuthSessionId::new("S0000000000000000000A").unwrap(),
            access_jti: generate_id(AUTH_DOMAIN_ID_LEN),
            access_expires_at_unix: u64::MAX,
        }
    }

    fn superuser() -> AuthenticatedSessionPrincipal {
        AuthenticatedSessionPrincipal {
            gateway_id: GatewayId::new("G00000000000000000001").unwrap(),
            principal_id: PrincipalId::new(crate::auth::test_support::TEST_SUPERUSER_ID).unwrap(),
            kind: PrincipalKind::Superuser,
            role_key: None,
            device_id: DeviceId::new("D00000000000000000001").unwrap(),
            session_id: AuthSessionId::new("S00000000000000000001").unwrap(),
            access_jti: generate_id(AUTH_DOMAIN_ID_LEN),
            access_expires_at_unix: u64::MAX,
        }
    }

    async fn seed_avatar(
        database: &sea_orm::DatabaseConnection,
        principal_id: &PrincipalId,
    ) -> String {
        let content = b"\x89PNG\r\n\x1a\nfixture-avatar".to_vec();
        let content_hash: [u8; 32] = Sha256::digest(content.as_slice()).into();
        let transaction = database.begin().await.unwrap();
        pioneer_crud::insert_principal_avatar(
            &transaction,
            NewPrincipalAvatarRow {
                principal_id: principal_id.clone(),
                media_type: ProfileAvatarMediaType::Png,
                content,
                content_hash,
                width: 1,
                height: 1,
                now: chrono::Utc::now().fixed_offset(),
            },
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();
        hex::encode(content_hash)
    }

    async fn materialize_superuser_session(harness: &IsolatedEpic4Harness) {
        harness
            .database
            .execute_unprepared(
                "INSERT INTO device(
                    id,gateway_id,principal_id,installation_id,display_name,client_kind,
                    platform,client_version,status,created_at,updated_at,last_seen_at,revoked_at
                 ) VALUES(
                    'D00000000000000000001','G00000000000000000001',
                    'P00000000000000000001','fixture-superuser','Superuser Desktop','desktop',
                    'test','1','active',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,NULL
                 );
                 INSERT INTO auth_session(
                    id,gateway_id,principal_id,device_id,token_family_id,created_by_session_id,
                    activation_token_hash,activation_locator_hash,activation_failed_attempts,
                    activation_expires_at,activated_at,status,refresh_generation,created_at,
                    updated_at,last_seen_at,last_refreshed_at,refresh_expires_at,revoked_at,
                    revoke_reason
                 ) VALUES(
                    'S00000000000000000001','G00000000000000000001',
                    'P00000000000000000001','D00000000000000000001',
                    'F00000000000000000001',NULL,
                    X'0000000000000000000000000000000000000000000000000000000000000001',
                    X'1000000000000000000000000000000000000000000000000000000000000001',
                    0,datetime('now','+10 minutes'),CURRENT_TIMESTAMP,'active',0,
                    CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,
                    datetime('now','+90 days'),NULL,NULL
                 )",
            )
            .await
            .unwrap();
    }

    fn directory_proof(
        store: &CrudStore,
        principal: &AuthenticatedSessionPrincipal,
    ) -> AuthorizedMemberDirectory {
        let gate = AuthorizationService::new().authorize_action(
            principal.kind,
            principal.role_key.as_ref(),
            ResourceAction::MemberDirectoryList,
        );
        match AuthorizationResolver::new(store.clone()).authorize_member_directory(principal, &gate)
        {
            ProofResolution::Authorized(proof) => proof,
            ProofResolution::Denied(decision) => panic!("unexpected denial: {decision:?}"),
        }
    }

    async fn avatar_proof(
        store: &CrudStore,
        principal: &AuthenticatedSessionPrincipal,
        target: &PrincipalId,
    ) -> ProofResolution<crate::authorization::AuthorizedMemberAvatar> {
        let gate = AuthorizationService::new().authorize_action(
            principal.kind,
            principal.role_key.as_ref(),
            ResourceAction::MemberAvatarRead,
        );
        AuthorizationResolver::new(store.clone())
            .authorize_member_avatar(&store.database_connection(), principal, &gate, target)
            .await
            .unwrap()
    }

    async fn workspace_proof(
        store: &CrudStore,
        principal: &AuthenticatedSessionPrincipal,
        action: ResourceAction,
        workspace_id: &WorkspaceId,
    ) -> ProofResolution<AuthorizedWorkspace> {
        let gate = AuthorizationService::new().authorize_action(
            principal.kind,
            principal.role_key.as_ref(),
            action,
        );
        AuthorizationResolver::new(store.clone())
            .authorize_workspace(principal, &gate, action, workspace_id.as_str())
            .await
            .unwrap()
    }

    async fn member_principal_proof(
        store: &CrudStore,
        principal: &AuthenticatedSessionPrincipal,
        action: ResourceAction,
        target: &PrincipalId,
    ) -> ProofResolution<AuthorizedMemberPrincipal> {
        let gate = AuthorizationService::new().authorize_action(
            principal.kind,
            principal.role_key.as_ref(),
            action,
        );
        AuthorizationResolver::new(store.clone())
            .authorize_member_principal(
                &store.database_connection(),
                principal,
                &gate,
                action,
                target,
            )
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn directory_acl_precedes_pagination_and_superuser_keeps_history() {
        let harness = IsolatedEpic4Harness::populated().await.unwrap();
        materialize_superuser_session(&harness).await;
        harness
            .database
            .execute_unprepared(
                "INSERT INTO workspace_membership(
                    principal_id,workspace_id,granted_by_actor_kind,granted_by_actor_id,
                    created_at,updated_at
                 ) VALUES(
                    'P0000000000000000000C','W00000000000000000001','system',NULL,
                    CURRENT_TIMESTAMP,CURRENT_TIMESTAMP
                 )",
            )
            .await
            .unwrap();
        let store = CrudStore::new(harness.database.clone());
        let service = MemberService::new(
            store.clone(),
            Arc::new(GatewaySecrets::new(Arc::new(MemorySecretStore::new()))),
        );
        let member = member_a();
        let proof = directory_proof(&store, &member);
        let mut cursor = None;
        let mut ids = Vec::new();
        loop {
            let page = service
                .list(
                    &member,
                    &proof,
                    MemberListParams {
                        cursor,
                        limit: Some(1),
                    },
                )
                .await
                .unwrap();
            ids.extend(page.members.into_iter().map(|member| member.principal_id));
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(
            ids,
            [
                MEMBER_A_ID,
                MEMBER_B_ID,
                "P0000000000000000000C",
                crate::auth::test_support::TEST_SUPERUSER_ID,
            ]
            .into_iter()
            .map(|id| PrincipalId::new(id).unwrap())
            .collect::<Vec<_>>()
        );
        assert!(!ids.iter().any(|id| id.as_str() == "P0000000000000000000D"));

        let root = superuser();
        let root_proof = directory_proof(&store, &root);
        let root_page = service
            .list(&root, &root_proof, MemberListParams::default())
            .await
            .unwrap();
        assert_eq!(root_page.members.len(), 5);
        assert!(root_page.members.iter().any(|member| {
            member.principal_id.as_str() == "P0000000000000000000D"
                && member.status == PrincipalStatus::Removed
        }));
    }

    #[tokio::test]
    async fn avatar_requires_current_directory_scope_and_returns_exact_bounded_bytes() {
        let harness = IsolatedEpic4Harness::populated().await.unwrap();
        let store = CrudStore::new(harness.database.clone());
        let service = MemberService::new(
            store.clone(),
            Arc::new(GatewaySecrets::new(Arc::new(MemorySecretStore::new()))),
        );
        let target = PrincipalId::new(MEMBER_B_ID).unwrap();
        let content = b"\x89PNG\r\n\x1a\nfixture-avatar".to_vec();
        let content_hash: [u8; 32] = Sha256::digest(content.as_slice()).into();
        let transaction = harness.database.begin().await.unwrap();
        pioneer_crud::insert_principal_avatar(
            &transaction,
            NewPrincipalAvatarRow {
                principal_id: target.clone(),
                media_type: ProfileAvatarMediaType::Png,
                content: content.clone(),
                content_hash,
                width: 1,
                height: 1,
                now: chrono::Utc::now().fixed_offset(),
            },
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();
        let viewer = member_a();
        let revision = hex::encode(content_hash);
        let snapshot = service
            .avatar_snapshot(&viewer, &target, Some(revision.as_str()))
            .await
            .unwrap();
        assert_eq!(snapshot.media_type(), ProfileAvatarMediaType::Png);
        assert_eq!(snapshot.revision(), revision);
        assert_eq!(snapshot.content(), content);
        assert!(format!("{snapshot:?}").contains("[redacted]"));
        assert!(matches!(
            service
                .avatar_snapshot(&viewer, &target, Some("0".repeat(64).as_str()))
                .await,
            Err(MemberServiceError::Authorization(_))
        ));

        harness
            .database
            .execute_unprepared(
                "UPDATE principal_avatar_revision SET media_type='image/jpeg' \
                 WHERE principal_id='P0000000000000000000B'",
            )
            .await
            .unwrap();
        assert!(matches!(
            service
                .avatar_snapshot(&viewer, &target, Some(revision.as_str()))
                .await,
            Err(MemberServiceError::Unavailable(_))
        ));

        harness
            .database
            .execute_unprepared(
                "UPDATE principal_avatar_revision \
                 SET media_type='image/png', content=zeroblob(length(content)) \
                 WHERE principal_id='P0000000000000000000B'",
            )
            .await
            .unwrap();
        assert!(matches!(
            service
                .avatar_snapshot(&viewer, &target, Some(revision.as_str()))
                .await,
            Err(MemberServiceError::Unavailable(_))
        ));

        harness
            .database
            .execute_unprepared(
                "DELETE FROM workspace_membership \
                 WHERE principal_id='P0000000000000000000B' \
                   AND workspace_id='W00000000000000000001'",
            )
            .await
            .unwrap();
        assert!(matches!(
            service
                .avatar_snapshot(&viewer, &target, Some(revision.as_str()))
                .await,
            Err(MemberServiceError::Authorization(_))
        ));

        let hidden = PrincipalId::new("P0000000000000000000C").unwrap();
        assert!(matches!(
            avatar_proof(&store, &viewer, &hidden).await,
            ProofResolution::Denied(_)
        ));
    }

    #[tokio::test]
    async fn historical_avatar_revisions_survive_replacement_and_current_removal() {
        let harness = IsolatedEpic4Harness::populated().await.unwrap();
        let service = MemberService::new(
            CrudStore::new(harness.database.clone()),
            Arc::new(GatewaySecrets::new(Arc::new(MemorySecretStore::new()))),
        );
        let target = PrincipalId::new(MEMBER_B_ID).unwrap();
        let viewer = member_a();
        let first_content = b"\x89PNG\r\n\x1a\nfirst-avatar".to_vec();
        let first_hash: [u8; 32] = Sha256::digest(first_content.as_slice()).into();
        let second_content = b"\x89PNG\r\n\x1a\nsecond-avatar".to_vec();
        let second_hash: [u8; 32] = Sha256::digest(second_content.as_slice()).into();
        let transaction = harness.database.begin().await.unwrap();
        pioneer_crud::insert_principal_avatar(
            &transaction,
            NewPrincipalAvatarRow {
                principal_id: target.clone(),
                media_type: ProfileAvatarMediaType::Png,
                content: first_content.clone(),
                content_hash: first_hash,
                width: 1,
                height: 1,
                now: chrono::Utc::now().fixed_offset(),
            },
        )
        .await
        .unwrap();
        pioneer_crud::replace_principal_avatar(
            &transaction,
            NewPrincipalAvatarRow {
                principal_id: target.clone(),
                media_type: ProfileAvatarMediaType::Png,
                content: second_content.clone(),
                content_hash: second_hash,
                width: 1,
                height: 1,
                now: chrono::Utc::now().fixed_offset(),
            },
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        let first_revision = hex::encode(first_hash);
        let second_revision = hex::encode(second_hash);
        assert_eq!(
            service
                .avatar_snapshot(&viewer, &target, Some(first_revision.as_str()))
                .await
                .unwrap()
                .content(),
            first_content
        );
        assert_eq!(
            service
                .avatar_snapshot(&viewer, &target, Some(second_revision.as_str()))
                .await
                .unwrap()
                .content(),
            second_content
        );

        let transaction = harness.database.begin().await.unwrap();
        assert!(
            pioneer_crud::delete_principal_avatar(&transaction, &target)
                .await
                .unwrap()
        );
        transaction.commit().await.unwrap();
        assert_eq!(
            service
                .avatar_snapshot(&viewer, &target, Some(first_revision.as_str()))
                .await
                .unwrap()
                .content(),
            first_content
        );
        assert!(
            service
                .avatar_snapshot(&viewer, &target, None)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn avatar_snapshot_preserves_proposal60_directory_visibility() {
        let harness = IsolatedEpic4Harness::populated().await.unwrap();
        materialize_superuser_session(&harness).await;
        let store = CrudStore::new(harness.database.clone());
        let service = MemberService::new(
            store,
            Arc::new(GatewaySecrets::new(Arc::new(MemorySecretStore::new()))),
        );
        let viewer = member_a();
        let self_id = viewer.principal_id.clone();
        let shared = PrincipalId::new(MEMBER_B_ID).unwrap();
        let suspended = PrincipalId::new(crate::tests::authorization::SUSPENDED_MEMBER_ID).unwrap();
        let removed = PrincipalId::new(crate::tests::authorization::REMOVED_MEMBER_ID).unwrap();
        let missing = PrincipalId::new("P0000000000000000000Z").unwrap();

        let self_revision = seed_avatar(&harness.database, &self_id).await;
        let shared_revision = seed_avatar(&harness.database, &shared).await;
        let suspended_revision = seed_avatar(&harness.database, &suspended).await;
        let removed_revision = seed_avatar(&harness.database, &removed).await;
        let missing_revision = "0".repeat(64);

        assert!(
            service
                .avatar_snapshot(&viewer, &self_id, Some(self_revision.as_str()))
                .await
                .is_ok()
        );
        assert!(
            service
                .avatar_snapshot(&viewer, &shared, Some(shared_revision.as_str()))
                .await
                .is_ok()
        );
        for (target, revision) in [
            (&suspended, suspended_revision.as_str()),
            (&removed, removed_revision.as_str()),
            (&missing, missing_revision.as_str()),
        ] {
            assert!(matches!(
                service
                    .avatar_snapshot(&viewer, target, Some(revision))
                    .await,
                Err(MemberServiceError::Authorization(_))
            ));
        }

        // Proposal 60 grants the Superuser authoritative directory visibility,
        // including lifecycle records hidden from ordinary members.
        assert!(
            service
                .avatar_snapshot(&superuser(), &removed, Some(removed_revision.as_str()))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn workspace_list_contains_only_explicit_members_and_member_cannot_list_foreign_scope() {
        let harness = IsolatedEpic4Harness::populated().await.unwrap();
        let store = CrudStore::new(harness.database.clone());
        let service = MemberService::new(
            store.clone(),
            Arc::new(GatewaySecrets::new(Arc::new(MemorySecretStore::new()))),
        );
        let actor = member_a();
        let red = WorkspaceId::new("W00000000000000000001").unwrap();
        let proof = match workspace_proof(&store, &actor, ResourceAction::WorkspaceMemberList, &red)
            .await
        {
            ProofResolution::Authorized(proof) => proof,
            ProofResolution::Denied(decision) => panic!("unexpected denial: {decision:?}"),
        };
        let response = service
            .workspace_list(
                &actor,
                &proof,
                WorkspaceMemberListParams {
                    workspace_id: red,
                    cursor: None,
                    limit: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            response
                .members
                .iter()
                .map(|member| member.principal_id.as_str())
                .collect::<Vec<_>>(),
            vec![MEMBER_A_ID, MEMBER_B_ID]
        );
        assert!(
            response
                .members
                .iter()
                .all(|member| member.kind == PrincipalKind::User)
        );
        harness
            .database
            .execute_unprepared(
                "DELETE FROM workspace_membership \
                 WHERE principal_id='P0000000000000000000A' \
                   AND workspace_id='W00000000000000000001'",
            )
            .await
            .unwrap();
        assert!(matches!(
            service
                .workspace_list(
                    &actor,
                    &proof,
                    WorkspaceMemberListParams {
                        workspace_id: WorkspaceId::new("W00000000000000000001").unwrap(),
                        cursor: None,
                        limit: None,
                    },
                )
                .await,
            Err(MemberServiceError::Authorization(_))
        ));
        let green = WorkspaceId::new("W00000000000000000003").unwrap();
        assert!(matches!(
            workspace_proof(&store, &actor, ResourceAction::WorkspaceMemberList, &green,).await,
            ProofResolution::Denied(_)
        ));
    }

    #[tokio::test]
    async fn direct_add_by_stable_id_is_idempotent_and_creates_only_membership_and_one_audit() {
        let harness = IsolatedEpic4Harness::populated().await.unwrap();
        let store = CrudStore::new(harness.database.clone());
        let target = PrincipalId::new("P0000000000000000000E").unwrap();
        pioneer_crud::create_member_principal(
            &harness.database,
            pioneer_crud::NewMemberPrincipalRow {
                id: target.clone(),
                gateway_id: GatewayId::new("G00000000000000000001").unwrap(),
                role_key: RoleKey::member(),
                display_name: "Hidden Existing Member".to_owned(),
                nickname: "hidden-existing".to_owned(),
                nickname_key: "hidden-existing".to_owned(),
                now: chrono::Utc::now().fixed_offset(),
            },
        )
        .await
        .unwrap();
        let service = MemberService::new(
            store.clone(),
            Arc::new(GatewaySecrets::new(Arc::new(MemorySecretStore::new()))),
        );
        let actor = member_a();
        assert!(matches!(
            avatar_proof(&store, &actor, &target).await,
            ProofResolution::Denied(_)
        ));
        let blue = WorkspaceId::new("W00000000000000000002").unwrap();
        let proof = match workspace_proof(&store, &actor, ResourceAction::WorkspaceMemberAdd, &blue)
            .await
        {
            ProofResolution::Authorized(proof) => proof,
            ProofResolution::Denied(decision) => panic!("unexpected denial: {decision:?}"),
        };
        let principals_before = gateway_principal::Entity::find()
            .all(&harness.database)
            .await
            .unwrap()
            .len();
        let devices_before = device::Entity::find()
            .all(&harness.database)
            .await
            .unwrap()
            .len();
        let sessions_before = auth_session::Entity::find()
            .all(&harness.database)
            .await
            .unwrap()
            .len();
        for expected_changed in [true, false] {
            let response = service
                .workspace_add(
                    &actor,
                    &proof,
                    WorkspaceMemberAddParams {
                        workspace_id: blue.clone(),
                        principal_id: target.clone(),
                    },
                )
                .await
                .unwrap();
            assert_eq!(response.changed, expected_changed);
            assert_eq!(response.member.principal_id, target);
        }
        assert!(
            pioneer_crud::find_workspace_membership(&harness.database, &target, blue.as_str())
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(
            audit_event::Entity::find()
                .filter(audit_event::Column::Action.eq("workspace_member_added"))
                .all(&harness.database)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            gateway_principal::Entity::find()
                .all(&harness.database)
                .await
                .unwrap()
                .len(),
            principals_before
        );
        assert_eq!(
            device::Entity::find()
                .all(&harness.database)
                .await
                .unwrap()
                .len(),
            devices_before
        );
        assert_eq!(
            auth_session::Entity::find()
                .all(&harness.database)
                .await
                .unwrap()
                .len(),
            sessions_before
        );
        assert!(
            invitation::Entity::find()
                .all(&harness.database)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(matches!(
            avatar_proof(&store, &actor, &target).await,
            ProofResolution::Authorized(_)
        ));
    }

    #[tokio::test]
    async fn direct_add_reauthorizes_actor_status_inside_the_transaction() {
        let harness = IsolatedEpic4Harness::populated().await.unwrap();
        let store = CrudStore::new(harness.database.clone());
        let service = MemberService::new(
            store.clone(),
            Arc::new(GatewaySecrets::new(Arc::new(MemorySecretStore::new()))),
        );
        let actor = member_a();
        let target = PrincipalId::new("P0000000000000000000E").unwrap();
        pioneer_crud::create_member_principal(
            &harness.database,
            pioneer_crud::NewMemberPrincipalRow {
                id: target.clone(),
                gateway_id: actor.gateway_id.clone(),
                role_key: RoleKey::member(),
                display_name: "Hidden Existing Member".to_owned(),
                nickname: "hidden-existing".to_owned(),
                nickname_key: "hidden-existing".to_owned(),
                now: chrono::Utc::now().fixed_offset(),
            },
        )
        .await
        .unwrap();
        let workspace_id = WorkspaceId::new("W00000000000000000002").unwrap();
        let proof = workspace_proof(
            &store,
            &actor,
            ResourceAction::WorkspaceMemberAdd,
            &workspace_id,
        )
        .await
        .into_authorized()
        .expect("actor initially has workspace access");

        harness
            .database
            .execute_unprepared(
                "UPDATE gateway_principal SET status='suspended' \
                 WHERE id='P0000000000000000000A'",
            )
            .await
            .unwrap();

        let error = service
            .workspace_add(
                &actor,
                &proof,
                WorkspaceMemberAddParams {
                    workspace_id: workspace_id.clone(),
                    principal_id: target.clone(),
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            MemberServiceError::Authorization(AuthorizationDecision::Deny {
                reason: DenyReason::InactivePrincipal,
                disclosure: DisclosurePolicy::AuthenticationTerminal,
            })
        ));
        assert!(
            pioneer_crud::find_workspace_membership(
                &harness.database,
                &target,
                workspace_id.as_str()
            )
            .await
            .unwrap()
            .is_none()
        );
        assert!(
            audit_event::Entity::find()
                .filter(audit_event::Column::Action.eq("workspace_member_added"))
                .all(&harness.database)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn superuser_mutations_reject_a_proof_after_the_actor_session_is_revoked() {
        let harness = IsolatedEpic4Harness::populated().await.unwrap();
        materialize_superuser_session(&harness).await;
        let store = CrudStore::new(harness.database.clone());
        let service = MemberService::new(
            store.clone(),
            Arc::new(GatewaySecrets::new(Arc::new(MemorySecretStore::new()))),
        );
        let actor = superuser();
        let target = PrincipalId::new(MEMBER_B_ID).unwrap();
        let workspace_id = WorkspaceId::new("W00000000000000000001").unwrap();
        let suspend_proof =
            member_principal_proof(&store, &actor, ResourceAction::MemberSuspend, &target)
                .await
                .into_authorized()
                .expect("initial suspension proof");
        let restore_proof =
            member_principal_proof(&store, &actor, ResourceAction::MemberRestore, &target)
                .await
                .into_authorized()
                .expect("initial restore proof");
        let remove_proof =
            member_principal_proof(&store, &actor, ResourceAction::MemberRemove, &target)
                .await
                .into_authorized()
                .expect("initial removal proof");
        let workspace_remove_proof = workspace_proof(
            &store,
            &actor,
            ResourceAction::WorkspaceMemberRemove,
            &workspace_id,
        )
        .await
        .into_authorized()
        .expect("initial workspace removal proof");

        harness
            .database
            .execute_unprepared(
                "UPDATE auth_session
                 SET status='revoked', revoked_at=CURRENT_TIMESTAMP, revoke_reason='logout'
                 WHERE id='S00000000000000000001'",
            )
            .await
            .unwrap();

        let errors = [
            service
                .suspend(
                    &actor,
                    &suspend_proof,
                    MemberSuspendParams {
                        principal_id: target.clone(),
                        expected_status: None,
                    },
                )
                .await
                .unwrap_err(),
            service
                .restore(
                    &actor,
                    &restore_proof,
                    MemberRestoreParams {
                        principal_id: target.clone(),
                        expected_status: None,
                    },
                )
                .await
                .unwrap_err(),
            service
                .remove(
                    &actor,
                    &remove_proof,
                    MemberRemoveParams {
                        principal_id: target.clone(),
                        expected_status: None,
                    },
                )
                .await
                .unwrap_err(),
            service
                .workspace_remove(
                    &actor,
                    &workspace_remove_proof,
                    WorkspaceMemberRemoveParams {
                        workspace_id: workspace_id.clone(),
                        principal_id: target.clone(),
                    },
                )
                .await
                .unwrap_err(),
        ];
        assert!(errors.into_iter().all(|error| matches!(
            error,
            MemberServiceError::Authorization(AuthorizationDecision::Deny {
                reason: DenyReason::InactivePrincipal,
                disclosure: DisclosurePolicy::AuthenticationTerminal,
            })
        )));
        assert_eq!(
            pioneer_crud::load_principal_by_id(&harness.database, &target)
                .await
                .unwrap()
                .unwrap()
                .status,
            PrincipalStatus::Active
        );
        assert!(
            pioneer_crud::find_workspace_membership(
                &harness.database,
                &target,
                workspace_id.as_str(),
            )
            .await
            .unwrap()
            .is_some()
        );
        assert!(
            audit_event::Entity::find()
                .all(&harness.database)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn concurrent_direct_add_and_membership_remove_follow_commit_order_once() {
        let harness = IsolatedEpic4Harness::populated().await.unwrap();
        materialize_superuser_session(&harness).await;
        let store = CrudStore::new(harness.database.clone());
        let service = MemberService::new(
            store.clone(),
            Arc::new(GatewaySecrets::new(Arc::new(MemorySecretStore::new()))),
        );
        let actor = superuser();
        let target = PrincipalId::new("P0000000000000000000E").unwrap();
        pioneer_crud::create_member_principal(
            &harness.database,
            pioneer_crud::NewMemberPrincipalRow {
                id: target.clone(),
                gateway_id: actor.gateway_id.clone(),
                role_key: RoleKey::member(),
                display_name: "Concurrent Existing Member".to_owned(),
                nickname: "concurrent-existing".to_owned(),
                nickname_key: "concurrent-existing".to_owned(),
                now: chrono::Utc::now().fixed_offset(),
            },
        )
        .await
        .unwrap();
        let workspace_id = WorkspaceId::new("W00000000000000000001").unwrap();
        let add_proof = workspace_proof(
            &store,
            &actor,
            ResourceAction::WorkspaceMemberAdd,
            &workspace_id,
        )
        .await
        .into_authorized()
        .expect("workspace add proof");
        let remove_proof = workspace_proof(
            &store,
            &actor,
            ResourceAction::WorkspaceMemberRemove,
            &workspace_id,
        )
        .await
        .into_authorized()
        .expect("workspace remove proof");

        let add = service.workspace_add(
            &actor,
            &add_proof,
            WorkspaceMemberAddParams {
                workspace_id: workspace_id.clone(),
                principal_id: target.clone(),
            },
        );
        let remove = service.workspace_remove(
            &actor,
            &remove_proof,
            WorkspaceMemberRemoveParams {
                workspace_id: workspace_id.clone(),
                principal_id: target.clone(),
            },
        );
        let (added, removed) = tokio::join!(add, remove);
        let added = added.unwrap();
        let removed = removed.unwrap();
        assert!(added.changed);
        let membership_exists = pioneer_crud::find_workspace_membership(
            &harness.database,
            &target,
            workspace_id.as_str(),
        )
        .await
        .unwrap()
        .is_some();
        assert_eq!(membership_exists, !removed.response.changed);
        assert_eq!(
            audit_event::Entity::find()
                .filter(audit_event::Column::Action.eq("workspace_member_added"))
                .all(&harness.database)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            audit_event::Entity::find()
                .filter(audit_event::Column::Action.eq("workspace_member_removed"))
                .all(&harness.database)
                .await
                .unwrap()
                .len(),
            usize::from(removed.response.changed)
        );
    }

    #[tokio::test]
    async fn direct_add_distinguishes_unavailable_id_from_invalid_loaded_target() {
        let harness = IsolatedEpic4Harness::populated().await.unwrap();
        let store = CrudStore::new(harness.database.clone());
        let service = MemberService::new(
            store.clone(),
            Arc::new(GatewaySecrets::new(Arc::new(MemorySecretStore::new()))),
        );
        let actor = member_a();
        let workspace_id = WorkspaceId::new("W00000000000000000002").unwrap();
        let proof = workspace_proof(
            &store,
            &actor,
            ResourceAction::WorkspaceMemberAdd,
            &workspace_id,
        )
        .await
        .into_authorized()
        .expect("actor has workspace access");

        let unavailable = service
            .workspace_add(
                &actor,
                &proof,
                WorkspaceMemberAddParams {
                    workspace_id: workspace_id.clone(),
                    principal_id: PrincipalId::new("P0000000000000000000F").unwrap(),
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(unavailable, MemberServiceError::TargetUnavailable));

        let invalid = service
            .workspace_add(
                &actor,
                &proof,
                WorkspaceMemberAddParams {
                    workspace_id,
                    principal_id: PrincipalId::new(crate::auth::test_support::TEST_SUPERUSER_ID)
                        .unwrap(),
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(invalid, MemberServiceError::InvalidTarget));
    }

    #[tokio::test]
    async fn superuser_remove_is_atomic_idempotent_and_preserves_unrelated_member_state() {
        let harness = IsolatedEpic4Harness::populated().await.unwrap();
        materialize_superuser_session(&harness).await;
        let store = CrudStore::new(harness.database.clone());
        let service = MemberService::new(
            store.clone(),
            Arc::new(GatewaySecrets::new(Arc::new(MemorySecretStore::new()))),
        );
        let target = PrincipalId::new(MEMBER_B_ID).unwrap();
        let red = WorkspaceId::new("W00000000000000000001").unwrap();
        let actor = superuser();
        let proof = match workspace_proof(
            &store,
            &actor,
            ResourceAction::WorkspaceMemberRemove,
            &red,
        )
        .await
        {
            ProofResolution::Authorized(proof) => proof,
            ProofResolution::Denied(decision) => panic!("unexpected denial: {decision:?}"),
        };
        assert!(matches!(
            workspace_proof(
                &store,
                &member_a(),
                ResourceAction::WorkspaceMemberRemove,
                &red,
            )
            .await,
            ProofResolution::Denied(_)
        ));
        let principal_before = gateway_principal::Entity::find_by_id(target.to_string())
            .one(&harness.database)
            .await
            .unwrap()
            .unwrap();
        let session_count_before = auth_session::Entity::find()
            .filter(auth_session::Column::PrincipalId.eq(target.to_string()))
            .all(&harness.database)
            .await
            .unwrap()
            .len();

        for expected_changed in [true, false] {
            let committed = service
                .workspace_remove(
                    &actor,
                    &proof,
                    WorkspaceMemberRemoveParams {
                        workspace_id: red.clone(),
                        principal_id: target.clone(),
                    },
                )
                .await
                .unwrap();
            assert_eq!(committed.response.changed, expected_changed);
            assert_eq!(committed.response.member.principal_id, target);
            assert_eq!(
                committed.removed_private_thread_ids,
                if expected_changed {
                    vec!["T00000000000000000002".to_owned()]
                } else {
                    Vec::new()
                }
            );
        }
        assert!(
            workspace_membership::Entity::find_by_id((target.to_string(), red.to_string()))
                .one(&harness.database)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            thread_membership::Entity::find_by_id((
                "T00000000000000000002".to_owned(),
                target.to_string(),
            ))
            .one(&harness.database)
            .await
            .unwrap()
            .is_none()
        );
        assert!(
            workspace_membership::Entity::find_by_id((
                target.to_string(),
                "W00000000000000000003".to_owned(),
            ))
            .one(&harness.database)
            .await
            .unwrap()
            .is_some()
        );
        assert!(
            thread_membership::Entity::find_by_id((
                "T00000000000000000006".to_owned(),
                target.to_string(),
            ))
            .one(&harness.database)
            .await
            .unwrap()
            .is_some()
        );
        assert_eq!(
            gateway_principal::Entity::find_by_id(target.to_string())
                .one(&harness.database)
                .await
                .unwrap()
                .unwrap(),
            principal_before
        );
        assert_eq!(
            auth_session::Entity::find()
                .filter(auth_session::Column::PrincipalId.eq(target.to_string()))
                .all(&harness.database)
                .await
                .unwrap()
                .len(),
            session_count_before
        );
        assert_eq!(
            audit_event::Entity::find()
                .filter(audit_event::Column::Action.eq("workspace_member_removed"))
                .all(&harness.database)
                .await
                .unwrap()
                .len(),
            1
        );
        assert!(matches!(
            avatar_proof(&store, &member_a(), &target).await,
            ProofResolution::Denied(_)
        ));
    }

    #[tokio::test]
    async fn existing_same_gateway_superuser_target_returns_safe_invalid_target() {
        let harness = IsolatedEpic4Harness::populated().await.unwrap();
        materialize_superuser_session(&harness).await;
        let store = CrudStore::new(harness.database.clone());
        let service = MemberService::new(
            store.clone(),
            Arc::new(GatewaySecrets::new(Arc::new(MemorySecretStore::new()))),
        );
        let actor = superuser();
        let target = actor.principal_id.clone();
        let proof =
            match member_principal_proof(&store, &actor, ResourceAction::MemberSuspend, &target)
                .await
            {
                ProofResolution::Authorized(proof) => proof,
                ProofResolution::Denied(decision) => panic!("unexpected denial: {decision:?}"),
            };

        assert!(matches!(
            service
                .suspend(
                    &actor,
                    &proof,
                    MemberSuspendParams {
                        principal_id: target,
                        expected_status: None,
                    },
                )
                .await,
            Err(MemberServiceError::InvalidTarget)
        ));
    }

    #[tokio::test]
    async fn suspend_revokes_credentials_once_and_preserves_identity_acl_and_history() {
        let harness = IsolatedEpic4Harness::populated().await.unwrap();
        materialize_superuser_session(&harness).await;
        let store = CrudStore::new(harness.database.clone());
        let service = MemberService::new(
            store.clone(),
            Arc::new(GatewaySecrets::new(Arc::new(MemorySecretStore::new()))),
        );
        let actor = superuser();
        let target = PrincipalId::new(MEMBER_B_ID).unwrap();
        let avatar_revision = seed_avatar(&harness.database, &target).await;
        let proof =
            match member_principal_proof(&store, &actor, ResourceAction::MemberSuspend, &target)
                .await
            {
                ProofResolution::Authorized(proof) => proof,
                ProofResolution::Denied(decision) => panic!("unexpected denial: {decision:?}"),
            };
        assert!(matches!(
            member_principal_proof(&store, &member_a(), ResourceAction::MemberSuspend, &target,)
                .await,
            ProofResolution::Denied(_)
        ));
        let before = pioneer_crud::load_principal_by_id(&harness.database, &target)
            .await
            .unwrap()
            .unwrap();
        let workspace_rows_before = workspace_membership::Entity::find()
            .filter(workspace_membership::Column::PrincipalId.eq(target.to_string()))
            .all(&harness.database)
            .await
            .unwrap();
        let thread_rows_before = thread_membership::Entity::find()
            .filter(thread_membership::Column::PrincipalId.eq(target.to_string()))
            .all(&harness.database)
            .await
            .unwrap();

        assert!(matches!(
            service
                .suspend(
                    &actor,
                    &proof,
                    MemberSuspendParams {
                        principal_id: target.clone(),
                        expected_status: Some(PrincipalStatus::Suspended),
                    },
                )
                .await,
            Err(MemberServiceError::Conflict(
                MemberManagementErrorReason::Conflict
            ))
        ));
        assert!(
            audit_event::Entity::find()
                .filter(audit_event::Column::Action.eq("member_suspended"))
                .all(&harness.database)
                .await
                .unwrap()
                .is_empty()
        );

        for expected_changed in [true, false] {
            let committed = service
                .suspend(
                    &actor,
                    &proof,
                    MemberSuspendParams {
                        principal_id: target.clone(),
                        expected_status: None,
                    },
                )
                .await
                .unwrap();
            assert_eq!(committed.response.changed, expected_changed);
            assert_eq!(committed.response.member.status, PrincipalStatus::Suspended);
            assert_eq!(
                committed.response.member.avatar_revision.as_deref(),
                Some(avatar_revision.as_str())
            );
            assert_eq!(
                committed.revoked_session_ids.len(),
                usize::from(expected_changed)
            );
            assert_eq!(
                committed.revoked_device_ids.len(),
                usize::from(expected_changed)
            );
        }
        let after = pioneer_crud::load_principal_by_id(&harness.database, &target)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after.status, PrincipalStatus::Suspended);
        assert_eq!(after.display_name, before.display_name);
        assert_eq!(after.nickname, before.nickname);
        assert_eq!(after.nickname_key, before.nickname_key);
        assert_eq!(after.role_key, before.role_key);
        assert_eq!(after.removed_at, None);
        assert_eq!(
            workspace_membership::Entity::find()
                .filter(workspace_membership::Column::PrincipalId.eq(target.to_string()))
                .all(&harness.database)
                .await
                .unwrap(),
            workspace_rows_before
        );
        assert_eq!(
            thread_membership::Entity::find()
                .filter(thread_membership::Column::PrincipalId.eq(target.to_string()))
                .all(&harness.database)
                .await
                .unwrap(),
            thread_rows_before
        );
        let target_sessions = auth_session::Entity::find()
            .filter(auth_session::Column::PrincipalId.eq(target.to_string()))
            .all(&harness.database)
            .await
            .unwrap();
        assert!(target_sessions.iter().all(|session| {
            session.status == "revoked"
                && session.revoke_reason.as_deref() == Some("principal_suspended")
        }));
        assert!(
            device::Entity::find()
                .filter(device::Column::PrincipalId.eq(target.to_string()))
                .all(&harness.database)
                .await
                .unwrap()
                .iter()
                .all(|device| device.status == "revoked")
        );
        assert!(
            auth_refresh_credential::Entity::find()
                .filter(
                    auth_refresh_credential::Column::SessionId.is_in(
                        target_sessions
                            .iter()
                            .map(|session| session.id.clone())
                            .collect::<Vec<_>>(),
                    )
                )
                .all(&harness.database)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            audit_event::Entity::find()
                .filter(audit_event::Column::Action.eq("member_suspended"))
                .all(&harness.database)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn restore_reactivates_only_principal_and_never_revives_old_credentials() {
        let harness = IsolatedEpic4Harness::populated().await.unwrap();
        materialize_superuser_session(&harness).await;
        let store = CrudStore::new(harness.database.clone());
        let service = MemberService::new(
            store.clone(),
            Arc::new(GatewaySecrets::new(Arc::new(MemorySecretStore::new()))),
        );
        let actor = superuser();
        let target = PrincipalId::new(MEMBER_B_ID).unwrap();
        let avatar_revision = seed_avatar(&harness.database, &target).await;
        let suspend_proof =
            match member_principal_proof(&store, &actor, ResourceAction::MemberSuspend, &target)
                .await
            {
                ProofResolution::Authorized(proof) => proof,
                ProofResolution::Denied(decision) => panic!("unexpected denial: {decision:?}"),
            };
        service
            .suspend(
                &actor,
                &suspend_proof,
                MemberSuspendParams {
                    principal_id: target.clone(),
                    expected_status: Some(PrincipalStatus::Active),
                },
            )
            .await
            .unwrap();
        let restore_proof =
            match member_principal_proof(&store, &actor, ResourceAction::MemberRestore, &target)
                .await
            {
                ProofResolution::Authorized(proof) => proof,
                ProofResolution::Denied(decision) => panic!("unexpected denial: {decision:?}"),
            };
        for expected_changed in [true, false] {
            let committed = service
                .restore(
                    &actor,
                    &restore_proof,
                    MemberRestoreParams {
                        principal_id: target.clone(),
                        expected_status: None,
                    },
                )
                .await
                .unwrap();
            assert_eq!(committed.response.changed, expected_changed);
            assert_eq!(committed.response.member.status, PrincipalStatus::Active);
            assert_eq!(
                committed.response.member.avatar_revision.as_deref(),
                Some(avatar_revision.as_str())
            );
            assert!(committed.revoked_session_ids.is_empty());
            assert!(committed.revoked_device_ids.is_empty());
        }
        assert!(
            auth_session::Entity::find()
                .filter(auth_session::Column::PrincipalId.eq(target.to_string()))
                .all(&harness.database)
                .await
                .unwrap()
                .iter()
                .all(|session| session.status == "revoked")
        );
        assert!(
            device::Entity::find()
                .filter(device::Column::PrincipalId.eq(target.to_string()))
                .all(&harness.database)
                .await
                .unwrap()
                .iter()
                .all(|device| device.status == "revoked")
        );
        assert_eq!(
            audit_event::Entity::find()
                .filter(audit_event::Column::Action.eq("member_restored"))
                .all(&harness.database)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn remove_atomically_terminates_authority_and_preserves_identity_and_history() {
        let harness = IsolatedEpic4Harness::populated().await.unwrap();
        materialize_superuser_session(&harness).await;
        let store = CrudStore::new(harness.database.clone());
        let service = MemberService::new(
            store.clone(),
            Arc::new(GatewaySecrets::new(Arc::new(MemorySecretStore::new()))),
        );
        let actor = superuser();
        let target = PrincipalId::new(MEMBER_B_ID).unwrap();
        let avatar_revision = seed_avatar(&harness.database, &target).await;
        let target_before = pioneer_crud::load_principal_by_id(&harness.database, &target)
            .await
            .unwrap()
            .unwrap();
        let target_session = auth_session::Entity::find()
            .filter(auth_session::Column::PrincipalId.eq(target.to_string()))
            .one(&harness.database)
            .await
            .unwrap()
            .unwrap();
        let target_session_id = AuthSessionId::new(target_session.id.clone()).unwrap();
        let target_device_id = DeviceId::new(target_session.device_id.clone()).unwrap();
        let now = chrono::Utc::now().fixed_offset();
        let pending_id = InvitationId::new(generate_id(ADMINISTRATION_DOMAIN_ID_LEN)).unwrap();
        let accepted_id = InvitationId::new(generate_id(ADMINISTRATION_DOMAIN_ID_LEN)).unwrap();
        let pending_hash = [0x51; 32];
        let accepted_hash = [0x52; 32];
        let transaction = harness.database.begin().await.unwrap();
        for (invitation_id, token_hash) in [
            (pending_id.clone(), pending_hash),
            (accepted_id.clone(), accepted_hash),
        ] {
            pioneer_crud::insert_invitation(
                &transaction,
                NewInvitationRow {
                    invitation_id: invitation_id.clone(),
                    gateway_id: actor.gateway_id.clone(),
                    created_by_principal_id: target.clone(),
                    created_by_session_id: target_session_id.clone(),
                    target_role_key: RoleKey::member(),
                    token_hash,
                    expires_at: now + chrono::Duration::days(7),
                    now,
                },
            )
            .await
            .unwrap();
            pioneer_crud::insert_invitation_grants(
                &transaction,
                &invitation_id,
                &[WorkspaceId::new("W00000000000000000001").unwrap()],
                now,
            )
            .await
            .unwrap();
        }
        assert!(matches!(
            pioneer_crud::transition_pending_to_accepted(
                &transaction,
                &accepted_id,
                &accepted_hash,
                &target,
                &target_device_id,
                &target_session_id,
                now,
            )
            .await
            .unwrap(),
            pioneer_crud::InvitationTransitionOutcome::Applied(_)
        ));
        transaction.commit().await.unwrap();

        let proof =
            match member_principal_proof(&store, &actor, ResourceAction::MemberRemove, &target)
                .await
            {
                ProofResolution::Authorized(proof) => proof,
                ProofResolution::Denied(decision) => panic!("unexpected denial: {decision:?}"),
            };
        assert!(matches!(
            member_principal_proof(&store, &member_a(), ResourceAction::MemberRemove, &target)
                .await,
            ProofResolution::Denied(_)
        ));
        let committed = service
            .remove(
                &actor,
                &proof,
                MemberRemoveParams {
                    principal_id: target.clone(),
                    expected_status: Some(PrincipalStatus::Active),
                },
            )
            .await
            .unwrap();
        assert!(committed.response.changed);
        assert_eq!(committed.response.member.status, PrincipalStatus::Removed);
        assert_eq!(
            committed.response.member.avatar_revision.as_deref(),
            Some(avatar_revision.as_str())
        );
        assert!(!committed.revoked_session_ids.is_empty());
        assert!(!committed.revoked_device_ids.is_empty());
        assert!(!committed.removed_workspace_ids.is_empty());
        assert!(!committed.removed_private_thread_ids.is_empty());
        assert_eq!(committed.changed_invitation_ids, vec![pending_id.clone()]);

        let target_after = pioneer_crud::load_principal_by_id(&harness.database, &target)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(target_after.status, PrincipalStatus::Removed);
        assert!(target_after.removed_at.is_some());
        assert_eq!(target_after.display_name, target_before.display_name);
        assert_eq!(target_after.nickname, target_before.nickname);
        assert_eq!(target_after.nickname_key, target_before.nickname_key);
        assert_eq!(target_after.role_key, target_before.role_key);
        assert!(
            pioneer_crud::load_principal_avatar(&harness.database, &target)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            workspace_membership::Entity::find()
                .filter(workspace_membership::Column::PrincipalId.eq(target.to_string()))
                .all(&harness.database)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            thread_membership::Entity::find()
                .filter(thread_membership::Column::PrincipalId.eq(target.to_string()))
                .all(&harness.database)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            auth_session::Entity::find()
                .filter(auth_session::Column::PrincipalId.eq(target.to_string()))
                .all(&harness.database)
                .await
                .unwrap()
                .iter()
                .all(|session| session.status == "revoked"
                    && session.revoke_reason.as_deref() == Some("principal_removed"))
        );
        assert!(
            device::Entity::find()
                .filter(device::Column::PrincipalId.eq(target.to_string()))
                .all(&harness.database)
                .await
                .unwrap()
                .iter()
                .all(|device| device.status == "revoked")
        );
        let pending = invitation::Entity::find_by_id(pending_id.to_string())
            .one(&harness.database)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(pending.status, "revoked");
        assert_eq!(
            pending.revoke_reason.as_deref(),
            Some("inviter_unavailable")
        );
        assert!(pending.token_hash.is_none());
        let accepted = invitation::Entity::find_by_id(accepted_id.to_string())
            .one(&harness.database)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(accepted.status, "accepted");
        assert_eq!(
            accepted.accepted_principal_id.as_deref(),
            Some(target.as_str())
        );
        assert!(accepted.token_hash.is_none());
        assert_eq!(
            audit_event::Entity::find()
                .filter(audit_event::Column::Action.eq("member_removed"))
                .all(&harness.database)
                .await
                .unwrap()
                .len(),
            1
        );
        let repeated_proof =
            match member_principal_proof(&store, &actor, ResourceAction::MemberRemove, &target)
                .await
            {
                ProofResolution::Authorized(proof) => proof,
                ProofResolution::Denied(decision) => panic!("unexpected denial: {decision:?}"),
            };
        let repeated = service
            .remove(
                &actor,
                &repeated_proof,
                MemberRemoveParams {
                    principal_id: target.clone(),
                    expected_status: Some(PrincipalStatus::Removed),
                },
            )
            .await
            .unwrap();
        assert!(!repeated.response.changed);
        assert_eq!(repeated.response.member.status, PrincipalStatus::Removed);
        assert!(repeated.revoked_session_ids.is_empty());
        assert!(repeated.revoked_device_ids.is_empty());
        assert!(repeated.removed_workspace_ids.is_empty());
        assert!(repeated.removed_private_thread_ids.is_empty());
        assert!(repeated.changed_invitation_ids.is_empty());
        assert_eq!(
            audit_event::Entity::find()
                .filter(audit_event::Column::Action.eq("member_removed"))
                .all(&harness.database)
                .await
                .unwrap()
                .len(),
            1
        );
        let removed_restore_proof =
            match member_principal_proof(&store, &actor, ResourceAction::MemberRestore, &target)
                .await
            {
                ProofResolution::Authorized(proof) => proof,
                ProofResolution::Denied(decision) => panic!("unexpected denial: {decision:?}"),
            };
        assert!(matches!(
            service
                .restore(
                    &actor,
                    &removed_restore_proof,
                    MemberRestoreParams {
                        principal_id: target,
                        expected_status: None,
                    },
                )
                .await,
            Err(MemberServiceError::InvalidTarget)
        ));
    }

    #[tokio::test]
    async fn suspended_member_can_be_terminally_removed_without_reviving_credentials() {
        let harness = IsolatedEpic4Harness::populated().await.unwrap();
        materialize_superuser_session(&harness).await;
        let store = CrudStore::new(harness.database.clone());
        let service = MemberService::new(
            store.clone(),
            Arc::new(GatewaySecrets::new(Arc::new(MemorySecretStore::new()))),
        );
        let actor = superuser();
        let target = PrincipalId::new(MEMBER_B_ID).unwrap();
        let suspend_proof =
            match member_principal_proof(&store, &actor, ResourceAction::MemberSuspend, &target)
                .await
            {
                ProofResolution::Authorized(proof) => proof,
                ProofResolution::Denied(decision) => panic!("unexpected denial: {decision:?}"),
            };
        service
            .suspend(
                &actor,
                &suspend_proof,
                MemberSuspendParams {
                    principal_id: target.clone(),
                    expected_status: Some(PrincipalStatus::Active),
                },
            )
            .await
            .unwrap();
        let remove_proof =
            match member_principal_proof(&store, &actor, ResourceAction::MemberRemove, &target)
                .await
            {
                ProofResolution::Authorized(proof) => proof,
                ProofResolution::Denied(decision) => panic!("unexpected denial: {decision:?}"),
            };
        let committed = service
            .remove(
                &actor,
                &remove_proof,
                MemberRemoveParams {
                    principal_id: target.clone(),
                    expected_status: Some(PrincipalStatus::Suspended),
                },
            )
            .await
            .unwrap();
        assert_eq!(committed.response.member.status, PrincipalStatus::Removed);
        assert!(committed.revoked_session_ids.is_empty());
        assert!(committed.revoked_device_ids.is_empty());
        assert!(
            auth_session::Entity::find()
                .filter(auth_session::Column::PrincipalId.eq(target.to_string()))
                .all(&harness.database)
                .await
                .unwrap()
                .iter()
                .all(|session| session.status == "revoked")
        );
        assert_eq!(
            audit_event::Entity::find()
                .filter(audit_event::Column::Action.eq("member_removed"))
                .all(&harness.database)
                .await
                .unwrap()
                .len(),
            1
        );
    }
}
