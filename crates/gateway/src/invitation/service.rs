use std::ops::Deref;
use std::sync::Arc;

use anyhow::{Context, Error};
use pioneer_crud::{
    CrudStore, InvitationProjectionRows, InvitationTransitionOutcome, NewInvitationRow,
    NewMemberPrincipalRow, NewPrincipalAvatarRow, NewWorkspaceMembership,
};
use pioneer_protocol::{
    ADMINISTRATION_DOMAIN_ID_LEN, AUTH_DOMAIN_ID_LEN, GatewayBaseUrl,
    INVITATION_MAX_WORKSPACE_GRANTS, InvitationAcceptParams, InvitationAcceptResponse,
    InvitationCreateParams, InvitationCreateResponse, InvitationCredential, InvitationErrorReason,
    InvitationId, InvitationInviterSummary, InvitationListParams, InvitationListResponse,
    InvitationPresentation, InvitationPreviewResponse, InvitationRevokeParams,
    InvitationRevokeReason, InvitationRevokeResponse, InvitationStatus, InvitationSummary,
    InvitationTransportSecurity, InvitationWorkspaceSummary, MemberSummary, PersistedActorRef,
    PioneerAppUrlScheme, PrincipalId, PrincipalKind, PrincipalStatus, RoleKey, WorkspaceId,
    generate_id,
};
use sea_orm::{
    DatabaseConnection, SqliteTransactionMode, TransactionOptions, TransactionTrait,
    entity::prelude::DateTimeWithTimeZone,
};

use crate::auth::{
    AuthenticatedSessionPrincipal, FirstMemberSessionIds, GatewayAuthService,
    InvitationAcceptCommitted, OpaqueCredentialFactory,
};
use crate::authorization::{
    AuthorizationDecision, AuthorizationResolver, AuthorizationService, AuthorizedInvitation,
    AuthorizedInvitationCollection, AuthorizedInvitationGrants, DenyReason, DisclosurePolicy,
    ProofResolution, ResourceAction, persisted_actor_is_current,
};
use crate::epic5_observability::{
    Epic5Operation, Epic5Outcome, Epic5RateLimits, MAX_LIVE_PENDING_INVITATIONS_PER_CREATOR,
    record_latency, record_outcome,
};
use crate::secrets::GatewaySecrets;

use super::{
    InvitationCredentialLookup, InvitationCredentialService, InvitationCursorCodec,
    ValidatedInvitationAccept, lookup_presented_with_factory, validate_accept_inputs,
};
use crate::administrative_audit::AdministrativeAuditWriter;

const INVITATION_CREDENTIAL_KEY_MIN_BYTES: usize = 32;

#[derive(Clone)]
pub(crate) struct InvitationService {
    store: CrudStore,
    secrets: Arc<GatewaySecrets>,
    gateway_base_url: GatewayBaseUrl,
    app_url_scheme: PioneerAppUrlScheme,
    audit: AdministrativeAuditWriter,
    rate_limits: Arc<Epic5RateLimits>,
}

#[derive(Debug)]
pub(crate) enum InvitationServiceError {
    InvalidParams,
    RateLimited,
    Authorization(AuthorizationDecision),
    CommittedTerminalHidden(InvitationId),
    Unavailable(Error),
}

#[derive(Debug)]
pub(crate) enum InvitationAcceptServiceError {
    Corrective(InvitationErrorReason),
    Unavailable,
    Contention,
    Storage(Error),
}

#[derive(Debug)]
pub(crate) struct InvitationListCommitted {
    pub(crate) response: InvitationListResponse,
    pub(crate) changed_invitation_ids: Vec<InvitationId>,
}

impl Deref for InvitationListCommitted {
    type Target = InvitationListResponse;

    fn deref(&self) -> &Self::Target {
        &self.response
    }
}

#[derive(Debug)]
pub(crate) struct InvitationRevokeCommitted {
    pub(crate) response: InvitationRevokeResponse,
    pub(crate) notification_changed: bool,
}

impl Deref for InvitationRevokeCommitted {
    type Target = InvitationRevokeResponse;

    fn deref(&self) -> &Self::Target {
        &self.response
    }
}

impl InvitationService {
    #[cfg(test)]
    pub(crate) async fn preview_restricted(
        database: &DatabaseConnection,
        credentials: &OpaqueCredentialFactory,
        gateway_id: &pioneer_protocol::GatewayId,
        raw_credential: &str,
        transport: InvitationTransportSecurity,
    ) -> Result<InvitationPreviewResponse, Error> {
        Self::preview_restricted_with_lifecycle(
            database,
            credentials,
            None,
            gateway_id,
            raw_credential,
            transport,
        )
        .await
    }

    pub(crate) async fn preview_restricted_with_lifecycle(
        database: &DatabaseConnection,
        credentials: &OpaqueCredentialFactory,
        auth_service: Option<&GatewayAuthService>,
        gateway_id: &pioneer_protocol::GatewayId,
        raw_credential: &str,
        transport: InvitationTransportSecurity,
    ) -> Result<InvitationPreviewResponse, Error> {
        let started = std::time::Instant::now();
        let now = chrono::Utc::now().fixed_offset();
        let transaction = database
            .begin_with_options(TransactionOptions {
                sqlite_transaction_mode: Some(SqliteTransactionMode::Immediate),
                ..Default::default()
            })
            .await
            .context("failed to begin invitation preview transaction")?;
        let result = preview_in_transaction(
            &transaction,
            credentials,
            gateway_id,
            raw_credential,
            transport,
            now,
        )
        .await;
        match result {
            Ok(PreviewTransactionOutcome::Available(preview)) => {
                transaction
                    .commit()
                    .await
                    .context("failed to commit invitation preview transaction")?;
                Ok(preview)
            }
            Ok(PreviewTransactionOutcome::Unavailable { terminal_change }) => {
                if let Some(change) = terminal_change {
                    transaction
                        .commit()
                        .await
                        .context("failed to commit invitation terminal transition")?;
                    record_outcome(change.operation, Epic5Outcome::Success);
                    record_latency(change.operation, started.elapsed());
                    if let Some(auth_service) = auth_service {
                        auth_service
                            .invitation_changed_committed(change.invitation_id)
                            .await;
                    }
                } else {
                    let _ = transaction.rollback().await;
                }
                Err(Error::msg("invitation unavailable"))
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    pub(crate) async fn accept_restricted(
        database: &DatabaseConnection,
        credentials: &OpaqueCredentialFactory,
        auth_service: &GatewayAuthService,
        gateway_id: &pioneer_protocol::GatewayId,
        raw_credential: &str,
        params: InvitationAcceptParams,
    ) -> Result<InvitationAcceptResponse, InvitationAcceptServiceError> {
        let started = std::time::Instant::now();
        let credential = InvitationCredential::parse(raw_credential.to_owned())
            .map_err(|_| InvitationAcceptServiceError::Unavailable)?;
        let expected_token_hash = credentials.fingerprint_invitation(&credential);
        preflight_accept_admission(
            database,
            credentials,
            auth_service,
            gateway_id,
            raw_credential,
        )
        .await?;
        let validated =
            validate_accept_inputs(params).map_err(InvitationAcceptServiceError::Corrective)?;
        let now = chrono::Utc::now().fixed_offset();
        let now_unix = u64::try_from(now.timestamp())
            .context("accept clock predates Unix epoch")
            .map_err(InvitationAcceptServiceError::Storage)?;
        let transaction = database
            .begin_with_options(TransactionOptions {
                sqlite_transaction_mode: Some(SqliteTransactionMode::Immediate),
                ..Default::default()
            })
            .await
            .context("failed to begin invitation accept transaction")
            .map_err(InvitationAcceptServiceError::Storage)?;
        let result = accept_in_transaction(
            &transaction,
            credentials,
            auth_service,
            gateway_id,
            raw_credential,
            &expected_token_hash,
            validated,
            now_unix,
            now,
        )
        .await;
        let accepted = match result {
            Ok(AcceptTransactionOutcome::Accepted(accepted)) => {
                transaction
                    .commit()
                    .await
                    .context("failed to commit invitation accept transaction")
                    .map_err(InvitationAcceptServiceError::Storage)?;
                accepted
            }
            Ok(AcceptTransactionOutcome::Unavailable { terminal_change }) => {
                if let Some(change) = terminal_change {
                    transaction
                        .commit()
                        .await
                        .context("failed to commit invitation terminal transition")
                        .map_err(InvitationAcceptServiceError::Storage)?;
                    record_outcome(change.operation, Epic5Outcome::Success);
                    record_latency(change.operation, started.elapsed());
                    auth_service
                        .invitation_changed_committed(change.invitation_id)
                        .await;
                } else {
                    let _ = transaction.rollback().await;
                }
                return Err(InvitationAcceptServiceError::Unavailable);
            }
            Ok(AcceptTransactionOutcome::Corrective(reason)) => {
                let _ = transaction.rollback().await;
                return Err(InvitationAcceptServiceError::Corrective(reason));
            }
            Ok(AcceptTransactionOutcome::Contended) => {
                let _ = transaction.rollback().await;
                return Err(InvitationAcceptServiceError::Contention);
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                return Err(InvitationAcceptServiceError::Storage(error));
            }
        };
        auth_service
            .invitation_accept_committed(accepted.committed.clone())
            .await;
        Ok(accepted.response)
    }

    #[cfg(test)]
    pub(crate) fn new(
        store: CrudStore,
        secrets: Arc<GatewaySecrets>,
        gateway_base_url: impl AsRef<str>,
    ) -> Result<Self, InvitationServiceError> {
        let gateway_base_url = GatewayBaseUrl::parse_presentation(gateway_base_url.as_ref())
            .map_err(|error| InvitationServiceError::Unavailable(Error::msg(error)))?;
        Self::with_rate_limits(
            store,
            secrets,
            gateway_base_url,
            Arc::new(Epic5RateLimits::default()),
        )
    }

    pub(crate) fn with_rate_limits(
        store: CrudStore,
        secrets: Arc<GatewaySecrets>,
        gateway_base_url: GatewayBaseUrl,
        rate_limits: Arc<Epic5RateLimits>,
    ) -> Result<Self, InvitationServiceError> {
        Ok(Self {
            store,
            secrets,
            gateway_base_url,
            app_url_scheme: PioneerAppUrlScheme::for_current_build(),
            audit: AdministrativeAuditWriter,
            rate_limits,
        })
    }

    pub(crate) async fn create(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        admission: &AuthorizedInvitationGrants,
        params: InvitationCreateParams,
    ) -> Result<InvitationCreateResponse, InvitationServiceError> {
        let started = std::time::Instant::now();
        let result = self.create_inner(principal, admission, params).await;
        let outcome = match &result {
            Ok(_) => Epic5Outcome::Success,
            Err(InvitationServiceError::InvalidParams) => Epic5Outcome::Invalid,
            Err(InvitationServiceError::RateLimited) => Epic5Outcome::RateLimited,
            Err(InvitationServiceError::Authorization(_)) => Epic5Outcome::Denied,
            Err(InvitationServiceError::CommittedTerminalHidden(_)) => Epic5Outcome::Denied,
            Err(InvitationServiceError::Unavailable(_)) => Epic5Outcome::Unavailable,
        };
        record_outcome(Epic5Operation::InvitationCreate, outcome);
        record_latency(Epic5Operation::InvitationCreate, started.elapsed());
        result
    }

    async fn create_inner(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        admission: &AuthorizedInvitationGrants,
        params: InvitationCreateParams,
    ) -> Result<InvitationCreateResponse, InvitationServiceError> {
        let params = InvitationCreateParams::new(params.workspace_ids)
            .map_err(|_| InvitationServiceError::InvalidParams)?;
        if admission.principal_id() != &principal.principal_id
            || admission.action() != ResourceAction::InvitationCreate
            || admission.workspace_ids()
                != params
                    .workspace_ids
                    .iter()
                    .map(WorkspaceId::as_str)
                    .collect::<Vec<_>>()
        {
            return Err(InvitationServiceError::Authorization(
                AuthorizationDecision::Deny {
                    reason: DenyReason::ResourceScopeMismatch,
                    disclosure: DisclosurePolicy::NotFound,
                },
            ));
        }
        if !self
            .rate_limits
            .allow_invitation_create(&principal.gateway_id, &principal.principal_id)
        {
            tracing::warn!(
                event = "epic5_rate_limited",
                operation = "invitation_create",
                actor_principal_id = %principal.principal_id,
                outcome = "rate_limited",
            );
            return Err(InvitationServiceError::RateLimited);
        }

        let key = self
            .secrets
            .load_or_create_auth_credential_hmac_key(INVITATION_CREDENTIAL_KEY_MIN_BYTES)
            .map_err(InvitationServiceError::Unavailable)?;
        let credentials =
            InvitationCredentialService::new(&key).map_err(InvitationServiceError::Unavailable)?;
        let issued = credentials.issue();
        let invitation_id = InvitationId::new(generate_id(ADMINISTRATION_DOMAIN_ID_LEN))
            .context("generated invalid invitation id")
            .map_err(InvitationServiceError::Unavailable)?;
        let now = chrono::Utc::now().fixed_offset();
        let expires_at = now
            .checked_add_signed(chrono::Duration::days(7))
            .context("invitation expiry overflow")
            .map_err(InvitationServiceError::Unavailable)?;
        let database = self.store.database_connection();
        let transaction = database
            .begin_with_options(TransactionOptions {
                sqlite_transaction_mode: Some(SqliteTransactionMode::Immediate),
                ..Default::default()
            })
            .await
            .context("failed to begin invitation create transaction")
            .map_err(InvitationServiceError::Unavailable)?;

        let result = async {
            let service = AuthorizationService::new();
            let gate = service.authorize_action(
                principal.kind,
                principal.role_key.as_ref(),
                ResourceAction::InvitationCreate,
            );
            let resolver = AuthorizationResolver::new(self.store.clone());
            let transaction_admission = resolver
                .authorize_invitation_grants(
                    &transaction,
                    principal,
                    &gate,
                    params.workspace_ids.as_slice(),
                )
                .await
                .context("failed to reauthorize invitation grants")?;
            match transaction_admission {
                ProofResolution::Authorized(_) => {}
                ProofResolution::Denied(decision) => {
                    record_outcome(Epic5Operation::GrantReauthorization, Epic5Outcome::Denied);
                    return Err(CreateTransactionError::Authorization(decision));
                }
            }

            if pioneer_crud::count_live_pending_invitations_for_creator(
                &transaction,
                &principal.gateway_id,
                &principal.principal_id,
                now,
            )
            .await?
                >= MAX_LIVE_PENDING_INVITATIONS_PER_CREATOR
            {
                return Err(CreateTransactionError::RateLimited);
            }

            let invitation = pioneer_crud::insert_invitation(
                &transaction,
                NewInvitationRow {
                    invitation_id: invitation_id.clone(),
                    gateway_id: principal.gateway_id.clone(),
                    created_by_principal_id: principal.principal_id.clone(),
                    created_by_session_id: principal.session_id.clone(),
                    token_hash: *issued.token_hash(),
                    expires_at,
                    now,
                },
            )
            .await?;
            pioneer_crud::insert_invitation_grants(
                &transaction,
                &invitation_id,
                params.workspace_ids.as_slice(),
                now,
            )
            .await?;
            self.audit
                .invitation_created(
                    &transaction,
                    &principal.gateway_id,
                    &principal.principal_id,
                    &principal.session_id,
                    &invitation_id,
                    params.workspace_ids.clone(),
                    now,
                )
                .await?;
            pioneer_crud::load_invitation_projection(&transaction, invitation)
                .await
                .map_err(Into::into)
        }
        .await;

        let projection = match result {
            Ok(projection) => {
                transaction
                    .commit()
                    .await
                    .context("failed to commit invitation create transaction")
                    .map_err(InvitationServiceError::Unavailable)?;
                projection
            }
            Err(CreateTransactionError::Authorization(decision)) => {
                let _ = transaction.rollback().await;
                return Err(InvitationServiceError::Authorization(decision));
            }
            Err(CreateTransactionError::RateLimited) => {
                let _ = transaction.rollback().await;
                return Err(InvitationServiceError::RateLimited);
            }
            Err(CreateTransactionError::Storage(error)) => {
                let _ = transaction.rollback().await;
                return Err(InvitationServiceError::Unavailable(error));
            }
        };

        let summary =
            invitation_summary(projection, now).map_err(InvitationServiceError::Unavailable)?;
        let presentation = InvitationPresentation::new_with_scheme(
            self.gateway_base_url.clone(),
            principal.gateway_id.clone(),
            issued.into_credential(),
            self.app_url_scheme,
        )
        .context("failed to construct canonical invitation presentation")
        .map_err(InvitationServiceError::Unavailable)?;
        let response = InvitationCreateResponse {
            invitation: summary,
            presentation,
        };
        Ok(response)
    }

    pub(crate) async fn list(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        admission: &AuthorizedInvitationCollection,
        params: InvitationListParams,
    ) -> Result<InvitationListCommitted, InvitationServiceError> {
        let started = std::time::Instant::now();
        if admission.principal_id() != &principal.principal_id
            || admission.action() != ResourceAction::InvitationList
        {
            return Err(scope_mismatch());
        }
        let limit = params
            .validate()
            .map_err(|_| InvitationServiceError::InvalidParams)?;
        let is_superuser = principal.kind == PrincipalKind::Superuser;
        if !is_superuser
            && (params
                .status
                .is_some_and(|status| status != InvitationStatus::Pending)
                || params
                    .creator_principal_id
                    .as_ref()
                    .is_some_and(|creator| creator != &principal.principal_id))
        {
            return Err(InvitationServiceError::InvalidParams);
        }

        let cursor_scope = invitation_cursor_scope(principal, &params);
        let key = self
            .secrets
            .load_or_create_auth_credential_hmac_key(INVITATION_CREDENTIAL_KEY_MIN_BYTES)
            .map_err(InvitationServiceError::Unavailable)?;
        let cursor_codec =
            InvitationCursorCodec::new(&key).map_err(InvitationServiceError::Unavailable)?;
        let cursor = params
            .cursor
            .as_deref()
            .map(|cursor| cursor_codec.decode(cursor, cursor_scope.as_str()))
            .transpose()
            .map_err(|_| InvitationServiceError::InvalidParams)?;
        let now = chrono::Utc::now().fixed_offset();
        let database = self.store.database_connection();
        let transaction = database
            .begin_with_options(TransactionOptions {
                sqlite_transaction_mode: Some(SqliteTransactionMode::Immediate),
                ..Default::default()
            })
            .await
            .context("failed to begin invitation list transaction")
            .map_err(InvitationServiceError::Unavailable)?;

        let result = async {
            if !persisted_actor_is_current(&transaction, principal).await? {
                return Err(ListTransactionError::Authorization(inactive_principal()));
            }
            let page = if is_superuser {
                pioneer_crud::list_invitations_for_superuser(
                    &transaction,
                    &principal.gateway_id,
                    params.status,
                    params.creator_principal_id.as_ref(),
                    now,
                    cursor.as_ref(),
                    u64::from(limit),
                )
                .await?
            } else {
                pioneer_crud::list_pending_invitations_for_creator(
                    &transaction,
                    &principal.gateway_id,
                    &principal.principal_id,
                    now,
                    cursor.as_ref(),
                    u64::from(limit),
                )
                .await?
            };
            for expired in &page.materialized_expirations {
                let invitation_id = InvitationId::new(expired.id.clone())
                    .context("persisted invitation id is invalid")?;
                self.audit
                    .invitation_expired(&transaction, &principal.gateway_id, &invitation_id, now)
                    .await?;
            }
            let mut invitations = Vec::with_capacity(page.invitations.len());
            for invitation in page.invitations {
                let rows =
                    pioneer_crud::load_invitation_projection(&transaction, invitation).await?;
                invitations.push(invitation_summary(rows, now)?);
            }
            let changed_invitation_ids = page
                .materialized_expirations
                .into_iter()
                .map(|expired| {
                    InvitationId::new(expired.id).context("persisted invitation id is invalid")
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok::<_, ListTransactionError>((invitations, page.next_cursor, changed_invitation_ids))
        }
        .await;

        let (invitations, next_cursor, changed_invitation_ids) = match result {
            Ok(result) => {
                transaction
                    .commit()
                    .await
                    .context("failed to commit invitation list transaction")
                    .map_err(InvitationServiceError::Unavailable)?;
                result
            }
            Err(ListTransactionError::Authorization(decision)) => {
                let _ = transaction.rollback().await;
                return Err(InvitationServiceError::Authorization(decision));
            }
            Err(ListTransactionError::Storage(error)) => {
                let _ = transaction.rollback().await;
                return Err(InvitationServiceError::Unavailable(error));
            }
        };
        for _ in &changed_invitation_ids {
            record_outcome(Epic5Operation::InvitationExpire, Epic5Outcome::Success);
            record_latency(Epic5Operation::InvitationExpire, started.elapsed());
        }
        Ok(InvitationListCommitted {
            response: InvitationListResponse {
                invitations,
                next_cursor: next_cursor
                    .as_ref()
                    .map(|cursor| cursor_codec.encode(cursor, cursor_scope.as_str())),
            },
            changed_invitation_ids,
        })
    }

    pub(crate) async fn revoke(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        admission: &AuthorizedInvitation,
        params: InvitationRevokeParams,
    ) -> Result<InvitationRevokeCommitted, InvitationServiceError> {
        let started = std::time::Instant::now();
        let result = self
            .revoke_inner(principal, admission, params, started)
            .await;
        let outcome = match &result {
            Ok(committed) if committed.response.changed => Epic5Outcome::Success,
            Ok(_) => Epic5Outcome::Noop,
            Err(InvitationServiceError::InvalidParams) => Epic5Outcome::Invalid,
            Err(InvitationServiceError::RateLimited) => Epic5Outcome::RateLimited,
            Err(InvitationServiceError::Authorization(_)) => Epic5Outcome::Denied,
            Err(InvitationServiceError::CommittedTerminalHidden(_)) => Epic5Outcome::Denied,
            Err(InvitationServiceError::Unavailable(_)) => Epic5Outcome::Unavailable,
        };
        record_outcome(Epic5Operation::InvitationRevoke, outcome);
        record_latency(Epic5Operation::InvitationRevoke, started.elapsed());
        result
    }

    async fn revoke_inner(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        admission: &AuthorizedInvitation,
        params: InvitationRevokeParams,
        started: std::time::Instant,
    ) -> Result<InvitationRevokeCommitted, InvitationServiceError> {
        if admission.principal_id() != &principal.principal_id
            || admission.action() != ResourceAction::InvitationRevoke
            || admission.invitation_id() != &params.invitation_id
        {
            return Err(scope_mismatch());
        }
        let now = chrono::Utc::now().fixed_offset();
        let database = self.store.database_connection();
        let transaction = database
            .begin_with_options(TransactionOptions {
                sqlite_transaction_mode: Some(SqliteTransactionMode::Immediate),
                ..Default::default()
            })
            .await
            .context("failed to begin invitation revoke transaction")
            .map_err(InvitationServiceError::Unavailable)?;

        let result = async {
            let gate = AuthorizationService::new().authorize_action(
                principal.kind,
                principal.role_key.as_ref(),
                ResourceAction::InvitationRevoke,
            );
            match AuthorizationResolver::new(self.store.clone())
                .authorize_invitation(&transaction, principal, &gate, &params.invitation_id)
                .await
                .context("failed to reauthorize invitation revoke")?
            {
                ProofResolution::Authorized(_) => {}
                ProofResolution::Denied(decision) => {
                    return Err(RevokeTransactionError::Authorization(decision));
                }
            }
            let outcome = pioneer_crud::transition_pending_to_revoked(
                &transaction,
                &params.invitation_id,
                InvitationRevokeReason::InviterRevoked,
                now,
            )
            .await?;
            let (invitation, changed, notification_changed, expired) = match outcome {
                InvitationTransitionOutcome::Applied(invitation) => {
                    self.audit
                        .invitation_revoked(
                            &transaction,
                            &principal.gateway_id,
                            &principal.principal_id,
                            &principal.session_id,
                            &params.invitation_id,
                            now,
                        )
                        .await?;
                    (invitation, true, true, false)
                }
                InvitationTransitionOutcome::Expired(invitation) => {
                    self.audit
                        .invitation_expired(
                            &transaction,
                            &principal.gateway_id,
                            &params.invitation_id,
                            now,
                        )
                        .await?;
                    (invitation, false, true, true)
                }
                InvitationTransitionOutcome::NotApplied(invitation) => {
                    (invitation, false, false, false)
                }
                InvitationTransitionOutcome::NotFound => {
                    return Err(RevokeTransactionError::Authorization(missing_resource()));
                }
            };
            let rows = pioneer_crud::load_invitation_projection(&transaction, invitation).await?;
            Ok::<_, RevokeTransactionError>((
                invitation_summary(rows, now)?,
                changed,
                notification_changed,
                expired,
            ))
        }
        .await;

        let (invitation, changed, notification_changed, expired) = match result {
            Ok(result) => {
                transaction
                    .commit()
                    .await
                    .context("failed to commit invitation revoke transaction")
                    .map_err(InvitationServiceError::Unavailable)?;
                result
            }
            Err(RevokeTransactionError::Authorization(decision)) => {
                let _ = transaction.rollback().await;
                return Err(InvitationServiceError::Authorization(decision));
            }
            Err(RevokeTransactionError::Storage(error)) => {
                let _ = transaction.rollback().await;
                return Err(InvitationServiceError::Unavailable(error));
            }
        };
        if expired {
            record_outcome(Epic5Operation::InvitationExpire, Epic5Outcome::Success);
            record_latency(Epic5Operation::InvitationExpire, started.elapsed());
        }
        if principal.kind != PrincipalKind::Superuser && !changed {
            if notification_changed {
                return Err(InvitationServiceError::CommittedTerminalHidden(
                    params.invitation_id,
                ));
            }
            return Err(InvitationServiceError::Authorization(missing_resource()));
        }
        let committed = InvitationRevokeCommitted {
            response: InvitationRevokeResponse {
                invitation,
                changed,
            },
            notification_changed,
        };
        Ok(committed)
    }
}

enum PreviewTransactionOutcome {
    Available(InvitationPreviewResponse),
    Unavailable {
        terminal_change: Option<CommittedInvitationChange>,
    },
}

enum ListTransactionError {
    Authorization(AuthorizationDecision),
    Storage(Error),
}

impl From<Error> for ListTransactionError {
    fn from(error: Error) -> Self {
        Self::Storage(error)
    }
}

struct AcceptedInvitation {
    response: InvitationAcceptResponse,
    committed: InvitationAcceptCommitted,
}

struct CommittedInvitationChange {
    invitation_id: InvitationId,
    operation: Epic5Operation,
}

enum AcceptTransactionOutcome {
    Accepted(AcceptedInvitation),
    Unavailable {
        terminal_change: Option<CommittedInvitationChange>,
    },
    Corrective(InvitationErrorReason),
    Contended,
}

enum InvitationAdmission {
    Available {
        invitation_id: InvitationId,
        inviter_id: PrincipalId,
        workspace_ids: Vec<WorkspaceId>,
    },
    Unavailable {
        terminal_change: Option<CommittedInvitationChange>,
    },
}

async fn preflight_accept_admission(
    database: &DatabaseConnection,
    credentials: &OpaqueCredentialFactory,
    auth_service: &GatewayAuthService,
    gateway_id: &pioneer_protocol::GatewayId,
    raw_credential: &str,
) -> Result<(), InvitationAcceptServiceError> {
    let started = std::time::Instant::now();
    let now = chrono::Utc::now().fixed_offset();
    let transaction = database
        .begin_with_options(TransactionOptions {
            sqlite_transaction_mode: Some(SqliteTransactionMode::Immediate),
            ..Default::default()
        })
        .await
        .context("failed to begin invitation accept preflight transaction")
        .map_err(InvitationAcceptServiceError::Storage)?;
    let admission =
        admit_invitation_in_transaction(&transaction, credentials, gateway_id, raw_credential, now)
            .await;
    match admission {
        Ok(InvitationAdmission::Available { .. }) => {
            let _ = transaction.rollback().await;
            Ok(())
        }
        Ok(InvitationAdmission::Unavailable { terminal_change }) => {
            if let Some(change) = terminal_change {
                transaction
                    .commit()
                    .await
                    .context("failed to commit invitation preflight terminal transition")
                    .map_err(InvitationAcceptServiceError::Storage)?;
                record_outcome(change.operation, Epic5Outcome::Success);
                record_latency(change.operation, started.elapsed());
                auth_service
                    .invitation_changed_committed(change.invitation_id)
                    .await;
            } else {
                let _ = transaction.rollback().await;
            }
            Err(InvitationAcceptServiceError::Unavailable)
        }
        Err(error) => {
            let _ = transaction.rollback().await;
            Err(InvitationAcceptServiceError::Storage(error))
        }
    }
}

async fn admit_invitation_in_transaction(
    transaction: &sea_orm::DatabaseTransaction,
    credentials: &OpaqueCredentialFactory,
    gateway_id: &pioneer_protocol::GatewayId,
    raw_credential: &str,
    now: DateTimeWithTimeZone,
) -> Result<InvitationAdmission, Error> {
    let lookup =
        lookup_presented_with_factory(credentials, transaction, raw_credential, now).await?;
    let invitation = match lookup {
        InvitationCredentialLookup::Available(invitation) => invitation,
        InvitationCredentialLookup::Expired(expired) => {
            let invitation_id =
                InvitationId::new(expired.id).context("persisted invitation id is invalid")?;
            AdministrativeAuditWriter
                .invitation_expired(transaction, gateway_id, &invitation_id, now)
                .await?;
            return Ok(InvitationAdmission::Unavailable {
                terminal_change: Some(CommittedInvitationChange {
                    invitation_id,
                    operation: Epic5Operation::InvitationExpire,
                }),
            });
        }
        InvitationCredentialLookup::Unavailable => {
            return Ok(InvitationAdmission::Unavailable {
                terminal_change: None,
            });
        }
    };
    if invitation.invitation.gateway_id != gateway_id.as_str() {
        return Ok(InvitationAdmission::Unavailable {
            terminal_change: None,
        });
    }
    let invitation_id = InvitationId::new(invitation.invitation.id.clone())
        .context("persisted invitation id is invalid")?;
    match validate_invitation_authority(transaction, gateway_id, &invitation).await? {
        InvitationAuthority::Valid {
            inviter_id,
            workspace_ids,
        } => {
            record_outcome(Epic5Operation::GrantReauthorization, Epic5Outcome::Success);
            Ok(InvitationAdmission::Available {
                invitation_id,
                inviter_id,
                workspace_ids,
            })
        }
        InvitationAuthority::Invalid(reason) => {
            record_outcome(Epic5Operation::GrantReauthorization, Epic5Outcome::Denied);
            let terminal_change = materialize_invalid_invitation(
                transaction,
                gateway_id,
                &invitation_id,
                reason,
                now,
            )
            .await?;
            Ok(InvitationAdmission::Unavailable { terminal_change })
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn accept_in_transaction(
    transaction: &sea_orm::DatabaseTransaction,
    credentials: &OpaqueCredentialFactory,
    auth_service: &GatewayAuthService,
    gateway_id: &pioneer_protocol::GatewayId,
    raw_credential: &str,
    expected_token_hash: &[u8; 32],
    validated: ValidatedInvitationAccept,
    now_unix: u64,
    now: DateTimeWithTimeZone,
) -> Result<AcceptTransactionOutcome, Error> {
    let admission =
        admit_invitation_in_transaction(transaction, credentials, gateway_id, raw_credential, now)
            .await?;
    let (invitation_id, inviter_id, workspace_ids) = match admission {
        InvitationAdmission::Available {
            invitation_id,
            inviter_id,
            workspace_ids,
        } => (invitation_id, inviter_id, workspace_ids),
        InvitationAdmission::Unavailable { terminal_change } => {
            return Ok(AcceptTransactionOutcome::Unavailable { terminal_change });
        }
    };

    if pioneer_crud::nickname_key_exists(
        transaction,
        gateway_id,
        validated.profile.nickname_key.as_str(),
    )
    .await?
    {
        return Ok(AcceptTransactionOutcome::Corrective(
            InvitationErrorReason::NicknameUnavailable,
        ));
    }

    let principal_id = PrincipalId::new(generate_id(AUTH_DOMAIN_ID_LEN))
        .context("generated invalid Member principal id")?;
    let session_ids = FirstMemberSessionIds::random()
        .map_err(|error| Error::msg(format!("failed to allocate first Member session: {error}")))?;
    let ValidatedInvitationAccept {
        profile,
        installation,
    } = validated;
    let avatar_revision = profile.avatar.as_ref().map(|avatar| avatar.revision());
    pioneer_crud::create_member_principal(
        transaction,
        NewMemberPrincipalRow {
            id: principal_id.clone(),
            gateway_id: gateway_id.clone(),
            display_name: profile.display_name.clone(),
            nickname: profile.nickname.clone(),
            nickname_key: profile.nickname_key,
            now,
        },
    )
    .await?;
    if let Some(avatar) = profile.avatar {
        pioneer_crud::insert_principal_avatar(
            transaction,
            NewPrincipalAvatarRow {
                principal_id: principal_id.clone(),
                media_type: avatar.media_type,
                content: avatar.content,
                content_hash: avatar.content_hash,
                width: avatar.width,
                height: avatar.height,
                now,
            },
        )
        .await?;
    }
    for workspace_id in &workspace_ids {
        pioneer_crud::insert_workspace_membership(
            transaction,
            &NewWorkspaceMembership {
                gateway_id: gateway_id.clone(),
                principal_id: principal_id.clone(),
                workspace_id: workspace_id.to_string(),
                granted_by: PersistedActorRef::Principal(inviter_id.clone()),
                now,
            },
        )
        .await?;
    }
    let grant = auth_service
        .provision_first_member_session_in_transaction(
            transaction,
            &principal_id,
            installation,
            &session_ids,
            now_unix,
        )
        .await
        .map_err(|error| {
            Error::msg(format!("failed to provision first Member session: {error}"))
        })?;
    match pioneer_crud::transition_pending_to_accepted(
        transaction,
        &invitation_id,
        expected_token_hash,
        &principal_id,
        &session_ids.device_id,
        &session_ids.session_id,
        now,
    )
    .await?
    {
        InvitationTransitionOutcome::Applied(_) => {}
        InvitationTransitionOutcome::Expired(_)
        | InvitationTransitionOutcome::NotApplied(_)
        | InvitationTransitionOutcome::NotFound => return Ok(AcceptTransactionOutcome::Contended),
    }
    AdministrativeAuditWriter
        .invitation_accepted(
            transaction,
            gateway_id,
            &invitation_id,
            &principal_id,
            &session_ids.device_id,
            &session_ids.session_id,
            workspace_ids.clone(),
            now,
        )
        .await?;

    Ok(AcceptTransactionOutcome::Accepted(AcceptedInvitation {
        response: InvitationAcceptResponse {
            grant,
            member: MemberSummary {
                principal_id: principal_id.clone(),
                kind: PrincipalKind::User,
                display_name: profile.display_name,
                nickname: profile.nickname,
                role_key: Some(RoleKey::member()),
                status: PrincipalStatus::Active,
                avatar_revision,
            },
            workspace_ids: workspace_ids.clone(),
        },
        committed: InvitationAcceptCommitted {
            invitation_id,
            inviter_principal_id: inviter_id,
            accepted_principal_id: principal_id,
            workspace_ids,
        },
    }))
}

async fn preview_in_transaction(
    transaction: &sea_orm::DatabaseTransaction,
    credentials: &OpaqueCredentialFactory,
    gateway_id: &pioneer_protocol::GatewayId,
    raw_credential: &str,
    transport: InvitationTransportSecurity,
    now: DateTimeWithTimeZone,
) -> Result<PreviewTransactionOutcome, Error> {
    let lookup =
        lookup_presented_with_factory(credentials, transaction, raw_credential, now).await?;
    let invitation = match lookup {
        InvitationCredentialLookup::Available(invitation) => invitation,
        InvitationCredentialLookup::Expired(expired) => {
            let invitation_id =
                InvitationId::new(expired.id).context("persisted invitation id is invalid")?;
            AdministrativeAuditWriter
                .invitation_expired(transaction, gateway_id, &invitation_id, now)
                .await?;
            return Ok(PreviewTransactionOutcome::Unavailable {
                terminal_change: Some(CommittedInvitationChange {
                    invitation_id,
                    operation: Epic5Operation::InvitationExpire,
                }),
            });
        }
        InvitationCredentialLookup::Unavailable => {
            return Ok(PreviewTransactionOutcome::Unavailable {
                terminal_change: None,
            });
        }
    };
    if invitation.invitation.gateway_id != gateway_id.as_str() {
        return Ok(PreviewTransactionOutcome::Unavailable {
            terminal_change: None,
        });
    }
    let invitation_id = InvitationId::new(invitation.invitation.id.clone())
        .context("persisted invitation id is invalid")?;
    if let InvitationAuthority::Invalid(reason) =
        validate_invitation_authority(transaction, gateway_id, &invitation).await?
    {
        let terminal_change =
            materialize_invalid_invitation(transaction, gateway_id, &invitation_id, reason, now)
                .await?;
        return Ok(PreviewTransactionOutcome::Unavailable { terminal_change });
    }
    let rows = pioneer_crud::load_invitation_projection(transaction, invitation.invitation).await?;
    let summary = invitation_summary(rows, now)?;
    Ok(PreviewTransactionOutcome::Available(
        InvitationPreviewResponse {
            gateway_id: gateway_id.clone(),
            gateway_display_name: None,
            inviter: summary.inviter,
            workspaces: summary.workspaces,
            expires_at_unix: summary.expires_at_unix,
            transport,
        },
    ))
}

enum InvitationAuthority {
    Valid {
        inviter_id: PrincipalId,
        workspace_ids: Vec<WorkspaceId>,
    },
    Invalid(InvitationRevokeReason),
}

async fn validate_invitation_authority(
    transaction: &sea_orm::DatabaseTransaction,
    gateway_id: &pioneer_protocol::GatewayId,
    invitation: &pioneer_crud::InvitationWithGrants,
) -> Result<InvitationAuthority, Error> {
    if !(1..=INVITATION_MAX_WORKSPACE_GRANTS).contains(&invitation.grants.len()) {
        return Ok(InvitationAuthority::Invalid(
            InvitationRevokeReason::WorkspaceUnavailable,
        ));
    }
    let inviter_id = PrincipalId::new(invitation.invitation.created_by_principal_id.clone())
        .context("persisted invitation creator id is invalid")?;
    let Some(inviter) = pioneer_crud::load_principal_by_id(transaction, &inviter_id).await? else {
        return Ok(InvitationAuthority::Invalid(
            InvitationRevokeReason::InviterUnavailable,
        ));
    };
    if inviter.gateway_id != *gateway_id
        || inviter.status != pioneer_protocol::PrincipalStatus::Active
        || !matches!(
            (inviter.kind, inviter.role_key.as_deref()),
            (PrincipalKind::Superuser, None)
                | (PrincipalKind::User, Some(pioneer_protocol::MEMBER_ROLE_KEY))
        )
    {
        return Ok(InvitationAuthority::Invalid(
            InvitationRevokeReason::InviterUnavailable,
        ));
    }
    let mut workspace_ids = Vec::with_capacity(invitation.grants.len());
    for grant in &invitation.grants {
        let workspace_id = WorkspaceId::new(grant.workspace_id.clone())
            .context("persisted invitation workspace grant is invalid")?;
        let Some(workspace) =
            pioneer_crud::resolve_workspace_authorization_scope(transaction, workspace_id.as_str())
                .await?
        else {
            return Ok(InvitationAuthority::Invalid(
                InvitationRevokeReason::WorkspaceUnavailable,
            ));
        };
        if !workspace.is_active {
            return Ok(InvitationAuthority::Invalid(
                InvitationRevokeReason::WorkspaceUnavailable,
            ));
        }
        if inviter.kind == PrincipalKind::User
            && pioneer_crud::find_active_workspace_for_principal(
                transaction,
                &inviter_id,
                workspace_id.as_str(),
            )
            .await?
            .is_none()
        {
            return Ok(InvitationAuthority::Invalid(
                InvitationRevokeReason::GrantAuthorityLost,
            ));
        }
        workspace_ids.push(workspace_id);
    }
    Ok(InvitationAuthority::Valid {
        inviter_id,
        workspace_ids,
    })
}

async fn materialize_invalid_invitation(
    transaction: &sea_orm::DatabaseTransaction,
    gateway_id: &pioneer_protocol::GatewayId,
    invitation_id: &InvitationId,
    reason: InvitationRevokeReason,
    now: DateTimeWithTimeZone,
) -> Result<Option<CommittedInvitationChange>, Error> {
    match pioneer_crud::transition_pending_to_revoked(transaction, invitation_id, reason, now)
        .await?
    {
        InvitationTransitionOutcome::Applied(_) => {
            AdministrativeAuditWriter
                .invitation_revoked_by_system(transaction, gateway_id, invitation_id, now)
                .await?;
            Ok(Some(CommittedInvitationChange {
                invitation_id: invitation_id.clone(),
                operation: Epic5Operation::InvitationRevoke,
            }))
        }
        InvitationTransitionOutcome::Expired(_) => {
            AdministrativeAuditWriter
                .invitation_expired(transaction, gateway_id, invitation_id, now)
                .await?;
            Ok(Some(CommittedInvitationChange {
                invitation_id: invitation_id.clone(),
                operation: Epic5Operation::InvitationExpire,
            }))
        }
        InvitationTransitionOutcome::NotApplied(_) | InvitationTransitionOutcome::NotFound => {
            Ok(None)
        }
    }
}

#[derive(Debug)]
enum CreateTransactionError {
    Authorization(AuthorizationDecision),
    RateLimited,
    Storage(Error),
}

impl From<Error> for CreateTransactionError {
    fn from(error: Error) -> Self {
        Self::Storage(error)
    }
}

#[derive(Debug)]
enum RevokeTransactionError {
    Authorization(AuthorizationDecision),
    Storage(Error),
}

impl From<Error> for RevokeTransactionError {
    fn from(error: Error) -> Self {
        Self::Storage(error)
    }
}

fn scope_mismatch() -> InvitationServiceError {
    InvitationServiceError::Authorization(AuthorizationDecision::Deny {
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

fn inactive_principal() -> AuthorizationDecision {
    AuthorizationDecision::Deny {
        reason: DenyReason::InactivePrincipal,
        disclosure: DisclosurePolicy::AuthenticationTerminal,
    }
}

fn invitation_cursor_scope(
    principal: &AuthenticatedSessionPrincipal,
    params: &InvitationListParams,
) -> String {
    if principal.kind != PrincipalKind::Superuser {
        return format!("member:{}", principal.principal_id);
    }
    let status = params
        .status
        .map(pioneer_crud::invitation_status_to_db)
        .unwrap_or("*");
    let creator = params
        .creator_principal_id
        .as_ref()
        .map(PrincipalId::as_str)
        .unwrap_or("*");
    format!("superuser:{}:{status}:{creator}", principal.gateway_id)
}

fn invitation_summary(
    rows: InvitationProjectionRows,
    now: DateTimeWithTimeZone,
) -> Result<InvitationSummary, Error> {
    let invitation_id = InvitationId::new(rows.invitation.id.clone())
        .context("persisted invitation id is invalid")?;
    let inviter_id = pioneer_protocol::PrincipalId::new(rows.inviter.id)
        .context("persisted invitation creator id is invalid")?;
    let inviter_kind = pioneer_crud::principal_kind_from_db(rows.inviter.kind.as_str())?;
    let workspaces = rows
        .workspaces
        .into_iter()
        .map(|workspace| {
            let workspace_id = WorkspaceId::new(workspace.workspace_id)
                .context("persisted invitation workspace id is invalid")?;
            Ok(InvitationWorkspaceSummary {
                name: workspace.name.unwrap_or_else(|| workspace_id.to_string()),
                workspace_id,
            })
        })
        .collect::<Result<Vec<_>, Error>>()?;
    let status = pioneer_crud::effective_invitation_status(&rows.invitation, now)?;
    Ok(InvitationSummary {
        invitation_id,
        status,
        revoke_reason: rows
            .invitation
            .revoke_reason
            .as_deref()
            .map(pioneer_crud::invitation_revoke_reason_from_db)
            .transpose()?,
        inviter: InvitationInviterSummary {
            principal_id: inviter_id,
            kind: inviter_kind,
            display_name: rows.inviter.display_name,
            nickname: rows.inviter.nickname,
        },
        workspaces,
        created_at_unix: u64::try_from(rows.invitation.created_at.timestamp())
            .context("invitation creation time predates Unix epoch")?,
        expires_at_unix: u64::try_from(rows.invitation.expires_at.timestamp())
            .context("invitation expiry predates Unix epoch")?,
        terminal_at_unix: match status {
            InvitationStatus::Pending => None,
            InvitationStatus::Accepted => rows.invitation.accepted_at,
            InvitationStatus::Revoked => rows.invitation.revoked_at,
            InvitationStatus::Expired => rows
                .invitation
                .expired_at
                .or(Some(rows.invitation.expires_at)),
        }
        .map(|timestamp| {
            u64::try_from(timestamp.timestamp())
                .context("invitation terminal time predates Unix epoch")
        })
        .transpose()?,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use pioneer_config::GatewayAuthConfig;
    use pioneer_entity::{
        audit_event, auth_refresh_credential, auth_session, device, gateway_principal, invitation,
        invitation_workspace_grant, workspace, workspace_membership,
    };
    use pioneer_keystore::MemorySecretStore;
    use pioneer_protocol::{
        AUTH_DOMAIN_ID_LEN, AuthSessionId, ClientInstallationDescriptor, ClientKind, DeviceId,
        GatewayId, MemberRemoveParams, NewMemberProfile, PrincipalId, PrincipalKind,
        ProfileAvatarInput, ProfileAvatarMediaType, RoleKey, WorkspaceId,
        WorkspaceMemberRemoveParams, generate_id,
    };
    use sea_orm::sea_query::Expr;
    use sea_orm::{
        ColumnTrait, ConnectionTrait, DatabaseBackend, EntityTrait, QueryFilter, QueryOrder,
        Statement,
    };

    use crate::authorization::{
        AuthorizationResolver, AuthorizationService, AuthorizedMemberPrincipal,
        AuthorizedWorkspace, ProofResolution,
    };
    use crate::member::MemberService;
    use crate::secrets::{AuthKeyMaterial, GatewaySecrets};
    use crate::tests::authorization::{
        IsolatedEpic4Harness, MEMBER_A_ID, WORKSPACE_BLUE_ID, WORKSPACE_GREEN_ID, WORKSPACE_RED_ID,
    };

    use super::*;

    struct InvitationAcceptCommitProbe {
        database: sea_orm::DatabaseConnection,
        accepted_calls: AtomicUsize,
        observed_committed_state: AtomicBool,
    }

    #[async_trait::async_trait]
    impl crate::auth::InvitationAcceptPostCommitHook for InvitationAcceptCommitProbe {
        async fn invitation_accepted(&self, committed: crate::auth::InvitationAcceptCommitted) {
            let persisted = invitation::Entity::find_by_id(committed.invitation_id.to_string())
                .one(&self.database)
                .await
                .expect("reload invitation from post-commit hook");
            let audit_count = audit_event::Entity::find()
                .filter(audit_event::Column::Action.eq("invitation_accepted"))
                .filter(audit_event::Column::TargetId.eq(committed.invitation_id.to_string()))
                .all(&self.database)
                .await
                .expect("reload invitation audit from post-commit hook")
                .len();
            let committed_is_visible = persisted.is_some_and(|row| {
                row.status == "accepted"
                    && row.accepted_principal_id.as_deref()
                        == Some(committed.accepted_principal_id.as_str())
            }) && audit_count == 1;
            self.observed_committed_state
                .store(committed_is_visible, Ordering::SeqCst);
            self.accepted_calls.fetch_add(1, Ordering::SeqCst);
        }

        async fn invitation_changed(&self, _invitation_id: InvitationId) {}
    }

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

    fn member_b() -> AuthenticatedSessionPrincipal {
        AuthenticatedSessionPrincipal {
            gateway_id: GatewayId::new("G00000000000000000001").unwrap(),
            principal_id: PrincipalId::new(crate::tests::authorization::MEMBER_B_ID).unwrap(),
            kind: PrincipalKind::User,
            role_key: Some(RoleKey::member()),
            device_id: DeviceId::new("D0000000000000000000B").unwrap(),
            session_id: AuthSessionId::new("S0000000000000000000B").unwrap(),
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

    fn accept_params(nickname: &str) -> InvitationAcceptParams {
        InvitationAcceptParams {
            profile: NewMemberProfile::new("New Member", nickname, None).unwrap(),
            installation: ClientInstallationDescriptor {
                installation_id: format!("installation-{nickname}"),
                display_name: "Pioneer Mobile".to_owned(),
                client_kind: ClientKind::Mobile,
                platform: Some("ios".to_owned()),
                client_version: Some("1.0".to_owned()),
            },
        }
    }

    fn invitation_auth_service(
        harness: &IsolatedEpic4Harness,
        secrets: &GatewaySecrets,
    ) -> GatewayAuthService {
        let credential_key = secrets
            .load_or_create_auth_credential_hmac_key(INVITATION_CREDENTIAL_KEY_MIN_BYTES)
            .unwrap();
        GatewayAuthService::new(
            harness.database.clone(),
            GatewayAuthConfig::default(),
            Arc::new(harness.identity.clone()),
            &AuthKeyMaterial::from_test_bytes(vec![7; 64]),
            &credential_key,
        )
        .unwrap()
    }

    async fn seed_superuser_session(database: &sea_orm::DatabaseConnection) {
        database
            .execute_unprepared(
                "INSERT INTO device(\
                    id,gateway_id,principal_id,installation_id,display_name,client_kind,\
                    platform,client_version,status,created_at,updated_at,last_seen_at,revoked_at\
                 ) VALUES(\
                    'D00000000000000000001','G00000000000000000001',\
                    'P00000000000000000001','fixture-superuser','Superuser Desktop','desktop',\
                    'test','1','active',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,NULL\
                 );\
                 INSERT INTO auth_session(\
                    id,gateway_id,principal_id,device_id,token_family_id,created_by_session_id,\
                    activation_token_hash,activation_locator_hash,activation_failed_attempts,\
                    activation_expires_at,activated_at,status,refresh_generation,created_at,\
                    updated_at,last_seen_at,last_refreshed_at,refresh_expires_at,revoked_at,\
                    revoke_reason\
                 ) VALUES(\
                    'S00000000000000000001','G00000000000000000001',\
                    'P00000000000000000001','D00000000000000000001',\
                    'F00000000000000000001',NULL,randomblob(32),randomblob(32),0,\
                    datetime('now','+10 minutes'),CURRENT_TIMESTAMP,'active',0,\
                    CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,\
                    datetime('now','+90 days'),NULL,NULL\
                 );",
            )
            .await
            .unwrap();
    }

    async fn authorized_grants(
        store: &CrudStore,
        principal: &AuthenticatedSessionPrincipal,
        workspace_ids: &[WorkspaceId],
    ) -> AuthorizedInvitationGrants {
        let gate = AuthorizationService::new().authorize_action(
            principal.kind,
            principal.role_key.as_ref(),
            ResourceAction::InvitationCreate,
        );
        match AuthorizationResolver::new(store.clone())
            .authorize_invitation_grants(
                &store.database_connection(),
                principal,
                &gate,
                workspace_ids,
            )
            .await
            .unwrap()
        {
            ProofResolution::Authorized(proof) => proof,
            ProofResolution::Denied(decision) => panic!("unexpected denial: {decision:?}"),
        }
    }

    fn authorized_collection(
        store: &CrudStore,
        principal: &AuthenticatedSessionPrincipal,
    ) -> AuthorizedInvitationCollection {
        let gate = AuthorizationService::new().authorize_action(
            principal.kind,
            principal.role_key.as_ref(),
            ResourceAction::InvitationList,
        );
        match AuthorizationResolver::new(store.clone())
            .authorize_invitation_collection(principal, &gate)
        {
            ProofResolution::Authorized(proof) => proof,
            ProofResolution::Denied(decision) => panic!("unexpected denial: {decision:?}"),
        }
    }

    async fn authorized_invitation(
        store: &CrudStore,
        principal: &AuthenticatedSessionPrincipal,
        invitation_id: &InvitationId,
    ) -> AuthorizedInvitation {
        let gate = AuthorizationService::new().authorize_action(
            principal.kind,
            principal.role_key.as_ref(),
            ResourceAction::InvitationRevoke,
        );
        match AuthorizationResolver::new(store.clone())
            .authorize_invitation(
                &store.database_connection(),
                principal,
                &gate,
                invitation_id,
            )
            .await
            .unwrap()
        {
            ProofResolution::Authorized(proof) => proof,
            ProofResolution::Denied(decision) => panic!("unexpected denial: {decision:?}"),
        }
    }

    async fn authorized_member_principal(
        store: &CrudStore,
        principal: &AuthenticatedSessionPrincipal,
        action: ResourceAction,
        target: &PrincipalId,
    ) -> AuthorizedMemberPrincipal {
        let gate = AuthorizationService::new().authorize_action(
            principal.kind,
            principal.role_key.as_ref(),
            action,
        );
        match AuthorizationResolver::new(store.clone())
            .authorize_member_principal(
                &store.database_connection(),
                principal,
                &gate,
                action,
                target,
            )
            .await
            .unwrap()
        {
            ProofResolution::Authorized(proof) => proof,
            ProofResolution::Denied(decision) => panic!("unexpected denial: {decision:?}"),
        }
    }

    async fn authorized_workspace(
        store: &CrudStore,
        principal: &AuthenticatedSessionPrincipal,
        action: ResourceAction,
        workspace_id: &WorkspaceId,
    ) -> AuthorizedWorkspace {
        let gate = AuthorizationService::new().authorize_action(
            principal.kind,
            principal.role_key.as_ref(),
            action,
        );
        match AuthorizationResolver::new(store.clone())
            .authorize_workspace(principal, &gate, action, workspace_id.as_str())
            .await
            .unwrap()
        {
            ProofResolution::Authorized(proof) => proof,
            ProofResolution::Denied(decision) => panic!("unexpected denial: {decision:?}"),
        }
    }

    async fn assert_database_does_not_contain(
        database: &sea_orm::DatabaseConnection,
        secret: &str,
    ) {
        let tables = database
            .query_all_raw(Statement::from_string(
                DatabaseBackend::Sqlite,
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name".to_owned(),
            ))
            .await
            .unwrap();
        for table in tables {
            let table_name = table.try_get::<String>("", "name").unwrap();
            let quoted_table = table_name.replace('"', "\"\"");
            let columns = database
                .query_all_raw(Statement::from_string(
                    DatabaseBackend::Sqlite,
                    format!("PRAGMA table_info(\"{quoted_table}\")"),
                ))
                .await
                .unwrap();
            for column in columns {
                let column_name = column.try_get::<String>("", "name").unwrap();
                let quoted_column = column_name.replace('"', "\"\"");
                let leak_count = database
                    .query_one_raw(Statement::from_sql_and_values(
                        DatabaseBackend::Sqlite,
                        format!(
                            "SELECT COUNT(*) AS leak_count FROM \"{quoted_table}\" \
                             WHERE instr(CAST(\"{quoted_column}\" AS BLOB), CAST(? AS BLOB)) > 0"
                        ),
                        [secret.into()],
                    ))
                    .await
                    .unwrap()
                    .unwrap()
                    .try_get::<i64>("", "leak_count")
                    .unwrap();
                assert_eq!(
                    leak_count, 0,
                    "secret persisted in {table_name}.{column_name}"
                );
            }
        }
    }

    #[tokio::test]
    async fn member_create_commits_hash_grants_and_audit_atomically() {
        let harness = IsolatedEpic4Harness::populated().await.unwrap();
        let store = CrudStore::new(harness.database.clone());
        let principal = member_a();
        let workspace_ids = vec![
            WorkspaceId::new(WORKSPACE_BLUE_ID).unwrap(),
            WorkspaceId::new(WORKSPACE_RED_ID).unwrap(),
        ];
        let proof = authorized_grants(&store, &principal, &workspace_ids).await;
        let service = InvitationService::new(
            store,
            Arc::new(GatewaySecrets::new(Arc::new(MemorySecretStore::new()))),
            "127.0.0.1:17878",
        )
        .unwrap();

        let response = service
            .create(
                &principal,
                &proof,
                InvitationCreateParams::new(workspace_ids).unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.invitation.status, InvitationStatus::Pending);
        assert_eq!(
            response.invitation.expires_at_unix - response.invitation.created_at_unix,
            pioneer_protocol::INVITATION_TTL_SECONDS
        );
        assert_eq!(
            response.presentation.gateway_base_url.as_str(),
            "http://127.0.0.1:17878/"
        );
        let rows = invitation::Entity::find()
            .all(&harness.database)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].token_hash.as_ref().map(Vec::len), Some(32));
        let grants = invitation_workspace_grant::Entity::find()
            .order_by_asc(invitation_workspace_grant::Column::WorkspaceId)
            .all(&harness.database)
            .await
            .unwrap();
        assert_eq!(
            grants
                .iter()
                .map(|grant| grant.workspace_id.as_str())
                .collect::<Vec<_>>(),
            vec![WORKSPACE_RED_ID, WORKSPACE_BLUE_ID]
        );
        let audits = audit_event::Entity::find()
            .all(&harness.database)
            .await
            .unwrap();
        assert_eq!(audits.len(), 1);
        assert_eq!(audits[0].action, "invitation_created");
        assert!(
            !audits[0]
                .metadata_json
                .contains(response.presentation.token())
        );
        assert_database_does_not_contain(&harness.database, response.presentation.token()).await;
        assert_database_does_not_contain(&harness.database, response.presentation.deep_link())
            .await;
    }

    #[tokio::test]
    async fn invitation_create_enforces_exact_live_pending_cap() {
        let harness = IsolatedEpic4Harness::populated().await.unwrap();
        let store = CrudStore::new(harness.database.clone());
        let principal = member_a();
        let workspace_ids = vec![WorkspaceId::new(WORKSPACE_RED_ID).unwrap()];
        let proof = authorized_grants(&store, &principal, &workspace_ids).await;
        let now = chrono::Utc::now().fixed_offset();
        let expires_at = now.checked_add_signed(chrono::Duration::days(7)).unwrap();
        let transaction = harness.database.begin().await.unwrap();

        for index in 0..MAX_LIVE_PENDING_INVITATIONS_PER_CREATOR {
            pioneer_crud::insert_invitation(
                &transaction,
                NewInvitationRow {
                    invitation_id: InvitationId::new(format!("I{index:020}")).unwrap(),
                    gateway_id: principal.gateway_id.clone(),
                    created_by_principal_id: principal.principal_id.clone(),
                    created_by_session_id: principal.session_id.clone(),
                    token_hash: [index as u8; 32],
                    expires_at,
                    now,
                },
            )
            .await
            .unwrap();
        }
        transaction.commit().await.unwrap();

        let service = InvitationService::new(
            store,
            Arc::new(GatewaySecrets::new(Arc::new(MemorySecretStore::new()))),
            "127.0.0.1:17878",
        )
        .unwrap();
        let result = service
            .create(
                &principal,
                &proof,
                InvitationCreateParams::new(workspace_ids).unwrap(),
            )
            .await;

        assert!(matches!(result, Err(InvitationServiceError::RateLimited)));
        assert_eq!(
            invitation::Entity::find()
                .all(&harness.database)
                .await
                .unwrap()
                .len() as u64,
            MAX_LIVE_PENDING_INVITATIONS_PER_CREATOR
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
    async fn mixed_member_grants_fail_before_any_write() {
        let harness = IsolatedEpic4Harness::populated().await.unwrap();
        let store = CrudStore::new(harness.database.clone());
        let principal = member_a();
        let workspace_ids = vec![
            WorkspaceId::new(WORKSPACE_RED_ID).unwrap(),
            WorkspaceId::new(WORKSPACE_GREEN_ID).unwrap(),
        ];
        let gate = AuthorizationService::new().authorize_action(
            principal.kind,
            principal.role_key.as_ref(),
            ResourceAction::InvitationCreate,
        );
        let resolution = AuthorizationResolver::new(store)
            .authorize_invitation_grants(
                &harness.database,
                &principal,
                &gate,
                workspace_ids.as_slice(),
            )
            .await
            .unwrap();
        assert!(matches!(resolution, ProofResolution::Denied(_)));
        assert!(
            invitation::Entity::find()
                .all(&harness.database)
                .await
                .unwrap()
                .is_empty()
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
    async fn superuser_create_requires_existent_active_grants_but_not_membership() {
        let harness = IsolatedEpic4Harness::populated().await.unwrap();
        seed_superuser_session(&harness.database).await;
        let store = CrudStore::new(harness.database.clone());
        let principal = superuser();
        let green = WorkspaceId::new(WORKSPACE_GREEN_ID).unwrap();
        let proof = authorized_grants(&store, &principal, std::slice::from_ref(&green)).await;
        let service = InvitationService::new(
            store.clone(),
            Arc::new(GatewaySecrets::new(Arc::new(MemorySecretStore::new()))),
            "127.0.0.1:17878",
        )
        .unwrap();
        assert!(
            service
                .create(
                    &principal,
                    &proof,
                    InvitationCreateParams::new(vec![green.clone()]).unwrap(),
                )
                .await
                .is_ok()
        );

        workspace::Entity::update_many()
            .col_expr(workspace::Column::IsActive, Expr::value(false))
            .filter(workspace::Column::Id.eq(green.to_string()))
            .exec(&harness.database)
            .await
            .unwrap();
        let gate = AuthorizationService::new().authorize_action(
            principal.kind,
            principal.role_key.as_ref(),
            ResourceAction::InvitationCreate,
        );
        for denied in [green, WorkspaceId::new("W00000000000000000099").unwrap()] {
            assert!(matches!(
                AuthorizationResolver::new(store.clone())
                    .authorize_invitation_grants(
                        &harness.database,
                        &principal,
                        &gate,
                        std::slice::from_ref(&denied),
                    )
                    .await
                    .unwrap(),
                ProofResolution::Denied(_)
            ));
        }
    }

    #[tokio::test]
    async fn invalid_actor_session_rolls_back_invitation_and_audit() {
        let harness = IsolatedEpic4Harness::populated().await.unwrap();
        let store = CrudStore::new(harness.database.clone());
        let mut principal = member_a();
        let workspace_ids = vec![WorkspaceId::new(WORKSPACE_RED_ID).unwrap()];
        let proof = authorized_grants(&store, &principal, &workspace_ids).await;
        principal.session_id = AuthSessionId::new("S0000000000000000000Z").unwrap();
        let service = InvitationService::new(
            store,
            Arc::new(GatewaySecrets::new(Arc::new(MemorySecretStore::new()))),
            "http://127.0.0.1:17878",
        )
        .unwrap();

        assert!(
            service
                .create(
                    &principal,
                    &proof,
                    InvitationCreateParams::new(workspace_ids).unwrap(),
                )
                .await
                .is_err()
        );
        assert!(
            invitation::Entity::find()
                .all(&harness.database)
                .await
                .unwrap()
                .is_empty()
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
    async fn member_list_is_scoped_before_pagination_and_revoke_is_idempotent() {
        let harness = IsolatedEpic4Harness::populated().await.unwrap();
        let store = CrudStore::new(harness.database.clone());
        let secrets = Arc::new(GatewaySecrets::new(Arc::new(MemorySecretStore::new())));
        let service = InvitationService::new(store.clone(), secrets, "127.0.0.1:17878").unwrap();
        let member_a = member_a();
        let member_b = member_b();
        let red = vec![WorkspaceId::new(WORKSPACE_RED_ID).unwrap()];
        let green = vec![WorkspaceId::new(WORKSPACE_GREEN_ID).unwrap()];
        let invitation_a = service
            .create(
                &member_a,
                &authorized_grants(&store, &member_a, &red).await,
                InvitationCreateParams::new(red).unwrap(),
            )
            .await
            .unwrap()
            .invitation;
        service
            .create(
                &member_b,
                &authorized_grants(&store, &member_b, &green).await,
                InvitationCreateParams::new(green).unwrap(),
            )
            .await
            .unwrap();

        let page = service
            .list(
                &member_a,
                &authorized_collection(&store, &member_a),
                InvitationListParams {
                    limit: Some(1),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(page.invitations.len(), 1);
        assert_eq!(
            page.invitations[0].invitation_id,
            invitation_a.invitation_id
        );
        assert!(page.next_cursor.is_none());

        let proof = authorized_invitation(&store, &member_a, &invitation_a.invitation_id).await;
        let response = service
            .revoke(
                &member_a,
                &proof,
                InvitationRevokeParams {
                    invitation_id: invitation_a.invitation_id.clone(),
                },
            )
            .await
            .unwrap();
        assert!(response.changed);
        assert!(response.notification_changed);
        assert_eq!(response.invitation.status, InvitationStatus::Revoked);
        assert_eq!(
            response.invitation.revoke_reason,
            Some(InvitationRevokeReason::InviterRevoked)
        );
        let replacement_workspace_ids = vec![WorkspaceId::new(WORKSPACE_RED_ID).unwrap()];
        let replacement = service
            .create(
                &member_a,
                &authorized_grants(&store, &member_a, &replacement_workspace_ids).await,
                InvitationCreateParams::new(replacement_workspace_ids).unwrap(),
            )
            .await
            .unwrap();
        assert_ne!(
            replacement.invitation.invitation_id,
            response.invitation.invitation_id
        );

        let terminal_proof =
            authorized_invitation(&store, &member_a, &invitation_a.invitation_id).await;
        assert!(
            service
                .revoke(
                    &member_a,
                    &terminal_proof,
                    InvitationRevokeParams {
                        invitation_id: invitation_a.invitation_id,
                    },
                )
                .await
                .is_err()
        );
        let audits = audit_event::Entity::find()
            .all(&harness.database)
            .await
            .unwrap();
        assert_eq!(
            audits
                .iter()
                .filter(|event| event.action == "invitation_revoked")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn list_materializes_expiration_and_audits_only_the_transition() {
        let harness = IsolatedEpic4Harness::populated().await.unwrap();
        let store = CrudStore::new(harness.database.clone());
        let service = InvitationService::new(
            store.clone(),
            Arc::new(GatewaySecrets::new(Arc::new(MemorySecretStore::new()))),
            "127.0.0.1:17878",
        )
        .unwrap();
        let member = member_a();
        let workspace_ids = vec![WorkspaceId::new(WORKSPACE_RED_ID).unwrap()];
        let created = service
            .create(
                &member,
                &authorized_grants(&store, &member, &workspace_ids).await,
                InvitationCreateParams::new(workspace_ids).unwrap(),
            )
            .await
            .unwrap();
        let now = chrono::Utc::now().fixed_offset();
        invitation::Entity::update_many()
            .col_expr(
                invitation::Column::CreatedAt,
                Expr::value(now - chrono::Duration::seconds(2)),
            )
            .col_expr(
                invitation::Column::ExpiresAt,
                Expr::value(now - chrono::Duration::seconds(1)),
            )
            .filter(invitation::Column::Id.eq(created.invitation.invitation_id.to_string()))
            .exec(&harness.database)
            .await
            .unwrap();
        let proof = authorized_collection(&store, &member);

        let first = service
            .list(&member, &proof, InvitationListParams::default())
            .await
            .unwrap();
        assert!(first.invitations.is_empty());
        assert_eq!(
            first.changed_invitation_ids,
            vec![created.invitation.invitation_id.clone()]
        );
        let second = service
            .list(&member, &proof, InvitationListParams::default())
            .await
            .unwrap();
        assert!(second.invitations.is_empty());
        assert!(second.changed_invitation_ids.is_empty());
        let audits = audit_event::Entity::find()
            .all(&harness.database)
            .await
            .unwrap();
        assert_eq!(
            audits
                .iter()
                .filter(|event| event.action == "invitation_expired")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn list_reauthorizes_actor_before_materializing_expirations() {
        let harness = IsolatedEpic4Harness::populated().await.unwrap();
        let store = CrudStore::new(harness.database.clone());
        let service = InvitationService::new(
            store.clone(),
            Arc::new(GatewaySecrets::new(Arc::new(MemorySecretStore::new()))),
            "127.0.0.1:17878",
        )
        .unwrap();
        let member = member_a();
        let workspace_ids = vec![WorkspaceId::new(WORKSPACE_RED_ID).unwrap()];
        let created = service
            .create(
                &member,
                &authorized_grants(&store, &member, &workspace_ids).await,
                InvitationCreateParams::new(workspace_ids).unwrap(),
            )
            .await
            .unwrap();
        let invitation_id = created.invitation.invitation_id;
        let proof = authorized_collection(&store, &member);
        let now = chrono::Utc::now().fixed_offset();
        invitation::Entity::update_many()
            .col_expr(
                invitation::Column::CreatedAt,
                Expr::value(now - chrono::Duration::seconds(2)),
            )
            .col_expr(
                invitation::Column::ExpiresAt,
                Expr::value(now - chrono::Duration::seconds(1)),
            )
            .filter(invitation::Column::Id.eq(invitation_id.to_string()))
            .exec(&harness.database)
            .await
            .unwrap();
        gateway_principal::Entity::update_many()
            .col_expr(gateway_principal::Column::Status, Expr::value("suspended"))
            .filter(gateway_principal::Column::Id.eq(member.principal_id.to_string()))
            .exec(&harness.database)
            .await
            .unwrap();

        let error = service
            .list(&member, &proof, InvitationListParams::default())
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            InvitationServiceError::Authorization(AuthorizationDecision::Deny {
                reason: DenyReason::InactivePrincipal,
                disclosure: DisclosurePolicy::AuthenticationTerminal,
            })
        ));
        let persisted = invitation::Entity::find_by_id(invitation_id.to_string())
            .one(&harness.database)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(persisted.status, "pending");
        assert!(persisted.expired_at.is_none());
        assert_eq!(
            audit_event::Entity::find()
                .all(&harness.database)
                .await
                .unwrap()
                .into_iter()
                .filter(|event| event.action == "invitation_expired")
                .count(),
            0
        );
    }

    #[tokio::test]
    async fn member_revoke_surfaces_a_hidden_committed_expiration_for_postcommit_publish() {
        let harness = IsolatedEpic4Harness::populated().await.unwrap();
        let store = CrudStore::new(harness.database.clone());
        let service = InvitationService::new(
            store.clone(),
            Arc::new(GatewaySecrets::new(Arc::new(MemorySecretStore::new()))),
            "127.0.0.1:17878",
        )
        .unwrap();
        let member = member_a();
        let workspace_ids = vec![WorkspaceId::new(WORKSPACE_RED_ID).unwrap()];
        let created = service
            .create(
                &member,
                &authorized_grants(&store, &member, &workspace_ids).await,
                InvitationCreateParams::new(workspace_ids).unwrap(),
            )
            .await
            .unwrap();
        let invitation_id = created.invitation.invitation_id;
        let proof = authorized_invitation(&store, &member, &invitation_id).await;
        let now = chrono::Utc::now().fixed_offset();
        invitation::Entity::update_many()
            .col_expr(
                invitation::Column::CreatedAt,
                Expr::value(now - chrono::Duration::seconds(2)),
            )
            .col_expr(
                invitation::Column::ExpiresAt,
                Expr::value(now - chrono::Duration::seconds(1)),
            )
            .filter(invitation::Column::Id.eq(invitation_id.to_string()))
            .exec(&harness.database)
            .await
            .unwrap();

        let error = service
            .revoke(
                &member,
                &proof,
                InvitationRevokeParams {
                    invitation_id: invitation_id.clone(),
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            InvitationServiceError::CommittedTerminalHidden(id) if id == invitation_id
        ));
        assert_eq!(
            audit_event::Entity::find()
                .all(&harness.database)
                .await
                .unwrap()
                .into_iter()
                .filter(|event| event.action == "invitation_expired")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn revoke_reauthorizes_actor_status_inside_the_transaction() {
        let harness = IsolatedEpic4Harness::populated().await.unwrap();
        let store = CrudStore::new(harness.database.clone());
        let member = member_a();
        let service = InvitationService::new(
            store.clone(),
            Arc::new(GatewaySecrets::new(Arc::new(MemorySecretStore::new()))),
            "127.0.0.1:17878",
        )
        .unwrap();
        let workspace_ids = vec![WorkspaceId::new(WORKSPACE_RED_ID).unwrap()];
        let created = service
            .create(
                &member,
                &authorized_grants(&store, &member, &workspace_ids).await,
                InvitationCreateParams::new(workspace_ids).unwrap(),
            )
            .await
            .unwrap();
        let invitation_id = created.invitation.invitation_id;
        let proof = authorized_invitation(&store, &member, &invitation_id).await;

        gateway_principal::Entity::update_many()
            .col_expr(gateway_principal::Column::Status, Expr::value("suspended"))
            .filter(gateway_principal::Column::Id.eq(member.principal_id.to_string()))
            .exec(&harness.database)
            .await
            .unwrap();

        let error = service
            .revoke(
                &member,
                &proof,
                InvitationRevokeParams {
                    invitation_id: invitation_id.clone(),
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            InvitationServiceError::Authorization(AuthorizationDecision::Deny {
                reason: DenyReason::InactivePrincipal,
                disclosure: DisclosurePolicy::AuthenticationTerminal,
            })
        ));
        assert_eq!(
            invitation::Entity::find_by_id(invitation_id.to_string())
                .one(&harness.database)
                .await
                .unwrap()
                .unwrap()
                .status,
            "pending"
        );
        assert_eq!(
            audit_event::Entity::find()
                .filter(audit_event::Column::Action.eq("invitation_revoked"))
                .all(&harness.database)
                .await
                .unwrap()
                .len(),
            0
        );
    }

    #[tokio::test]
    async fn restricted_preview_is_pinned_safe_and_non_consuming_over_remote_ws() {
        let harness = IsolatedEpic4Harness::populated().await.unwrap();
        let store = CrudStore::new(harness.database.clone());
        let secrets = Arc::new(GatewaySecrets::new(Arc::new(MemorySecretStore::new())));
        let service =
            InvitationService::new(store.clone(), secrets.clone(), "198.51.100.8:17878").unwrap();
        let principal = member_a();
        let workspace_ids = vec![WorkspaceId::new(WORKSPACE_RED_ID).unwrap()];
        let created = service
            .create(
                &principal,
                &authorized_grants(&store, &principal, &workspace_ids).await,
                InvitationCreateParams::new(workspace_ids).unwrap(),
            )
            .await
            .unwrap();
        let raw_credential = created.presentation.token().to_owned();
        let credential_key = secrets
            .load_or_create_auth_credential_hmac_key(INVITATION_CREDENTIAL_KEY_MIN_BYTES)
            .unwrap();
        let credentials = OpaqueCredentialFactory::new(credential_key.as_bytes()).unwrap();

        assert!(
            InvitationService::preview_restricted(
                &harness.database,
                &credentials,
                &GatewayId::new("G00000000000000000099").unwrap(),
                raw_credential.as_str(),
                InvitationTransportSecurity::InsecureWs,
            )
            .await
            .is_err()
        );

        let preview = InvitationService::preview_restricted(
            &harness.database,
            &credentials,
            &principal.gateway_id,
            raw_credential.as_str(),
            InvitationTransportSecurity::InsecureWs,
        )
        .await
        .unwrap();
        assert_eq!(preview.gateway_id, principal.gateway_id);
        assert_eq!(preview.gateway_display_name, None);
        assert_eq!(preview.inviter, created.invitation.inviter);
        assert_eq!(preview.workspaces, created.invitation.workspaces);
        assert_eq!(preview.expires_at_unix, created.invitation.expires_at_unix);
        assert_eq!(preview.transport, InvitationTransportSecurity::InsecureWs);

        let persisted =
            invitation::Entity::find_by_id(created.invitation.invitation_id.to_string())
                .one(&harness.database)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(persisted.status, "pending");
        assert_eq!(persisted.token_hash.as_ref().map(Vec::len), Some(32));
        assert!(persisted.accepted_at.is_none());
    }

    #[tokio::test]
    async fn preview_materializes_lost_member_authority_only_once() {
        let harness = IsolatedEpic4Harness::populated().await.unwrap();
        let store = CrudStore::new(harness.database.clone());
        let secrets = Arc::new(GatewaySecrets::new(Arc::new(MemorySecretStore::new())));
        let service =
            InvitationService::new(store.clone(), secrets.clone(), "http://127.0.0.1:17878")
                .unwrap();
        let principal = member_a();
        let workspace_ids = vec![WorkspaceId::new(WORKSPACE_RED_ID).unwrap()];
        let created = service
            .create(
                &principal,
                &authorized_grants(&store, &principal, &workspace_ids).await,
                InvitationCreateParams::new(workspace_ids).unwrap(),
            )
            .await
            .unwrap();
        let raw_credential = created.presentation.token().to_owned();
        let credential_key = secrets
            .load_or_create_auth_credential_hmac_key(INVITATION_CREDENTIAL_KEY_MIN_BYTES)
            .unwrap();
        let credentials = OpaqueCredentialFactory::new(credential_key.as_bytes()).unwrap();

        workspace_membership::Entity::delete_many()
            .filter(workspace_membership::Column::PrincipalId.eq(MEMBER_A_ID))
            .filter(workspace_membership::Column::WorkspaceId.eq(WORKSPACE_RED_ID))
            .exec(&harness.database)
            .await
            .unwrap();
        for _ in 0..2 {
            assert!(
                InvitationService::preview_restricted(
                    &harness.database,
                    &credentials,
                    &principal.gateway_id,
                    raw_credential.as_str(),
                    InvitationTransportSecurity::SecureWss,
                )
                .await
                .is_err()
            );
        }

        let persisted =
            invitation::Entity::find_by_id(created.invitation.invitation_id.to_string())
                .one(&harness.database)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(persisted.status, "revoked");
        assert_eq!(
            persisted.revoke_reason.as_deref(),
            Some("grant_authority_lost")
        );
        assert!(persisted.token_hash.is_none());
        let audits = audit_event::Entity::find()
            .all(&harness.database)
            .await
            .unwrap();
        assert_eq!(
            audits
                .iter()
                .filter(|event| event.action == "invitation_revoked")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn preview_materializes_expiration_without_exposing_the_invitation() {
        let harness = IsolatedEpic4Harness::populated().await.unwrap();
        let store = CrudStore::new(harness.database.clone());
        let secrets = Arc::new(GatewaySecrets::new(Arc::new(MemorySecretStore::new())));
        let service =
            InvitationService::new(store.clone(), secrets.clone(), "http://127.0.0.1:17878")
                .unwrap();
        let principal = member_a();
        let workspace_ids = vec![WorkspaceId::new(WORKSPACE_RED_ID).unwrap()];
        let created = service
            .create(
                &principal,
                &authorized_grants(&store, &principal, &workspace_ids).await,
                InvitationCreateParams::new(workspace_ids).unwrap(),
            )
            .await
            .unwrap();
        let raw_credential = created.presentation.token().to_owned();
        let credential_key = secrets
            .load_or_create_auth_credential_hmac_key(INVITATION_CREDENTIAL_KEY_MIN_BYTES)
            .unwrap();
        let credentials = OpaqueCredentialFactory::new(credential_key.as_bytes()).unwrap();
        invitation::Entity::update_many()
            .col_expr(
                invitation::Column::CreatedAt,
                Expr::value(chrono::Utc::now().fixed_offset() - chrono::Duration::seconds(2)),
            )
            .col_expr(
                invitation::Column::ExpiresAt,
                Expr::value(chrono::Utc::now().fixed_offset() - chrono::Duration::seconds(1)),
            )
            .filter(invitation::Column::Id.eq(created.invitation.invitation_id.to_string()))
            .exec(&harness.database)
            .await
            .unwrap();

        for _ in 0..2 {
            assert!(
                InvitationService::preview_restricted(
                    &harness.database,
                    &credentials,
                    &principal.gateway_id,
                    raw_credential.as_str(),
                    InvitationTransportSecurity::SecureWss,
                )
                .await
                .is_err()
            );
        }
        let persisted =
            invitation::Entity::find_by_id(created.invitation.invitation_id.to_string())
                .one(&harness.database)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(persisted.status, "expired");
        assert!(persisted.token_hash.is_none());
        let audits = audit_event::Entity::find()
            .all(&harness.database)
            .await
            .unwrap();
        assert_eq!(
            audits
                .iter()
                .filter(|event| event.action == "invitation_expired")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn accept_atomically_creates_exact_member_grants_session_and_audit() {
        let harness = IsolatedEpic4Harness::populated().await.unwrap();
        let store = CrudStore::new(harness.database.clone());
        let secrets = Arc::new(GatewaySecrets::new(Arc::new(MemorySecretStore::new())));
        let service =
            InvitationService::new(store.clone(), secrets.clone(), "http://127.0.0.1:17878")
                .unwrap();
        let inviter = member_a();
        let workspace_ids = vec![
            WorkspaceId::new(WORKSPACE_RED_ID).unwrap(),
            WorkspaceId::new(WORKSPACE_BLUE_ID).unwrap(),
        ];
        let created = service
            .create(
                &inviter,
                &authorized_grants(&store, &inviter, &workspace_ids).await,
                InvitationCreateParams::new(workspace_ids.clone()).unwrap(),
            )
            .await
            .unwrap();
        let raw_credential = created.presentation.token().to_owned();
        let credential_key = secrets
            .load_or_create_auth_credential_hmac_key(INVITATION_CREDENTIAL_KEY_MIN_BYTES)
            .unwrap();
        let credentials = OpaqueCredentialFactory::new(credential_key.as_bytes()).unwrap();
        let auth_service = invitation_auth_service(&harness, secrets.as_ref());
        let commit_probe = Arc::new(InvitationAcceptCommitProbe {
            database: harness.database.clone(),
            accepted_calls: AtomicUsize::new(0),
            observed_committed_state: AtomicBool::new(false),
        });
        auth_service.set_invitation_accept_post_commit_hook(commit_probe.clone());

        let response = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            InvitationService::accept_restricted(
                &harness.database,
                &credentials,
                &auth_service,
                &inviter.gateway_id,
                raw_credential.as_str(),
                accept_params("new-member"),
            ),
        )
        .await
        .expect("post-commit hook must not run while the accept transaction is open")
        .unwrap();

        assert_eq!(response.workspace_ids, workspace_ids);
        assert_eq!(response.member.principal_id, response.grant.principal.id);
        assert_eq!(response.grant.refresh_generation, 0);
        assert_eq!(
            response.grant.credential_storage_order,
            pioneer_protocol::CredentialStorageOrder::PersistRefreshBeforeActivatingAccess
        );
        let accepted = invitation::Entity::find_by_id(created.invitation.invitation_id.to_string())
            .one(&harness.database)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(accepted.status, "accepted");
        assert!(accepted.token_hash.is_none());
        assert_eq!(
            accepted.accepted_principal_id.as_deref(),
            Some(response.member.principal_id.as_str())
        );
        assert_eq!(
            accepted.accepted_device_id.as_deref(),
            Some(response.grant.device.id.as_str())
        );
        assert_eq!(
            accepted.accepted_session_id.as_deref(),
            Some(response.grant.session.id.as_str())
        );
        let memberships = workspace_membership::Entity::find()
            .filter(
                workspace_membership::Column::PrincipalId
                    .eq(response.member.principal_id.to_string()),
            )
            .order_by_asc(workspace_membership::Column::WorkspaceId)
            .all(&harness.database)
            .await
            .unwrap();
        assert_eq!(
            memberships
                .iter()
                .map(|row| row.workspace_id.as_str())
                .collect::<Vec<_>>(),
            vec![WORKSPACE_RED_ID, WORKSPACE_BLUE_ID]
        );
        assert!(
            device::Entity::find_by_id(response.grant.device.id.to_string())
                .one(&harness.database)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            auth_session::Entity::find_by_id(response.grant.session.id.to_string())
                .one(&harness.database)
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(
            auth_refresh_credential::Entity::find()
                .filter(
                    auth_refresh_credential::Column::SessionId.eq(response
                        .grant
                        .session
                        .id
                        .to_string()),
                )
                .all(&harness.database)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            audit_event::Entity::find()
                .filter(audit_event::Column::Action.eq("invitation_accepted"))
                .all(&harness.database)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(commit_probe.accepted_calls.load(Ordering::SeqCst), 1);
        assert!(
            commit_probe.observed_committed_state.load(Ordering::SeqCst),
            "post-commit projection must observe both accepted state and its durable audit"
        );
    }

    #[tokio::test]
    async fn nickname_collision_is_corrective_and_leaves_invitation_pending() {
        let harness = IsolatedEpic4Harness::populated().await.unwrap();
        let store = CrudStore::new(harness.database.clone());
        let secrets = Arc::new(GatewaySecrets::new(Arc::new(MemorySecretStore::new())));
        let service =
            InvitationService::new(store.clone(), secrets.clone(), "127.0.0.1:17878").unwrap();
        let inviter = member_a();
        let workspace_ids = vec![WorkspaceId::new(WORKSPACE_RED_ID).unwrap()];
        let created = service
            .create(
                &inviter,
                &authorized_grants(&store, &inviter, &workspace_ids).await,
                InvitationCreateParams::new(workspace_ids).unwrap(),
            )
            .await
            .unwrap();
        let credential_key = secrets
            .load_or_create_auth_credential_hmac_key(INVITATION_CREDENTIAL_KEY_MIN_BYTES)
            .unwrap();
        let credentials = OpaqueCredentialFactory::new(credential_key.as_bytes()).unwrap();
        let auth_service = invitation_auth_service(&harness, secrets.as_ref());

        let error = InvitationService::accept_restricted(
            &harness.database,
            &credentials,
            &auth_service,
            &inviter.gateway_id,
            created.presentation.token(),
            accept_params("member-a"),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            InvitationAcceptServiceError::Corrective(InvitationErrorReason::NicknameUnavailable)
        ));
        let pending = invitation::Entity::find_by_id(created.invitation.invitation_id.to_string())
            .one(&harness.database)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(pending.status, "pending");
        assert_eq!(pending.token_hash.as_ref().map(Vec::len), Some(32));
        assert_eq!(
            gateway_principal::Entity::find()
                .filter(gateway_principal::Column::NicknameKey.eq("member-a"))
                .all(&harness.database)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn corrective_profile_avatar_and_installation_errors_do_not_consume_invitation() {
        let harness = IsolatedEpic4Harness::populated().await.unwrap();
        let store = CrudStore::new(harness.database.clone());
        let secrets = Arc::new(GatewaySecrets::new(Arc::new(MemorySecretStore::new())));
        let service =
            InvitationService::new(store.clone(), secrets.clone(), "127.0.0.1:17878").unwrap();
        let inviter = member_a();
        let workspace_ids = vec![WorkspaceId::new(WORKSPACE_RED_ID).unwrap()];
        let created = service
            .create(
                &inviter,
                &authorized_grants(&store, &inviter, &workspace_ids).await,
                InvitationCreateParams::new(workspace_ids).unwrap(),
            )
            .await
            .unwrap();
        let credential_key = secrets
            .load_or_create_auth_credential_hmac_key(INVITATION_CREDENTIAL_KEY_MIN_BYTES)
            .unwrap();
        let credentials = OpaqueCredentialFactory::new(credential_key.as_bytes()).unwrap();
        let auth_service = invitation_auth_service(&harness, secrets.as_ref());

        let mut invalid_profile = accept_params("corrective-profile");
        invalid_profile.profile.display_name = "bad\nname".to_owned();
        let mut invalid_avatar = accept_params("corrective-avatar");
        invalid_avatar.profile.avatar =
            Some(ProfileAvatarInput::new(ProfileAvatarMediaType::Png, "not-base64").unwrap());
        let mut invalid_installation = accept_params("corrective-installation");
        invalid_installation.installation.installation_id = "\0".to_owned();

        for (params, expected) in [
            (invalid_profile, InvitationErrorReason::InvalidProfile),
            (invalid_avatar, InvitationErrorReason::AvatarInvalid),
            (
                invalid_installation,
                InvitationErrorReason::InvalidInstallation,
            ),
        ] {
            let error = InvitationService::accept_restricted(
                &harness.database,
                &credentials,
                &auth_service,
                &inviter.gateway_id,
                created.presentation.token(),
                params,
            )
            .await
            .unwrap_err();
            assert!(matches!(
                error,
                InvitationAcceptServiceError::Corrective(reason) if reason == expected
            ));
        }

        let pending = invitation::Entity::find_by_id(created.invitation.invitation_id.to_string())
            .one(&harness.database)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(pending.status, "pending");
        assert_eq!(pending.token_hash.as_ref().map(Vec::len), Some(32));
        assert!(
            gateway_principal::Entity::find()
                .filter(gateway_principal::Column::NicknameKey.is_in([
                    "corrective-profile",
                    "corrective-avatar",
                    "corrective-installation",
                ]),)
                .all(&harness.database)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn first_session_failure_rolls_back_every_accept_row() {
        let harness = IsolatedEpic4Harness::populated().await.unwrap();
        let store = CrudStore::new(harness.database.clone());
        let secrets = Arc::new(GatewaySecrets::new(Arc::new(MemorySecretStore::new())));
        let service =
            InvitationService::new(store.clone(), secrets.clone(), "127.0.0.1:17878").unwrap();
        let inviter = member_a();
        let workspace_ids = vec![WorkspaceId::new(WORKSPACE_RED_ID).unwrap()];
        let created = service
            .create(
                &inviter,
                &authorized_grants(&store, &inviter, &workspace_ids).await,
                InvitationCreateParams::new(workspace_ids).unwrap(),
            )
            .await
            .unwrap();
        let credential_key = secrets
            .load_or_create_auth_credential_hmac_key(INVITATION_CREDENTIAL_KEY_MIN_BYTES)
            .unwrap();
        let credentials = OpaqueCredentialFactory::new(credential_key.as_bytes()).unwrap();
        let auth_service = invitation_auth_service(&harness, secrets.as_ref());
        auth_service.set_activation_failpoint(3);

        assert!(
            InvitationService::accept_restricted(
                &harness.database,
                &credentials,
                &auth_service,
                &inviter.gateway_id,
                created.presentation.token(),
                accept_params("rollback-member"),
            )
            .await
            .is_err()
        );
        let pending = invitation::Entity::find_by_id(created.invitation.invitation_id.to_string())
            .one(&harness.database)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(pending.status, "pending");
        assert!(pending.token_hash.is_some());
        assert!(
            gateway_principal::Entity::find()
                .filter(gateway_principal::Column::NicknameKey.eq("rollback-member"))
                .all(&harness.database)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            audit_event::Entity::find()
                .filter(audit_event::Column::Action.eq("invitation_accepted"))
                .all(&harness.database)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn accept_reauthorizes_all_grants_and_terminally_revokes_lost_authority() {
        let harness = IsolatedEpic4Harness::populated().await.unwrap();
        let store = CrudStore::new(harness.database.clone());
        let secrets = Arc::new(GatewaySecrets::new(Arc::new(MemorySecretStore::new())));
        let service =
            InvitationService::new(store.clone(), secrets.clone(), "127.0.0.1:17878").unwrap();
        let inviter = member_a();
        let workspace_ids = vec![WorkspaceId::new(WORKSPACE_RED_ID).unwrap()];
        let created = service
            .create(
                &inviter,
                &authorized_grants(&store, &inviter, &workspace_ids).await,
                InvitationCreateParams::new(workspace_ids).unwrap(),
            )
            .await
            .unwrap();
        workspace_membership::Entity::delete_many()
            .filter(workspace_membership::Column::PrincipalId.eq(MEMBER_A_ID))
            .filter(workspace_membership::Column::WorkspaceId.eq(WORKSPACE_RED_ID))
            .exec(&harness.database)
            .await
            .unwrap();
        let credential_key = secrets
            .load_or_create_auth_credential_hmac_key(INVITATION_CREDENTIAL_KEY_MIN_BYTES)
            .unwrap();
        let credentials = OpaqueCredentialFactory::new(credential_key.as_bytes()).unwrap();
        let auth_service = invitation_auth_service(&harness, secrets.as_ref());

        assert!(matches!(
            InvitationService::accept_restricted(
                &harness.database,
                &credentials,
                &auth_service,
                &inviter.gateway_id,
                created.presentation.token(),
                accept_params("lost-authority-member"),
            )
            .await,
            Err(InvitationAcceptServiceError::Unavailable)
        ));
        let revoked = invitation::Entity::find_by_id(created.invitation.invitation_id.to_string())
            .one(&harness.database)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(revoked.status, "revoked");
        assert_eq!(
            revoked.revoke_reason.as_deref(),
            Some("grant_authority_lost")
        );
        assert!(revoked.token_hash.is_none());
        assert!(
            gateway_principal::Entity::find()
                .filter(gateway_principal::Column::NicknameKey.eq("lost-authority-member"))
                .all(&harness.database)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            audit_event::Entity::find()
                .filter(audit_event::Column::Action.eq("invitation_revoked"))
                .all(&harness.database)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn accept_reloads_inviter_status_and_cannot_commit_after_suspension() {
        let harness = IsolatedEpic4Harness::populated().await.unwrap();
        let store = CrudStore::new(harness.database.clone());
        let secrets = Arc::new(GatewaySecrets::new(Arc::new(MemorySecretStore::new())));
        let service =
            InvitationService::new(store.clone(), secrets.clone(), "127.0.0.1:17878").unwrap();
        let inviter = member_a();
        let workspace_ids = vec![WorkspaceId::new(WORKSPACE_RED_ID).unwrap()];
        let created = service
            .create(
                &inviter,
                &authorized_grants(&store, &inviter, &workspace_ids).await,
                InvitationCreateParams::new(workspace_ids).unwrap(),
            )
            .await
            .unwrap();
        gateway_principal::Entity::update_many()
            .col_expr(gateway_principal::Column::Status, Expr::value("suspended"))
            .filter(gateway_principal::Column::Id.eq(inviter.principal_id.to_string()))
            .exec(&harness.database)
            .await
            .unwrap();
        let credential_key = secrets
            .load_or_create_auth_credential_hmac_key(INVITATION_CREDENTIAL_KEY_MIN_BYTES)
            .unwrap();
        let credentials = OpaqueCredentialFactory::new(credential_key.as_bytes()).unwrap();
        let auth_service = invitation_auth_service(&harness, secrets.as_ref());

        assert!(matches!(
            InvitationService::accept_restricted(
                &harness.database,
                &credentials,
                &auth_service,
                &inviter.gateway_id,
                created.presentation.token(),
                accept_params("suspended-inviter-member"),
            )
            .await,
            Err(InvitationAcceptServiceError::Unavailable)
        ));
        let revoked = invitation::Entity::find_by_id(created.invitation.invitation_id.to_string())
            .one(&harness.database)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(revoked.status, "revoked");
        assert_eq!(
            revoked.revoke_reason.as_deref(),
            Some("inviter_unavailable")
        );
        assert!(revoked.token_hash.is_none());
        assert!(
            gateway_principal::Entity::find()
                .filter(gateway_principal::Column::NicknameKey.eq("suspended-inviter-member"))
                .all(&harness.database)
                .await
                .unwrap()
                .is_empty()
        );
    }

    async fn assert_one_concurrent_accept_winner(contenders: usize) {
        let harness = IsolatedEpic4Harness::populated().await.unwrap();
        let store = CrudStore::new(harness.database.clone());
        let secrets = Arc::new(GatewaySecrets::new(Arc::new(MemorySecretStore::new())));
        let service =
            InvitationService::new(store.clone(), secrets.clone(), "127.0.0.1:17878").unwrap();
        let inviter = member_a();
        let workspace_ids = vec![WorkspaceId::new(WORKSPACE_RED_ID).unwrap()];
        let created = service
            .create(
                &inviter,
                &authorized_grants(&store, &inviter, &workspace_ids).await,
                InvitationCreateParams::new(workspace_ids).unwrap(),
            )
            .await
            .unwrap();
        let credential_key = secrets
            .load_or_create_auth_credential_hmac_key(INVITATION_CREDENTIAL_KEY_MIN_BYTES)
            .unwrap();
        let credentials =
            Arc::new(OpaqueCredentialFactory::new(credential_key.as_bytes()).unwrap());
        let auth_service = Arc::new(invitation_auth_service(&harness, secrets.as_ref()));
        let raw_credential = created.presentation.token().to_owned();
        let gateway_id = inviter.gateway_id.clone();
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..contenders {
            let database = harness.database.clone();
            let credentials = credentials.clone();
            let auth_service = auth_service.clone();
            let raw_credential = raw_credential.clone();
            let gateway_id = gateway_id.clone();
            tasks.spawn(async move {
                InvitationService::accept_restricted(
                    &database,
                    credentials.as_ref(),
                    auth_service.as_ref(),
                    &gateway_id,
                    raw_credential.as_str(),
                    accept_params("concurrent-member"),
                )
                .await
            });
        }
        let mut successes = 0;
        while let Some(result) = tasks.join_next().await {
            if result.unwrap().is_ok() {
                successes += 1;
            }
        }
        assert_eq!(successes, 1, "contenders={contenders}");
        let principals = gateway_principal::Entity::find()
            .filter(gateway_principal::Column::NicknameKey.eq("concurrent-member"))
            .all(&harness.database)
            .await
            .unwrap();
        assert_eq!(principals.len(), 1, "contenders={contenders}");
        let principal_id = principals[0].id.clone();
        assert_eq!(
            workspace_membership::Entity::find()
                .filter(workspace_membership::Column::PrincipalId.eq(principal_id.clone()))
                .all(&harness.database)
                .await
                .unwrap()
                .len(),
            1,
            "contenders={contenders}"
        );
        assert_eq!(
            device::Entity::find()
                .filter(device::Column::PrincipalId.eq(principal_id.clone()))
                .all(&harness.database)
                .await
                .unwrap()
                .len(),
            1,
            "contenders={contenders}"
        );
        assert_eq!(
            auth_session::Entity::find()
                .filter(auth_session::Column::PrincipalId.eq(principal_id))
                .all(&harness.database)
                .await
                .unwrap()
                .len(),
            1,
            "contenders={contenders}"
        );
        assert_eq!(
            audit_event::Entity::find()
                .filter(audit_event::Column::Action.eq("invitation_accepted"))
                .all(&harness.database)
                .await
                .unwrap()
                .len(),
            1,
            "contenders={contenders}"
        );
    }

    #[tokio::test]
    async fn two_ten_and_many_concurrent_accepts_have_exactly_one_winner() {
        for contenders in [2, 10, 32] {
            assert_one_concurrent_accept_winner(contenders).await;
        }
    }

    #[tokio::test]
    async fn concurrent_accept_and_revoke_commit_exactly_one_terminal_outcome() {
        let harness = IsolatedEpic4Harness::populated().await.unwrap();
        seed_superuser_session(&harness.database).await;
        let store = CrudStore::new(harness.database.clone());
        let secrets = Arc::new(GatewaySecrets::new(Arc::new(MemorySecretStore::new())));
        let service =
            InvitationService::new(store.clone(), secrets.clone(), "127.0.0.1:17878").unwrap();
        let inviter = member_a();
        let workspace_ids = vec![WorkspaceId::new(WORKSPACE_RED_ID).unwrap()];
        let created = service
            .create(
                &inviter,
                &authorized_grants(&store, &inviter, &workspace_ids).await,
                InvitationCreateParams::new(workspace_ids).unwrap(),
            )
            .await
            .unwrap();
        let raw_credential = created.presentation.token().to_owned();
        let invitation_id = created.invitation.invitation_id;
        let credential_key = secrets
            .load_or_create_auth_credential_hmac_key(INVITATION_CREDENTIAL_KEY_MIN_BYTES)
            .unwrap();
        let credentials = OpaqueCredentialFactory::new(credential_key.as_bytes()).unwrap();
        let auth_service = invitation_auth_service(&harness, secrets.as_ref());
        let actor = superuser();
        let revoke_proof = authorized_invitation(&store, &actor, &invitation_id).await;

        let accept = InvitationService::accept_restricted(
            &harness.database,
            &credentials,
            &auth_service,
            &inviter.gateway_id,
            raw_credential.as_str(),
            accept_params("accept-revoke-race"),
        );
        let revoke = service.revoke(
            &actor,
            &revoke_proof,
            InvitationRevokeParams {
                invitation_id: invitation_id.clone(),
            },
        );
        let (accepted, revoked) = tokio::join!(accept, revoke);
        let accepted = accepted.ok();
        let revoked = revoked.unwrap();
        assert_eq!(
            usize::from(accepted.is_some()) + usize::from(revoked.response.changed),
            1
        );

        let terminal = invitation::Entity::find_by_id(invitation_id.to_string())
            .one(&harness.database)
            .await
            .unwrap()
            .unwrap();
        assert!(terminal.token_hash.is_none());
        assert_eq!(
            terminal.status,
            if accepted.is_some() {
                "accepted"
            } else {
                "revoked"
            }
        );
        assert_eq!(
            gateway_principal::Entity::find()
                .filter(gateway_principal::Column::NicknameKey.eq("accept-revoke-race"))
                .all(&harness.database)
                .await
                .unwrap()
                .len(),
            usize::from(accepted.is_some())
        );
        let audits = audit_event::Entity::find()
            .all(&harness.database)
            .await
            .unwrap();
        assert_eq!(
            audits
                .iter()
                .filter(|event| {
                    matches!(
                        event.action.as_str(),
                        "invitation_accepted" | "invitation_revoked"
                    )
                })
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn concurrent_accept_and_inviter_access_loss_follow_commit_order() {
        let harness = IsolatedEpic4Harness::populated().await.unwrap();
        seed_superuser_session(&harness.database).await;
        let store = CrudStore::new(harness.database.clone());
        let secrets = Arc::new(GatewaySecrets::new(Arc::new(MemorySecretStore::new())));
        let invitation_service =
            InvitationService::new(store.clone(), secrets.clone(), "127.0.0.1:17878").unwrap();
        let member_service = MemberService::new(store.clone(), secrets.clone());
        let inviter = member_a();
        let workspace_id = WorkspaceId::new(WORKSPACE_RED_ID).unwrap();
        let created = invitation_service
            .create(
                &inviter,
                &authorized_grants(&store, &inviter, std::slice::from_ref(&workspace_id)).await,
                InvitationCreateParams::new(vec![workspace_id.clone()]).unwrap(),
            )
            .await
            .unwrap();
        let raw_credential = created.presentation.token().to_owned();
        let invitation_id = created.invitation.invitation_id;
        let credential_key = secrets
            .load_or_create_auth_credential_hmac_key(INVITATION_CREDENTIAL_KEY_MIN_BYTES)
            .unwrap();
        let credentials = OpaqueCredentialFactory::new(credential_key.as_bytes()).unwrap();
        let auth_service = invitation_auth_service(&harness, secrets.as_ref());
        let actor = superuser();
        let remove_proof = authorized_workspace(
            &store,
            &actor,
            ResourceAction::WorkspaceMemberRemove,
            &workspace_id,
        )
        .await;

        let accept = InvitationService::accept_restricted(
            &harness.database,
            &credentials,
            &auth_service,
            &inviter.gateway_id,
            raw_credential.as_str(),
            accept_params("access-loss-race"),
        );
        let remove = member_service.workspace_remove(
            &actor,
            &remove_proof,
            WorkspaceMemberRemoveParams {
                workspace_id: workspace_id.clone(),
                principal_id: inviter.principal_id.clone(),
            },
        );
        let (accepted, removed) = tokio::join!(accept, remove);
        let accepted = accepted.ok();
        let removed = removed.unwrap();
        assert!(removed.response.changed);

        let terminal = invitation::Entity::find_by_id(invitation_id.to_string())
            .one(&harness.database)
            .await
            .unwrap()
            .unwrap();
        assert!(terminal.token_hash.is_none());
        assert_eq!(
            terminal.status,
            if accepted.is_some() {
                "accepted"
            } else {
                "revoked"
            }
        );
        if accepted.is_none() {
            assert_eq!(
                terminal.revoke_reason.as_deref(),
                Some("grant_authority_lost")
            );
        }
        let accepted_members = gateway_principal::Entity::find()
            .filter(gateway_principal::Column::NicknameKey.eq("access-loss-race"))
            .all(&harness.database)
            .await
            .unwrap();
        assert_eq!(accepted_members.len(), usize::from(accepted.is_some()));
        if let Some(member) = accepted_members.first() {
            assert!(
                workspace_membership::Entity::find_by_id((
                    member.id.clone(),
                    workspace_id.to_string(),
                ))
                .one(&harness.database)
                .await
                .unwrap()
                .is_some()
            );
        }
        assert!(
            workspace_membership::Entity::find_by_id((
                inviter.principal_id.to_string(),
                workspace_id.to_string(),
            ))
            .one(&harness.database)
            .await
            .unwrap()
            .is_none()
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
        assert_eq!(
            audit_event::Entity::find()
                .filter(audit_event::Column::Action.eq("invitation_accepted"))
                .all(&harness.database)
                .await
                .unwrap()
                .len(),
            usize::from(accepted.is_some())
        );
        assert_eq!(
            audit_event::Entity::find()
                .filter(audit_event::Column::Action.eq("invitation_revoked"))
                .all(&harness.database)
                .await
                .unwrap()
                .len(),
            usize::from(accepted.is_none())
        );
    }

    #[tokio::test]
    async fn concurrent_accept_and_inviter_removal_follow_commit_order() {
        let harness = IsolatedEpic4Harness::populated().await.unwrap();
        seed_superuser_session(&harness.database).await;
        let store = CrudStore::new(harness.database.clone());
        let secrets = Arc::new(GatewaySecrets::new(Arc::new(MemorySecretStore::new())));
        let invitation_service =
            InvitationService::new(store.clone(), secrets.clone(), "127.0.0.1:17878").unwrap();
        let member_service = MemberService::new(store.clone(), secrets.clone());
        let inviter = member_a();
        let workspace_id = WorkspaceId::new(WORKSPACE_RED_ID).unwrap();
        let created = invitation_service
            .create(
                &inviter,
                &authorized_grants(&store, &inviter, std::slice::from_ref(&workspace_id)).await,
                InvitationCreateParams::new(vec![workspace_id.clone()]).unwrap(),
            )
            .await
            .unwrap();
        let raw_credential = created.presentation.token().to_owned();
        let invitation_id = created.invitation.invitation_id;
        let credential_key = secrets
            .load_or_create_auth_credential_hmac_key(INVITATION_CREDENTIAL_KEY_MIN_BYTES)
            .unwrap();
        let credentials = OpaqueCredentialFactory::new(credential_key.as_bytes()).unwrap();
        let auth_service = invitation_auth_service(&harness, secrets.as_ref());
        let actor = superuser();
        let remove_proof = authorized_member_principal(
            &store,
            &actor,
            ResourceAction::MemberRemove,
            &inviter.principal_id,
        )
        .await;

        let accept = InvitationService::accept_restricted(
            &harness.database,
            &credentials,
            &auth_service,
            &inviter.gateway_id,
            raw_credential.as_str(),
            accept_params("inviter-removal-race"),
        );
        let remove = member_service.remove(
            &actor,
            &remove_proof,
            MemberRemoveParams {
                principal_id: inviter.principal_id.clone(),
                expected_status: None,
            },
        );
        let (accepted, removed) = tokio::join!(accept, remove);
        let accepted = accepted.ok();
        let removed = removed.unwrap();
        assert!(removed.response.changed);
        assert_eq!(removed.response.member.status, PrincipalStatus::Removed);
        assert_eq!(
            removed.changed_invitation_ids,
            if accepted.is_some() {
                Vec::new()
            } else {
                vec![invitation_id.clone()]
            }
        );

        let terminal = invitation::Entity::find_by_id(invitation_id.to_string())
            .one(&harness.database)
            .await
            .unwrap()
            .unwrap();
        assert!(terminal.token_hash.is_none());
        assert_eq!(
            terminal.status,
            if accepted.is_some() {
                "accepted"
            } else {
                "revoked"
            }
        );
        if accepted.is_none() {
            assert_eq!(
                terminal.revoke_reason.as_deref(),
                Some("inviter_unavailable")
            );
        }
        assert_eq!(
            gateway_principal::Entity::find()
                .filter(gateway_principal::Column::NicknameKey.eq("inviter-removal-race"))
                .all(&harness.database)
                .await
                .unwrap()
                .len(),
            usize::from(accepted.is_some())
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
        assert_eq!(
            audit_event::Entity::find()
                .filter(audit_event::Column::Action.eq("invitation_accepted"))
                .all(&harness.database)
                .await
                .unwrap()
                .len(),
            usize::from(accepted.is_some())
        );
    }

    #[tokio::test]
    async fn accept_loses_to_committed_expiry_without_partial_identity() {
        let harness = IsolatedEpic4Harness::populated().await.unwrap();
        let store = CrudStore::new(harness.database.clone());
        let secrets = Arc::new(GatewaySecrets::new(Arc::new(MemorySecretStore::new())));
        let service =
            InvitationService::new(store.clone(), secrets.clone(), "127.0.0.1:17878").unwrap();
        let inviter = member_a();
        let workspace_ids = vec![WorkspaceId::new(WORKSPACE_RED_ID).unwrap()];
        let created = service
            .create(
                &inviter,
                &authorized_grants(&store, &inviter, &workspace_ids).await,
                InvitationCreateParams::new(workspace_ids).unwrap(),
            )
            .await
            .unwrap();
        let invitation_id = created.invitation.invitation_id;
        let raw_credential = created.presentation.token().to_owned();
        let now = chrono::Utc::now().fixed_offset();
        invitation::Entity::update_many()
            .col_expr(
                invitation::Column::CreatedAt,
                Expr::value(now - chrono::Duration::seconds(2)),
            )
            .col_expr(
                invitation::Column::ExpiresAt,
                Expr::value(now - chrono::Duration::seconds(1)),
            )
            .filter(invitation::Column::Id.eq(invitation_id.to_string()))
            .exec(&harness.database)
            .await
            .unwrap();
        let credential_key = secrets
            .load_or_create_auth_credential_hmac_key(INVITATION_CREDENTIAL_KEY_MIN_BYTES)
            .unwrap();
        let credentials = OpaqueCredentialFactory::new(credential_key.as_bytes()).unwrap();
        let auth_service = invitation_auth_service(&harness, secrets.as_ref());

        assert!(matches!(
            InvitationService::accept_restricted(
                &harness.database,
                &credentials,
                &auth_service,
                &inviter.gateway_id,
                raw_credential.as_str(),
                accept_params("expired-accept"),
            )
            .await,
            Err(InvitationAcceptServiceError::Unavailable)
        ));
        let expired = invitation::Entity::find_by_id(invitation_id.to_string())
            .one(&harness.database)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(expired.status, "expired");
        assert!(expired.token_hash.is_none());
        assert!(
            gateway_principal::Entity::find()
                .filter(gateway_principal::Column::NicknameKey.eq("expired-accept"))
                .all(&harness.database)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            audit_event::Entity::find()
                .filter(audit_event::Column::Action.eq("invitation_expired"))
                .all(&harness.database)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn malformed_unknown_and_terminal_credentials_share_unavailable_preview() {
        let harness = IsolatedEpic4Harness::populated().await.unwrap();
        let key = AuthKeyMaterial::from_test_bytes(vec![9; 64]);
        let credentials = OpaqueCredentialFactory::new(key.as_bytes()).unwrap();
        let gateway_id = GatewayId::new("G00000000000000000001").unwrap();
        for credential in [
            "not-an-invitation".to_owned(),
            credentials.generate_invitation().expose_secret().to_owned(),
        ] {
            assert!(
                InvitationService::preview_restricted(
                    &harness.database,
                    &credentials,
                    &gateway_id,
                    credential.as_str(),
                    InvitationTransportSecurity::SecureWss,
                )
                .await
                .is_err()
            );
        }
    }
}
