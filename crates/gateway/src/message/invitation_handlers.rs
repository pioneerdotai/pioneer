use pioneer_protocol::{
    INVALID_PARAMS_CODE, INVALID_REQUEST_CODE, InvitationCreateParams, InvitationListParams,
    InvitationRevokeParams, JsonRpcErrorResponse, JsonRpcResponse, RequestId,
};
use tracing::warn;

use crate::authorization::{
    AuthorizationExternalError, AuthorizedInvitation, AuthorizedInvitationCollection,
    AuthorizedInvitationGrants, external_error_for_decision,
};
use crate::invitation::{InvitationService, InvitationServiceError};
use crate::request_context::RequestContext;

use super::MessageProcessor;

impl MessageProcessor {
    pub(in crate::message) async fn invitation_create(
        &self,
        context: &RequestContext,
        authorization: &AuthorizedInvitationGrants,
        request_id: RequestId,
        params: InvitationCreateParams,
    ) {
        let service = match InvitationService::with_rate_limits(
            (*self.crud_store).clone(),
            self.gateway_secrets.clone(),
            self.invitation_gateway_base_url.as_ref().clone(),
            self.epic5_rate_limits.clone(),
        ) {
            Ok(service) => service,
            Err(error) => {
                self.send_invitation_service_error(context, request_id, error)
                    .await;
                return;
            }
        };
        match service
            .create(context.principal(), authorization, params)
            .await
        {
            Ok(result) => {
                let revision = self
                    .authorization_invalidation_hub
                    .advance_snapshot_revision();
                self.send_scoped_invitation_changed_notification(
                    &result.invitation.invitation_id,
                    revision,
                )
                .await;
                match JsonRpcResponse::from_result(request_id.clone(), &result) {
                    Ok(response) => {
                        if let Err(error) = self.send_json(context.connection_id(), &response).await
                        {
                            warn!(
                                error = %format!("{error:#}"),
                                "failed to enqueue invitation create response"
                            );
                        }
                    }
                    Err(_) => {
                        self.send_error(
                            context.connection_id(),
                            AuthorizationExternalError::Unavailable.response(request_id),
                        )
                        .await;
                    }
                }
            }
            Err(error) => {
                self.send_invitation_service_error(context, request_id, error)
                    .await;
            }
        }
    }

    pub(in crate::message) async fn invitation_list(
        &self,
        context: &RequestContext,
        authorization: &AuthorizedInvitationCollection,
        request_id: RequestId,
        params: InvitationListParams,
    ) {
        let service = match self.invitation_service() {
            Ok(service) => service,
            Err(error) => {
                self.send_invitation_service_error(context, request_id, error)
                    .await;
                return;
            }
        };
        match service
            .list(context.principal(), authorization, params)
            .await
        {
            Ok(committed) => {
                for invitation_id in &committed.changed_invitation_ids {
                    let revision = self
                        .authorization_invalidation_hub
                        .advance_snapshot_revision();
                    self.send_scoped_invitation_changed_notification(invitation_id, revision)
                        .await;
                }
                self.send_invitation_result(context, request_id, &committed.response)
                    .await
            }
            Err(error) => {
                self.send_invitation_service_error(context, request_id, error)
                    .await;
            }
        }
    }

    pub(in crate::message) async fn invitation_revoke(
        &self,
        context: &RequestContext,
        authorization: &AuthorizedInvitation,
        request_id: RequestId,
        params: InvitationRevokeParams,
    ) {
        let service = match self.invitation_service() {
            Ok(service) => service,
            Err(error) => {
                self.send_invitation_service_error(context, request_id, error)
                    .await;
                return;
            }
        };
        match service
            .revoke(context.principal(), authorization, params)
            .await
        {
            Ok(committed) => {
                if committed.notification_changed {
                    let revision = self
                        .authorization_invalidation_hub
                        .advance_snapshot_revision();
                    self.send_scoped_invitation_changed_notification(
                        &committed.response.invitation.invitation_id,
                        revision,
                    )
                    .await;
                }
                self.send_invitation_result(context, request_id, &committed.response)
                    .await
            }
            Err(InvitationServiceError::CommittedTerminalHidden(invitation_id)) => {
                let revision = self
                    .authorization_invalidation_hub
                    .advance_snapshot_revision();
                self.send_scoped_invitation_changed_notification(&invitation_id, revision)
                    .await;
                self.send_error(
                    context.connection_id(),
                    AuthorizationExternalError::NotFound.response(request_id),
                )
                .await;
            }
            Err(error) => {
                self.send_invitation_service_error(context, request_id, error)
                    .await;
            }
        }
    }

    fn invitation_service(&self) -> Result<InvitationService, InvitationServiceError> {
        InvitationService::with_rate_limits(
            (*self.crud_store).clone(),
            self.gateway_secrets.clone(),
            self.invitation_gateway_base_url.as_ref().clone(),
            self.epic5_rate_limits.clone(),
        )
    }

    async fn send_invitation_result<T: serde::Serialize>(
        &self,
        context: &RequestContext,
        request_id: RequestId,
        result: &T,
    ) {
        match JsonRpcResponse::from_result(request_id.clone(), result) {
            Ok(response) => {
                if let Err(error) = self.send_json(context.connection_id(), &response).await {
                    warn!(
                        error = %format!("{error:#}"),
                        "failed to enqueue invitation response"
                    );
                }
            }
            Err(_) => {
                self.send_error(
                    context.connection_id(),
                    AuthorizationExternalError::Unavailable.response(request_id),
                )
                .await;
            }
        }
    }

    async fn send_invitation_service_error(
        &self,
        context: &RequestContext,
        request_id: RequestId,
        error: InvitationServiceError,
    ) {
        let response = match error {
            InvitationServiceError::InvalidParams => JsonRpcErrorResponse::new(
                Some(request_id),
                INVALID_PARAMS_CODE,
                "invalid invitation parameters",
            ),
            InvitationServiceError::RateLimited => JsonRpcErrorResponse::new(
                Some(request_id),
                INVALID_REQUEST_CODE,
                "request rate limited",
            ),
            InvitationServiceError::Authorization(decision) => {
                external_error_for_decision(&decision)
                    .unwrap_or(AuthorizationExternalError::NotFound)
                    .response(request_id)
            }
            InvitationServiceError::CommittedTerminalHidden(_) => {
                AuthorizationExternalError::NotFound.response(request_id)
            }
            InvitationServiceError::Unavailable(error) => {
                warn!(
                    error = %format!("{error:#}"),
                    "invitation service request failed"
                );
                AuthorizationExternalError::Unavailable.response(request_id)
            }
        };
        self.send_error(context.connection_id(), response).await;
    }
}
