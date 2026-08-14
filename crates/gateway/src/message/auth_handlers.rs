use pioneer_protocol::{
    AuthProfileUpdateParams, AuthSessionRevokeParams, AuthSessionTerminationReason,
    AuthorizationCapabilitiesParams, JsonRpcError, JsonRpcErrorResponse, JsonRpcResponse,
    RequestId,
};
use serde::Serialize;
use serde_json::json;
use std::time::Duration;

use super::MessageProcessor;
use crate::authorization::{
    AuthorizationCapabilitySnapshotService, AuthorizationResolver, AuthorizationResource,
    AuthorizedSession, ResourceAction,
};
use crate::request_context::RequestContext;

const AUTH_ERROR_JSONRPC_CODE: i64 = -32040;
const AUTH_RESPONSE_ENQUEUE_TIMEOUT: Duration = Duration::from_millis(250);

impl MessageProcessor {
    pub(in crate::message) async fn authorization_capabilities(
        &self,
        context: &RequestContext,
        authorization: &AuthorizedSession,
        request_id: RequestId,
        params: AuthorizationCapabilitiesParams,
    ) {
        if !authorized_session_matches(
            context,
            authorization,
            ResourceAction::SessionReadOwn,
            &context.principal().session_id,
        ) {
            self.send_auth_error(context, request_id, "authorization_unavailable")
                .await;
            return;
        }
        let invalid_scope = params
            .workspace_id
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
            || params
                .thread_id
                .as_deref()
                .is_some_and(|value| value.trim().is_empty())
            || (params.thread_id.is_some() && params.workspace_id.is_none());
        if invalid_scope {
            self.send_error(
                context.connection_id(),
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    pioneer_protocol::INVALID_PARAMS_CODE,
                    "capability scope must contain non-empty identifiers and thread_id requires workspace_id",
                ),
            )
            .await;
            return;
        }
        let service = AuthorizationCapabilitySnapshotService::new(AuthorizationResolver::new(
            self.crud_store.as_ref().clone(),
        ));
        let mut result = None;
        for _ in 0..2 {
            let revision = match self.current_authorization_revision().await {
                Ok(revision) => revision,
                Err(error) => {
                    tracing::warn!(
                        error = %format!("{error:#}"),
                        "capability snapshot policy generation is unavailable"
                    );
                    break;
                }
            };
            let snapshot = service
                .snapshot(context.principal(), params.clone(), revision)
                .await;
            let current_revision = match self.current_authorization_revision().await {
                Ok(revision) => revision,
                Err(error) => {
                    tracing::warn!(
                        error = %format!("{error:#}"),
                        "capability snapshot consistency fence is unavailable"
                    );
                    break;
                }
            };
            if current_revision == revision {
                result = Some(snapshot);
                break;
            }
        }
        match result.unwrap_or_else(|| {
            Err(anyhow::anyhow!(
                "authorization changed while capability snapshot was being built"
            ))
        }) {
            Ok(response) => self.send_auth_result(context, request_id, &response).await,
            Err(error) => {
                tracing::warn!(error = %format!("{error:#}"), "capability snapshot failed");
                self.send_auth_error(context, request_id, "authorization_unavailable")
                    .await;
            }
        }
    }

    pub(in crate::message) async fn auth_me(
        &self,
        context: &RequestContext,
        authorization: &AuthorizedSession,
        request_id: RequestId,
    ) {
        if !authorized_session_matches(
            context,
            authorization,
            ResourceAction::SessionReadOwn,
            &context.principal().session_id,
        ) {
            self.send_auth_error(context, request_id, "authorization_unavailable")
                .await;
            return;
        }
        let Some(service) = self.auth_service.as_ref() else {
            self.send_error(
                context.connection_id(),
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    AUTH_ERROR_JSONRPC_CODE,
                    "auth_not_ready",
                ),
            )
            .await;
            return;
        };
        match service.auth_me(context.principal()).await {
            Ok(response) => self.send_auth_result(context, request_id, &response).await,
            Err(error) => {
                self.send_error(
                    context.connection_id(),
                    JsonRpcErrorResponse {
                        jsonrpc: pioneer_protocol::JSONRPC_VERSION.to_owned(),
                        id: Some(request_id),
                        error: JsonRpcError {
                            code: AUTH_ERROR_JSONRPC_CODE,
                            message: error.code().as_str().to_owned(),
                            data: Some(json!({ "code": error.code().as_str() })),
                        },
                    },
                )
                .await;
            }
        }
    }

    pub(in crate::message) async fn auth_session_list(
        &self,
        context: &RequestContext,
        authorization: &AuthorizedSession,
        request_id: RequestId,
    ) {
        if !authorized_session_matches(
            context,
            authorization,
            ResourceAction::SessionReadOwn,
            &context.principal().session_id,
        ) {
            self.send_auth_error(context, request_id, "authorization_unavailable")
                .await;
            return;
        }
        let Some(service) = self.auth_service.as_ref() else {
            self.send_auth_error(context, request_id, "auth_not_ready")
                .await;
            return;
        };
        match service.list_sessions(context.principal()).await {
            Ok(response) => self.send_auth_result(context, request_id, &response).await,
            Err(error) => {
                self.send_auth_error(context, request_id, error.code().as_str())
                    .await
            }
        }
    }

    pub(in crate::message) async fn auth_profile_update(
        &self,
        context: &RequestContext,
        authorization: &AuthorizedSession,
        request_id: RequestId,
        params: AuthProfileUpdateParams,
    ) {
        if !authorized_session_matches(
            context,
            authorization,
            ResourceAction::ProfileUpdateOwn,
            &context.principal().session_id,
        ) {
            self.send_auth_error(context, request_id, "authorization_unavailable")
                .await;
            return;
        }
        let Some(service) = self.auth_service.as_ref() else {
            self.send_auth_error(context, request_id, "auth_not_ready")
                .await;
            return;
        };
        match service.update_profile(context.principal(), params).await {
            Ok(response) => {
                let changed = response.changed;
                self.send_auth_result(context, request_id, &response).await;
                if changed {
                    self.publish_profile_change(&context.principal().principal_id)
                        .await;
                }
            }
            Err(error) => {
                self.send_auth_error(context, request_id, error.code().as_str())
                    .await
            }
        }
    }

    pub(in crate::message) async fn auth_session_revoke(
        &self,
        context: &RequestContext,
        authorization: &AuthorizedSession,
        request_id: RequestId,
        params: AuthSessionRevokeParams,
    ) {
        if !authorized_session_matches(
            context,
            authorization,
            ResourceAction::SessionRevokeOwn,
            &params.session_id,
        ) {
            self.send_auth_error(context, request_id, "authorization_unavailable")
                .await;
            return;
        }
        let Some(service) = self.auth_service.as_ref() else {
            self.send_auth_error(context, request_id, "auth_not_ready")
                .await;
            return;
        };
        match service
            .revoke_owned_session(
                context.principal(),
                &params.session_id,
                params.expected_status,
                pioneer_protocol::AuthSessionRevokeReason::SelfRevoke,
            )
            .await
        {
            Ok(response) => {
                self.send_auth_result(context, request_id, &response).await;
                if response.revoked {
                    service
                        .disconnect_committed_session(
                            &response.session_id,
                            AuthSessionTerminationReason::SessionRevoked,
                        )
                        .await;
                }
            }
            Err(error) => {
                self.send_auth_error(context, request_id, error.code().as_str())
                    .await
            }
        }
    }

    pub(in crate::message) async fn auth_logout(
        &self,
        context: &RequestContext,
        authorization: &AuthorizedSession,
        request_id: RequestId,
    ) {
        if !authorized_session_matches(
            context,
            authorization,
            ResourceAction::SessionRevokeOwn,
            &context.principal().session_id,
        ) {
            self.send_auth_error(context, request_id, "authorization_unavailable")
                .await;
            return;
        }
        let Some(service) = self.auth_service.as_ref() else {
            self.send_auth_error(context, request_id, "auth_not_ready")
                .await;
            return;
        };
        match service.logout(context.principal()).await {
            Ok(response) => {
                self.send_auth_result(context, request_id, &response).await;
                if response.revoked {
                    service
                        .disconnect_committed_session(
                            &response.session_id,
                            AuthSessionTerminationReason::SessionRevoked,
                        )
                        .await;
                }
            }
            Err(error) => {
                self.send_auth_error(context, request_id, error.code().as_str())
                    .await
            }
        }
    }

    pub(in crate::message) async fn auth_device_create(
        &self,
        context: &RequestContext,
        authorization: &AuthorizedSession,
        request_id: RequestId,
    ) {
        if !authorized_session_matches(
            context,
            authorization,
            ResourceAction::SessionRevokeOwn,
            &context.principal().session_id,
        ) {
            self.send_auth_error(context, request_id, "authorization_unavailable")
                .await;
            return;
        }
        let Some(service) = self.auth_service.as_ref() else {
            self.send_auth_error(context, request_id, "auth_not_ready")
                .await;
            return;
        };
        match service.create_device(context.principal()).await {
            Ok(response) => self.send_auth_result(context, request_id, &response).await,
            Err(error) => {
                self.send_auth_error(context, request_id, error.code().as_str())
                    .await
            }
        }
    }

    async fn send_auth_result<T: Serialize>(
        &self,
        context: &RequestContext,
        request_id: RequestId,
        result: &T,
    ) {
        let payload = JsonRpcResponse::from_result(request_id.clone(), result)
            .and_then(|response| serde_json::to_string(&response).map_err(Into::into));
        match payload {
            Ok(payload) => {
                let _ = tokio::time::timeout(
                    AUTH_RESPONSE_ENQUEUE_TIMEOUT,
                    self.session_manager
                        .send_text(context.connection_id(), payload),
                )
                .await;
            }
            Err(_) => {
                self.send_auth_error(context, request_id, "auth_response_failed")
                    .await;
            }
        }
    }

    async fn send_auth_error(&self, context: &RequestContext, request_id: RequestId, code: &str) {
        self.send_error(
            context.connection_id(),
            JsonRpcErrorResponse {
                jsonrpc: pioneer_protocol::JSONRPC_VERSION.to_owned(),
                id: Some(request_id),
                error: JsonRpcError {
                    code: AUTH_ERROR_JSONRPC_CODE,
                    message: code.to_owned(),
                    data: Some(json!({ "code": code })),
                },
            },
        )
        .await;
    }
}

fn authorized_session_matches(
    context: &RequestContext,
    authorization: &AuthorizedSession,
    action: ResourceAction,
    session_id: &pioneer_protocol::AuthSessionId,
) -> bool {
    authorization.principal_id() == &context.principal().principal_id
        && authorization.action() == action
        && matches!(
            authorization.resource(),
            AuthorizationResource::Session {
                principal_id,
                session_id: authorized_session_id,
            } if principal_id == &context.principal().principal_id
                && authorized_session_id == session_id
        )
        && authorization.decision().is_allowed()
}
