use pioneer_protocol::{
    AuthSessionRevokeParams, AuthSessionTerminationReason, JsonRpcError, JsonRpcErrorResponse,
    JsonRpcResponse, RequestId,
};
use serde::Serialize;
use serde_json::json;
use std::time::Duration;

use super::MessageProcessor;
use crate::request_context::RequestContext;

const AUTH_ERROR_JSONRPC_CODE: i64 = -32040;
const AUTH_RESPONSE_ENQUEUE_TIMEOUT: Duration = Duration::from_millis(250);

impl MessageProcessor {
    pub(in crate::message) async fn auth_me(
        &self,
        context: &RequestContext,
        request_id: RequestId,
    ) {
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
        request_id: RequestId,
    ) {
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

    pub(in crate::message) async fn auth_session_revoke(
        &self,
        context: &RequestContext,
        request_id: RequestId,
        params: AuthSessionRevokeParams,
    ) {
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
        request_id: RequestId,
    ) {
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
        request_id: RequestId,
    ) {
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
