use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use axum::extract::ws::{CloseFrame, Message, WebSocket};
use pioneer_protocol::{
    INVALID_REQUEST_CODE, JSONRPC_VERSION, JsonRpcError, JsonRpcErrorResponse, JsonRpcRequest,
    JsonRpcResponse, PARSE_ERROR_CODE, PROFILE_AVATAR_MAX_BASE64_LEN, RequestId,
};
use serde_json::{Value as JsonValue, json};
use tokio::time::timeout;

use crate::auth::{AuthError, AuthErrorCode, RestrictedAdmission, RestrictedAuthContext};

pub(crate) const AUTH_DEVICE_ACTIVATE: &str =
    pioneer_protocol::constants::methods::AUTH_DEVICE_ACTIVATE;
pub(crate) const AUTH_REFRESH: &str = pioneer_protocol::constants::methods::AUTH_REFRESH;
pub(crate) const INVITE_PREVIEW: &str = pioneer_protocol::constants::methods::INVITE_PREVIEW;
pub(crate) const INVITE_ACCEPT: &str = pioneer_protocol::constants::methods::INVITE_ACCEPT;

const MAX_RESTRICTED_REQUEST_BYTES: usize = PROFILE_AVATAR_MAX_BASE64_LEN + 16 * 1024;
const MAX_RESTRICTED_RESPONSE_BYTES: usize = 256 * 1024;
const AUTH_ERROR_JSONRPC_CODE: i64 = -32040;
const CLOSE_AUTH_RESTRICTED_DONE: u16 = 4403;
const CLOSE_AUTH_INVALID_REQUEST: u16 = 4400;
const CLOSE_AUTH_TIMEOUT: u16 = 4408;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RestrictedExchangeOutcome {
    Succeeded,
    Failed,
}

#[async_trait]
pub(crate) trait RestrictedExchangeExecutor: Send + Sync {
    async fn execute(
        &self,
        admission: RestrictedAdmission,
        request: JsonRpcRequest,
    ) -> std::result::Result<JsonValue, AuthError>;
}

pub(crate) async fn run(
    mut ws: WebSocket,
    admission: RestrictedAdmission,
    deadline: Duration,
    executor: Arc<dyn RestrictedExchangeExecutor>,
) -> Result<RestrictedExchangeOutcome> {
    match timeout(deadline, run_one_exchange(&mut ws, admission, executor)).await {
        Ok(outcome) => outcome,
        Err(_) => {
            let _ = timeout(
                Duration::from_millis(250),
                close(
                    &mut ws,
                    CLOSE_AUTH_TIMEOUT,
                    AuthErrorCode::ExchangeTimeout.as_str(),
                ),
            )
            .await;
            Ok(RestrictedExchangeOutcome::Failed)
        }
    }
}

async fn run_one_exchange(
    ws: &mut WebSocket,
    admission: RestrictedAdmission,
    executor: Arc<dyn RestrictedExchangeExecutor>,
) -> Result<RestrictedExchangeOutcome> {
    let exchange_outcome = match receive_one_request(ws).await {
        Err(failure) => {
            if let Some(response) = failure.response {
                send_bounded_json(ws, &response).await?;
            }
            close(ws, failure.close_code, failure.close_reason).await?;
            RestrictedExchangeOutcome::Failed
        }
        Ok(request) => {
            let request_id = request.id.clone();
            match authorize_method(admission.context(), request.method.as_str()) {
                Ok(()) => {
                    let exchange_method = request.method.clone();
                    log_exchange_started(exchange_method.as_str());
                    let response = match executor.execute(admission, request).await {
                        Ok(result) => {
                            log_exchange_completed(exchange_method.as_str());
                            let response = JsonRpcResponse::from_result(request_id, &result)
                                .map_err(anyhow::Error::from)
                                .and_then(|value| serde_json::to_value(value).map_err(Into::into))?;
                            send_bounded_json(ws, &response).await?;
                            close(ws, CLOSE_AUTH_RESTRICTED_DONE, "auth_exchange_complete").await?;
                            return Ok(RestrictedExchangeOutcome::Succeeded);
                        }
                        Err(error) => {
                            log_exchange_failed(exchange_method.as_str(), error.code());
                            auth_error_response(Some(request_id), error.code())?
                        }
                    };
                    send_bounded_json(ws, &response).await?;
                    close(ws, CLOSE_AUTH_RESTRICTED_DONE, "auth_exchange_complete").await?;
                    RestrictedExchangeOutcome::Failed
                }
                Err(code) => {
                    let response = auth_error_response(Some(request_id), code)?;
                    send_bounded_json(ws, &response).await?;
                    close(ws, CLOSE_AUTH_RESTRICTED_DONE, code.as_str()).await?;
                    RestrictedExchangeOutcome::Failed
                }
            }
        }
    };
    Ok(exchange_outcome)
}

fn log_exchange_started(method: &str) {
    let event = match method {
        AUTH_DEVICE_ACTIVATE => "auth_device_activation_started",
        AUTH_REFRESH => "auth_refresh_started",
        INVITE_PREVIEW => "invitation_preview_started",
        INVITE_ACCEPT => "invitation_accept_started",
        _ => return,
    };
    tracing::info!(event, outcome = "started");
}

fn log_exchange_completed(method: &str) {
    let event = match method {
        AUTH_DEVICE_ACTIVATE => "auth_device_activation_completed",
        AUTH_REFRESH => "auth_refresh_completed",
        INVITE_PREVIEW => "invitation_preview_completed",
        INVITE_ACCEPT => "invitation_accept_completed",
        _ => return,
    };
    tracing::info!(event, outcome = "completed");
}

fn log_exchange_failed(method: &str, code: AuthErrorCode) {
    let event = match method {
        AUTH_DEVICE_ACTIVATE => "auth_device_activation_failed",
        AUTH_REFRESH => "auth_refresh_failed",
        INVITE_PREVIEW => "invitation_preview_failed",
        INVITE_ACCEPT => "invitation_accept_failed",
        _ => return,
    };
    tracing::info!(event, outcome = "failed", reason = code.as_str());
}

#[derive(Debug)]
struct RestrictedReadFailure {
    response: Option<JsonValue>,
    close_code: u16,
    close_reason: &'static str,
}

async fn receive_one_request(
    ws: &mut WebSocket,
) -> std::result::Result<JsonRpcRequest, RestrictedReadFailure> {
    loop {
        let Some(frame) = ws.recv().await else {
            return Err(closed_failure());
        };
        let frame = frame.map_err(|_| RestrictedReadFailure {
            response: None,
            close_code: CLOSE_AUTH_INVALID_REQUEST,
            close_reason: "auth_exchange_invalid_frame",
        })?;
        if let Some(request) = decode_request_frame(frame)? {
            return Ok(request);
        }
    }
}

fn decode_request_frame(
    frame: Message,
) -> std::result::Result<Option<JsonRpcRequest>, RestrictedReadFailure> {
    match frame {
        Message::Text(payload) => {
            if payload.len() > MAX_RESTRICTED_REQUEST_BYTES {
                return Err(read_error(
                    None,
                    INVALID_REQUEST_CODE,
                    "restricted auth request is too large",
                    "auth_request_too_large",
                ));
            }
            let request = serde_json::from_str::<JsonRpcRequest>(payload.as_ref()).map_err(|_| {
                read_error(
                    None,
                    PARSE_ERROR_CODE,
                    "invalid JSON-RPC request",
                    "auth_invalid_request",
                )
            })?;
            if request.jsonrpc != JSONRPC_VERSION {
                return Err(read_error(
                    Some(request.id),
                    INVALID_REQUEST_CODE,
                    "unsupported JSON-RPC version",
                    "auth_invalid_request",
                ));
            }
            Ok(Some(request))
        }
        Message::Binary(_) => Err(RestrictedReadFailure {
            response: None,
            close_code: CLOSE_AUTH_INVALID_REQUEST,
            close_reason: "auth_binary_forbidden",
        }),
        // Axum owns Ping/Pong protocol replies; neither counts as the one request.
        Message::Ping(_) | Message::Pong(_) => Ok(None),
        Message::Close(_) => Err(closed_failure()),
    }
}

fn closed_failure() -> RestrictedReadFailure {
    RestrictedReadFailure {
        response: None,
        close_code: CLOSE_AUTH_INVALID_REQUEST,
        close_reason: "auth_exchange_closed",
    }
}

fn read_error(
    id: Option<RequestId>,
    numeric_code: i64,
    message: &'static str,
    machine_code: &'static str,
) -> RestrictedReadFailure {
    let mut response = JsonRpcErrorResponse::new(id, numeric_code, message);
    response.error.data = Some(json!({ "code": machine_code }));
    RestrictedReadFailure {
        response: serde_json::to_value(response).ok(),
        close_code: CLOSE_AUTH_INVALID_REQUEST,
        close_reason: machine_code,
    }
}

fn authorize_method(
    context: &RestrictedAuthContext,
    method: &str,
) -> std::result::Result<(), AuthErrorCode> {
    let allowed = match context {
        RestrictedAuthContext::DeviceActivation(_) => method == AUTH_DEVICE_ACTIVATE,
        RestrictedAuthContext::Refresh(_) => method == AUTH_REFRESH,
        RestrictedAuthContext::Invitation(_) => {
            matches!(method, INVITE_PREVIEW | INVITE_ACCEPT)
        }
    };
    if allowed {
        Ok(())
    } else {
        Err(AuthErrorCode::MethodNotAllowed)
    }
}

fn auth_error_response(id: Option<RequestId>, code: AuthErrorCode) -> Result<JsonValue> {
    let data = json!({ "code": code.as_str() });
    serde_json::to_value(JsonRpcErrorResponse {
        jsonrpc: JSONRPC_VERSION.to_owned(),
        id,
        error: JsonRpcError {
            code: AUTH_ERROR_JSONRPC_CODE,
            message: code.as_str().to_owned(),
            data: Some(data),
        },
    })
    .context("failed to serialize restricted auth error response")
}

async fn send_bounded_json(ws: &mut WebSocket, response: &JsonValue) -> Result<()> {
    let payload = serde_json::to_string(response).context("failed to serialize auth response")?;
    if payload.len() > MAX_RESTRICTED_RESPONSE_BYTES {
        anyhow::bail!("restricted auth response exceeded transport limit");
    }
    ws.send(Message::Text(payload.into()))
        .await
        .context("failed to send restricted auth response")
}

async fn close(ws: &mut WebSocket, code: u16, reason: &'static str) -> Result<()> {
    ws.send(Message::Close(Some(CloseFrame {
        code,
        reason: reason.into(),
    })))
    .await
    .context("failed to close restricted auth connection")
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::{
        DeviceActivationContext, InvitationExchangeContext, RefreshExchangeContext,
    };
    use pioneer_protocol::{GatewayId, InvitationTransportSecurity};

    #[test]
    fn method_matrix_is_exact_and_rejects_wrong_methods() {
        let device = RestrictedAuthContext::DeviceActivation(DeviceActivationContext {
            gateway_id: GatewayId::new("G00000000000000000001").unwrap(),
        });
        let refresh = RestrictedAuthContext::Refresh(RefreshExchangeContext);
        let invitation = invitation_context();

        assert!(authorize_method(&device, AUTH_DEVICE_ACTIVATE).is_ok());
        assert!(authorize_method(&refresh, AUTH_REFRESH).is_ok());
        assert!(authorize_method(&invitation, INVITE_PREVIEW).is_ok());
        assert!(authorize_method(&invitation, INVITE_ACCEPT).is_ok());
        assert_eq!(
            authorize_method(&device, "workspace/list"),
            Err(AuthErrorCode::MethodNotAllowed)
        );
        assert_eq!(
            authorize_method(&refresh, AUTH_DEVICE_ACTIVATE),
            Err(AuthErrorCode::MethodNotAllowed)
        );
        for denied in [
            AUTH_REFRESH,
            AUTH_DEVICE_ACTIVATE,
            "workspace/list",
            "subscription/start",
        ] {
            assert_eq!(
                authorize_method(&invitation, denied),
                Err(AuthErrorCode::MethodNotAllowed)
            );
        }
        assert_eq!(
            authorize_method(&refresh, INVITE_PREVIEW),
            Err(AuthErrorCode::MethodNotAllowed)
        );
    }

    #[test]
    fn restricted_frame_decoder_allows_one_text_request_only() {
        let request = format!(
            r#"{{"jsonrpc":"2.0","id":"R00000000000000000001","method":"{AUTH_REFRESH}","params":{{}}}}"#
        );
        let decoded = decode_request_frame(Message::Text(request.into()))
            .unwrap()
            .expect("text request");
        assert_eq!(decoded.method, AUTH_REFRESH);
        assert!(decode_request_frame(Message::Binary(vec![1].into())).is_err());
        assert!(decode_request_frame(Message::Ping(vec![1].into()))
            .unwrap()
            .is_none());
        assert!(decode_request_frame(Message::Close(None)).is_err());
    }

    #[test]
    fn restricted_errors_never_echo_secret_bearing_payloads() {
        let secret = "prf2_secret-that-must-not-leak";
        let failure = decode_request_frame(Message::Text(secret.into())).unwrap_err();
        let rendered = format!("{failure:?}");
        assert!(!rendered.contains(secret));
    }

    fn invitation_context() -> RestrictedAuthContext {
        RestrictedAuthContext::Invitation(InvitationExchangeContext {
            gateway_id: GatewayId::new("G00000000000000000001").unwrap(),
            transport: InvitationTransportSecurity::InsecureWs,
        })
    }
}
