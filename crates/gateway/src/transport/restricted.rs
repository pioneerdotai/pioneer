use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use pioneer_protocol::{
    INVALID_REQUEST_CODE, JSONRPC_VERSION, JsonRpcError, JsonRpcErrorResponse, JsonRpcRequest,
    JsonRpcResponse, PARSE_ERROR_CODE, RequestId,
};
use serde_json::{Value as JsonValue, json};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::time::timeout;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, frame::coding::CloseCode};

use crate::auth::{AuthError, AuthErrorCode, RestrictedAdmission, RestrictedAuthContext};

pub(crate) const AUTH_DEVICE_ACTIVATE: &str =
    pioneer_protocol::constants::methods::AUTH_DEVICE_ACTIVATE;
pub(crate) const AUTH_REFRESH: &str = pioneer_protocol::constants::methods::AUTH_REFRESH;

const MAX_RESTRICTED_REQUEST_BYTES: usize = 64 * 1024;
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

pub(crate) async fn run<S>(
    mut ws: WebSocketStream<S>,
    admission: RestrictedAdmission,
    deadline: Duration,
    executor: Arc<dyn RestrictedExchangeExecutor>,
) -> Result<RestrictedExchangeOutcome>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    match timeout(deadline, run_one_exchange(&mut ws, admission, executor)).await {
        Ok(outcome) => outcome,
        Err(_) => {
            // The deadline covers admission payload receipt, execution,
            // response delivery, and the restricted close handshake. Closing
            // after cancellation is itself bounded so a peer that stopped
            // reading cannot retain a Gateway task indefinitely.
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

async fn run_one_exchange<S>(
    ws: &mut WebSocketStream<S>,
    admission: RestrictedAdmission,
    executor: Arc<dyn RestrictedExchangeExecutor>,
) -> Result<RestrictedExchangeOutcome>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
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
                                .and_then(|value| {
                                    serde_json::to_value(value).map_err(Into::into)
                                })?;
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
        _ => return,
    };
    tracing::info!(event, outcome = "started");
}

fn log_exchange_completed(method: &str) {
    let event = match method {
        AUTH_DEVICE_ACTIVATE => "auth_device_activation_completed",
        AUTH_REFRESH => "auth_refresh_completed",
        _ => return,
    };
    tracing::info!(event, outcome = "completed");
}

fn log_exchange_failed(method: &str, code: AuthErrorCode) {
    let event = match method {
        AUTH_DEVICE_ACTIVATE => "auth_device_activation_failed",
        AUTH_REFRESH => "auth_refresh_failed",
        _ => return,
    };
    tracing::info!(event, outcome = "failed", reason = code.as_str(),);
}

struct RestrictedReadFailure {
    response: Option<JsonValue>,
    close_code: u16,
    close_reason: &'static str,
}

async fn receive_one_request<S>(
    ws: &mut WebSocketStream<S>,
) -> std::result::Result<JsonRpcRequest, RestrictedReadFailure>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        let Some(frame) = ws.next().await else {
            return Err(RestrictedReadFailure {
                response: None,
                close_code: CLOSE_AUTH_INVALID_REQUEST,
                close_reason: "auth_exchange_closed",
            });
        };
        let frame = frame.map_err(|_| RestrictedReadFailure {
            response: None,
            close_code: CLOSE_AUTH_INVALID_REQUEST,
            close_reason: "auth_exchange_invalid_frame",
        })?;
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
                let request =
                    serde_json::from_str::<JsonRpcRequest>(payload.as_ref()).map_err(|_| {
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
                return Ok(request);
            }
            Message::Binary(_) => {
                return Err(RestrictedReadFailure {
                    response: None,
                    close_code: CLOSE_AUTH_INVALID_REQUEST,
                    close_reason: "auth_binary_forbidden",
                });
            }
            Message::Ping(payload) => {
                ws.send(Message::Pong(payload))
                    .await
                    .map_err(|_| RestrictedReadFailure {
                        response: None,
                        close_code: CLOSE_AUTH_INVALID_REQUEST,
                        close_reason: "auth_exchange_write_failed",
                    })?;
            }
            Message::Pong(_) | Message::Frame(_) => {}
            Message::Close(_) => {
                return Err(RestrictedReadFailure {
                    response: None,
                    close_code: CLOSE_AUTH_INVALID_REQUEST,
                    close_reason: "auth_exchange_closed",
                });
            }
        }
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
    let expected = match context {
        RestrictedAuthContext::DeviceActivation(_) => AUTH_DEVICE_ACTIVATE,
        RestrictedAuthContext::Refresh(_) => AUTH_REFRESH,
    };
    if method == expected {
        return Ok(());
    }
    Err(AuthErrorCode::MethodNotAllowed)
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

async fn send_bounded_json<S>(ws: &mut WebSocketStream<S>, response: &JsonValue) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let payload = serde_json::to_string(response).context("failed to serialize auth response")?;
    if payload.len() > MAX_RESTRICTED_RESPONSE_BYTES {
        anyhow::bail!("restricted auth response exceeded transport limit");
    }
    ws.send(Message::Text(payload.into()))
        .await
        .context("failed to send restricted auth response")
}

async fn close<S>(ws: &mut WebSocketStream<S>, code: u16, reason: &'static str) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    ws.close(Some(CloseFrame {
        code: CloseCode::from(code),
        reason: reason.into(),
    }))
    .await
    .context("failed to close restricted auth connection")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::{DeviceActivationContext, PresentedCredential, RefreshExchangeContext};
    use pioneer_protocol::GatewayId;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::duplex;
    use tokio_tungstenite::tungstenite::protocol::Role;

    struct RecordingExecutor {
        calls: AtomicUsize,
    }

    struct BlockingExecutor {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl RestrictedExchangeExecutor for RecordingExecutor {
        async fn execute(
            &self,
            _admission: RestrictedAdmission,
            request: JsonRpcRequest,
        ) -> std::result::Result<JsonValue, AuthError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(json!({ "accepted_method": request.method }))
        }
    }

    #[async_trait]
    impl RestrictedExchangeExecutor for BlockingExecutor {
        async fn execute(
            &self,
            _admission: RestrictedAdmission,
            _request: JsonRpcRequest,
        ) -> std::result::Result<JsonValue, AuthError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_secs(60)).await;
            Ok(JsonValue::Null)
        }
    }

    #[test]
    fn method_matrix_is_exact_and_rejects_wrong_methods() {
        let device_activation = device_activation_context();
        let refresh = RestrictedAuthContext::Refresh(RefreshExchangeContext);

        assert!(authorize_method(&device_activation, AUTH_DEVICE_ACTIVATE).is_ok());
        assert!(authorize_method(&refresh, AUTH_REFRESH).is_ok());
        assert_eq!(
            authorize_method(&device_activation, "workspace/list"),
            Err(AuthErrorCode::MethodNotAllowed)
        );
        assert_eq!(
            authorize_method(&refresh, AUTH_DEVICE_ACTIVATE),
            Err(AuthErrorCode::MethodNotAllowed)
        );
    }

    #[tokio::test]
    async fn one_text_request_gets_one_response_then_connection_closes() {
        let executor = Arc::new(RecordingExecutor {
            calls: AtomicUsize::new(0),
        });
        let (server_io, client_io) = duplex(128 * 1024);
        let server_ws = WebSocketStream::from_raw_socket(server_io, Role::Server, None).await;
        let mut client_ws = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
        let task_executor = executor.clone();
        let task = tokio::spawn(async move {
            run(
                server_ws,
                refresh_admission(),
                Duration::from_secs(1),
                task_executor,
            )
            .await
        });
        client_ws
            .send(Message::Text(request(AUTH_REFRESH).into()))
            .await
            .unwrap();
        client_ws
            .send(Message::Text(request(AUTH_REFRESH).into()))
            .await
            .unwrap();

        let response = client_ws.next().await.unwrap().unwrap();
        assert!(matches!(response, Message::Text(_)));
        let close = client_ws.next().await.unwrap().unwrap();
        assert!(matches!(close, Message::Close(_)));
        task.await.unwrap().unwrap();
        assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn binary_frame_never_reaches_executor() {
        let executor = Arc::new(RecordingExecutor {
            calls: AtomicUsize::new(0),
        });
        let (server_io, client_io) = duplex(1024);
        let server_ws = WebSocketStream::from_raw_socket(server_io, Role::Server, None).await;
        let mut client_ws = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
        let task_executor = executor.clone();
        let task = tokio::spawn(async move {
            run(
                server_ws,
                refresh_admission(),
                Duration::from_secs(1),
                task_executor,
            )
            .await
        });
        client_ws
            .send(Message::Binary(vec![1, 2, 3].into()))
            .await
            .unwrap();
        assert!(matches!(
            client_ws.next().await.unwrap().unwrap(),
            Message::Close(_)
        ));
        task.await.unwrap().unwrap();
        assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn timeout_closes_without_dispatch() {
        let executor = Arc::new(RecordingExecutor {
            calls: AtomicUsize::new(0),
        });
        let (server_io, client_io) = duplex(1024);
        let server_ws = WebSocketStream::from_raw_socket(server_io, Role::Server, None).await;
        let mut client_ws = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
        let task_executor = executor.clone();
        let task = tokio::spawn(async move {
            run(
                server_ws,
                refresh_admission(),
                Duration::from_millis(10),
                task_executor,
            )
            .await
        });
        assert!(matches!(
            client_ws.next().await.unwrap().unwrap(),
            Message::Close(_)
        ));
        task.await.unwrap().unwrap();
        assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn timeout_cancels_a_stalled_exchange_executor() {
        let executor = Arc::new(BlockingExecutor {
            calls: AtomicUsize::new(0),
        });
        let (server_io, client_io) = duplex(1024);
        let server_ws = WebSocketStream::from_raw_socket(server_io, Role::Server, None).await;
        let mut client_ws = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
        let task_executor = executor.clone();
        let task = tokio::spawn(async move {
            run(
                server_ws,
                refresh_admission(),
                Duration::from_millis(20),
                task_executor,
            )
            .await
        });
        client_ws
            .send(Message::Text(request(AUTH_REFRESH).into()))
            .await
            .unwrap();

        assert!(matches!(
            client_ws.next().await.unwrap().unwrap(),
            Message::Close(_)
        ));
        assert_eq!(
            task.await.unwrap().unwrap(),
            RestrictedExchangeOutcome::Failed
        );
        assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
    }

    fn request(method: &str) -> String {
        json!({
            "jsonrpc": "2.0",
            "id": "R00000000000000000001",
            "method": method,
            "params": {},
        })
        .to_string()
    }

    fn refresh_admission() -> RestrictedAdmission {
        let credential = format!(
            "prf_{}",
            "r".repeat(pioneer_protocol::MIN_OPAQUE_CREDENTIAL_BODY_LEN)
        );
        RestrictedAdmission::new(
            PresentedCredential::classify(&credential).unwrap(),
            RestrictedAuthContext::Refresh(RefreshExchangeContext),
        )
    }

    fn device_activation_context() -> RestrictedAuthContext {
        RestrictedAuthContext::DeviceActivation(DeviceActivationContext {
            gateway_id: GatewayId::new("G00000000000000000001").unwrap(),
        })
    }
}
