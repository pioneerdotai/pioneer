//! JSON-RPC request lifecycle primitives.

use anyhow::{Context, Result, anyhow};
use pioneer_protocol::{
    AUTHENTICATION_TERMINAL_CODE, FORBIDDEN_CODE, JSONRPC_VERSION, JsonRpcRequest, NOT_FOUND_CODE,
    REQUEST_ID_LEN, RequestId, generate_id,
};
use serde::de::DeserializeOwned;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::fmt;
use std::sync::mpsc::{SendError, Sender};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

pub const JSON_RPC_REQUEST_FAILED_MESSAGE: &str = "JSON-RPC request failed";
pub const INVALID_JSON_RPC_RESPONSE_PAYLOAD_MESSAGE: &str = "invalid JSON-RPC response payload";
pub const WEBSOCKET_WORKER_UNAVAILABLE_MESSAGE: &str = "websocket worker is not available";
pub const RPC_REQUEST_TIMEOUT: Duration = Duration::from_secs(8);
pub const RPC_UNSUBSCRIBE_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JsonRpcRequestPayload {
    pub request_id: String,
    pub payload: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JsonRpcAuthorizationFailure {
    AuthenticationTerminal,
    Forbidden,
    InaccessibleResource,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JsonRpcResponseError {
    Transport {
        message: String,
    },
    InvalidResponse,
    DeadlineExceeded,
    Server {
        code: Option<i64>,
        message: String,
        machine_code: Option<String>,
    },
}

impl JsonRpcResponseError {
    pub fn transport(message: impl Into<String>) -> Self {
        Self::Transport {
            message: message.into(),
        }
    }

    pub fn server(
        code: Option<i64>,
        message: impl Into<String>,
        machine_code: Option<String>,
    ) -> Self {
        Self::Server {
            code,
            message: message.into(),
            machine_code,
        }
    }

    pub fn authorization_failure(&self) -> Option<JsonRpcAuthorizationFailure> {
        match self {
            Self::Server { code, .. } if *code == Some(AUTHENTICATION_TERMINAL_CODE) => {
                Some(JsonRpcAuthorizationFailure::AuthenticationTerminal)
            }
            Self::Server { code, .. } if *code == Some(FORBIDDEN_CODE) => {
                Some(JsonRpcAuthorizationFailure::Forbidden)
            }
            Self::Server { code, .. } if *code == Some(NOT_FOUND_CODE) => {
                Some(JsonRpcAuthorizationFailure::InaccessibleResource)
            }
            Self::Transport { .. }
            | Self::InvalidResponse
            | Self::DeadlineExceeded
            | Self::Server { .. } => None,
        }
    }

    pub fn machine_code(&self) -> Option<&str> {
        match self {
            Self::Server { machine_code, .. } => machine_code.as_deref(),
            Self::Transport { .. } | Self::InvalidResponse | Self::DeadlineExceeded => None,
        }
    }
}

impl fmt::Display for JsonRpcResponseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport { message } => formatter.write_str(message),
            Self::InvalidResponse => formatter.write_str(INVALID_JSON_RPC_RESPONSE_PAYLOAD_MESSAGE),
            Self::DeadlineExceeded => formatter.write_str("local RPC request deadline exceeded"),
            Self::Server {
                code,
                message,
                machine_code,
            } => match self.authorization_failure() {
                Some(JsonRpcAuthorizationFailure::AuthenticationTerminal) => {
                    formatter.write_str("authentication_terminal")
                }
                Some(JsonRpcAuthorizationFailure::Forbidden) => formatter.write_str("forbidden"),
                Some(JsonRpcAuthorizationFailure::InaccessibleResource) => {
                    formatter.write_str("not_found")
                }
                None => match machine_code {
                    Some(machine_code) => write!(formatter, "{message} [{machine_code}]"),
                    None => match code {
                        Some(code) => write!(formatter, "{message} [{code}]"),
                        None => formatter.write_str(message),
                    },
                },
            },
        }
    }
}

impl std::error::Error for JsonRpcResponseError {}

pub type JsonRpcResponseResult = std::result::Result<JsonValue, JsonRpcResponseError>;
/// A single local request lifetime shared by the waiter, transport and queued frame.
/// Retiring it never cancels work that may already have reached the gateway.
#[derive(Clone)]
pub struct JsonRpcResponseSender {
    state: Arc<Mutex<RequestState>>,
}

type PendingMap = Mutex<HashMap<String, JsonRpcResponseSender>>;

struct PendingRegistration {
    requests: Weak<PendingMap>,
    request_id: String,
}

struct RequestState {
    response: Option<Sender<JsonRpcResponseResult>>,
    deadline: Instant,
    write_started: bool,
    registration: Option<PendingRegistration>,
}

impl Drop for RequestState {
    fn drop(&mut self) {
        if let Some(response) = self.response.take() {
            let error = if Instant::now() >= self.deadline {
                JsonRpcResponseError::DeadlineExceeded
            } else {
                JsonRpcResponseError::transport("websocket request owner dropped")
            };
            let _ = response.send(Err(error));
        }
    }
}

impl JsonRpcResponseSender {
    /// Use the request's original process-local deadline, including admission time.
    pub fn new(response: Sender<JsonRpcResponseResult>, deadline: Instant) -> Self {
        Self {
            state: Arc::new(Mutex::new(RequestState {
                response: Some(response),
                deadline,
                write_started: false,
                registration: None,
            })),
        }
    }

    pub(crate) fn retire_on_drop(&self) -> impl Drop + use<> {
        RequestWaitGuard(Arc::downgrade(&self.state))
    }

    pub fn send(
        &self,
        response: JsonRpcResponseResult,
    ) -> Result<(), SendError<JsonRpcResponseResult>> {
        self.finish(Some(response))
    }

    /// Retire local observation, including pending registration, without data-queue admission.
    pub fn cancel(&self) {
        let _ = self.finish(None);
    }

    fn finish(
        &self,
        response: Option<JsonRpcResponseResult>,
    ) -> Result<(), SendError<JsonRpcResponseResult>> {
        let (sender, registration, expired) = {
            let mut state = self.state.lock().unwrap();
            (
                state.response.take(),
                state.registration.take(),
                Instant::now() >= state.deadline,
            )
        };
        // insert holds state before registry; release state before acquiring
        // registry here so cancellation and registration cannot invert locks.
        if let Some(registration) = registration {
            self.remove_registration(registration);
        }
        match (sender, response) {
            (Some(sender), Some(response)) if !expired => sender.send(response),
            (Some(sender), Some(response)) => {
                let _ = sender.send(Err(JsonRpcResponseError::DeadlineExceeded));
                Err(SendError(response))
            }
            (Some(sender), None) => sender.send(Err(if expired {
                JsonRpcResponseError::DeadlineExceeded
            } else {
                JsonRpcResponseError::transport("local RPC request retired")
            })),
            (None, Some(response)) => Err(SendError(response)),
            (None, None) => Ok(()),
        }
    }

    fn remove_registration(&self, registration: PendingRegistration) {
        if let Some(requests) = registration.requests.upgrade() {
            let mut requests = requests.lock().unwrap();
            if requests
                .get(&registration.request_id)
                .is_some_and(|current| Arc::ptr_eq(&current.state, &self.state))
            {
                requests.remove(&registration.request_id);
            }
        }
    }

    pub(crate) fn is_active(&self) -> bool {
        let state = self.state.lock().unwrap();
        state.response.is_some() && Instant::now() < state.deadline
    }

    /// Linearization point: after this claim, a write may reach the peer.
    /// Cancellation after the claim only retires the local waiter; it cannot roll back RPCs.
    pub(crate) fn try_start_write(&self) -> bool {
        let mut state = self.state.lock().unwrap();
        if state.response.is_none() || state.write_started {
            return false;
        }
        if Instant::now() >= state.deadline {
            drop(state);
            self.cancel();
            return false;
        }
        state.write_started = true;
        true
    }
}

struct RequestWaitGuard(Weak<Mutex<RequestState>>);
impl Drop for RequestWaitGuard {
    fn drop(&mut self) {
        if let Some(state) = self.0.upgrade() {
            JsonRpcResponseSender { state }.cancel();
        }
    }
}

pub trait JsonRpcRequestTransport {
    fn send_json_rpc_request(
        &self,
        request_id: String,
        payload: String,
        response_tx: JsonRpcResponseSender,
    ) -> std::result::Result<(), String>;
}

#[derive(Default)]
pub struct PendingJsonRpcRequests {
    pending: Arc<PendingMap>,
}

impl PendingJsonRpcRequests {
    pub fn insert(
        &mut self,
        request_id: impl Into<String>,
        response_tx: JsonRpcResponseSender,
    ) -> Option<JsonRpcResponseSender> {
        let request_id = request_id.into();
        let mut state = response_tx.state.lock().unwrap();
        if state.response.is_none() || Instant::now() >= state.deadline {
            drop(state);
            response_tx.cancel();
            return None;
        }
        state.registration = Some(PendingRegistration {
            requests: Arc::downgrade(&self.pending),
            request_id: request_id.clone(),
        });
        let previous = self
            .pending
            .lock()
            .unwrap()
            .insert(request_id, response_tx.clone());
        drop(state);
        previous
    }

    pub fn remove(&mut self, request_id: &str) -> Option<JsonRpcResponseSender> {
        self.pending.lock().unwrap().remove(request_id)
    }

    pub fn fail_all(&mut self, error: &str) -> usize {
        let pending = self
            .pending
            .lock()
            .unwrap()
            .drain()
            .map(|(_, sender)| sender)
            .collect::<Vec<_>>();
        let count = pending.len();
        for response_tx in pending {
            let _ = response_tx.send(Err(JsonRpcResponseError::transport(error)));
        }
        count
    }

    pub fn len(&self) -> usize {
        self.pending.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Drop for PendingJsonRpcRequests {
    fn drop(&mut self) {
        self.fail_all("websocket connection owner dropped");
    }
}

pub fn new_request_id() -> String {
    generate_id(REQUEST_ID_LEN)
}

pub fn build_json_rpc_request_payload(
    method: &str,
    params: JsonValue,
) -> Result<JsonRpcRequestPayload> {
    let mut params = params;
    pioneer_observability::turn_startup::inject_params(method, &mut params);
    let request_id = new_request_id();
    let request = JsonRpcRequest {
        jsonrpc: JSONRPC_VERSION.to_owned(),
        id: RequestId::new(request_id.clone())
            .map_err(|error| anyhow!("failed to build request id: {error}"))?,
        method: method.to_owned(),
        params: Some(params),
    };
    let payload =
        serde_json::to_string(&request).context("failed to serialize JSON-RPC request")?;

    Ok(JsonRpcRequestPayload {
        request_id,
        payload,
    })
}

pub fn decode_json_rpc_response_value(
    value: &JsonValue,
) -> Option<(String, JsonRpcResponseResult)> {
    let response_id = value
        .get("id")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)?;

    if let Some(result) = value.get("result") {
        return Some((response_id, Ok(result.clone())));
    }

    if let Some(error) = value.get("error") {
        let message = error
            .get("message")
            .and_then(serde_json::Value::as_str)
            .unwrap_or(JSON_RPC_REQUEST_FAILED_MESSAGE);
        let code = error.get("code").and_then(serde_json::Value::as_i64);
        let machine_code = error.get("data").and_then(|data| {
            data.as_str()
                .or_else(|| data.get("code").and_then(serde_json::Value::as_str))
                .map(str::to_owned)
        });
        return Some((
            response_id,
            Err(JsonRpcResponseError::server(code, message, machine_code)),
        ));
    }

    Some((response_id, Err(JsonRpcResponseError::InvalidResponse)))
}

pub fn json_rpc_authorization_failure(
    error: &anyhow::Error,
) -> Option<JsonRpcAuthorizationFailure> {
    error.chain().find_map(|cause| {
        cause
            .downcast_ref::<JsonRpcResponseError>()
            .and_then(JsonRpcResponseError::authorization_failure)
    })
}

pub fn json_rpc_response_error(error: &anyhow::Error) -> Option<&JsonRpcResponseError> {
    error
        .chain()
        .find_map(|cause| cause.downcast_ref::<JsonRpcResponseError>())
}

pub fn deserialize_json_rpc_result<T>(method: &str, result: JsonValue) -> Result<T>
where
    T: DeserializeOwned,
{
    serde_json::from_value(result)
        .with_context(|| format!("failed to decode `{method}` response payload"))
}

pub fn request_timeout_message(method: &str) -> String {
    format!("timed out waiting for `{method}` response")
}

pub fn fail_pending_json_rpc_requests(
    pending_requests: &mut PendingJsonRpcRequests,
    error: &str,
) -> usize {
    pending_requests.fail_all(error)
}

pub fn send_json_rpc_request_value<TTransport>(
    transport: &TTransport,
    method: &str,
    params: JsonValue,
    timeout: Duration,
) -> Result<JsonValue>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    use pioneer_observability::work::{Boundary, ExecutorRole, Trigger, WorkCarrier, WorkKind};
    let executor =
        pioneer_observability::work::current().map_or(ExecutorRole::Foreign, |c| c.executor);
    let work = WorkCarrier::child(
        Boundary::ClientRpc,
        WorkKind::ClientRpc,
        Trigger::Rpc,
        executor,
    );
    work.run_result(|| {
        let startup_key = if matches!(method, "turn/start" | "voice/session/finalize") {
            pioneer_observability::turn_startup::request_key(&params)
        } else {
            None
        };
        let deadline = Instant::now() + timeout;
        let request = build_json_rpc_request_payload(method, params)?;
        let (response_tx, response_rx) = std::sync::mpsc::channel();
        let response_tx = JsonRpcResponseSender::new(response_tx, deadline);
        let _request_guard = response_tx.retire_on_drop();

        transport
            .send_json_rpc_request(request.request_id, request.payload, response_tx)
            .map_err(|error| {
                if let Some(key) = startup_key.as_deref() {
                    pioneer_observability::turn_startup::finish(
                        key,
                        pioneer_observability::turn_startup::Outcome::ObservationLost,
                    );
                }
                anyhow::Error::msg(error)
            })?;

        let _wait = pioneer_observability::live::WaitGuard::with_timeout(
            pioneer_observability::live::WaitKind::Reply,
            deadline.saturating_duration_since(Instant::now()),
        );
        let response = response_rx
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .map_err(|error| {
                if let Some(key) = startup_key.as_deref() {
                    let outcome = if matches!(error, std::sync::mpsc::RecvTimeoutError::Timeout) {
                        pioneer_observability::turn_startup::Outcome::DeadlineExceeded
                    } else {
                        pioneer_observability::turn_startup::Outcome::ObservationLost
                    };
                    pioneer_observability::turn_startup::finish(key, outcome);
                }
                anyhow!("{}", request_timeout_message(method))
            })?;

        response.map_err(|error| {
            if matches!(error, JsonRpcResponseError::DeadlineExceeded) {
                if let Some(key) = startup_key.as_deref() {
                    pioneer_observability::turn_startup::finish(
                        key,
                        pioneer_observability::turn_startup::Outcome::DeadlineExceeded,
                    );
                }
                anyhow!("{}", request_timeout_message(method))
            } else {
                if matches!(error, JsonRpcResponseError::Transport { .. }) {
                    if let Some(key) = startup_key.as_deref() {
                        pioneer_observability::turn_startup::finish(
                            key,
                            pioneer_observability::turn_startup::Outcome::ObservationLost,
                        );
                    }
                }
                anyhow::Error::new(error)
            }
        })
    })
}

pub fn send_json_rpc_request_typed<TResponse, TParams, TTransport>(
    transport: &TTransport,
    method: &str,
    params: &TParams,
    timeout: Duration,
) -> Result<TResponse>
where
    TResponse: DeserializeOwned,
    TParams: serde::Serialize,
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    let params_value = serde_json::to_value(params).context("failed to encode JSON-RPC params")?;
    let result = send_json_rpc_request_value(transport, method, params_value, timeout)?;

    deserialize_json_rpc_result(method, result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::JsonRpcRequest;
    use serde_json::json;
    use std::sync::mpsc;

    #[test]
    fn rpc_request_payload_contains_generated_id_method_and_params() {
        let payload =
            build_json_rpc_request_payload("workspace/list", json!({"workspace_id": "ws_1"}))
                .expect("payload");

        assert_eq!(payload.request_id.chars().count(), REQUEST_ID_LEN);

        let request: JsonRpcRequest = serde_json::from_str(payload.payload.as_str())
            .expect("serialized request should decode");
        assert_eq!(request.jsonrpc, JSONRPC_VERSION);
        assert_eq!(request.id.as_str(), payload.request_id);
        assert_eq!(request.method, "workspace/list");
        assert_eq!(request.params, Some(json!({"workspace_id": "ws_1"})));
    }

    #[test]
    fn rpc_pending_requests_remove_matching_sender() {
        let (response_tx, response_rx) = mpsc::channel();
        let mut pending = PendingJsonRpcRequests::default();

        assert!(
            pending
                .insert(
                    "request-1",
                    JsonRpcResponseSender::new(response_tx, Instant::now() + RPC_REQUEST_TIMEOUT)
                )
                .is_none()
        );
        assert_eq!(pending.len(), 1);

        let sender = pending.remove("request-1").expect("sender");
        sender.send(Ok(json!({"ok": true}))).expect("send");

        assert_eq!(
            response_rx.recv().expect("response"),
            Ok(json!({"ok": true}))
        );
        assert!(pending.is_empty());
    }

    #[test]
    fn rpc_pending_requests_fail_all_drains_map() {
        let (first_tx, first_rx) = mpsc::channel();
        let (second_tx, second_rx) = mpsc::channel();
        let mut pending = PendingJsonRpcRequests::default();

        pending.insert(
            "request-1",
            JsonRpcResponseSender::new(first_tx, Instant::now() + RPC_REQUEST_TIMEOUT),
        );
        pending.insert(
            "request-2",
            JsonRpcResponseSender::new(second_tx, Instant::now() + RPC_REQUEST_TIMEOUT),
        );

        assert_eq!(pending.fail_all("websocket closed"), 2);
        assert!(pending.is_empty());
        assert_eq!(
            first_rx.recv().expect("first response"),
            Err(JsonRpcResponseError::transport("websocket closed"))
        );
        assert_eq!(
            second_rx.recv().expect("second response"),
            Err(JsonRpcResponseError::transport("websocket closed"))
        );
    }

    #[test]
    fn rpc_decode_response_value_maps_result_error_and_invalid_payloads() {
        assert_eq!(
            decode_json_rpc_response_value(&json!({
                "id": "request-1",
                "result": {"ok": true}
            })),
            Some(("request-1".to_owned(), Ok(json!({"ok": true}))))
        );
        assert_eq!(
            decode_json_rpc_response_value(&json!({
                "id": "request-1",
                "error": {"message": "boom"}
            })),
            Some((
                "request-1".to_owned(),
                Err(JsonRpcResponseError::server(None, "boom", None))
            ))
        );
        assert_eq!(
            decode_json_rpc_response_value(&json!({
                "id": "request-1",
                "error": {
                    "code": -32600,
                    "message": "message revision conflict",
                    "data": "revision_conflict"
                }
            })),
            Some((
                "request-1".to_owned(),
                Err(JsonRpcResponseError::server(
                    Some(-32600),
                    "message revision conflict",
                    Some("revision_conflict".to_owned())
                ))
            ))
        );
        assert_eq!(
            decode_json_rpc_response_value(&json!({
                "id": "request-1",
                "error": {
                    "code": -32001,
                    "message": "session is no longer active",
                    "data": {"code": "session_revoked"}
                }
            })),
            Some((
                "request-1".to_owned(),
                Err(JsonRpcResponseError::server(
                    Some(-32001),
                    "session is no longer active",
                    Some("session_revoked".to_owned())
                ))
            ))
        );
        assert_eq!(
            decode_json_rpc_response_value(&json!({
                "id": "request-1",
                "error": {}
            })),
            Some((
                "request-1".to_owned(),
                Err(JsonRpcResponseError::server(
                    None,
                    JSON_RPC_REQUEST_FAILED_MESSAGE,
                    None
                ))
            ))
        );
        assert_eq!(
            decode_json_rpc_response_value(&json!({
                "id": "request-1"
            })),
            Some((
                "request-1".to_owned(),
                Err(JsonRpcResponseError::InvalidResponse)
            ))
        );
        assert_eq!(
            decode_json_rpc_response_value(&json!({
                "method": "workspace/changed"
            })),
            None
        );
    }

    #[test]
    fn rpc_authorization_failures_use_numeric_codes_not_peer_messages() {
        for (code, expected, rendered) in [
            (
                AUTHENTICATION_TERMINAL_CODE,
                JsonRpcAuthorizationFailure::AuthenticationTerminal,
                "authentication_terminal",
            ),
            (
                FORBIDDEN_CODE,
                JsonRpcAuthorizationFailure::Forbidden,
                "forbidden",
            ),
            (
                NOT_FOUND_CODE,
                JsonRpcAuthorizationFailure::InaccessibleResource,
                "not_found",
            ),
        ] {
            let (_, response) = decode_json_rpc_response_value(&json!({
                "id": "request-1",
                "error": {
                    "code": code,
                    "message": "peer-controlled secret should not escape",
                    "data": {"code": "peer_controlled"}
                }
            }))
            .expect("response");
            let response_error = response.expect_err("authorization error");
            assert_eq!(response_error.authorization_failure(), Some(expected));
            assert_eq!(response_error.to_string(), rendered);

            let error = anyhow::Error::new(response_error);
            assert_eq!(json_rpc_authorization_failure(&error), Some(expected));
        }
    }

    #[test]
    fn rpc_timeout_retires_pending_without_disconnect_and_rejects_late_response() {
        struct HoldingTransport {
            pending: Mutex<PendingJsonRpcRequests>,
            held: Mutex<Option<JsonRpcResponseSender>>,
        }
        impl JsonRpcRequestTransport for HoldingTransport {
            fn send_json_rpc_request(
                &self,
                id: String,
                _: String,
                response: JsonRpcResponseSender,
            ) -> Result<(), String> {
                *self.held.lock().unwrap() = Some(response.clone());
                let mut pending = self.pending.lock().unwrap();
                pending.insert(id, response);
                assert_eq!(
                    pending.len(),
                    1,
                    "request must actually enter pending before timeout"
                );
                Ok(())
            }
        }
        let transport = HoldingTransport {
            pending: Mutex::default(),
            held: Mutex::default(),
        };
        let error = send_json_rpc_request_value(
            &transport,
            "synthetic/timeout",
            json!({}),
            Duration::from_secs(1),
        )
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            request_timeout_message("synthetic/timeout")
        );
        assert!(transport.pending.lock().unwrap().is_empty());
        assert!(
            transport
                .held
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .send(Ok(json!({"late": true})))
                .is_err()
        );
    }

    #[test]
    fn rpc_expired_admission_cannot_turn_a_ready_response_into_success() {
        struct Transport(bool);
        impl JsonRpcRequestTransport for Transport {
            fn send_json_rpc_request(
                &self,
                _: String,
                _: String,
                response: JsonRpcResponseSender,
            ) -> Result<(), String> {
                if self.0 {
                    let _ = response.send(Ok(json!("already too late")));
                }
                Ok(())
            }
        }
        for reply in [true, false] {
            let error = send_json_rpc_request_value(
                &Transport(reply),
                "synthetic/expired",
                json!({}),
                Duration::ZERO,
            )
            .unwrap_err();
            assert_eq!(
                error.to_string(),
                request_timeout_message("synthetic/expired")
            );
        }
    }

    #[test]
    fn rpc_retirement_is_identity_bound_across_replacement_and_incarnations() {
        let mut old_connection = PendingJsonRpcRequests::default();
        let mut new_connection = PendingJsonRpcRequests::default();
        let (old_tx, old_rx) = mpsc::channel();
        let old = JsonRpcResponseSender::new(old_tx, Instant::now() + RPC_REQUEST_TIMEOUT);
        let (new_tx, new_rx) = mpsc::channel();
        let new = JsonRpcResponseSender::new(new_tx, Instant::now() + RPC_REQUEST_TIMEOUT);
        old_connection.insert("same-id", old.clone());
        new_connection.insert("same-id", new.clone());
        old.cancel();
        assert!(old_connection.is_empty());
        assert!(old_rx.recv().unwrap().is_err());
        assert_eq!(new_connection.len(), 1);
        assert!(new_rx.try_recv().is_err());

        let (replacement_tx, replacement_rx) = mpsc::channel();
        let replacement =
            JsonRpcResponseSender::new(replacement_tx, Instant::now() + RPC_REQUEST_TIMEOUT);
        new_connection.insert("same-id", replacement);
        new.cancel();
        assert_eq!(
            new_connection.len(),
            1,
            "old completion must not remove a replacement entry"
        );
        new_connection
            .remove("same-id")
            .unwrap()
            .send(Ok(json!("current")))
            .unwrap();
        assert_eq!(replacement_rx.recv().unwrap(), Ok(json!("current")));
        assert!(new_connection.is_empty());
    }

    #[test]
    fn rpc_cancel_response_race_completes_once_and_drop_retires_registration() {
        let (tx, rx) = mpsc::channel();
        let response = JsonRpcResponseSender::new(tx, Instant::now() + RPC_REQUEST_TIMEOUT);
        let mut pending = PendingJsonRpcRequests::default();
        pending.insert("racing", response.clone());
        let barrier = Arc::new(std::sync::Barrier::new(2));
        std::thread::scope(|scope| {
            let reply = response.clone();
            let reply_barrier = barrier.clone();
            scope.spawn(move || {
                reply_barrier.wait();
                let _ = reply.send(Ok(json!("response")));
            });
            barrier.wait();
            response.cancel();
        });
        let result = rx.recv().unwrap();
        assert!(
            result == Ok(json!("response"))
                || matches!(result, Err(JsonRpcResponseError::Transport { .. }))
        );
        assert!(rx.try_recv().is_err());
        assert!(pending.is_empty());

        let (tx, rx) = mpsc::channel();
        let response = JsonRpcResponseSender::new(tx, Instant::now() + RPC_REQUEST_TIMEOUT);
        let guard = response.retire_on_drop();
        pending.insert("dropped-waiter", response.clone());
        drop(guard);
        assert!(pending.is_empty());
        assert!(rx.recv().unwrap().is_err());
        assert!(response.send(Ok(json!("late"))).is_err());
    }

    #[derive(Debug, serde::Deserialize, PartialEq)]
    struct TypedResult {
        value: String,
    }

    #[test]
    fn rpc_deserialize_json_rpc_result_preserves_method_context() {
        let decoded: TypedResult =
            deserialize_json_rpc_result("demo/method", json!({"value": "ok"})).expect("decode");
        assert_eq!(
            decoded,
            TypedResult {
                value: "ok".to_owned()
            }
        );

        let error = deserialize_json_rpc_result::<TypedResult>("demo/method", json!({"value": 42}))
            .expect_err("decode should fail");
        assert!(format!("{error:#}").contains("failed to decode `demo/method` response payload"));
    }

    #[test]
    fn rpc_timeout_and_worker_failure_messages_match_desktop_contract() {
        assert_eq!(RPC_REQUEST_TIMEOUT, Duration::from_secs(8));
        assert_eq!(RPC_UNSUBSCRIBE_TIMEOUT, Duration::from_secs(1));
        assert_eq!(
            WEBSOCKET_WORKER_UNAVAILABLE_MESSAGE,
            "websocket worker is not available"
        );
        assert_eq!(
            request_timeout_message("thread/start"),
            "timed out waiting for `thread/start` response"
        );
    }

    struct ImmediateTransport {
        response: JsonRpcResponseResult,
    }

    impl JsonRpcRequestTransport for ImmediateTransport {
        fn send_json_rpc_request(
            &self,
            request_id: String,
            payload: String,
            response_tx: JsonRpcResponseSender,
        ) -> std::result::Result<(), String> {
            assert_eq!(request_id.chars().count(), REQUEST_ID_LEN);

            let request: JsonRpcRequest =
                serde_json::from_str(payload.as_str()).expect("request payload");
            assert_eq!(request.id.as_str(), request_id);

            response_tx
                .send(self.response.clone())
                .expect("response should send");
            Ok(())
        }
    }

    struct FailingTransport;

    impl JsonRpcRequestTransport for FailingTransport {
        fn send_json_rpc_request(
            &self,
            _request_id: String,
            _payload: String,
            _response_tx: JsonRpcResponseSender,
        ) -> std::result::Result<(), String> {
            Err(WEBSOCKET_WORKER_UNAVAILABLE_MESSAGE.to_owned())
        }
    }

    #[test]
    fn rpc_send_json_rpc_request_value_uses_transport_and_response_channel() {
        let transport = ImmediateTransport {
            response: Ok(json!({"value": "ok"})),
        };

        let response = send_json_rpc_request_value(
            &transport,
            "demo/method",
            json!({"input": true}),
            RPC_REQUEST_TIMEOUT,
        )
        .expect("response");

        assert_eq!(response, json!({"value": "ok"}));
    }

    #[test]
    fn rpc_send_json_rpc_request_value_reports_transport_failure() {
        let error = send_json_rpc_request_value(
            &FailingTransport,
            "demo/method",
            json!({}),
            RPC_REQUEST_TIMEOUT,
        )
        .expect_err("request should fail");

        assert_eq!(
            format!("{error:#}"),
            WEBSOCKET_WORKER_UNAVAILABLE_MESSAGE.to_owned()
        );
    }

    #[test]
    fn rpc_send_json_rpc_request_typed_deserializes_response() {
        let transport = ImmediateTransport {
            response: Ok(json!({"value": "ok"})),
        };

        let response: TypedResult = send_json_rpc_request_typed(
            &transport,
            "demo/method",
            &json!({"input": true}),
            RPC_REQUEST_TIMEOUT,
        )
        .expect("typed response");

        assert_eq!(
            response,
            TypedResult {
                value: "ok".to_owned()
            }
        );
    }
}

#[cfg(test)]
mod work_boundary_tests {
    use super::*;
    #[test]
    fn actual_sync_rpc_factory_records_failure_and_restores_parent() {
        use pioneer_observability::work::{
            self, Boundary, ExecutorRole, Trigger, WorkCarrier, WorkKind,
        };
        struct Transport(std::cell::RefCell<Option<work::WorkContext>>);
        impl JsonRpcRequestTransport for Transport {
            fn send_json_rpc_request(
                &self,
                _id: String,
                _payload: String,
                response: JsonRpcResponseSender,
            ) -> std::result::Result<(), String> {
                let context = work::current().expect("RPC owner");
                assert_eq!(context.kind, WorkKind::ClientRpc);
                assert!(
                    pioneer_observability::live::snapshot()
                        .unwrap()
                        .active
                        .iter()
                        .any(|w| w.context.token == context.token)
                );
                *self.0.borrow_mut() = Some(context);
                response
                    .send(Err(JsonRpcResponseError::transport("fixture rejection")))
                    .unwrap();
                Ok(())
            }
        }
        let _lock = crate::OBSERVABILITY_TEST_LOCK.lock().unwrap();
        let old = pioneer_observability::telemetry_enabled();
        pioneer_observability::set_telemetry_enabled(true);
        let parent = WorkCarrier::independent(
            Boundary::StdThread,
            WorkKind::ClientCommand,
            Trigger::Event,
            ExecutorRole::ClientController,
        );
        let transport = Transport(Default::default());
        parent.run(|| {
            assert!(
                send_json_rpc_request_value(
                    &transport,
                    "workspace/list",
                    serde_json::json!({}),
                    Duration::from_secs(1)
                )
                .is_err()
            );
            assert_eq!(work::current(), parent.context());
        });
        let child = transport.0.borrow().unwrap();
        assert_eq!(child.parent, parent.context().map(|c| c.token));
        let snapshot = pioneer_observability::live::snapshot().unwrap();
        assert!(
            !snapshot
                .active
                .iter()
                .any(|w| w.context.token == child.token)
        );
        assert!(snapshot.journal.iter().any(|r|matches!(&r.payload,pioneer_observability::live::JournalPayload::Work(w) if w.context.token==child.token && w.state==pioneer_observability::live::WorkState::Terminal(work::Outcome::Failed))));
        pioneer_observability::set_telemetry_enabled(old);
    }
}
