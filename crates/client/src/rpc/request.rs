//! JSON-RPC request lifecycle primitives.

use anyhow::{Context, Result, anyhow};
use pioneer_protocol::{JSONRPC_VERSION, JsonRpcRequest, REQUEST_ID_LEN, RequestId, generate_id};
use serde::de::DeserializeOwned;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::sync::mpsc::Sender;
use std::time::Duration;

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

pub type JsonRpcResponseResult = std::result::Result<JsonValue, String>;
pub type JsonRpcResponseSender = Sender<JsonRpcResponseResult>;

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
    pending: HashMap<String, JsonRpcResponseSender>,
}

impl PendingJsonRpcRequests {
    pub fn insert(
        &mut self,
        request_id: impl Into<String>,
        response_tx: JsonRpcResponseSender,
    ) -> Option<JsonRpcResponseSender> {
        self.pending.insert(request_id.into(), response_tx)
    }

    pub fn remove(&mut self, request_id: &str) -> Option<JsonRpcResponseSender> {
        self.pending.remove(request_id)
    }

    pub fn fail_all(&mut self, error: &str) -> usize {
        let count = self.pending.len();
        for (_, response_tx) in self.pending.drain() {
            let _ = response_tx.send(Err(error.to_owned()));
        }
        count
    }

    pub fn len(&self) -> usize {
        self.pending.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
}

pub fn new_request_id() -> String {
    generate_id(REQUEST_ID_LEN)
}

pub fn build_json_rpc_request_payload(
    method: &str,
    params: JsonValue,
) -> Result<JsonRpcRequestPayload> {
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
        let code = error
            .get("data")
            .and_then(|data| data.get("code"))
            .and_then(serde_json::Value::as_str);
        let message = code
            .map(|code| format!("{message} [{code}]"))
            .unwrap_or_else(|| message.to_owned());
        return Some((response_id, Err(message)));
    }

    Some((
        response_id,
        Err(INVALID_JSON_RPC_RESPONSE_PAYLOAD_MESSAGE.to_owned()),
    ))
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
    let request = build_json_rpc_request_payload(method, params)?;
    let (response_tx, response_rx) = std::sync::mpsc::channel();

    transport
        .send_json_rpc_request(request.request_id, request.payload, response_tx)
        .map_err(anyhow::Error::msg)?;

    let response = response_rx
        .recv_timeout(timeout)
        .map_err(|_| anyhow!("{}", request_timeout_message(method)))?;

    response.map_err(anyhow::Error::msg)
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

        assert!(pending.insert("request-1", response_tx).is_none());
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

        pending.insert("request-1", first_tx);
        pending.insert("request-2", second_tx);

        assert_eq!(pending.fail_all("websocket closed"), 2);
        assert!(pending.is_empty());
        assert_eq!(
            first_rx.recv().expect("first response"),
            Err("websocket closed".to_owned())
        );
        assert_eq!(
            second_rx.recv().expect("second response"),
            Err("websocket closed".to_owned())
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
            Some(("request-1".to_owned(), Err("boom".to_owned())))
        );
        assert_eq!(
            decode_json_rpc_response_value(&json!({
                "id": "request-1",
                "error": {
                    "message": "session is no longer active",
                    "data": {"code": "session_revoked"}
                }
            })),
            Some((
                "request-1".to_owned(),
                Err("session is no longer active [session_revoked]".to_owned())
            ))
        );
        assert_eq!(
            decode_json_rpc_response_value(&json!({
                "id": "request-1",
                "error": {}
            })),
            Some((
                "request-1".to_owned(),
                Err(JSON_RPC_REQUEST_FAILED_MESSAGE.to_owned())
            ))
        );
        assert_eq!(
            decode_json_rpc_response_value(&json!({
                "id": "request-1"
            })),
            Some((
                "request-1".to_owned(),
                Err(INVALID_JSON_RPC_RESPONSE_PAYLOAD_MESSAGE.to_owned())
            ))
        );
        assert_eq!(
            decode_json_rpc_response_value(&json!({
                "method": "workspace/changed"
            })),
            None
        );
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

    #[test]
    fn rpc_fail_pending_json_rpc_requests_delegates_to_pending_map() {
        let (response_tx, response_rx) = mpsc::channel();
        let mut pending = PendingJsonRpcRequests::default();
        pending.insert("request-1", response_tx);

        assert_eq!(
            fail_pending_json_rpc_requests(&mut pending, "websocket closed"),
            1
        );
        assert!(pending.is_empty());
        assert_eq!(
            response_rx.recv().expect("response"),
            Err("websocket closed".to_owned())
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
