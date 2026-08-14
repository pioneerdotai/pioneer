//! Runtime driver traits and orchestration primitives.

use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use tokio::io::{AsyncBufRead, AsyncWrite, AsyncWriteExt};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(untagged)]
pub enum JsonlRpcId {
    Number(i64),
    String(String),
}

impl JsonlRpcId {
    pub fn as_pending_key(&self) -> String {
        match self {
            Self::Number(value) => value.to_string(),
            Self::String(value) => value.clone(),
        }
    }
}

impl fmt::Display for JsonlRpcId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Number(value) => write!(f, "{value}"),
            Self::String(value) => f.write_str(value),
        }
    }
}

impl From<i64> for JsonlRpcId {
    fn from(value: i64) -> Self {
        Self::Number(value)
    }
}

impl From<&str> for JsonlRpcId {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

impl From<String> for JsonlRpcId {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl<'de> Deserialize<'de> for JsonlRpcId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = JsonValue::deserialize(deserializer)?;
        match value {
            JsonValue::Number(number) => number
                .as_i64()
                .map(Self::Number)
                .ok_or_else(|| D::Error::custom("jsonl-rpc id number must be a signed integer")),
            JsonValue::String(value) => Ok(Self::String(value)),
            _ => Err(D::Error::custom(
                "jsonl-rpc id must be a string or signed integer",
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JsonlRpcRequest {
    pub method: String,
    pub id: JsonlRpcId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<JsonValue>,
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, JsonValue>,
    #[serde(skip)]
    pub raw: JsonValue,
}

impl JsonlRpcRequest {
    pub fn new(
        id: impl Into<JsonlRpcId>,
        method: impl Into<String>,
        params: Option<JsonValue>,
    ) -> Self {
        Self {
            method: method.into(),
            id: id.into(),
            params,
            extra: BTreeMap::new(),
            raw: JsonValue::Null,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JsonlRpcNotification {
    pub method: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<JsonValue>,
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, JsonValue>,
    #[serde(skip)]
    pub raw: JsonValue,
}

impl JsonlRpcNotification {
    pub fn new(method: impl Into<String>, params: Option<JsonValue>) -> Self {
        Self {
            method: method.into(),
            params,
            extra: BTreeMap::new(),
            raw: JsonValue::Null,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JsonlRpcResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<JsonlRpcId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<JsonValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonlRpcError>,
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, JsonValue>,
    #[serde(skip)]
    pub raw: JsonValue,
}

impl JsonlRpcResponse {
    pub fn is_error(&self) -> bool {
        self.error.is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JsonlRpcError {
    pub code: i64,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<JsonValue>,
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum JsonlRpcIncomingMessage {
    Response(JsonlRpcResponse),
    Notification(JsonlRpcNotification),
    ServerRequest(JsonlRpcRequest),
}

impl JsonlRpcIncomingMessage {
    pub fn raw(&self) -> &JsonValue {
        match self {
            Self::Response(message) => &message.raw,
            Self::Notification(message) => &message.raw,
            Self::ServerRequest(message) => &message.raw,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonlRpcDecodeErrorKind {
    EmptyLine,
    MalformedJson,
    ExpectedObject,
    UnknownShape,
    InvalidMessage,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonlRpcDecodeError {
    kind: JsonlRpcDecodeErrorKind,
    message: String,
}

impl JsonlRpcDecodeError {
    pub fn kind(&self) -> JsonlRpcDecodeErrorKind {
        self.kind
    }

    pub(crate) fn new(kind: JsonlRpcDecodeErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

impl fmt::Display for JsonlRpcDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for JsonlRpcDecodeError {}

pub fn encode_jsonl_rpc_line<T>(message: &T) -> serde_json::Result<String>
where
    T: Serialize,
{
    let mut encoded = serde_json::to_string(message)?;
    encoded.push('\n');
    Ok(encoded)
}

pub fn encode_jsonl_rpc_request_line(request: &JsonlRpcRequest) -> serde_json::Result<String> {
    encode_jsonl_rpc_line(request)
}

pub fn encode_jsonl_rpc_notification_line(
    notification: &JsonlRpcNotification,
) -> serde_json::Result<String> {
    encode_jsonl_rpc_line(notification)
}

pub async fn write_jsonl_rpc_request<W>(
    writer: &mut W,
    request: &JsonlRpcRequest,
) -> serde_json::Result<()>
where
    W: AsyncWrite + Unpin,
{
    write_jsonl_rpc_message(writer, request).await
}

pub async fn write_jsonl_rpc_notification<W>(
    writer: &mut W,
    notification: &JsonlRpcNotification,
) -> serde_json::Result<()>
where
    W: AsyncWrite + Unpin,
{
    write_jsonl_rpc_message(writer, notification).await
}

pub async fn write_jsonl_rpc_message<W, T>(writer: &mut W, message: &T) -> serde_json::Result<()>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let encoded = encode_jsonl_rpc_line(message)?;
    writer
        .write_all(encoded.as_bytes())
        .await
        .map_err(serde_json::Error::io)?;
    writer.flush().await.map_err(serde_json::Error::io)
}

pub async fn read_jsonl_rpc_message<R>(
    reader: &mut R,
) -> Result<Option<JsonlRpcIncomingMessage>, JsonlRpcDecodeError>
where
    R: AsyncBufRead + Unpin,
{
    read_jsonl_rpc_message_with_budget(reader, crate::NativeEventBudget::default()).await
}

pub async fn read_jsonl_rpc_message_with_budget<R>(
    reader: &mut R,
    budget: crate::NativeEventBudget,
) -> Result<Option<JsonlRpcIncomingMessage>, JsonlRpcDecodeError>
where
    R: AsyncBufRead + Unpin,
{
    let codec = crate::BoundedNativeEventCodec::new(budget);
    let Some(line) = codec.read_frame(reader).await.map_err(|error| {
        JsonlRpcDecodeError::new(JsonlRpcDecodeErrorKind::InvalidMessage, error.to_string())
    })?
    else {
        return Ok(None);
    };
    decode_jsonl_rpc_frame_with_budget(&line, budget).map(Some)
}

pub(crate) fn decode_jsonl_rpc_frame_with_budget(
    frame: &[u8],
    budget: crate::NativeEventBudget,
) -> Result<JsonlRpcIncomingMessage, JsonlRpcDecodeError> {
    let codec = crate::BoundedNativeEventCodec::new(budget);
    let line = std::str::from_utf8(frame).map_err(|_| {
        JsonlRpcDecodeError::new(
            JsonlRpcDecodeErrorKind::InvalidMessage,
            "jsonl-rpc line is not UTF-8",
        )
    })?;
    let message = decode_jsonl_rpc_line(line)?;
    let raw = serde_json::from_str::<JsonValue>(line.trim_end_matches(['\r', '\n'])).map_err(
        |error| JsonlRpcDecodeError::new(JsonlRpcDecodeErrorKind::MalformedJson, error.to_string()),
    )?;
    codec.validate_value(&raw).map_err(|error| {
        JsonlRpcDecodeError::new(JsonlRpcDecodeErrorKind::InvalidMessage, error.to_string())
    })?;
    Ok(message)
}

pub fn decode_jsonl_rpc_line(line: &str) -> Result<JsonlRpcIncomingMessage, JsonlRpcDecodeError> {
    let line = line.trim_end_matches(['\r', '\n']);
    if line.trim().is_empty() {
        return Err(JsonlRpcDecodeError::new(
            JsonlRpcDecodeErrorKind::EmptyLine,
            "jsonl-rpc line is empty",
        ));
    }

    let raw: JsonValue = serde_json::from_str(line).map_err(|error| {
        JsonlRpcDecodeError::new(
            JsonlRpcDecodeErrorKind::MalformedJson,
            format!("malformed jsonl-rpc json: {error}"),
        )
    })?;

    let object = raw.as_object().ok_or_else(|| {
        JsonlRpcDecodeError::new(
            JsonlRpcDecodeErrorKind::ExpectedObject,
            "jsonl-rpc message must be a json object",
        )
    })?;

    let has_result_or_error = object.contains_key("result") || object.contains_key("error");
    let method = object.get("method");
    let has_id = object.contains_key("id");

    if has_result_or_error {
        return decode_response(raw);
    }

    if let Some(method) = method {
        if !method.is_string() {
            return Err(JsonlRpcDecodeError::new(
                JsonlRpcDecodeErrorKind::InvalidMessage,
                "jsonl-rpc method must be a string",
            ));
        }

        if has_id {
            return decode_server_request(raw);
        }

        return decode_notification(raw);
    }

    Err(JsonlRpcDecodeError::new(
        JsonlRpcDecodeErrorKind::UnknownShape,
        "jsonl-rpc object is not a response, notification, or server request",
    ))
}

fn decode_response(raw: JsonValue) -> Result<JsonlRpcIncomingMessage, JsonlRpcDecodeError> {
    let Some(raw_object) = raw.as_object() else {
        return Err(JsonlRpcDecodeError::new(
            JsonlRpcDecodeErrorKind::ExpectedObject,
            "jsonl-rpc line must decode to an object",
        ));
    };
    let has_result = raw_object.contains_key("result");
    let has_error = raw_object.contains_key("error");
    let mut response: JsonlRpcResponse =
        serde_json::from_value(raw.clone()).map_err(invalid_message_error)?;
    if has_result && response.result.is_none() {
        response.result = Some(JsonValue::Null);
    }
    match (has_result, has_error) {
        (true, true) => {
            return Err(JsonlRpcDecodeError::new(
                JsonlRpcDecodeErrorKind::InvalidMessage,
                "jsonl-rpc response cannot contain both result and error",
            ));
        }
        (false, false) => {
            return Err(JsonlRpcDecodeError::new(
                JsonlRpcDecodeErrorKind::InvalidMessage,
                "jsonl-rpc response must contain result or error",
            ));
        }
        _ => {}
    }
    if has_error && response.error.is_none() {
        return Err(JsonlRpcDecodeError::new(
            JsonlRpcDecodeErrorKind::InvalidMessage,
            "jsonl-rpc error must be an object",
        ));
    }
    response.raw = raw;
    Ok(JsonlRpcIncomingMessage::Response(response))
}

fn decode_notification(raw: JsonValue) -> Result<JsonlRpcIncomingMessage, JsonlRpcDecodeError> {
    let mut notification: JsonlRpcNotification =
        serde_json::from_value(raw.clone()).map_err(invalid_message_error)?;
    notification.raw = raw;
    Ok(JsonlRpcIncomingMessage::Notification(notification))
}

fn decode_server_request(raw: JsonValue) -> Result<JsonlRpcIncomingMessage, JsonlRpcDecodeError> {
    let mut request: JsonlRpcRequest =
        serde_json::from_value(raw.clone()).map_err(invalid_message_error)?;
    request.raw = raw;
    Ok(JsonlRpcIncomingMessage::ServerRequest(request))
}

fn invalid_message_error(error: serde_json::Error) -> JsonlRpcDecodeError {
    JsonlRpcDecodeError::new(
        JsonlRpcDecodeErrorKind::InvalidMessage,
        format!("invalid jsonl-rpc message: {error}"),
    )
}

#[cfg(test)]
mod tests {
    use super::{
        JsonlRpcDecodeErrorKind, JsonlRpcId, JsonlRpcIncomingMessage, JsonlRpcNotification,
        JsonlRpcRequest, decode_jsonl_rpc_line, encode_jsonl_rpc_notification_line,
        encode_jsonl_rpc_request_line, read_jsonl_rpc_message, write_jsonl_rpc_request,
    };
    use serde_json::{Value as JsonValue, json};
    use tokio::io::BufReader;

    #[test]
    fn jsonl_rpc_encodes_client_request_without_jsonrpc_header() {
        let request = JsonlRpcRequest::new(
            0,
            "initialize",
            Some(json!({
                "clientInfo": {
                    "name": "pioneer_desktop",
                    "title": "Pioneer",
                    "version": "0.1.0"
                }
            })),
        );

        let encoded =
            encode_jsonl_rpc_request_line(&request).expect("request should serialize to JSONL");
        assert!(encoded.ends_with('\n'));

        let decoded: JsonValue =
            serde_json::from_str(&encoded).expect("encoded request should be valid JSON");
        assert_eq!(decoded.get("method"), Some(&json!("initialize")));
        assert_eq!(decoded.get("id"), Some(&json!(0)));
        assert!(decoded.get("params").is_some());
        assert!(decoded.get("jsonrpc").is_none());
    }

    #[test]
    fn jsonl_rpc_encodes_client_notification_without_id_or_jsonrpc_header() {
        let notification = JsonlRpcNotification::new("initialized", Some(json!({})));

        let encoded = encode_jsonl_rpc_notification_line(&notification)
            .expect("notification should serialize to JSONL");
        let decoded: JsonValue =
            serde_json::from_str(&encoded).expect("encoded notification should be valid JSON");

        assert_eq!(decoded, json!({"method": "initialized", "params": {}}));
    }

    #[test]
    fn jsonl_rpc_decodes_initialize_response_and_preserves_unknown_fields() {
        let message = decode_jsonl_rpc_line(include_str!(
            "../tests/fixtures/jsonl_rpc/initialize_response.jsonl"
        ))
        .expect("initialize response should decode");

        let JsonlRpcIncomingMessage::Response(response) = message else {
            panic!("expected response");
        };
        assert_eq!(response.id, Some(JsonlRpcId::Number(0)));
        assert_eq!(
            response.result,
            Some(json!({"userAgent": "codex", "platformFamily": "unix"}))
        );
        assert_eq!(response.extra.get("future"), Some(&json!(true)));
        assert_eq!(
            response.raw,
            json!({"id": 0, "result": {"userAgent": "codex", "platformFamily": "unix"}, "future": true})
        );
    }

    #[test]
    fn jsonl_rpc_decodes_unknown_notification_as_raw_event() {
        let message = decode_jsonl_rpc_line(include_str!(
            "../tests/fixtures/jsonl_rpc/notification.jsonl"
        ))
        .expect("notification should decode");

        let JsonlRpcIncomingMessage::Notification(notification) = message else {
            panic!("expected notification");
        };
        assert_eq!(notification.method, "item/future_event");
        assert_eq!(notification.params, Some(json!({"item": {"id": "i1"}})));
        assert_eq!(notification.extra.get("future"), Some(&json!(1)));
        assert_eq!(
            notification.raw,
            json!({"method": "item/future_event", "params": {"item": {"id": "i1"}}, "future": 1})
        );
    }

    #[test]
    fn jsonl_rpc_decodes_server_request() {
        let message = decode_jsonl_rpc_line(include_str!(
            "../tests/fixtures/jsonl_rpc/server_request.jsonl"
        ))
        .expect("server request should decode");

        let JsonlRpcIncomingMessage::ServerRequest(request) = message else {
            panic!("expected server request");
        };
        assert_eq!(request.method, "command/approval/request");
        assert_eq!(request.id, JsonlRpcId::Number(17));
        assert_eq!(request.params, Some(json!({"requestId": "r1"})));
        assert_eq!(request.extra.get("future"), Some(&json!("x")));
    }

    #[test]
    fn jsonl_rpc_decodes_error_response() {
        let message = decode_jsonl_rpc_line(include_str!(
            "../tests/fixtures/jsonl_rpc/error_response.jsonl"
        ))
        .expect("error response should decode");

        let JsonlRpcIncomingMessage::Response(response) = message else {
            panic!("expected response");
        };
        assert_eq!(response.id, Some(JsonlRpcId::Number(10)));
        assert!(response.result.is_none());
        let error = response.error.expect("error response must have error");
        assert_eq!(error.code, 123);
        assert_eq!(error.message, "Something went wrong");
        assert_eq!(error.data, Some(json!({"retry": false})));
        assert_eq!(response.extra.get("retryable"), Some(&json!(false)));
    }

    #[test]
    fn jsonl_rpc_decodes_null_result_response() {
        let message = decode_jsonl_rpc_line(r#"{"id":10,"result":null}"#)
            .expect("null result response should decode");

        let JsonlRpcIncomingMessage::Response(response) = message else {
            panic!("expected response");
        };
        assert_eq!(response.id, Some(JsonlRpcId::Number(10)));
        assert_eq!(response.result, Some(JsonValue::Null));
        assert!(response.error.is_none());
    }

    #[test]
    fn jsonl_rpc_classifies_malformed_json() {
        let error = decode_jsonl_rpc_line(r#"{"id":0"#).expect_err("must reject malformed json");
        assert_eq!(error.kind(), JsonlRpcDecodeErrorKind::MalformedJson);
    }

    #[test]
    fn jsonl_rpc_classifies_unknown_shape() {
        let error = decode_jsonl_rpc_line(r#"{"params":{}}"#).expect_err("must reject shape");
        assert_eq!(error.kind(), JsonlRpcDecodeErrorKind::UnknownShape);
    }

    #[tokio::test]
    async fn jsonl_rpc_reads_one_line_from_buffer() {
        let input = br#"{"id":"abc","result":{"ok":true}}
"#;
        let mut reader = BufReader::new(&input[..]);

        let message = read_jsonl_rpc_message(&mut reader)
            .await
            .expect("line should decode")
            .expect("reader should produce message");

        let JsonlRpcIncomingMessage::Response(response) = message else {
            panic!("expected response");
        };
        assert_eq!(response.id, Some(JsonlRpcId::String("abc".to_owned())));
        assert_eq!(response.result, Some(json!({"ok": true})));
    }

    #[tokio::test]
    async fn jsonl_rpc_writes_request_line() {
        let request = JsonlRpcRequest::new(1, "thread/start", Some(json!({"model": "gpt-5.4"})));
        let mut output = Vec::new();

        write_jsonl_rpc_request(&mut output, &request)
            .await
            .expect("request should write");

        let encoded = String::from_utf8(output).expect("output should be utf8");
        assert_eq!(
            encoded,
            r#"{"method":"thread/start","id":1,"params":{"model":"gpt-5.4"}}
"#
        );
    }
}
