use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value as JsonValue;
use std::fmt;
use std::str::FromStr;

pub const JSONRPC_VERSION: &str = "2.0";
pub const REQUEST_ID_LEN: usize = 21;

pub const PARSE_ERROR_CODE: i64 = -32700;
pub const INVALID_REQUEST_CODE: i64 = -32600;
pub const METHOD_NOT_FOUND_CODE: i64 = -32601;
pub const INVALID_PARAMS_CODE: i64 = -32602;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct RequestId(String);

impl RequestId {
    pub fn new(value: impl Into<String>) -> Result<Self, RequestIdError> {
        let value = value.into();
        if value.chars().count() != REQUEST_ID_LEN {
            return Err(RequestIdError::InvalidLength {
                expected: REQUEST_ID_LEN,
                actual: value.chars().count(),
            });
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl FromStr for RequestId {
    type Err = RequestIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value.to_owned())
    }
}

impl<'de> Deserialize<'de> for RequestId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(D::Error::custom)
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestIdError {
    InvalidLength { expected: usize, actual: usize },
}

impl fmt::Display for RequestIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLength { expected, actual } => {
                write!(
                    f,
                    "request id must be exactly {expected} characters, got {actual}"
                )
            }
        }
    }
}

impl std::error::Error for RequestIdError {}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub id: RequestId,
    pub method: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<JsonValue>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JsonRpcNotification {
    pub jsonrpc: String,
    pub method: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<JsonValue>,
}

impl JsonRpcNotification {
    pub fn from_params<T: Serialize>(
        method: impl Into<String>,
        params: &T,
    ) -> serde_json::Result<Self> {
        Ok(Self {
            jsonrpc: JSONRPC_VERSION.to_owned(),
            method: method.into(),
            params: Some(serde_json::to_value(params)?),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    pub id: RequestId,
    pub result: JsonValue,
}

impl JsonRpcResponse {
    pub fn from_result<T: Serialize>(id: RequestId, result: &T) -> serde_json::Result<Self> {
        Ok(Self {
            jsonrpc: JSONRPC_VERSION.to_owned(),
            id,
            result: serde_json::to_value(result)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JsonRpcErrorResponse {
    pub jsonrpc: String,
    pub id: Option<RequestId>,
    pub error: JsonRpcError,
}

impl JsonRpcErrorResponse {
    pub fn new(id: Option<RequestId>, code: i64, message: impl Into<String>) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION.to_owned(),
            id,
            error: JsonRpcError {
                code,
                message: message.into(),
                data: None,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JsonRpcError {
    pub code: i64,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<JsonValue>,
}

#[cfg(test)]
mod tests {
    use super::{
        JSONRPC_VERSION, JsonRpcErrorResponse, JsonRpcNotification, JsonRpcResponse,
        REQUEST_ID_LEN, RequestId, RequestIdError,
    };
    use serde_json::json;

    #[test]
    fn creates_success_response_with_jsonrpc_version() {
        let request_id = RequestId::new("aaaaaaaaaaaaaaaaaaaaa").expect("request id must be valid");
        let response = JsonRpcResponse::from_result(request_id.clone(), &json!({"ok": true}))
            .expect("response should serialize");

        assert_eq!(response.jsonrpc, JSONRPC_VERSION);
        assert_eq!(response.id, request_id);
        assert_eq!(response.result, json!({"ok": true}));
    }

    #[test]
    fn creates_notification_with_jsonrpc_version() {
        let notification = JsonRpcNotification::from_params("thread/started", &json!({"id": "t1"}))
            .expect("notification should serialize");

        assert_eq!(notification.jsonrpc, JSONRPC_VERSION);
        assert_eq!(notification.method, "thread/started");
        assert_eq!(notification.params, Some(json!({"id": "t1"})));
    }

    #[test]
    fn error_response_defaults_to_null_id_when_absent() {
        let response = JsonRpcErrorResponse::new(None, -32600, "invalid request");
        let encoded =
            serde_json::to_value(response).expect("error response should serialize to json value");

        assert_eq!(encoded.get("id"), Some(&serde_json::Value::Null));
    }

    #[test]
    fn request_id_rejects_invalid_length() {
        let error = RequestId::new("short").expect_err("request id must reject invalid length");
        assert_eq!(
            error,
            RequestIdError::InvalidLength {
                expected: REQUEST_ID_LEN,
                actual: 5
            }
        );
    }

    #[test]
    fn request_id_deserialization_rejects_invalid_length() {
        let error =
            serde_json::from_value::<RequestId>(json!("too-short")).expect_err("must reject id");
        assert!(
            error.to_string().contains("exactly 21 characters"),
            "unexpected error: {error}"
        );
    }
}
