use pioneer_protocol::{
    JSONRPC_VERSION, JsonRpcError, JsonRpcErrorResponse, PUBLIC_ERROR_VERSION, PublicError,
    PublicErrorCode, PublicErrorStage, RequestId,
};

const PUBLIC_ERROR_MESSAGE_MAX_BYTES: usize = 256;

/// Maps internal failure chains to the only error shape allowed to cross a
/// public transport or persistence projection boundary.
pub(crate) fn map_agent_failure(
    code: PublicErrorCode,
    stage: PublicErrorStage,
    raw_diagnostic: impl std::fmt::Display,
) -> PublicError {
    let raw_diagnostic = raw_diagnostic.to_string();
    let correlation_id = uuid::Uuid::new_v4().to_string();
    tracing::error!(
        correlation_id,
        stage = ?stage,
        code = ?code,
        raw_diagnostic,
        "agent-domain operation failed"
    );
    PublicError {
        version: PUBLIC_ERROR_VERSION,
        code,
        stage,
        message: bounded_message(public_message(code)),
        retryable: matches!(
            code,
            PublicErrorCode::Unavailable | PublicErrorCode::Timeout | PublicErrorCode::Conflict
        ),
        retry_after_ms: None,
        correlation_id,
    }
}

/// Builds the only JSON-RPC error shape that agent-domain operations may expose.
///
/// `raw_diagnostic` is deliberately consumed only by [`map_agent_failure`], which
/// records it against a correlation id. It is never copied into the transport
/// message or `data` projection.
pub(crate) fn agent_rpc_error(
    request_id: Option<RequestId>,
    jsonrpc_code: i64,
    public_code: PublicErrorCode,
    stage: PublicErrorStage,
    raw_diagnostic: impl std::fmt::Display,
) -> JsonRpcErrorResponse {
    let public_error = map_agent_failure(public_code, stage, raw_diagnostic);
    JsonRpcErrorResponse {
        jsonrpc: JSONRPC_VERSION.to_owned(),
        id: request_id,
        error: JsonRpcError {
            code: jsonrpc_code,
            message: public_error.message.clone(),
            data: Some(serde_json::json!({ "public_error": public_error })),
        },
    }
}

fn public_message(code: PublicErrorCode) -> &'static str {
    match code {
        PublicErrorCode::InvalidInput => "The request is invalid.",
        PublicErrorCode::PolicyDenied => "This operation is not permitted.",
        PublicErrorCode::NotFound => "The requested resource is unavailable.",
        PublicErrorCode::Conflict => "The operation conflicts with current state.",
        PublicErrorCode::ResourceExhausted => "The operation exceeds its resource budget.",
        PublicErrorCode::Unavailable => "The agent service is temporarily unavailable.",
        PublicErrorCode::Timeout => "The agent operation timed out.",
        PublicErrorCode::Internal => "The agent operation could not be completed.",
    }
}

fn bounded_message(message: &str) -> String {
    if message.len() <= PUBLIC_ERROR_MESSAGE_MAX_BYTES {
        return message.to_owned();
    }
    let mut end = PUBLIC_ERROR_MESSAGE_MAX_BYTES;
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    message[..end].to_owned()
}

#[cfg(test)]
mod tests {
    use pioneer_protocol::{PublicErrorCode, PublicErrorStage, RequestId};

    use super::agent_rpc_error;

    #[test]
    fn rpc_boundary_never_serializes_raw_diagnostics() {
        let canary = "postgres://secret@host/db /Users/operator/.ssh/id_ed25519 bearer-token";
        let response = agent_rpc_error(
            Some(RequestId::new("aaaaaaaaaaaaaaaaaaaaa").expect("valid request id")),
            -32600,
            PublicErrorCode::Internal,
            PublicErrorStage::Execution,
            format_args!("runtime failed: {canary}"),
        );
        let encoded = serde_json::to_string(&response).expect("public error must serialize");

        assert!(!encoded.contains(canary));
        assert!(!encoded.contains("id_ed25519"));
        assert!(!encoded.contains("bearer-token"));
        let public_error: pioneer_protocol::PublicError = response
            .error
            .data
            .as_ref()
            .and_then(|value| value.get("public_error"))
            .cloned()
            .and_then(|value| serde_json::from_value(value).ok())
            .expect("typed public error must be present");
        assert_eq!(public_error.code, PublicErrorCode::Internal);
        assert_eq!(public_error.stage, PublicErrorStage::Execution);
        assert_eq!(response.error.message, public_error.message);
        assert!(!public_error.correlation_id.is_empty());
    }
}
