//! Provider-neutral failure classification shared by transport redaction and agent fallback.
//! Raw adapter text is classified before it is discarded by the endpoint boundary.

use crate::types::ProviderFailureClassification;
use pioneer_protocol::{ProviderFailureClass, ProviderFailureStage};

/// Confirmed protocol completion failure, detected by the OpenRouter decoder.
/// This contains no response, endpoint or provider-controlled detail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderStreamIncomplete {
    EofWithoutTerminalMarker,
    DoneWithoutFinishReason,
}

impl std::fmt::Display for ProviderStreamIncomplete {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::EofWithoutTerminalMarker => "provider stream ended before a terminal marker",
            Self::DoneWithoutFinishReason => "provider stream ended without a finish_reason",
        })
    }
}

impl std::error::Error for ProviderStreamIncomplete {}

/// Endpoint redaction retains only this safe typed source, never the raw error.
pub fn provider_stream_incomplete(error: &anyhow::Error) -> Option<ProviderStreamIncomplete> {
    error
        .chain()
        .find_map(|cause| cause.downcast_ref::<ProviderStreamIncomplete>().copied())
}

/// A bounded error body has no safe provider detail to classify. Preserve the
/// existing status-only fallback used by agent for this typed transport error.
pub fn classify_http_error_body_too_large(status: u16) -> ProviderFailureClassification {
    let class = match status {
        429 => ProviderFailureClass::RateLimit,
        500..=599 => ProviderFailureClass::Provider5xx,
        _ => ProviderFailureClass::ProviderRejected,
    };
    let mut classification = ProviderFailureClassification::new(class);
    classification.http_status = Some(status);
    classification
}

/// Classify already-lowercased adapter text using the established agent fallback.
/// Callers with a typed HTTP status should pass it instead of inferring one
/// from a URL or server-controlled body.
pub fn classify_provider_failure_class(
    message_lower: &str,
    stage: ProviderFailureStage,
    http_status: Option<u16>,
    provider_code: Option<&str>,
) -> ProviderFailureClass {
    if matches!(
        stage,
        ProviderFailureStage::FirstChunk | ProviderFailureStage::MidStream
    ) && message_lower.contains("stream stall")
    {
        return ProviderFailureClass::StreamStall;
    }
    if message_lower.contains("stream truncated") {
        return ProviderFailureClass::StreamTruncated;
    }
    if message_lower.contains("max_output_tokens")
        || message_lower.contains("maximum output tokens")
        || message_lower.contains("output token limit")
    {
        return ProviderFailureClass::MaxOutputTokens;
    }
    if message_lower.contains("prompt too long")
        || message_lower.contains("context length")
        || message_lower.contains("maximum context")
        || message_lower.contains("context too long")
        || message_lower.contains("context window")
        || http_status == Some(413)
    {
        return ProviderFailureClass::ContextTooLarge;
    }
    if http_status == Some(429) || message_lower.contains("rate limit") {
        return ProviderFailureClass::RateLimit;
    }
    if http_status.is_some_and(|status| (500..600).contains(&status)) {
        return ProviderFailureClass::Provider5xx;
    }
    if http_status == Some(401)
        || (http_status == Some(403)
            && (message_lower.contains("token expired")
                || message_lower.contains("token revoked")
                || message_lower.contains("unauthorized")
                || message_lower.contains("authentication")))
        || provider_code
            .map(|value| {
                value.contains("invalid_api_key")
                    || value.contains("auth")
                    || value.contains("token_expired")
            })
            .unwrap_or(false)
    {
        return ProviderFailureClass::AuthExpired;
    }
    if is_image_input_capability_mismatch(message_lower) {
        return ProviderFailureClass::UnsupportedImageInput;
    }
    if is_tool_calling_capability_mismatch(message_lower) {
        return ProviderFailureClass::UnsupportedToolCalling;
    }
    if is_streaming_capability_mismatch(message_lower) {
        return ProviderFailureClass::UnsupportedStreaming;
    }
    if is_unsupported_parameter(message_lower, provider_code) {
        return ProviderFailureClass::UnsupportedParameter;
    }
    if is_generic_capability_mismatch(message_lower) {
        return ProviderFailureClass::UnsupportedCapability;
    }
    if http_status == Some(404)
        || message_lower.contains("model not found")
        || message_lower.contains("unknown model")
        || message_lower.contains("no such model")
    {
        return ProviderFailureClass::ModelNotFound;
    }
    if http_status == Some(403)
        || message_lower.contains("permission denied")
        || message_lower.contains("forbidden")
    {
        return ProviderFailureClass::AuthOrPermission;
    }
    if is_malformed_provider_request(message_lower, provider_code) {
        return ProviderFailureClass::MalformedProviderRequest;
    }
    if http_status == Some(400)
        || message_lower.contains("invalid request")
        || message_lower.contains("bad request")
    {
        return ProviderFailureClass::ProviderRejected;
    }
    if message_lower.contains("error sending request")
        || message_lower.contains("connection")
        || message_lower.contains("dns")
        || message_lower.contains("timed out")
        || message_lower.contains("tunnel error")
        || message_lower.contains("unexpected end of file")
        || message_lower.contains("connection reset")
        || message_lower.contains("broken pipe")
    {
        return ProviderFailureClass::NetworkTransient;
    }
    if matches!(
        stage,
        ProviderFailureStage::FirstChunk | ProviderFailureStage::MidStream
    ) {
        return ProviderFailureClass::StreamStall;
    }
    ProviderFailureClass::Unknown
}

fn is_image_input_capability_mismatch(message_lower: &str) -> bool {
    message_lower.contains("image input")
        && (message_lower.contains("no endpoints found")
            || message_lower.contains("does not support")
            || message_lower.contains("not support")
            || message_lower.contains("unsupported"))
}

fn is_tool_calling_capability_mismatch(message_lower: &str) -> bool {
    (message_lower.contains("tool call")
        || message_lower.contains("tool use")
        || message_lower.contains("function call")
        || message_lower.contains("tools"))
        && (message_lower.contains("does not support")
            || message_lower.contains("not support")
            || message_lower.contains("unsupported")
            || message_lower.contains("no endpoints found"))
}

fn is_streaming_capability_mismatch(message_lower: &str) -> bool {
    message_lower.contains("stream")
        && (message_lower.contains("does not support")
            || message_lower.contains("not support")
            || message_lower.contains("unsupported")
            || message_lower.contains("streaming disabled"))
}

fn is_unsupported_parameter(message_lower: &str, provider_code: Option<&str>) -> bool {
    provider_code
        .map(|value| {
            value.contains("unsupported_parameter")
                || value.contains("unknown_parameter")
                || value.contains("unrecognized_parameter")
        })
        .unwrap_or(false)
        || message_lower.contains("unsupported parameter")
        || message_lower.contains("unknown parameter")
        || message_lower.contains("unrecognized parameter")
        || message_lower.contains("unrecognized request argument")
        || message_lower.contains("extra inputs are not permitted")
}

fn is_generic_capability_mismatch(message_lower: &str) -> bool {
    (message_lower.contains("does not support")
        || message_lower.contains("not support")
        || message_lower.contains("unsupported"))
        && (message_lower.contains("capability")
            || message_lower.contains("feature")
            || message_lower.contains("modality")
            || message_lower.contains("endpoint"))
}

fn is_malformed_provider_request(message_lower: &str, provider_code: Option<&str>) -> bool {
    provider_code
        .map(|value| {
            value.contains("invalid_request_error")
                || value.contains("invalid_request")
                || value.contains("bad_request")
        })
        .unwrap_or(false)
        && (message_lower.contains("schema")
            || message_lower.contains("malformed")
            || message_lower.contains("invalid json")
            || message_lower.contains("parse"))
}

/// Extract an untrusted provider code for classification; do not publish it
/// from an endpoint-redacted error without separate sanitization.
pub fn extract_provider_code(message: &str) -> Option<String> {
    let marker = "\"code\":\"";
    let start = message.find(marker)?;
    let rest = &message[start + marker.len()..];
    let end = rest.find('"')?;
    Some(rest[..end].to_owned())
}

/// Read a numeric retry interval from already-lowercased adapter text.
pub fn extract_retry_after_ms(message_lower: &str) -> Option<u64> {
    let marker = "retry-after";
    let index = message_lower.find(marker)?;
    let rest = &message_lower[index + marker.len()..];
    let seconds = rest
        .chars()
        .skip_while(|ch| !ch.is_ascii_digit())
        .take_while(|ch| ch.is_ascii_digit())
        .collect::<String>();
    let secs = seconds.parse::<u64>().ok()?;
    Some(secs.saturating_mul(1000))
}
