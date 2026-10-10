//! Provider-neutral failure classification shared by transport redaction and agent fallback.
//! Only transport facts and controlled machine codes select recovery policy.

/// Safe adapter-owned invalidation receipt; never accepted from upstream JSON.
pub const REJECTED_CACHED_FILE_REFERENCE_CODE: &str = "pioneer_cached_file_reference_rejected";

use crate::types::ProviderFailureClassification;
use pioneer_protocol::{ProviderFailureClass, ProviderFailureStage};

/// Facts captured at the transport boundary. No upstream text or source chain
/// is retained: even Debug and chained anyhow formatting are safe.
#[derive(Debug, Clone)]
pub struct ProviderTransportFailure {
    pub classification: ProviderFailureClassification,
}

impl std::fmt::Display for ProviderTransportFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("provider request failed")?;
        if let Some(status) = self.classification.http_status {
            write!(f, " (HTTP {status})")?;
        }
        Ok(())
    }
}
impl std::error::Error for ProviderTransportFailure {}

/// Status is authoritative. Only controlled machine codes refine an ordinary
/// request rejection; provider-controlled prose never selects recovery.
pub fn classify_transport_status(
    status: Option<u16>,
    code: Option<&str>,
) -> ProviderFailureClassification {
    let mut result = ProviderFailureClassification::new(match status {
        Some(429) => ProviderFailureClass::RateLimit,
        Some(500..=599) => ProviderFailureClass::Provider5xx,
        Some(401 | 403) => ProviderFailureClass::AuthOrPermission,
        Some(413) => ProviderFailureClass::ContextTooLarge,
        Some(400 | 422) => match code {
            Some("context_length_exceeded" | "prompt_too_long") => {
                ProviderFailureClass::ContextTooLarge
            }
            Some("unsupported_image_input") => ProviderFailureClass::UnsupportedImageInput,
            Some("unsupported_tool_calling") => ProviderFailureClass::UnsupportedToolCalling,
            Some("unsupported_streaming") => ProviderFailureClass::UnsupportedStreaming,
            Some("unsupported_parameter") => ProviderFailureClass::UnsupportedParameter,
            Some("model_not_found") => ProviderFailureClass::ModelNotFound,
            _ => ProviderFailureClass::ProviderRejected,
        },
        Some(404) if code == Some("model_not_found") => ProviderFailureClass::ModelNotFound,
        Some(400..=499) => ProviderFailureClass::ProviderRejected,
        _ => ProviderFailureClass::Unknown,
    });
    result.http_status = status;
    result.provider_code = code
        .filter(|code| {
            matches!(
                *code,
                "context_length_exceeded"
                    | "prompt_too_long"
                    | "unsupported_image_input"
                    | "unsupported_tool_calling"
                    | "unsupported_streaming"
                    | "unsupported_parameter"
                    | "model_not_found"
                    | "invalid_api_key"
                    | "rate_limit_exceeded"
                    | "file_not_found"
                    | "file_expired"
                    | "invalid_request_error"
                    | "authentication_error"
                    | "permission_error"
                    | "overloaded_error"
            )
        })
        .map(str::to_owned);
    result
}

pub(crate) fn retry_after_ms(value: &str, now: chrono::DateTime<chrono::Utc>) -> Option<u64> {
    if let Ok(seconds) = value.trim().parse::<u64>() {
        return Some(seconds.saturating_mul(1000));
    }
    let date = chrono::DateTime::parse_from_rfc2822(value.trim())
        .map(|date| date.with_timezone(&chrono::Utc))
        .ok()
        .or_else(|| {
            ["%A, %d-%b-%y %H:%M:%S GMT", "%a %b %e %H:%M:%S %Y"]
                .into_iter()
                .find_map(|format| {
                    chrono::NaiveDateTime::parse_from_str(value.trim(), format)
                        .ok()
                        .map(|date| date.and_utc())
                })
        })?;
    Some(date.signed_duration_since(now).num_milliseconds().max(0) as u64)
}

pub(crate) fn transport_timeout_error() -> anyhow::Error {
    let mut classification =
        ProviderFailureClassification::new(ProviderFailureClass::NetworkTransient);
    classification.is_network_error = true;
    ProviderTransportFailure { classification }.into()
}

pub(crate) async fn http_error(response: reqwest::Response) -> anyhow::Error {
    let status = response.status().as_u16();
    let header_id = ["x-generation-id", "x-request-id"]
        .into_iter()
        .find_map(|name| {
            response
                .headers()
                .get(name)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| {
                    pioneer_protocol::ProviderRequestId::try_from(value.to_owned()).ok()
                })
        });
    let retry = response
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| retry_after_ms(value, chrono::Utc::now()));
    // Parsing is bounded before allocation. Bad/oversized bodies still retain
    // the original status and headers, without retaining the parsing error.
    let body = crate::http::read_response_bytes_bounded(response, 16 * 1024, "provider_error_body")
        .await
        .ok();
    let value = body
        .as_deref()
        .and_then(|body| serde_json::from_slice::<serde_json::Value>(body).ok());
    let code = value
        .as_ref()
        .and_then(|value| value.get("error"))
        .and_then(|error| error.get("code").or_else(|| error.get("type")))
        .and_then(serde_json::Value::as_str);
    let mut classification = classify_transport_status(Some(status), code);
    classification.retry_after_ms = retry;
    classification.request_id = value
        .as_ref()
        .and_then(|value| value.get("id"))
        .and_then(serde_json::Value::as_str)
        .and_then(|id| pioneer_protocol::ProviderRequestId::try_from(id.to_owned()).ok())
        .or(header_id);
    classification.error_reason = value
        .as_ref()
        .and_then(|value| value.get("error"))
        .and_then(|error| error.get("metadata"))
        .and_then(|metadata| metadata.get("error_type"))
        .and_then(serde_json::Value::as_str)
        .and_then(pioneer_protocol::ProviderErrorReason::from_openrouter_code);
    ProviderTransportFailure { classification }.into()
}

pub fn classify_transport_error(error: &anyhow::Error) -> Option<ProviderFailureClassification> {
    if let Some(typed) = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<ProviderTransportFailure>())
    {
        return Some(typed.classification.clone());
    }
    if let Some(oversized) = error.downcast_ref::<crate::ProviderHttpErrorBodyTooLarge>() {
        return Some(classify_transport_status(Some(oversized.status), None));
    }
    error
        .chain()
        .find_map(|cause| cause.downcast_ref::<reqwest::Error>())
        .map(|transport| {
            let mut classification =
                classify_transport_status(transport.status().map(|status| status.as_u16()), None);
            classification.is_network_error =
                transport.is_connect() || transport.is_timeout() || transport.is_body();
            if classification.http_status.is_none() && classification.is_network_error {
                classification.class = ProviderFailureClass::NetworkTransient;
            }
            classification
        })
}

pub(crate) fn safe_transport_error(error: reqwest::Error) -> anyhow::Error {
    let error = anyhow::Error::new(error);
    ProviderTransportFailure {
        classification: classify_transport_error(&error)
            .unwrap_or_else(|| ProviderFailureClassification::new(ProviderFailureClass::Unknown)),
    }
    .into()
}

#[cfg(test)]
mod transport_contract_tests {
    use super::*;

    #[test]
    fn status_is_authoritative_and_unknown_codes_are_not_retained() {
        for code in [
            None,
            Some("unsupported_image_input"),
            Some("rate limit secret-token"),
            Some("model_not_found"),
        ] {
            assert_eq!(
                classify_transport_status(Some(503), code).class,
                ProviderFailureClass::Provider5xx
            );
            assert_eq!(
                classify_transport_status(Some(401), code).class,
                ProviderFailureClass::AuthOrPermission
            );
        }
        assert_eq!(
            classify_transport_status(Some(404), None).class,
            ProviderFailureClass::ProviderRejected
        );
        assert_eq!(
            classify_transport_status(Some(404), Some("model_not_found")).class,
            ProviderFailureClass::ModelNotFound
        );
        assert!(
            classify_transport_status(Some(400), Some("secret-token"))
                .provider_code
                .is_none()
        );
        assert_eq!(
            classify_transport_status(None, None).class,
            ProviderFailureClass::Unknown
        );
    }

    #[test]
    fn retry_header_seconds_and_http_date_share_the_same_interval() {
        let now = chrono::DateTime::parse_from_rfc2822("Wed, 21 Oct 2015 07:28:00 GMT")
            .unwrap()
            .with_timezone(&chrono::Utc);
        assert_eq!(retry_after_ms(" 120 ", now), Some(120_000));
        assert_eq!(
            retry_after_ms("Wed, 21 Oct 2015 07:30:00 GMT", now),
            Some(120_000)
        );
        assert_eq!(
            retry_after_ms("Wed, 21 Oct 2015 07:27:00 GMT", now),
            Some(0)
        );
        assert_eq!(retry_after_ms("secret unrelated 120", now), None);
    }

    #[tokio::test]
    async fn http_boundary_drops_body_and_preserves_headers_even_when_body_is_oversized() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        for body in [
            "credential=secret-token rate limit unsupported image input".to_owned(),
            "secret-token".repeat(2000),
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = [0u8; 2048];
                socket.read(&mut request).await.unwrap();
                let response = format!(
                    "HTTP/1.1 503 Service Unavailable\r\nRetry-After: 7\r\nx-request-id: req-safe\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                // Oversized bodies can close the reader before the full write.
                let _ = socket.write_all(response.as_bytes()).await;
            });
            let response = reqwest::Client::builder()
                .no_proxy()
                .build()
                .unwrap()
                .get(format!("http://{address}/private-secret-path"))
                .send()
                .await
                .unwrap();
            let error = http_error(response).await;
            let classification = classify_transport_error(&error).unwrap();
            assert_eq!(classification.class, ProviderFailureClass::Provider5xx);
            assert_eq!(classification.http_status, Some(503));
            assert_eq!(classification.retry_after_ms, Some(7000));
            for rendered in [
                format!("{error}"),
                format!("{error:?}"),
                format!("{error:#}"),
            ] {
                assert!(!rendered.contains("secret-token"));
                assert!(!rendered.contains("private-secret-path"));
            }
            assert_eq!(error.chain().count(), 1);
            server.await.unwrap();
        }
    }
}

/// Confirmed protocol completion failure, detected by a stream decoder.
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

/// A native error outcome with an allowlisted cause. Provider-controlled error
/// messages and unknown codes are deliberately discarded at the decode boundary.
/// Display/Debug and downstream diagnostics can never expose response payloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnthropicStreamError {
    Overloaded,
    RateLimit,
    Authentication,
    Permission,
    InvalidRequest,
    NotFound,
    Api,
    Unknown,
}

impl AnthropicStreamError {
    pub fn from_type(error_type: Option<&str>) -> Self {
        match error_type {
            Some("overloaded_error") => Self::Overloaded,
            Some("rate_limit_error") => Self::RateLimit,
            Some("authentication_error") => Self::Authentication,
            Some("permission_error") => Self::Permission,
            Some("invalid_request_error") => Self::InvalidRequest,
            Some("not_found_error") => Self::NotFound,
            Some("api_error") => Self::Api,
            _ => Self::Unknown,
        }
    }

    pub fn code(self) -> &'static str {
        match self {
            Self::Overloaded => "overloaded_error",
            Self::RateLimit => "rate_limit_error",
            Self::Authentication => "authentication_error",
            Self::Permission => "permission_error",
            Self::InvalidRequest => "invalid_request_error",
            Self::NotFound => "not_found_error",
            Self::Api => "api_error",
            Self::Unknown => "unknown_native_error",
        }
    }

    fn classification(self) -> ProviderFailureClassification {
        let class = match self {
            Self::Overloaded | Self::Api => ProviderFailureClass::Provider5xx,
            Self::RateLimit => ProviderFailureClass::RateLimit,
            Self::Authentication | Self::Permission => ProviderFailureClass::AuthOrPermission,
            Self::InvalidRequest | Self::NotFound | Self::Unknown => {
                ProviderFailureClass::ProviderRejected
            }
        };
        let mut classification = ProviderFailureClassification::new(class);
        classification.provider_code = (self != Self::Unknown).then(|| self.code().to_owned());
        // Native events have no HTTP failure status: the response was HTTP 200.
        classification
    }
}

impl std::fmt::Display for AnthropicStreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Anthropic stream failed ({})", self.code())
    }
}

impl std::error::Error for AnthropicStreamError {}

pub fn anthropic_stream_error(error: &anyhow::Error) -> Option<AnthropicStreamError> {
    error
        .chain()
        .find_map(|cause| cause.downcast_ref::<AnthropicStreamError>().copied())
}

pub fn classify_stream_error(error: &anyhow::Error) -> Option<ProviderFailureClassification> {
    if let Some(native) = anthropic_stream_error(error) {
        return Some(native.classification());
    }
    if let Some(native) = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<NativeChatStreamError>())
    {
        return Some(native.classification.clone());
    }
    if let Some(native) = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<NativeStreamStatusError>())
    {
        return Some(ProviderFailureClassification::new(match native.status {
            Some(429) => ProviderFailureClass::RateLimit,
            Some(401 | 403) => ProviderFailureClass::AuthOrPermission,
            Some(500..=599) => ProviderFailureClass::Provider5xx,
            _ => ProviderFailureClass::ProviderRejected,
        }));
    }
    provider_stream_incomplete(error)
        .map(|_| ProviderFailureClassification::new(ProviderFailureClass::StreamTruncated))
}

/// Classify native Chat error details once, then discard all provider-controlled
/// text. Numeric event codes are classification hints, never HTTP status metadata.
pub(crate) fn native_chat_stream_error(code: &str, native_status: Option<u16>) -> anyhow::Error {
    let mut classification = classify_transport_status(
        native_status
            .filter(|status| (100..600).contains(status))
            .or(Some(400)),
        Some(code),
    );
    if native_status.is_none() {
        classification.class = match code {
            "rate_limit_exceeded" => ProviderFailureClass::RateLimit,
            "invalid_api_key" | "authentication_error" | "permission_error" => {
                ProviderFailureClass::AuthOrPermission
            }
            "overloaded_error" => ProviderFailureClass::Provider5xx,
            _ => classification.class,
        };
    }
    classification.http_status = None;
    if classification.class == ProviderFailureClass::Unknown {
        classification.class = ProviderFailureClass::ProviderRejected;
    }
    NativeChatStreamError { classification }.into()
}

#[derive(Debug)]
struct NativeChatStreamError {
    classification: ProviderFailureClassification,
}
impl std::fmt::Display for NativeChatStreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Chat Completions stream failed ({:?})",
            self.classification.class
        )
    }
}
impl std::error::Error for NativeChatStreamError {}

/// Numeric native status, without raw provider text or payloads. This is an
/// event outcome, not the status of the already accepted HTTP response.
#[derive(Debug)]
pub(crate) struct NativeStreamStatusError {
    protocol: &'static str,
    status: Option<u16>,
}

impl NativeStreamStatusError {
    pub(crate) fn new(protocol: &'static str, status: Option<u16>) -> Self {
        Self {
            protocol,
            status: status.filter(|status| (100..600).contains(status)),
        }
    }
}

impl std::fmt::Display for NativeStreamStatusError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} stream failed", self.protocol)?;
        if let Some(status) = self.status {
            write!(f, " (native status {status})")?;
        }
        Ok(())
    }
}

impl std::error::Error for NativeStreamStatusError {}

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

/// Compatibility entry point: only typed status and allowlisted code select
/// policy. The obsolete message/stage arguments have no classification effect.
pub fn classify_provider_failure_class(
    _message_lower: &str,
    _stage: ProviderFailureStage,
    http_status: Option<u16>,
    provider_code: Option<&str>,
) -> ProviderFailureClass {
    classify_transport_status(http_status, provider_code).class
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

#[cfg(test)]
mod replay_classification_tests {
    use super::*;

    #[test]
    fn history_errors_wrapped_by_stream_transport_do_not_disable_streaming() {
        for message in [
            "provider stream error: native replay unsupported: upstream binding authority is unproven",
            "provider stream error: OpenRouter opaque native replay unsupported: upstream prefix/account authority is not exposed by this Chat transport",
            "provider stream error: unsupported history for this endpoint",
        ] {
            assert_ne!(
                classify_provider_failure_class(message, ProviderFailureStage::Connect, None, None),
                ProviderFailureClass::UnsupportedStreaming,
            );
        }
    }

    #[test]
    fn explicit_streaming_rejection_is_still_a_streaming_capability_failure() {
        for message in [
            "model does not support streaming",
            "streaming is not supported",
            "unsupported streaming",
        ] {
            assert_eq!(
                classify_provider_failure_class(
                    message,
                    ProviderFailureStage::Connect,
                    Some(400),
                    Some("unsupported_streaming")
                ),
                ProviderFailureClass::UnsupportedStreaming,
            );
        }
    }
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

#[cfg(test)]
mod native_stream_privacy_tests {
    use super::*;
    #[test]
    fn unknown_native_type_is_safe_rejection_without_invented_provider_code() {
        let error: anyhow::Error =
            AnthropicStreamError::from_type(Some("credential=secret")).into();
        let classification = classify_stream_error(&error).unwrap();
        assert_eq!(classification.class, ProviderFailureClass::ProviderRejected);
        assert!(classification.provider_code.is_none());
        assert!(classification.http_status.is_none());
        assert!(!format!("{error:?}").contains("secret"));
        let chat = native_chat_stream_error("unrecognized private payload", None);
        assert_eq!(
            classify_stream_error(&chat).unwrap().class,
            ProviderFailureClass::ProviderRejected
        );
        assert!(!format!("{chat:?}").contains("private"));
    }

    #[test]
    fn native_machine_code_is_preserved_without_inventing_http_status() {
        let error = native_chat_stream_error("rate_limit_exceeded", None);
        let failure = classify_stream_error(&error).unwrap();
        assert_eq!(failure.class, ProviderFailureClass::RateLimit);
        assert_eq!(
            failure.provider_code.as_deref(),
            Some("rate_limit_exceeded")
        );
        assert!(failure.http_status.is_none());
    }
}
