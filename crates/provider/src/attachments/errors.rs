use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachmentPipelineError {
    code: &'static str,
    message: String,
}

impl AttachmentPipelineError {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn attachment_too_large(size: usize, max_size: usize, _label: &str) -> Self {
        Self::new(
            "ATTACHMENT_TOO_LARGE",
            format!("attachment is too large: {size} bytes exceeds max {max_size} bytes"),
        )
    }

    pub fn attachment_total_budget_exceeded(total: usize, max_total: usize) -> Self {
        Self::new(
            "ATTACHMENT_TOTAL_BUDGET_EXCEEDED",
            format!(
                "attachment total budget exceeded: {total} bytes exceeds max {max_total} bytes"
            ),
        )
    }

    pub fn declared_size_mismatch(declared: u64, actual: usize, _label: &str) -> Self {
        Self::new(
            "ATTACHMENT_SIZE_MISMATCH",
            format!("attachment declared {declared} bytes but materialized {actual} bytes"),
        )
    }

    pub fn attachment_count_exceeded(count: usize, max_count: usize) -> Self {
        Self::new(
            "ATTACHMENT_COUNT_EXCEEDED",
            format!("attachment count exceeded: {count} exceeds max {max_count}"),
        )
    }

    pub fn unsupported_attachment_source(_source: &str) -> Self {
        Self::new(
            "UNSUPPORTED_ATTACHMENT_SOURCE",
            "attachment source is not supported",
        )
    }

    pub fn invalid_mime(_mime: &str) -> Self {
        Self::new("INVALID_MIME", "attachment mime_type is invalid")
    }

    pub fn mime_mismatch(_declared: &str, _sniffed: &str) -> Self {
        Self::new(
            "MIME_MISMATCH",
            "attachment content does not match declared MIME",
        )
    }

    pub fn invalid_file_name(_name: &str) -> Self {
        Self::new("INVALID_FILENAME", "attachment filename is invalid")
    }

    pub fn url_source_blocked(reason: impl Into<String>) -> Self {
        let reason = reason.into();
        let safe = match reason.as_str() {
            "private IP blocked" => "private IP blocked",
            "connected peer does not match the pinned DNS decision" => {
                "connected peer does not match the pinned DNS decision"
            }
            "URL userinfo is forbidden" => "URL userinfo is forbidden",
            _ => "attachment URL source blocked by policy",
        };
        Self::new("URL_SOURCE_BLOCKED", safe)
    }

    pub fn url_redirect_blocked(_reason: impl Into<String>) -> Self {
        Self::new(
            "URL_REDIRECT_BLOCKED",
            "attachment URL redirect blocked by policy",
        )
    }

    pub fn url_fetch_timeout(_url: &str, timeout_ms: u64) -> Self {
        Self::new(
            "URL_FETCH_TIMEOUT",
            format!("attachment URL fetch timed out after {timeout_ms}ms"),
        )
    }

    pub fn url_fetch_budget_exceeded(limit: usize) -> Self {
        Self::new(
            "URL_FETCH_BUDGET_EXCEEDED",
            format!("attachment URL fetch exceeded max bytes limit {limit}"),
        )
    }

    pub fn retry_exhausted(_operation: &str, attempts: usize, _reason: &str) -> Self {
        Self::new(
            "ATTACHMENT_RETRY_EXHAUSTED",
            format!("attachment operation exhausted after {attempts} attempt(s)"),
        )
    }

    pub fn circuit_breaker_open(_operation: &str, retry_after_ms: u64) -> Self {
        Self::new(
            "ATTACHMENT_CIRCUIT_BREAKER_OPEN",
            format!("attachment operation is blocked by circuit breaker for {retry_after_ms}ms"),
        )
    }

    pub fn contract_violation(_message: impl Into<String>) -> Self {
        Self::new(
            "ATTACHMENT_PIPELINE_CONTRACT_VIOLATION",
            "attachment contract rejected",
        )
    }

    pub fn http_status(status: u16) -> Self {
        Self::new(
            "URL_FETCH_HTTP_ERROR",
            format!("attachment URL returned HTTP {status}"),
        )
    }

    pub fn code(&self) -> &'static str {
        self.code
    }
}

impl Display for AttachmentPipelineError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.code, self.message)
    }
}

impl Error for AttachmentPipelineError {}
