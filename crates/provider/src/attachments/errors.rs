use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachmentPipelineError {
    code: &'static str,
    message: String,
}

impl AttachmentPipelineError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn attachment_too_large(size: usize, max_size: usize, label: &str) -> Self {
        Self::new(
            "ATTACHMENT_TOO_LARGE",
            format!("attachment `{label}` is too large: {size} bytes exceeds max {max_size} bytes"),
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

    pub fn attachment_count_exceeded(count: usize, max_count: usize) -> Self {
        Self::new(
            "ATTACHMENT_COUNT_EXCEEDED",
            format!("attachment count exceeded: {count} exceeds max {max_count}"),
        )
    }

    pub fn unsupported_attachment_source(source: &str) -> Self {
        Self::new(
            "UNSUPPORTED_ATTACHMENT_SOURCE",
            format!("attachment source `{source}` is not supported"),
        )
    }

    pub fn invalid_mime(mime: &str) -> Self {
        Self::new(
            "INVALID_MIME",
            format!("attachment mime_type must be a valid MIME value: `{mime}`"),
        )
    }

    pub fn mime_mismatch(declared: &str, sniffed: &str) -> Self {
        Self::new(
            "MIME_MISMATCH",
            format!(
                "attachment mime mismatch: declared `{declared}` but content looks like `{sniffed}`"
            ),
        )
    }

    pub fn invalid_file_name(name: &str) -> Self {
        Self::new(
            "INVALID_FILENAME",
            format!("attachment filename `{name}` is invalid after normalization"),
        )
    }

    pub fn url_source_blocked(reason: impl Into<String>) -> Self {
        Self::new("URL_SOURCE_BLOCKED", reason)
    }

    pub fn url_redirect_blocked(reason: impl Into<String>) -> Self {
        Self::new("URL_REDIRECT_BLOCKED", reason)
    }

    pub fn url_fetch_timeout(url: &str, timeout_ms: u64) -> Self {
        Self::new(
            "URL_FETCH_TIMEOUT",
            format!("attachment URL fetch timed out for `{url}` after {timeout_ms}ms"),
        )
    }

    pub fn url_fetch_budget_exceeded(limit: usize) -> Self {
        Self::new(
            "URL_FETCH_BUDGET_EXCEEDED",
            format!("attachment URL fetch exceeded max bytes limit {limit}"),
        )
    }

    pub fn retry_exhausted(operation: &str, attempts: usize, reason: &str) -> Self {
        Self::new(
            "ATTACHMENT_RETRY_EXHAUSTED",
            format!(
                "attachment operation `{operation}` exhausted after {attempts} attempt(s): {reason}"
            ),
        )
    }

    pub fn circuit_breaker_open(operation: &str, retry_after_ms: u64) -> Self {
        Self::new(
            "ATTACHMENT_CIRCUIT_BREAKER_OPEN",
            format!(
                "attachment operation `{operation}` is blocked by circuit breaker for {retry_after_ms}ms"
            ),
        )
    }

    pub fn contract_violation(message: impl Into<String>) -> Self {
        Self::new("ATTACHMENT_PIPELINE_CONTRACT_VIOLATION", message)
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
