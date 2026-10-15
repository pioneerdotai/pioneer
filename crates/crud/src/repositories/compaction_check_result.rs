//! Small diagnostics and bounded retry policy for the background check only.
//! Runner-owned provider retries must never be restarted by this scheduler.
use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct HistoryCheckDiagnostic {
    pub stage: String,
    pub code: String,
    pub explanation: String,
    pub observed_ms: u64,
    pub attempt: u64,
    pub estimated_input_tokens: Option<u64>,
    pub padded_input_tokens: Option<u64>,
    pub context_tokens: Option<u64>,
    pub input_limit: Option<u64>,
    pub output_reserve: Option<u64>,
    pub operation: Option<String>,
    pub checkpoint: Option<String>,
    pub legacy_reason_unknown: bool,
}
impl HistoryCheckDiagnostic {
    pub fn new(stage: &str, code: &str, explanation: &str) -> Self {
        Self {
            stage: stage.into(),
            code: code.into(),
            explanation: explanation.into(),
            ..Self::default()
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HistoryCheckOutcome {
    Fits,
    Compacted,
    Preparing,
    WaitingCatalog,
    WaitingExecutor,
    WaitingSettings,
    Retryable,
    Failed,
    Cancelled,
}
impl HistoryCheckOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Fits => "fits",
            Self::Compacted => "compacted",
            Self::Preparing => "preparing",
            Self::WaitingCatalog => "waiting_catalog",
            Self::WaitingExecutor => "waiting_executor",
            Self::WaitingSettings => "waiting_settings",
            Self::Retryable => "retry_wait",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
    pub fn schedule(self, failures: i64, now: i64) -> (&'static str, i64, i64) {
        match self {
            Self::Retryable if failures < 3 => (
                "pending",
                failures + 1,
                now.saturating_add([60_000, 300_000, 900_000][failures.max(0) as usize]),
            ),
            Self::Retryable => ("finished", failures + 1, 0),
            Self::Preparing
            | Self::WaitingCatalog
            | Self::WaitingExecutor
            | Self::WaitingSettings => ("pending", failures, now.saturating_add(60_000)),
            _ => ("finished", failures, 0),
        }
    }
}
