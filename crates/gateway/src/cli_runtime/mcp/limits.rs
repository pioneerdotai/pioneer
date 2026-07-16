use pioneer_cli_mcp_bridge::MAX_FRAME_PAYLOAD_BYTES;
use std::fmt;
use std::time::Duration;

const RESPONSE_ENVELOPE_RESERVE_BYTES: usize = 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CliMcpFacadeLimits {
    pub(crate) max_frame_bytes: usize,
    pub(crate) max_arguments_bytes: usize,
    pub(crate) max_arguments_depth: usize,
    pub(crate) max_result_bytes: usize,
    pub(crate) max_result_images: usize,
    pub(crate) max_result_tokens: usize,
    pub(crate) max_active_calls: usize,
    pub(crate) max_queued_calls: usize,
    pub(crate) max_ledger_entries: usize,
    pub(crate) max_queue_wait: Duration,
    pub(crate) max_execution_duration: Duration,
    pub(crate) completed_entry_ttl: Duration,
    pub(crate) shutdown_drain_duration: Duration,
}

impl Default for CliMcpFacadeLimits {
    fn default() -> Self {
        Self {
            max_frame_bytes: MAX_FRAME_PAYLOAD_BYTES,
            max_arguments_bytes: 128 * 1024,
            max_arguments_depth: 32,
            max_result_bytes: 1024 * 1024,
            max_result_images: 8,
            max_result_tokens: 64 * 1024,
            max_active_calls: 8,
            max_queued_calls: 16,
            max_ledger_entries: 64,
            max_queue_wait: Duration::from_secs(30),
            max_execution_duration: Duration::from_secs(120),
            completed_entry_ttl: Duration::from_secs(300),
            shutdown_drain_duration: Duration::from_secs(5),
        }
    }
}

impl CliMcpFacadeLimits {
    pub(crate) fn validate(&self) -> Result<(), CliMcpLimitConfigurationError> {
        if self.max_frame_bytes == 0 || self.max_frame_bytes > MAX_FRAME_PAYLOAD_BYTES {
            return Err(CliMcpLimitConfigurationError::Frame);
        }
        if self.max_arguments_bytes == 0
            || self.max_arguments_bytes > self.max_frame_bytes
            || self.max_arguments_depth == 0
        {
            return Err(CliMcpLimitConfigurationError::Arguments);
        }
        if self.max_result_bytes == 0
            || self
                .max_result_bytes
                .checked_add(RESPONSE_ENVELOPE_RESERVE_BYTES)
                .is_none_or(|bytes| bytes > self.max_frame_bytes)
            || self.max_result_images == 0
            || self.max_result_tokens == 0
        {
            return Err(CliMcpLimitConfigurationError::Result);
        }
        if self.max_active_calls == 0
            || self.max_queued_calls == 0
            || self.max_ledger_entries < self.max_active_calls.saturating_add(self.max_queued_calls)
        {
            return Err(CliMcpLimitConfigurationError::Admission);
        }
        if self.max_queue_wait.is_zero()
            || self.max_execution_duration.is_zero()
            || self.completed_entry_ttl.is_zero()
            || self.shutdown_drain_duration.is_zero()
        {
            return Err(CliMcpLimitConfigurationError::Duration);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CliMcpLimitConfigurationError {
    Frame,
    Arguments,
    Result,
    Admission,
    Duration,
}

impl fmt::Display for CliMcpLimitConfigurationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Frame => formatter.write_str("invalid CLI MCP frame limit"),
            Self::Arguments => formatter.write_str("invalid CLI MCP argument limit"),
            Self::Result => formatter.write_str("invalid CLI MCP result limit"),
            Self::Admission => formatter.write_str("invalid CLI MCP admission limit"),
            Self::Duration => formatter.write_str("invalid CLI MCP duration limit"),
        }
    }
}

impl std::error::Error for CliMcpLimitConfigurationError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_mcp_limits_defaults_are_bounded_and_valid() {
        CliMcpFacadeLimits::default().validate().expect("defaults");
    }

    #[test]
    fn cli_mcp_limits_reject_frame_and_admission_overflow() {
        let mut limits = CliMcpFacadeLimits::default();
        limits.max_frame_bytes = MAX_FRAME_PAYLOAD_BYTES + 1;
        assert_eq!(limits.validate(), Err(CliMcpLimitConfigurationError::Frame));

        let mut limits = CliMcpFacadeLimits::default();
        limits.max_ledger_entries = 1;
        assert_eq!(
            limits.validate(),
            Err(CliMcpLimitConfigurationError::Admission)
        );
    }
}
