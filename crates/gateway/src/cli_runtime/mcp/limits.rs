use pioneer_cli_mcp_bridge::MAX_FRAME_PAYLOAD_BYTES;
use std::fmt;
use std::time::Duration;

pub(crate) const RESPONSE_ENVELOPE_RESERVE_BYTES: usize = 1024;
pub(crate) const MAX_FACADE_LIST_RESULT_BYTES: usize =
    MAX_FRAME_PAYLOAD_BYTES - RESPONSE_ENVELOPE_RESERVE_BYTES;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CliMcpRuntimeLimits {
    max_tools: usize,
    max_total_schema_bytes: usize,
    max_concurrent_calls_per_turn: usize,
}

impl CliMcpRuntimeLimits {
    pub(crate) fn new(
        max_tools: usize,
        max_total_schema_bytes: usize,
        max_concurrent_calls_per_turn: usize,
    ) -> Result<Self, CliMcpLimitConfigurationError> {
        if max_tools == 0
            || max_total_schema_bytes == 0
            || max_total_schema_bytes > MAX_FACADE_LIST_RESULT_BYTES
            || max_concurrent_calls_per_turn == 0
        {
            return Err(CliMcpLimitConfigurationError::Runtime);
        }
        Ok(Self {
            max_tools,
            max_total_schema_bytes,
            max_concurrent_calls_per_turn,
        })
    }

    pub(crate) const fn max_tools(self) -> usize {
        self.max_tools
    }

    pub(crate) const fn max_total_schema_bytes(self) -> usize {
        self.max_total_schema_bytes
    }

    pub(crate) const fn max_concurrent_calls_per_turn(self) -> usize {
        self.max_concurrent_calls_per_turn
    }

    pub(crate) const fn facade_projection_limits(self) -> CliMcpFacadeProjectionLimits {
        CliMcpFacadeProjectionLimits::transport_bounded(self.max_tools)
    }

    pub(crate) fn facade_limits(self) -> CliMcpFacadeLimits {
        let mut limits = CliMcpFacadeLimits::default();
        limits.max_active_calls = self.max_concurrent_calls_per_turn();
        limits.max_queued_calls = limits
            .max_queued_calls
            .max(self.max_concurrent_calls_per_turn());
        limits.max_ledger_entries = limits.max_ledger_entries.max(
            limits
                .max_active_calls
                .saturating_add(limits.max_queued_calls),
        );
        limits
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CliMcpFacadeProjectionLimits {
    pub(crate) max_tools: usize,
    pub(crate) max_list_result_bytes: usize,
}

impl CliMcpFacadeProjectionLimits {
    pub(crate) const fn transport_bounded(max_tools: usize) -> Self {
        Self {
            max_tools,
            max_list_result_bytes: MAX_FACADE_LIST_RESULT_BYTES,
        }
    }
}

#[cfg(test)]
impl Default for CliMcpFacadeProjectionLimits {
    fn default() -> Self {
        Self {
            max_tools: 128,
            max_list_result_bytes: 1024 * 1024,
        }
    }
}

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
    Runtime,
    Frame,
    Arguments,
    Result,
    Admission,
    Duration,
}

impl fmt::Display for CliMcpLimitConfigurationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Runtime => formatter.write_str("invalid CLI MCP runtime limits"),
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

    #[test]
    fn configured_runtime_limits_drive_projection_and_call_admission() {
        let runtime = CliMcpRuntimeLimits::new(512, 3_145_728, 16)
            .expect("configured CLI MCP runtime limits");

        assert_eq!(runtime.max_tools(), 512);
        assert_eq!(runtime.max_total_schema_bytes(), 3_145_728);
        assert_eq!(runtime.max_concurrent_calls_per_turn(), 16);
        assert_eq!(runtime.facade_projection_limits().max_tools, 512);
        assert_eq!(
            runtime.facade_projection_limits().max_list_result_bytes,
            MAX_FACADE_LIST_RESULT_BYTES
        );

        let facade = runtime.facade_limits();
        assert_eq!(facade.max_active_calls, 16);
        assert!(facade.max_ledger_entries >= facade.max_active_calls + facade.max_queued_calls);
        facade.validate().expect("derived facade limits");
    }

    #[test]
    fn configured_runtime_limits_reject_unrepresentable_values_up_front() {
        assert_eq!(
            CliMcpRuntimeLimits::new(0, 3_145_728, 16),
            Err(CliMcpLimitConfigurationError::Runtime)
        );
        assert_eq!(
            CliMcpRuntimeLimits::new(512, MAX_FACADE_LIST_RESULT_BYTES + 1, 16),
            Err(CliMcpLimitConfigurationError::Runtime)
        );
        assert_eq!(
            CliMcpRuntimeLimits::new(512, 3_145_728, 0),
            Err(CliMcpLimitConfigurationError::Runtime)
        );
    }
}
