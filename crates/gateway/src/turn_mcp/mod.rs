//! Gateway-owned MCP resolution and execution contracts for a Pioneer turn.

pub(crate) mod invoker;
pub(crate) mod persistence;
pub(crate) mod projection;
pub(crate) mod result;

pub(crate) use projection::{
    DEFAULT_MCP_TURN_TOOL_TIMEOUT_MS, MCP_TURN_PROJECTION_VERSION, McpProjectionLimits,
    McpResolutionDiagnostic, McpSelectionReason, ResolvedMcpTurnProjection, ResolvedMcpTurnTool,
};
