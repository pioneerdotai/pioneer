//! Shared boundary for local CLI-backed agent runtimes.
//!
//! `pioneer-cli-agent-runtime` is for runtimes that drive a local CLI process,
//! such as Codex app-server and Claude CLI. It is intentionally
//! separate from `pioneer-provider`, which remains the API-provider transport
//! abstraction.
//!
//! ACP integrations should be added as a sibling backend, not as a subtype of
//! this CLI process/runtime boundary.

pub mod approval;
pub mod capabilities;
pub mod claude;
pub mod codex;
pub mod codex_attestation;
pub mod config;
pub mod driver;
pub mod event;
pub mod input;
pub mod mcp;
pub mod process;
pub mod registry;
pub mod reserved_args;
pub mod session;

pub const CLI_AGENT_RUNTIME_BOUNDARY: &str = "cli-agent-runtime";

#[cfg(test)]
mod tests {
    #[test]
    fn exposes_boundary_marker() {
        assert_eq!(crate::CLI_AGENT_RUNTIME_BOUNDARY, "cli-agent-runtime");
    }
}
