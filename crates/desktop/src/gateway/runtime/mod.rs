//! Native installation discovery and runtime directory adapter.
mod compat;
pub(super) mod discovery;
use anyhow::{Context, Result};
#[cfg(test)]
pub(crate) use compat::is_same_gateway_version;
use pioneer_config::AppConfig;
use std::path::PathBuf;
use tracing::info;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum DesktopSessionConnectionOutcome {
    Connected {
        connection_id: u64,
        metadata: pioneer_client::gateway::session_lifecycle::GatewaySessionMetadata,
        access_expires_at_unix: u64,
    },
    Terminal(pioneer_client::gateway::session_refresh::GatewaySessionTerminal),
}
pub fn ensure_runtime_home_dir() -> Result<PathBuf> {
    let config = AppConfig::load().context(t!("errors.config.load_app").to_string())?;
    let runtime_home = config
        .ensure_runtime_home_dir()
        .context(t!("errors.runtime.ensure_home").to_string())?;

    info!(
        runtime_home = %runtime_home.display(),
        message = %t!("logs.runtime.home_ready")
    );

    Ok(runtime_home)
}
