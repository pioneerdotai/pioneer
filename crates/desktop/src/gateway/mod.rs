mod connectivity;
mod control;
mod registry;
mod runtime;
mod secrets;
mod timings;
mod ws;

pub(crate) use connectivity::normalize_address;
pub(crate) use control::GatewayInstallWarning;
pub(crate) use timings::GatewayWsTimings;

pub use runtime::{GatewayRuntime, ensure_runtime_home_dir};
pub(crate) use ws::DesktopGatewayWsCommandSenderExt;
pub use ws::{GatewayWsClient, GatewayWsCommandSender};

#[cfg(test)]
mod tests;
