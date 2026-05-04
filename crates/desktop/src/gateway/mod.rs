mod connectivity;
mod control;
mod registry;
mod runtime;
mod secrets;
mod timings;
mod types;
mod ws;

pub(crate) use connectivity::normalize_address;
pub(crate) use control::GatewayInstallWarning;

pub use runtime::{ActiveGatewayState, GatewayRuntime, ensure_runtime_home_dir};
pub use types::{GatewayEndpoint, GatewayEndpointKind};
pub use ws::{GatewayWsClient, GatewayWsCommandSender, GatewayWsConnectSpec, GatewayWsEvent};

#[cfg(test)]
mod tests;
