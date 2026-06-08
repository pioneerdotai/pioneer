mod command_sender;

pub(crate) use command_sender::DesktopGatewayWsCommandSenderExt;
pub use pioneer_client::transport::ws::{GatewayWsClient, GatewayWsCommandSender};

#[cfg(test)]
pub use pioneer_client::transport::ws::{GatewayWsConnectSpec, GatewayWsEvent};

#[cfg(test)]
mod tests;
