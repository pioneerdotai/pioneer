mod command_sender;

pub(crate) use command_sender::DesktopGatewayWsCommandSenderExt;
pub use pioneer_client::transport::ws::GatewayWsCommandSender;

#[cfg(test)]
pub use pioneer_client::transport::ws::{GatewayWsClient, GatewayWsConnectSpec, GatewayWsEvent};

#[cfg(test)]
mod tests;
