mod http;
mod network;
mod protocol;
mod restricted;
mod server;
mod ws;

pub(crate) use http::ReadinessDegradation;
#[cfg(test)]
pub(crate) use http::{GatewayHttpState, gateway_router};
pub(crate) use restricted::{
    AUTH_DEVICE_ACTIVATE, AUTH_REFRESH, INVITE_ACCEPT, INVITE_PREVIEW, RestrictedExchangeExecutor,
};

pub use server::spawn_server;

#[cfg(test)]
pub(crate) use server::{
    ConnectionAccessLease, ConnectionHandlers, IngressMode, process_with_access_renewals,
    run_normal_connection,
};
