mod http;
pub(crate) mod local_operations;
mod network;
mod protocol;
mod restricted;
mod server;
mod voice_ingress;
mod ws;

#[cfg(test)]
pub(crate) use http::gateway_router;
pub(crate) use http::{GatewayHttpState, ReadinessDegradation};
pub(crate) use restricted::{
    AUTH_DEVICE_ACTIVATE, AUTH_REFRESH, INVITE_ACCEPT, INVITE_PREVIEW, RestrictedExchangeExecutor,
};

pub use server::spawn_server;

#[cfg(test)]
pub(crate) use server::{
    ConnectionAccessLease, ConnectionHandlers, IngressMode, NormalConnectionTestHooks,
    process_with_access_renewals, run_normal_connection, run_normal_connection_inner,
};
