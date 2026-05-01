mod install;
mod launcher;
mod status;

pub(crate) use install::managed_gateway_install;
pub(crate) use launcher::{
    GatewayInstallWarning, request_local_superuser_token, start_gateway_service,
    update_gateway_service_from_desktop_binary,
};
pub(crate) use status::is_configured_service_active;
