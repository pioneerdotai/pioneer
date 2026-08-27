mod activation;
mod connectivity;
mod control;
mod http;
mod registry;
mod runtime;
mod secrets;
mod timings;
mod ws;

pub(crate) use control::GatewayInstallWarning;
pub(crate) use http::DesktopGatewayHttpClient;

pub use pioneer_client::runtime::ClientRuntime;
pub(crate) use runtime::observe_startup_stage;
pub(crate) use runtime::{DesktopInvitationCommitError, DesktopInvitationRegistryRecovery};
pub(crate) use runtime::{DesktopSessionConnectionOutcome, DesktopSessionPreparation};
pub use runtime::{GatewayRuntime, ensure_runtime_home_dir};
pub(crate) use ws::DesktopGatewayWsCommandSenderExt;
pub use ws::GatewayWsCommandSender;

pub(crate) fn is_supported_session_principal_kind(kind: &pioneer_protocol::PrincipalKind) -> bool {
    matches!(
        kind,
        pioneer_protocol::PrincipalKind::Superuser | pioneer_protocol::PrincipalKind::User
    )
}

#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
mod tests;
