mod activation;
mod connectivity;
mod control;
mod http;
mod registry;
mod runtime;
mod secrets;
pub(crate) use secrets::DesktopSecrets;
mod identity_binding;
pub(crate) use identity_binding::IdentityAuthorizationBinding;
mod session_binding;
pub(crate) use session_binding::GatewaySessionBinding;
mod timings;
mod ws;

pub(crate) use control::GatewayInstallWarning;
pub(crate) use http::DesktopGatewayHttpClient;
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

#[derive(Clone)]
pub struct ClientRuntime {
    core: std::sync::Arc<pioneer_client::core::ClientCore>,
}

impl ClientRuntime {
    pub(crate) fn from_core(core: std::sync::Arc<pioneer_client::core::ClientCore>) -> Self {
        Self { core }
    }

    pub fn ws_command_sender(&self) -> pioneer_client::transport::ws::GatewayWsCommandSender {
        self.client_core()
            .compatibility_runtime()
            .ws_command_sender()
    }

    pub fn client_core(&self) -> &std::sync::Arc<pioneer_client::core::ClientCore> {
        &self.core
    }

    pub fn drain_ws_events(&self) -> Vec<pioneer_client::transport::ws::GatewayWsEvent> {
        self.client_core()
            .drain_gateway_compatibility_events()
            .into_iter()
            .map(|event| event.into_event())
            .collect()
    }

    pub fn reduce_gateway_notification(
        &self,
        notification: pioneer_protocol::GatewayNotification,
        context: pioneer_client::runtime::ClientRuntimeNotificationContext<'_>,
    ) -> Option<pioneer_client::runtime::ClientRuntimeNotification> {
        crate::render_guard::assert_not_rendering("Client notification reduction");
        self.client_core()
            .compatibility_runtime()
            .reduce_gateway_notification(notification, context)
    }

    pub fn drive_post_event_batch<Sink>(
        &self,
        events_applied: bool,
        sink: &mut Sink,
    ) -> pioneer_client::runtime::ClientRuntimePostEventOutcome
    where
        Sink: pioneer_client::runtime::ClientRuntimePostEventSink,
    {
        crate::render_guard::assert_not_rendering("Client post-event reduction");
        self.client_core()
            .compatibility_runtime()
            .drive_post_event_batch(events_applied, sink)
    }
}

#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
mod tests;

mod settings_binding;
pub(crate) use settings_binding::GatewaySettingsBinding;
