use super::root::PioneerDesktop;
use gpui_kit::prelude::*;
use gpui_kit::{App, Context, Window};
use pioneer_client::{core::ClientCore, gateway::onboarding_runtime::OnboardingIntent};
use pioneer_desktop_onboarding::{OnboardingConfig, OnboardingEvent};
use std::{rc::Rc, sync::Arc};
pub(super) fn config(client: Arc<ClientCore>, cx: &App) -> OnboardingConfig {
    OnboardingConfig {
        client,
        bindings: cx
            .global::<crate::client_runtime::DesktopRuntimeCoordinator>()
            .registrar(),
        photos: Rc::new(crate::profile_photo::DesktopProfilePhotoPort),
    }
}
impl PioneerDesktop {
    pub(super) fn onboarding_event(&mut self, event: &OnboardingEvent, cx: &mut Context<Self>) {
        match event {
            OnboardingEvent::NavigationChanged {
                invitation_active,
                setup_required,
            } => {
                self.onboarding_route = if *invitation_active {
                    crate::desktop_navigation::WindowRoute::InvitationJoin
                } else if *setup_required {
                    crate::desktop_navigation::WindowRoute::GatewaySetup
                } else {
                    crate::desktop_navigation::WindowRoute::Main
                };
            }
            OnboardingEvent::Authenticated { .. } => {
                self.replay_deferred_gateway_ws_events(cx);
                self.schedule_gateway_session_refresh(cx);
            }
        }
        self.publish_frame_changes(cx);
        cx.notify();
    }
    pub(crate) fn handle_invitation_url(
        &mut self,
        uri: String,
        _window: &mut Window,
        _cx: &mut Context<Self>,
    ) {
        let Ok(presentation) = pioneer_protocol::InvitationPresentation::parse(&uri) else {
            return;
        };
        if presentation.app_url_scheme()
            != pioneer_protocol::PioneerAppUrlScheme::for_current_build()
        {
            return;
        }
        self.gateway
            .client_runtime
            .client_core()
            .onboarding_intent(OnboardingIntent::Invitation {
                intent: pioneer_client::gateway::invitation_controller::InvitationIntent::Open {
                    uri: pioneer_protocol::AuthSecretString::new(uri),
                },
            });
    }
    pub(in crate::app) fn gateway_busy(&self) -> bool {
        self.gateway.client_runtime.client_core().onboarding_busy()
    }
    pub(in crate::app) fn gateway_bootstrap_complete(&self) -> bool {
        self.gateway
            .client_runtime
            .client_core()
            .gateway_registry()
            .is_some()
            && !self
                .gateway
                .client_runtime
                .client_core()
                .onboarding_loading()
    }
}
