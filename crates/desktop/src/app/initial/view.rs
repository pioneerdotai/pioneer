use crate::app::{
    gateway_setup::{GATEWAY_SETUP_INITIAL_CARD_WIDTH_PX, render_gateway_setup_form},
    root::{GatewaySetupFormMode, PioneerDesktop},
};
use crate::gateway::GatewayRuntime;
use gpui_kit::component::{theme::ActiveTheme, *};
use gpui_kit::{prelude::*, *};

impl PioneerDesktop {
    pub(crate) fn render_initial_setup(
        &self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let reauthentication = self
            .gateway
            .runtime
            .as_ref()
            .and_then(GatewayRuntime::active_gateway)
            .filter(|endpoint| {
                endpoint.kind == pioneer_client::gateway::types::GatewayEndpointKind::Remote
                    && endpoint.session_ref.is_none()
                    && endpoint.server_gateway_id.is_none()
            })
            .cloned();
        let (title, description, mode) = if let Some(endpoint) = reauthentication {
            (
                t!(
                    "gateway.reauthenticate.title",
                    name = endpoint.name.as_str()
                )
                .to_string(),
                t!("gateway.reauthenticate.description").to_string(),
                GatewaySetupFormMode::ReauthenticateGateway {
                    endpoint_id: endpoint.id,
                    name: endpoint.name,
                    gateway_base_url: endpoint.gateway_base_url.to_string(),
                    close_dialog_on_success: false,
                },
            )
        } else {
            let allow_local = self.gateway_setup_allows_local();
            (
                t!("gateway.initial.title").to_string(),
                t!("gateway.initial.description").to_string(),
                GatewaySetupFormMode::Initial { allow_local },
            )
        };
        let desktop_entity = cx.entity().clone();
        let form_state = self.gateway_setup_form_state.clone();

        v_flex()
            .size_full()
            .bg(cx.theme().background)
            .justify_center()
            .items_center()
            .child(
                v_flex()
                    .w(px(GATEWAY_SETUP_INITIAL_CARD_WIDTH_PX))
                    .p_4()
                    .pb_6()
                    .gap_5()
                    .border_1()
                    .border_color(cx.theme().border)
                    .rounded_2xl()
                    .bg(cx.theme().background)
                    .child(
                        v_flex()
                            .w_full()
                            .gap_2()
                            .child(
                                div()
                                    .w_full()
                                    .text_center()
                                    .text_xl()
                                    .font_bold()
                                    .child(title),
                            )
                            .child(
                                div()
                                    .w_full()
                                    .text_sm()
                                    .text_center()
                                    .opacity(0.6)
                                    .child(description),
                            ),
                    )
                    .child(render_gateway_setup_form(
                        form_state,
                        mode,
                        desktop_entity,
                        window,
                        cx,
                    )),
            )
            .into_any_element()
    }
}
