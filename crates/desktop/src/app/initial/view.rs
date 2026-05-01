use crate::{
    app::root::{GatewaySetupAction, PioneerDesktop},
    components::buttonts::{default_outline_button, default_primary_button},
};
use gpui::{prelude::*, *};
use gpui_component::{divider::Divider, input::Input, label::Label, theme::ActiveTheme, *};

impl PioneerDesktop {
    pub(crate) fn render_initial_setup(&self, cx: &mut Context<Self>) -> AnyElement {
        v_flex()
            .size_full()
            .bg(cx.theme().background)
            .justify_center()
            .items_center()
            .child(
                v_flex()
                    .w_96()
                    .p_4()
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
                                    .child(t!("gateway.initial.title").to_string()),
                            )
                            .child(
                                div()
                                    .w_full()
                                    .text_sm()
                                    .text_center()
                                    .opacity(0.6)
                                    .child(t!("gateway.initial.description").to_string()),
                            ),
                    )
                    .child(self.render_initial_form(cx)),
            )
            .into_any_element()
    }

    fn render_initial_form(&self, cx: &mut Context<Self>) -> AnyElement {
        v_flex()
            .w_full()
            .gap_6()
            .child(
                v_flex()
                    .w_full()
                    .gap_3()
                    .child(
                        v_flex()
                            .w_full()
                            .gap_2()
                            .child(Label::new(t!("common.name").to_string()).text_sm())
                            .child(
                                Input::new(&self.gateway_name_input_state)
                                    .disabled(self.gateway.connecting),
                            ),
                    )
                    .child(
                        v_flex()
                            .w_full()
                            .gap_2()
                            .child(Label::new(t!("common.address").to_string()).text_sm())
                            .child(
                                Input::new(&self.gateway_address_input_state)
                                    .disabled(self.gateway.connecting),
                            ),
                    )
                    .child(
                        v_flex()
                            .w_full()
                            .gap_2()
                            .child(Label::new(t!("common.token").to_string()).text_sm())
                            .child(
                                Input::new(&self.gateway_token_input_state)
                                    .disabled(self.gateway.connecting),
                            ),
                    ),
            )
            .child(self.render_initial_form_actions(cx))
            .into_any_element()
    }

    fn render_initial_form_actions(&self, cx: &mut Context<Self>) -> AnyElement {
        let mut actions = v_flex()
            .w_full()
            .gap_3()
            .child(
                default_primary_button("connect-remote-gateway")
                    .label(t!("gateway.action.connect_remote").to_string())
                    .loading(
                        self.gateway.connecting
                            && self.gateway.setup_action == Some(GatewaySetupAction::ConnectRemote),
                    )
                    .disabled(self.gateway.connecting)
                    .on_click(cx.listener(|view, _, window, cx| {
                        view.connect_remote_gateway(window, cx);
                    })),
            )
            .child(Divider::horizontal().label(t!("common.or").to_string()))
            .child(
                default_outline_button("start-local-gateway")
                    .label(t!("gateway.action.start_local").to_string())
                    .loading(
                        self.gateway.connecting
                            && self.gateway.setup_action == Some(GatewaySetupAction::StartLocal),
                    )
                    .disabled(self.gateway.connecting)
                    .on_click(cx.listener(|view, _, window, cx| {
                        view.start_local_gateway(window, cx);
                    })),
            );

        if let Some(error) = self.gateway.error.as_ref() {
            actions = actions.child(self.render_error_status(
                cx,
                t!("gateway.error.with_details", error = error).to_string(),
            ));
        } else if self.gateway.connecting {
            actions = actions.child(self.render_start_status());
        }

        actions.into_any_element()
    }

    fn render_start_status(&self) -> AnyElement {
        div()
            .w_full()
            .pt_1()
            .whitespace_normal()
            .text_sm()
            .opacity(0.6)
            .text_center()
            .child(self.gateway.status.clone())
            .into_any_element()
    }

    fn render_error_status(&self, cx: &mut Context<Self>, str: String) -> AnyElement {
        div()
            .w_full()
            .pt_1()
            .whitespace_normal()
            .text_sm()
            .text_color(cx.theme().red)
            .text_center()
            .child(str)
            .into_any_element()
    }
}
