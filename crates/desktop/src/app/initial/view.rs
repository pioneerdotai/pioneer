use crate::app::{
    gateway_setup::render_gateway_setup_form,
    root::{GatewaySetupFormMode, PioneerDesktop},
};
use gpui::{prelude::*, *};
use gpui_component::{theme::ActiveTheme, *};

impl PioneerDesktop {
    pub(crate) fn render_initial_setup(&self, cx: &mut Context<Self>) -> AnyElement {
        let allow_local = self.gateway_setup_allows_local();
        let desktop_entity = cx.entity().clone();
        let form_state = self.gateway_setup_form_state.clone();

        v_flex()
            .size_full()
            .bg(cx.theme().background)
            .justify_center()
            .items_center()
            .child(
                v_flex()
                    .w_96()
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
                    .child(render_gateway_setup_form(
                        form_state,
                        GatewaySetupFormMode::Initial { allow_local },
                        desktop_entity,
                        cx,
                    )),
            )
            .into_any_element()
    }
}
