use super::view::{GatewaySetupFormState, render_gateway_setup_form};
use crate::app::root::{GatewaySetupFormMode, PioneerDesktop};
use gpui::{prelude::*, *};
use gpui_component::{StyledExt, WindowExt, *};

impl PioneerDesktop {
    pub(crate) fn open_add_gateway_dialog(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let allow_local = self.gateway_setup_allows_local();
        let desktop_entity = cx.entity().clone();
        let initial_operation = self.gateway_setup_dialog_state().without_error();
        let form_state =
            cx.new(|cx| GatewaySetupFormState::new(window, cx, initial_operation.clone()));

        window.open_dialog(cx, move |dialog, window, cx| {
            form_state.update(cx, |state, cx| state.focus_name_input_once(window, cx));
            let is_connecting = form_state.read(cx).is_connecting();
            let description = if allow_local {
                t!("gateway.add.description").to_string()
            } else {
                t!("gateway.add.description_remote_only").to_string()
            };

            dialog
                .w(px(384.))
                .gap_1()
                .rounded_2xl()
                .close_button(!is_connecting)
                .overlay_closable(!is_connecting)
                .keyboard(!is_connecting)
                .title(
                    div()
                        .text_base()
                        .font_semibold()
                        .child(t!("gateway.add.title").to_string()),
                )
                .child(
                    v_flex()
                        .w_full()
                        .gap_4()
                        .child(
                            div()
                                .text_sm()
                                .opacity(0.6)
                                .line_height(relative(1.35))
                                .child(description),
                        )
                        .child(render_gateway_setup_form(
                            form_state.clone(),
                            GatewaySetupFormMode::AddGateway { allow_local },
                            desktop_entity.clone(),
                            cx,
                        )),
                )
        });
    }
}
