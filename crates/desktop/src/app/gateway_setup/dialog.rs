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

    pub(crate) fn open_edit_gateway_dialog(
        &mut self,
        endpoint_id: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(runtime) = self.gateway.runtime.as_ref() else {
            return;
        };
        let Some(endpoint) = runtime.endpoint(endpoint_id.as_str()) else {
            return;
        };

        let token = runtime
            .gateway_auth_token_for_endpoint(&endpoint)
            .ok()
            .flatten()
            .unwrap_or_default();
        let desktop_entity = cx.entity().clone();
        let initial_operation = self.gateway_setup_dialog_state().without_error();
        let form_state =
            cx.new(|cx| GatewaySetupFormState::new(window, cx, initial_operation.clone()));
        form_state.update(cx, |state, cx| {
            state.set_inputs(
                window,
                cx,
                endpoint.name.clone(),
                endpoint.address.clone(),
                token.clone(),
            );
        });

        let endpoint_id = endpoint.id.clone();
        let endpoint_name = endpoint.name.clone();

        window.open_dialog(cx, move |dialog, window, cx| {
            form_state.update(cx, |state, cx| state.focus_name_input_once(window, cx));
            let is_connecting = form_state.read(cx).is_connecting();

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
                        .child(t!("gateway.edit.title", name = endpoint_name.as_str()).to_string()),
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
                                .child(t!("gateway.edit.description").to_string()),
                        )
                        .child(render_gateway_setup_form(
                            form_state.clone(),
                            GatewaySetupFormMode::EditGateway {
                                endpoint_id: endpoint_id.clone(),
                            },
                            desktop_entity.clone(),
                            cx,
                        )),
                )
        });
    }

    pub(in crate::app) fn confirm_delete_gateway_from_edit_dialog(
        &mut self,
        endpoint_id: String,
        form_state: Option<Entity<GatewaySetupFormState>>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let gateway_name = self
            .gateway
            .runtime
            .as_ref()
            .and_then(|runtime| runtime.endpoint(endpoint_id.as_str()))
            .map(|endpoint| endpoint.name)
            .unwrap_or_else(|| endpoint_id.clone());
        let title = t!("gateway.delete.confirm_title", name = gateway_name.as_str()).to_string();
        let description = t!("gateway.delete.confirm_description").to_string();
        let answer = window.prompt(
            PromptLevel::Warning,
            title.as_str(),
            Some(description.as_str()),
            &[
                PromptButton::new(t!("gateway.action.delete").to_string()),
                PromptButton::cancel(t!("buttons.cancel").to_string()),
            ],
            cx,
        );

        cx.spawn_in(
            window,
            move |this: WeakEntity<Self>, cx: &mut AsyncWindowContext| {
                let mut cx = cx.clone();
                let endpoint_id = endpoint_id.clone();
                let form_state = form_state.clone();

                async move {
                    if answer.await != Ok(0) {
                        return;
                    }

                    let _ = this.update_in(&mut cx, |view, window, cx| {
                        view.delete_gateway_from_edit_dialog(
                            window,
                            cx,
                            endpoint_id.clone(),
                            form_state.clone(),
                        );
                    });
                }
            },
        )
        .detach();
    }
}
