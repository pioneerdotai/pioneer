use super::GATEWAY_SETUP_FORM_WIDTH_PX;
use crate::{
    app::root::{GatewaySetupAction, GatewaySetupFormMode, PioneerDesktop},
    components::buttonts::{default_outline_button, default_primary_button},
};
use gpui_kit::component::{
    button::ButtonVariants,
    form::{field, v_form},
    input::{Input, InputState, OtpInput, OtpState},
    separator::Separator,
    theme::ActiveTheme,
    *,
};
use gpui_kit::{prelude::*, *};
use tracing::warn;

const ACTIVATION_CODE_CELL_SIZE_PX: f32 = 32.0;

#[derive(Clone)]
pub(crate) struct GatewaySetupDialogState {
    connecting: bool,
    setup_action: Option<GatewaySetupAction>,
    error: Option<String>,
    status: String,
}

impl GatewaySetupDialogState {
    pub(in crate::app) fn new(
        connecting: bool,
        setup_action: Option<GatewaySetupAction>,
        error: Option<String>,
        status: String,
    ) -> Self {
        Self {
            connecting,
            setup_action,
            error,
            status,
        }
    }

    pub(in crate::app) fn without_error(mut self) -> Self {
        self.error = None;
        self
    }

    pub(in crate::app) fn with_error(mut self, error: String) -> Self {
        self.error = Some(error);
        self
    }
}

pub(crate) struct GatewaySetupFormState {
    name_input_state: Entity<InputState>,
    address_input_state: Entity<InputState>,
    activation_input_state: Entity<OtpState>,
    operation: GatewaySetupDialogState,
    did_focus_primary_input: bool,
}

impl GatewaySetupFormState {
    pub(crate) fn new(
        window: &mut Window,
        cx: &mut Context<Self>,
        operation: GatewaySetupDialogState,
    ) -> Self {
        Self {
            name_input_state: cx.new(|cx| InputState::new(window, cx)),
            address_input_state: cx.new(|cx| InputState::new(window, cx)),
            activation_input_state: cx.new(|cx| OtpState::new(8, window, cx)),
            operation,
            did_focus_primary_input: false,
        }
    }

    pub(crate) fn set_operation_state(
        &mut self,
        operation: GatewaySetupDialogState,
        cx: &mut Context<Self>,
    ) {
        self.operation = operation;
        cx.notify();
    }

    pub(crate) fn clear_inputs(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.name_input_state
            .update(cx, |state, cx| state.set_value("", window, cx));
        self.address_input_state
            .update(cx, |state, cx| state.set_value("", window, cx));
        self.activation_input_state
            .update(cx, |state, cx| state.set_value("", window, cx));
    }

    pub(crate) fn set_inputs(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
        name: String,
        gateway_base_url: String,
    ) {
        self.name_input_state
            .update(cx, |state, cx| state.set_value(name.clone(), window, cx));
        self.address_input_state.update(cx, |state, cx| {
            state.set_value(gateway_base_url.clone(), window, cx)
        });
        self.activation_input_state
            .update(cx, |state, cx| state.set_value("", window, cx));
    }

    pub(super) fn focus_name_input_once(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.did_focus_primary_input {
            return;
        }

        self.name_input_state
            .update(cx, |state, cx| state.focus(window, cx));
        self.did_focus_primary_input = true;
    }

    pub(super) fn focus_activation_input_once(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.did_focus_primary_input {
            return;
        }

        self.activation_input_state
            .update(cx, |state, cx| state.focus(window, cx));
        self.did_focus_primary_input = true;
    }

    pub(super) fn is_connecting(&self) -> bool {
        self.operation.connecting
    }

    fn snapshot(&self) -> GatewaySetupFormSnapshot {
        GatewaySetupFormSnapshot {
            name_input_state: self.name_input_state.clone(),
            address_input_state: self.address_input_state.clone(),
            activation_input_state: self.activation_input_state.clone(),
            connecting: self.operation.connecting,
            setup_action: self.operation.setup_action,
            error: self.operation.error.clone(),
            status: self.operation.status.clone(),
        }
    }
}

#[derive(Clone)]
pub(super) struct GatewaySetupFormSnapshot {
    pub(super) name_input_state: Entity<InputState>,
    pub(super) address_input_state: Entity<InputState>,
    pub(super) activation_input_state: Entity<OtpState>,
    pub(super) connecting: bool,
    pub(super) setup_action: Option<GatewaySetupAction>,
    pub(super) error: Option<String>,
    pub(super) status: String,
}

impl PioneerDesktop {
    pub(crate) fn gateway_setup_dialog_state(&self) -> GatewaySetupDialogState {
        GatewaySetupDialogState::new(
            self.gateway.connecting,
            self.gateway.setup_action,
            self.gateway.error.clone(),
            self.gateway.status.clone(),
        )
    }

    pub(crate) fn gateway_setup_allows_local(&self) -> bool {
        if should_keep_local_gateway_setup_visible_during_operation(
            self.gateway.connecting,
            self.gateway.setup_action,
        ) {
            return true;
        }

        let Some(runtime) = self.gateway.runtime.as_ref() else {
            return true;
        };

        match runtime.local_gateway_provisioned() {
            Ok(provisioned) => !provisioned,
            Err(error) => {
                warn!(
                    error = %format!("{error:#}"),
                    "failed to determine whether local gateway is already provisioned"
                );
                false
            }
        }
    }
}

fn should_keep_local_gateway_setup_visible_during_operation(
    connecting: bool,
    setup_action: Option<GatewaySetupAction>,
) -> bool {
    connecting && setup_action == Some(GatewaySetupAction::StartLocal)
}

pub(crate) fn render_gateway_setup_form(
    form_state: Entity<GatewaySetupFormState>,
    mode: GatewaySetupFormMode,
    desktop_entity: Entity<PioneerDesktop>,
    window: &mut Window,
    cx: &mut App,
) -> AnyElement {
    pioneer_observability::record_qualification_diagnostic!(record_render(
        pioneer_observability::RenderRegion::GatewaySetup
    ));
    let snapshot = form_state.read(cx).snapshot();
    let status = render_gateway_setup_status(&snapshot, cx);

    let mut form = v_form();

    if let GatewaySetupFormMode::ReauthenticateGateway {
        name,
        gateway_base_url,
        ..
    } = &mode
    {
        form = form
            .child(
                field()
                    .label(t!("common.name").to_string())
                    .child(render_readonly_gateway_value(name.clone(), cx)),
            )
            .child(
                field()
                    .label(t!("common.address").to_string())
                    .child(render_readonly_gateway_value(gateway_base_url.clone(), cx)),
            );
    } else {
        form = form
            .child(
                field()
                    .label(t!("common.name").to_string())
                    .child(Input::new(&snapshot.name_input_state).min_w_0()),
            )
            .child(
                field()
                    .label(t!("common.address").to_string())
                    .child(Input::new(&snapshot.address_input_state).min_w_0()),
            );
    }

    if !matches!(mode, GatewaySetupFormMode::EditGateway { .. }) {
        form = form.child(
            field()
                .label(t!("common.activation_code").to_string())
                .child(
                    div()
                        .w_full()
                        .min_w_0()
                        .when(!snapshot.connecting, |this| {
                            this.on_key_down(window.listener_for(
                                &snapshot.activation_input_state,
                                handle_alphanumeric_otp_key_down,
                            ))
                        })
                        .child(
                            OtpInput::new(&snapshot.activation_input_state)
                                .groups(2)
                                .with_size(px(ACTIVATION_CODE_CELL_SIZE_PX))
                                .disabled(snapshot.connecting),
                        ),
                ),
        );
    }

    form = form.child(
        field()
            .label_indent(false)
            .child(render_gateway_setup_form_actions(
                snapshot,
                mode,
                desktop_entity,
                form_state,
            )),
    );

    if let Some(status) = status {
        form = form.child(field().label_indent(false).child(status));
    }

    h_flex()
        .w_full()
        .justify_center()
        .child(
            div()
                .w(px(GATEWAY_SETUP_FORM_WIDTH_PX))
                .min_w_0()
                .child(form),
        )
        .into_any_element()
}

fn handle_alphanumeric_otp_key_down(
    state: &mut OtpState,
    event: &KeyDownEvent,
    window: &mut Window,
    cx: &mut Context<OtpState>,
) {
    let keystroke = &event.keystroke;
    if keystroke.modifiers.secondary() && keystroke.key.eq_ignore_ascii_case("v") {
        if let Some(value) = cx.read_from_clipboard().and_then(|item| item.text())
            && let Ok(normalized) =
                pioneer_protocol::normalize_device_activation_code_input(value.trim())
            && !normalized.is_empty()
        {
            state.set_value(normalized, window, cx);
        }
        window.prevent_default();
        cx.stop_propagation();
        return;
    }
    if keystroke.modifiers.control
        || keystroke.modifiers.alt
        || keystroke.modifiers.platform
        || keystroke.modifiers.function
    {
        return;
    }
    let typed = keystroke
        .key_char
        .as_deref()
        .unwrap_or(keystroke.key.as_str());
    if typed.chars().count() != 1 || !typed.is_ascii() {
        return;
    }
    let mut candidate = state.value().to_string();
    candidate.push_str(typed);
    if let Ok(normalized) = pioneer_protocol::normalize_device_activation_code_input(&candidate) {
        state.set_value(normalized, window, cx);
    }
    window.prevent_default();
    cx.stop_propagation();
}

fn render_gateway_setup_status(
    snapshot: &GatewaySetupFormSnapshot,
    cx: &mut App,
) -> Option<AnyElement> {
    if let Some(error) = snapshot.error.as_ref() {
        Some(render_error_status(
            cx,
            t!("gateway.error.with_details", error = error).to_string(),
        ))
    } else if snapshot.connecting {
        Some(render_start_status(snapshot.status.clone()))
    } else {
        None
    }
}

fn render_gateway_setup_form_actions(
    snapshot: GatewaySetupFormSnapshot,
    mode: GatewaySetupFormMode,
    desktop_entity: Entity<PioneerDesktop>,
    form_state: Entity<GatewaySetupFormState>,
) -> AnyElement {
    let primary_action = if matches!(mode, GatewaySetupFormMode::EditGateway { .. }) {
        GatewaySetupAction::SaveGateway
    } else {
        GatewaySetupAction::ConnectRemote
    };
    let primary_label = match &mode {
        GatewaySetupFormMode::EditGateway { .. } => t!("buttons.save").to_string(),
        GatewaySetupFormMode::ReauthenticateGateway { .. } => {
            t!("gateway.action.reauthenticate").to_string()
        }
        _ => t!("gateway.action.connect_remote").to_string(),
    };
    let name_input_state = snapshot.name_input_state.clone();
    let address_input_state = snapshot.address_input_state.clone();
    let activation_input_state = snapshot.activation_input_state.clone();

    let mut actions = v_flex().w_full().min_w_0().pt_4().gap_3().child(
        default_primary_button(mode.remote_button_id())
            .label(primary_label)
            .loading(crate::qualification_diagnostics::observed_loading!(
                pioneer_observability::AnimationSourceId::GatewaySetupRemoteButton,
                snapshot.connecting && snapshot.setup_action == Some(primary_action),
            ))
            .disabled(snapshot.connecting)
            .on_click({
                let desktop_entity = desktop_entity.clone();
                let form_state = form_state.clone();
                let mode = mode.clone();
                let name_input_state = name_input_state.clone();
                let address_input_state = address_input_state.clone();
                let activation_input_state = activation_input_state.clone();
                move |_, window, cx| {
                    let name = name_input_state.read(cx).value().to_string();
                    let gateway_base_url = address_input_state.read(cx).value().to_string();
                    let activation_code = activation_input_state.read(cx).value().to_string();
                    let _ = desktop_entity.update(cx, |view, cx| {
                        if let GatewaySetupFormMode::EditGateway { endpoint_id } = &mode {
                            view.save_gateway_from_edit_dialog(
                                window,
                                cx,
                                endpoint_id.clone(),
                                name.clone(),
                                gateway_base_url.clone(),
                                Some(form_state.clone()),
                            );
                        } else if let GatewaySetupFormMode::ReauthenticateGateway {
                            endpoint_id,
                            close_dialog_on_success,
                            ..
                        } = &mode
                        {
                            view.reauthenticate_remote_gateway_from_form(
                                window,
                                cx,
                                endpoint_id.clone(),
                                activation_code.clone(),
                                Some(form_state.clone()),
                                *close_dialog_on_success,
                            );
                        } else if let Some(source) = mode.operation_source() {
                            view.connect_remote_gateway_from_values(
                                window,
                                cx,
                                source,
                                name.clone(),
                                gateway_base_url.clone(),
                                activation_code.clone(),
                                Some(form_state.clone()),
                            );
                        }
                    });
                }
            }),
    );

    if let GatewaySetupFormMode::EditGateway { endpoint_id } = &mode {
        actions = actions.child(
            default_outline_button(
                mode.secondary_button_id()
                    .expect("edit mode has a delete button id"),
            )
            .label(t!("gateway.action.delete").to_string())
            .danger()
            .loading(crate::qualification_diagnostics::observed_loading!(
                pioneer_observability::AnimationSourceId::GatewaySetupDeleteButton,
                snapshot.connecting
                    && snapshot.setup_action == Some(GatewaySetupAction::DeleteGateway),
            ))
            .disabled(snapshot.connecting)
            .on_click({
                let desktop_entity = desktop_entity.clone();
                let form_state = form_state.clone();
                let endpoint_id = endpoint_id.clone();
                move |_, window, cx| {
                    let _ = desktop_entity.update(cx, |view, cx| {
                        view.confirm_delete_gateway_from_edit_dialog(
                            endpoint_id.clone(),
                            Some(form_state.clone()),
                            window,
                            cx,
                        );
                    });
                }
            }),
        );
    } else if mode.allow_local() {
        let source = mode
            .operation_source()
            .expect("local gateway action should only render for setup modes");
        actions = actions
            .child(Separator::horizontal().label(t!("common.or").to_string()))
            .child(
                default_outline_button(
                    mode.secondary_button_id()
                        .expect("local setup action has a button id"),
                )
                .label(t!("gateway.action.start_local").to_string())
                .loading(crate::qualification_diagnostics::observed_loading!(
                    pioneer_observability::AnimationSourceId::GatewaySetupLocalButton,
                    snapshot.connecting
                        && snapshot.setup_action == Some(GatewaySetupAction::StartLocal),
                ))
                .disabled(snapshot.connecting)
                .on_click({
                    let desktop_entity = desktop_entity.clone();
                    let form_state = form_state.clone();
                    move |_, window, cx| {
                        let _ = desktop_entity.update(cx, |view, cx| {
                            view.start_local_gateway_from_form(
                                window,
                                cx,
                                source,
                                Some(form_state.clone()),
                            );
                        });
                    }
                }),
            );
    }

    actions.into_any_element()
}

fn render_readonly_gateway_value(value: String, cx: &mut App) -> AnyElement {
    div()
        .w_full()
        .min_w_0()
        .h_9()
        .px_3()
        .flex()
        .items_center()
        .rounded_md()
        .border_1()
        .border_color(cx.theme().border)
        .bg(cx.theme().muted.opacity(0.35))
        .text_sm()
        .overflow_hidden()
        .child(value)
        .into_any_element()
}

fn render_start_status(status: String) -> AnyElement {
    div()
        .w_full()
        .min_w_0()
        .overflow_x_hidden()
        .pt_4()
        .whitespace_normal()
        .text_sm()
        .opacity(0.6)
        .text_center()
        .child(status)
        .into_any_element()
}

fn render_error_status(cx: &mut App, message: String) -> AnyElement {
    div()
        .w_full()
        .min_w_0()
        .overflow_x_hidden()
        .pt_4()
        .whitespace_normal()
        .text_sm()
        .text_color(cx.theme().red)
        .text_center()
        .child(message)
        .into_any_element()
}
