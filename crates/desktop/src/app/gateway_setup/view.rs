use crate::{
    app::root::{GatewaySetupAction, GatewaySetupFormMode, PioneerDesktop},
    components::buttonts::{default_outline_button, default_primary_button},
};
use gpui::{prelude::*, *};
use gpui_component::{
    divider::Divider,
    form::{field, v_form},
    input::{Input, InputState},
    theme::ActiveTheme,
    *,
};
use tracing::warn;

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
    token_input_state: Entity<InputState>,
    operation: GatewaySetupDialogState,
    did_focus_name_input: bool,
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
            token_input_state: cx
                .new(|cx| InputState::new(window, cx).multi_line(true).auto_grow(3, 7)),
            operation,
            did_focus_name_input: false,
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
        self.token_input_state
            .update(cx, |state, cx| state.set_value("", window, cx));
    }

    pub(super) fn focus_name_input_once(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.did_focus_name_input {
            return;
        }

        self.name_input_state
            .update(cx, |state, cx| state.focus(window, cx));
        self.did_focus_name_input = true;
    }

    pub(super) fn is_connecting(&self) -> bool {
        self.operation.connecting
    }

    fn snapshot(&self) -> GatewaySetupFormSnapshot {
        GatewaySetupFormSnapshot {
            name_input_state: self.name_input_state.clone(),
            address_input_state: self.address_input_state.clone(),
            token_input_state: self.token_input_state.clone(),
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
    pub(super) token_input_state: Entity<InputState>,
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
    cx: &mut App,
) -> AnyElement {
    let snapshot = form_state.read(cx).snapshot();
    let status = render_gateway_setup_status(&snapshot, cx);

    let mut form = v_form()
        .child(
            field()
                .label(t!("common.name").to_string())
                .child(Input::new(&snapshot.name_input_state).min_w_0()),
        )
        .child(
            field()
                .label(t!("common.address").to_string())
                .child(Input::new(&snapshot.address_input_state).min_w_0()),
        )
        .child(
            field()
                .label(t!("common.token").to_string())
                .child(Input::new(&snapshot.token_input_state).min_w_0()),
        )
        .child(
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

    form.into_any_element()
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
    let source = mode.operation_source();
    let name_input_state = snapshot.name_input_state.clone();
    let address_input_state = snapshot.address_input_state.clone();
    let token_input_state = snapshot.token_input_state.clone();

    let mut actions = v_flex().w_full().min_w_0().pt_4().gap_3().child(
        default_primary_button(mode.remote_button_id())
            .label(t!("gateway.action.connect_remote").to_string())
            .loading(
                snapshot.connecting
                    && snapshot.setup_action == Some(GatewaySetupAction::ConnectRemote),
            )
            .disabled(snapshot.connecting)
            .on_click({
                let desktop_entity = desktop_entity.clone();
                let form_state = form_state.clone();
                let name_input_state = name_input_state.clone();
                let address_input_state = address_input_state.clone();
                let token_input_state = token_input_state.clone();
                move |_, window, cx| {
                    let name = name_input_state.read(cx).value().to_string();
                    let address = address_input_state.read(cx).value().to_string();
                    let token = token_input_state.read(cx).value().to_string();
                    let _ = desktop_entity.update(cx, |view, cx| {
                        view.connect_remote_gateway_from_values(
                            window,
                            cx,
                            source,
                            name.clone(),
                            address.clone(),
                            token.clone(),
                            Some(form_state.clone()),
                        );
                    });
                }
            }),
    );

    if mode.allow_local() {
        actions = actions
            .child(Divider::horizontal().label(t!("common.or").to_string()))
            .child(
                default_outline_button(mode.local_button_id())
                    .label(t!("gateway.action.start_local").to_string())
                    .loading(
                        snapshot.connecting
                            && snapshot.setup_action == Some(GatewaySetupAction::StartLocal),
                    )
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
