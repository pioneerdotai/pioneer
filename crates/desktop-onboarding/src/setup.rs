use crate::{OnboardingConfig, binding::Binding, buttons::*};
use gpui_kit::component::button::ButtonVariants;
use gpui_kit::component::{
    form::{field, v_form},
    input::{Input, InputEvent, InputState, OtpEvent, OtpInput, OtpState},
    separator::Separator,
    *,
};
use gpui_kit::{prelude::*, *};
use pioneer_client::{
    core::ClientScope,
    gateway::{
        onboarding_runtime::OnboardingIntent, runtime::GatewaySetupAction, setup_controller::*,
    },
};
use std::sync::Arc;

pub(crate) struct GatewaySetupScreenView {
    config: OnboardingConfig,
    name: Entity<InputState>,
    address: Entity<InputState>,
    activation: Entity<OtpState>,
    value: GatewaySetupPublication,
    dialog_owner: Option<u64>,
    dialog_focus: Option<FocusHandle>,
    close_pending: bool,
    _dialog_focus_subscription: Option<Subscription>,
    invalidated: bool,
    environment_state: (bool, Option<String>),
    _inputs: Vec<Subscription>,
    _binding: Arc<Binding>,
    _delivery: Task<()>,
}
impl GatewaySetupScreenView {
    pub fn new(
        config: OnboardingConfig,
        dialog: bool,
        window: &mut Window,
        cx: &mut App,
    ) -> Entity<Self> {
        cx.new(|cx: &mut Context<Self>| {
            let value = config
                .client
                .snapshot(&ClientScope::GatewaySetup)
                .and_then(|p| p.typed::<GatewaySetupPublication>())
                .map(|p| p.payload().as_ref().clone())
                .unwrap_or_default();
            let name = cx.new(|cx| InputState::new(window, cx).default_value(value.name.clone()));
            let address =
                cx.new(|cx| InputState::new(window, cx).default_value(value.address.clone()));
            let activation = cx.new(|cx| OtpState::new(8, window, cx));
            let inputs = vec![
                cx.subscribe(&name, |view, input, event, cx| match event {
                    InputEvent::Change => {
                        let value = input.read(cx).value().to_string();
                        if value != view.value.name {
                            view.intent(GatewaySetupIntent::EditName { value });
                        }
                    }
                    InputEvent::PressEnter { .. } => view.intent(GatewaySetupIntent::SubmitRemote),
                    _ => {}
                }),
                cx.subscribe(&address, |view, input, event, cx| match event {
                    InputEvent::Change => {
                        let value = input.read(cx).value().to_string();
                        if value != view.value.address {
                            view.intent(GatewaySetupIntent::EditAddress { value });
                        }
                    }
                    InputEvent::PressEnter { .. } => view.intent(GatewaySetupIntent::SubmitRemote),
                    _ => {}
                }),
                cx.subscribe(&activation, |view, input, event, cx| {
                    if matches!(event, OtpEvent::Change | OtpEvent::Complete) {
                        view.activation_changed(input.read(cx).value().to_string());
                    }
                }),
            ];
            let binding = Binding::new(
                vec![ClientScope::GatewaySetup, ClientScope::GatewayDestinations],
                &config.bindings,
            );
            let mut changed = binding.changed.subscribe();
            let handle = window.window_handle();
            let delivery = cx.spawn(async move |view: WeakEntity<Self>, cx| {
                while changed.changed().await.is_ok() {
                    if handle
                        .update(cx, |_, window, cx| {
                            view.update(cx, |view, cx| view.sync(window, cx))
                        })
                        .is_err()
                    {
                        break;
                    }
                }
            });
            Self {
                config,
                name,
                address,
                activation,
                environment_state: (false, None),
                dialog_owner: dialog.then_some(value.owner_generation),
                dialog_focus: None,
                close_pending: false,
                _dialog_focus_subscription: None,
                invalidated: false,
                value,
                _inputs: inputs,
                _binding: binding,
                _delivery: delivery,
            }
        })
    }
    fn intent(&self, intent: GatewaySetupIntent) {
        if self.invalidated {
            return;
        }
        if self
            .config
            .client
            .snapshot(&ClientScope::GatewaySetup)
            .and_then(|p| p.typed::<GatewaySetupPublication>())
            .is_none_or(|p| p.payload().owner_generation != self.value.owner_generation)
        {
            return;
        }
        if self.dialog_owner.is_none()
            && !matches!(
                self.value.mode,
                GatewaySetupMode::Initial { .. }
                    | GatewaySetupMode::ReauthenticateGateway {
                        close_on_success: false,
                        ..
                    }
            )
        {
            return;
        }
        let intent = match intent {
            GatewaySetupIntent::EditName { value } => GatewaySetupIntent::EditNameForOwner {
                expected_owner: self.value.owner_generation,
                value,
            },
            GatewaySetupIntent::EditAddress { value } => GatewaySetupIntent::EditAddressForOwner {
                expected_owner: self.value.owner_generation,
                value,
            },
            GatewaySetupIntent::EditActivation { value } => {
                GatewaySetupIntent::EditActivationForOwner {
                    expected_owner: self.value.owner_generation,
                    value,
                }
            }
            GatewaySetupIntent::SubmitRemote => GatewaySetupIntent::SubmitForOwner {
                expected_owner: self.value.owner_generation,
                local: false,
            },
            GatewaySetupIntent::SubmitLocal => GatewaySetupIntent::SubmitForOwner {
                expected_owner: self.value.owner_generation,
                local: true,
            },
            other => other,
        };
        self.config
            .client
            .onboarding_intent(OnboardingIntent::Setup { intent });
    }
    fn activation_changed(&self, value: String) {
        self.intent(GatewaySetupIntent::EditActivation {
            value: AuthSecretString::new(value),
        });
    }
    fn sync(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(value) = self
            .config
            .client
            .snapshot(&ClientScope::GatewaySetup)
            .and_then(|p| p.typed::<GatewaySetupPublication>())
        else {
            return;
        };
        let value = value.payload();
        let environment_state=self.config.client.snapshot(&ClientScope::GatewayDestinations).and_then(|p|p.typed::<pioneer_client::gateway::onboarding_runtime::GatewayDestinationsPublication>()).map(|p|(p.payload().loading,if p.payload().installation_id.is_none(){p.payload().error.clone()}else{None})).unwrap_or_default();
        let environment_changed = self.environment_state != environment_state;
        self.environment_state = environment_state;
        if self
            .dialog_owner
            .is_some_and(|owner| owner != value.owner_generation)
        {
            if !self.invalidated {
                self.invalidated = true;
                self.value.pending = false;
                self.activation
                    .update(cx, |input, cx| input.set_value("", window, cx));
                cx.notify();
            }
            return;
        }
        if self.dialog_owner.is_none()
            && !matches!(
                value.mode,
                GatewaySetupMode::Initial { .. }
                    | GatewaySetupMode::ReauthenticateGateway {
                        close_on_success: false,
                        ..
                    }
            )
        {
            return;
        }
        if self.value == *value && !environment_changed {
            return;
        }
        let reset = self.value.input_reset_generation != value.input_reset_generation;
        let completed = self.value.pending
            && !value.pending
            && value.error.is_none()
            && value.completed_endpoint.is_some();
        self.value = value.as_ref().clone();
        if self.name.read(cx).value().as_str() != self.value.name {
            self.name.update(cx, |input, cx| {
                input.set_value(self.value.name.clone(), window, cx)
            });
        }
        if self.address.read(cx).value().as_str() != self.value.address {
            self.address.update(cx, |input, cx| {
                input.set_value(self.value.address.clone(), window, cx)
            });
        }
        if reset {
            self.activation
                .update(cx, |input, cx| input.set_value("", window, cx));
        }
        if completed
            && self.dialog_owner == Some(self.value.owner_generation)
            && !matches!(
                self.value.mode,
                GatewaySetupMode::Initial { .. }
                    | GatewaySetupMode::ReauthenticateGateway {
                        close_on_success: false,
                        ..
                    }
            )
        {
            self.close_pending = true;
            self.close_completed_dialog(window, cx);
        }
        cx.notify();
    }
    pub fn bind_dialog_focus(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(focus) = window.focused(cx) else {
            return;
        };
        self._dialog_focus_subscription =
            Some(cx.on_focus_in(&focus, window, |view, window, cx| {
                view.close_completed_dialog(window, cx);
            }));
        self.dialog_focus = Some(focus);
    }
    fn close_completed_dialog(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.close_pending
            && !self.invalidated
            && self
                .dialog_focus
                .as_ref()
                .is_some_and(|focus| focus.contains_focused(window, cx))
        {
            self.close_pending = false;
            window.close_dialog(cx);
        }
    }
    pub fn name_for_title(&self) -> String {
        self.value.name.clone()
    }
    pub fn owner_generation(&self) -> u64 {
        self.value.owner_generation
    }
    pub fn pending(&self) -> bool {
        self.value.pending
    }
    pub fn focus(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if matches!(
            self.value.mode,
            GatewaySetupMode::ReauthenticateGateway { .. }
        ) {
            self.activation
                .update(cx, |input, cx| input.focus(window, cx));
        } else {
            self.name.update(cx, |input, cx| input.focus(window, cx));
        }
    }
    fn otp_key(&mut self, event: &KeyDownEvent, window: &mut Window, cx: &mut Context<Self>) {
        if self.value.pending {
            return;
        }
        if event.keystroke.key == "enter" {
            self.intent(GatewaySetupIntent::SubmitRemote);
            window.prevent_default();
            cx.stop_propagation();
            return;
        }
        self.activation.update(cx, |input, cx| {
            handle_alphanumeric_otp_key_down(input, event, window, cx)
        });
        self.activation_changed(self.activation.read(cx).value().to_string());
    }
    fn confirm_delete(&self, window: &mut Window, cx: &mut Context<Self>) {
        let client = self.config.client.clone();
        let generation = self.value.owner_generation;
        let name = self.value.name.clone();
        window.open_dialog(cx, move |dialog, _, _| {
            let client = client.clone();
            dialog
                .title(t!("gateway.delete.confirm_title", name = name.as_str()).to_string())
                .child(t!("gateway.delete.confirm_description").to_string())
                .on_ok(move |_, _, _| {
                    let current = client
                        .snapshot(&ClientScope::GatewaySetup)
                        .and_then(|p| p.typed::<GatewaySetupPublication>())
                        .is_some_and(|p| p.payload().owner_generation == generation);
                    if current {
                        client.onboarding_intent(OnboardingIntent::Setup {
                            intent: GatewaySetupIntent::DeleteForOwner {
                                expected_owner: generation,
                            },
                        });
                    }
                    true
                })
        });
    }
}
impl Render for GatewaySetupScreenView {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let value = &self.value;
        let pending = value.pending || self.environment_state.0;
        let disabled = pending || self.invalidated;
        let mut form = v_form();
        if matches!(value.mode, GatewaySetupMode::ReauthenticateGateway { .. }) {
            form = form
                .child(
                    field()
                        .label(t!("common.name").to_string())
                        .child(render_readonly_gateway_value(value.name.clone(), cx)),
                )
                .child(
                    field()
                        .label(t!("common.address").to_string())
                        .child(render_readonly_gateway_value(value.address.clone(), cx)),
                );
        } else {
            form = form
                .child(
                    field()
                        .label(t!("common.name").to_string())
                        .child(Input::new(&self.name).min_w_0().disabled(disabled)),
                )
                .child(
                    field()
                        .label(t!("common.address").to_string())
                        .child(Input::new(&self.address).min_w_0().disabled(disabled)),
                );
        }
        if !matches!(value.mode, GatewaySetupMode::EditGateway { .. }) {
            form = form.child(
                field()
                    .label(t!("common.activation_code").to_string())
                    .child(
                        div()
                            .w_full()
                            .min_w_0()
                            .on_key_down(cx.listener(Self::otp_key))
                            .child(
                                OtpInput::new(&self.activation)
                                    .groups(2)
                                    .with_size(px(32.))
                                    .disabled(disabled),
                            ),
                    ),
            );
        }
        let primary = match value.mode {
            GatewaySetupMode::EditGateway { .. } => "save-gateway",
            GatewaySetupMode::AddGateway { .. } => "add-connect-remote-gateway",
            GatewaySetupMode::ReauthenticateGateway { .. } => "reauthenticate-remote-gateway",
            _ => "connect-remote-gateway",
        };
        let label = match value.mode {
            GatewaySetupMode::EditGateway { .. } => t!("buttons.save"),
            GatewaySetupMode::ReauthenticateGateway { .. } => t!("gateway.action.reauthenticate"),
            _ => t!("gateway.action.connect_remote"),
        }
        .to_string();
        let mut actions = v_flex().w_full().min_w_0().pt_4().gap_3().child(
            default_primary_button(primary)
                .label(label)
                .loading(
                    value.pending
                        && matches!(
                            value.action,
                            Some(
                                GatewaySetupAction::ConnectRemote | GatewaySetupAction::SaveGateway
                            )
                        ),
                )
                .disabled(disabled)
                .on_click(
                    cx.listener(|view, _, _, _| view.intent(GatewaySetupIntent::SubmitRemote)),
                ),
        );
        if matches!(value.mode, GatewaySetupMode::EditGateway { .. }) {
            actions = actions.child(
                default_outline_button("delete-gateway")
                    .label(t!("gateway.action.delete").to_string())
                    .danger()
                    .loading(
                        value.pending && value.action == Some(GatewaySetupAction::DeleteGateway),
                    )
                    .disabled(disabled)
                    .on_click(cx.listener(|view, _, window, cx| view.confirm_delete(window, cx))),
            );
        } else if value.mode.allow_local() {
            actions = actions
                .child(Separator::horizontal().label(t!("common.or").to_string()))
                .child(
                    default_outline_button(
                        if matches!(value.mode, GatewaySetupMode::AddGateway { .. }) {
                            "add-start-local-gateway"
                        } else {
                            "start-local-gateway"
                        },
                    )
                    .label(t!("gateway.action.start_local").to_string())
                    .loading(value.pending && value.action == Some(GatewaySetupAction::StartLocal))
                    .disabled(disabled)
                    .on_click(
                        cx.listener(|view, _, _, _| view.intent(GatewaySetupIntent::SubmitLocal)),
                    ),
                );
        }
        form = form.child(field().label_indent(false).child(actions));
        if let Some(error) = value
            .error
            .as_ref()
            .or(value.address_error.as_ref())
            .or(value.activation_error.as_ref())
            .or(self.environment_state.1.as_ref())
        {
            form = form.child(
                field().label_indent(false).child(render_error_status(
                    cx,
                    t!(
                        "gateway.error.with_details",
                        error = setup_error_message(error, &value.address)
                    )
                    .to_string(),
                )),
            );
        } else if pending {
            let status = if value.action == Some(GatewaySetupAction::StartLocal) {
                t!("gateway.status.starting_local")
            } else {
                t!("gateway.status.connecting_remote")
            }
            .to_string();
            form = form.child(
                field()
                    .label_indent(false)
                    .child(render_start_status(status)),
            );
        }
        let _ = window;
        h_flex()
            .w_full()
            .justify_center()
            .child(div().w(px(300.)).min_w_0().child(form))
    }
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
            && let Ok(normalized) = normalize_device_activation_code_input(value.trim())
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
    if let Ok(normalized) = normalize_device_activation_code_input(&candidate) {
        state.set_value(normalized, window, cx);
    }
    window.prevent_default();
    cx.stop_propagation();
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

fn setup_error_message(code: &str, address: &str) -> String {
    match code {
        "invalid_gateway_address" => {
            t!("errors.gateway.invalid_address", normalized = address).to_string()
        }
        "invalid_activation_code" | "gateway_activation_required" => {
            t!("gateway.session_terminal.authentication_required").to_string()
        }
        "gateway_identity_mismatch" => t!("gateway.session_terminal.gateway_mismatch").to_string(),
        "gateway_environment_load_failed" => t!("gateway.status.subsystem_not_ready").to_string(),
        "gateway_secure_storage_failed" | "gateway_registry_write_failed" => {
            t!("gateway.session_terminal.storage_failed").to_string()
        }
        _ => t!("gateway.status.unavailable").to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Arc, ClientScope, GatewaySetupIntent, GatewaySetupMode, GatewaySetupPublication,
        GatewaySetupScreenView, InputEvent, OnboardingConfig, OnboardingIntent,
    };
    use gpui_kit::{App, Task, TestAppContext};
    use pioneer_desktop_foundation::{
        ClientBindingRegistrar, ClientBindingRegistration, ClientPublicationSink, profile_photo::*,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Registrar(Arc<AtomicUsize>);
    impl ClientBindingRegistrar for Registrar {
        fn register(
            &self,
            _: ClientScope,
            _: std::sync::Weak<dyn ClientPublicationSink>,
        ) -> ClientBindingRegistration {
            self.0.fetch_add(1, Ordering::SeqCst);
            let count = self.0.clone();
            ClientBindingRegistration::new(move || {
                count.fetch_sub(1, Ordering::SeqCst);
            })
        }
    }
    struct Photos;
    impl ProfilePhotoPort for Photos {
        fn select(
            &self,
            _: &mut App,
        ) -> Task<Result<Option<ProfilePhotoSelection>, ProfilePhotoError>> {
            panic!("synthetic form cannot execute native photo effects")
        }
    }
    fn config(count: Arc<AtomicUsize>) -> OnboardingConfig {
        config_with_remotes(count, vec![])
    }
    fn config_with_remotes(
        count: Arc<AtomicUsize>,
        remotes: Vec<pioneer_client::gateway::types::GatewayEndpoint>,
    ) -> OnboardingConfig {
        use pioneer_client::gateway::{
            onboarding_effects::OnboardingEnvironment,
            registry::{GatewayRegistryConfig, default_registry},
            timings::*,
        };
        let client = Arc::new(pioneer_client::core::ClientCore::new());
        let mut registry = default_registry(&GatewayRegistryConfig { local: None });
        registry.installation_id = Some("synthetic".into());
        registry.remotes = remotes;
        client.install_onboarding_environment_for_test(OnboardingEnvironment {
            registry,binding_journals:vec![],discard_unbound_remote_candidates:false,default_remote_name:"Remote".into(),remote_connect_timeout_min:std::time::Duration::ZERO,
            installation:serde_json::from_value(serde_json::json!({"installation_id":"synthetic","display_name":"Synthetic","client_kind":"desktop","platform":null,"client_version":null})).unwrap(),
            timings:GatewayTimings::from_millis(10,10,10).unwrap(),ws_timings:GatewayWsTimings::from_millis(10,10,10,10,20,0).unwrap(),
            local_provisioned:false,local_install_required:false,local_update_required:false,
        });
        client.onboarding_intent(OnboardingIntent::Setup {
            intent: GatewaySetupIntent::Open {
                mode: GatewaySetupMode::Initial { allow_local: false },
            },
        });
        OnboardingConfig {
            client,
            bindings: Arc::new(Registrar(count)),
            photos: std::rc::Rc::new(Photos),
        }
    }
    struct SwitcherHost {
        owner: gpui_kit::Entity<crate::OnboardingView>,
    }
    impl gpui_kit::Render for SwitcherHost {
        fn render(
            &mut self,
            window: &mut gpui_kit::Window,
            cx: &mut gpui_kit::Context<Self>,
        ) -> impl gpui_kit::IntoElement {
            use gpui_kit::ParentElement;
            let dialogs = gpui_kit::component::Root::render_dialog_layer(window, cx);
            gpui_kit::div()
                .child(self.owner.read(cx).gateway_switcher_surface())
                .children(dialogs)
        }
    }
    #[gpui_kit::test]
    fn switcher_dismissal_requires_an_accepted_selection(cx: &mut TestAppContext) {
        use gpui_kit::AppContext;
        cx.update(gpui_kit::init);
        let endpoint = serde_json::from_value(serde_json::json!({"id":"remote-a","name":"Synthetic","kind":"remote","gateway_base_url":"https://gateway.invalid","server_gateway_id":null,"session_ref":"synthetic-reference","workspace_id":null,"service_name":null})).unwrap();
        let config = config_with_remotes(Arc::new(AtomicUsize::new(0)), vec![endpoint]);
        let client = config.client.clone();
        let (root, cx) = cx.add_window_view(|window, cx| {
            let owner = crate::OnboardingView::new(config, window, cx);
            let host = cx.new(|_| SwitcherHost { owner });
            gpui_kit::component::Root::new(host, window, cx)
        });
        let host = root.read_with(cx, |root, _| {
            root.view().clone().downcast::<SwitcherHost>().unwrap()
        });
        let owner = host.read_with(cx, |host, _| host.owner.clone());
        let select = |id: &str, cx: &mut gpui_kit::VisualTestContext| {
            cx.update(|window, cx| {
                owner.update(cx, |owner, cx| {
                    owner.activate_gateway(id.into(), String::new(), window, cx)
                })
            })
        };
        assert!(!select("missing", cx));
        assert!(select("remote-a", cx));
        let before = client
            .snapshot(&ClientScope::GatewayDestinations)
            .unwrap()
            .snapshot();
        assert!(!select("remote-a", cx));
        assert!(Arc::ptr_eq(
            &before,
            &client
                .snapshot(&ClientScope::GatewayDestinations)
                .unwrap()
                .snapshot()
        ));
        client.shutdown();
    }

    #[gpui_kit::test]
    fn completed_setup_does_not_dismiss_a_different_top_dialog(cx: &mut TestAppContext) {
        use gpui_kit::component::WindowExt;
        use gpui_kit::{AppContext, ParentElement};
        cx.update(gpui_kit::init);
        let config = config(Arc::new(AtomicUsize::new(0)));
        let form_config = config.clone();
        let (_, cx) = cx.add_window_view(|window, cx| {
            let owner = crate::OnboardingView::new(config, window, cx);
            let host = cx.new(|_| SwitcherHost { owner });
            gpui_kit::component::Root::new(host, window, cx)
        });
        cx.update(|window, _| window.activate_window());
        let form = cx.update(|window, cx| {
            let form = GatewaySetupScreenView::new(form_config, true, window, cx);
            let content = form.clone();
            window.open_dialog(cx, move |dialog, _, _| dialog.child(content.clone()));
            form.update(cx, |view, cx| view.bind_dialog_focus(window, cx));
            form
        });
        cx.update(|window, cx| window.draw(cx).clear(cx));
        cx.run_until_parked();
        let top = cx.update(|window, cx| {
            window.open_dialog(cx, |dialog, _, _| dialog.child("Another dialog"));
            window.focused(cx).unwrap()
        });
        cx.update(|window, cx| window.draw(cx).clear(cx));
        cx.run_until_parked();
        cx.update(|window, cx| {
            form.update(cx, |view, cx| {
                view.close_pending = true;
                view.close_completed_dialog(window, cx);
            });
            assert!(top.is_focused(window));
            assert!(window.has_active_dialog(cx));
            window.close_dialog(cx);
        });
        cx.update(|window, cx| window.draw(cx).clear(cx));
        cx.run_until_parked();
        cx.update(|window, cx| {
            assert!(!form.read(cx).close_pending);
            assert!(!window.has_active_dialog(cx));
        });
    }

    #[gpui_kit::test]
    fn retained_switcher_reopens_and_dismisses_without_new_bindings(cx: &mut TestAppContext) {
        use gpui_kit::AppContext;
        cx.update(gpui_kit::init);
        let count = Arc::new(AtomicUsize::new(0));
        let endpoint = serde_json::from_value(serde_json::json!({
            "id":"remote-a", "name":"Synthetic Gateway", "kind":"remote",
            "gateway_base_url":"https://gateway.invalid", "server_gateway_id":null,
            "session_ref":null, "workspace_id":null, "service_name":null
        }))
        .unwrap();
        let config = config_with_remotes(count.clone(), vec![endpoint]);
        let client = config.client.clone();
        let (root, cx) = cx.add_window_view(|window, cx| {
            let owner = crate::OnboardingView::new(config, window, cx);
            let host = cx.new(|_| SwitcherHost { owner });
            gpui_kit::component::Root::new(host, window, cx)
        });
        let host = root.read_with(cx, |root, _| {
            root.view().clone().downcast::<SwitcherHost>().unwrap()
        });
        let owner_id = host.read_with(cx, |host, _| host.owner.entity_id());
        cx.update(|window, cx| window.draw(cx).clear(cx));
        let subscriptions = count.load(Ordering::SeqCst);
        for _ in 0..2 {
            let trigger = cx.debug_bounds("gateway-switcher-trigger").unwrap();
            cx.simulate_click(trigger.center(), Default::default());
            cx.update(|window, cx| window.draw(cx).clear(cx));
            assert!(cx.debug_bounds("gateway-switcher-content").is_some());
            cx.simulate_keystrokes("escape");
            cx.update(|window, cx| window.draw(cx).clear(cx));
            assert!(cx.debug_bounds("gateway-switcher-content").is_none());
            assert_eq!(count.load(Ordering::SeqCst), subscriptions);
            assert_eq!(
                host.read_with(cx, |host, _| host.owner.entity_id()),
                owner_id
            );
        }
        client.shutdown();
    }

    #[gpui_kit::test]
    fn retained_setup_preserves_selection_avoids_echo_and_submits_once(cx: &mut TestAppContext) {
        cx.update(gpui_kit::init);
        let count = Arc::new(AtomicUsize::new(0));
        let config = config(count.clone());
        let client = config.client.clone();
        let (root, cx) = cx.add_window_view(|window, cx| {
            let form = GatewaySetupScreenView::new(config.clone(), false, window, cx);
            gpui_kit::component::Root::new(form, window, cx)
        });
        let form = root.read_with(cx, |root, _| {
            root.view()
                .clone()
                .downcast::<GatewaySetupScreenView>()
                .unwrap()
        });
        let address = form.read_with(cx, |form, _| form.address.clone());
        cx.update(|window, cx| address.update(cx, |input, cx| input.focus(window, cx)));
        cx.simulate_input("https://gateway.invalid");
        cx.run_until_parked();
        cx.update(|window, cx| form.update(cx, |form, cx| form.sync(window, cx)));
        cx.update(|_, cx| address.update(cx, |input, cx| input.set_selected_range(1..5, cx)));
        let revision = client
            .snapshot(&ClientScope::GatewaySetup)
            .unwrap()
            .revisions()
            .scoped();
        cx.update(|window, cx| form.update(cx, |form, cx| form.sync(window, cx)));
        cx.run_until_parked();
        assert_eq!(
            address.read_with(cx, |input, _| input.selected_range()),
            1..5
        );
        assert_eq!(
            client
                .snapshot(&ClientScope::GatewaySetup)
                .unwrap()
                .revisions()
                .scoped(),
            revision
        );
        assert_eq!(
            address.entity_id(),
            form.read_with(cx, |form, _| form.address.entity_id())
        );
        cx.update(|_, cx| form.update(cx, |form, _| form.activation_changed("K7M4P9Q2".into())));
        cx.update(|_, cx| {
            address.update(cx, |_, cx| {
                cx.emit(InputEvent::PressEnter {
                    secondary: false,
                    shift: false,
                })
            })
        });
        cx.run_until_parked();
        let submitted = client.snapshot(&ClientScope::GatewaySetup).unwrap();
        assert!(
            submitted
                .typed::<GatewaySetupPublication>()
                .unwrap()
                .payload()
                .pending
        );
        cx.update(|_, cx| form.update(cx, |form, _| form.intent(GatewaySetupIntent::SubmitRemote)));
        assert_eq!(
            client
                .snapshot(&ClientScope::GatewaySetup)
                .unwrap()
                .revisions(),
            submitted.revisions()
        );
        assert_eq!(count.load(Ordering::SeqCst), 2);
        client.shutdown();
    }
    #[gpui_kit::test]
    fn replaced_dialog_releases_pending_state_and_drops_its_bindings(cx: &mut TestAppContext) {
        cx.update(gpui_kit::init);
        let count = Arc::new(AtomicUsize::new(0));
        let config = config(count.clone());
        let (_, cx) = cx.add_window_view(|window, cx| {
            let form = GatewaySetupScreenView::new(config.clone(), false, window, cx);
            gpui_kit::component::Root::new(form, window, cx)
        });
        config.client.onboarding_intent(OnboardingIntent::Setup {
            intent: GatewaySetupIntent::Open {
                mode: GatewaySetupMode::AddGateway { allow_local: false },
            },
        });
        let dialog =
            cx.update(|window, cx| GatewaySetupScreenView::new(config.clone(), true, window, cx));
        assert_eq!(count.load(Ordering::SeqCst), 4);
        config.client.onboarding_intent(OnboardingIntent::Setup {
            intent: GatewaySetupIntent::Cancel,
        });
        cx.update(|window, cx| dialog.update(cx, |form, cx| form.sync(window, cx)));
        assert!(dialog.read_with(cx, |form, _| form.invalidated));
        assert!(!dialog.read_with(cx, |form, _| form.pending()));
        let weak = dialog.downgrade();
        drop(dialog);
        cx.update(|_, _| {});
        cx.run_until_parked();
        assert!(weak.upgrade().is_none());
        assert_eq!(count.load(Ordering::SeqCst), 2);
        config.client.shutdown();
    }
}
