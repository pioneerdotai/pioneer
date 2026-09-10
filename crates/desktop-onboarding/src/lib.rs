//! Window-retained setup, invitation, and Gateway switcher surfaces.
#[macro_use]
extern crate rust_i18n;
rust_i18n::i18n!("locales", fallback = "en");
mod assets;
mod binding;
mod buttons;
mod invitation;
mod profile_presentation;
mod setup;
mod switcher;
use binding::Binding;
use gpui_kit::component::{theme::ActiveTheme, *};
use gpui_kit::{prelude::*, *};
use pioneer_client::{
    core::{ClientCore, ClientScope},
    gateway::{
        onboarding_runtime::{GatewayDestinationsPublication, OnboardingIntent},
        setup_controller::{GatewaySetupIntent, GatewaySetupMode},
    },
};
use pioneer_desktop_foundation::ClientBindingRegistrar;
use std::sync::Arc;
#[derive(Clone)]
pub struct OnboardingConfig {
    pub client: Arc<ClientCore>,
    pub bindings: Arc<dyn ClientBindingRegistrar>,
    pub photos: std::rc::Rc<dyn OnboardingPhotoPort>,
}
#[derive(Clone, Debug)]
pub enum OnboardingEvent {
    Authenticated {
        endpoint_id: String,
    },
    NavigationChanged {
        invitation_active: bool,
        setup_required: bool,
    },
}
pub struct OnboardingView {
    config: OnboardingConfig,
    setup: Entity<setup::GatewaySetupScreenView>,
    invitation: Entity<invitation::InvitationJoinScreenView>,
    invitation_active: bool,
    reauthentication_name: Option<String>,
    switcher: Entity<switcher::GatewaySwitcherView>,
    authenticated: Option<(String, String)>,
    route: Option<(bool, bool)>,
    last_warning: u64,
    _binding: Arc<Binding>,
    _delivery: Task<()>,
}
struct GatewayInstallWarningNotification;
impl EventEmitter<OnboardingEvent> for OnboardingView {}
impl OnboardingView {
    pub fn new(config: OnboardingConfig, window: &mut Window, cx: &mut App) -> Entity<Self> {
        let view = cx.new(|cx| {
            let setup = setup::GatewaySetupScreenView::new(config.clone(), false, window, cx);
            let invitation = invitation::InvitationJoinScreenView::new(config.clone(), window, cx);
            let switcher = cx.new(|cx| switcher::GatewaySwitcherView::new(config.clone(), cx));
            let binding = Binding::new(
                vec![
                    ClientScope::OnboardingInvitation,
                    ClientScope::GatewayDestinations,
                    ClientScope::Session,
                    ClientScope::Administration { workspace_id: None },
                ],
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
                config: config.clone(),
                setup,
                invitation,
                invitation_active: false,
                reauthentication_name: reauthentication_name(&config.client),
                switcher,
                authenticated: None,
                route: None,
                last_warning: 0,
                _binding: binding,
                _delivery: delivery,
            }
        });
        let parent = view.downgrade();
        view.read(cx)
            .switcher
            .clone()
            .update(cx, |switcher, _| switcher.set_owner(parent));
        view
    }
    fn sync(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let name = reauthentication_name(&self.config.client);
        if self.reauthentication_name != name {
            self.reauthentication_name = name;
            cx.notify();
        }
        for warning in self.destinations().warnings {
            if warning.id <= self.last_warning {
                continue;
            }
            self.last_warning = warning.id;
            let message = warning.message;
            window.push_notification(
                gpui_kit::component::notification::Notification::new()
                    .with_type(gpui_kit::component::notification::NotificationType::Warning)
                    .id1::<GatewayInstallWarningNotification>((
                        "gateway-install-warning",
                        warning.id,
                    ))
                    .content(move |_, _, _| {
                        v_flex()
                            .child(
                                div()
                                    .text_sm()
                                    .opacity(0.8)
                                    .line_height(relative(1.4))
                                    .child(message.clone()),
                            )
                            .into_any_element()
                    }),
                cx,
            );
        }
        let active = self
            .config
            .client
            .snapshot(&ClientScope::OnboardingInvitation)
            .and_then(|p| {
                p.typed::<pioneer_client::gateway::invitation_controller::InvitationPublication>()
            })
            .is_some_and(|p| p.payload().active && p.payload().completed_endpoint.is_none());
        if self.invitation_active != active {
            self.invitation_active = active;
            cx.notify();
        }
        let route = (active, self.config.client.onboarding_setup_required());
        if self.route != Some(route) && self.config.client.gateway_registry().is_some() {
            self.route = Some(route);
            cx.emit(OnboardingEvent::NavigationChanged {
                invitation_active: route.0,
                setup_required: route.1,
            });
        }
        let selected = (|| {
            let auth = self.config.client.current_auth()?;
            let endpoint = self.config.client.active_gateway_endpoint()?;
            if endpoint.server_gateway_id.as_ref() != Some(&auth.gateway.id) {
                return None;
            }
            let session = self.config.client.gateway_session();
            let connection = session.connections.get(&endpoint.id)?.connected.as_ref()?;
            if connection.metadata.session_id != auth.session.id
                || connection.metadata.gateway_id != auth.gateway.id
            {
                return None;
            }
            Some((endpoint.id, auth.session.id.to_string()))
        })();
        if self.authenticated != selected {
            self.authenticated = selected.clone();
            if let Some((endpoint_id, _)) = selected {
                cx.emit(OnboardingEvent::Authenticated { endpoint_id });
            }
            cx.notify();
        }
    }
    pub fn gateway_switcher_surface(&self) -> AnyView {
        self.switcher.clone().into()
    }
    pub fn setup_surface(&self) -> AnyView {
        self.setup.clone().into()
    }
    fn destinations(&self) -> GatewayDestinationsPublication {
        self.config
            .client
            .snapshot(&ClientScope::GatewayDestinations)
            .and_then(|p| p.typed::<GatewayDestinationsPublication>())
            .map(|p| p.payload().as_ref().clone())
            .unwrap_or_default()
    }
    pub fn open_add_gateway_dialog(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let allow_local = self
            .config
            .client
            .gateway_registry()
            .is_some_and(|r| r.local.is_some_and(|local| local.session_ref.is_none()));
        self.open_setup_dialog(GatewaySetupMode::AddGateway { allow_local }, window, cx);
    }
    pub fn open_edit_gateway_dialog(
        &mut self,
        id: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.open_setup_dialog(
            GatewaySetupMode::EditGateway { endpoint_id: id },
            window,
            cx,
        );
    }
    pub fn open_reauthenticate_gateway_dialog(
        &mut self,
        id: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        self.open_setup_dialog(
            GatewaySetupMode::ReauthenticateGateway {
                endpoint_id: id,
                close_on_success: true,
            },
            window,
            cx,
        )
    }
    fn open_setup_dialog(
        &mut self,
        mode: GatewaySetupMode,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        let transition = self
            .config
            .client
            .onboarding_intent(OnboardingIntent::Setup {
                intent: GatewaySetupIntent::Open { mode: mode.clone() },
            });
        if transition.outcome() != pioneer_client::core::ClientTransitionOutcome::Changed {
            return false;
        }
        let form = setup::GatewaySetupScreenView::new(self.config.clone(), true, window, cx);
        form.update(cx, |form, cx| form.focus(window, cx));
        let client = self.config.client.clone();
        let expected_owner = form.read(cx).owner_generation();
        let name = form.read(cx).name_for_title();
        let mounted_form = form.clone();
        window.open_dialog(cx, move |dialog, _, cx| {
            let pending = form.read(cx).pending();
            let title = match mode {
                GatewaySetupMode::EditGateway { .. } => {
                    t!("gateway.edit.title", name = name.as_str())
                }
                GatewaySetupMode::ReauthenticateGateway { .. } => {
                    t!("gateway.reauthenticate.title", name = name.as_str())
                }
                _ => t!("gateway.add.title"),
            }
            .to_string();
            let client = client.clone();
            dialog
                .w(px(350.))
                .gap_1()
                .rounded_2xl()
                .close_button(!pending)
                .overlay_closable(!pending)
                .keyboard(!pending)
                .title(div().text_base().child(title))
                .child(form.clone())
                .on_close(move |_, _, _| {
                    client.onboarding_intent(OnboardingIntent::Setup {
                        intent: GatewaySetupIntent::Close { expected_owner },
                    });
                })
        });
        mounted_form.update(cx, |form, cx| form.bind_dialog_focus(window, cx));
        true
    }
    fn activate_gateway(
        &mut self,
        id: String,
        _name: String,
        _window: &mut Window,
        _cx: &mut Context<Self>,
    ) -> bool {
        self.config
            .client
            .onboarding_intent(OnboardingIntent::SelectGateway { endpoint_id: id })
            .outcome()
            == pioneer_client::core::ClientTransitionOutcome::Changed
    }
}
impl Render for OnboardingView {
    fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
        if self.invitation_active {
            return self.invitation.clone().into_any_element();
        }
        let (title, description) = self
            .reauthentication_name
            .as_ref()
            .map(|name| {
                (
                    t!("gateway.reauthenticate.title", name = name.as_str()).to_string(),
                    t!("gateway.reauthenticate.description").to_string(),
                )
            })
            .unwrap_or_else(|| {
                (
                    t!("gateway.initial.title").to_string(),
                    t!("gateway.initial.description").to_string(),
                )
            });
        OnboardingSurface {
            title,
            description,
            content: self.setup.clone().into(),
        }
        .into_any_element()
    }
}

#[derive(IntoElement)]
struct OnboardingSurface {
    title: String,
    description: String,
    content: AnyView,
}
impl RenderOnce for OnboardingSurface {
    fn render(self, _: &mut Window, cx: &mut App) -> impl IntoElement {
        let Self {
            title,
            description,
            content,
        } = self;
        v_flex()
            .size_full()
            .bg(cx.theme().background)
            .justify_center()
            .items_center()
            .child(
                v_flex()
                    .w(px(334.))
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
                    .child(content),
            )
            .into_any_element()
    }
}

pub struct OnboardingPhotoSelection {
    pub preview: String,
    pub avatar: pioneer_client::settings::types::ProfileAvatarInput,
}
pub enum OnboardingPhotoError {
    Picker,
    InvalidAvatar,
}
pub trait OnboardingPhotoPort {
    fn select(
        &self,
        cx: &mut gpui_kit::App,
    ) -> gpui_kit::Task<Result<Option<OnboardingPhotoSelection>, OnboardingPhotoError>>;
}

fn reauthentication_name(client: &ClientCore) -> Option<String> {
    client
        .gateway_registry()?
        .active_gateway()
        .filter(|endpoint| {
            endpoint.kind == pioneer_client::gateway::types::GatewayEndpointKind::Remote
                && endpoint.session_ref.is_none()
        })
        .map(|endpoint| endpoint.name.clone())
}
