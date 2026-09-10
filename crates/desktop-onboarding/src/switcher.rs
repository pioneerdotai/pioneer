use crate::assets::PioneerIconName;
use crate::{OnboardingConfig, OnboardingView, binding::Binding};
use gpui_kit::component::{
    button::{Button, ButtonVariants},
    popover::{Popover, PopoverState},
    separator::Separator,
    spinner::Spinner,
    *,
};
use gpui_kit::{prelude::*, *};
use pioneer_client::{
    core::ClientScope,
    gateway::types::{GatewayEndpoint, GatewayEndpointKind},
    state::client_state::GatewayStatusLevel,
};
use std::sync::Arc;

pub(super) fn gateway_endpoint_subtitle(endpoint: &GatewayEndpoint) -> String {
    let address = endpoint.gateway_base_url.as_str();
    match endpoint.kind {
        GatewayEndpointKind::Local => {
            t!("gateway.endpoint.local_with_address", address = address).to_string()
        }
        GatewayEndpointKind::Remote => {
            t!("gateway.endpoint.remote_with_address", address = address).to_string()
        }
    }
}

impl OnboardingView {
    pub(crate) fn render_gateways_popover(
        &self,
        desktop_entity: Entity<Self>,
        publication: Option<
            &pioneer_client::gateway::session_controller::GatewaySessionPublication,
        >,
        show_spinner: bool,
        cx: &gpui_kit::App,
    ) -> AnyElement {
        let destinations = self.destinations();
        let connecting = destinations.loading || destinations.pending_endpoint.is_some();
        let scoped_status = publication
            .and_then(|p| p.status.as_ref())
            .filter(|_| !connecting);
        let gateway_status_color =
            match scoped_status.map_or(GatewayStatusLevel::Neutral, |s| s.status_level) {
                GatewayStatusLevel::Neutral => cx.theme().muted_foreground,
                GatewayStatusLevel::Connected => cx.theme().success,
                GatewayStatusLevel::Degraded => cx.theme().warning,
                GatewayStatusLevel::Failed => cx.theme().danger,
            };
        let gateway_endpoints = self
            .config
            .client
            .gateway_registry()
            .map(|registry| {
                let local_id = registry
                    .local
                    .as_ref()
                    .map_or("", |local| local.id.as_str());
                pioneer_client::gateway::runtime::selectable_gateway_endpoints(&registry, local_id)
            })
            .unwrap_or_default();
        let active_gateway_id = destinations.selected_endpoint;
        let active_gateway = active_gateway_id
            .as_deref()
            .and_then(|id| gateway_endpoints.iter().find(|endpoint| endpoint.id == id))
            .or_else(|| gateway_endpoints.first());

        let gateway_trigger_label = active_gateway
            .map(|endpoint| format!("{} ({})", endpoint.name, endpoint.gateway_base_url))
            .unwrap_or_else(|| t!("gateway.status.connecting").to_string());

        let gateway_hover_status = scoped_status
            .and_then(|status| match &status.status {
                pioneer_client::state::reducers::GatewayStatusTextUpdate::Set(status) => {
                    Some(gateway_status_message_text(status))
                }
                pioneer_client::state::reducers::GatewayStatusTextUpdate::KeepExisting => None,
            })
            .unwrap_or_else(|| t!("gateway.status.connecting").to_string());
        let gateway_hover_error = if scoped_status.is_some() {
            publication.and_then(|publication| publication.gateway_error.clone())
        } else {
            destinations.error.clone()
        };

        let gateway_selection_locked = connecting;

        let active_indicator_color = cx.theme().success;
        let inactive_indicator_color = cx.theme().yellow;
        let option_foreground = cx.theme().foreground;
        let option_muted_background = cx.theme().muted;

        let ghost_hover = if cx.theme().mode.is_dark() {
            cx.theme().secondary.lighten(0.2).opacity(0.8)
        } else {
            cx.theme().secondary.darken(0.1).opacity(0.8)
        };

        let ghost_active = if cx.theme().mode.is_dark() {
            cx.theme().secondary.lighten(0.3).opacity(0.8)
        } else {
            cx.theme().secondary.darken(0.2).opacity(0.8)
        };

        Popover::new("gateway-switcher-popover")
            .anchor(Anchor::TopLeft)
            .p_0()
            .trigger(
                Button::new("gateway-switcher-button")
                    .ghost()
                    .small()
                    .compact()
                    .child(
                        h_flex()
                            .items_center()
                            .gap_2()
                            .when(show_spinner, |this| {
                                this.child(
                                    Spinner::new().with_size(gpui_kit::component::Size::Small),
                                )
                            })
                            .when(!show_spinner, |this| {
                                this.child(
                                    div().size(px(8.)).rounded_full().bg(gateway_status_color),
                                )
                            })
                            .child(
                                div()
                                    .whitespace_normal()
                                    .text_sm()
                                    .text_center()
                                    .opacity(0.6)
                                    .child(gateway_trigger_label.clone()),
                            )
                            .id("gateway-switcher-hover-card")
                            .debug_selector(|| "gateway-switcher-trigger".into())
                            .tooltip({
                                let gateway_hover_status = gateway_hover_status.clone();
                                let gateway_hover_error = gateway_hover_error.clone();

                                move |window, tooltip_cx| {
                                    let gateway_hover_status = gateway_hover_status.clone();
                                    let gateway_hover_error = gateway_hover_error.clone();

                                    gpui_kit::component::tooltip::Tooltip::element(
                                        move |_, element_cx| {
                                            v_flex()
                                                .w(px(360.))
                                                .gap_1()
                                                .p_2()
                                                .child(
                                                    div()
                                                        .w_full()
                                                        .text_xs()
                                                        .line_height(relative(1.15))
                                                        .whitespace_normal()
                                                        .child(gateway_hover_status.clone()),
                                                )
                                                .when_some(
                                                    gateway_hover_error.clone(),
                                                    |this, error| {
                                                        this.child(
                                                            div()
                                                                .w_full()
                                                                .text_xs()
                                                                .line_height(relative(1.15))
                                                                .whitespace_normal()
                                                                .text_color(
                                                                    element_cx.theme().danger,
                                                                )
                                                                .child(error),
                                                        )
                                                    },
                                                )
                                        },
                                    )
                                    .build(window, tooltip_cx)
                                }
                            }),
                    ),
            )
            .content({
                let desktop_entity = desktop_entity.clone();
                let gateway_endpoints = gateway_endpoints.clone();
                let active_gateway_id = active_gateway_id.clone();
                let active_indicator_color = active_indicator_color;
                let inactive_indicator_color = inactive_indicator_color;
                let option_foreground = option_foreground;
                let option_muted_background = option_muted_background;
                let ghost_hover = ghost_hover;
                let ghost_active = ghost_active;

                move |_, _window, popover_cx| {
                    let popover_entity = popover_cx.entity();

                    v_flex()
                        .debug_selector(|| "gateway-switcher-content".into())
                        .w(px(320.))
                        .gap_2()
                        .when(gateway_endpoints.is_empty(), |this| {
                            this.child(
                                div()
                                    .w_full()
                                    .text_sm()
                                    .opacity(0.6)
                                    .child(t!("gateway.popover.no_available").to_string()),
                            )
                        })
                        .when(!gateway_endpoints.is_empty(), |this| {
                            this.child(
                                v_flex().w_full().p_2().pb_0().gap_1().children(
                                    gateway_endpoints.iter().enumerate().map(
                                        |(index, endpoint)| {
                                            Self::render_gateways_popover_option(
                                                index,
                                                endpoint,
                                                active_gateway_id.as_deref(),
                                                gateway_selection_locked,
                                                active_indicator_color,
                                                inactive_indicator_color,
                                                option_foreground,
                                                option_muted_background,
                                                ghost_hover,
                                                ghost_active,
                                                desktop_entity.clone(),
                                                popover_entity.clone(),
                                            )
                                        },
                                    ),
                                ),
                            )
                        })
                        .child(Separator::horizontal())
                        .child(h_flex().p_2().pt_0().justify_start().child(
                            Self::render_add_gateway_popover_action(
                                gateway_selection_locked,
                                desktop_entity.clone(),
                                popover_entity.clone(),
                            ),
                        ))
                }
            })
            .into_any_element()
    }

    fn render_add_gateway_popover_action(
        gateway_selection_locked: bool,
        desktop_entity: Entity<Self>,
        popover_entity: Entity<PopoverState>,
    ) -> AnyElement {
        Button::new("add-gateway")
            .ghost()
            .xsmall()
            .compact()
            .disabled(gateway_selection_locked)
            .child(div().opacity(0.6).child(IconName::Plus))
            .child(
                div()
                    .opacity(0.6)
                    .child(t!("gateway.action.add").to_string()),
            )
            .on_click({
                let desktop_entity = desktop_entity.clone();
                let popover_entity = popover_entity.clone();

                move |_, window, cx| {
                    let _ = popover_entity.update(cx, |state, cx| {
                        state.dismiss(window, cx);
                    });
                    let _ = desktop_entity.update(cx, |view, cx| {
                        view.open_add_gateway_dialog(window, cx);
                    });
                }
            })
            .into_any_element()
    }

    fn render_gateways_popover_option(
        _index: usize,
        endpoint: &GatewayEndpoint,
        active_gateway_id: Option<&str>,
        gateway_selection_locked: bool,
        active_indicator_color: Hsla,
        inactive_indicator_color: Hsla,
        option_foreground: Hsla,
        option_muted_background: Hsla,
        ghost_hover: Hsla,
        ghost_active: Hsla,
        desktop_entity: Entity<Self>,
        popover_entity: Entity<PopoverState>,
    ) -> AnyElement {
        let endpoint_id = endpoint.id.clone();
        let endpoint_name = endpoint.name.clone();
        let requires_reauthentication = endpoint.kind == GatewayEndpointKind::Remote
            && endpoint.session_ref.is_none()
            && endpoint.server_gateway_id.is_none();
        let subtitle = gateway_endpoint_subtitle(endpoint);
        let endpoint_id_for_click = endpoint_id.clone();
        let endpoint_name_for_click = endpoint_name.clone();
        let is_active = active_gateway_id == Some(endpoint_id.as_str());

        let select_button = div()
            .id(format!("gateway-option:{}", endpoint.id))
            .w_full()
            .min_w_0()
            .cursor_pointer()
            .rounded_lg()
            .p_2()
            .text_color(option_foreground)
            .when(is_active, |this| this.bg(option_muted_background))
            .hover(move |this| this.bg(ghost_hover))
            .active(move |this| this.bg(ghost_active))
            .when(gateway_selection_locked, |this| this.opacity(0.5))
            .when(endpoint.kind == GatewayEndpointKind::Remote, |this| {
                this.pr(px(36.))
            })
            .on_mouse_down(MouseButton::Left, |_, window, _| {
                window.prevent_default();
            })
            .on_click({
                let desktop_entity = desktop_entity.clone();
                let popover_entity = popover_entity.clone();

                move |_, window, cx| {
                    if gateway_selection_locked {
                        cx.stop_propagation();
                        return;
                    }
                    let started = desktop_entity.update(cx, |view, cx| {
                        if requires_reauthentication {
                            view.open_reauthenticate_gateway_dialog(
                                endpoint_id_for_click.clone(),
                                window,
                                cx,
                            )
                        } else {
                            view.activate_gateway(
                                endpoint_id_for_click.clone(),
                                endpoint_name_for_click.clone(),
                                window,
                                cx,
                            )
                        }
                    });

                    if started {
                        let _ = popover_entity.update(cx, |state, cx| {
                            state.dismiss(window, cx);
                        });
                    }
                }
            })
            .child(
                h_flex()
                    .w_full()
                    .items_center()
                    .gap_3()
                    .when(is_active, |this| {
                        this.child(div().size(px(8.)).rounded_full().bg(active_indicator_color))
                    })
                    .when(!is_active, |this| {
                        this.child(
                            div()
                                .size(px(8.))
                                .rounded_full()
                                .bg(inactive_indicator_color),
                        )
                    })
                    .child(
                        v_flex()
                            .w_full()
                            .min_w_0()
                            .gap_1()
                            .child(
                                div()
                                    .w_full()
                                    .min_w_0()
                                    .text_sm()
                                    .text_color(option_foreground)
                                    .line_height(relative(1.0))
                                    .font_semibold()
                                    .overflow_hidden()
                                    .whitespace_nowrap()
                                    .text_ellipsis()
                                    .child(endpoint_name),
                            )
                            .child(
                                div()
                                    .w_full()
                                    .min_w_0()
                                    .text_xs()
                                    .text_color(option_foreground)
                                    .line_height(relative(1.05))
                                    .opacity(0.6)
                                    .overflow_hidden()
                                    .whitespace_nowrap()
                                    .text_ellipsis()
                                    .child(subtitle),
                            ),
                    ),
            );

        div()
            .relative()
            .w_full()
            .min_w_0()
            .child(select_button)
            .when(endpoint.kind == GatewayEndpointKind::Remote, |this| {
                let endpoint_id_for_edit = endpoint_id.clone();
                this.child(
                    div()
                        .absolute()
                        .top_0()
                        .right_0()
                        .bottom_0()
                        .flex()
                        .items_center()
                        .pr_1()
                        .child(
                            Button::new(format!("gateway-option-edit:{}", endpoint.id))
                                .ghost()
                                .xsmall()
                                .compact()
                                .disabled(gateway_selection_locked)
                                .icon(PioneerIconName::Bolt)
                                .tooltip(t!("gateway.action.edit").to_string())
                                .on_click({
                                    let desktop_entity = desktop_entity.clone();
                                    let popover_entity = popover_entity.clone();

                                    move |_, window, cx| {
                                        cx.stop_propagation();
                                        let _ = popover_entity.update(cx, |state, cx| {
                                            state.dismiss(window, cx);
                                        });
                                        let _ = desktop_entity.update(cx, |view, cx| {
                                            view.open_edit_gateway_dialog(
                                                endpoint_id_for_edit.clone(),
                                                window,
                                                cx,
                                            );
                                        });
                                    }
                                }),
                        ),
                )
            })
            .into_any_element()
    }
}

#[derive(PartialEq, Eq)]
struct SwitcherProjection {
    endpoints: Vec<GatewayEndpoint>,
    selected: Option<String>,
    loading: bool,
    pending: Option<String>,
    error: Option<String>,
    status: Option<pioneer_client::state::reducers::GatewayStatusProjection>,
    session_error: Option<String>,
}
impl SwitcherProjection {
    fn read(client: &pioneer_client::core::ClientCore) -> Self {
        let destinations=client.snapshot(&ClientScope::GatewayDestinations).and_then(|p|p.typed::<pioneer_client::gateway::onboarding_runtime::GatewayDestinationsPublication>()).map(|p|p.payload().as_ref().clone()).unwrap_or_default();
        let session = client.gateway_session();
        Self {
            endpoints: destinations.endpoints,
            selected: destinations.selected_endpoint,
            loading: destinations.loading,
            pending: destinations.pending_endpoint,
            error: destinations.error,
            status: session.status.clone(),
            session_error: session.gateway_error.clone(),
        }
    }
}
pub(crate) struct GatewaySwitcherView {
    config: OnboardingConfig,
    owner: Option<WeakEntity<OnboardingView>>,
    projection: SwitcherProjection,
    _binding: Arc<Binding>,
    _delivery: Task<()>,
}
impl GatewaySwitcherView {
    pub fn new(config: OnboardingConfig, cx: &mut Context<Self>) -> Self {
        let binding = Binding::new(
            vec![ClientScope::GatewayDestinations, ClientScope::Session],
            &config.bindings,
        );
        let mut changed = binding.changed.subscribe();
        let delivery = cx.spawn(async move |view: WeakEntity<Self>, cx| {
            while changed.changed().await.is_ok() {
                if view
                    .update(cx, |view, cx| {
                        let projection = SwitcherProjection::read(&view.config.client);
                        if view.projection != projection {
                            view.projection = projection;
                            cx.notify();
                        }
                    })
                    .is_err()
                {
                    break;
                }
            }
        });
        let projection = SwitcherProjection::read(&config.client);
        Self {
            config,
            projection,
            owner: None,
            _binding: binding,
            _delivery: delivery,
        }
    }
    pub fn set_owner(&mut self, owner: WeakEntity<OnboardingView>) {
        self.owner = Some(owner);
    }
}
impl Render for GatewaySwitcherView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let Some(owner) = self.owner.as_ref().and_then(WeakEntity::upgrade) else {
            return div().into_any_element();
        };
        let registry = self.config.client.gateway_registry();
        if registry.as_ref().is_none_or(|registry| {
            pioneer_client::gateway::runtime::selectable_gateway_endpoints(
                registry,
                registry
                    .local
                    .as_ref()
                    .map_or("", |local| local.id.as_str()),
            )
            .is_empty()
        }) {
            return div().into_any_element();
        }
        let session = self.config.client.gateway_session();
        let destinations = owner.read(cx).destinations();
        let loading = destinations.loading
            || destinations.pending_endpoint.is_some()
            || session
                .status
                .as_ref()
                .is_some_and(|s| s.connection_state.is_transitioning());
        owner
            .read(cx)
            .render_gateways_popover(owner.clone(), Some(&session), loading, cx)
    }
}

use pioneer_client::state::reducers::GatewayStatusMessage;
fn gateway_status_message_text(status: &GatewayStatusMessage) -> String {
    match status {
        GatewayStatusMessage::Connecting => t!("gateway.status.connecting").to_string(),
        GatewayStatusMessage::ConnectingNamed { endpoint_name } => t!(
            "gateway.status.connecting_named",
            gateway_name = endpoint_name.as_str()
        )
        .to_string(),
        GatewayStatusMessage::StartingLocal => t!("gateway.status.starting_local").to_string(),
        GatewayStatusMessage::Reconnecting {
            endpoint_name,
            attempt,
            delay_ms,
        } => format!(
            "{} (attempt {attempt}, {} ms)",
            t!(
                "gateway.status.connecting_named",
                gateway_name = endpoint_name.as_str()
            ),
            delay_ms
        ),
        GatewayStatusMessage::Connected => t!("gateway.status.connected").to_string(),
        GatewayStatusMessage::ConnectedEndpoint {
            endpoint_name,
            gateway_base_url,
        } => format!(
            "{}: {} ({})",
            t!("gateway.status.connected"),
            endpoint_name.as_str(),
            gateway_base_url.as_str()
        ),
        GatewayStatusMessage::LocalStopped { gateway_base_url } => t!(
            "gateway.status.local_stopped",
            gateway_address = gateway_base_url.as_str()
        )
        .to_string(),
        GatewayStatusMessage::RemoteUnavailable {
            endpoint_name,
            gateway_base_url,
        } => t!(
            "gateway.status.remote_unavailable",
            gateway_name = endpoint_name.as_str(),
            gateway_address = gateway_base_url.as_str()
        )
        .to_string(),
        GatewayStatusMessage::NotConfigured => t!("gateway.status.not_configured").to_string(),
        GatewayStatusMessage::Unavailable => t!("gateway.status.unavailable").to_string(),
        GatewayStatusMessage::LocalConflictAt { gateway_base_url } => t!(
            "gateway.status.local_conflict_at",
            gateway_address = gateway_base_url.as_str()
        )
        .to_string(),
        GatewayStatusMessage::LocalConflict => t!("gateway.status.local_conflict").to_string(),
        GatewayStatusMessage::FailedCheck { error } => {
            t!("gateway.status.failed_check", error = error.as_str()).to_string()
        }
        GatewayStatusMessage::SubsystemFailed { error } => {
            t!("gateway.status.subsystem_failed", error = error.as_str()).to_string()
        }
        GatewayStatusMessage::SubsystemNotReady => {
            t!("gateway.status.subsystem_not_ready").to_string()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::gateway_endpoint_subtitle;
    use pioneer_client::gateway::endpoint::GatewayBaseUrl;
    use pioneer_client::gateway::types::{GatewayEndpoint, GatewayEndpointKind};
    #[test]
    fn gateway_subtitles_interpolate_canonical_address() {
        rust_i18n::set_locale("en");

        let endpoint = |kind, address: &str| GatewayEndpoint {
            id: "gateway-id".to_owned(),
            name: "Gateway".to_owned(),
            gateway_base_url: GatewayBaseUrl::parse_presentation(address)
                .expect("valid gateway base URL"),
            kind,
            session_ref: None,
            server_gateway_id: None,
            workspace_id: None,
            service_name: None,
        };

        let local = gateway_endpoint_subtitle(&endpoint(
            GatewayEndpointKind::Local,
            "http://127.0.0.1:17878/",
        ));
        let remote = gateway_endpoint_subtitle(&endpoint(
            GatewayEndpointKind::Remote,
            "https://relay.example.com/pioneer/",
        ));

        assert_eq!(local, "Local - http://127.0.0.1:17878/");
        assert_eq!(remote, "Remote - https://relay.example.com/pioneer/");
        assert!(!local.contains("%{"));
        assert!(!remote.contains("%{"));
    }
}
