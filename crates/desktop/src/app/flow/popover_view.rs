use super::*;
use crate::assets::PioneerIconName;

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

impl PioneerDesktop {
    pub(crate) fn render_gateways_popover(&self, cx: &mut Context<Self>) -> AnyElement {
        let show_spinner = !self.is_gateway_setup_required()
            && (self.gateway.connecting || self.gateway.connection_state.is_transitioning());

        let gateway_status_color = match self.gateway.status_level {
            GatewayStatusLevel::Neutral => cx.theme().muted_foreground,
            GatewayStatusLevel::Connected => cx.theme().success,
            GatewayStatusLevel::Degraded => cx.theme().warning,
            GatewayStatusLevel::Failed => cx.theme().danger,
        };

        let gateway_endpoints = self
            .gateway
            .runtime
            .as_ref()
            .map(GatewayRuntime::endpoints)
            .unwrap_or_default();

        let active_gateway_id = self
            .gateway
            .runtime
            .as_ref()
            .and_then(|runtime| runtime.active_gateway_id().map(|id| id.to_owned()));

        let active_gateway = active_gateway_id
            .as_deref()
            .and_then(|id| gateway_endpoints.iter().find(|endpoint| endpoint.id == id))
            .or_else(|| gateway_endpoints.first());

        let gateway_trigger_label = active_gateway
            .map(|endpoint| format!("{} ({})", endpoint.name, endpoint.gateway_base_url))
            .unwrap_or_else(|| t!("gateway.status.connecting").to_string());

        let gateway_hover_status = self.gateway.status.clone();
        let gateway_hover_error = self.gateway.error.clone();

        let gateway_selection_locked = self.gateway.connecting;

        let active_indicator_color = cx.theme().success;
        let inactive_indicator_color = cx.theme().yellow;
        let option_foreground = cx.theme().foreground;
        let option_muted_background = cx.theme().muted;

        let desktop_entity = cx.entity();

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
                                this.child(Spinner::new().with_size(gpui_component::Size::Small))
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
                            .tooltip({
                                let gateway_hover_status = gateway_hover_status.clone();
                                let gateway_hover_error = gateway_hover_error.clone();

                                move |window, tooltip_cx| {
                                    let gateway_hover_status = gateway_hover_status.clone();
                                    let gateway_hover_error = gateway_hover_error.clone();

                                    gpui_component::tooltip::Tooltip::element(
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
        index: usize,
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
            .id(("gateway-option", index))
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
                            );
                            true
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
                            Button::new(("gateway-option-edit", index))
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
