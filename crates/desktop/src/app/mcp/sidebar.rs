use crate::{
    app::root::{GatewayConnectionState, PioneerDesktop},
    assets::PioneerIconName,
};
use gpui::{prelude::*, *};
use gpui_component::{button::*, theme::ActiveTheme, *};

const SIDEBAR_MENU_ITEM_OPACITY: f32 = 0.8;

impl PioneerDesktop {
    pub(crate) fn render_mcp_sidebar(&self, cx: &mut Context<Self>) -> AnyElement {
        let desktop_entity = cx.entity().clone();
        let is_connected = self.gateway.connection_state == GatewayConnectionState::Connected;
        let install_pending = self.is_mcp_pending("__install__");

        v_flex()
            .size_full()
            .bg(cx.theme().sidebar)
            .p_0()
            .gap_1()
            .child(
                v_flex()
                    .pt_4()
                    .px_2()
                    .gap_1()
                    .child(
                        Button::new("mcp-sidebar-install")
                            .ghost()
                            .justify_start()
                            .px_2()
                            .disabled(!is_connected || install_pending)
                            .loading(install_pending)
                            .child(sidebar_icon(IconName::Plus, cx))
                            .child(sidebar_label(t!("mcp.sidebar.install").to_string()))
                            .on_click({
                                let desktop_entity = desktop_entity.clone();
                                move |_, window, cx| {
                                    let _ = desktop_entity.update(cx, |view, cx| {
                                        view.open_mcp_config_dialog(None, window, cx);
                                        cx.notify();
                                    });
                                }
                            }),
                    )
                    .child(
                        Button::new("mcp-sidebar-refresh")
                            .ghost()
                            .justify_start()
                            .px_2()
                            .disabled(!is_connected)
                            .loading(self.mcp_loading)
                            .child(sidebar_pioneer_icon(PioneerIconName::RefreshCw))
                            .child(sidebar_label(t!("mcp.sidebar.refresh").to_string()))
                            .on_click(cx.listener(|view, _, _, cx| {
                                view.refresh_mcp_servers(cx);
                                cx.notify();
                            })),
                    ),
            )
            .into_any_element()
    }

    pub(crate) fn render_mcp_details_sidebar(&self, cx: &mut Context<Self>) -> AnyElement {
        let desktop_entity = cx.entity().clone();
        let is_connected = self.gateway.connection_state == GatewayConnectionState::Connected;
        let selected = self.mcp_selected_server_id.as_ref().and_then(|server_id| {
            self.mcp_servers
                .iter()
                .find(|server| server.id == *server_id)
                .cloned()
        });
        let is_pending = selected
            .as_ref()
            .is_some_and(|server| self.is_mcp_pending(server.name.as_str()));
        let restart_disabled = selected
            .as_ref()
            .is_none_or(|server| !server.policy.enabled);

        v_flex()
            .size_full()
            .bg(cx.theme().sidebar)
            .p_0()
            .gap_1()
            .child(
                v_flex()
                    .pt_4()
                    .px_2()
                    .gap_1()
                    .child(
                        Button::new("mcp-details-sidebar-back")
                            .ghost()
                            .justify_start()
                            .px_2()
                            .child({
                                let icon_bg = cx.theme().foreground.opacity(0.075);
                                let icon_bg_hover = cx.theme().foreground.opacity(0.1);
                                div()
                                    .id("mcp-details-sidebar-back-icon")
                                    .size_6()
                                    .rounded_full()
                                    .bg(icon_bg)
                                    .group_hover("mcp-details-sidebar-back-btn", move |style| {
                                        style.bg(icon_bg_hover)
                                    })
                                    .flex()
                                    .items_center()
                                    .justify_center()
                                    .child(
                                        Icon::new(IconName::ChevronLeft)
                                            .size_5()
                                            .ml_neg_px()
                                            .opacity(SIDEBAR_MENU_ITEM_OPACITY),
                                    )
                            })
                            .child(sidebar_label(t!("mcp.sidebar.back").to_string()))
                            .on_click({
                                let desktop_entity = desktop_entity.clone();
                                move |_, _, cx| {
                                    let _ = desktop_entity.update(cx, |view, cx| {
                                        view.close_mcp_details_screen(cx);
                                        cx.notify();
                                    });
                                }
                            }),
                    )
                    .child(
                        Button::new("mcp-details-sidebar-update")
                            .ghost()
                            .justify_start()
                            .px_2()
                            .disabled(!is_connected || selected.is_none() || is_pending)
                            .child(sidebar_pioneer_icon(PioneerIconName::RefreshCw))
                            .child(sidebar_label(t!("mcp.sidebar.update_config").to_string()))
                            .on_click({
                                let desktop_entity = desktop_entity.clone();
                                move |_, window, cx| {
                                    let _ = desktop_entity.update(cx, |view, cx| {
                                        view.open_mcp_config_dialog(None, window, cx);
                                        cx.notify();
                                    });
                                }
                            }),
                    )
                    .child(
                        Button::new("mcp-details-sidebar-restart")
                            .ghost()
                            .justify_start()
                            .px_2()
                            .disabled(
                                !is_connected
                                    || selected.is_none()
                                    || is_pending
                                    || restart_disabled,
                            )
                            .loading(is_pending)
                            .child(sidebar_pioneer_icon(PioneerIconName::RotateCcw))
                            .child(sidebar_label(t!("mcp.sidebar.restart").to_string()))
                            .on_click({
                                let desktop_entity = desktop_entity.clone();
                                let selected = selected.clone();
                                move |_, _, cx| {
                                    let Some(selected) = selected.clone() else {
                                        return;
                                    };
                                    let _ = desktop_entity.update(cx, |view, cx| {
                                        view.restart_mcp_server(selected.name.clone(), cx);
                                        cx.notify();
                                    });
                                }
                            }),
                    )
                    .child(
                        Button::new("mcp-details-sidebar-uninstall")
                            .ghost()
                            .justify_start()
                            .px_2()
                            .disabled(!is_connected || selected.is_none() || is_pending)
                            .child(sidebar_pioneer_icon(PioneerIconName::Trash))
                            .child(sidebar_label(t!("mcp.sidebar.uninstall").to_string()))
                            .on_click({
                                let desktop_entity = desktop_entity.clone();
                                let selected = selected.clone();
                                move |_, window, cx| {
                                    let Some(selected) = selected.clone() else {
                                        return;
                                    };
                                    let _ = desktop_entity.update(cx, |view, cx| {
                                        view.confirm_uninstall_mcp_server(
                                            selected.name.clone(),
                                            window,
                                            cx,
                                        );
                                        cx.notify();
                                    });
                                }
                            }),
                    ),
            )
            .into_any_element()
    }
}

fn sidebar_icon(icon: IconName, cx: &mut Context<PioneerDesktop>) -> AnyElement {
    let icon_bg = cx.theme().foreground.opacity(0.075);
    div()
        .size_6()
        .rounded_full()
        .bg(icon_bg)
        .flex()
        .items_center()
        .justify_center()
        .child(Icon::new(icon).size_4().opacity(SIDEBAR_MENU_ITEM_OPACITY))
        .into_any_element()
}

fn sidebar_pioneer_icon(icon: PioneerIconName) -> AnyElement {
    div()
        .size_6()
        .rounded_full()
        .flex()
        .items_center()
        .justify_center()
        .child(Icon::new(icon).size_4().opacity(SIDEBAR_MENU_ITEM_OPACITY))
        .into_any_element()
}

fn sidebar_label(label: String) -> AnyElement {
    div()
        .line_height(relative(1.))
        .opacity(SIDEBAR_MENU_ITEM_OPACITY)
        .child(label)
        .into_any_element()
}
