use crate::{app::root::PioneerDesktop, assets::PioneerIconName};
use gpui::{prelude::*, *};
use gpui_component::{button::*, scroll::Scrollbar, theme::ActiveTheme, *};
use pioneer_protocol::{McpListItem, McpRuntimeState, McpServerStatus, McpTransportSummary};
use std::rc::Rc;

const MCP_SERVER_CARD_HEIGHT: f32 = 78.0;
const MCP_SERVER_ROW_GAP: f32 = 10.0;
const MCP_SERVER_ROW_HEIGHT: f32 = MCP_SERVER_CARD_HEIGHT + MCP_SERVER_ROW_GAP;

impl PioneerDesktop {
    pub(crate) fn render_mcp(&self, _window: &Window, cx: &mut Context<Self>) -> AnyElement {
        let desktop_entity = cx.entity().clone();
        let servers = Rc::new(self.mcp_servers.clone());

        v_flex()
            .size_full()
            .bg(cx.theme().background)
            .child(
                v_flex()
                    .pt_3()
                    .px_6()
                    .pb_3()
                    .gap_2()
                    .border_b_1()
                    .border_color(cx.theme().border)
                    .child(
                        v_flex()
                            .child(
                                div()
                                    .text_xl()
                                    .font_semibold()
                                    .child(t!("mcp.screen.title").to_string()),
                            )
                            .child(
                                div()
                                    .text_sm()
                                    .opacity(0.6)
                                    .child(t!("mcp.screen.description").to_string()),
                            ),
                    )
                    .child(h_flex().items_center().gap_2().child(
                        div().text_xs().opacity(0.6).child(format!(
                            "{} {}",
                            t!("mcp.screen.found_count"),
                            servers.len()
                        )),
                    )),
            )
            .child(
                v_flex()
                    .id("mcp-scroll")
                    .flex_1()
                    .overflow_hidden()
                    .p_6()
                    .child(
                        v_flex()
                            .w_full()
                            .h_full()
                            .gap_3()
                            .when(servers.is_empty(), |this| {
                                this.child(empty_state(desktop_entity.clone(), cx))
                            })
                            .when(!servers.is_empty(), |this| {
                                this.child(self.render_mcp_servers_virtual_list(
                                    servers.clone(),
                                    desktop_entity.clone(),
                                    cx,
                                ))
                            }),
                    ),
            )
            .into_any_element()
    }

    fn render_mcp_servers_virtual_list(
        &self,
        servers: Rc<Vec<McpListItem>>,
        desktop_entity: Entity<Self>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let item_sizes = Rc::new(
            (0..servers.len())
                .map(|_| size(px(0.), px(MCP_SERVER_ROW_HEIGHT)))
                .collect::<Vec<_>>(),
        );
        let scroll_handle = self.mcp_list_scroll_handle.clone();

        div()
            .w_full()
            .h_full()
            .relative()
            .overflow_hidden()
            .child(
                v_virtual_list(
                    cx.entity(),
                    "mcp-installed-virtual-list",
                    item_sizes,
                    move |view, visible_range, _, cx| {
                        visible_range
                            .filter_map(|ix| {
                                servers.get(ix).map(|server| {
                                    let is_pending = view.is_mcp_pending(server.name.as_str());
                                    Self::render_mcp_server_row(
                                        ix,
                                        server,
                                        is_pending,
                                        desktop_entity.clone(),
                                        cx,
                                    )
                                })
                            })
                            .collect::<Vec<_>>()
                    },
                )
                .with_sizing_behavior(ListSizingBehavior::Auto)
                .track_scroll(&scroll_handle),
            )
            .child(
                div()
                    .absolute()
                    .top_0()
                    .left_0()
                    .right_0()
                    .bottom_0()
                    .child(Scrollbar::vertical(&scroll_handle)),
            )
            .into_any_element()
    }

    fn render_mcp_server_row(
        index: usize,
        server: &McpListItem,
        is_pending: bool,
        desktop_entity: Entity<Self>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let status_color = Self::mcp_status_color(server.status, cx);
        let status_label = Self::mcp_status_label(server.status);
        let capability_labels = Self::mcp_capability_labels(
            server.tools_count,
            server.resources_count,
            server.resource_templates_count,
            server.prompts_count,
        );
        let display_name = server
            .display_name
            .clone()
            .unwrap_or_else(|| server.name.clone());

        v_flex()
            .id(("mcp-server-row", index))
            .w_full()
            .h(px(MCP_SERVER_ROW_HEIGHT))
            .pb(px(MCP_SERVER_ROW_GAP))
            .cursor_pointer()
            .on_click({
                let desktop_entity = desktop_entity.clone();
                let server_id = server.id.clone();
                move |_, _, cx| {
                    let _ = desktop_entity.update(cx, |view, cx| {
                        view.open_mcp_server_details(server_id.clone(), cx);
                        cx.notify();
                    });
                }
            })
            .child(
                h_flex()
                    .w_full()
                    .h(px(MCP_SERVER_CARD_HEIGHT))
                    .pt_3()
                    .px_4()
                    .pb_3()
                    .rounded_lg()
                    .bg(cx.theme().background)
                    .border_1()
                    .border_color(cx.theme().border)
                    .gap_4()
                    .items_start()
                    .hover(|this| this.bg(cx.theme().secondary.opacity(0.45)))
                    .child(
                        v_flex()
                            .flex_1()
                            .h_full()
                            .gap_1()
                            .child(
                                h_flex()
                                    .w_full()
                                    .justify_between()
                                    .items_center()
                                    .gap_2()
                                    .child(div().text_sm().font_semibold().child(display_name)),
                            )
                            .child(div().flex_1())
                            .child(
                                h_flex().justify_between().items_center().gap_2().child(
                                    h_flex()
                                        .items_center()
                                        .gap_2()
                                        .children(
                                            capability_labels.into_iter().map(|label| {
                                                div().text_xs().opacity(0.6).child(label)
                                            }),
                                        )
                                        .when(is_pending, |this| {
                                            this.child(
                                                Icon::new(PioneerIconName::RefreshCw)
                                                    .size_3()
                                                    .text_color(cx.theme().warning),
                                            )
                                        }),
                                ),
                            ),
                    )
                    .child(
                        div()
                            .mt_1p5()
                            .px_2()
                            .py_0p5()
                            .rounded_full()
                            .border_1()
                            .border_color(status_color)
                            .text_xs()
                            .text_color(status_color)
                            .font_medium()
                            .child(status_label),
                    ),
            )
            .into_any_element()
    }

    pub(super) fn mcp_status_label(status: McpServerStatus) -> String {
        match status {
            McpServerStatus::NotStarted => t!("mcp.status.not_started").to_string(),
            McpServerStatus::Disabled => t!("mcp.status.disabled").to_string(),
            McpServerStatus::Starting => t!("mcp.status.starting").to_string(),
            McpServerStatus::Ready => t!("mcp.status.ready").to_string(),
            McpServerStatus::Degraded => t!("mcp.status.degraded").to_string(),
            McpServerStatus::AuthRequired => t!("mcp.status.auth_required").to_string(),
            McpServerStatus::Failed => t!("mcp.status.failed").to_string(),
            McpServerStatus::Stopping => t!("mcp.status.stopping").to_string(),
            McpServerStatus::Stopped => t!("mcp.status.stopped").to_string(),
            McpServerStatus::Restarting => t!("mcp.status.restarting").to_string(),
        }
    }

    pub(super) fn mcp_runtime_label(status: McpRuntimeState) -> String {
        match status {
            McpRuntimeState::NotStarted => t!("mcp.status.not_started").to_string(),
            McpRuntimeState::Disabled => t!("mcp.status.disabled").to_string(),
            McpRuntimeState::Starting => t!("mcp.status.starting").to_string(),
            McpRuntimeState::Ready => t!("mcp.status.ready").to_string(),
            McpRuntimeState::Degraded => t!("mcp.status.degraded").to_string(),
            McpRuntimeState::AuthRequired => t!("mcp.status.auth_required").to_string(),
            McpRuntimeState::Failed => t!("mcp.status.failed").to_string(),
            McpRuntimeState::Stopping => t!("mcp.status.stopping").to_string(),
            McpRuntimeState::Stopped => t!("mcp.status.stopped").to_string(),
            McpRuntimeState::Restarting => t!("mcp.status.restarting").to_string(),
        }
    }

    pub(super) fn mcp_status_color(status: McpServerStatus, cx: &mut Context<Self>) -> Hsla {
        match status {
            McpServerStatus::Ready => cx.theme().success,
            McpServerStatus::Degraded | McpServerStatus::Starting | McpServerStatus::Restarting => {
                cx.theme().warning
            }
            McpServerStatus::Failed | McpServerStatus::AuthRequired => cx.theme().danger,
            McpServerStatus::Disabled | McpServerStatus::Stopped | McpServerStatus::Stopping => {
                cx.theme().muted_foreground
            }
            McpServerStatus::NotStarted => cx.theme().foreground.opacity(0.45),
        }
    }

    pub(super) fn mcp_transport_label(transport: &McpTransportSummary) -> String {
        match transport {
            McpTransportSummary::Stdio { command } => format!("stdio: {command}"),
            McpTransportSummary::StreamableHttp { url } => format!("http: {url}"),
        }
    }

    pub(super) fn mcp_capability_labels(
        tools_count: usize,
        resources_count: usize,
        resource_templates_count: usize,
        prompts_count: usize,
    ) -> Vec<String> {
        [
            (tools_count, t!("mcp.capabilities.tools").to_string()),
            (
                resources_count,
                t!("mcp.capabilities.resources").to_string(),
            ),
            (
                resource_templates_count,
                t!("mcp.capabilities.templates").to_string(),
            ),
            (prompts_count, t!("mcp.capabilities.prompts").to_string()),
        ]
        .into_iter()
        .filter(|(count, _)| *count > 0)
        .map(|(count, label)| format!("{count} {label}"))
        .collect()
    }
}

fn empty_state(
    desktop_entity: Entity<PioneerDesktop>,
    cx: &mut Context<PioneerDesktop>,
) -> AnyElement {
    v_flex()
        .w_full()
        .h_full()
        .items_center()
        .justify_center()
        .gap_4()
        .p_8()
        .bg(cx.theme().background)
        .child(
            Button::new("mcp-empty-install")
                .text()
                .group("mcp-empty-install-btn")
                .on_click(move |_, window, cx| {
                    let _ = desktop_entity.update(cx, |view, cx| {
                        view.open_mcp_config_dialog(None, window, cx);
                        cx.notify();
                    });
                })
                .child(
                    div()
                        .size_10()
                        .rounded_full()
                        .bg(cx.theme().foreground)
                        .flex()
                        .items_center()
                        .justify_center()
                        .text_color(cx.theme().primary_foreground)
                        .child(Icon::new(IconName::Plus).size_6()),
                ),
        )
        .child(
            v_flex()
                .items_center()
                .justify_center()
                .gap_1()
                .child(
                    div()
                        .text_base()
                        .font_semibold()
                        .child(t!("mcp.empty.title").to_string()),
                )
                .child(
                    div()
                        .text_sm()
                        .opacity(0.6)
                        .child(t!("mcp.empty.description").to_string()),
                ),
        )
        .into_any_element()
}
