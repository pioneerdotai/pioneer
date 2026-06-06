use crate::{app::root::PioneerDesktop, assets::PioneerIconName};
use gpui::{prelude::*, *};
use gpui_component::{button::*, scroll::Scrollbar, theme::ActiveTheme, *};
use pioneer_client::mcp::presentation as mcp_presentation;
use pioneer_protocol::{McpListItem, McpServerStatus};
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
        let capability_labels =
            Self::mcp_capability_labels(mcp_presentation::mcp_capability_counts(server).as_slice());
        let display_name = mcp_presentation::mcp_display_name(server);

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
        Self::mcp_status_label_from_kind(mcp_presentation::mcp_status_label(status))
    }

    pub(super) fn mcp_status_label_from_kind(status: mcp_presentation::McpStatusLabel) -> String {
        match status {
            mcp_presentation::McpStatusLabel::NotStarted => {
                t!("mcp.status.not_started").to_string()
            }
            mcp_presentation::McpStatusLabel::Disabled => t!("mcp.status.disabled").to_string(),
            mcp_presentation::McpStatusLabel::Starting => t!("mcp.status.starting").to_string(),
            mcp_presentation::McpStatusLabel::Ready => t!("mcp.status.ready").to_string(),
            mcp_presentation::McpStatusLabel::Degraded => t!("mcp.status.degraded").to_string(),
            mcp_presentation::McpStatusLabel::AuthRequired => {
                t!("mcp.status.auth_required").to_string()
            }
            mcp_presentation::McpStatusLabel::Failed => t!("mcp.status.failed").to_string(),
            mcp_presentation::McpStatusLabel::Stopping => t!("mcp.status.stopping").to_string(),
            mcp_presentation::McpStatusLabel::Stopped => t!("mcp.status.stopped").to_string(),
            mcp_presentation::McpStatusLabel::Restarting => t!("mcp.status.restarting").to_string(),
        }
    }

    pub(super) fn mcp_status_color(status: McpServerStatus, cx: &mut Context<Self>) -> Hsla {
        match mcp_presentation::mcp_status_tone(status) {
            mcp_presentation::McpPresentationTone::Success => cx.theme().success,
            mcp_presentation::McpPresentationTone::Warning => cx.theme().warning,
            mcp_presentation::McpPresentationTone::Danger => cx.theme().danger,
            mcp_presentation::McpPresentationTone::Muted => cx.theme().muted_foreground,
            mcp_presentation::McpPresentationTone::Default => cx.theme().foreground.opacity(0.84),
        }
    }

    pub(super) fn mcp_transport_label_from_presentation(
        transport: &mcp_presentation::McpTransportPresentation,
    ) -> String {
        match transport {
            mcp_presentation::McpTransportPresentation::Stdio { command } => {
                format!("stdio: {command}")
            }
            mcp_presentation::McpTransportPresentation::StreamableHttp { url } => {
                format!("http: {url}")
            }
        }
    }

    pub(super) fn mcp_capability_labels(
        counts: &[mcp_presentation::McpCapabilityCount],
    ) -> Vec<String> {
        counts
            .iter()
            .map(|count| {
                let label = match count.kind {
                    mcp_presentation::McpCapabilityKind::Tools => {
                        t!("mcp.capabilities.tools").to_string()
                    }
                    mcp_presentation::McpCapabilityKind::Resources => {
                        t!("mcp.capabilities.resources").to_string()
                    }
                    mcp_presentation::McpCapabilityKind::ResourceTemplates => {
                        t!("mcp.capabilities.templates").to_string()
                    }
                    mcp_presentation::McpCapabilityKind::Prompts => {
                        t!("mcp.capabilities.prompts").to_string()
                    }
                };
                format!("{} {label}", count.count)
            })
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
