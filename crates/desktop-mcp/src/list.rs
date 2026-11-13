use crate::catalog::McpCatalogView;
use gpui_kit::component::{button::*, scroll::Scrollbar, theme::ActiveTheme, *};
use gpui_kit::{prelude::*, *};
use pioneer_client::mcp::presentation as mcp_presentation;
use pioneer_client::mcp::types::{McpListItem, McpServerStatus};
use std::rc::Rc;

const MCP_SERVER_CARD_HEIGHT: f32 = crate::McpServerCard::HEIGHT;
const MCP_SERVER_ROW_GAP: f32 = 10.0;
const MCP_SERVER_ROW_HEIGHT: f32 = MCP_SERVER_CARD_HEIGHT + MCP_SERVER_ROW_GAP;

impl McpCatalogView {
    pub(crate) fn render_mcp(&self, _window: &Window, cx: &mut Context<Self>) -> AnyElement {
        let desktop_entity = cx.entity().clone();
        let servers = Rc::new(self.input.mcp_servers.clone());

        v_flex()
            .size_full()
            .bg(cx.theme().background)
            .child(pioneer_desktop_foundation::catalog_list_header(
                t!("mcp.screen.title").to_string(),
                t!("mcp.screen.description").to_string(),
                format!("{} {}", t!("mcp.screen.found_count"), servers.len()),
                cx,
            ))
            .child(
                v_flex()
                    .id(self.ui_id("catalog", "scroll"))
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
        _desktop_entity: Entity<Self>,
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
                    move |view, visible_range, _, _cx| {
                        visible_range
                            .filter_map(|ix| {
                                servers
                                    .get(ix)
                                    .and_then(|server| view.rows.get(&server.id))
                                    .map(|row| row.clone().into_any_element())
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
        identity: SharedString,
        server: &McpListItem,
        is_pending: bool,
        desktop_entity: WeakEntity<Self>,
        cx: &mut App,
    ) -> AnyElement {
        v_flex()
            .id(identity)
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
            .child(crate::McpServerCard::new(server, cx).pending(is_pending))
            .into_any_element()
    }

    pub(super) fn mcp_status_label(status: McpServerStatus) -> String {
        crate::McpServerCard::mcp_status_label(status)
    }

    pub(super) fn mcp_status_label_from_kind(status: mcp_presentation::McpStatusLabel) -> String {
        crate::McpServerCard::mcp_status_label_from_kind(status)
    }

    pub(super) fn mcp_status_color(status: McpServerStatus, cx: &mut App) -> Hsla {
        crate::McpServerCard::mcp_status_color(status, cx)
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
        crate::McpServerCard::mcp_capability_labels(counts)
    }
}

fn empty_state(
    desktop_entity: Entity<McpCatalogView>,
    cx: &mut Context<McpCatalogView>,
) -> AnyElement {
    pioneer_desktop_foundation::catalog_empty_state(
        t!("mcp.empty.title").to_string(),
        t!("mcp.empty.description").to_string(),
        Some(
            Button::new("mcp-empty-install")
                .text()
                .group("mcp-empty-install-btn")
                .on_click(move |_, window, cx| {
                    let _ = desktop_entity.update(cx, |view, cx| {
                        view.open_mcp_config_dialog(None, window, cx);
                        cx.notify();
                    });
                }),
        ),
        cx,
    )
}

use std::sync::Arc;
pub(crate) struct McpServerRow {
    server: Arc<McpListItem>,
    pending: bool,
    owner: WeakEntity<McpCatalogView>,
    identity: SharedString,
}
impl Render for McpServerRow {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        McpCatalogView::render_mcp_server_row(
            self.identity.clone(),
            &self.server,
            self.pending,
            self.owner.clone(),
            cx,
        )
    }
}
impl McpCatalogView {
    pub(crate) fn sync_rows(&mut self, cx: &mut Context<Self>) {
        self.rows
            .retain(|id, _| self.input.mcp_servers.iter().any(|s| &s.id == id));
        for server in &self.input.mcp_servers {
            let pending = self.input.is_mcp_pending(&server.id);
            if let Some(row) = self.rows.get(&server.id) {
                row.update(cx, |row, cx| {
                    let visible_changed = mcp_presentation::mcp_display_name(&row.server)
                        != mcp_presentation::mcp_display_name(server)
                        || row.server.status != server.status
                        || mcp_presentation::mcp_capability_counts(&row.server)
                            != mcp_presentation::mcp_capability_counts(server)
                        || row.pending != pending;
                    if *row.server != *server || row.pending != pending {
                        row.server = Arc::new(server.clone());
                        row.pending = pending;
                        if visible_changed {
                            cx.notify();
                        }
                    }
                });
            } else {
                let owner = cx.weak_entity();
                let identity = self.ui_id(&server.id, "catalog-row");
                let row = cx.new(|_| McpServerRow {
                    server: Arc::new(server.clone()),
                    pending,
                    owner,
                    identity,
                });
                self.rows.insert(server.id.clone(), row);
            }
        }
    }
}
