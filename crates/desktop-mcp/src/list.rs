use crate::catalog::McpCatalogView;
use gpui_kit::component::{button::*, scroll::Scrollbar, theme::ActiveTheme, *};
use gpui_kit::{prelude::*, *};
use pioneer_client::mcp::presentation as mcp_presentation;
use pioneer_client::mcp::types::{McpListItem, McpServerStatus};

impl McpCatalogView {
    pub(crate) fn render_mcp(&self, _window: &Window, cx: &mut Context<Self>) -> AnyElement {
        let desktop_entity = cx.entity().clone();
        let servers = &self.input.mcp_servers;

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
                                this.child(self.render_mcp_servers_list())
                            }),
                    ),
            )
            .into_any_element()
    }

    fn render_mcp_servers_list(&self) -> AnyElement {
        let items: Vec<_> = self
            .input
            .mcp_servers
            .iter()
            .map(|server| self.rows.get(&server.id).cloned())
            .collect();
        let scroll_handle = self.mcp_list_state.clone();

        div()
            .w_full()
            .h_full()
            .relative()
            .overflow_hidden()
            .child(
                gpui_kit::list(scroll_handle.clone(), move |ix, _, _| {
                    items
                        .get(ix)
                        .and_then(Option::as_ref)
                        .map(|row| row.clone().into_any_element())
                        .unwrap_or_else(|| div().into_any_element())
                })
                .size_full()
                .with_sizing_behavior(ListSizingBehavior::Auto),
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
            .pb_2p5()
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
        for (ix, server) in self.input.mcp_servers.iter().enumerate() {
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
                            self.mcp_list_state.remeasure_items(ix..ix + 1);
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
