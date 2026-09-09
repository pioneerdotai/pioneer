use crate::app::root::{MainContentView, PioneerDesktop};
use gpui_kit::*;
impl PioneerDesktop {
    pub(in crate::app) fn open_mcp_screen_from_bottom_bar(&mut self, cx: &mut Context<Self>) {
        if self
            .principal_presentation_capabilities()
            .can_manage_capabilities
        {
            self.set_main_content_view(MainContentView::Mcp, cx);
        }
    }
    pub(crate) fn open_mcp_server_details_from_timeline(
        &mut self,
        server_id: String,
        _: &mut Context<Self>,
    ) {
        if !self.principal_presentation_capabilities().can_use_mcp {
            return;
        }
        let client = self.gateway.client_runtime.client_core();
        let id = self
            .navigation_input
            .workspace_id()
            .and_then(|workspace| client.mcp_catalog_snapshot(workspace))
            .and_then(|p| {
                p.servers()
                    .iter()
                    .find(|s| s.id == server_id || s.name == server_id)
                    .map(|s| s.id.clone())
            })
            .unwrap_or(server_id);
        client.navigate(
            pioneer_client::navigation::NavigationIntent::Navigate {
                destination: pioneer_client::navigation::SemanticDestination::Mcp {
                    server_id: Some(id),
                },
            },
            None,
        );
    }
}
