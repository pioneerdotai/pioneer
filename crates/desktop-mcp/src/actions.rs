use crate::catalog::McpCatalogView;
use gpui_kit::*;
use pioneer_client::{
    mcp::operations::McpIntent,
    navigation::{NavigationIntent, SemanticDestination},
};
impl McpCatalogView {
    pub(crate) fn refresh_mcp_servers(&mut self, _: &mut Context<Self>) {
        if let Some(w) = self.input.navigation_input.workspace_id() {
            self.client.refresh_mcp(w);
        }
    }
    pub(crate) fn open_mcp_server_details(&mut self, id: String, _: &mut Context<Self>) {
        if self
            .principal_presentation_capabilities()
            .can_manage_capabilities
        {
            self.client.navigate(
                NavigationIntent::Navigate {
                    destination: SemanticDestination::Mcp {
                        server_id: Some(id),
                    },
                },
                None,
            );
        }
    }
    pub(crate) fn close_mcp_details_screen(&mut self, _: &mut Context<Self>) {
        self.client.navigate(
            NavigationIntent::Navigate {
                destination: if self
                    .principal_presentation_capabilities()
                    .can_manage_capabilities
                {
                    SemanticDestination::Mcp { server_id: None }
                } else {
                    SemanticDestination::Threads
                },
            },
            None,
        );
    }
    pub(crate) fn set_mcp_policy(
        &mut self,
        server_id: String,
        enabled: bool,
        allow_implicit_invocation: bool,
        cx: &mut Context<Self>,
    ) {
        self.send(
            McpIntent::Policy {
                server_id,
                enabled,
                allow_implicit_invocation,
            },
            cx,
        );
    }
    pub(crate) fn restart_mcp_server(&mut self, server_id: String, cx: &mut Context<Self>) {
        self.send(McpIntent::Restart { server_id }, cx);
    }
    pub(crate) fn uninstall_mcp_server(&mut self, server_id: String, cx: &mut Context<Self>) {
        self.send(McpIntent::Remove { server_id }, cx);
    }
    fn send(&mut self, intent: McpIntent, cx: &mut Context<Self>) {
        if let Some(w) = self.input.navigation_input.workspace_id() {
            if self.client.mcp_intent(w, intent).is_err() {
                self.presentation_error = Some(t!("mcp.error.gateway_not_connected").to_string());
                cx.notify();
            }
        }
    }
}
