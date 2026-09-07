use super::*;

impl PioneerDesktop {
    pub(in crate::app) fn refresh_workspace_bound_screens_after_switch(
        &mut self,
        cx: &mut Context<Self>,
    ) {
        match self.main_content_view() {
            MainContentView::Skills | MainContentView::SkillDetails => {
                self.queue_skills_refresh();
                self.refresh_installed_skills(cx);
            }
            MainContentView::Mcp | MainContentView::McpDetails => {
                self.queue_mcp_refresh();
                self.refresh_mcp_servers(cx);
                if self.navigation_input.mcp_server_id().is_some() {
                    self.queue_mcp_details_refresh();
                }
            }
            MainContentView::Providers => {
                self.sync_provider_sidebar_tree_state(cx);
            }
            MainContentView::Administration => {
                self.sync_administration_sidebar_tree_state(cx);
                self.refresh_current_administration_content(cx);
            }
            _ => {}
        }
    }
}
