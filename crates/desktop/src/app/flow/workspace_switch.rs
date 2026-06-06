use super::*;
#[cfg(test)]
use pioneer_client::threads::tree::WorkspaceThreadState;
use pioneer_client::threads::tree::{
    remember_workspace_thread_state, restore_workspace_thread_state,
};
use pioneer_client::workspaces::actions as workspace_actions;
use pioneer_client::workspaces::selectors as workspace_selectors;
#[cfg(test)]
pub(crate) use pioneer_client::workspaces::selectors::{
    workspace_switch_is_noop, workspace_switch_target_is_known_active,
};

impl PioneerDesktop {
    pub(in crate::app) fn switch_workspace_from_ui(
        &mut self,
        workspace_id: String,
        cx: &mut Context<Self>,
    ) {
        let current_workspace_id = self.current_workspace_scope();
        let switch_plan = workspace_actions::plan_workspace_switch_from_ui(
            workspace_id,
            self.workspace_action_in_progress(),
            current_workspace_id.as_deref(),
            self.workspaces(),
        );
        let workspace_id = match switch_plan {
            workspace_actions::WorkspaceSwitchPlan::Switch { workspace_id } => workspace_id,
            workspace_actions::WorkspaceSwitchPlan::MissingWorkspaceId => {
                self.set_workspaces_error(Some("workspace id is required".to_owned()));
                return;
            }
            workspace_actions::WorkspaceSwitchPlan::UnknownTarget { workspace_id } => {
                self.set_workspaces_error(Some(format!(
                    "workspace `{}` is not available",
                    workspace_id
                )));
                return;
            }
            workspace_actions::WorkspaceSwitchPlan::Busy
            | workspace_actions::WorkspaceSwitchPlan::Noop => return,
        };

        if self.gateway.connection_state != GatewayConnectionState::Connected {
            self.set_workspaces_error(Some(t!("mcp.error.gateway_not_connected").to_string()));
            return;
        }
        let Some(connection_id) = self.gateway.ws_connection_id else {
            self.set_workspaces_error(Some(t!("mcp.error.gateway_not_connected").to_string()));
            return;
        };

        if let Some(old_workspace_id) = current_workspace_id.as_deref() {
            self.remember_current_workspace_thread_state(old_workspace_id);
        }
        self.remember_active_thread_draft(cx);
        self.reset_thread_start_state();
        self.clear_thread_start_queue();
        self.clear_turn_resume_queue();
        self.thread_list_loading = false;
        self.set_workspace_action_in_progress(true);
        self.set_workspaces_error(None);

        let ws_sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let requested_workspace_id = workspace_id.clone();

            async move {
                let result = cx
                    .background_spawn(async move {
                        ws_sender.workspace_select(WorkspaceSelectParams {
                            workspace_id: requested_workspace_id,
                            make_current: false,
                        })
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if !workspace_actions::workspace_action_result_matches_connection(
                        connection_id,
                        view.gateway.ws_connection_id,
                    ) {
                        return;
                    }

                    view.set_workspace_action_in_progress(false);

                    match result {
                        Ok(response) => {
                            let selected_workspace_id =
                                workspace_actions::apply_workspace_select_response_to_catalog(
                                    &mut view.workspaces,
                                    response.workspace,
                                );
                            view.persist_active_gateway_workspace_id(selected_workspace_id.clone());
                            view.set_preferred_workspace_id(Some(selected_workspace_id.clone()));
                            view.load_thread_folder_expansion_for_workspace(
                                selected_workspace_id.as_str(),
                                cx,
                            );
                            view.restore_workspace_thread_state(selected_workspace_id.as_str());
                            view.thread_list_loading = false;
                            view.refresh_thread_list(cx);
                            view.refresh_workspace_bound_screens_after_switch(cx);
                        }
                        Err(error) => {
                            let message = format!("{error:#}");
                            warn!(error = message.as_str(), "failed to switch workspace");
                            view.set_workspaces_error(Some(message));
                        }
                    }

                    cx.notify();
                });
            }
        })
        .detach();
    }

    fn current_workspace_scope(&self) -> Option<String> {
        let runtime_workspace_id = self
            .gateway
            .runtime
            .as_ref()
            .and_then(GatewayRuntime::active_workspace_id);
        workspace_selectors::resolve_workspace_scope(
            self.active_workspace_id(),
            self.preferred_workspace_id(),
            runtime_workspace_id,
        )
    }

    fn remember_current_workspace_thread_state(&mut self, workspace_id: &str) {
        let remembered = remember_workspace_thread_state(
            workspace_id,
            self.current_active_thread_id(),
            self.draft_thread_id(),
            self.thread_start_coordinator().pending_thread_id.as_deref(),
            |thread_id, workspace_id| self.thread_workspace_matches(thread_id, workspace_id),
        );

        self.remember_last_active_thread_for_workspace(workspace_id, remembered.active_thread_id);
        self.remember_draft_thread_for_workspace(workspace_id, remembered.draft_thread_id);
    }

    fn restore_workspace_thread_state(&mut self, workspace_id: &str) {
        let restored = restore_workspace_thread_state(
            workspace_id,
            self.last_active_thread_for_workspace(workspace_id),
            self.draft_thread_for_workspace(workspace_id),
            |thread_id, workspace_id| self.thread_workspace_matches(thread_id, workspace_id),
        );

        self.set_active_thread_id(restored.active_thread_id);
        self.set_draft_thread_id(restored.draft_thread_id);
    }

    fn refresh_workspace_bound_screens_after_switch(&mut self, cx: &mut Context<Self>) {
        match self.main_content_view {
            MainContentView::Skills | MainContentView::SkillDetails => {
                self.queue_skills_refresh();
                self.refresh_installed_skills(cx);
            }
            MainContentView::Mcp | MainContentView::McpDetails => {
                self.queue_mcp_refresh();
                self.refresh_mcp_servers(cx);
                if self.mcp_selected_server_id.is_some() {
                    self.queue_mcp_details_refresh();
                }
            }
            MainContentView::Providers => {
                self.providers.clear_for_workspace_switch();
                self.refresh_configured_providers(cx);
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn matches_workspace<'a>(
        threads: &'a HashMap<&'a str, &'a str>,
    ) -> impl Fn(&str, &str) -> bool + 'a {
        |thread_id, workspace_id| threads.get(thread_id).copied() == Some(workspace_id)
    }

    #[::core::prelude::v1::test]
    fn workspace_thread_state_restores_a_after_switching_a_to_b_to_a() {
        let threads = HashMap::from([
            ("thr_a", "ws_a"),
            ("draft_a", "ws_a"),
            ("thr_b", "ws_b"),
            ("draft_b", "ws_b"),
        ]);
        let remembered_a = remember_workspace_thread_state(
            "ws_a",
            Some("thr_a"),
            Some("draft_a"),
            None,
            matches_workspace(&threads),
        );
        let remembered_b = remember_workspace_thread_state(
            "ws_b",
            Some("thr_b"),
            Some("draft_b"),
            None,
            matches_workspace(&threads),
        );

        let restored_a = restore_workspace_thread_state(
            "ws_a",
            remembered_a.active_thread_id.as_deref(),
            remembered_a.draft_thread_id.as_deref(),
            matches_workspace(&threads),
        );

        assert_eq!(
            restored_a,
            WorkspaceThreadState {
                active_thread_id: Some("thr_a".to_owned()),
                draft_thread_id: Some("draft_a".to_owned()),
            }
        );
        assert_eq!(
            remembered_b,
            WorkspaceThreadState {
                active_thread_id: Some("thr_b".to_owned()),
                draft_thread_id: Some("draft_b".to_owned()),
            }
        );
    }

    #[::core::prelude::v1::test]
    fn workspace_thread_state_keeps_drafts_isolated_by_workspace() {
        let threads = HashMap::from([("draft_a", "ws_a"), ("draft_b", "ws_b")]);

        let remembered_a = remember_workspace_thread_state(
            "ws_a",
            None,
            Some("draft_a"),
            None,
            matches_workspace(&threads),
        );
        let remembered_b = remember_workspace_thread_state(
            "ws_b",
            None,
            Some("draft_b"),
            None,
            matches_workspace(&threads),
        );

        assert_eq!(remembered_a.draft_thread_id.as_deref(), Some("draft_a"));
        assert_eq!(remembered_b.draft_thread_id.as_deref(), Some("draft_b"));
    }

    #[::core::prelude::v1::test]
    fn workspace_thread_state_falls_back_to_valid_draft_when_last_active_missing() {
        let threads = HashMap::from([("draft_a", "ws_a"), ("thr_b", "ws_b")]);

        let restored = restore_workspace_thread_state(
            "ws_a",
            Some("thr_missing"),
            Some("draft_a"),
            matches_workspace(&threads),
        );

        assert_eq!(
            restored,
            WorkspaceThreadState {
                active_thread_id: Some("draft_a".to_owned()),
                draft_thread_id: Some("draft_a".to_owned()),
            }
        );

        let restored_missing = restore_workspace_thread_state(
            "ws_a",
            Some("thr_b"),
            Some("draft_missing"),
            matches_workspace(&threads),
        );
        assert_eq!(
            restored_missing,
            WorkspaceThreadState {
                active_thread_id: None,
                draft_thread_id: None,
            }
        );
    }
}
