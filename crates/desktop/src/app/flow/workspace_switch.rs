use super::*;
use pioneer_client::threads::start as thread_start;
use pioneer_client::threads::tree::{
    remember_workspace_thread_state, restore_workspace_thread_state,
};
use pioneer_client::workspaces::actions as workspace_actions;
use pioneer_client::workspaces::selectors as workspace_selectors;

const WORKSPACE_SWITCH_SESSION_RETRY_DELAY: Duration = Duration::from_millis(100);

impl PioneerDesktop {
    fn apply_workspace_switch_success_reduction(
        &mut self,
        reduction: workspace_actions::WorkspaceSwitchSuccessReduction,
        cx: &mut Context<Self>,
    ) {
        self.set_workspaces(reduction.workspaces);
        self.persist_active_gateway_workspace_id(
            reduction
                .selected
                .persist_active_gateway_workspace_id
                .clone(),
        );
        self.set_preferred_workspace_id(Some(reduction.selected.set_preferred_workspace_id));
        self.clear_task_user_notification_inbox();
        self.refresh_current_principal(cx);
        if reduction.clear_thread_list_loading {
            self.thread_list_loading = false;
        }
        if reduction.selected.refresh_thread_list {
            self.refresh_thread_list(cx);
        }
        self.providers.clear_for_workspace_switch();
        self.refresh_configured_providers(cx);
        self.refresh_cli_providers_auto(cx);
        if reduction.refresh_workspace_bound_screens {
            self.refresh_workspace_bound_screens_after_switch(cx);
        }
    }

    pub(in crate::app) fn switch_workspace_from_ui(
        &mut self,
        workspace_id: String,
        cx: &mut Context<Self>,
    ) {
        // Do not move the server-side connection scope while a composer request
        // still owns the current workspace context.
        if self.desktop_voice_context_locked() || self.composer_upload_in_progress {
            return;
        }
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
                self.set_workspaces_error(Some(t!("workspace.error.id_required").to_string()));
                return;
            }
            workspace_actions::WorkspaceSwitchPlan::UnknownTarget { workspace_id } => {
                self.set_workspaces_error(Some(
                    t!(
                        "workspace.error.not_available",
                        workspace_id = workspace_id.as_str()
                    )
                    .to_string(),
                ));
                return;
            }
            workspace_actions::WorkspaceSwitchPlan::Busy
            | workspace_actions::WorkspaceSwitchPlan::Noop => return,
        };

        if self.gateway.connection_state != GatewayConnectionState::Connected {
            self.set_workspaces_error(Some(
                t!("workspace.error.gateway_not_connected").to_string(),
            ));
            return;
        }
        if self.gateway.ws_connection_id.is_none() {
            self.set_workspaces_error(Some(
                t!("workspace.error.gateway_not_connected").to_string(),
            ));
            return;
        }

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

        // Publish the user's selection immediately. Server scope selection and
        // thread subscription continue below in the background; composer
        // actions perform their own thread/start preflight before sending.
        self.set_preferred_workspace_id(Some(workspace_id.clone()));
        self.load_thread_folder_expansion_for_workspace(workspace_id.as_str(), cx);
        self.restore_workspace_thread_state(workspace_id.as_str());
        let target_active_thread_id = self.current_active_thread_id().map(str::to_owned);
        self.rebuild_sidebar_tree_state(cx);
        cx.notify();

        let previous_workspace_id = current_workspace_id;
        if self.gateway.session_refresh_in_flight {
            self.defer_workspace_switch_until_session_refresh(
                workspace_id,
                target_active_thread_id,
                previous_workspace_id,
                cx,
            );
            return;
        }
        self.begin_workspace_switch_transport(
            workspace_id,
            target_active_thread_id,
            previous_workspace_id,
            cx,
        );
    }

    fn begin_workspace_switch_transport(
        &mut self,
        workspace_id: String,
        target_active_thread_id: Option<String>,
        previous_workspace_id: Option<String>,
        cx: &mut Context<Self>,
    ) {
        if self.gateway.connection_state != GatewayConnectionState::Connected {
            self.rollback_optimistic_workspace_switch(
                workspace_id.as_str(),
                previous_workspace_id.as_deref(),
                cx,
            );
            self.set_workspace_action_in_progress(false);
            self.set_workspaces_error(Some(
                t!("workspace.error.gateway_not_connected").to_string(),
            ));
            cx.notify();
            return;
        }
        let Some(connection_id) = self.gateway.ws_connection_id else {
            self.rollback_optimistic_workspace_switch(
                workspace_id.as_str(),
                previous_workspace_id.as_deref(),
                cx,
            );
            self.set_workspace_action_in_progress(false);
            self.set_workspaces_error(Some(
                t!("workspace.error.gateway_not_connected").to_string(),
            ));
            cx.notify();
            return;
        };

        let ws_sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let requested_workspace_id = workspace_id.clone();

            async move {
                let result = cx
                    .background_spawn(async move {
                        let response = ws_sender.workspace_select(
                            workspace_actions::workspace_select_params(
                                requested_workspace_id.clone(),
                                false,
                            ),
                        )?;
                        let restored_thread = target_active_thread_id.map(|thread_id| {
                            let result = ws_sender.thread_start(thread_start::thread_start_params(
                                thread_id.clone(),
                                requested_workspace_id,
                            ));
                            (thread_id, result)
                        });
                        Ok::<_, anyhow::Error>((response, restored_thread))
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if !workspace_actions::workspace_action_result_matches_connection(
                        connection_id,
                        view.gateway.ws_connection_id,
                    ) {
                        view.set_workspace_action_in_progress(false);
                        cx.notify();
                        return;
                    }

                    match result {
                        Ok((response, restored_thread)) => {
                            let workspaces = std::mem::take(&mut view.workspaces);
                            let reduction = workspace_actions::reduce_workspace_switch_success(
                                workspaces,
                                response.workspace,
                            );
                            view.apply_workspace_switch_success_reduction(reduction, cx);

                            if let Some((thread_id, result)) = restored_thread {
                                match result {
                                    Ok(response) => {
                                        let reduction =
                                            thread_start::reduce_thread_start_subscription_success(
                                                response,
                                            );
                                        view.upsert_thread_snapshot(reduction.thread);
                                        view.upsert_thread_for_workspace(
                                            reduction.thread_id.as_str(),
                                            reduction.workspace_id.as_str(),
                                        );
                                        if view.current_active_thread_id()
                                            == Some(thread_id.as_str())
                                        {
                                            view.active_thread_resubscribe_pending = false;
                                            view.reconcile_semantic_timeline_after_reconnect(cx);
                                            view.refresh_desktop_voice_status(cx);
                                        }
                                    }
                                    Err(error) => {
                                        let message = format!("{error:#}");
                                        warn!(
                                            thread_id = thread_id.as_str(),
                                            error = message.as_str(),
                                            "failed to restore thread while switching workspace"
                                        );
                                        // Keep the optimistically selected workspace and thread
                                        // visible. Text and Voice perform an idempotent
                                        // thread/start immediately before their own request, so a
                                        // transient background restore failure must not throw the
                                        // user back to the previous workspace.
                                        view.set_workspaces_error(Some(message));
                                    }
                                }
                            }
                        }
                        Err(error) => {
                            let message = format!("{error:#}");
                            warn!(error = message.as_str(), "failed to switch workspace");
                            view.rollback_optimistic_workspace_switch(
                                workspace_id.as_str(),
                                previous_workspace_id.as_deref(),
                                cx,
                            );
                            view.set_workspaces_error(Some(message));
                        }
                    }

                    view.set_workspace_action_in_progress(false);

                    cx.notify();
                });
            }
        })
        .detach();
    }

    fn defer_workspace_switch_until_session_refresh(
        &mut self,
        workspace_id: String,
        target_active_thread_id: Option<String>,
        previous_workspace_id: Option<String>,
        cx: &mut Context<Self>,
    ) {
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                cx.background_executor()
                    .timer(WORKSPACE_SWITCH_SESSION_RETRY_DELAY)
                    .await;
                let _ = this.update(&mut cx, |view, cx| {
                    if view.gateway.session_refresh_in_flight {
                        view.defer_workspace_switch_until_session_refresh(
                            workspace_id,
                            target_active_thread_id,
                            previous_workspace_id,
                            cx,
                        );
                        return;
                    }
                    view.begin_workspace_switch_transport(
                        workspace_id,
                        target_active_thread_id,
                        previous_workspace_id,
                        cx,
                    );
                });
            }
        })
        .detach();
    }

    fn rollback_optimistic_workspace_switch(
        &mut self,
        requested_workspace_id: &str,
        previous_workspace_id: Option<&str>,
        cx: &mut Context<Self>,
    ) {
        if self.preferred_workspace_id() != Some(requested_workspace_id) {
            return;
        }

        self.set_preferred_workspace_id(previous_workspace_id.map(str::to_owned));
        if let Some(previous_workspace_id) = previous_workspace_id {
            self.load_thread_folder_expansion_for_workspace(previous_workspace_id, cx);
            self.restore_workspace_thread_state(previous_workspace_id);
        } else {
            self.set_active_thread_id(None);
            self.set_draft_thread_id(None);
        }
        self.rebuild_sidebar_tree_state(cx);
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
