use crate::{
    app::root::{GatewayConnectionState, MainContentView, PioneerDesktop},
    gateway::GatewayRuntime,
};
use gpui::{prelude::*, *};
use pioneer_client::{
    skills::actions as skill_actions, skills::catalog as skill_catalog,
    workspaces::selectors as workspace_selectors,
};
use pioneer_protocol::SkillId;
use std::time::Duration;
use tracing::warn;

const SKILLS_POLL_INTERVAL_SECS: u64 = 20;

impl PioneerDesktop {
    pub(in crate::app) fn open_skills_screen_from_bottom_bar(&mut self, cx: &mut Context<Self>) {
        self.selected_skill_target = None;
        self.set_main_content_view(MainContentView::Skills, cx);
        self.ensure_skills_poller(cx);
        self.refresh_installed_skills(cx);
    }

    pub(in crate::app) fn open_skill_from_sidebar(
        &mut self,
        skill_id: SkillId,
        cx: &mut Context<Self>,
    ) {
        let exists = skill_catalog::skill_exists(self.installed_skills.as_slice(), &skill_id);

        if !exists {
            self.skills_error = Some(t!("skills.error.invalid_skill_target").to_string());
            return;
        }

        self.selected_skill_target = Some(skill_id);
        self.set_main_content_view(MainContentView::SkillDetails, cx);
    }

    pub(in crate::app) fn close_skill_details_screen(&mut self, cx: &mut Context<Self>) {
        self.set_main_content_view(MainContentView::Skills, cx);
    }

    pub(in crate::app) fn queue_skills_refresh(&mut self) {
        self.skills_refresh_requested = true;
    }

    pub(in crate::app) fn take_skills_refresh_request(&mut self) -> bool {
        if !self.skills_refresh_requested {
            return false;
        }
        self.skills_refresh_requested = false;
        true
    }

    pub(in crate::app) fn refresh_installed_skills(&mut self, cx: &mut Context<Self>) {
        let scope = match skill_actions::plan_skill_action_scope(
            matches!(
                self.gateway.connection_state,
                GatewayConnectionState::Connected
            ),
            self.gateway.ws_connection_id,
            self.skills_workspace_scope(),
        ) {
            skill_actions::SkillActionScopePlan::Send(scope) => scope,
            skill_actions::SkillActionScopePlan::Unavailable(reason) => {
                self.apply_skills_refresh_unavailable(reason);
                return;
            }
        };
        let connection_id = scope.connection_id;
        let workspace_id = scope.workspace_id;

        self.skills_loading = true;
        self.skills_error = None;

        let ws_sender = self.gateway.ws_command_sender.clone();

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let workspace_id_for_request = workspace_id.clone();
                let result = cx
                    .background_spawn(async move {
                        skill_catalog::load_skills_snapshot(&ws_sender, workspace_id_for_request)
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if view.gateway.ws_connection_id != Some(connection_id) {
                        return;
                    }

                    view.skills_loading = false;

                    match result {
                        Ok(snapshot) => {
                            let reduction = skill_catalog::reduce_skills_catalog_refresh_success(
                                snapshot,
                                std::mem::take(&mut view.skills_pending_actions),
                                view.selected_skill_target.clone(),
                            );
                            view.apply_skills_catalog_refresh_success_reduction(reduction, cx);
                        }
                        Err(error) => {
                            let reduction = skill_catalog::reduce_skills_catalog_refresh_failure(
                                format!("{}: {error:#}", t!("skills.error.load_failed")),
                            );
                            view.apply_skills_catalog_refresh_failure_reduction(reduction);
                            warn!(error = %format!("{error:#}"), "failed to fetch skills snapshot");
                        }
                    }

                    cx.notify();
                });
            }
        })
        .detach();
    }

    pub(in crate::app) fn skills_workspace_scope(&self) -> Option<String> {
        let runtime_workspace_id = self
            .gateway
            .runtime
            .as_ref()
            .and_then(GatewayRuntime::active_workspace_id);
        workspace_selectors::resolve_workspace_scope(
            None,
            self.preferred_workspace_id(),
            runtime_workspace_id,
        )
    }

    fn apply_skills_refresh_unavailable(&mut self, reason: skill_actions::SkillActionUnavailable) {
        self.skills_loading = false;
        self.skills_error = Some(match reason {
            skill_actions::SkillActionUnavailable::GatewayNotConnected => {
                t!("skills.error.gateway_not_connected").to_string()
            }
            skill_actions::SkillActionUnavailable::WorkspaceNotSelected => {
                t!("skills.error.workspace_not_selected").to_string()
            }
        });
    }

    pub(super) fn is_skill_pending(&self, skill_id: &SkillId) -> bool {
        skill_catalog::is_skill_pending(&self.skills_pending_actions, skill_id)
    }

    pub(super) fn mark_skill_pending(&mut self, skill_id: &SkillId, pending: bool) {
        skill_catalog::mark_skill_pending(&mut self.skills_pending_actions, skill_id, pending);
    }

    pub(super) fn apply_skills_catalog_refresh_success_reduction(
        &mut self,
        reduction: skill_catalog::SkillsCatalogRefreshSuccessReduction,
        cx: &mut Context<Self>,
    ) {
        self.skills_catalog = reduction.catalog;
        self.installed_skills = reduction.installed;
        self.skills_health_details = reduction.health_details;
        self.skills_pending_actions = reduction.pending_actions;
        self.selected_skill_target = reduction.selected_target;
        self.skills_error = None;

        if reduction.selected_target_cleared {
            if self.main_content_view == MainContentView::SkillDetails {
                self.set_main_content_view(MainContentView::Skills, cx);
            }
        }
    }

    pub(super) fn apply_skills_catalog_refresh_failure_reduction(
        &mut self,
        reduction: skill_catalog::SkillsCatalogRefreshFailureReduction,
    ) {
        self.skills_error = Some(reduction.error);
    }

    fn ensure_skills_poller(&mut self, cx: &mut Context<Self>) {
        if self.skills_poller_started {
            return;
        }

        self.skills_poller_started = true;
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                loop {
                    Timer::after(Duration::from_secs(SKILLS_POLL_INTERVAL_SECS)).await;

                    let updated = this.update(&mut cx, |view, cx| {
                        if matches!(
                            view.main_content_view,
                            MainContentView::Skills | MainContentView::SkillDetails
                        ) && view.gateway.connection_state == GatewayConnectionState::Connected
                        {
                            view.queue_skills_refresh();
                            cx.notify();
                        }
                    });
                    if updated.is_err() {
                        break;
                    }
                }
            }
        })
        .detach();
    }
}
