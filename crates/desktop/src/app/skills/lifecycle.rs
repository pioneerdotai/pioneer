use crate::{
    app::root::{GatewayConnectionState, MainContentView, PioneerDesktop},
    gateway::GatewayRuntime,
};
use gpui::{prelude::*, *};
use pioneer_client::{
    skills::catalog as skill_catalog, workspaces::selectors as workspace_selectors,
};
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
        slug: String,
        source_kind: String,
        cx: &mut Context<Self>,
    ) {
        let exists = skill_catalog::skill_exists(
            self.installed_skills.as_slice(),
            slug.as_str(),
            source_kind.as_str(),
        );

        if !exists {
            self.skills_error = Some(t!("skills.error.invalid_skill_target").to_string());
            return;
        }

        self.selected_skill_target = Some((slug, source_kind));
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
        if self.gateway.connection_state != GatewayConnectionState::Connected {
            self.skills_loading = false;
            self.skills_error = Some(t!("skills.error.gateway_not_connected").to_string());
            return;
        }

        let Some(connection_id) = self.gateway.ws_connection_id else {
            self.skills_loading = false;
            self.skills_error = Some(t!("skills.error.gateway_not_connected").to_string());
            return;
        };

        let Some(workspace_id) = self.skills_workspace_scope() else {
            self.skills_loading = false;
            self.skills_error = Some(t!("skills.error.workspace_not_selected").to_string());
            return;
        };

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
                            view.apply_skills_snapshot(snapshot, cx);
                            view.skills_error = None;
                        }
                        Err(error) => {
                            view.skills_error =
                                Some(format!("{}: {error:#}", t!("skills.error.load_failed")));
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

    pub(super) fn is_skill_pending(&self, slug: &str, source_kind: &str) -> bool {
        skill_catalog::is_skill_pending(&self.skills_pending_actions, slug, source_kind)
    }

    pub(super) fn mark_skill_pending(&mut self, slug: &str, source_kind: &str, pending: bool) {
        skill_catalog::mark_skill_pending(
            &mut self.skills_pending_actions,
            slug,
            source_kind,
            pending,
        );
    }

    pub(super) fn apply_skills_snapshot(
        &mut self,
        snapshot: skill_catalog::SkillsCatalogSnapshot,
        cx: &mut Context<Self>,
    ) {
        let reconciled = skill_catalog::reconcile_skills_snapshot(
            snapshot,
            &mut self.skills_pending_actions,
            self.selected_skill_target.clone(),
        );
        let snapshot = reconciled.snapshot;

        self.skills_catalog = snapshot.catalog;
        self.installed_skills = snapshot.installed;
        self.skills_health_details = snapshot.health_details;
        self.selected_skill_target = reconciled.selected_target;

        if reconciled.selected_target_cleared {
            if self.main_content_view == MainContentView::SkillDetails {
                self.set_main_content_view(MainContentView::Skills, cx);
            }
        }
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
