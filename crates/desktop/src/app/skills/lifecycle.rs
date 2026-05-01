use crate::{
    app::root::{GatewayConnectionState, MainContentView, PioneerDesktop},
    gateway::GatewayRuntime,
};
use gpui::{prelude::*, *};
use pioneer_protocol::{
    SkillHealthItem, SkillHealthTarget, SkillListItem, SkillListParams, SkillsHealthParams,
};
use std::collections::{HashMap, HashSet};
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
        let exists = self
            .installed_skills
            .iter()
            .any(|skill| skill.slug == slug && skill.source_kind == source_kind);

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
                        let list = ws_sender.skills_list(SkillListParams {
                            workspace_id: workspace_id_for_request.clone(),
                            include_health: true,
                            include_policy: true,
                        })?;

                        let targets = list
                            .skills
                            .iter()
                            .map(|skill| SkillHealthTarget {
                                slug: skill.slug.clone(),
                                source_kind: skill.source_kind.clone(),
                            })
                            .collect::<Vec<_>>();

                        let health_items = if targets.is_empty() {
                            Vec::new()
                        } else {
                            ws_sender
                                .skills_health(SkillsHealthParams {
                                    workspace_id: workspace_id_for_request,
                                    skills: targets,
                                    audit_limit: 16,
                                })?
                                .skills
                        };

                        Ok::<_, anyhow::Error>((list.skills, health_items))
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if view.gateway.ws_connection_id != Some(connection_id) {
                        return;
                    }

                    view.skills_loading = false;

                    match result {
                        Ok((catalog, health_items)) => {
                            view.apply_skills_snapshot(catalog, health_items, cx);
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

    pub(super) fn skills_workspace_scope(&self) -> Option<String> {
        self.preferred_workspace_id()
            .map(str::to_owned)
            .or_else(|| {
                self.gateway
                    .runtime
                    .as_ref()
                    .and_then(GatewayRuntime::active_workspace_id)
                    .map(str::to_owned)
            })
            .and_then(|workspace_id| {
                let trimmed = workspace_id.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed.to_owned())
                }
            })
    }

    pub(super) fn skill_key(slug: &str, source_kind: &str) -> String {
        format!("{slug}::{source_kind}")
    }

    pub(super) fn is_skill_pending(&self, slug: &str, source_kind: &str) -> bool {
        self.skills_pending_actions
            .contains(Self::skill_key(slug, source_kind).as_str())
    }

    pub(super) fn mark_skill_pending(&mut self, slug: &str, source_kind: &str, pending: bool) {
        let key = Self::skill_key(slug, source_kind);
        if pending {
            self.skills_pending_actions.insert(key);
        } else {
            self.skills_pending_actions.remove(key.as_str());
        }
    }

    pub(super) fn apply_skills_snapshot(
        &mut self,
        catalog: Vec<SkillListItem>,
        health_items: Vec<SkillHealthItem>,
        cx: &mut Context<Self>,
    ) {
        let (catalog, installed_skills) = derive_skills_catalog_and_installed(catalog);

        let health_details = health_items
            .into_iter()
            .map(|item| {
                (
                    Self::skill_key(item.slug.as_str(), item.source_kind.as_str()),
                    item,
                )
            })
            .collect::<HashMap<_, _>>();

        let catalog_keys = catalog
            .iter()
            .map(|skill| Self::skill_key(skill.slug.as_str(), skill.source_kind.as_str()))
            .collect::<HashSet<_>>();

        self.skills_pending_actions
            .retain(|key| catalog_keys.contains(key));

        self.skills_catalog = catalog;
        self.installed_skills = installed_skills;
        self.skills_health_details = health_details;

        if let Some((slug, source_kind)) = self.selected_skill_target.as_ref() {
            let still_present = self
                .installed_skills
                .iter()
                .any(|skill| skill.slug == *slug && skill.source_kind == *source_kind);
            if !still_present {
                self.selected_skill_target = None;
                if self.main_content_view == MainContentView::SkillDetails {
                    self.set_main_content_view(MainContentView::Skills, cx);
                }
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

fn derive_skills_catalog_and_installed(
    mut catalog: Vec<SkillListItem>,
) -> (Vec<SkillListItem>, Vec<SkillListItem>) {
    catalog.sort_by(|left, right| {
        left.source_kind
            .cmp(&right.source_kind)
            .then_with(|| left.slug.cmp(&right.slug))
    });

    let installed = catalog
        .iter()
        .filter(|skill| skill.install.installed)
        .cloned()
        .collect::<Vec<_>>();
    (catalog, installed)
}
