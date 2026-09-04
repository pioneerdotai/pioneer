use super::{ADMINISTRATION_CONTENT_INVITATIONS_NODE_ID, ADMINISTRATION_CONTENT_MEMBERS_NODE_ID};
use crate::app::root::{
    AdministrationContentView, GatewayConnectionState, MainContentView, PioneerDesktop,
};
use gpui_kit::component::tree::TreeItem;
use gpui_kit::*;
use std::time::Duration;
use tracing::warn;

const CURRENT_PRINCIPAL_REFRESH_RETRY_DELAYS: [Duration; 4] = [
    Duration::from_millis(100),
    Duration::from_millis(250),
    Duration::from_millis(500),
    Duration::from_secs(1),
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CurrentPrincipalRefreshDecision {
    Complete,
    Retry,
    Stale,
}

fn current_principal_refresh_context_matches(
    expected_generation: u64,
    current_generation: u64,
    expected_connection_id: Option<u64>,
    current_connection_id: Option<u64>,
    expected_workspace_id: Option<&str>,
    current_workspace_id: Option<&str>,
) -> bool {
    expected_generation == current_generation
        && expected_connection_id == current_connection_id
        && expected_workspace_id == current_workspace_id
}

fn retain_verified_auth_after_capability_failure<T>(
    current: Option<T>,
    refreshed: Option<T>,
) -> Option<T> {
    refreshed.or(current)
}

const fn capability_content_requires_threads_fallback(
    content: MainContentView,
    can_manage_capabilities: bool,
    can_use_mcp: bool,
) -> bool {
    match content {
        MainContentView::Mcp => !can_manage_capabilities,
        MainContentView::McpDetails => !can_use_mcp,
        _ => false,
    }
}

impl PioneerDesktop {
    pub(in crate::app) fn refresh_current_principal(&mut self, cx: &mut Context<Self>) {
        self.gateway.current_principal_refresh_generation = self
            .gateway
            .current_principal_refresh_generation
            .wrapping_add(1);
        if self.gateway.connection_state != GatewayConnectionState::Connected {
            self.gateway.current_auth = None;
            self.gateway.capability_snapshot = None;
            self.sync_settings_sidebar_tree_state(cx);
            self.sync_administration_sidebar_tree_state(cx);
            return;
        }
        self.startup
            .begin(pioneer_observability::DesktopStartupStage::AuthorizationLoad);
        let generation = self.gateway.current_principal_refresh_generation;
        let connection_id = self.gateway.ws_connection_id;
        let sender = self.gateway.ws_command_sender.clone();
        let workspace_id = self.active_workspace_id().map(str::to_owned);

        // A workspace-scoped projection must never remain discoverable after
        // the active scope changes. Global bits are fetched again together
        // with the new workspace projection.
        let scope_changed = self
            .gateway
            .capability_snapshot
            .as_ref()
            .is_some_and(|snapshot| {
                snapshot
                    .workspace
                    .as_ref()
                    .map(|workspace| workspace.workspace_id.as_str())
                    != workspace_id.as_deref()
            });
        if scope_changed {
            self.gateway.capability_snapshot = None;
        }
        if self.gateway.capability_snapshot.is_none() {
            self.sync_settings_sidebar_tree_state(cx);
            self.sync_administration_sidebar_tree_state(cx);
            cx.notify();
        }

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                for attempt in 0..=CURRENT_PRINCIPAL_REFRESH_RETRY_DELAYS.len() {
                    let request_sender = sender.clone();
                    let request_workspace_id = workspace_id.clone();
                    let result = cx
                        .background_spawn(async move {
                            let auth = match request_sender.auth_me() {
                                Ok(auth) => auth,
                                Err(error) => return Err((None, error)),
                            };
                            let snapshot = match request_sender.authorization_capabilities(
                                pioneer_protocol::AuthorizationCapabilitiesParams {
                                    workspace_id: request_workspace_id.clone(),
                                    thread_id: None,
                                },
                            ) {
                                Ok(snapshot) => snapshot,
                                Err(error) => return Err((Some(auth), error)),
                            };
                            if !pioneer_client::authorization::authorization_capability_snapshot_is_compatible(
                                &snapshot,
                                &auth.principal.id,
                                request_workspace_id.as_deref(),
                                None,
                            ) {
                                return Err((
                                    Some(auth),
                                    anyhow::anyhow!(
                                        "Gateway returned an incompatible capability snapshot"
                                    ),
                                ));
                            }
                            Ok((auth, snapshot))
                        })
                        .await;

                    let decision = this
                        .update(&mut cx, |view, cx| {
                            if view.gateway.connection_state
                                != GatewayConnectionState::Connected
                                || !current_principal_refresh_context_matches(
                                    generation,
                                    view.gateway.current_principal_refresh_generation,
                                    connection_id,
                                    view.gateway.ws_connection_id,
                                    workspace_id.as_deref(),
                                    view.active_workspace_id(),
                                )
                            {
                                return CurrentPrincipalRefreshDecision::Stale;
                            }

                            match result {
                                Ok((auth, snapshot)) => {
                                    if view.gateway.authorization_projections.accept(snapshot)
                                        != pioneer_client::authorization::AuthorizationProjectionAcceptance::Accepted
                                    {
                                        return CurrentPrincipalRefreshDecision::Retry;
                                    }
                                    view.gateway.current_auth = Some(auth);
                                    view.gateway.authorization_revision = view
                                        .gateway
                                        .authorization_projections
                                        .accepted_revision();
                                    view.gateway.capability_snapshot = view
                                        .gateway
                                        .authorization_projections
                                        .snapshot(workspace_id.as_deref(), None)
                                        .or_else(|| {
                                            view.gateway
                                                .authorization_projections
                                                .snapshot(None, None)
                                        });
                                    view.startup.succeed(
                                        pioneer_observability::DesktopStartupStage::AuthorizationLoad,
                                    );
                                    view.reconcile_composer_permission_mode_with_capabilities();

                                    let capabilities =
                                        view.principal_presentation_capabilities();
                                    if capability_content_requires_threads_fallback(
                                        view.main_content_view,
                                        capabilities.can_manage_capabilities,
                                        capabilities.can_use_mcp,
                                    )
                                    {
                                        view.set_main_content_view(MainContentView::Threads, cx);
                                    }
                                    if !capabilities.can_manage_workspace
                                        && view.main_content_view == MainContentView::AgentsDoc
                                    {
                                        view.active_agents_doc_editor_scope = None;
                                        view.agents_doc_editor = None;
                                        view.set_main_content_view(MainContentView::Threads, cx);
                                    }
                                    view.resolve_current_principal_avatar(cx);
                                    view.refresh_task_user_notifications(cx);
                                    view.sync_settings_sidebar_tree_state(cx);
                                    view.sync_administration_sidebar_tree_state(cx);
                                    if view.main_content_view == MainContentView::Administration {
                                        view.refresh_current_administration_content(cx);
                                    }
                                    cx.notify();
                                    CurrentPrincipalRefreshDecision::Complete
                                }
                                Err((auth, error)) => {
                                    // `auth/me` and the capability projection have different
                                    // failure domains. Preserve a verified identity and any
                                    // still-current projection while a transient snapshot request
                                    // retries; an authorization-epoch invalidation has already
                                    // removed projections that must fail closed.
                                    let principal_changed = auth.as_ref().is_some_and(|auth| {
                                        view.gateway.capability_snapshot.as_ref().is_some_and(
                                            |snapshot| {
                                                snapshot.principal_id != auth.principal.id
                                            },
                                        )
                                    });
                                    if principal_changed {
                                        view.gateway.capability_snapshot = None;
                                        view.sync_settings_sidebar_tree_state(cx);
                                        view.sync_administration_sidebar_tree_state(cx);
                                    }
                                    view.gateway.current_auth =
                                        retain_verified_auth_after_capability_failure(
                                            view.gateway.current_auth.take(),
                                            auth,
                                        );
                                    warn!(
                                        attempt,
                                        error = %format!("{error:#}"),
                                        "current principal capability refresh failed"
                                    );
                                    cx.notify();
                                    CurrentPrincipalRefreshDecision::Retry
                                }
                            }
                        })
                        .unwrap_or(CurrentPrincipalRefreshDecision::Stale);

                    match decision {
                        CurrentPrincipalRefreshDecision::Complete
                        | CurrentPrincipalRefreshDecision::Stale => return,
                        CurrentPrincipalRefreshDecision::Retry
                            if attempt < CURRENT_PRINCIPAL_REFRESH_RETRY_DELAYS.len() =>
                        {
                            cx.background_executor()
                                .timer(CURRENT_PRINCIPAL_REFRESH_RETRY_DELAYS[attempt])
                                .await;
                        }
                        CurrentPrincipalRefreshDecision::Retry => {
                            let _ = this.update(&mut cx, |view, _| {
                                view.startup.fail(
                                    pioneer_observability::DesktopStartupStage::AuthorizationLoad,
                                );
                            });
                            return;
                        }
                    }
                }
            }
        })
        .detach();
    }

    pub(in crate::app) fn open_administration_screen_from_bottom_bar(
        &mut self,
        cx: &mut Context<Self>,
    ) {
        self.sync_administration_sidebar_tree_state(cx);
        self.set_main_content_view(MainContentView::Administration, cx);
        self.refresh_current_administration_content(cx);
    }

    pub(in crate::app) fn open_administration_content(
        &mut self,
        content_view: AdministrationContentView,
        cx: &mut Context<Self>,
    ) {
        self.administration_content_view = content_view;
        self.sync_administration_sidebar_tree_state(cx);
        self.set_main_content_view(MainContentView::Administration, cx);
        self.refresh_current_administration_content(cx);
    }

    pub(in crate::app) fn sync_administration_sidebar_tree_state(
        &mut self,
        cx: &mut Context<Self>,
    ) {
        let capabilities = self.principal_presentation_capabilities();
        let mut items = Vec::with_capacity(2);
        if capabilities.can_view_member_directory {
            items.push((
                AdministrationContentView::Members,
                TreeItem::new(ADMINISTRATION_CONTENT_MEMBERS_NODE_ID, "members"),
            ));
        }
        if capabilities.can_view_invitations {
            items.push((
                AdministrationContentView::Invitations,
                TreeItem::new(ADMINISTRATION_CONTENT_INVITATIONS_NODE_ID, "invitations"),
            ));
        }

        if !items
            .iter()
            .any(|(content_view, _)| *content_view == self.administration_content_view)
        {
            self.administration_content_view = items
                .first()
                .map(|(content_view, _)| *content_view)
                .unwrap_or(AdministrationContentView::Members);
        }

        let selected_ix = items
            .iter()
            .position(|(content_view, _)| *content_view == self.administration_content_view);
        let administration_tree_state = self.administration_tree_state.clone();
        administration_tree_state.update(cx, |state, cx| {
            state.set_items(
                items.into_iter().map(|(_, item)| item).collect::<Vec<_>>(),
                cx,
            );
            state.set_selected_index(selected_ix, cx);
        });
    }

    pub(in crate::app) fn refresh_current_administration_content(
        &mut self,
        cx: &mut Context<Self>,
    ) {
        match self.administration_content_view {
            AdministrationContentView::Members => {
                self.refresh_members(false, cx);
                self.refresh_all_workspace_members(cx);
            }
            AdministrationContentView::Invitations => self.refresh_invitations(false, cx),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        capability_content_requires_threads_fallback, current_principal_refresh_context_matches,
        retain_verified_auth_after_capability_failure,
    };
    use crate::app::root::MainContentView;

    #[test]
    fn principal_refresh_rejects_stale_generation_connection_and_workspace() {
        assert!(current_principal_refresh_context_matches(
            7,
            7,
            Some(11),
            Some(11),
            Some("workspace-a"),
            Some("workspace-a"),
        ));
        assert!(!current_principal_refresh_context_matches(
            7,
            8,
            Some(11),
            Some(11),
            Some("workspace-a"),
            Some("workspace-a"),
        ));
        assert!(!current_principal_refresh_context_matches(
            7,
            7,
            Some(11),
            Some(12),
            Some("workspace-a"),
            Some("workspace-a"),
        ));
        assert!(!current_principal_refresh_context_matches(
            7,
            7,
            Some(11),
            Some(11),
            Some("workspace-a"),
            Some("workspace-b"),
        ));
    }

    #[test]
    fn capability_failure_preserves_verified_auth_and_prefers_a_refresh() {
        assert_eq!(
            retain_verified_auth_after_capability_failure(Some("current"), None),
            Some("current")
        );
        assert_eq!(
            retain_verified_auth_after_capability_failure(Some("current"), Some("refreshed")),
            Some("refreshed")
        );
    }

    #[test]
    fn operational_member_keeps_timeline_mcp_details_but_not_management_inventory() {
        assert!(capability_content_requires_threads_fallback(
            MainContentView::Mcp,
            false,
            true,
        ));
        assert!(!capability_content_requires_threads_fallback(
            MainContentView::McpDetails,
            false,
            true,
        ));
        assert!(capability_content_requires_threads_fallback(
            MainContentView::McpDetails,
            false,
            false,
        ));
    }
}
