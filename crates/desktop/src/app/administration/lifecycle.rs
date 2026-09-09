use crate::app::root::{
    GatewayConnectionState, MainContentView, PioneerDesktop,
};
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

            return;
        }
        self.startup
            .begin(pioneer_observability::DesktopStartupStage::AuthorizationLoad);
        let generation = self.gateway.current_principal_refresh_generation;
        let connection_id = self.gateway.ws_connection_id;
        let client_core = self.gateway.client_runtime.client_core().clone();
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
        let reload_protected_content = self.gateway.capability_snapshot.is_none();
        if reload_protected_content {
            self.sync_settings_sidebar_tree_state(cx);

            cx.notify();
        }

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                for attempt in 0..=CURRENT_PRINCIPAL_REFRESH_RETRY_DELAYS.len() {
                    let request_core = client_core.clone();
                    let request_workspace_id = workspace_id.clone();
                    #[cfg(feature = "qualification-diagnostics")]
                    if attempt > 0 {
                        pioneer_observability::record_qualification_diagnostic!(
                            record_animation_activity(
                                pioneer_observability::AnimationSourceId::CurrentPrincipalRefreshRetry,
                                pioneer_observability::DiagnosticAction::Requested,
                                pioneer_observability::Visibility::NotApplicable,
                            )
                        );
                    }
                    let result = cx
                        .background_spawn(async move {
                            request_core.refresh_identity_authorization(pioneer_protocol::AuthorizationCapabilitiesParams {
                                workspace_id: request_workspace_id.clone(), thread_id: None,
                            })
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
                                Ok((_auth, _snapshot)) => {
                                    view.gateway.current_auth = view.gateway.client_runtime.client_core().current_auth();

                                    view.gateway.capability_snapshot = view
                                        .gateway
                                        .client_runtime.client_core().authorization_snapshot(workspace_id.as_deref(), None)
                                        .or_else(|| {
                                            view.gateway
                                                .client_runtime.client_core().authorization_snapshot(None, None)
                                        });
                                    view.startup.succeed(
                                        pioneer_observability::DesktopStartupStage::AuthorizationLoad,
                                    );


                                    let capabilities =
                                        view.principal_presentation_capabilities();
                                    if capability_content_requires_threads_fallback(
                                        view.main_content_view(),
                                        capabilities.can_manage_capabilities,
                                        capabilities.can_use_mcp,
                                    )
                                    {
                                        view.set_main_content_view(MainContentView::Threads, cx);
                                    }
                                    if !capabilities.can_manage_workspace
                                        && view.main_content_view() == MainContentView::AgentsDoc
                                    {
                                        view.active_agents_doc_editor_scope = None;
                                        view.agents_doc_editor = None;
                                        view.set_main_content_view(MainContentView::Threads, cx);
                                    }
                                    view.resolve_current_principal_avatar(cx);
                                    view.sync_settings_sidebar_tree_state(cx);

                                    if reload_protected_content && view.gateway.capability_snapshot.is_some() {
                                        match view.main_content_view() {
                                            MainContentView::Providers => {
                                                view.refresh_configured_providers(cx);
                                                view.load_cli_provider_snapshot(cx);
                                            }
                                            MainContentView::Skills | MainContentView::SkillDetails
                                            | MainContentView::Mcp | MainContentView::McpDetails => {
                                                view.refresh_workspace_bound_screens_after_switch(cx);
                                            }
                                            _ => {}
                                        }
                                    }
                                    cx.notify();
                                    CurrentPrincipalRefreshDecision::Complete
                                }
                                Err((_auth, error)) => {
                                    let auth = view.gateway.client_runtime.client_core().current_auth();
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

                                    }
                                    view.gateway.current_auth = auth;
                                    view.gateway.capability_snapshot = view.gateway.client_runtime.client_core()
                                        .authorization_snapshot(workspace_id.as_deref(), None)
                                        .or_else(|| view.gateway.client_runtime.client_core().authorization_snapshot(None, None));
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
                        #[cfg(not(feature = "qualification-diagnostics"))]
                        CurrentPrincipalRefreshDecision::Complete
                        | CurrentPrincipalRefreshDecision::Stale => return,
                        #[cfg(feature = "qualification-diagnostics")]
                        CurrentPrincipalRefreshDecision::Complete => {
                            if attempt > 0 {
                                pioneer_observability::record_qualification_diagnostic!(
                                    record_animation_activity(
                                        pioneer_observability::AnimationSourceId::CurrentPrincipalRefreshRetry,
                                        pioneer_observability::DiagnosticAction::Completed,
                                        pioneer_observability::Visibility::NotApplicable,
                                    )
                                );
                            }
                            return;
                        }
                        #[cfg(feature = "qualification-diagnostics")]
                        CurrentPrincipalRefreshDecision::Stale => {
                            if attempt > 0 {
                                pioneer_observability::record_qualification_diagnostic!(
                                    record_animation_activity(
                                        pioneer_observability::AnimationSourceId::CurrentPrincipalRefreshRetry,
                                        pioneer_observability::DiagnosticAction::Cancelled,
                                        pioneer_observability::Visibility::NotApplicable,
                                    )
                                );
                            }
                            return;
                        }
                        CurrentPrincipalRefreshDecision::Retry
                            if attempt < CURRENT_PRINCIPAL_REFRESH_RETRY_DELAYS.len() =>
                        {
                            pioneer_observability::record_qualification_diagnostic!(
                                record_animation_activity(
                                    pioneer_observability::AnimationSourceId::CurrentPrincipalRefreshRetry,
                                    pioneer_observability::DiagnosticAction::Scheduled,
                                    pioneer_observability::Visibility::NotApplicable,
                                )
                            );
                            cx.background_executor()
                                .timer(CURRENT_PRINCIPAL_REFRESH_RETRY_DELAYS[attempt])
                                .await;
                            pioneer_observability::record_qualification_diagnostic!(
                                record_animation_activity(
                                    pioneer_observability::AnimationSourceId::CurrentPrincipalRefreshRetry,
                                    pioneer_observability::DiagnosticAction::Woke,
                                    pioneer_observability::Visibility::NotApplicable,
                                )
                            );
                        }
                        CurrentPrincipalRefreshDecision::Retry => {
                            let _ = this.update(&mut cx, |view, _| {
                                view.startup.fail(
                                    pioneer_observability::DesktopStartupStage::AuthorizationLoad,
                                );
                            });
                            #[cfg(feature = "qualification-diagnostics")]
                            if attempt > 0 {
                                pioneer_observability::record_qualification_diagnostic!(
                                    record_animation_activity(
                                        pioneer_observability::AnimationSourceId::CurrentPrincipalRefreshRetry,
                                        pioneer_observability::DiagnosticAction::Completed,
                                        pioneer_observability::Visibility::NotApplicable,
                                    )
                                );
                            }
                            return;
                        }
                    }
                }
            }
        })
        .detach();
    }

    pub(in crate::app) fn open_administration_screen_from_bottom_bar(&mut self, cx: &mut Context<Self>) {
        self.set_main_content_view(MainContentView::Administration, cx);
    }
}

#[cfg(test)]
mod tests {
    #[gpui_kit::test]
    fn pending_permissions_keep_administration_and_settings_routes(
        cx: &mut gpui_kit::TestAppContext,
    ) {
        use gpui_kit::{App, AppContext};
        use pioneer_client::navigation::{
            AdministrationRoute, NavigationIntent, SemanticDestination, SettingsRoute,
        };
        cx.update(gpui_kit::init);
        cx.update(crate::client_runtime::DesktopRuntimeCoordinator::install_for_test);
        let core = cx.update(|cx: &mut App| {
            cx.global::<crate::client_runtime::DesktopRuntimeCoordinator>()
                .core()
        });
        let (root, cx) = cx.add_window_view(|window, cx| {
            let registrar = cx
                .global::<crate::client_runtime::DesktopRuntimeCoordinator>()
                .registrar();
            let navigation =
                crate::desktop_navigation::DesktopNavigationStore::new(registrar.as_ref());
            let layout = cx.new(|cx| crate::shell_state::ShellStateStore::new(window, cx));
            let desktop = cx.new(|cx| {
                crate::app::LegacyScreenAdapter::new(
                    window,
                    cx,
                    pioneer_observability::DesktopStartupTrace::start(),
                    navigation,
                    layout,
                )
            });
            gpui_kit::component::Root::new(desktop, window, cx)
        });
        for destination in [
            SemanticDestination::Administration {
                route: AdministrationRoute::Invitations,
            },
            SemanticDestination::Settings {
                route: SettingsRoute::Memory,
            },
        ] {
            core.navigate(
                NavigationIntent::Navigate {
                    destination: destination.clone(),
                },
                None,
            );
            cx.update(|_, cx| {
                let desktop = root
                    .read(cx)
                    .view()
                    .clone()
                    .downcast::<crate::app::LegacyScreenAdapter>()
                    .unwrap();
                desktop.update(cx, |view, cx| {
                    view.apply_navigation_publication(core.navigation_snapshot(), cx);
                    view.gateway.capability_snapshot = None;

                    view.sync_settings_sidebar_tree_state(cx);
                });
            });
            assert_eq!(core.navigation_snapshot().destination(), &destination);
        }
    }

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
