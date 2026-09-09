use super::*;
use crate::components::member_picker::{MemberPickerDelegate, new_member_picker_state};
use crate::state;
use gpui_kit::component::table::TableState;

impl PioneerDesktop {
    pub fn new(
        window: &mut Window,
        cx: &mut Context<Self>,
        startup_trace: pioneer_observability::DesktopStartupTrace,
        navigation: Arc<crate::desktop_navigation::DesktopNavigationStore>,
        shell_state: Entity<crate::shell_state::ShellStateStore>,
    ) -> Self {
        let gateway_setup_form_state = cx.new(|cx| {
            GatewaySetupFormState::new(
                window,
                cx,
                GatewaySetupDialogState::new(
                    true,
                    None,
                    None,
                    t!("gateway.status.connecting").to_string(),
                ),
            )
        });
        let settings_tree_state = cx.new(|cx| TreeState::new(cx));
        crate::client_runtime::DesktopRuntimeCoordinator::install(cx);
        let client_runtime = ClientRuntime::from_core(
            cx.global::<crate::client_runtime::DesktopRuntimeCoordinator>()
                .core(),
        );
        let session_binding = crate::gateway::GatewaySessionBinding::new(
            cx.global::<crate::client_runtime::DesktopRuntimeCoordinator>()
                .registrar()
                .as_ref(),
        );
        let desktop = cx.weak_entity();
        let switcher_view = cx.new(|cx| {
            crate::app::flow::GatewaySwitcherView::new(desktop.clone(), session_binding.clone(), cx)
        });

        let setup_view = cx.new(|cx| {
            crate::app::initial::InitialGatewaySetupView::new(desktop, session_binding.clone(), cx)
        });

        let providers_view = pioneer_desktop_providers::ProviderCatalogView::new(
            crate::app::providers::provider_config(client_runtime.client_core().clone(), cx), window, cx);
        let width = if shell_state.read(cx).sidebar_visible() { shell_state.read(cx).sidebar_width() } else { px(0.) };
        providers_view.update(cx, |view, cx| view.set_sidebar_width(width, cx));
        let provider_layout = providers_view.downgrade();
        let providers_layout_subscription = cx.observe(&shell_state, move |_, layout, cx| {
            let width = if layout.read(cx).sidebar_visible() { layout.read(cx).sidebar_width() } else { px(0.) };
            let _ = provider_layout.update(cx, |view, cx| view.set_sidebar_width(width, cx));
        });
        let registrar = cx
            .global::<crate::client_runtime::DesktopRuntimeCoordinator>()
            .registrar();
        let mcp_view = pioneer_desktop_mcp::McpCatalogView::new(
            pioneer_desktop_mcp::McpCatalogConfig::new(
                client_runtime.client_core().clone(),
                registrar.clone(),
            ),
            window,
            cx,
        );
        let skills_view = pioneer_desktop_skills::SkillsCatalogView::new(
            pioneer_desktop_skills::SkillsCatalogConfig::new(
                client_runtime.client_core().clone(),
                registrar,
            ),
            window,
            cx,
        );
        mcp_view.update(cx, |view, cx| view.set_sidebar_width(width, cx));
        skills_view.update(cx, |view, cx| view.set_sidebar_width(width, cx));
        let mcp_layout = mcp_view.downgrade();
        let skills_layout = skills_view.downgrade();
        let catalog_layout_subscription = cx.observe(&shell_state, move |_, layout, cx| {
            let width = if layout.read(cx).sidebar_visible() {
                layout.read(cx).sidebar_width()
            } else {
                px(0.)
            };
            let _ = mcp_layout.update(cx, |view, cx| view.set_sidebar_width(width, cx));
            let _ = skills_layout.update(cx, |view, cx| view.set_sidebar_width(width, cx));
        });
        let mut view = Self {
            window_active: window.is_window_active(),
            frame_presentation: None,
            navigation_input: navigation.snapshot().navigation().clone(),
            navigation,
            shell_state,
            startup: DesktopStartupCoordinator::new(
                startup_trace,
                client_runtime.client_core().clone(),
            ),
            invitation_join: None,
            invitation_join_input_subscriptions: Vec::new(),
            active_agents_doc_editor_scope: None,
            agents_doc_editor: None,
            profile_editor: None,
            profile_editor_input_subscriptions: Vec::new(),
            administration_view: pioneer_desktop_administration::AdministrationView::new(
                crate::app::administration::administration_config(client_runtime.client_core().clone(), cx), window, cx),
            member_avatar_state: DesktopMemberAvatarState::new(
                cx.global::<crate::client_runtime::DesktopRuntimeCoordinator>()
                    .core(),
                cx.global::<crate::client_runtime::DesktopRuntimeCoordinator>()
                    .registrar(),
                cx,
            ),
            voice_input_action_error: None,
            voice_input_action_generation: 0,
            pending_voice_input_enabled: None,
            remote_access_settings_expanded: false,
            remote_access_key_input_revision: 0,
            remote_access_status_poll_generation: 0,
            self_improvement_status_poll: None,
            settings_tree_state,

            active_thread_resubscribe_pending: false,
            task_notification_surface: None,
            workspace_catalog_input: Default::default(),
            providers_view,
            _providers_layout_subscription: providers_layout_subscription,
            mcp_view,
            skills_view,
            _catalog_layout_subscription: catalog_layout_subscription,
            pending_thread_create_visibility: ThreadVisibility::Private,
            gateway_setup_form_state,
            gateway: GatewayCoordinator {
                compatibility_task: None,
                settings_task: None,
                settings_binding: crate::gateway::GatewaySettingsBinding::new(
                    cx.global::<crate::client_runtime::DesktopRuntimeCoordinator>()
                        .registrar()
                        .as_ref(),
                ),
                identity_task: None,
                session_task: None,
                transport_verification_task: None,
                transport_verification_id: None,
                applied_transport_revision: 0,
                identity_binding: crate::gateway::IdentityAuthorizationBinding::new(
                    cx.global::<crate::client_runtime::DesktopRuntimeCoordinator>()
                        .registrar()
                        .as_ref(),
                ),
                session_binding,
                switcher_view,
                setup_view,
                runtime: None,
                client_runtime,
                http_client: None,
                ws_connection_id: None,
                current_principal_refresh_generation: 0,
                connection_state: GatewayConnectionState::Connecting,
                status: t!("gateway.status.connecting").to_string(),
                status_level: GatewayStatusLevel::Neutral,
                error: None,
                connecting: true,
                setup_action: None,
                bootstrap_complete: false,
                settings: None,
                settings_loading: false,
                settings_error: None,
                auth_session_action_error: None,
                auth_session_action_pending: None,
                current_auth: None,
                capability_snapshot: None,
            },
        };

        cx.observe_window_activation(window, |view, window, cx| {
            if window.is_window_active() {
                view.recover_gateway_session_on_foreground(cx);
            }
        })
        .detach();

        cx.observe_in(&cx.entity(), window, |view, _, window, cx| {
            view.reconcile_desktop_startup_readiness(window, cx);
            view.publish_frame_changes(cx);
        })
        .detach();
        cx.defer_in(window, |view, window, cx| {
            view.reconcile_desktop_startup_readiness(window, cx)
        });
        view.sync_settings_sidebar_tree_state(cx);

        view.start_gateway_ws_event_pump(window, cx);
        view.bootstrap_gateway_runtime(cx);

        view
    }
}
