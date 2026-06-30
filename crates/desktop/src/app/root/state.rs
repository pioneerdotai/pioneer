use super::*;
use crate::app::skills::details::table::SkillDiagnosticsTableDelegate;
use crate::{state, window};
use gpui_component::table::TableState;
use pioneer_client::composer::{
    model_selection::default_composer_turn_mode, permissions::default_composer_permission_mode,
};

impl PioneerDesktop {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        window::install_window_state_persistence(window, cx);

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
        let composer_state = cx.new(|cx| {
            InputState::new(window, cx)
                .multi_line(true)
                .auto_grow(2, 13)
                .placeholder(t!("chat.composer.placeholder").to_string())
        });
        let thread_tree_state = cx.new(|cx| TreeState::new(cx));
        let settings_tree_state = cx.new(|cx| TreeState::new(cx));
        let provider_tree_state = cx.new(|cx| TreeState::new(cx));
        let skills_audit_table_state = cx.new(|cx| {
            TableState::new(
                SkillDiagnosticsTableDelegate::new("skills-audit-table"),
                window,
                cx,
            )
            .row_selectable(false)
            .col_selectable(false)
            .sortable(false)
            .col_movable(false)
            .col_resizable(false)
            .loop_selection(false)
        });
        let mcp_audit_table_state = cx.new(|cx| {
            TableState::new(
                SkillDiagnosticsTableDelegate::new("mcp-audit-table"),
                window,
                cx,
            )
            .row_selectable(false)
            .col_selectable(false)
            .sortable(false)
            .col_movable(false)
            .col_resizable(false)
            .loop_selection(false)
        });
        let client_runtime = ClientRuntime::new();
        let gateway_ws_command_sender = client_runtime.ws_command_sender();
        if let Ok(runtime_home) = state::runtime_home_dir() {
            let _ = gateway_ws_command_sender.set_artifact_cache_root(runtime_home);
        }

        let mut view = Self {
            thread_coordinators: HashMap::new(),
            thread_folders: HashMap::new(),
            thread_placements: HashMap::new(),
            thread_agents_doc_summaries: HashMap::new(),
            active_agents_doc_editor_scope: None,
            agents_doc_editor: None,
            thread_folder_expanded: state::thread_folders_expanded_for_workspace(cx, None),
            thread_tree_selected_node_id: None,
            thread_tree_state,
            settings_content_view: SettingsContentView::General,
            remote_access_settings_expanded: false,
            remote_access_key_input_revision: 0,
            remote_access_status_poll_generation: 0,
            settings_tree_state,
            provider_tree_state,
            thread_list_loading: false,
            thread_list_refresh_requested: false,
            active_thread_id: None,
            draft_thread_id: None,
            task_thread_navigation_stack: Vec::new(),
            preferred_workspace_id: None,
            workspaces: Vec::new(),
            workspaces_loading: false,
            workspaces_error: None,
            workspace_action_in_progress: false,
            last_active_thread_by_workspace: HashMap::new(),
            draft_thread_by_workspace: HashMap::new(),
            composer_state,
            composer_attachments: Vec::new(),
            composer_capabilities: Vec::new(),
            composer_upload_in_progress: false,
            composer_upload_error: None,
            composer_turn_mode: default_composer_turn_mode(),
            composer_selected_provider: None,
            composer_selected_model: None,
            composer_selected_reasoning_effort: None,
            composer_permission_mode: default_composer_permission_mode(),
            composer_model_selection_manually_selected: false,
            composer_model_display_cache: HashMap::new(),
            composer_model_display_loading_key: None,
            main_content_view: MainContentView::Threads,
            providers: Default::default(),
            pending_requests: Default::default(),
            cli_runtime_thread_bindings: HashMap::new(),
            mcp_servers: Vec::new(),
            mcp_selected_server_id: None,
            mcp_server_details: None,
            mcp_loading: false,
            mcp_details_loading: false,
            mcp_error: None,
            mcp_refresh_requested: false,
            mcp_details_refresh_requested: false,
            mcp_poller_started: false,
            mcp_pending_actions: HashSet::new(),
            mcp_list_scroll_handle: VirtualListScrollHandle::new(),
            mcp_details_expanded_sections: HashSet::new(),
            mcp_audit_table_state,
            installed_skills: Vec::new(),
            skills_catalog: Vec::new(),
            skills_health_details: HashMap::new(),
            skills_loading: false,
            skills_error: None,
            skills_upload_progress: None,
            skills_upload_cancel_token: None,
            skills_refresh_requested: false,
            skills_poller_started: false,
            skills_pending_actions: HashSet::new(),
            selected_skill_target: None,
            skills_list_scroll_handle: VirtualListScrollHandle::new(),
            skills_details_expanded_sections: HashSet::new(),
            skills_audit_table_state,
            thread_drafts: HashMap::new(),
            thread_draft_attachments: HashMap::new(),
            thread_draft_capabilities: HashMap::new(),
            thread_draft_permission_modes: HashMap::new(),
            thread_start: ThreadStartCoordinator::default(),
            thread_start_requested: false,
            thread_timeline_scroll_handle: VirtualListScrollHandle::new(),
            thread_timeline_view_state: RefCell::new(ThreadTimelineViewState::default()),
            thread_timeline_item_expanded: RefCell::new(HashSet::new()),
            thread_timeline_terminal_item: RefCell::new(HashMap::new()),
            semantic_timelines: SemanticTimelineState::default(),
            semantic_timeline_revision: 0,
            semantic_timeline_in_flight: HashSet::new(),
            task_review_actions: TaskReviewActionState::default(),
            thread_artifacts: ThreadArtifactsState::default(),
            show_thread_artifacts_sidebar: false,
            thread_artifacts_sidebar_width: px(340.),
            ready_turn_resume_threads: VecDeque::new(),
            ready_turn_resume_thread_set: HashSet::new(),
            gateway_setup_form_state,
            show_sidebar: true,
            sidebar_panel_width: px(320.),
            gateway: GatewayCoordinator {
                runtime: None,
                client_runtime,
                ws_command_sender: gateway_ws_command_sender,
                ws_connection_id: None,
                connection_epoch: 0,
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
            },
        };

        cx.observe_window_bounds(window, |view, _, cx| {
            {
                let mut state = view.thread_timeline_view_state.borrow_mut();
                state.pending_width_probe = true;
                state.width_probe_attempts = 0;
            }
            cx.notify();
        })
        .detach();

        view.sync_settings_sidebar_tree_state(cx);
        view.sync_provider_sidebar_tree_state(cx);
        view.start_gateway_ws_event_pump(cx);
        view.bootstrap_gateway_runtime(cx);
        view.prune_thread_artifact_preview_cache(cx);

        view
    }
}
