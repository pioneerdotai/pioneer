use super::*;
use crate::app::skills::details::table::SkillDiagnosticsTableDelegate;
use crate::components::member_picker::{MemberPickerDelegate, new_member_picker_state};
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
        let composer_mention_select = cx.new(|cx| new_member_picker_state(window, cx));
        let thread_member_select = cx.new(|cx| new_member_picker_state(window, cx));
        let thread_tree_state = cx.new(|cx| TreeState::new(cx));
        let settings_tree_state = cx.new(|cx| TreeState::new(cx));
        let administration_tree_state = cx.new(|cx| TreeState::new(cx));
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
        let desktop_microphone_gate = DesktopMicrophoneGateReport::unknown();

        let mut view = Self {
            invitation_join: None,
            invitation_join_input_subscriptions: Vec::new(),
            thread_coordinators: HashMap::new(),
            thread_unread: HashMap::new(),
            thread_folders: HashMap::new(),
            thread_placements: HashMap::new(),
            thread_agents_doc_summaries: HashMap::new(),
            active_agents_doc_editor_scope: None,
            agents_doc_editor: None,
            thread_folder_expanded: state::thread_folders_expanded_for_workspace(cx, None),
            thread_tree_selected_node_id: None,
            thread_tree_state,
            administration_content_view: AdministrationContentView::Members,
            settings_content_view: SettingsContentView::Account,
            profile_editor: None,
            profile_editor_input_subscriptions: Vec::new(),
            administration: AdministrationCache::default(),
            workspace_members_loading: HashSet::new(),
            thread_scope_pending: ThreadScopePendingAction::Idle,
            thread_scope_error: None,
            message_revision_dialog: None,
            message_revision_loading: false,
            message_mutation_pending: false,
            invitations_loading: false,
            invitations_error: None,
            members_loading: false,
            member_workspaces_saving: false,
            members_error: None,
            member_avatar_state: DesktopMemberAvatarState::default(),
            voice_input_action_error: None,
            voice_input_action_generation: 0,
            pending_voice_input_enabled: None,
            remote_access_settings_expanded: false,
            remote_access_key_input_revision: 0,
            remote_access_status_poll_generation: 0,
            settings_tree_state,
            administration_tree_state,
            provider_tree_state,
            thread_list_loading: false,
            thread_list_refresh_requested: false,
            active_thread_id: None,
            active_thread_resubscribe_pending: false,
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
            composer_input_subscription: None,
            composer_mention_select,
            composer_mention_select_subscription: None,
            composer_mention_items: Vec::new(),
            thread_member_select,
            thread_member_select_subscription: None,
            thread_member_items: Vec::new(),
            thread_members_thread_id: None,
            thread_scope_capabilities_thread_id: None,
            thread_scope_capabilities_loading_thread_id: None,
            thread_scope_capabilities_refresh_generation: 0,
            thread_scope_capabilities: ThreadPresentationCapabilities::default(),
            thread_members: Vec::new(),
            thread_members_loading: false,
            composer_attachments: Vec::new(),
            composer_capabilities: Vec::new(),
            composer_skill_selections: Vec::new(),
            composer_authorization_fingerprint: None,
            composer_upload_in_progress: false,
            composer_upload_error: None,
            composer_turn_mode: default_composer_turn_mode(),
            composer_hovered_mode: None,
            composer_mode_manually_selected: false,
            composer_reply_target: None,
            composer_edit_target: None,
            composer_selected_mentions: Vec::new(),
            composer_selected_provider: None,
            composer_capability_target: ComposerCapabilityTarget::native(),
            composer_selected_model: None,
            composer_selected_reasoning_effort: None,
            composer_permission_mode: default_composer_permission_mode(),
            desktop_microphone_gate,
            desktop_voice_status: VoiceStatus::Unavailable,
            desktop_voice_status_error: None,
            desktop_voice_status_poll_generation: 0,
            desktop_voice_composer: DesktopVoiceComposerState::Idle,
            desktop_voice_prepare_request: None,
            desktop_voice_capture: None,
            desktop_update: DesktopUpdateUiState::initial(),
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
            skills_management: SkillManagementProjection::default(),
            skills_expanded_pack_ids: HashSet::new(),
            skills_pending_pack_actions: HashSet::new(),
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
            composer_draft_lifecycle: ComposerDraftLifecycleState::default(),
            thread_start: ThreadStartCoordinator::default(),
            thread_start_requested: false,
            pending_thread_create_visibility: ThreadVisibility::Private,
            thread_timeline_scroll_handle: VirtualListScrollHandle::new(),
            thread_timeline_view_state: RefCell::new(ThreadTimelineViewState::default()),
            thread_timeline_item_expanded: RefCell::new(HashSet::new()),
            thread_timeline_terminal_item: RefCell::new(HashMap::new()),
            code_highlight_cache: RefCell::new(DesktopCodeHighlightCache::default()),
            semantic_timelines: SemanticTimelineState::default(),
            semantic_timeline_revision: 0,
            semantic_timeline_in_flight: HashSet::new(),
            semantic_timeline_pending: HashMap::new(),
            task_review_actions: TaskReviewActionState::default(),
            task_user_notifications_workspace_id: None,
            task_user_notifications: Vec::new(),
            task_user_notifications_next_cursor: None,
            task_user_notifications_loading: false,
            task_user_notifications_refresh_requested: false,
            task_user_notifications_refresh_generation: 0,
            task_user_notifications_error: None,
            thread_artifacts: ThreadArtifactsState::default(),
            artifact_download_cancellations: HashMap::new(),
            show_thread_artifacts_sidebar: false,
            show_thread_members_sidebar: false,
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
                http_client: None,
                ws_connection_id: None,
                deferred_ws_events: VecDeque::new(),
                connection_epoch: 0,
                session_refresh_generation: 0,
                current_principal_refresh_generation: 0,
                session_refresh_in_flight: false,
                authorization_revision: None,
                authorization_projections: Default::default(),
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
                auth_sessions: Vec::new(),
                auth_sessions_loading: false,
                auth_sessions_error: None,
                auth_session_action_pending: None,
                current_auth: None,
                capability_snapshot: None,
            },
        };

        let composer_state = view.composer_state.clone();
        view.composer_input_subscription = Some(cx.subscribe(
            &composer_state,
            |view, input, event: &gpui_component::input::InputEvent, cx| {
                if !matches!(event, gpui_component::input::InputEvent::Change) {
                    return;
                }
                let text = input.read(cx).value();
                if view
                    .reduce_composer_domain(
                        pioneer_client::composer::state_machine::ComposerDomainAction::ReconcileMentionsWithText {
                            text: text.to_string(),
                        },
                    )
                    .changed
                {
                    cx.notify();
                }
            },
        ));

        let composer_mention_select = view.composer_mention_select.clone();
        view.composer_mention_select_subscription = Some(cx.subscribe_in(
            &composer_mention_select,
            window,
            |_view,
             select,
             event: &gpui_component::combobox::ComboboxEvent<MemberPickerDelegate>,
             window,
             cx| {
                if let gpui_component::combobox::ComboboxEvent::Confirm(candidates) = event {
                    let Some(candidate) = candidates.first().cloned() else {
                        return;
                    };

                    // Keep the parent update outside the render pass.
                    let desktop_entity = cx.entity().clone();
                    let select = select.clone();
                    window.defer(cx, move |window, cx| {
                        let _ = desktop_entity.update(cx, |view, cx| {
                            view.insert_composer_mention(candidate, window, cx);
                        });
                        let _ = select.update(cx, |state, cx| {
                            state.clear_selection(cx);
                        });
                    });
                }
            },
        ));

        let thread_member_select = view.thread_member_select.clone();
        view.thread_member_select_subscription = Some(cx.subscribe_in(
            &thread_member_select,
            window,
            |_view,
             select,
             event: &gpui_component::combobox::ComboboxEvent<MemberPickerDelegate>,
             window,
             cx| {
                if let gpui_component::combobox::ComboboxEvent::Confirm(candidates) = event {
                    let Some(candidate) = candidates.first().cloned() else {
                        return;
                    };

                    let desktop_entity = cx.entity().clone();
                    let select = select.clone();
                    window.defer(cx, move |_, cx| {
                        let _ = desktop_entity.update(cx, |view, cx| {
                            view.add_thread_member(candidate.principal_id, cx);
                        });
                        let _ = select.update(cx, |state, cx| {
                            state.clear_selection(cx);
                        });
                    });
                }
            },
        ));

        cx.observe_window_bounds(window, |view, _, cx| {
            {
                let mut state = view.thread_timeline_view_state.borrow_mut();
                state.pending_width_probe = true;
                state.width_probe_attempts = 0;
            }
            cx.notify();
        })
        .detach();
        cx.observe_window_activation(window, |view, window, cx| {
            if window.is_window_active() {
                view.recover_gateway_session_on_foreground(cx);
            }
        })
        .detach();

        view.sync_settings_sidebar_tree_state(cx);
        view.sync_administration_sidebar_tree_state(cx);
        view.sync_provider_sidebar_tree_state(cx);
        view.start_gateway_ws_event_pump(cx);
        view.bootstrap_gateway_runtime(cx);
        view.start_desktop_update_check(cx);
        view.prune_thread_artifact_preview_cache(cx);

        view
    }
}
