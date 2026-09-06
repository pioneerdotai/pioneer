use crate::{
    app::root::{GatewayConnectionState, MainContentView, PioneerDesktop, SettingsContentView},
    app::settings::{
        MemoryModelSetting, MemorySettingToggle, SETTINGS_CONTENT_ACCOUNT_NODE_ID,
        SETTINGS_CONTENT_GENERAL_NODE_ID, SETTINGS_CONTENT_MEMORY_NODE_ID,
        SETTINGS_CONTENT_SELF_IMPROVEMENT_NODE_ID, SelfImprovementModelSetting,
        VoiceInputEnableAction,
    },
    settings::{self, AppLanguagePreference, WindowThemePreference},
    window,
};
use gpui_kit::component::{
    theme::{Theme, ThemeMode},
    tree::TreeItem,
};
use gpui_kit::{prelude::*, *};
use pioneer_client::settings::{
    gateway as gateway_settings, memory as settings_memory,
    self_improvement as settings_self_improvement, voice as voice_input_settings,
};
use pioneer_protocol::{
    GatewayMemoryModelSelection, GatewayMemorySettings, GatewaySelfImprovementModelSelection,
    GatewaySettingsUpdate, GatewayThreadEpisodicVectorProvider,
    GatewayThreadEpisodicVectorSearchSettings,
};
#[cfg(test)]
use pioneer_protocol::{
    GatewayVoiceInputProvider, GatewayVoiceInputRuntimePhase, GatewayVoiceInputSettings,
};
use std::time::Duration;
use tracing::warn;

const REMOTE_ACCESS_STATUS_POLL_ATTEMPTS: usize = 12;
const REMOTE_ACCESS_STATUS_POLL_INTERVAL: Duration = Duration::from_secs(1);

fn vector_search_embedding_provider_from_selector(
    provider: &str,
) -> Option<GatewayThreadEpisodicVectorProvider> {
    match provider.trim().to_ascii_lowercase().as_str() {
        "openai" => Some(GatewayThreadEpisodicVectorProvider::OpenAi),
        "openrouter" => Some(GatewayThreadEpisodicVectorProvider::OpenRouter),
        "local" => Some(GatewayThreadEpisodicVectorProvider::Local),
        _ => None,
    }
}

impl PioneerDesktop {
    pub(in crate::app) fn open_settings_content_from_sidebar(
        &mut self,
        mut content_view: SettingsContentView,
        cx: &mut Context<Self>,
    ) {
        if matches!(
            content_view,
            SettingsContentView::Memory | SettingsContentView::SelfImprovement
        ) && !self
            .principal_presentation_capabilities()
            .can_manage_gateway_settings
        {
            content_view = SettingsContentView::Account;
        }
        self.profile_editor = None;
        self.profile_editor_input_subscriptions.clear();
        self.settings_content_view = content_view;
        self.sync_settings_sidebar_tree_state(cx);
        self.set_main_content_view(MainContentView::Settings, cx);
        match content_view {
            SettingsContentView::Account => self.refresh_auth_sessions(cx),
            SettingsContentView::General => {
                if self
                    .principal_presentation_capabilities()
                    .can_manage_gateway_settings
                {
                    self.refresh_gateway_settings(cx);
                }
            }
            SettingsContentView::Memory | SettingsContentView::SelfImprovement => {
                self.refresh_gateway_settings(cx)
            }
        }
    }

    pub(in crate::app) fn sync_settings_sidebar_tree_state(&mut self, cx: &mut Context<Self>) {
        let mut items = vec![
            (
                SettingsContentView::Account,
                TreeItem::new(SETTINGS_CONTENT_ACCOUNT_NODE_ID, "account"),
            ),
            (
                SettingsContentView::General,
                TreeItem::new(SETTINGS_CONTENT_GENERAL_NODE_ID, "general"),
            ),
        ];
        if self
            .principal_presentation_capabilities()
            .can_manage_gateway_settings
        {
            items.extend([
                (
                    SettingsContentView::Memory,
                    TreeItem::new(SETTINGS_CONTENT_MEMORY_NODE_ID, "memory"),
                ),
                (
                    SettingsContentView::SelfImprovement,
                    TreeItem::new(
                        SETTINGS_CONTENT_SELF_IMPROVEMENT_NODE_ID,
                        "self-improvement",
                    ),
                ),
            ]);
        }
        if !items
            .iter()
            .any(|(content_view, _)| *content_view == self.settings_content_view)
        {
            self.settings_content_view = SettingsContentView::Account;
        }
        let selected_ix = items
            .iter()
            .position(|(content_view, _)| *content_view == self.settings_content_view);
        let settings_tree_state = self.settings_tree_state.clone();
        settings_tree_state.update(cx, |state, cx| {
            state.set_items(
                items.into_iter().map(|(_, item)| item).collect::<Vec<_>>(),
                cx,
            );
            state.set_selected_index(selected_ix, cx);
        });
    }

    pub(super) fn apply_language_setting(
        &mut self,
        language: AppLanguagePreference,
        cx: &mut Context<Self>,
    ) {
        let locale = language.resolve_locale();
        rust_i18n::set_locale(locale.as_str());

        if let Err(error) = settings::set_app_language(cx, language) {
            warn!(
                error = %format!("{error:#}"),
                "failed to save app language preference"
            );
        }
    }

    pub(super) fn apply_theme_setting(
        &mut self,
        preference: WindowThemePreference,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match preference {
            WindowThemePreference::System => Theme::sync_system_appearance(None, cx),
            WindowThemePreference::Light => Theme::change(ThemeMode::Light, Some(window), cx),
            WindowThemePreference::Dark => Theme::change(ThemeMode::Dark, Some(window), cx),
        }

        window::persist_theme_preference(window, preference, cx);
    }

    pub(super) fn apply_memory_setting(
        &mut self,
        toggle: MemorySettingToggle,
        enabled: bool,
        cx: &mut Context<Self>,
    ) {
        let Some(memory) = self.current_gateway_memory_settings() else {
            self.refresh_gateway_settings(cx);
            return;
        };
        self.apply_gateway_memory_settings(
            settings_memory::memory_settings_with_toggle(memory, toggle, enabled),
            cx,
        );
    }

    pub(in crate::app) fn apply_keepawake_setting(
        &mut self,
        enabled: bool,
        cx: &mut Context<Self>,
    ) {
        let Some(plan) =
            gateway_settings::keepawake_update_plan(self.gateway.settings.as_ref(), enabled)
        else {
            self.refresh_gateway_settings(cx);
            return;
        };

        self.apply_gateway_settings_update(plan.snapshot, plan.update, cx);
    }

    pub(super) fn apply_telemetry_setting(&mut self, enabled: bool, cx: &mut Context<Self>) {
        pioneer_observability::set_telemetry_enabled(enabled);
        let Some(plan) = gateway_settings::telemetry_enabled_update_plan(
            self.gateway.settings.as_ref(),
            enabled,
        ) else {
            self.refresh_gateway_settings(cx);
            return;
        };

        self.apply_gateway_settings_update(plan.snapshot, plan.update, cx);
    }

    pub(super) fn toggle_remote_access_settings_expanded(&mut self) {
        self.remote_access_settings_expanded = !self.remote_access_settings_expanded;
    }

    pub(super) fn apply_remote_access_setting(
        &mut self,
        enabled: bool,
        key: Option<String>,
        clear_key: bool,
        cx: &mut Context<Self>,
    ) {
        let Some(plan) = gateway_settings::remote_access_update_plan(
            self.gateway.settings.as_ref(),
            enabled,
            key,
            clear_key,
        ) else {
            self.refresh_gateway_settings(cx);
            return;
        };

        self.apply_gateway_settings_update(plan.snapshot, plan.update, cx);
    }

    pub(super) fn save_remote_access_key_inline(&mut self, key: String, cx: &mut Context<Self>) {
        if key.trim().is_empty() {
            return;
        }
        let Some(settings) = self.gateway.settings.as_ref() else {
            self.refresh_gateway_settings(cx);
            return;
        };
        self.remote_access_key_input_revision =
            self.remote_access_key_input_revision.wrapping_add(1);
        self.apply_remote_access_setting(settings.remote_access.enabled, Some(key), false, cx);
    }

    pub(super) fn apply_vector_search_enabled(&mut self, enabled: bool, cx: &mut Context<Self>) {
        let Some(mut vector_search) = self.current_vector_search_settings() else {
            self.refresh_gateway_settings(cx);
            return;
        };
        vector_search.enabled = enabled;
        self.apply_vector_search_settings(vector_search, cx);
    }

    pub(super) fn apply_voice_input_enabled(
        &mut self,
        enabled: bool,
        cx: &mut Context<Self>,
    ) -> VoiceInputEnableAction {
        let Some(current) = self
            .gateway
            .settings
            .as_ref()
            .map(|settings| settings.voice_input.clone())
        else {
            self.refresh_gateway_settings(cx);
            return VoiceInputEnableAction::Noop;
        };

        let plan = if enabled {
            voice_input_settings::voice_input_enable_plan(&current)
        } else {
            voice_input_settings::voice_input_disable_plan(&current)
        };
        match plan {
            voice_input_settings::VoiceInputSettingsPlan::Update { update } => {
                if self.apply_gateway_voice_settings_update(update, Some(enabled), cx) {
                    VoiceInputEnableAction::Sent
                } else {
                    VoiceInputEnableAction::Noop
                }
            }
            voice_input_settings::VoiceInputSettingsPlan::NeedsSelection => {
                VoiceInputEnableAction::NeedsSelection
            }
            voice_input_settings::VoiceInputSettingsPlan::Noop
            | voice_input_settings::VoiceInputSettingsPlan::Rejected { .. } => {
                VoiceInputEnableAction::Noop
            }
        }
    }

    pub(super) fn apply_voice_input_model_selection(
        &mut self,
        selection: crate::components::model_selector::ModelSelectorSelection,
        cx: &mut Context<Self>,
    ) -> bool {
        let Some(current) = self
            .gateway
            .settings
            .as_ref()
            .map(|settings| &settings.voice_input)
        else {
            self.refresh_gateway_settings(cx);
            return false;
        };
        let voice_input_settings::VoiceInputSettingsPlan::Update { update } =
            voice_input_settings::voice_input_model_selection_plan(
                current,
                selection.provider.as_deref(),
                selection.model,
            )
        else {
            return false;
        };

        self.apply_gateway_voice_settings_update(update, Some(true), cx)
    }

    pub(super) fn retry_voice_input_install(&mut self, cx: &mut Context<Self>) -> bool {
        let Some(current) = self
            .gateway
            .settings
            .as_ref()
            .map(|settings| &settings.voice_input)
        else {
            self.refresh_gateway_settings(cx);
            return false;
        };
        let voice_input_settings::VoiceInputSettingsPlan::Update { update } =
            voice_input_settings::voice_input_retry_plan(current)
        else {
            return false;
        };

        self.apply_gateway_voice_settings_update(update, None, cx)
    }

    pub(super) fn apply_vector_search_use_search_instructions(
        &mut self,
        enabled: bool,
        cx: &mut Context<Self>,
    ) {
        let Some(mut vector_search) = self.current_vector_search_settings() else {
            self.refresh_gateway_settings(cx);
            return;
        };
        vector_search.use_search_instructions = enabled;
        self.apply_vector_search_settings(vector_search, cx);
    }

    pub(super) fn apply_vector_search_embedding_model_selection(
        &mut self,
        selection: crate::components::model_selector::ModelSelectorSelection,
        cx: &mut Context<Self>,
    ) -> bool {
        let Some(provider) = selection
            .provider
            .as_deref()
            .and_then(vector_search_embedding_provider_from_selector)
        else {
            return false;
        };
        let Some(model) = selection
            .model
            .map(|model| model.trim().to_owned())
            .filter(|model| !model.is_empty())
        else {
            return false;
        };
        let Some(mut vector_search) = self.current_vector_search_settings() else {
            self.refresh_gateway_settings(cx);
            return false;
        };

        vector_search.enabled = true;
        vector_search.provider = Some(provider);
        vector_search.model = Some(model.clone());
        if provider == GatewayThreadEpisodicVectorProvider::Local {
            vector_search.local_model = Some(model);
        }
        vector_search.embedding_dimension = None;
        self.apply_vector_search_settings(vector_search, cx);
        true
    }

    pub(super) fn apply_memory_model_setting(
        &mut self,
        setting: MemoryModelSetting,
        model_selection: GatewayMemoryModelSelection,
        cx: &mut Context<Self>,
    ) {
        let Some(memory) = self.current_gateway_memory_settings() else {
            self.refresh_gateway_settings(cx);
            return;
        };
        self.apply_gateway_memory_settings(
            settings_memory::memory_settings_with_model_selection(memory, setting, model_selection),
            cx,
        );
    }

    pub(super) fn apply_self_improvement_enabled(&mut self, enabled: bool, cx: &mut Context<Self>) {
        let Some(plan) =
            settings_self_improvement::enabled_update_plan(self.gateway.settings.as_ref(), enabled)
        else {
            self.refresh_gateway_settings(cx);
            return;
        };
        self.apply_gateway_settings_update(plan.snapshot, plan.update, cx);
    }

    pub(super) fn apply_self_improvement_model_setting(
        &mut self,
        setting: SelfImprovementModelSetting,
        selection: Option<GatewaySelfImprovementModelSelection>,
        cx: &mut Context<Self>,
    ) {
        let Some(plan) = settings_self_improvement::model_update_plan(
            self.gateway.settings.as_ref(),
            setting,
            selection,
        ) else {
            self.refresh_gateway_settings(cx);
            return;
        };
        self.apply_gateway_settings_update(plan.snapshot, plan.update, cx);
    }

    pub(super) fn apply_preflight_model_setting(
        &mut self,
        model_selection: GatewayMemoryModelSelection,
        cx: &mut Context<Self>,
    ) {
        let Some(plan) = gateway_settings::preflight_model_update_plan(
            self.gateway.settings.as_ref(),
            model_selection,
        ) else {
            self.refresh_gateway_settings(cx);
            return;
        };

        self.apply_gateway_settings_update(plan.snapshot, plan.update, cx);
    }

    pub(super) fn apply_thread_episodic_setting(
        &mut self,
        toggle: gateway_settings::ThreadEpisodicSettingToggle,
        enabled: bool,
        cx: &mut Context<Self>,
    ) {
        let Some(current) = self.gateway.settings.as_ref() else {
            self.refresh_gateway_settings(cx);
            return;
        };
        let _ = toggle;
        let Some(plan) =
            gateway_settings::thread_episodic_enabled_update_plan(Some(current), enabled)
        else {
            self.refresh_gateway_settings(cx);
            return;
        };

        self.apply_gateway_settings_update(plan.snapshot, plan.update, cx);
    }

    pub(in crate::app) fn refresh_gateway_settings(&mut self, cx: &mut Context<Self>) {
        // The initial request is deliberately deferred until the first
        // operational Desktop frame. Before then the principal capability
        // snapshot may not exist yet, which used to turn the connection-time
        // refresh into a no-op until the user opened Settings manually.
        if !self.startup.has_presented_operational_frame() {
            return;
        }
        if !self
            .principal_presentation_capabilities()
            .can_manage_gateway_settings
        {
            self.gateway.settings = None;
            self.gateway.settings_loading = false;
            self.gateway.settings_error = None;
            return;
        }
        let plan = gateway_settings::plan_gateway_settings_refresh(
            self.gateway.settings_loading,
            self.gateway.connection_state == GatewayConnectionState::Connected,
            self.gateway.ws_connection_id,
            self.gateway
                .client_runtime
                .client_core()
                .gateway_operation_epoch(),
        );
        let scope = match plan {
            gateway_settings::GatewaySettingsRefreshPlan::Send(scope) => scope,
            gateway_settings::GatewaySettingsRefreshPlan::SkipAlreadyLoading => return,
            gateway_settings::GatewaySettingsRefreshPlan::Unavailable(reason) => {
                self.apply_gateway_settings_refresh_unavailable(reason);
                return;
            }
        };
        let connection_id = scope.connection_id;
        let connection_epoch = scope.connection_epoch;

        gateway_settings::begin_gateway_settings_refresh(
            &mut self.gateway.settings_loading,
            &mut self.gateway.settings_error,
        );

        let client_core = self.gateway.client_runtime.client_core().clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let result = cx
                    .background_spawn(async move { client_core.refresh_gateway_settings() })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if !gateway_settings::settings_action_matches_connection(
                        connection_id,
                        connection_epoch,
                        view.gateway.ws_connection_id,
                        view.gateway
                            .client_runtime
                            .client_core()
                            .gateway_operation_epoch(),
                    ) {
                        return;
                    }

                    match result {
                        Ok(_) => {
                            let current =
                                view.gateway.client_runtime.client_core().gateway_settings();
                            view.gateway.settings = current.settings;
                            view.gateway.settings_loading = current.loading;
                            view.gateway.settings_error = current.error;
                            if let Some(settings) = view.gateway.settings.as_ref() {
                                pioneer_observability::set_telemetry_enabled(
                                    settings.general.telemetry_enabled,
                                );
                            }
                        }
                        Err(error) => {
                            let current =
                                view.gateway.client_runtime.client_core().gateway_settings();
                            view.gateway.settings = current.settings;
                            view.gateway.settings_loading = current.loading;
                            view.gateway.settings_error = current.error;
                            warn!(
                                error = %format!("{error:#}"),
                                "failed to fetch gateway settings"
                            );
                        }
                    }
                    cx.notify();
                });
            }
        })
        .detach();
    }

    fn current_gateway_memory_settings(&self) -> Option<GatewayMemorySettings> {
        settings_memory::current_gateway_memory_settings(self.gateway.settings.as_ref())
    }

    fn current_vector_search_settings(&self) -> Option<GatewayThreadEpisodicVectorSearchSettings> {
        self.gateway
            .settings
            .as_ref()
            .map(|settings| settings.thread_episodic.vector_search.clone())
    }

    fn apply_vector_search_settings(
        &mut self,
        vector_search: GatewayThreadEpisodicVectorSearchSettings,
        cx: &mut Context<Self>,
    ) {
        let Some(plan) = gateway_settings::thread_episodic_vector_search_update_plan(
            self.gateway.settings.as_ref(),
            vector_search,
        ) else {
            self.refresh_gateway_settings(cx);
            return;
        };

        self.apply_gateway_settings_update(plan.snapshot, plan.update, cx);
    }

    fn apply_gateway_memory_settings(
        &mut self,
        memory: GatewayMemorySettings,
        cx: &mut Context<Self>,
    ) {
        let snapshot = settings_memory::gateway_settings_snapshot_with_memory(
            self.gateway.settings.as_ref(),
            memory.clone(),
        );
        self.apply_gateway_settings_update(
            snapshot,
            settings_memory::gateway_settings_update_for_memory(memory),
            cx,
        );
    }

    fn apply_gateway_voice_settings_update(
        &mut self,
        update: GatewaySettingsUpdate,
        pending_enabled: Option<bool>,
        cx: &mut Context<Self>,
    ) -> bool {
        if !self
            .principal_presentation_capabilities()
            .can_manage_gateway_settings
        {
            return false;
        }
        let Some(scope) = gateway_settings::plan_gateway_settings_update_action(
            self.gateway.ws_connection_id,
            self.gateway
                .client_runtime
                .client_core()
                .gateway_operation_epoch(),
        ) else {
            warn!("cannot update Voice Input settings without an active gateway connection");
            return false;
        };
        let connection_id = scope.connection_id;
        let connection_epoch = scope.connection_epoch;
        self.voice_input_action_generation = self.voice_input_action_generation.wrapping_add(1);
        let action_generation = self.voice_input_action_generation;
        self.pending_voice_input_enabled = pending_enabled;
        self.voice_input_action_error = None;

        let client_core = self.gateway.client_runtime.client_core().clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let result = cx
                    .background_spawn(async move { client_core.update_gateway_settings(update) })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if !gateway_settings::settings_action_matches_connection(
                        connection_id,
                        connection_epoch,
                        view.gateway.ws_connection_id,
                        view.gateway
                            .client_runtime
                            .client_core()
                            .gateway_operation_epoch(),
                    ) {
                        return;
                    }

                    if view.voice_input_action_generation == action_generation {
                        view.pending_voice_input_enabled = None;
                    }

                    match result {
                        Ok(_) => {
                            let current =
                                view.gateway.client_runtime.client_core().gateway_settings();
                            view.gateway.settings = current.settings;
                            view.voice_input_action_error = current.error;
                        }
                        Err(error) => {
                            gateway_settings::apply_gateway_settings_update_error(
                                &mut view.voice_input_action_error,
                                format!("{error:#}"),
                            );
                            warn!(
                                error = %format!("{error:#}"),
                                "failed to update Voice Input settings"
                            );
                        }
                    }
                    cx.notify();
                });
            }
        })
        .detach();
        true
    }

    pub(in crate::app) fn apply_gateway_settings_update(
        &mut self,
        snapshot: pioneer_protocol::GatewaySettingsSnapshot,
        update: GatewaySettingsUpdate,
        cx: &mut Context<Self>,
    ) {
        if !self
            .principal_presentation_capabilities()
            .can_manage_gateway_settings
        {
            return;
        }
        let Some(scope) = gateway_settings::plan_gateway_settings_update_action(
            self.gateway.ws_connection_id,
            self.gateway
                .client_runtime
                .client_core()
                .gateway_operation_epoch(),
        ) else {
            warn!("cannot update gateway settings without an active gateway connection");
            return;
        };
        let connection_id = scope.connection_id;
        let connection_epoch = scope.connection_epoch;
        let reload_cli_provider_snapshot_after_update = update.cli_runtimes.is_some();
        let poll_remote_access_status_after_update = update.remote_access.is_some();

        let client_core = self.gateway.client_runtime.client_core().clone();
        let generation = match client_core.prepare_gateway_settings_update(Some(snapshot)) {
            Ok(generation) => generation,
            Err(error) => {
                self.gateway.settings_error = Some(format!("{error:#}"));
                return;
            }
        };
        let publication = client_core.gateway_settings();
        self.gateway.settings = publication.settings;
        self.gateway.settings_error = publication.error;
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let result = cx
                    .background_spawn(async move {
                        client_core.execute_gateway_settings_update(generation, update)
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if !gateway_settings::settings_action_matches_connection(
                        connection_id,
                        connection_epoch,
                        view.gateway.ws_connection_id,
                        view.gateway
                            .client_runtime
                            .client_core()
                            .gateway_operation_epoch(),
                    ) {
                        return;
                    }

                    match result {
                        Ok(_) => {
                            let current =
                                view.gateway.client_runtime.client_core().gateway_settings();
                            view.gateway.settings = current.settings;
                            view.gateway.settings_error = current.error;
                            if reload_cli_provider_snapshot_after_update {
                                view.load_cli_provider_snapshot(cx);
                            }
                            if poll_remote_access_status_after_update {
                                view.schedule_remote_access_status_poll(
                                    connection_id,
                                    connection_epoch,
                                    cx,
                                );
                            }
                        }
                        Err(error) => {
                            gateway_settings::apply_gateway_settings_update_error(
                                &mut view.gateway.settings_error,
                                format!("{error:#}"),
                            );
                            warn!(
                                error = %format!("{error:#}"),
                                "failed to update gateway settings"
                            );
                        }
                    }
                    cx.notify();
                });
            }
        })
        .detach();
    }

    fn schedule_remote_access_status_poll(
        &mut self,
        connection_id: u64,
        connection_epoch: u64,
        cx: &mut Context<Self>,
    ) {
        self.remote_access_status_poll_generation =
            self.remote_access_status_poll_generation.wrapping_add(1);
        let generation = self.remote_access_status_poll_generation;

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                for _ in 0..REMOTE_ACCESS_STATUS_POLL_ATTEMPTS {
                    pioneer_observability::record_qualification_diagnostic!(record_animation_activity(
                        pioneer_observability::AnimationSourceId::RemoteAccessPoller,
                        pioneer_observability::DiagnosticAction::Scheduled,
                        pioneer_observability::Visibility::Global,
                    ));
                    cx.background_executor()
                        .timer(REMOTE_ACCESS_STATUS_POLL_INTERVAL)
                        .await;
                    pioneer_observability::record_qualification_diagnostic!(record_animation_activity(
                        pioneer_observability::AnimationSourceId::RemoteAccessPoller,
                        pioneer_observability::DiagnosticAction::Woke,
                        pioneer_observability::Visibility::Global,
                    ));

                    let updated = this.update(&mut cx, |view, cx| {
                        if view.remote_access_status_poll_generation != generation {
                            return false;
                        }
                        if !gateway_settings::settings_action_matches_connection(
                            connection_id,
                            connection_epoch,
                            view.gateway.ws_connection_id,
                            view.gateway
                                .client_runtime
                                .client_core()
                                .gateway_operation_epoch(),
                        ) {
                            return false;
                        }
                        if !gateway_settings::remote_access_status_needs_poll(
                            view.gateway.settings.as_ref(),
                        ) {
                            return false;
                        }

                        pioneer_observability::record_qualification_diagnostic!(
                            record_animation_activity(
                                pioneer_observability::AnimationSourceId::RemoteAccessPoller,
                                pioneer_observability::DiagnosticAction::Requested,
                                pioneer_observability::Visibility::NotApplicable,
                            )
                        );
                        view.refresh_gateway_settings(cx);
                        true
                    });

                    match updated {
                        Ok(true) => {}
                        #[cfg(not(feature = "qualification-diagnostics"))]
                        _ => break,
                        #[cfg(feature = "qualification-diagnostics")]
                        _ => {
                            pioneer_observability::record_qualification_diagnostic!(record_animation_activity(
                                pioneer_observability::AnimationSourceId::RemoteAccessPoller,
                                pioneer_observability::DiagnosticAction::Cancelled,
                                pioneer_observability::Visibility::Global,
                            ));
                            return;
                        }
                    }
                }
                pioneer_observability::record_qualification_diagnostic!(record_animation_activity(
                    pioneer_observability::AnimationSourceId::RemoteAccessPoller,
                    pioneer_observability::DiagnosticAction::Completed,
                    pioneer_observability::Visibility::Global,
                ));
            }
        })
        .detach();
    }

    fn apply_gateway_settings_refresh_unavailable(
        &mut self,
        reason: gateway_settings::GatewaySettingsRefreshUnavailable,
    ) {
        let message = match reason {
            gateway_settings::GatewaySettingsRefreshUnavailable::GatewayNotConnected => {
                t!("settings.gateway_not_connected")
            }
        };
        gateway_settings::apply_gateway_settings_unavailable(
            &mut self.gateway.settings,
            &mut self.gateway.settings_loading,
            &mut self.gateway.settings_error,
            message,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn production_lifecycle_source() -> &'static str {
        include_str!("lifecycle.rs")
            .split("\n#[cfg(test)]\nmod tests")
            .next()
            .expect("production source segment exists")
    }

    fn voice_settings(
        enabled: bool,
        provider: Option<GatewayVoiceInputProvider>,
        model: Option<&str>,
        phase: GatewayVoiceInputRuntimePhase,
    ) -> GatewayVoiceInputSettings {
        GatewayVoiceInputSettings {
            enabled,
            provider,
            model: model.map(str::to_owned),
            runtime: pioneer_protocol::GatewayVoiceInputRuntimeSnapshot {
                phase,
                ..pioneer_protocol::GatewayVoiceInputRuntimeSnapshot::default()
            },
        }
    }

    #[::core::prelude::v1::test]
    fn voice_settings_lifecycle_enable_selection_disable_and_retry_updates_are_exact() {
        let unselected = voice_settings(false, None, None, GatewayVoiceInputRuntimePhase::Disabled);
        assert!(matches!(
            voice_input_settings::voice_input_enable_plan(&unselected),
            voice_input_settings::VoiceInputSettingsPlan::NeedsSelection
        ));

        let voice_input_settings::VoiceInputSettingsPlan::Update { update: selection } =
            voice_input_settings::voice_input_model_selection_plan(
                &unselected,
                Some("local"),
                Some(" parakeet-tdt-0.6b-v3 ".to_owned()),
            )
        else {
            panic!("valid local selection must produce an update")
        };
        let selection = selection.voice_input.expect("voice selection update");
        assert_eq!(selection.enabled, Some(true));
        assert_eq!(
            selection.provider,
            Some(Some(GatewayVoiceInputProvider::Local))
        );
        assert_eq!(
            selection.model.as_ref().and_then(|model| model.as_deref()),
            Some("parakeet-tdt-0.6b-v3")
        );
        assert!(!selection.retry_install);

        let selected = voice_settings(
            true,
            Some(GatewayVoiceInputProvider::Local),
            Some("parakeet-tdt-0.6b-v3"),
            GatewayVoiceInputRuntimePhase::Ready,
        );
        let voice_input_settings::VoiceInputSettingsPlan::Update { update: disable } =
            voice_input_settings::voice_input_disable_plan(&selected)
        else {
            panic!("disable must send an update");
        };
        let disable = disable.voice_input.expect("voice disable update");
        assert_eq!(disable.enabled, Some(false));
        assert_eq!(disable.provider, None);
        assert_eq!(disable.model, None);
        assert!(!disable.retry_install);

        let failed = voice_settings(
            true,
            Some(GatewayVoiceInputProvider::Local),
            Some("parakeet-tdt-0.6b-v3"),
            GatewayVoiceInputRuntimePhase::Failed,
        );
        let voice_input_settings::VoiceInputSettingsPlan::Update { update: retry } =
            voice_input_settings::voice_input_retry_plan(&failed)
        else {
            panic!("failed selected model may retry")
        };
        let retry = retry.voice_input.expect("voice retry update");
        assert_eq!(retry.enabled, None);
        assert_eq!(retry.provider, None);
        assert_eq!(retry.model, None);
        assert!(retry.retry_install);
    }

    #[::core::prelude::v1::test]
    fn voice_settings_lifecycle_cancel_and_noop_paths_send_nothing() {
        let selected = voice_settings(
            true,
            Some(GatewayVoiceInputProvider::Local),
            Some("small"),
            GatewayVoiceInputRuntimePhase::Ready,
        );
        assert!(matches!(
            voice_input_settings::voice_input_enable_plan(&selected),
            voice_input_settings::VoiceInputSettingsPlan::Noop
        ));
        assert!(matches!(
            voice_input_settings::voice_input_model_selection_plan(
                &selected,
                None,
                Some("medium".to_owned())
            ),
            voice_input_settings::VoiceInputSettingsPlan::Rejected { .. }
        ));
        assert!(matches!(
            voice_input_settings::voice_input_model_selection_plan(&selected, Some("local"), None),
            voice_input_settings::VoiceInputSettingsPlan::Rejected { .. }
        ));
        assert!(matches!(
            voice_input_settings::voice_input_model_selection_plan(
                &selected,
                Some("local"),
                Some("small".to_owned())
            ),
            voice_input_settings::VoiceInputSettingsPlan::Noop
        ));
        assert!(matches!(
            voice_input_settings::voice_input_retry_plan(&selected),
            voice_input_settings::VoiceInputSettingsPlan::Rejected { .. }
        ));
    }

    #[::core::prelude::v1::test]
    fn voice_settings_lifecycle_uses_shared_client_plans() {
        let source = production_lifecycle_source();
        assert!(source.contains("voice_input_settings::voice_input_enable_plan"));
        assert!(source.contains("voice_input_settings::voice_input_disable_plan"));
        assert!(source.contains("voice_input_settings::voice_input_model_selection_plan"));
        assert!(source.contains("voice_input_settings::voice_input_retry_plan"));
        assert!(!source.contains("fn plan_voice_input_"));
    }

    #[::core::prelude::v1::test]
    fn self_improvement_lifecycle_uses_shared_client_plans_and_authoritative_response() {
        let source = production_lifecycle_source();
        let self_improvement = source
            .split("pub(super) fn apply_self_improvement_enabled")
            .nth(1)
            .expect("Self-improvement lifecycle exists")
            .split("pub(super) fn apply_preflight_model_setting")
            .next()
            .expect("Self-improvement lifecycle boundary exists");
        assert!(self_improvement.contains("settings_self_improvement::enabled_update_plan"));
        assert!(self_improvement.contains("settings_self_improvement::model_update_plan"));
        assert!(self_improvement.contains("self.apply_gateway_settings_update"));

        let common_update = source
            .split("pub(in crate::app) fn apply_gateway_settings_update")
            .nth(1)
            .expect("common settings update exists");
        assert!(common_update.contains("apply_optimistic_gateway_settings_update"));
        assert!(common_update.contains("apply_gateway_settings_update_response"));
    }

    #[::core::prelude::v1::test]
    fn voice_settings_lifecycle_rejection_keeps_authoritative_snapshot() {
        let source = production_lifecycle_source();
        let voice_update_fn = source
            .split("fn apply_gateway_voice_settings_update")
            .nth(1)
            .expect("authoritative Voice Input update function exists")
            .split("pub(in crate::app) fn apply_gateway_settings_update")
            .next()
            .expect("Voice Input update function body exists");

        assert!(voice_update_fn.contains("gateway_settings_update(update)"));
        assert!(voice_update_fn.contains("apply_gateway_settings_update_response"));
        assert!(voice_update_fn.contains("apply_gateway_settings_update_error"));
        assert!(!voice_update_fn.contains("apply_optimistic_gateway_settings_update"));
    }

    #[::core::prelude::v1::test]
    fn settings_preflight_model_update_writes_general_settings_only() {
        let source = production_lifecycle_source();
        let preflight_fn = source
            .split("pub(super) fn apply_preflight_model_setting")
            .nth(1)
            .expect("preflight setting function exists")
            .split("pub(in crate::app) fn refresh_gateway_settings")
            .next()
            .expect("preflight setting function body exists");

        assert!(preflight_fn.contains("gateway_settings::preflight_model_update_plan"));
        assert!(preflight_fn.contains("self.apply_gateway_settings_update"));
        assert!(!preflight_fn.contains("settings_memory::gateway_settings_update_for_memory"));
    }

    #[::core::prelude::v1::test]
    fn telemetry_lifecycle_uses_shared_client_plan_and_common_update_path() {
        let source = production_lifecycle_source();
        let telemetry_fn = source
            .split("pub(super) fn apply_telemetry_setting")
            .nth(1)
            .expect("telemetry setting function exists")
            .split("pub(super) fn toggle_remote_access_settings_expanded")
            .next()
            .expect("telemetry setting function body exists");

        assert!(telemetry_fn.contains("gateway_settings::telemetry_enabled_update_plan"));
        assert!(telemetry_fn.contains("self.apply_gateway_settings_update"));
    }

    #[::core::prelude::v1::test]
    fn settings_memory_model_update_keeps_only_proactive_write_model_owned_by_memory() {
        let source = production_lifecycle_source();
        let memory_model_fn = source
            .split("pub(super) fn apply_memory_model_setting")
            .nth(1)
            .expect("memory model setting function exists")
            .split("pub(super) fn apply_preflight_model_setting")
            .next()
            .expect("memory model setting function body exists");

        assert!(memory_model_fn.contains("settings_memory::memory_settings_with_model_selection"));
        assert!(memory_model_fn.contains("self.apply_gateway_memory_settings"));
        assert!(!memory_model_fn.contains("preflight_model"));
        assert!(!memory_model_fn.contains("ActiveRecall"));
    }

    #[::core::prelude::v1::test]
    fn settings_thread_episodic_update_writes_thread_episodic_settings_only() {
        let source = production_lifecycle_source();
        let thread_episodic_fn = source
            .split("pub(super) fn apply_thread_episodic_setting")
            .nth(1)
            .expect("thread episodic setting function exists")
            .split("pub(in crate::app) fn refresh_gateway_settings")
            .next()
            .expect("thread episodic setting function body exists");

        assert!(thread_episodic_fn.contains("thread_episodic_enabled_update_plan"));
        assert!(thread_episodic_fn.contains("self.apply_gateway_settings_update"));
        assert!(
            !thread_episodic_fn.contains("settings_memory::gateway_settings_update_for_memory")
        );
        assert!(!thread_episodic_fn.contains("desktop-settings.toml"));
    }

    #[::core::prelude::v1::test]
    fn settings_vector_search_update_writes_thread_episodic_settings_only() {
        let source = production_lifecycle_source();
        let vector_fn = source
            .split("fn apply_vector_search_settings")
            .nth(1)
            .expect("vector search setting function exists")
            .split("fn apply_gateway_memory_settings")
            .next()
            .expect("vector search setting function body exists");

        assert!(vector_fn.contains("thread_episodic_vector_search_update_plan"));
        assert!(vector_fn.contains("self.apply_gateway_settings_update"));
        assert!(!vector_fn.contains("settings_memory::gateway_settings_update_for_memory"));
        assert!(!vector_fn.contains("preflight_model"));
        assert!(!vector_fn.contains("remote_access"));
    }

    #[::core::prelude::v1::test]
    fn settings_vector_search_embedding_model_selection_writes_thread_episodic_settings_only() {
        let source = production_lifecycle_source();
        let embedding_model_fn = source
            .split("pub(super) fn apply_vector_search_embedding_model_selection")
            .nth(1)
            .expect("vector embedding model function exists")
            .split("pub(super) fn apply_memory_model_setting")
            .next()
            .expect("vector embedding model function body exists");

        assert!(embedding_model_fn.contains("vector_search_embedding_provider_from_selector"));
        assert!(embedding_model_fn.contains("vector_search.enabled = true"));
        assert!(embedding_model_fn.contains("self.apply_vector_search_settings"));
        assert!(!embedding_model_fn.contains("provider_set_api_key"));
        assert!(
            !embedding_model_fn.contains("settings_memory::gateway_settings_update_for_memory")
        );
    }

    #[::core::prelude::v1::test]
    fn settings_vector_search_instruction_toggle_writes_thread_episodic_settings_only() {
        let source = production_lifecycle_source();
        let instruction_fn = source
            .split("pub(super) fn apply_vector_search_use_search_instructions")
            .nth(1)
            .expect("vector search instruction function exists")
            .split("pub(super) fn apply_vector_search_embedding_model_selection")
            .next()
            .expect("vector search instruction function body exists");

        assert!(instruction_fn.contains("vector_search.use_search_instructions = enabled"));
        assert!(instruction_fn.contains("self.apply_vector_search_settings"));
        assert!(!instruction_fn.contains("settings_memory::gateway_settings_update_for_memory"));
    }
}
