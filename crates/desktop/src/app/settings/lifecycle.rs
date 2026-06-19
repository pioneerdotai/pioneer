use crate::{
    app::root::{GatewayConnectionState, MainContentView, PioneerDesktop, SettingsContentView},
    app::settings::{
        MemoryModelSetting, MemorySettingToggle, SETTINGS_CONTENT_GENERAL_NODE_ID,
        SETTINGS_CONTENT_MEMORY_NODE_ID,
    },
    settings::{self, AppLanguagePreference, WindowThemePreference},
    window,
};
use gpui::{prelude::*, *};
use gpui_component::{
    theme::{Theme, ThemeMode},
    tree::TreeItem,
};
use pioneer_client::settings::{gateway as gateway_settings, memory as settings_memory};
use pioneer_protocol::{GatewayMemoryModelSelection, GatewayMemorySettings, GatewaySettingsUpdate};
use std::time::Duration;
use tracing::warn;

const REMOTE_ACCESS_STATUS_POLL_ATTEMPTS: usize = 12;
const REMOTE_ACCESS_STATUS_POLL_INTERVAL: Duration = Duration::from_secs(1);

impl PioneerDesktop {
    pub(in crate::app) fn open_settings_content_from_sidebar(
        &mut self,
        content_view: SettingsContentView,
        cx: &mut Context<Self>,
    ) {
        self.settings_content_view = content_view;
        self.sync_settings_sidebar_tree_state(cx);
        self.set_main_content_view(MainContentView::Settings, cx);
        self.refresh_gateway_settings(cx);
    }

    pub(in crate::app) fn sync_settings_sidebar_tree_state(&mut self, cx: &mut Context<Self>) {
        let selected_ix = match self.settings_content_view {
            SettingsContentView::General => Some(0),
            SettingsContentView::Memory => Some(1),
        };
        let settings_tree_state = self.settings_tree_state.clone();
        settings_tree_state.update(cx, |state, cx| {
            state.set_items(
                vec![
                    TreeItem::new(SETTINGS_CONTENT_GENERAL_NODE_ID, "general"),
                    TreeItem::new(SETTINGS_CONTENT_MEMORY_NODE_ID, "memory"),
                ],
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
        let plan = gateway_settings::plan_gateway_settings_refresh(
            self.gateway.settings_loading,
            self.gateway.connection_state == GatewayConnectionState::Connected,
            self.gateway.ws_connection_id,
            self.gateway.connection_epoch,
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

        let ws_sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let result = cx
                    .background_spawn(async move { ws_sender.gateway_settings_get() })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if !gateway_settings::settings_action_matches_connection(
                        connection_id,
                        connection_epoch,
                        view.gateway.ws_connection_id,
                        view.gateway.connection_epoch,
                    ) {
                        return;
                    }

                    match result {
                        Ok(response) => {
                            gateway_settings::apply_gateway_settings_get_response(
                                &mut view.gateway.settings,
                                &mut view.gateway.settings_loading,
                                &mut view.gateway.settings_error,
                                response,
                            );
                        }
                        Err(error) => {
                            gateway_settings::apply_gateway_settings_get_error(
                                &mut view.gateway.settings_loading,
                                &mut view.gateway.settings_error,
                                format!("{error:#}"),
                            );
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

    pub(in crate::app) fn apply_gateway_settings_update(
        &mut self,
        snapshot: pioneer_protocol::GatewaySettingsSnapshot,
        update: GatewaySettingsUpdate,
        cx: &mut Context<Self>,
    ) {
        let Some(scope) = gateway_settings::plan_gateway_settings_update_action(
            self.gateway.ws_connection_id,
            self.gateway.connection_epoch,
        ) else {
            warn!("cannot update gateway settings without an active gateway connection");
            return;
        };
        let connection_id = scope.connection_id;
        let connection_epoch = scope.connection_epoch;
        let refresh_cli_providers_after_update = update.cli_runtimes.is_some();
        let poll_remote_access_status_after_update = update.remote_access.is_some();

        gateway_settings::apply_optimistic_gateway_settings_update(
            &mut self.gateway.settings,
            &mut self.gateway.settings_error,
            snapshot,
        );

        let ws_sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let result = cx
                    .background_spawn(async move { ws_sender.gateway_settings_update(update) })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if !gateway_settings::settings_action_matches_connection(
                        connection_id,
                        connection_epoch,
                        view.gateway.ws_connection_id,
                        view.gateway.connection_epoch,
                    ) {
                        return;
                    }

                    match result {
                        Ok(response) => {
                            gateway_settings::apply_gateway_settings_update_response(
                                &mut view.gateway.settings,
                                &mut view.gateway.settings_error,
                                response,
                            );
                            if refresh_cli_providers_after_update {
                                view.refresh_cli_providers(cx);
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
                    Timer::after(REMOTE_ACCESS_STATUS_POLL_INTERVAL).await;

                    let updated = this.update(&mut cx, |view, cx| {
                        if view.remote_access_status_poll_generation != generation {
                            return false;
                        }
                        if !gateway_settings::settings_action_matches_connection(
                            connection_id,
                            connection_epoch,
                            view.gateway.ws_connection_id,
                            view.gateway.connection_epoch,
                        ) {
                            return false;
                        }
                        if !gateway_settings::remote_access_status_needs_poll(
                            view.gateway.settings.as_ref(),
                        ) {
                            return false;
                        }

                        view.refresh_gateway_settings(cx);
                        true
                    });

                    match updated {
                        Ok(true) => {}
                        _ => break,
                    }
                }
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
    fn production_lifecycle_source() -> &'static str {
        include_str!("lifecycle.rs")
            .split("#[cfg(test)]")
            .next()
            .expect("production source segment exists")
    }

    #[test]
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

    #[test]
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

    #[test]
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
}
