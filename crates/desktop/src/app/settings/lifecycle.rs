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
use pioneer_protocol::{GatewayMemoryModelSelection, GatewayMemorySettings, GatewaySettingsUpdate};
use tracing::warn;

impl PioneerDesktop {
    pub(in crate::app) fn open_settings_content_from_sidebar(
        &mut self,
        content_view: SettingsContentView,
        cx: &mut Context<Self>,
    ) {
        self.settings_content_view = content_view;
        self.sync_settings_sidebar_tree_state(cx);
        self.set_main_content_view(MainContentView::Settings, cx);
        if content_view == SettingsContentView::Memory {
            self.refresh_gateway_settings(cx);
        }
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
        let Some(mut memory) = self.current_gateway_memory_settings() else {
            self.refresh_gateway_settings(cx);
            return;
        };
        match toggle {
            MemorySettingToggle::Enabled => memory.enabled = enabled,
            MemorySettingToggle::ActiveRecall => memory.active_recall_enabled = enabled,
            MemorySettingToggle::ProactiveWrites => memory.proactive_writes_enabled = enabled,
            MemorySettingToggle::BackgroundExtraction => {
                memory.background_extraction_enabled = enabled
            }
            MemorySettingToggle::DebugTrace => memory.debug_trace_enabled = enabled,
        }

        self.apply_gateway_memory_settings(memory, cx);
    }

    pub(super) fn apply_memory_model_setting(
        &mut self,
        setting: MemoryModelSetting,
        model_selection: GatewayMemoryModelSelection,
        cx: &mut Context<Self>,
    ) {
        let Some(mut memory) = self.current_gateway_memory_settings() else {
            self.refresh_gateway_settings(cx);
            return;
        };
        match setting {
            MemoryModelSetting::ActiveRecallPlanner => memory.active_recall_model = model_selection,
            MemoryModelSetting::PostTurnExtractor => {
                memory.proactive_writes_model = model_selection
            }
        }

        self.apply_gateway_memory_settings(memory, cx);
    }

    pub(in crate::app) fn refresh_gateway_settings(&mut self, cx: &mut Context<Self>) {
        if self.gateway.settings_loading {
            return;
        }
        if self.gateway.connection_state != GatewayConnectionState::Connected {
            self.gateway.settings_error = Some("Gateway is not connected".to_owned());
            return;
        }
        let Some(connection_id) = self.gateway.ws_connection_id else {
            self.gateway.settings_error = Some("Gateway is not connected".to_owned());
            return;
        };

        self.gateway.settings_loading = true;
        self.gateway.settings_error = None;

        let ws_sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let result = cx
                    .background_spawn(async move { ws_sender.gateway_settings_get() })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if view.gateway.ws_connection_id != Some(connection_id) {
                        return;
                    }

                    view.gateway.settings_loading = false;
                    match result {
                        Ok(response) => {
                            view.gateway.settings = Some(response.settings);
                            view.gateway.settings_error = None;
                        }
                        Err(error) => {
                            view.gateway.settings_error = Some(format!("{error:#}"));
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
        self.gateway
            .settings
            .as_ref()
            .map(|settings| settings.memory.clone())
    }

    fn apply_gateway_memory_settings(
        &mut self,
        memory: GatewayMemorySettings,
        cx: &mut Context<Self>,
    ) {
        let Some(connection_id) = self.gateway.ws_connection_id else {
            warn!("cannot update gateway memory settings without an active gateway connection");
            return;
        };

        let mut snapshot = self.gateway.settings.clone().unwrap_or_else(|| {
            pioneer_protocol::GatewaySettingsSnapshot {
                memory: GatewayMemorySettings::default(),
            }
        });
        snapshot.memory = memory.clone();
        self.gateway.settings = Some(snapshot);
        self.gateway.settings_error = None;

        let ws_sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let result = cx
                    .background_spawn(async move {
                        ws_sender.gateway_settings_update(GatewaySettingsUpdate {
                            memory: Some(memory),
                        })
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if view.gateway.ws_connection_id != Some(connection_id) {
                        return;
                    }

                    match result {
                        Ok(response) => {
                            view.gateway.settings = Some(response.settings);
                            view.gateway.settings_error = None;
                        }
                        Err(error) => {
                            view.gateway.settings_error = Some(format!("{error:#}"));
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
}
