use crate::{
    app::root::{MainContentView, PioneerDesktop, SettingsContentView},
    app::settings::{MemorySettingToggle, SETTINGS_CONTENT_GENERAL_NODE_ID},
    settings::{self, AppLanguagePreference, WindowThemePreference},
    window,
};
use gpui::{prelude::*, *};
use gpui_component::{
    theme::{Theme, ThemeMode},
    tree::TreeItem,
};
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
    }

    pub(in crate::app) fn sync_settings_sidebar_tree_state(&mut self, cx: &mut Context<Self>) {
        let selected_ix = match self.settings_content_view {
            SettingsContentView::General => Some(0),
        };
        let settings_tree_state = self.settings_tree_state.clone();
        settings_tree_state.update(cx, |state, cx| {
            state.set_items(
                vec![TreeItem::new(SETTINGS_CONTENT_GENERAL_NODE_ID, "general")],
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
        let mut memory = settings::memory_settings(cx);
        match toggle {
            MemorySettingToggle::Enabled => memory.enabled = enabled,
            MemorySettingToggle::ProactiveWrites => memory.proactive_writes_enabled = enabled,
            MemorySettingToggle::ActiveRecall => memory.active_recall_enabled = enabled,
            MemorySettingToggle::DebugTrace => memory.debug_trace_enabled = enabled,
        }

        if let Err(error) = settings::set_memory_settings(cx, memory) {
            warn!(
                error = %format!("{error:#}"),
                "failed to save memory settings"
            );
        }
    }
}
