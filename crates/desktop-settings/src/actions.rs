use crate::{AppLanguagePreference, FileOpenerId, WindowThemePreference};
use crate::{model_selector::ModelSelectorSelection, screen::*};
use gpui_kit::{prelude::*, *};
use pioneer_client::settings::types::*;
use pioneer_client::settings::{
    gateway as gateway_policy, runtime::SettingsIntent, voice as voice_policy,
};
impl SettingsScreenView {
    fn platform_result(&mut self, result: anyhow::Result<()>) {
        if let Err(error) = result {
            self.pending_notification = Some(error.to_string());
            self.signal();
        }
    }
    fn intent(&self, intent: SettingsIntent) {
        self.config.client.settings_intent(intent);
    }
    pub fn refresh_gateway_settings(&mut self, _: &mut Context<Self>) {
        self.intent(SettingsIntent::Refresh);
    }
    pub fn refresh_auth_sessions(&mut self, _: &mut Context<Self>) {
        self.intent(SettingsIntent::RefreshSessions);
    }
    pub fn apply_keepawake_setting(&mut self, enabled: bool, _: &mut Context<Self>) {
        self.intent(SettingsIntent::Keepawake { enabled });
    }
    pub fn apply_telemetry_setting(&mut self, enabled: bool, _: &mut Context<Self>) {
        self.intent(SettingsIntent::Telemetry { enabled });
    }
    pub fn apply_preflight_model_setting(
        &mut self,
        selection: GatewayMemoryModelSelection,
        _: &mut Context<Self>,
    ) {
        self.intent(SettingsIntent::PreflightModel { selection });
    }
    pub fn toggle_remote_access_settings_expanded(&mut self) {
        self.remote_access_settings_expanded = !self.remote_access_settings_expanded;
    }
    pub fn apply_remote_access_setting(
        &mut self,
        enabled: bool,
        key: Option<String>,
        clear_key: bool,
        _: &mut Context<Self>,
    ) {
        self.intent(SettingsIntent::RemoteAccess {
            enabled,
            key,
            clear_key,
        });
    }
    pub fn save_remote_access_key_inline(&mut self, key: String, _: &mut Context<Self>) {
        if key.trim().is_empty() {
            return;
        }
        if let Some(s) = &self.gateway.settings {
            self.intent(SettingsIntent::RemoteAccess {
                enabled: s.remote_access.enabled,
                key: Some(key),
                clear_key: false,
            });
        }
    }
    pub fn apply_memory_setting(
        &mut self,
        toggle: MemorySettingToggle,
        enabled: bool,
        _: &mut Context<Self>,
    ) {
        self.intent(SettingsIntent::MemoryToggle { toggle, enabled });
    }
    pub fn apply_memory_model_setting(
        &mut self,
        setting: MemoryModelSetting,
        selection: GatewayMemoryModelSelection,
        _: &mut Context<Self>,
    ) {
        self.intent(SettingsIntent::MemoryModel { setting, selection });
    }
    pub fn apply_thread_episodic_setting(
        &mut self,
        _: gateway_policy::ThreadEpisodicSettingToggle,
        enabled: bool,
        _: &mut Context<Self>,
    ) {
        self.intent(SettingsIntent::ThreadEpisodic { enabled });
    }
    pub fn apply_vector_search_enabled(&mut self, enabled: bool, _: &mut Context<Self>) {
        self.intent(SettingsIntent::VectorEnabled { enabled });
    }
    pub fn apply_vector_search_use_search_instructions(
        &mut self,
        enabled: bool,
        _: &mut Context<Self>,
    ) {
        self.intent(SettingsIntent::VectorInstructions { enabled });
    }
    pub fn apply_vector_search_embedding_model_selection(
        &mut self,
        selection: ModelSelectorSelection,
        _: &mut Context<Self>,
    ) -> bool {
        self.intent(SettingsIntent::VectorModel { selection });
        true
    }
    pub fn apply_self_improvement_enabled(&mut self, enabled: bool, _: &mut Context<Self>) {
        self.intent(SettingsIntent::ImprovementEnabled { enabled });
    }
    pub fn apply_self_improvement_model_setting(
        &mut self,
        setting: SelfImprovementModelSetting,
        selection: Option<GatewaySelfImprovementModelSelection>,
        _: &mut Context<Self>,
    ) {
        self.intent(SettingsIntent::ImprovementModel { setting, selection });
    }
    pub fn apply_voice_input_enabled(
        &mut self,
        enabled: bool,
        _: &mut Context<Self>,
    ) -> VoiceInputEnableAction {
        let Some(s) = &self.gateway.settings else {
            return VoiceInputEnableAction::Noop;
        };
        if enabled
            && matches!(
                voice_policy::voice_input_enable_plan(&s.voice_input),
                voice_policy::VoiceInputSettingsPlan::NeedsSelection
            )
        {
            return VoiceInputEnableAction::NeedsSelection;
        }
        self.intent(SettingsIntent::VoiceEnabled { enabled });
        VoiceInputEnableAction::Sent
    }
    pub fn apply_voice_input_model_selection(
        &mut self,
        selection: ModelSelectorSelection,
        _: &mut Context<Self>,
    ) -> bool {
        self.intent(SettingsIntent::VoiceModel {
            provider: selection.provider,
            model: selection.model,
        });
        true
    }
    pub fn retry_voice_input_install(&mut self, _: &mut Context<Self>) -> bool {
        self.intent(SettingsIntent::VoiceRetry);
        true
    }
    pub fn active_workspace_file_opener(&self, cx: &App) -> FileOpenerId {
        self.config
            .platform
            .file_opener(self.active_workspace_id(), cx)
    }
    pub fn apply_workspace_file_opener(&mut self, value: FileOpenerId, cx: &mut Context<Self>) {
        let result = self
            .config
            .platform
            .set_file_opener(self.active_workspace_id(), value, cx);
        self.platform_result(result);
    }
    pub fn apply_language_setting(&mut self, value: AppLanguagePreference, cx: &mut Context<Self>) {
        let result = self.config.platform.set_language(value, cx);
        self.platform_result(result);
    }
    pub fn apply_theme_setting(
        &mut self,
        value: WindowThemePreference,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let result = self.config.platform.set_theme(value, window, cx);
        self.platform_result(result);
    }
}
