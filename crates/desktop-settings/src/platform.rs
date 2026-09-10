use crate::{AppLanguagePreference, FileOpenerId, WindowThemePreference};
use gpui_kit::{App, Window};
/// Desktop preferences and existing native integrations. Domain saves use Client intents.
pub trait SettingsPlatform {
    fn telemetry(&self, enabled: bool);
    fn language(&self, cx: &App) -> AppLanguagePreference;
    fn theme(&self, cx: &App) -> WindowThemePreference;
    fn set_language(&self, value: AppLanguagePreference, cx: &mut App) -> anyhow::Result<()>;
    fn set_theme(
        &self,
        value: WindowThemePreference,
        window: &mut Window,
        cx: &mut App,
    ) -> anyhow::Result<()>;
    fn file_opener(&self, workspace: Option<&str>, cx: &App) -> FileOpenerId;
    fn available_file_openers(&self) -> Vec<FileOpenerId>;
    fn set_file_opener(
        &self,
        workspace: Option<&str>,
        value: FileOpenerId,
        cx: &mut App,
    ) -> anyhow::Result<()>;
    fn avatar_path(&self, principal: &str, cx: &App) -> Option<std::path::PathBuf>;
}

pub struct SettingsPhotoSelection {
    pub preview: String,
    pub avatar: pioneer_client::settings::types::ProfileAvatarInput,
}
pub enum SettingsPhotoError {
    Picker,
    InvalidAvatar,
}
pub trait SettingsPhotoPort {
    fn select(
        &self,
        cx: &mut gpui_kit::App,
    ) -> gpui_kit::Task<Result<Option<SettingsPhotoSelection>, SettingsPhotoError>>;
}
