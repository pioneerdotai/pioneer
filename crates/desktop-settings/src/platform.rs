use gpui_kit::{App, Window};
use pioneer_desktop_foundation::{
    file_opener::FileOpenerId,
    preferences::{AppLanguagePreference, WindowThemePreference},
};
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

pub use pioneer_desktop_foundation::profile_photo::{
    ProfilePhotoError as SettingsPhotoError, ProfilePhotoPort as SettingsPhotoPort,
    ProfilePhotoSelection as SettingsPhotoSelection,
};
