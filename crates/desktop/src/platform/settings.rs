use crate::{
    file_opener::FileOpenerId,
    settings::{self, AppLanguagePreference, FileOpenerWorkspaceScope, WindowThemePreference},
};
use gpui_kit::{App, Window};
use pioneer_client::core::{ClientCore, ClientScope};
use pioneer_desktop_settings::{SettingsConfig, SettingsPlatform};
use std::{rc::Rc, sync::Arc};
struct DesktopSettingsPlatform {
    client: Arc<ClientCore>,
}
impl DesktopSettingsPlatform {
    fn scope(&self, workspace: Option<&str>) -> Option<FileOpenerWorkspaceScope> {
        let identity=self.client.snapshot(&ClientScope::Administration{workspace_id:None})?.typed::<pioneer_client::gateway::identity_authorization::IdentityAuthorizationPublication>()?;
        Some(FileOpenerWorkspaceScope {
            principal_id: identity
                .payload()
                .current_auth
                .as_ref()?
                .principal
                .id
                .to_string(),
            gateway_id: identity.payload().endpoint_id.clone()?,
            workspace_id: workspace?.into(),
        })
    }
}
impl SettingsPlatform for DesktopSettingsPlatform {
    fn language(&self, cx: &App) -> AppLanguagePreference {
        settings::app_language(cx)
    }
    fn theme(&self, cx: &App) -> WindowThemePreference {
        settings::window_theme(cx)
    }
    fn set_language(&self, value: AppLanguagePreference, cx: &mut App) -> anyhow::Result<()> {
        settings::set_app_language(cx, value)?;
        rust_i18n::set_locale(&settings::resolve_language_locale(value));
        cx.refresh_windows();
        Ok(())
    }
    fn set_theme(
        &self,
        value: WindowThemePreference,
        window: &mut Window,
        cx: &mut App,
    ) -> anyhow::Result<()> {
        use gpui_kit::component::theme::{Theme, ThemeMode};
        settings::set_window_theme(cx, value)?;
        match value {
            WindowThemePreference::System => Theme::sync_system_appearance(None, cx),
            WindowThemePreference::Light => Theme::change(ThemeMode::Light, Some(window), cx),
            WindowThemePreference::Dark => Theme::change(ThemeMode::Dark, Some(window), cx),
        }
        Ok(())
    }
    fn file_opener(&self, workspace: Option<&str>, cx: &App) -> FileOpenerId {
        self.scope(workspace)
            .map(|scope| {
                crate::file_opener::available_or_file_manager(settings::workspace_file_opener(
                    cx, &scope,
                ))
            })
            .unwrap_or_default()
    }
    fn available_file_openers(&self) -> Vec<FileOpenerId> {
        crate::file_opener::available_file_openers()
            .iter()
            .map(|opener| opener.id)
            .collect()
    }
    fn set_file_opener(
        &self,
        workspace: Option<&str>,
        value: FileOpenerId,
        cx: &mut App,
    ) -> anyhow::Result<()> {
        let scope = self
            .scope(workspace)
            .ok_or_else(|| anyhow::anyhow!("workspace preference scope is unavailable"))?;
        settings::set_workspace_file_opener(cx, &scope, value)
    }
    fn telemetry(&self, enabled: bool) {
        pioneer_observability::set_telemetry_enabled(enabled);
    }
}
impl pioneer_desktop_settings::SettingsAvatarPort for DesktopSettingsPlatform {
    fn resolve(
        &self,
        request: pioneer_client::avatars::AvatarCacheRequest,
        cancellation: tokio_util::sync::CancellationToken,
        cx: &mut App,
    ) -> gpui_kit::Task<
        Result<
            pioneer_client::avatars::AvatarCacheResult,
            pioneer_client::avatars::AvatarCacheError,
        >,
    > {
        use gpui_kit::AppContext;
        use pioneer_client::avatars::AvatarCacheError;
        let client = self.client.clone();
        cx.background_spawn(async move {
            let home = crate::state::runtime_home_dir().map_err(|_| AvatarCacheError::Offline)?;
            let service = client.avatar_cache_service(home)?;
            let runtime = tokio::runtime::Runtime::new().map_err(|_| AvatarCacheError::Offline)?;
            runtime.block_on(client.resolve_member_avatar(&service, request, cancellation))
        })
    }
}
pub(crate) fn settings_config(client: Arc<ClientCore>, cx: &App) -> SettingsConfig {
    let platform = Rc::new(DesktopSettingsPlatform {
        client: client.clone(),
    });
    SettingsConfig {
        platform: platform.clone(),
        avatars: platform.clone(),
        photos: Rc::new(crate::profile_photo::DesktopProfilePhotoPort),
        client,
        bindings: cx
            .global::<crate::client_runtime::DesktopRuntimeCoordinator>()
            .registrar(),
    }
}
