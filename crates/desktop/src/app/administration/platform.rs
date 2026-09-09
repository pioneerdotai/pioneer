use gpui_kit::{AppContext, App, ClipboardItem, Task};
use pioneer_client::{core::ClientCore, avatars::{AvatarCacheRequest, AvatarCacheResult, AvatarCacheError}};
use pioneer_desktop_administration::{AdministrationConfig, AdministrationAvatarPort, AdministrationActivationPort, AdministrationExternalNavigationPort, AdministrationEffectIdentity, AdministrationEffectCompletion};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

struct DesktopAdministrationPlatform { client: Arc<ClientCore> }
impl AdministrationAvatarPort for DesktopAdministrationPlatform {
    fn resolve(&self, request: AvatarCacheRequest, cancellation: CancellationToken, cx: &mut App) -> Task<Result<AvatarCacheResult, AvatarCacheError>> {
        let client = self.client.clone();
        cx.background_spawn(async move {
            let home = crate::state::runtime_home_dir().map_err(|_| AvatarCacheError::Offline)?;
            let service = client.avatar_cache_service(home)?;
            let runtime = tokio::runtime::Runtime::new().map_err(|_| AvatarCacheError::Offline)?;
            runtime.block_on(client.resolve_member_avatar(&service, request, cancellation))
        })
    }
}
impl AdministrationActivationPort for DesktopAdministrationPlatform {
    fn copy(&self, identity: AdministrationEffectIdentity, value: &str, cx: &mut App) -> AdministrationEffectCompletion {
        cx.write_to_clipboard(ClipboardItem::new_string(value.to_owned()));
        AdministrationEffectCompletion::new(identity, true)
    }
}
impl AdministrationExternalNavigationPort for DesktopAdministrationPlatform {
    fn open(&self, identity: AdministrationEffectIdentity, uri: &str, cx: &mut App) -> AdministrationEffectCompletion {
        cx.open_url(uri);
        AdministrationEffectCompletion::new(identity, true)
    }
}
pub(crate) fn administration_config(client: Arc<ClientCore>, cx: &App) -> AdministrationConfig {
    let platform = Arc::new(DesktopAdministrationPlatform { client: client.clone() });
    AdministrationConfig::new(client, cx.global::<crate::client_runtime::DesktopRuntimeCoordinator>().registrar(), platform.clone(), platform.clone(), platform)
}
