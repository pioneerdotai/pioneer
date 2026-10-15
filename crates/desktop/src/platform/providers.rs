use gpui_kit::{App, ClipboardItem};
use pioneer_client::core::ClientCore;
use pioneer_desktop_providers::*;
use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
};
struct DesktopProviderPlatform;
impl ProviderCredentialPort for DesktopProviderPlatform {
    fn copy(
        &self,
        identity: ProviderEffectIdentity,
        value: &str,
        cx: &mut App,
    ) -> ProviderEffectCompletion {
        cx.write_to_clipboard(ClipboardItem::new_string(value.to_owned()));
        ProviderEffectCompletion::new(identity, true)
    }
}
impl ProviderExternalNavigationPort for DesktopProviderPlatform {
    fn open_path(
        &self,
        identity: ProviderEffectIdentity,
        path: &str,
        _: &mut App,
    ) -> ProviderEffectCompletion {
        ProviderEffectCompletion::new(
            identity,
            expand_cli_runtime_provider_path(path)
                .and_then(|path| open_path(&path))
                .is_ok(),
        )
    }
}
struct DesktopProviderModelPicker {
    client: Arc<ClientCore>,
    registrar: Arc<dyn pioneer_desktop_foundation::ClientBindingRegistrar>,
}
impl ProviderModelPickerPort for DesktopProviderModelPicker {
    fn open(
        &self,
        request: ProviderModelPickerRequest,
        window: &mut gpui_kit::Window,
        cx: &mut App,
    ) {
        pioneer_desktop_settings::open_shared_model_selector(
            pioneer_desktop_settings::SharedModelSelectorOptions::new(
                request.title().to_owned(),
                request.workspace_id().to_owned(),
                self.client.clone(),
                self.registrar.clone(),
                request.on_save(),
            )
            .selection(request.selection().clone())
            .fixed_provider()
            .on_refresh(request.on_refresh()),
            window,
            cx,
        );
    }
}
pub(crate) fn provider_config(client: Arc<ClientCore>, cx: &App) -> ProviderCatalogConfig {
    let platform = Arc::new(DesktopProviderPlatform);
    let registrar = cx
        .global::<crate::client_runtime::DesktopRuntimeCoordinator>()
        .registrar();
    let picker = Arc::new(DesktopProviderModelPicker {
        client: client.clone(),
        registrar: registrar.clone(),
    });
    ProviderCatalogConfig::new(client, registrar, platform.clone(), platform, picker)
}
fn expand_cli_runtime_provider_path(raw: &str) -> anyhow::Result<PathBuf> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        anyhow::bail!("path must not be empty");
    }
    let expanded = if trimmed == "~" {
        home_dir()?
    } else if let Some(rest) = trimmed.strip_prefix("~/") {
        home_dir()?.join(rest)
    } else if trimmed.starts_with('~') {
        anyhow::bail!("unsupported home expansion in `{raw}`");
    } else {
        PathBuf::from(trimmed)
    };
    if expanded.is_absolute() {
        Ok(expanded)
    } else {
        Ok(std::env::current_dir()?.join(expanded))
    }
}

fn home_dir() -> anyhow::Result<PathBuf> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| anyhow::anyhow!("HOME is not set"))
}

fn open_path(path: &Path) -> anyhow::Result<()> {
    spawn_open_path(path)
}

#[cfg(target_os = "macos")]
fn spawn_open_path(path: &Path) -> anyhow::Result<()> {
    Command::new("open").arg(path).spawn()?;
    Ok(())
}

#[cfg(all(unix, not(target_os = "macos")))]
fn spawn_open_path(path: &Path) -> anyhow::Result<()> {
    Command::new("xdg-open").arg(path).spawn()?;
    Ok(())
}

#[cfg(windows)]
fn spawn_open_path(path: &Path) -> anyhow::Result<()> {
    Command::new("explorer").arg(path).spawn()?;
    Ok(())
}
