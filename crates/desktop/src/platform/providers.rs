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
pub(crate) fn provider_config(client: Arc<ClientCore>, cx: &App) -> ProviderCatalogConfig {
    let platform = Arc::new(DesktopProviderPlatform);
    ProviderCatalogConfig::new(
        client,
        cx.global::<crate::client_runtime::DesktopRuntimeCoordinator>()
            .registrar(),
        platform.clone(),
        platform,
    )
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
