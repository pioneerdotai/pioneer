use anyhow::{Context, Result, bail};
use rand::RngCore;
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Component, Path};

use crate::helpers::{encode_hex, normalize_non_empty};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct GatewaySettings {
    version: u32,
    jwt_secret: String,
    #[serde(default)]
    providers: ProviderSettings,
    #[serde(default)]
    mcp: McpSecretSettings,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct ProviderSettings {
    #[serde(default)]
    keys: HashMap<String, String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct McpSecretSettings {
    #[serde(default)]
    secrets: HashMap<String, String>,
}

impl GatewaySettings {
    pub(crate) fn jwt_secret(&self) -> &str {
        self.jwt_secret.as_str()
    }

    pub(crate) fn provider_api_key(&self, provider_name: &str) -> Option<&str> {
        self.providers
            .keys
            .get(provider_name)
            .map(String::as_str)
            .filter(|v| !v.is_empty())
    }

    pub(crate) fn set_provider_api_key(&mut self, provider_name: &str, api_key: String) {
        self.providers
            .keys
            .insert(provider_name.to_owned(), api_key);
    }

    pub(crate) fn delete_provider_api_key(&mut self, provider_name: &str) -> bool {
        self.providers.keys.remove(provider_name).is_some()
    }

    pub(crate) fn configured_provider_names(&self) -> Vec<String> {
        self.providers
            .keys
            .iter()
            .filter(|(_, v)| !v.is_empty())
            .map(|(k, _)| k.clone())
            .collect()
    }

    pub(crate) fn set_mcp_secret(&mut self, ref_id: &str, value: String) {
        self.mcp.secrets.insert(ref_id.to_owned(), value);
    }

    #[allow(dead_code)]
    pub(crate) fn mcp_secret(&self, ref_id: &str) -> Option<&str> {
        self.mcp
            .secrets
            .get(ref_id)
            .map(String::as_str)
            .filter(|value| !value.is_empty())
    }
}

pub(crate) fn normalize_settings_file_name(value: &str) -> Result<String> {
    let trimmed = normalize_non_empty(value, "settings_file_name must not be empty")?;
    let path = Path::new(trimmed.as_str());

    if path.is_absolute() {
        bail!("settings_file_name must be relative");
    }

    if path.components().any(is_disallowed_component) {
        bail!("settings_file_name must not contain parent or root components");
    }

    Ok(trimmed)
}

pub(crate) fn load_or_create_gateway_settings(
    path: &Path,
    expected_version: u32,
    settings_file_name: &str,
    secret_size_bytes: usize,
) -> Result<GatewaySettings> {
    if path.exists() {
        return load_gateway_settings(path, expected_version, settings_file_name);
    }

    let mut secret = vec![0u8; secret_size_bytes];
    OsRng.fill_bytes(secret.as_mut_slice());

    let settings = GatewaySettings {
        version: expected_version,
        jwt_secret: encode_hex(secret.as_slice()),
        providers: ProviderSettings::default(),
        mcp: McpSecretSettings::default(),
    };

    save_gateway_settings(path, &settings)?;
    Ok(settings)
}

fn load_gateway_settings(
    path: &Path,
    expected_version: u32,
    _settings_file_name: &str,
) -> Result<GatewaySettings> {
    let path_display = path.display().to_string();
    let content = fs::read_to_string(path)
        .with_context(|| format!("failed to read gateway settings `{path_display}`"))?;

    let settings = toml::from_str::<GatewaySettings>(&content)
        .with_context(|| format!("failed to parse gateway settings `{path_display}`"))?;

    if settings.version != expected_version {
        bail!(
            "unsupported gateway settings version `{}` in `{}`; expected `{}`",
            settings.version,
            path.display(),
            expected_version
        );
    }

    Ok(settings)
}

pub(crate) fn save_gateway_settings(path: &Path, settings: &GatewaySettings) -> Result<()> {
    let content =
        toml::to_string_pretty(settings).context("failed to serialize gateway settings")?;
    write_settings_file(path, content.as_str())
}

fn write_settings_file(path: &Path, content: &str) -> Result<()> {
    let path_display = path.display().to_string();
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .map(|name| name.to_string_lossy())
        .unwrap_or_else(|| "gateway-settings.toml".into());
    let now_nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let temp_path = parent.join(format!(
        ".{file_name}.{}.{}.tmp",
        std::process::id(),
        now_nanos
    ));

    fs::write(&temp_path, content)
        .with_context(|| format!("failed to write `{}`", temp_path.display()))?;
    set_private_permissions(&temp_path)?;
    if let Err(error) = fs::rename(&temp_path, path) {
        let _ = fs::remove_file(&temp_path);
        return Err(error).with_context(|| format!("failed to replace `{path_display}`"));
    }
    set_private_permissions(path)?;
    Ok(())
}

#[cfg(unix)]
fn set_private_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let mut permissions = fs::metadata(path)
        .with_context(|| format!("failed to read metadata for `{}`", path.display()))?
        .permissions();
    permissions.set_mode(0o600);
    fs::set_permissions(path, permissions)
        .with_context(|| format!("failed to set permissions for `{}`", path.display()))
}

#[cfg(not(unix))]
fn set_private_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

fn is_disallowed_component(component: Component<'_>) -> bool {
    matches!(
        component,
        Component::ParentDir | Component::RootDir | Component::Prefix(_)
    )
}

#[cfg(test)]
impl GatewaySettings {
    pub(crate) fn default_for_tests() -> Self {
        Self {
            version: 1,
            jwt_secret: "00".repeat(32),
            providers: ProviderSettings::default(),
            mcp: McpSecretSettings::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::load_or_create_gateway_settings;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn rejects_gateway_settings_with_unsupported_version() {
        let temp_dir = unique_temp_dir();
        fs::create_dir_all(&temp_dir).expect("create temp dir");
        let path = temp_dir.join("gateway-settings.toml");
        fs::write(
            &path,
            r#"
version = 2
jwt_secret = "abcd"
"#,
        )
        .expect("write unsupported-version settings");

        let error = load_or_create_gateway_settings(&path, 1, "gateway-settings.toml", 64)
            .expect_err("unsupported settings version should be rejected");
        assert!(
            format!("{error:#}").contains("unsupported gateway settings version"),
            "unexpected error: {error:#}"
        );

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn rejects_gateway_settings_with_major_version_mismatch() {
        let temp_dir = unique_temp_dir();
        fs::create_dir_all(&temp_dir).expect("create temp dir");
        let path = temp_dir.join("gateway-settings.toml");
        fs::write(
            &path,
            r#"
version = 99
jwt_secret = "abcd"
"#,
        )
        .expect("write old settings");

        let error = load_or_create_gateway_settings(&path, 1, "gateway-settings.toml", 64)
            .expect_err("unsupported settings version should be rejected");
        assert!(
            format!("{error:#}").contains("unsupported gateway settings version"),
            "unexpected error: {error:#}"
        );

        let _ = fs::remove_dir_all(temp_dir);
    }

    fn unique_temp_dir() -> PathBuf {
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before unix epoch")
            .as_nanos();
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("pioneer-settings-tests-{nanos}-{id}"))
    }
}
