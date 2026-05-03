use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Component, Path};

use crate::helpers::normalize_non_empty;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GatewaySettings {
    version: u32,
    secrets: GatewaySecretsSettings,
    #[serde(default, skip_serializing_if = "McpSecretSettings::is_empty")]
    mcp: McpSecretSettings,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GatewaySecretsSettings {
    backend: GatewaySecretsBackend,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum GatewaySecretsBackend {
    Keystore,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct McpSecretSettings {
    #[serde(default)]
    secrets: std::collections::HashMap<String, String>,
}

impl Default for GatewaySecretsSettings {
    fn default() -> Self {
        Self {
            backend: GatewaySecretsBackend::Keystore,
        }
    }
}

impl GatewaySettings {
    pub(crate) fn secrets_backend(&self) -> GatewaySecretsBackend {
        self.secrets.backend
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

impl McpSecretSettings {
    fn is_empty(&self) -> bool {
        self.secrets.is_empty()
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
) -> Result<GatewaySettings> {
    if path.exists() {
        return load_gateway_settings(path, expected_version, settings_file_name);
    }

    let settings = GatewaySettings {
        version: expected_version,
        secrets: GatewaySecretsSettings::default(),
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

    if settings.secrets_backend() != GatewaySecretsBackend::Keystore {
        bail!(
            "unsupported gateway secrets backend in `{}`",
            path.display()
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
            secrets: GatewaySecretsSettings::default(),
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
    fn creates_sanitized_gateway_settings_without_jwt_or_provider_secrets() {
        let temp_dir = unique_temp_dir();
        fs::create_dir_all(&temp_dir).expect("create temp dir");
        let path = temp_dir.join("gateway-settings.toml");

        let _settings = load_or_create_gateway_settings(&path, 1, "gateway-settings.toml")
            .expect("settings should be created");
        let content = fs::read_to_string(&path).expect("read settings");

        assert!(content.contains("[secrets]"));
        assert!(content.contains("backend = \"keystore\""));
        assert!(!content.contains("jwt_secret"));
        assert!(!content.contains("[providers]"));
        assert!(!content.contains("[providers.keys]"));

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn rejects_gateway_settings_with_unsupported_version() {
        let temp_dir = unique_temp_dir();
        fs::create_dir_all(&temp_dir).expect("create temp dir");
        let path = temp_dir.join("gateway-settings.toml");
        fs::write(
            &path,
            r#"
version = 2

[secrets]
backend = "keystore"
"#,
        )
        .expect("write unsupported-version settings");

        let error = load_or_create_gateway_settings(&path, 1, "gateway-settings.toml")
            .expect_err("unsupported settings version should be rejected");
        assert!(
            format!("{error:#}").contains("unsupported gateway settings version"),
            "unexpected error: {error:#}"
        );

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn rejects_gateway_settings_with_removed_jwt_secret_field() {
        let temp_dir = unique_temp_dir();
        fs::create_dir_all(&temp_dir).expect("create temp dir");
        let path = temp_dir.join("gateway-settings.toml");
        fs::write(
            &path,
            r#"
version = 1
jwt_secret = "abcd"

[secrets]
backend = "keystore"
"#,
        )
        .expect("write settings with removed jwt field");

        let error = load_or_create_gateway_settings(&path, 1, "gateway-settings.toml")
            .expect_err("removed jwt_secret field should be rejected");
        assert!(
            format!("{error:#}").contains("unknown field `jwt_secret`"),
            "unexpected error: {error:#}"
        );

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn rejects_gateway_settings_with_removed_providers_field() {
        let temp_dir = unique_temp_dir();
        fs::create_dir_all(&temp_dir).expect("create temp dir");
        let path = temp_dir.join("gateway-settings.toml");
        fs::write(
            &path,
            r#"
version = 1

[secrets]
backend = "keystore"

[providers.keys]
openrouter = "sk-test"
"#,
        )
        .expect("write settings with removed providers field");

        let error = load_or_create_gateway_settings(&path, 1, "gateway-settings.toml")
            .expect_err("removed providers field should be rejected");
        assert!(
            format!("{error:#}").contains("unknown field `providers`"),
            "unexpected error: {error:#}"
        );

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn rejects_gateway_settings_with_unsupported_secret_backend() {
        let temp_dir = unique_temp_dir();
        fs::create_dir_all(&temp_dir).expect("create temp dir");
        let path = temp_dir.join("gateway-settings.toml");
        fs::write(
            &path,
            r#"
version = 1

[secrets]
backend = "db-keystore"
"#,
        )
        .expect("write settings with unsupported backend");

        let error = load_or_create_gateway_settings(&path, 1, "gateway-settings.toml")
            .expect_err("unsupported backend should be rejected");
        assert!(
            format!("{error:#}").contains("unknown variant `db-keystore`"),
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
