use anyhow::{Context, Result};
use pioneer_client::gateway::endpoint::GatewayBaseUrl;
use pioneer_client::gateway::migration::{GatewayRegistryLoad, load_registry_json};
use pioneer_client::gateway::registry::{
    CURRENT_GATEWAY_REGISTRY_VERSION, GatewayLocalRegistryConfig, GatewayRegistryConfig,
    default_registry as default_client_registry, normalize_registry as normalize_client_registry,
    setup_required as client_setup_required,
};
use pioneer_client::gateway::types::GatewayRegistry;
use pioneer_config::AppConfig;
use pioneer_keystore::{ensure_private_file, ensure_private_runtime_dir};
use pioneer_protocol::generate_id;
use serde::Deserialize;
use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

const INSTALLATION_ID_LEN: usize = 21;
static REGISTRY_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[derive(Deserialize)]
struct RegistryVersion {
    version: u32,
}

#[derive(Debug)]
pub(crate) struct LoadedGatewayRegistry {
    pub(crate) registry: GatewayRegistry,
}

#[derive(Debug)]
pub(crate) struct GatewayRegistryReconfigurationRequired {
    pub(crate) endpoint_ids: Vec<String>,
}

impl std::fmt::Display for GatewayRegistryReconfigurationRequired {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "Gateway registry endpoints require reconfiguration: {}",
            self.endpoint_ids.join(", ")
        )
    }
}

impl std::error::Error for GatewayRegistryReconfigurationRequired {}

pub(crate) fn load_registry_for_runtime(
    path: &Path,
    config: &AppConfig,
) -> Result<LoadedGatewayRegistry> {
    let mut registry = if path.exists() {
        let path_display = path.display().to_string();
        let content = fs::read_to_string(path).with_context(|| {
            t!("errors.registry.read_failed", path = path_display.as_str()).to_string()
        })?;
        let version = toml::from_str::<RegistryVersion>(&content)
            .with_context(|| {
                t!("errors.registry.parse_failed", path = path_display.as_str()).to_string()
            })?
            .version;
        if version == CURRENT_GATEWAY_REGISTRY_VERSION {
            toml::from_str::<GatewayRegistry>(&content).with_context(|| {
                t!("errors.registry.parse_failed", path = path_display.as_str()).to_string()
            })?
        } else if version == 2 {
            migrate_registry_v2(content.as_str(), path)?
        } else {
            anyhow::bail!(
                "{}",
                t!(
                    "errors.registry.unsupported_version",
                    version = version,
                    path = path_display.as_str(),
                        current_registry_version = CURRENT_GATEWAY_REGISTRY_VERSION
                    )
                );
        }
    } else {
        default_registry(config)?
    };

    normalize_registry(&mut registry, config)?;
    ensure_installation_id(&mut registry);
    save_registry(path, &registry)?;

    Ok(LoadedGatewayRegistry { registry })
}

#[cfg(test)]
pub(crate) fn load_registry(path: &Path, config: &AppConfig) -> Result<GatewayRegistry> {
    Ok(load_registry_for_runtime(path, config)?.registry)
}

pub(crate) fn save_registry(path: &Path, registry: &GatewayRegistry) -> Result<()> {
    let content = toml::to_string_pretty(registry)
        .context(t!("errors.registry.serialize_failed").to_string())?;
    let path_display = path.display().to_string();
    write_private_registry_file(path, content.as_str()).with_context(|| {
        t!("errors.registry.write_failed", path = path_display.as_str()).to_string()
    })
}

pub(crate) fn default_registry(config: &AppConfig) -> Result<GatewayRegistry> {
    let mut registry = default_client_registry(&registry_config(config)?);
    ensure_installation_id(&mut registry);
    Ok(registry)
}

pub(crate) fn setup_required(registry: &GatewayRegistry) -> bool {
    client_setup_required(registry)
}

pub(crate) fn normalize_registry(registry: &mut GatewayRegistry, config: &AppConfig) -> Result<()> {
    let registry_config = registry_config(config)?;
    normalize_client_registry(registry, &registry_config, |index| {
        t!("gateway.endpoint.remote_name", index = index).to_string()
    })
    .map_err(anyhow::Error::new)
}

fn ensure_installation_id(registry: &mut GatewayRegistry) {
    if registry
        .installation_id
        .as_deref()
        .is_none_or(|value| value.trim().is_empty())
    {
        registry.installation_id = Some(generate_id(INSTALLATION_ID_LEN));
    }
}

fn migrate_registry_v2(content: &str, path: &Path) -> Result<GatewayRegistry> {
    let legacy: toml::Value = toml::from_str(content)
        .with_context(|| format!("failed to parse Gateway registry v2 `{}`", path.display()))?;
    let json = serde_json::to_string(&legacy)
        .context("failed to project Gateway registry v2 into the migration boundary")?;
    match load_registry_json(json.as_str()).map_err(anyhow::Error::new)? {
        GatewayRegistryLoad::Migrated(registry) => Ok(registry),
        GatewayRegistryLoad::ReconfigurationRequired { endpoint_ids } => {
            Err(GatewayRegistryReconfigurationRequired { endpoint_ids }.into())
        }
        GatewayRegistryLoad::Current(_) => {
            anyhow::bail!("Gateway registry v2 migration returned a v3 document")
        }
    }
}

fn registry_config(config: &AppConfig) -> Result<GatewayRegistryConfig> {
    let local_gateway_id = local_gateway_id(config);
    let local_gateway_base_url =
        GatewayBaseUrl::from_local_listen_addr(config.gateway.listen_addr.as_str())
            .context("failed to derive the local Gateway client base URL")?;

    Ok(GatewayRegistryConfig {
        local: Some(GatewayLocalRegistryConfig {
            gateway_id: local_gateway_id.to_owned(),
            name: t!("gateway.endpoint.local_name").to_string(),
            // A bind gateway_base_url such as 0.0.0.0 is not a safe authenticated
            // client destination. The shared resolver maps unspecified binds
            // to loopback while preserving the configured port.
            gateway_base_url: local_gateway_base_url,
            service_name: Some(config.gateway.service_name.trim().to_owned()),
        }),
    })
}

fn local_gateway_id(config: &AppConfig) -> &str {
    config.desktop.gateway.local_gateway_id.trim()
}

fn write_private_registry_file(path: &Path, content: &str) -> Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    if parent != Path::new(".") {
        ensure_private_runtime_dir(parent).with_context(|| {
            format!(
                "failed to harden gateway registry directory {}",
                parent.display()
            )
        })?;
    }

    let sequence = REGISTRY_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let temp_path = path.with_extension(format!(
        "tmp.{}.{}.{}",
        std::process::id(),
        timestamp,
        sequence
    ));
    let write_result = (|| -> Result<()> {
        let mut temp = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp_path)
            .with_context(|| {
                format!(
                    "failed to create temporary gateway registry file {}",
                    temp_path.display()
                )
            })?;
        temp.write_all(content.as_bytes()).with_context(|| {
            format!(
                "failed to write temporary gateway registry file {}",
                temp_path.display()
            )
        })?;
        temp.sync_all().with_context(|| {
            format!(
                "failed to sync temporary gateway registry file {}",
                temp_path.display()
            )
        })?;
        ensure_private_file(&temp_path).with_context(|| {
            format!(
                "failed to harden temporary gateway registry file {}",
                temp_path.display()
            )
        })?;
        temp.sync_all().with_context(|| {
            format!(
                "failed to sync hardened temporary gateway registry file {}",
                temp_path.display()
            )
        })?;
        replace_registry_file(&temp_path, path).with_context(|| {
            format!(
                "failed to atomically replace gateway registry file {}",
                path.display()
            )
        })?;
        sync_registry_parent(parent)?;
        Ok(())
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }
    write_result?;
    ensure_private_file(path)
        .with_context(|| format!("failed to harden gateway registry file {}", path.display()))?;
    Ok(())
}

#[cfg(not(windows))]
fn replace_registry_file(source: &Path, destination: &Path) -> std::io::Result<()> {
    fs::rename(source, destination)
}

#[cfg(windows)]
fn replace_registry_file(source: &Path, destination: &Path) -> std::io::Result<()> {
    use std::{iter, os::windows::ffi::OsStrExt};
    use windows_sys::Win32::Storage::FileSystem::{
        MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
    };

    let source = source
        .as_os_str()
        .encode_wide()
        .chain(iter::once(0))
        .collect::<Vec<_>>();
    let destination = destination
        .as_os_str()
        .encode_wide()
        .chain(iter::once(0))
        .collect::<Vec<_>>();
    let result = unsafe {
        MoveFileExW(
            source.as_ptr(),
            destination.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if result == 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(unix)]
fn sync_registry_parent(parent: &Path) -> Result<()> {
    fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .with_context(|| {
            format!(
                "failed to sync gateway registry directory {}",
                parent.display()
            )
        })
}

#[cfg(not(unix))]
fn sync_registry_parent(_parent: &Path) -> Result<()> {
    Ok(())
}
