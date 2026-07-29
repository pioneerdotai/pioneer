use anyhow::{Context, Result};
use pioneer_client::gateway::connectivity::resolve_gateway_socket_addrs;
use pioneer_client::gateway::registry::{
    GATEWAY_REGISTRY_V2, GatewayLocalRegistryConfig, GatewayRegistryConfig,
    default_registry as default_client_registry, normalize_registry as normalize_client_registry,
    setup_required as client_setup_required,
};
use pioneer_client::gateway::types::{GatewayEndpoint, GatewayEndpointKind, GatewayRegistry};
use pioneer_config::AppConfig;
use pioneer_keystore::{ensure_private_file, ensure_private_runtime_dir};
use pioneer_protocol::generate_id;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

const LEGACY_GATEWAY_REGISTRY_V1: u32 = 1;
const REGISTRY_UPGRADE_STATE_VERSION: u32 = 1;
const INSTALLATION_ID_LEN: usize = 21;
static REGISTRY_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[derive(Deserialize)]
struct RegistryVersion {
    version: u32,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GatewayRegistryV1 {
    version: u32,
    active_gateway_id: Option<String>,
    #[serde(default)]
    local: Option<GatewayEndpointV1>,
    #[serde(default)]
    remotes: Vec<GatewayEndpointV1>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GatewayEndpointV1 {
    id: String,
    name: String,
    address: String,
    kind: GatewayEndpointKind,
    #[serde(default, rename = "auth_token_ref")]
    _auth_token_ref: Option<String>,
    #[serde(default)]
    workspace_id: Option<String>,
    service_name: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct GatewayRegistryUpgradeState {
    version: u32,
    installation_id: String,
}

pub(crate) struct LoadedGatewayRegistry {
    pub(crate) registry: GatewayRegistry,
    pub(crate) upgrade_pending: bool,
}

pub(crate) fn load_registry_for_runtime(
    path: &Path,
    config: &AppConfig,
) -> Result<LoadedGatewayRegistry> {
    let mut upgrade_pending = false;
    let mut registry = if path.exists() {
        let path_display = path.display().to_string();
        let content = fs::read_to_string(path).with_context(|| {
            t!("errors.registry.read_failed", path = path_display.as_str()).to_string()
        })?;
        let current_registry_version = current_registry_version(config);
        let version = toml::from_str::<RegistryVersion>(&content)
            .with_context(|| {
                t!("errors.registry.parse_failed", path = path_display.as_str()).to_string()
            })?
            .version;
        if version < current_registry_version {
            if version != LEGACY_GATEWAY_REGISTRY_V1 {
                anyhow::bail!(
                    "{}",
                    t!(
                        "errors.registry.unsupported_version",
                        version = version,
                        path = path_display.as_str(),
                        current_registry_version = current_registry_version
                    )
                );
            }
            upgrade_pending = true;
            migrate_registry_v1(content.as_str(), path, config)?
        } else if version > current_registry_version {
            anyhow::bail!(
                "{}",
                t!(
                    "errors.registry.unsupported_version",
                    version = version,
                    path = path_display.as_str(),
                    current_registry_version = current_registry_version
                )
            );
        } else {
            let registry = toml::from_str::<GatewayRegistry>(&content).with_context(|| {
                t!("errors.registry.parse_failed", path = path_display.as_str()).to_string()
            })?;
            if let Some(state) = load_registry_upgrade_state(path)? {
                validate_registry_upgrade_state(&state, &registry, path)?;
                upgrade_pending = true;
            }
            registry
        }
    } else {
        let mut registry = default_registry(config)?;
        if let Some(state) = load_registry_upgrade_state(path)? {
            registry.installation_id = Some(state.installation_id);
            upgrade_pending = true;
        }
        registry
    };

    normalize_registry(&mut registry, config)?;
    ensure_installation_id(&mut registry);
    if !upgrade_pending {
        save_registry(path, &registry)?;
    }

    Ok(LoadedGatewayRegistry {
        registry,
        upgrade_pending,
    })
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

pub(crate) fn complete_registry_upgrade(path: &Path) -> Result<()> {
    let state_path = registry_upgrade_state_path(path);
    match fs::remove_file(&state_path) {
        Ok(()) => {
            let parent = state_path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."));
            sync_registry_parent(parent).with_context(|| {
                format!(
                    "failed to sync completed Gateway registry upgrade state removal in {}",
                    parent.display()
                )
            })
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| {
            format!(
                "failed to remove completed Gateway registry upgrade state {}",
                state_path.display()
            )
        }),
    }
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

fn migrate_registry_v1(content: &str, path: &Path, config: &AppConfig) -> Result<GatewayRegistry> {
    let legacy = toml::from_str::<GatewayRegistryV1>(content).with_context(|| {
        format!(
            "failed to parse Gateway registry v1 `{}` for upgrade",
            path.display()
        )
    })?;
    if legacy.version != LEGACY_GATEWAY_REGISTRY_V1 {
        anyhow::bail!(
            "Gateway registry v1 decoder received version `{}` from `{}`",
            legacy.version,
            path.display()
        );
    }

    let local_gateway_id = local_gateway_id(config);
    let local_workspace_id = legacy
        .local
        .as_ref()
        .filter(|endpoint| {
            endpoint.kind == GatewayEndpointKind::Local && endpoint.id.trim() == local_gateway_id
        })
        .and_then(|endpoint| endpoint.workspace_id.clone());
    let mut registry = default_registry(config)?;
    registry.installation_id = Some(load_or_create_registry_upgrade_installation_id(path)?);
    if let Some(local) = registry.local.as_mut() {
        local.workspace_id = local_workspace_id;
    }
    registry.remotes = legacy
        .remotes
        .into_iter()
        .filter(|endpoint| endpoint.kind == GatewayEndpointKind::Remote)
        .map(|endpoint| GatewayEndpoint {
            id: endpoint.id,
            name: endpoint.name,
            address: endpoint.address,
            kind: GatewayEndpointKind::Remote,
            session_ref: None,
            server_gateway_id: None,
            workspace_id: endpoint.workspace_id,
            service_name: endpoint.service_name,
        })
        .collect();
    registry.active_gateway_id = legacy.active_gateway_id;
    Ok(registry)
}

fn load_or_create_registry_upgrade_installation_id(path: &Path) -> Result<String> {
    if let Some(state) = load_registry_upgrade_state(path)? {
        return Ok(state.installation_id);
    }

    let state_path = registry_upgrade_state_path(path);
    let state = GatewayRegistryUpgradeState {
        version: REGISTRY_UPGRADE_STATE_VERSION,
        installation_id: generate_id(INSTALLATION_ID_LEN),
    };
    let content = toml::to_string_pretty(&state)
        .context("failed to encode Gateway registry upgrade state")?;
    write_private_registry_file(&state_path, content.as_str()).with_context(|| {
        format!(
            "failed to persist Gateway registry upgrade state {}",
            state_path.display()
        )
    })?;
    Ok(state.installation_id)
}

fn load_registry_upgrade_state(path: &Path) -> Result<Option<GatewayRegistryUpgradeState>> {
    let state_path = registry_upgrade_state_path(path);
    let content = match fs::read_to_string(&state_path) {
        Ok(content) => content,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "failed to read Gateway registry upgrade state {}",
                    state_path.display()
                )
            });
        }
    };
    let state =
        toml::from_str::<GatewayRegistryUpgradeState>(content.as_str()).with_context(|| {
            format!(
                "failed to parse Gateway registry upgrade state {}",
                state_path.display()
            )
        })?;
    if state.version != REGISTRY_UPGRADE_STATE_VERSION {
        anyhow::bail!(
            "unsupported Gateway registry upgrade state version `{}` in `{}`",
            state.version,
            state_path.display()
        );
    }
    validate_upgrade_installation_id(state.installation_id.as_str(), &state_path)?;
    Ok(Some(state))
}

fn validate_registry_upgrade_state(
    state: &GatewayRegistryUpgradeState,
    registry: &GatewayRegistry,
    path: &Path,
) -> Result<()> {
    if registry.installation_id.as_deref() != Some(state.installation_id.as_str()) {
        anyhow::bail!(
            "Gateway registry upgrade state does not match the installation id in `{}`",
            path.display()
        );
    }
    Ok(())
}

fn validate_upgrade_installation_id(value: &str, path: &Path) -> Result<()> {
    if value.len() != INSTALLATION_ID_LEN || !value.bytes().all(|byte| byte.is_ascii_alphanumeric())
    {
        anyhow::bail!(
            "invalid Gateway registry upgrade installation id in `{}`",
            path.display()
        );
    }
    Ok(())
}

fn registry_upgrade_state_path(path: &Path) -> std::path::PathBuf {
    path.with_extension("upgrade-v2")
}

fn registry_config(config: &AppConfig) -> Result<GatewayRegistryConfig> {
    let local_gateway_id = local_gateway_id(config);
    let local_address = resolve_gateway_socket_addrs(config.gateway.listen_addr.as_str())
        .context("failed to derive the local Gateway client address")?
        .into_iter()
        .next()
        .context("local Gateway listen address did not resolve")?
        .to_string();

    Ok(GatewayRegistryConfig {
        version: current_registry_version(config),
        local: Some(GatewayLocalRegistryConfig {
            gateway_id: local_gateway_id.to_owned(),
            name: t!("gateway.endpoint.local_name").to_string(),
            // A bind address such as 0.0.0.0 is not a safe authenticated
            // client destination. The shared resolver maps unspecified binds
            // to loopback while preserving the configured port.
            address: local_address,
            service_name: Some(config.gateway.service_name.trim().to_owned()),
        }),
    })
}

fn local_gateway_id(config: &AppConfig) -> &str {
    config.desktop.gateway.local_gateway_id.trim()
}

fn current_registry_version(config: &AppConfig) -> u32 {
    debug_assert_eq!(config.desktop.gateway.registry_version, GATEWAY_REGISTRY_V2);
    config.desktop.gateway.registry_version
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
