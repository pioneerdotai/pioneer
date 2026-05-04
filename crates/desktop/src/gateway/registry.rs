use crate::gateway::connectivity::normalize_address;
use crate::gateway::types::{GatewayEndpoint, GatewayEndpointKind, GatewayRegistry};
use anyhow::{Context, Result, bail};
use pioneer_config::AppConfig;
use pioneer_keystore::{SecretId, ensure_private_file, ensure_private_runtime_dir};
use pioneer_protocol::generate_id;
use std::collections::HashSet;
use std::path::Path;

pub(crate) fn load_registry(path: &Path, config: &AppConfig) -> Result<GatewayRegistry> {
    let mut registry = if path.exists() {
        let path_display = path.display().to_string();
        let content = std::fs::read_to_string(path).with_context(|| {
            t!("errors.registry.read_failed", path = path_display.as_str()).to_string()
        })?;
        let registry = toml::from_str::<GatewayRegistry>(&content).with_context(|| {
            t!("errors.registry.parse_failed", path = path_display.as_str()).to_string()
        })?;
        let current_registry_version = current_registry_version(config);
        if registry.version != current_registry_version {
            bail!(
                "{}",
                t!(
                    "errors.registry.unsupported_version",
                    version = registry.version,
                    path = path_display.as_str(),
                    current_registry_version = current_registry_version
                )
            );
        }
        registry
    } else {
        default_registry(config)?
    };

    normalize_registry(&mut registry, config)?;
    save_registry(path, &registry)?;

    Ok(registry)
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
    Ok(GatewayRegistry {
        version: current_registry_version(config),
        active_gateway_id: None,
        local: local_endpoint_from_config(config, None)?,
        remotes: Vec::new(),
    })
}

pub(crate) fn setup_required(registry: &GatewayRegistry) -> bool {
    registry.active_gateway_id.is_none()
}

pub(crate) fn normalize_registry(registry: &mut GatewayRegistry, config: &AppConfig) -> Result<()> {
    let local_gateway_id = local_gateway_id(config);
    let local_workspace_id = registry
        .local
        .workspace_id
        .take()
        .and_then(normalize_workspace_id);
    registry.version = current_registry_version(config);
    registry.local = local_endpoint_from_config(config, local_workspace_id)?;

    let mut seen_ids = HashSet::from([local_gateway_id.to_owned()]);
    let mut seen_addresses = HashSet::new();
    let mut remotes = Vec::new();

    for mut endpoint in std::mem::take(&mut registry.remotes) {
        let address = match normalize_address(endpoint.address.as_str()) {
            Ok(value) => value,
            Err(_) => continue,
        };

        endpoint.kind = GatewayEndpointKind::Remote;
        endpoint.service_name = None;
        endpoint.address = address.clone();
        endpoint.workspace_id = endpoint.workspace_id.and_then(normalize_workspace_id);
        let has_auth_token_ref =
            normalize_auth_token_ref(endpoint.auth_token_ref.take())?.is_some();

        if endpoint.name.trim().is_empty() {
            endpoint.name =
                t!("gateway.endpoint.remote_name", index = remotes.len() + 1).to_string();
        } else {
            endpoint.name = endpoint.name.trim().to_owned();
        }

        endpoint.id = endpoint.id.trim().to_owned();
        if endpoint.id.is_empty() || endpoint.id == local_gateway_id {
            endpoint.id = format!("remote-{}", generate_id(8));
        }

        while !seen_ids.insert(endpoint.id.clone()) {
            endpoint.id = format!("remote-{}", generate_id(8));
        }

        endpoint.auth_token_ref = if has_auth_token_ref {
            Some(gateway_auth_token_ref(endpoint.id.as_str())?)
        } else {
            None
        };

        if !seen_addresses.insert(address) {
            continue;
        }

        remotes.push(endpoint);
    }

    registry.remotes = remotes;

    if let Some(active) = registry.active_gateway_id.as_deref()
        && !registry.local.id.eq(active)
        && !registry
            .remotes
            .iter()
            .any(|endpoint| endpoint.id == active)
    {
        registry.active_gateway_id = None;
    }

    Ok(())
}

pub(crate) fn gateway_auth_token_ref(endpoint_id: &str) -> Result<String> {
    let id = SecretId::desktop_gateway_auth_token(endpoint_id)
        .context("invalid desktop gateway endpoint id")?;
    Ok(id.user().to_owned())
}

fn local_endpoint_from_config(
    config: &AppConfig,
    workspace_id: Option<String>,
) -> Result<GatewayEndpoint> {
    let local_gateway_id = local_gateway_id(config);

    Ok(GatewayEndpoint {
        id: local_gateway_id.to_owned(),
        name: t!("gateway.endpoint.local_name").to_string(),
        address: config.gateway.listen_addr.trim().to_owned(),
        kind: GatewayEndpointKind::Local,
        auth_token_ref: Some(gateway_auth_token_ref(local_gateway_id)?),
        workspace_id,
        service_name: Some(config.gateway.service_name.trim().to_owned()),
    })
}

fn local_gateway_id(config: &AppConfig) -> &str {
    config.desktop.gateway.local_gateway_id.trim()
}

fn current_registry_version(config: &AppConfig) -> u32 {
    config.desktop.gateway.registry_version
}

fn normalize_auth_token_ref(value: Option<String>) -> Result<Option<String>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    Ok(Some(gateway_auth_token_ref(trimmed)?))
}

fn normalize_workspace_id(value: String) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then_some(trimmed.to_owned())
}

fn write_private_registry_file(path: &Path, content: &str) -> Result<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        ensure_private_runtime_dir(parent).with_context(|| {
            format!(
                "failed to harden gateway registry directory {}",
                parent.display()
            )
        })?;
    }

    std::fs::write(path, content)
        .with_context(|| format!("failed to write gateway registry file {}", path.display()))?;
    ensure_private_file(path)
        .with_context(|| format!("failed to harden gateway registry file {}", path.display()))?;
    Ok(())
}
