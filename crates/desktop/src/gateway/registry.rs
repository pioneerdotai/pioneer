use anyhow::{Context, Result, bail};
use pioneer_client::gateway::registry::{
    GatewayRegistryConfig, GatewayRegistryError, default_registry as default_client_registry,
    normalize_registry as normalize_client_registry, setup_required as client_setup_required,
};
use pioneer_client::gateway::secrets::{
    EndpointIdGatewayAuthTokenRefNamer, GatewayAuthTokenRefNamer,
};
use pioneer_client::gateway::types::GatewayRegistry;
use pioneer_config::AppConfig;
use pioneer_keystore::{SecretId, ensure_private_file, ensure_private_runtime_dir};
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
    Ok(default_client_registry(&registry_config(config)?))
}

pub(crate) fn setup_required(registry: &GatewayRegistry) -> bool {
    client_setup_required(registry)
}

pub(crate) fn normalize_registry(registry: &mut GatewayRegistry, config: &AppConfig) -> Result<()> {
    let registry_config = registry_config(config)?;
    normalize_client_registry(
        registry,
        &registry_config,
        |endpoint_id| {
            gateway_auth_token_ref(endpoint_id).map_err(|error| {
                GatewayRegistryError::invalid_auth_token_ref(endpoint_id, format!("{error:#}"))
            })
        },
        |index| t!("gateway.endpoint.remote_name", index = index).to_string(),
    )
    .map_err(anyhow::Error::new)
}

pub(crate) fn gateway_auth_token_ref(endpoint_id: &str) -> Result<String> {
    let token_ref = EndpointIdGatewayAuthTokenRefNamer
        .auth_token_ref_for_endpoint(endpoint_id)
        .context("invalid gateway auth token ref")?;
    let id = SecretId::desktop_gateway_auth_token(token_ref.as_str())
        .context("invalid desktop gateway endpoint id")?;
    Ok(id.user().to_owned())
}

fn registry_config(config: &AppConfig) -> Result<GatewayRegistryConfig> {
    let local_gateway_id = local_gateway_id(config);

    Ok(GatewayRegistryConfig {
        version: current_registry_version(config),
        local_gateway_id: local_gateway_id.to_owned(),
        local_name: t!("gateway.endpoint.local_name").to_string(),
        local_address: config.gateway.listen_addr.trim().to_owned(),
        local_auth_token_ref: Some(gateway_auth_token_ref(local_gateway_id)?),
        local_service_name: Some(config.gateway.service_name.trim().to_owned()),
    })
}

fn local_gateway_id(config: &AppConfig) -> &str {
    config.desktop.gateway.local_gateway_id.trim()
}

fn current_registry_version(config: &AppConfig) -> u32 {
    config.desktop.gateway.registry_version
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
