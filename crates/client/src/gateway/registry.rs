//! Gateway registry normalization and validation.

use super::{
    connectivity::normalize_address,
    types::{GatewayEndpoint, GatewayEndpointKind, GatewayRegistry},
};
use pioneer_protocol::generate_id;
use std::{collections::HashSet, error::Error, fmt};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GatewayRegistryConfig {
    pub version: u32,
    pub local: Option<GatewayLocalRegistryConfig>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GatewayLocalRegistryConfig {
    pub gateway_id: String,
    pub name: String,
    pub address: String,
    pub auth_token_ref: Option<String>,
    pub service_name: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GatewayRegistryError {
    InvalidAuthTokenRef { value: String, reason: String },
}

impl GatewayRegistryError {
    pub fn invalid_auth_token_ref(value: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::InvalidAuthTokenRef {
            value: value.into(),
            reason: reason.into(),
        }
    }
}

pub fn default_registry(config: &GatewayRegistryConfig) -> GatewayRegistry {
    GatewayRegistry {
        version: config.version,
        active_gateway_id: None,
        local: config
            .local
            .as_ref()
            .map(|local| local_endpoint_from_config(local, None)),
        remotes: Vec::new(),
    }
}

pub fn setup_required(registry: &GatewayRegistry) -> bool {
    registry.active_gateway_id.is_none()
}

pub fn normalize_registry<F, G>(
    registry: &mut GatewayRegistry,
    config: &GatewayRegistryConfig,
    mut auth_token_ref_for_endpoint: F,
    mut remote_name_for_index: G,
) -> Result<(), GatewayRegistryError>
where
    F: FnMut(&str) -> Result<String, GatewayRegistryError>,
    G: FnMut(usize) -> String,
{
    let local_gateway_id = config
        .local
        .as_ref()
        .map(|local| local.gateway_id.trim())
        .filter(|value| !value.is_empty());
    let local_workspace_id = registry
        .local
        .take()
        .and_then(|mut endpoint| endpoint.workspace_id.take())
        .and_then(normalize_workspace_id);
    registry.version = config.version;
    registry.local = config
        .local
        .as_ref()
        .map(|local| local_endpoint_from_config(local, local_workspace_id));

    let mut seen_ids = local_gateway_id
        .map(|id| HashSet::from([id.to_owned()]))
        .unwrap_or_default();
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
        let has_auth_token_ref = normalize_auth_token_ref(
            endpoint.auth_token_ref.take(),
            &mut auth_token_ref_for_endpoint,
        )?
        .is_some();

        if endpoint.name.trim().is_empty() {
            endpoint.name = remote_name_for_index(remotes.len() + 1);
        } else {
            endpoint.name = endpoint.name.trim().to_owned();
        }

        endpoint.id = endpoint.id.trim().to_owned();
        if endpoint.id.is_empty() || local_gateway_id == Some(endpoint.id.as_str()) {
            endpoint.id = generated_remote_id();
        }

        while !seen_ids.insert(endpoint.id.clone()) {
            endpoint.id = generated_remote_id();
        }

        endpoint.auth_token_ref = if has_auth_token_ref {
            Some(auth_token_ref_for_endpoint(endpoint.id.as_str())?)
        } else {
            None
        };

        if !seen_addresses.insert(address) {
            continue;
        }

        remotes.push(endpoint);
    }

    registry.remotes = remotes;

    if let Some(active) = registry.active_gateway_id.as_deref() {
        let active_is_local = registry
            .local
            .as_ref()
            .is_some_and(|endpoint| endpoint.id == active);
        let active_is_remote = registry
            .remotes
            .iter()
            .any(|endpoint| endpoint.id == active);
        if !active_is_local && !active_is_remote {
            registry.active_gateway_id = None;
        }
    }

    Ok(())
}

pub fn remote_index_by_address(
    registry: &GatewayRegistry,
    address: &str,
    exclude_id: Option<&str>,
) -> Option<usize> {
    registry
        .remotes
        .iter()
        .enumerate()
        .find(|(_, remote)| remote.address == address && exclude_id != Some(remote.id.as_str()))
        .map(|(index, _)| index)
}

pub fn normalize_workspace_id(value: String) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then_some(trimmed.to_owned())
}

fn local_endpoint_from_config(
    config: &GatewayLocalRegistryConfig,
    workspace_id: Option<String>,
) -> GatewayEndpoint {
    GatewayEndpoint {
        id: config.gateway_id.trim().to_owned(),
        name: config.name.trim().to_owned(),
        address: config.address.trim().to_owned(),
        kind: GatewayEndpointKind::Local,
        auth_token_ref: config.auth_token_ref.clone(),
        workspace_id,
        service_name: config
            .service_name
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned),
    }
}

fn normalize_auth_token_ref<F>(
    value: Option<String>,
    auth_token_ref_for_endpoint: &mut F,
) -> Result<Option<String>, GatewayRegistryError>
where
    F: FnMut(&str) -> Result<String, GatewayRegistryError>,
{
    let Some(value) = value else {
        return Ok(None);
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    auth_token_ref_for_endpoint(trimmed)?;
    Ok(Some(trimmed.to_owned()))
}

fn generated_remote_id() -> String {
    format!("remote-{}", generate_id(8))
}

impl fmt::Display for GatewayRegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidAuthTokenRef { value, reason } => {
                write!(f, "invalid gateway auth token ref `{value}`: {reason}")
            }
        }
    }
}

impl Error for GatewayRegistryError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> GatewayRegistryConfig {
        GatewayRegistryConfig {
            version: 1,
            local: Some(GatewayLocalRegistryConfig {
                gateway_id: "local".to_owned(),
                name: "Local Gateway".to_owned(),
                address: "0.0.0.0:17878".to_owned(),
                auth_token_ref: Some("local".to_owned()),
                service_name: Some("com.pioneer.gateway".to_owned()),
            }),
        }
    }

    fn token_ref(value: &str) -> Result<String, GatewayRegistryError> {
        if value.contains('/') || value.contains('\\') {
            return Err(GatewayRegistryError::invalid_auth_token_ref(
                value,
                "path separators are not allowed",
            ));
        }

        Ok(value.to_owned())
    }

    #[test]
    fn gateway_registry_normalization_enforces_local_and_deduplicates_remotes() {
        let config = test_config();
        let mut registry = GatewayRegistry {
            version: 99,
            active_gateway_id: Some("unknown-id".to_owned()),
            local: Some(GatewayEndpoint {
                id: "bad-local".to_owned(),
                name: "Old Local".to_owned(),
                address: "127.0.0.1:9999".to_owned(),
                kind: GatewayEndpointKind::Remote,
                auth_token_ref: None,
                workspace_id: None,
                service_name: None,
            }),
            remotes: vec![
                GatewayEndpoint {
                    id: "".to_owned(),
                    name: " ".to_owned(),
                    address: "127.0.0.1:22000".to_owned(),
                    kind: GatewayEndpointKind::Local,
                    auth_token_ref: Some(" remote-a ".to_owned()),
                    workspace_id: Some("  ws_remote_a  ".to_owned()),
                    service_name: Some("should-be-cleared".to_owned()),
                },
                GatewayEndpoint {
                    id: "duplicate-id".to_owned(),
                    name: "Remote Duplicate".to_owned(),
                    address: "127.0.0.1:22000".to_owned(),
                    kind: GatewayEndpointKind::Remote,
                    auth_token_ref: None,
                    workspace_id: None,
                    service_name: None,
                },
                GatewayEndpoint {
                    id: "duplicate-id".to_owned(),
                    name: "Unique".to_owned(),
                    address: "127.0.0.1:22001".to_owned(),
                    kind: GatewayEndpointKind::Remote,
                    auth_token_ref: None,
                    workspace_id: Some("   ".to_owned()),
                    service_name: None,
                },
            ],
        };

        normalize_registry(&mut registry, &config, token_ref, |index| {
            format!("Remote Gateway {index}")
        })
        .expect("normalize registry");

        assert_eq!(registry.version, 1);
        let local = registry.local.as_ref().expect("local gateway");
        assert_eq!(local.id, "local");
        assert_eq!(local.kind, GatewayEndpointKind::Local);
        assert_eq!(local.auth_token_ref.as_deref(), Some("local"));
        assert_eq!(registry.remotes.len(), 2);
        assert_eq!(
            registry.remotes[0].workspace_id.as_deref(),
            Some("ws_remote_a")
        );
        assert_eq!(
            registry.remotes[0].auth_token_ref.as_deref(),
            Some(registry.remotes[0].id.as_str())
        );
        assert!(registry.remotes[1].workspace_id.is_none());
        assert!(registry.active_gateway_id.is_none());
    }

    #[test]
    fn gateway_registry_normalization_allows_remote_only_clients() {
        let config = GatewayRegistryConfig {
            version: 1,
            local: None,
        };
        let mut registry = GatewayRegistry {
            version: 99,
            active_gateway_id: Some("remote-a".to_owned()),
            local: Some(GatewayEndpoint {
                id: "stale-local".to_owned(),
                name: "Stale Local".to_owned(),
                address: "127.0.0.1:17878".to_owned(),
                kind: GatewayEndpointKind::Local,
                auth_token_ref: None,
                workspace_id: Some("stale-workspace".to_owned()),
                service_name: Some("com.pioneer.gateway".to_owned()),
            }),
            remotes: vec![GatewayEndpoint {
                id: "remote-a".to_owned(),
                name: "Remote A".to_owned(),
                address: "127.0.0.1:22000".to_owned(),
                kind: GatewayEndpointKind::Local,
                auth_token_ref: None,
                workspace_id: None,
                service_name: Some("should-be-cleared".to_owned()),
            }],
        };

        normalize_registry(&mut registry, &config, token_ref, |index| {
            format!("Remote Gateway {index}")
        })
        .expect("normalize remote-only registry");

        assert_eq!(registry.version, 1);
        assert!(registry.local.is_none());
        assert_eq!(registry.active_gateway_id.as_deref(), Some("remote-a"));
        assert_eq!(registry.remotes.len(), 1);
        assert_eq!(registry.remotes[0].kind, GatewayEndpointKind::Remote);
        assert_eq!(registry.remotes[0].service_name, None);
    }

    #[test]
    fn gateway_registry_duplicate_address_lookup_can_exclude_current_endpoint() {
        let registry = GatewayRegistry {
            version: 1,
            active_gateway_id: None,
            local: test_config()
                .local
                .as_ref()
                .map(|local| local_endpoint_from_config(local, None)),
            remotes: vec![
                GatewayEndpoint {
                    id: "remote-a".to_owned(),
                    name: "Remote A".to_owned(),
                    address: "127.0.0.1:22000".to_owned(),
                    kind: GatewayEndpointKind::Remote,
                    auth_token_ref: None,
                    workspace_id: None,
                    service_name: None,
                },
                GatewayEndpoint {
                    id: "remote-b".to_owned(),
                    name: "Remote B".to_owned(),
                    address: "127.0.0.1:22001".to_owned(),
                    kind: GatewayEndpointKind::Remote,
                    auth_token_ref: None,
                    workspace_id: None,
                    service_name: None,
                },
            ],
        };

        assert_eq!(
            remote_index_by_address(&registry, "127.0.0.1:22000", None),
            Some(0)
        );
        assert_eq!(
            remote_index_by_address(&registry, "127.0.0.1:22000", Some("remote-a")),
            None
        );
    }
}
