//! One-shot local registry v2 to v3 data migration.
//!
//! Legacy types in this module are intentionally not exported to transport or
//! runtime code. Updated clients persist only [`GatewayRegistry`] v3.

use pioneer_protocol::GatewayId;
use serde::Deserialize;
use std::fmt;
use url::Url;

use super::{
    endpoint::{GatewayBaseUrl, GatewayBaseUrlError},
    registry::CURRENT_GATEWAY_REGISTRY_VERSION,
    types::{GatewayEndpoint, GatewayEndpointKind, GatewayRegistry},
};

const GATEWAY_REGISTRY_V2: u32 = 2;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GatewayRegistryLoad {
    Current(GatewayRegistry),
    Migrated(GatewayRegistry),
    ReconfigurationRequired { endpoint_ids: Vec<String> },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GatewayRegistryLoadError {
    InvalidDocument,
    UnsupportedVersion,
    InvalidEndpoint,
}

impl fmt::Display for GatewayRegistryLoadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidDocument => "Gateway registry document is invalid",
            Self::UnsupportedVersion => "Gateway registry version is unsupported",
            Self::InvalidEndpoint => "Gateway registry contains an invalid endpoint",
        })
    }
}

impl std::error::Error for GatewayRegistryLoadError {}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyGatewayRegistryV2 {
    version: u32,
    #[serde(default)]
    installation_id: Option<String>,
    active_gateway_id: Option<String>,
    #[serde(default)]
    local: Option<LegacyGatewayEndpointV2>,
    #[serde(default)]
    remotes: Vec<LegacyGatewayEndpointV2>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyGatewayEndpointV2 {
    id: String,
    name: String,
    address: String,
    kind: GatewayEndpointKind,
    #[serde(default)]
    session_ref: Option<String>,
    #[serde(default)]
    server_gateway_id: Option<GatewayId>,
    #[serde(default)]
    workspace_id: Option<String>,
    #[serde(default)]
    service_name: Option<String>,
}

pub fn load_registry_json(input: &str) -> Result<GatewayRegistryLoad, GatewayRegistryLoadError> {
    let value: serde_json::Value =
        serde_json::from_str(input).map_err(|_| GatewayRegistryLoadError::InvalidDocument)?;
    let version = value
        .get("version")
        .and_then(serde_json::Value::as_u64)
        .ok_or(GatewayRegistryLoadError::InvalidDocument)?;
    match version {
        version if version == u64::from(CURRENT_GATEWAY_REGISTRY_VERSION) => {
            let registry = serde_json::from_value(value)
                .map_err(|_| GatewayRegistryLoadError::InvalidDocument)?;
            Ok(GatewayRegistryLoad::Current(registry))
        }
        version if version == u64::from(GATEWAY_REGISTRY_V2) => {
            let legacy: LegacyGatewayRegistryV2 = serde_json::from_value(value)
                .map_err(|_| GatewayRegistryLoadError::InvalidDocument)?;
            migrate_v2(legacy)
        }
        _ => Err(GatewayRegistryLoadError::UnsupportedVersion),
    }
}

fn migrate_v2(
    legacy: LegacyGatewayRegistryV2,
) -> Result<GatewayRegistryLoad, GatewayRegistryLoadError> {
    if legacy.version != GATEWAY_REGISTRY_V2 {
        return Err(GatewayRegistryLoadError::UnsupportedVersion);
    }

    let mut reconfiguration_ids = Vec::new();
    let local = match legacy.local {
        Some(endpoint) => match migrate_endpoint(endpoint, true) {
            Ok(endpoint) => Some(endpoint),
            Err(MigrationEndpointError::Ambiguous(id)) => {
                reconfiguration_ids.push(id);
                None
            }
            Err(MigrationEndpointError::Invalid) => {
                return Err(GatewayRegistryLoadError::InvalidEndpoint);
            }
        },
        None => None,
    };
    let mut remotes = Vec::with_capacity(legacy.remotes.len());
    for endpoint in legacy.remotes {
        match migrate_endpoint(endpoint, false) {
            Ok(endpoint) => remotes.push(endpoint),
            Err(MigrationEndpointError::Ambiguous(id)) => reconfiguration_ids.push(id),
            Err(MigrationEndpointError::Invalid) => {
                return Err(GatewayRegistryLoadError::InvalidEndpoint);
            }
        }
    }
    if !reconfiguration_ids.is_empty() {
        reconfiguration_ids.sort();
        reconfiguration_ids.dedup();
        tracing::warn!(
            event = "gateway_registry_migration",
            outcome = "reconfiguration_required",
            reason_code = "ambiguous_custom_path",
        );
        return Ok(GatewayRegistryLoad::ReconfigurationRequired {
            endpoint_ids: reconfiguration_ids,
        });
    }

    Ok(GatewayRegistryLoad::Migrated(GatewayRegistry {
        version: CURRENT_GATEWAY_REGISTRY_VERSION,
        installation_id: legacy.installation_id,
        active_gateway_id: legacy.active_gateway_id,
        local,
        remotes,
    }))
}

enum MigrationEndpointError {
    Ambiguous(String),
    Invalid,
}

fn migrate_endpoint(
    legacy: LegacyGatewayEndpointV2,
    local: bool,
) -> Result<GatewayEndpoint, MigrationEndpointError> {
    let gateway_base_url = migrate_address(legacy.address.as_str(), local).map_err(|error| {
        if error == AddressMigrationError::Ambiguous {
            MigrationEndpointError::Ambiguous(legacy.id.clone())
        } else {
            MigrationEndpointError::Invalid
        }
    })?;
    Ok(GatewayEndpoint {
        id: legacy.id,
        name: legacy.name,
        gateway_base_url,
        kind: legacy.kind,
        session_ref: legacy.session_ref,
        server_gateway_id: legacy.server_gateway_id,
        workspace_id: legacy.workspace_id,
        service_name: legacy.service_name,
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AddressMigrationError {
    Ambiguous,
    Invalid,
}

fn migrate_address(input: &str, local: bool) -> Result<GatewayBaseUrl, AddressMigrationError> {
    let trimmed = input.trim();
    if local
        && let Ok(socket) = trimmed.parse::<std::net::SocketAddr>()
        && socket.ip().is_unspecified()
    {
        return GatewayBaseUrl::from_local_listen_addr(trimmed).map_err(map_base_error);
    }
    if trimmed.contains("://") {
        let mut url = Url::parse(trimmed).map_err(|_| AddressMigrationError::Invalid)?;
        if url.path() != "/" || url.query().is_some() || url.fragment().is_some() {
            return Err(AddressMigrationError::Ambiguous);
        }
        match url.scheme() {
            "ws" => url
                .set_scheme("http")
                .map_err(|_| AddressMigrationError::Invalid)?,
            "wss" => url
                .set_scheme("https")
                .map_err(|_| AddressMigrationError::Invalid)?,
            "http" | "https" => {}
            _ => return Err(AddressMigrationError::Invalid),
        }
        if local && matches!(url.host_str(), Some("0.0.0.0" | "::")) {
            let port = url
                .port_or_known_default()
                .ok_or(AddressMigrationError::Invalid)?;
            let listen = if url.host_str() == Some("::") {
                format!("[::]:{port}")
            } else {
                format!("0.0.0.0:{port}")
            };
            return GatewayBaseUrl::from_local_listen_addr(listen.as_str()).map_err(map_base_error);
        }
        return GatewayBaseUrl::parse_presentation(url.as_str()).map_err(map_base_error);
    }
    // A v2 address did not distinguish a WebSocket route from an HTTP base
    // prefix. Only a path-free authority can therefore be converted without
    // guessing user intent. Custom base prefixes remain valid when entered
    // explicitly into the v3 model.
    if trimmed.contains(['/', '?', '#']) {
        return Err(AddressMigrationError::Ambiguous);
    }
    GatewayBaseUrl::parse_presentation(trimmed).map_err(map_base_error)
}

fn map_base_error(_: GatewayBaseUrlError) -> AddressMigrationError {
    AddressMigrationError::Invalid
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v2_root_endpoints_migrate_once_to_v3() {
        let input = r#"{
            "version":2,
            "installation_id":"installation",
            "active_gateway_id":"local",
            "local":{"id":"local","name":"Local","address":"ws://0.0.0.0:17878","kind":"local","service_name":"pioneer"},
            "remotes":[{"id":"relay","name":"Relay","address":"wss://relay.example","kind":"remote"}]
        }"#;
        let GatewayRegistryLoad::Migrated(registry) = load_registry_json(input).unwrap() else {
            panic!("expected migrated registry");
        };
        assert_eq!(registry.version, CURRENT_GATEWAY_REGISTRY_VERSION);
        assert_eq!(
            registry.local.as_ref().unwrap().gateway_base_url.as_str(),
            "http://127.0.0.1:17878/"
        );
        assert_eq!(
            registry.remotes[0].gateway_base_url.as_str(),
            "https://relay.example/"
        );
        let serialized = serde_json::to_string(&registry).unwrap();
        assert!(!serialized.contains("\"address\""));
    }

    #[test]
    fn v2_custom_websocket_path_requires_reconfiguration() {
        for address in [
            "wss://relay.example/socket",
            "https://relay.example/socket",
            "relay.example/socket",
        ] {
            let input = format!(
                r#"{{
                    "version":2,
                    "active_gateway_id":"custom",
                    "remotes":[{{"id":"custom","name":"Custom","address":"{address}","kind":"remote"}}]
                }}"#
            );
            assert_eq!(
                load_registry_json(input.as_str()).unwrap(),
                GatewayRegistryLoad::ReconfigurationRequired {
                    endpoint_ids: vec!["custom".to_owned()]
                }
            );
        }
    }

    #[test]
    fn v3_unknown_fields_are_rejected() {
        let input = r#"{
            "version":3,
            "active_gateway_id":null,
            "remotes":[],
            "legacy_ws_url":"wss://legacy.example/socket"
        }"#;
        assert_eq!(
            load_registry_json(input),
            Err(GatewayRegistryLoadError::InvalidDocument)
        );
    }
}
