//! Gateway registry normalization and validation.

use super::{
    connectivity::normalize_address,
    types::{GatewayEndpoint, GatewayEndpointKind, GatewayRegistry},
};
use pioneer_protocol::{GatewayId, generate_id};
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
    pub service_name: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GatewayRegistryError {
    InvalidSessionRef {
        value: String,
        reason: String,
    },
    PartialSessionBinding {
        endpoint_id: String,
    },
    GatewayIdentityMismatch {
        endpoint_id: String,
        expected: GatewayId,
        observed: GatewayId,
    },
}

impl GatewayRegistryError {
    pub fn invalid_session_ref(value: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::InvalidSessionRef {
            value: value.into(),
            reason: reason.into(),
        }
    }
}

pub fn default_registry(config: &GatewayRegistryConfig) -> GatewayRegistry {
    GatewayRegistry {
        version: config.version,
        installation_id: None,
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

pub fn normalize_registry<G>(
    registry: &mut GatewayRegistry,
    config: &GatewayRegistryConfig,
    mut remote_name_for_index: G,
) -> Result<(), GatewayRegistryError>
where
    G: FnMut(usize) -> String,
{
    let mut normalized = registry.clone();
    normalize_registry_in_place(&mut normalized, config, &mut remote_name_for_index)?;
    *registry = normalized;
    Ok(())
}

fn normalize_registry_in_place<G>(
    registry: &mut GatewayRegistry,
    config: &GatewayRegistryConfig,
    remote_name_for_index: &mut G,
) -> Result<(), GatewayRegistryError>
where
    G: FnMut(usize) -> String,
{
    let local_gateway_id = config
        .local
        .as_ref()
        .map(|local| local.gateway_id.trim())
        .filter(|value| !value.is_empty());
    let previous_local = registry.local.take();
    if let Some(previous) = previous_local.as_ref()
        && let Some(session_ref) = previous.session_ref.as_deref()
        && local_gateway_id != Some(previous.id.as_str())
    {
        return Err(GatewayRegistryError::invalid_session_ref(
            session_ref,
            "session-bound local endpoint id cannot be removed or changed by normalization",
        ));
    }
    let local_workspace_id = previous_local
        .as_ref()
        .and_then(|endpoint| endpoint.workspace_id.clone())
        .and_then(normalize_workspace_id);
    registry.version = config.version;
    registry.local = config
        .local
        .as_ref()
        .map(|local| local_endpoint_from_config(local, local_workspace_id));
    if let (Some(previous), Some(local)) = (previous_local, registry.local.as_mut())
        && previous.id == local.id
    {
        local.session_ref = previous.session_ref;
        local.server_gateway_id = previous.server_gateway_id;
    }
    if let Some(local) = registry.local.as_ref() {
        validate_endpoint_session_binding(local)?;
    }

    let mut seen_ids = local_gateway_id
        .map(|id| HashSet::from([id.to_owned()]))
        .unwrap_or_default();
    let mut seen_session_refs = registry
        .local
        .as_ref()
        .and_then(|endpoint| endpoint.session_ref.clone())
        .map(|session_ref| HashSet::from([session_ref]))
        .unwrap_or_default();
    let mut seen_addresses = HashSet::new();
    let mut remotes = Vec::new();

    for mut endpoint in std::mem::take(&mut registry.remotes) {
        let address = match normalize_address(endpoint.address.as_str()) {
            Ok(value) => value,
            Err(_) => {
                reject_session_bound_endpoint_rewrite(
                    endpoint.session_ref.as_deref(),
                    "session-bound endpoint address must be valid",
                )?;
                continue;
            }
        };

        endpoint.kind = GatewayEndpointKind::Remote;
        endpoint.service_name = None;
        endpoint.address = address.clone();
        endpoint.workspace_id = endpoint.workspace_id.and_then(normalize_workspace_id);
        validate_endpoint_session_binding(&endpoint)?;
        let session_ref = endpoint.session_ref.clone();
        if let Some(session_ref) = session_ref.as_ref()
            && !seen_session_refs.insert(session_ref.clone())
        {
            return Err(GatewayRegistryError::invalid_session_ref(
                session_ref,
                "session ref must belong to exactly one endpoint",
            ));
        }
        if endpoint.name.trim().is_empty() {
            endpoint.name = remote_name_for_index(remotes.len() + 1);
        } else {
            endpoint.name = endpoint.name.trim().to_owned();
        }

        endpoint.id = endpoint.id.trim().to_owned();
        if endpoint.id.is_empty() || local_gateway_id == Some(endpoint.id.as_str()) {
            reject_session_bound_endpoint_rewrite(
                session_ref.as_deref(),
                "session-bound endpoint id cannot be empty or collide with the local endpoint",
            )?;
            endpoint.id = generated_remote_id();
        }

        if seen_ids.contains(&endpoint.id) {
            reject_session_bound_endpoint_rewrite(
                session_ref.as_deref(),
                "session-bound endpoint id must be unique",
            )?;
            while !seen_ids.insert(endpoint.id.clone()) {
                endpoint.id = generated_remote_id();
            }
        } else {
            seen_ids.insert(endpoint.id.clone());
        }

        if !seen_addresses.insert(address) {
            reject_session_bound_endpoint_rewrite(
                session_ref.as_deref(),
                "session-bound endpoint address must be unique",
            )?;
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

pub const GATEWAY_REGISTRY_V2: u32 = 2;

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GatewayIdPinValidation {
    Establish,
    Matched,
}

pub fn validate_gateway_id_pin(
    endpoint: &GatewayEndpoint,
    observed: &GatewayId,
) -> Result<GatewayIdPinValidation, GatewayRegistryError> {
    match endpoint.server_gateway_id.as_ref() {
        None => Ok(GatewayIdPinValidation::Establish),
        Some(expected) if expected == observed => Ok(GatewayIdPinValidation::Matched),
        Some(expected) => Err(GatewayRegistryError::GatewayIdentityMismatch {
            endpoint_id: endpoint.id.clone(),
            expected: expected.clone(),
            observed: observed.clone(),
        }),
    }
}

pub fn bind_endpoint_session(
    endpoint: &mut GatewayEndpoint,
    session_ref: &str,
    observed_gateway_id: &GatewayId,
) -> Result<(), GatewayRegistryError> {
    let session_ref = validate_session_ref(session_ref)?;
    validate_gateway_id_pin(endpoint, observed_gateway_id)?;
    endpoint.session_ref = Some(session_ref);
    endpoint.server_gateway_id = Some(observed_gateway_id.clone());
    Ok(())
}

pub fn commit_registry_v2_binding(
    registry: &mut GatewayRegistry,
    endpoint_id: &str,
    session_ref: &str,
    observed_gateway_id: &GatewayId,
) -> Result<(), GatewayRegistryError> {
    let endpoint = endpoint_by_id_mut(registry, endpoint_id).ok_or_else(|| {
        GatewayRegistryError::InvalidSessionRef {
            value: session_ref.to_owned(),
            reason: format!("unknown Gateway endpoint `{endpoint_id}`"),
        }
    })?;
    bind_endpoint_session(endpoint, session_ref, observed_gateway_id)?;
    registry.version = GATEWAY_REGISTRY_V2;
    Ok(())
}

pub fn clear_endpoint_session_binding(
    registry: &mut GatewayRegistry,
    endpoint_id: &str,
) -> Result<Option<String>, GatewayRegistryError> {
    let endpoint = endpoint_by_id_mut(registry, endpoint_id).ok_or_else(|| {
        GatewayRegistryError::InvalidSessionRef {
            value: endpoint_id.to_owned(),
            reason: "unknown Gateway endpoint".to_owned(),
        }
    })?;
    validate_endpoint_session_binding(endpoint)?;
    let session_ref = endpoint.session_ref.take();
    endpoint.server_gateway_id = None;
    Ok(session_ref)
}

fn endpoint_by_id_mut<'a>(
    registry: &'a mut GatewayRegistry,
    endpoint_id: &str,
) -> Option<&'a mut GatewayEndpoint> {
    if registry
        .local
        .as_ref()
        .is_some_and(|endpoint| endpoint.id == endpoint_id)
    {
        return registry.local.as_mut();
    }
    registry
        .remotes
        .iter_mut()
        .find(|endpoint| endpoint.id == endpoint_id)
}

pub fn validate_endpoint_session_binding(
    endpoint: &GatewayEndpoint,
) -> Result<(), GatewayRegistryError> {
    match (&endpoint.session_ref, &endpoint.server_gateway_id) {
        (None, None) => Ok(()),
        (Some(session_ref), Some(_)) => validate_session_ref(session_ref).map(|_| ()),
        _ => Err(GatewayRegistryError::PartialSessionBinding {
            endpoint_id: endpoint.id.clone(),
        }),
    }
}

fn validate_session_ref(value: &str) -> Result<String, GatewayRegistryError> {
    let trimmed = value.trim();
    if trimmed.is_empty()
        || trimmed.chars().count() > 255
        || trimmed.contains(['/', '\\'])
        || trimmed.chars().any(char::is_control)
    {
        return Err(GatewayRegistryError::invalid_session_ref(
            value,
            "session ref must be bounded, non-empty and must not contain path separators",
        ));
    }
    Ok(trimmed.to_owned())
}

fn reject_session_bound_endpoint_rewrite(
    session_ref: Option<&str>,
    reason: &'static str,
) -> Result<(), GatewayRegistryError> {
    if let Some(session_ref) = session_ref {
        return Err(GatewayRegistryError::invalid_session_ref(
            session_ref,
            reason,
        ));
    }
    Ok(())
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
        session_ref: None,
        server_gateway_id: None,
        workspace_id,
        service_name: config
            .service_name
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned),
    }
}

fn generated_remote_id() -> String {
    format!("remote-{}", generate_id(8))
}

impl fmt::Display for GatewayRegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSessionRef { value, reason } => {
                write!(f, "invalid Gateway session ref `{value}`: {reason}")
            }
            Self::PartialSessionBinding { endpoint_id } => {
                write!(
                    f,
                    "Gateway endpoint `{endpoint_id}` has a partial session binding"
                )
            }
            Self::GatewayIdentityMismatch {
                endpoint_id,
                expected,
                observed,
            } => write!(
                f,
                "Gateway endpoint `{endpoint_id}` is pinned to `{expected}` but server presented `{observed}`"
            ),
        }
    }
}

impl Error for GatewayRegistryError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn gateway_id(value: &str) -> GatewayId {
        GatewayId::new(value).expect("valid Gateway id")
    }

    fn config() -> GatewayRegistryConfig {
        GatewayRegistryConfig {
            version: GATEWAY_REGISTRY_V2,
            local: Some(GatewayLocalRegistryConfig {
                gateway_id: "local".to_owned(),
                name: "Local Gateway".to_owned(),
                address: "ws://localhost:17878".to_owned(),
                service_name: Some("com.pioneer.gateway".to_owned()),
            }),
        }
    }

    #[test]
    fn normalization_preserves_only_complete_device_session_bindings() {
        let mut registry = default_registry(&config());
        let local = registry.local.as_mut().expect("local endpoint");
        local.session_ref = Some("local-session".to_owned());
        local.server_gateway_id = Some(gateway_id("G00000000000000000001"));

        normalize_registry(&mut registry, &config(), |index| format!("Remote {index}"))
            .expect("normalize registry");

        let local = registry.local.expect("local endpoint");
        assert_eq!(local.session_ref.as_deref(), Some("local-session"));
        assert_eq!(
            local.server_gateway_id,
            Some(gateway_id("G00000000000000000001"))
        );
    }

    #[test]
    fn partial_session_binding_is_rejected() {
        let endpoint = GatewayEndpoint {
            id: "remote".to_owned(),
            name: "Remote".to_owned(),
            address: "wss://gateway.example.test".to_owned(),
            kind: GatewayEndpointKind::Remote,
            session_ref: Some("remote-session".to_owned()),
            server_gateway_id: None,
            workspace_id: None,
            service_name: None,
        };

        assert!(matches!(
            validate_endpoint_session_binding(&endpoint),
            Err(GatewayRegistryError::PartialSessionBinding { .. })
        ));
    }

    #[test]
    fn gateway_pin_cannot_be_rebound_to_another_server() {
        let mut endpoint = GatewayEndpoint {
            id: "remote".to_owned(),
            name: "Remote".to_owned(),
            address: "wss://gateway.example.test".to_owned(),
            kind: GatewayEndpointKind::Remote,
            session_ref: None,
            server_gateway_id: None,
            workspace_id: None,
            service_name: None,
        };
        bind_endpoint_session(
            &mut endpoint,
            "remote-session",
            &gateway_id("G00000000000000000001"),
        )
        .expect("initial binding");

        assert!(matches!(
            bind_endpoint_session(
                &mut endpoint,
                "remote-session",
                &gateway_id("G00000000000000000002")
            ),
            Err(GatewayRegistryError::GatewayIdentityMismatch { .. })
        ));
    }
}
