//! Shared gateway setup workflows.

use super::{
    connectivity::{GatewayAddressError, is_gateway_reachable},
    endpoint::{GatewayBaseUrl, GatewayBaseUrlError, GatewayTransportSecurity},
    runtime::{
        AddRemoteGatewayProfilePlan, GatewayProfileError, activate_gateway,
        apply_add_remote_gateway_profile_plan, apply_delete_remote_gateway_profile_plan,
        apply_update_remote_gateway_profile_plan, endpoint_by_id, plan_add_remote_gateway_profile,
        plan_delete_remote_gateway_profile, plan_update_remote_gateway_profile,
        remote_delete_fallback_endpoint, rollback_add_remote_gateway_profile_plan,
        set_gateway_workspace_id,
    },
    types::{GatewayEndpoint, GatewayRegistry},
};
use pioneer_protocol::generate_id;
use serde::{Deserialize, Serialize};
use std::{error::Error, fmt, time::Duration};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum RemoteGatewayValidation {
    Reachable {
        gateway_base_url: GatewayBaseUrl,
        transport_security: GatewayTransportSecurity,
    },
    Unreachable {
        gateway_base_url: GatewayBaseUrl,
        transport_security: GatewayTransportSecurity,
    },
}

impl RemoteGatewayValidation {
    pub fn gateway_base_url(&self) -> &GatewayBaseUrl {
        match self {
            Self::Reachable {
                gateway_base_url, ..
            }
            | Self::Unreachable {
                gateway_base_url, ..
            } => {
                gateway_base_url
            }
        }
    }

    pub fn is_reachable(&self) -> bool {
        matches!(self, Self::Reachable { .. })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RemoteGatewayValidationError {
    InvalidTimeout {
        timeout_ms: u64,
    },
    InvalidGatewayBaseUrl(GatewayBaseUrlError),
    ResolveFailed {
        gateway_base_url: GatewayBaseUrl,
        source: GatewayAddressError,
    },
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize)]
pub struct ActivateGatewayRegistryPlan {
    pub registry: GatewayRegistry,
    pub endpoint: GatewayEndpoint,
    pub previous_active_gateway_id: Option<String>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize)]
pub struct SetGatewayWorkspaceRegistryPlan {
    pub registry: GatewayRegistry,
    pub endpoint: GatewayEndpoint,
    pub previous_endpoint: GatewayEndpoint,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize)]
pub struct UpdateRemoteGatewayRegistryPlan {
    pub registry: GatewayRegistry,
    pub endpoint: GatewayEndpoint,
    pub previous_endpoint: GatewayEndpoint,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize)]
pub struct DeleteRemoteGatewayRegistryPlan {
    pub registry: GatewayRegistry,
    pub endpoint: GatewayEndpoint,
    pub deleted_active: bool,
    pub previous_active_gateway_id: Option<String>,
    pub fallback_endpoint: Option<GatewayEndpoint>,
}

#[derive(Clone, PartialEq, Eq)]
pub struct AddRemoteGatewayInput<'a> {
    pub name: &'a str,
    pub gateway_base_url: &'a str,
    pub new_endpoint_id: String,
    pub default_remote_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateRemoteGatewayRegistryInput<'a> {
    pub gateway_id: &'a str,
    pub name: &'a str,
    pub gateway_base_url: &'a str,
    pub default_remote_name: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeleteRemoteGatewayRegistryInput<'a> {
    pub gateway_id: &'a str,
    pub local_gateway_id: Option<&'a str>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AddRemoteGatewayApplyMode {
    ProfileOnly,
    ActivateEndpoint,
}

#[derive(Clone, Debug)]
pub struct AddRemoteGatewayCommit {
    pub endpoint: GatewayEndpoint,
    pub previous_endpoint: Option<GatewayEndpoint>,
    pub previous_active_gateway_id: Option<String>,
}

#[derive(Clone, Debug)]
pub struct AddRemoteGatewayChange {
    profile_plan: AddRemoteGatewayProfilePlan,
}

impl AddRemoteGatewayChange {
    pub fn endpoint(&self) -> &GatewayEndpoint {
        self.profile_plan.endpoint()
    }

    pub fn previous_endpoint(&self) -> Option<&GatewayEndpoint> {
        match &self.profile_plan {
            AddRemoteGatewayProfilePlan::Add { .. } => None,
            AddRemoteGatewayProfilePlan::UpdateExisting {
                previous_endpoint, ..
            } => Some(previous_endpoint),
        }
    }

    pub fn apply(&self, registry: &mut GatewayRegistry) {
        apply_add_remote_gateway_profile_plan(registry, &self.profile_plan);
    }

    pub fn rollback(&self, registry: &mut GatewayRegistry) {
        rollback_add_remote_gateway_profile_plan(registry, &self.profile_plan);
    }

    pub fn apply_to_registry(
        &self,
        registry: &mut GatewayRegistry,
        mode: AddRemoteGatewayApplyMode,
    ) -> Result<AddRemoteGatewayCommit, GatewayProfileError> {
        let commit = AddRemoteGatewayCommit {
            endpoint: self.endpoint().clone(),
            previous_endpoint: self.previous_endpoint().cloned(),
            previous_active_gateway_id: registry.active_gateway_id.clone(),
        };

        apply_add_remote_gateway_profile_plan(registry, &self.profile_plan);
        if mode == AddRemoteGatewayApplyMode::ActivateEndpoint
            && let Err(error) = activate_gateway(registry, commit.endpoint.id.as_str())
        {
            rollback_add_remote_gateway_profile_plan(registry, &self.profile_plan);
            registry.active_gateway_id = commit.previous_active_gateway_id.clone();
            return Err(error);
        }

        Ok(commit)
    }

    pub fn rollback_commit(&self, registry: &mut GatewayRegistry, commit: &AddRemoteGatewayCommit) {
        rollback_add_remote_gateway_profile_plan(registry, &self.profile_plan);
        registry.active_gateway_id = commit.previous_active_gateway_id.clone();
    }
}

pub fn generated_remote_gateway_endpoint_id() -> String {
    format!("remote-{}", generate_id(8))
}

pub fn validate_remote_gateway_base_url(
    gateway_base_url: &str,
    connect_timeout: Duration,
) -> Result<RemoteGatewayValidation, RemoteGatewayValidationError> {
    let gateway_base_url = GatewayBaseUrl::parse_presentation(gateway_base_url)
        .map_err(RemoteGatewayValidationError::InvalidGatewayBaseUrl)?;
    let reachable = is_gateway_reachable(&gateway_base_url, connect_timeout).map_err(|source| {
        RemoteGatewayValidationError::ResolveFailed {
            gateway_base_url: gateway_base_url.clone(),
            source,
        }
    })?;

    if reachable {
        let transport_security = gateway_base_url.transport_security();
        Ok(RemoteGatewayValidation::Reachable {
            gateway_base_url,
            transport_security,
        })
    } else {
        let transport_security = gateway_base_url.transport_security();
        Ok(RemoteGatewayValidation::Unreachable {
            gateway_base_url,
            transport_security,
        })
    }
}

pub fn plan_add_remote_gateway(
    registry: &GatewayRegistry,
    input: AddRemoteGatewayInput<'_>,
) -> Result<AddRemoteGatewayChange, GatewayProfileError> {
    let profile_plan = plan_add_remote_gateway_profile(
        registry,
        input.new_endpoint_id,
        input.name,
        input.gateway_base_url,
        input.default_remote_name,
    )?;

    Ok(AddRemoteGatewayChange { profile_plan })
}

pub fn plan_activate_gateway_registry(
    registry: &GatewayRegistry,
    gateway_id: &str,
) -> Result<ActivateGatewayRegistryPlan, GatewayProfileError> {
    let mut next_registry = registry.clone();
    let endpoint = endpoint_by_id(registry, gateway_id)
        .cloned()
        .ok_or_else(|| GatewayProfileError::EndpointNotFound {
            id: gateway_id.to_owned(),
        })?;
    let previous_active_gateway_id = registry.active_gateway_id.clone();
    activate_gateway(&mut next_registry, gateway_id)?;

    Ok(ActivateGatewayRegistryPlan {
        registry: next_registry,
        endpoint,
        previous_active_gateway_id,
    })
}

pub fn plan_set_gateway_workspace_registry(
    registry: &GatewayRegistry,
    gateway_id: &str,
    workspace_id: Option<String>,
) -> Result<SetGatewayWorkspaceRegistryPlan, GatewayProfileError> {
    let previous_endpoint = endpoint_by_id(registry, gateway_id)
        .cloned()
        .ok_or_else(|| GatewayProfileError::EndpointNotFound {
            id: gateway_id.to_owned(),
        })?;
    let mut next_registry = registry.clone();
    set_gateway_workspace_id(&mut next_registry, gateway_id, workspace_id)?;
    let endpoint = endpoint_by_id(&next_registry, gateway_id)
        .cloned()
        .ok_or_else(|| GatewayProfileError::EndpointNotFound {
            id: gateway_id.to_owned(),
        })?;

    Ok(SetGatewayWorkspaceRegistryPlan {
        registry: next_registry,
        endpoint,
        previous_endpoint,
    })
}

pub fn plan_update_remote_gateway_registry(
    registry: &GatewayRegistry,
    input: UpdateRemoteGatewayRegistryInput<'_>,
) -> Result<UpdateRemoteGatewayRegistryPlan, GatewayProfileError> {
    let mut next_registry = registry.clone();

    let profile_plan = plan_update_remote_gateway_profile(
        &next_registry,
        input.gateway_id,
        input.name,
        input.gateway_base_url,
        input.default_remote_name,
    )?;

    apply_update_remote_gateway_profile_plan(&mut next_registry, &profile_plan);

    Ok(UpdateRemoteGatewayRegistryPlan {
        registry: next_registry,
        endpoint: profile_plan.endpoint,
        previous_endpoint: profile_plan.previous_endpoint,
    })
}

pub fn plan_delete_remote_gateway_registry(
    registry: &GatewayRegistry,
    input: DeleteRemoteGatewayRegistryInput<'_>,
) -> Result<DeleteRemoteGatewayRegistryPlan, GatewayProfileError> {
    let fallback_endpoint = if registry.active_gateway_id.as_deref() == Some(input.gateway_id) {
        remote_delete_fallback_endpoint(
            registry,
            input.gateway_id,
            input.local_gateway_id.unwrap_or_default(),
        )
    } else {
        None
    };
    let mut next_registry = registry.clone();
    let profile_plan =
        plan_delete_remote_gateway_profile(&next_registry, input.gateway_id, fallback_endpoint)?;
    apply_delete_remote_gateway_profile_plan(&mut next_registry, &profile_plan);

    Ok(DeleteRemoteGatewayRegistryPlan {
        registry: next_registry,
        endpoint: profile_plan.endpoint,
        deleted_active: profile_plan.deleted_active,
        previous_active_gateway_id: profile_plan.previous_active_gateway_id,
        fallback_endpoint: profile_plan.fallback_endpoint,
    })
}

impl fmt::Display for RemoteGatewayValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTimeout { timeout_ms } => {
                write!(
                    f,
                    "remote gateway validation timeout must be positive, got {timeout_ms} ms"
                )
            }
            Self::InvalidGatewayBaseUrl(error) => write!(f, "{error}"),
            Self::ResolveFailed { source, .. } => write!(f, "failed to resolve Gateway: {source}"),
        }
    }
}

impl Error for RemoteGatewayValidationError {}
