//! Active gateway profile and operation epoch state.

use super::{
    connectivity::normalize_address,
    registry::remote_index_by_address,
    timings::GatewayWsTimings,
    types::{GatewayEndpoint, GatewayEndpointKind, GatewayRegistry},
};
use std::{error::Error, fmt, time::Duration};

pub type GatewayOperationEpoch = u64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActiveGatewayState {
    NotConfigured,
    Connected,
    Unreachable,
    LocalAddressConflict,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum GatewaySetupAction {
    ConnectRemote,
    StartLocal,
    SaveGateway,
    DeleteGateway,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GatewayProfileError {
    EndpointNotFound { id: String },
    LocalGatewayDeleteUnsupported,
    DuplicateRemoteAddress { address: String },
    InvalidAddress { address: String, reason: String },
    SessionBoundAddressChange { endpoint_id: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AddRemoteGatewayProfilePlan {
    Add {
        endpoint: GatewayEndpoint,
    },
    UpdateExisting {
        index: usize,
        previous_endpoint: GatewayEndpoint,
        endpoint: GatewayEndpoint,
    },
}

impl AddRemoteGatewayProfilePlan {
    pub fn endpoint(&self) -> &GatewayEndpoint {
        match self {
            Self::Add { endpoint } | Self::UpdateExisting { endpoint, .. } => endpoint,
        }
    }
}

#[derive(Clone, Debug)]
pub struct UpdateRemoteGatewayProfilePlan {
    pub index: usize,
    pub previous_endpoint: GatewayEndpoint,
    pub endpoint: GatewayEndpoint,
}

#[derive(Clone, Debug)]
pub struct DeleteRemoteGatewayProfilePlan {
    pub index: usize,
    pub endpoint: GatewayEndpoint,
    pub deleted_active: bool,
    pub previous_active_gateway_id: Option<String>,
    pub fallback_endpoint: Option<GatewayEndpoint>,
}

pub fn next_gateway_operation_epoch(current: GatewayOperationEpoch) -> GatewayOperationEpoch {
    current.saturating_add(1)
}

pub fn should_apply_gateway_operation_result(
    current_epoch: GatewayOperationEpoch,
    operation_epoch: GatewayOperationEpoch,
) -> bool {
    current_epoch == operation_epoch
}

pub fn classify_local_gateway_state(reachable: bool, service_active: bool) -> ActiveGatewayState {
    match (reachable, service_active) {
        (true, true) => ActiveGatewayState::Connected,
        (true, false) => ActiveGatewayState::LocalAddressConflict,
        (false, _) => ActiveGatewayState::Unreachable,
    }
}

pub fn normalize_local_service_active(reachable: bool, service_active: bool) -> bool {
    if reachable {
        return true;
    }

    service_active
}

pub fn active_gateway_id(registry: &GatewayRegistry) -> Option<&str> {
    registry.active_gateway_id.as_deref()
}

pub fn endpoint_by_id<'a>(registry: &'a GatewayRegistry, id: &str) -> Option<&'a GatewayEndpoint> {
    if let Some(local) = registry.local.as_ref().filter(|local| local.id == id) {
        return Some(local);
    }

    registry.remotes.iter().find(|endpoint| endpoint.id == id)
}

pub fn endpoint_by_id_mut<'a>(
    registry: &'a mut GatewayRegistry,
    id: &str,
) -> Option<&'a mut GatewayEndpoint> {
    if registry.local.as_ref().is_some_and(|local| local.id == id) {
        return registry.local.as_mut();
    }

    registry
        .remotes
        .iter_mut()
        .find(|endpoint| endpoint.id == id)
}

pub fn active_gateway(registry: &GatewayRegistry) -> Option<&GatewayEndpoint> {
    let active_id = registry.active_gateway_id.as_deref()?;
    endpoint_by_id(registry, active_id)
}

pub fn active_workspace_id(registry: &GatewayRegistry) -> Option<&str> {
    active_gateway(registry).and_then(|endpoint| endpoint.workspace_id.as_deref())
}

pub fn local_gateway_is_selectable(registry: &GatewayRegistry, local_gateway_id: &str) -> bool {
    let local_gateway_id = local_gateway_id.trim();
    registry
        .local
        .as_ref()
        .is_some_and(|local| local.id == local_gateway_id)
        && (registry.active_gateway_id.as_deref() == Some(local_gateway_id)
            || registry
                .local
                .as_ref()
                .is_some_and(|local| local.session_ref.is_some()))
}

pub fn selectable_gateway_endpoints(
    registry: &GatewayRegistry,
    local_gateway_id: &str,
) -> Vec<GatewayEndpoint> {
    let mut endpoints = Vec::with_capacity(registry.remotes.len() + 1);
    if local_gateway_is_selectable(registry, local_gateway_id) {
        if let Some(local) = registry.local.as_ref() {
            endpoints.push(local.clone());
        }
    }
    endpoints.extend(registry.remotes.clone());
    endpoints
}

pub fn remote_delete_fallback_endpoint(
    registry: &GatewayRegistry,
    deleted_id: &str,
    local_gateway_id: &str,
) -> Option<GatewayEndpoint> {
    if local_gateway_is_selectable(registry, local_gateway_id) {
        return registry.local.clone();
    }

    registry
        .remotes
        .iter()
        .find(|endpoint| endpoint.id != deleted_id)
        .cloned()
}

pub fn activate_gateway(
    registry: &mut GatewayRegistry,
    id: &str,
) -> Result<(), GatewayProfileError> {
    if endpoint_by_id(registry, id).is_none() {
        return Err(GatewayProfileError::EndpointNotFound { id: id.to_owned() });
    }

    registry.active_gateway_id = Some(id.to_owned());
    Ok(())
}

pub fn set_gateway_workspace_id(
    registry: &mut GatewayRegistry,
    gateway_id: &str,
    workspace_id: Option<String>,
) -> Result<(), GatewayProfileError> {
    let Some(endpoint) = endpoint_by_id_mut(registry, gateway_id) else {
        return Err(GatewayProfileError::EndpointNotFound {
            id: gateway_id.to_owned(),
        });
    };

    endpoint.workspace_id = workspace_id;
    Ok(())
}

pub fn gateway_activation_requires_local_start(endpoint_kind: Option<GatewayEndpointKind>) -> bool {
    endpoint_kind == Some(GatewayEndpointKind::Local)
}

pub fn gateway_activation_is_noop(
    active_gateway_id: Option<&str>,
    gateway_id: &str,
    has_ready_connection: bool,
) -> bool {
    active_gateway_id == Some(gateway_id) && has_ready_connection
}

pub fn gateway_setup_required(
    bootstrap_complete: bool,
    runtime_setup_required: Option<bool>,
) -> bool {
    bootstrap_complete && runtime_setup_required.unwrap_or(true)
}

pub fn plan_add_remote_gateway_profile(
    registry: &GatewayRegistry,
    new_endpoint_id: String,
    name: &str,
    address: &str,
    default_remote_name: String,
) -> Result<AddRemoteGatewayProfilePlan, GatewayProfileError> {
    let address = normalize_remote_gateway_address(address)?;

    if let Some(existing_index) = remote_index_by_address(registry, address.as_str(), None) {
        let previous_endpoint = registry.remotes[existing_index].clone();
        let endpoint_name = remote_gateway_name_or_default(name, previous_endpoint.name.clone());
        let mut endpoint = previous_endpoint.clone();
        endpoint.name = endpoint_name;
        endpoint.address = address;
        return Ok(AddRemoteGatewayProfilePlan::UpdateExisting {
            index: existing_index,
            previous_endpoint,
            endpoint,
        });
    }

    let endpoint_name = remote_gateway_name_or_default(name, default_remote_name);
    let endpoint = GatewayEndpoint {
        id: new_endpoint_id,
        name: endpoint_name,
        address,
        kind: GatewayEndpointKind::Remote,
        session_ref: None,
        server_gateway_id: None,
        workspace_id: None,
        service_name: None,
    };

    Ok(AddRemoteGatewayProfilePlan::Add { endpoint })
}

pub fn apply_add_remote_gateway_profile_plan(
    registry: &mut GatewayRegistry,
    plan: &AddRemoteGatewayProfilePlan,
) {
    match plan {
        AddRemoteGatewayProfilePlan::Add { endpoint } => {
            registry.remotes.push(endpoint.clone());
        }
        AddRemoteGatewayProfilePlan::UpdateExisting {
            index, endpoint, ..
        } => {
            if let Some(existing) = registry.remotes.get_mut(*index) {
                *existing = endpoint.clone();
            }
        }
    }
}

pub fn rollback_add_remote_gateway_profile_plan(
    registry: &mut GatewayRegistry,
    plan: &AddRemoteGatewayProfilePlan,
) {
    match plan {
        AddRemoteGatewayProfilePlan::Add { endpoint } => {
            registry
                .remotes
                .retain(|remote| remote.id != endpoint.id.as_str());
        }
        AddRemoteGatewayProfilePlan::UpdateExisting {
            index,
            previous_endpoint,
            ..
        } => {
            if let Some(existing) = registry.remotes.get_mut(*index) {
                *existing = previous_endpoint.clone();
            }
        }
    }
}

pub fn plan_update_remote_gateway_profile(
    registry: &GatewayRegistry,
    id: &str,
    name: &str,
    address: &str,
    default_remote_name: String,
) -> Result<UpdateRemoteGatewayProfilePlan, GatewayProfileError> {
    let address = normalize_remote_gateway_address(address)?;
    let Some(existing_index) = registry.remotes.iter().position(|remote| remote.id == id) else {
        return Err(GatewayProfileError::EndpointNotFound { id: id.to_owned() });
    };

    if remote_index_by_address(registry, address.as_str(), Some(id)).is_some() {
        return Err(GatewayProfileError::DuplicateRemoteAddress { address });
    }

    let previous_endpoint = registry.remotes[existing_index].clone();
    if previous_endpoint.session_ref.is_some() && address != previous_endpoint.address {
        return Err(GatewayProfileError::SessionBoundAddressChange {
            endpoint_id: id.to_owned(),
        });
    }
    let endpoint_name = remote_gateway_name_or_default(name, default_remote_name);

    let mut endpoint = previous_endpoint.clone();
    endpoint.name = endpoint_name;
    endpoint.address = address;
    endpoint.kind = GatewayEndpointKind::Remote;
    endpoint.service_name = None;

    Ok(UpdateRemoteGatewayProfilePlan {
        index: existing_index,
        previous_endpoint,
        endpoint,
    })
}

pub fn apply_update_remote_gateway_profile_plan(
    registry: &mut GatewayRegistry,
    plan: &UpdateRemoteGatewayProfilePlan,
) {
    if let Some(existing) = registry.remotes.get_mut(plan.index) {
        *existing = plan.endpoint.clone();
    }
}

pub fn rollback_update_remote_gateway_profile_plan(
    registry: &mut GatewayRegistry,
    plan: &UpdateRemoteGatewayProfilePlan,
) {
    if let Some(existing) = registry.remotes.get_mut(plan.index) {
        *existing = plan.previous_endpoint.clone();
    }
}

pub fn plan_delete_remote_gateway_profile(
    registry: &GatewayRegistry,
    id: &str,
    fallback_endpoint: Option<GatewayEndpoint>,
) -> Result<DeleteRemoteGatewayProfilePlan, GatewayProfileError> {
    if registry.local.as_ref().is_some_and(|local| local.id == id) {
        return Err(GatewayProfileError::LocalGatewayDeleteUnsupported);
    }

    let Some(index) = registry.remotes.iter().position(|remote| remote.id == id) else {
        return Err(GatewayProfileError::EndpointNotFound { id: id.to_owned() });
    };

    Ok(DeleteRemoteGatewayProfilePlan {
        index,
        endpoint: registry.remotes[index].clone(),
        deleted_active: registry.active_gateway_id.as_deref() == Some(id),
        previous_active_gateway_id: registry.active_gateway_id.clone(),
        fallback_endpoint,
    })
}

pub fn apply_delete_remote_gateway_profile_plan(
    registry: &mut GatewayRegistry,
    plan: &DeleteRemoteGatewayProfilePlan,
) {
    if plan.index < registry.remotes.len() {
        registry.remotes.remove(plan.index);
    }
    if plan.deleted_active {
        registry.active_gateway_id = plan
            .fallback_endpoint
            .as_ref()
            .map(|endpoint| endpoint.id.clone());
    }
}

pub fn rollback_delete_remote_gateway_profile_plan(
    registry: &mut GatewayRegistry,
    plan: &DeleteRemoteGatewayProfilePlan,
) {
    let insert_index = plan.index.min(registry.remotes.len());
    if !registry
        .remotes
        .iter()
        .any(|endpoint| endpoint.id == plan.endpoint.id)
    {
        registry.remotes.insert(insert_index, plan.endpoint.clone());
    }
    registry.active_gateway_id = plan.previous_active_gateway_id.clone();
}

pub fn ws_timings_for_endpoint(
    mut timings: GatewayWsTimings,
    endpoint_kind: GatewayEndpointKind,
    remote_connect_timeout_min: Duration,
) -> GatewayWsTimings {
    if endpoint_kind == GatewayEndpointKind::Remote
        && timings.connect_timeout < remote_connect_timeout_min
    {
        timings.connect_timeout = remote_connect_timeout_min;
    }

    timings
}

fn normalize_remote_gateway_address(address: &str) -> Result<String, GatewayProfileError> {
    normalize_address(address).map_err(|error| GatewayProfileError::InvalidAddress {
        address: address.trim().to_owned(),
        reason: error.to_string(),
    })
}

fn remote_gateway_name_or_default(name: &str, default_name: String) -> String {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        default_name
    } else {
        trimmed.to_owned()
    }
}

impl fmt::Display for GatewayProfileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EndpointNotFound { id } => write!(f, "gateway endpoint `{id}` was not found"),
            Self::LocalGatewayDeleteUnsupported => {
                write!(f, "local gateway cannot be deleted")
            }
            Self::DuplicateRemoteAddress { address } => {
                write!(f, "gateway address `{address}` already exists")
            }
            Self::InvalidAddress { address, reason } => {
                write!(f, "invalid gateway address `{address}`: {reason}")
            }
            Self::SessionBoundAddressChange { endpoint_id } => write!(
                f,
                "session-bound Gateway endpoint `{endpoint_id}` cannot change address without reauthentication"
            ),
        }
    }
}

impl Error for GatewayProfileError {}
