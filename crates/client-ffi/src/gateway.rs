use pioneer_client::gateway::{
    runtime::GatewayProfileError,
    setup::{
        ActivateGatewayRegistryPlan, AddRemoteGatewayApplyMode, AddRemoteGatewayInput,
        DeleteRemoteGatewayRegistryInput, DeleteRemoteGatewayRegistryPlan, GatewayAuthTokenUpdate,
        GatewayAuthTokenWrite, RemoteGatewayValidation, RemoteGatewayValidationError,
        SetGatewayWorkspaceRegistryPlan, UpdateRemoteGatewayRegistryInput,
        UpdateRemoteGatewayRegistryPlan, generated_remote_gateway_endpoint_id,
        plan_activate_gateway_registry, plan_add_remote_gateway,
        plan_delete_remote_gateway_registry, plan_set_gateway_workspace_registry,
        plan_update_remote_gateway_registry, validate_remote_gateway_connection,
    },
    types::{GatewayEndpoint, GatewayRegistry},
};
use serde::{Deserialize, Serialize};

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RemoteGatewayValidationRequest {
    pub address: String,
    #[serde(default)]
    pub auth_token: Option<String>,
    pub timeout_ms: u64,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanAddRemoteGatewayRequest {
    pub registry: GatewayRegistry,
    pub name: String,
    pub address: String,
    pub auth_token: Option<String>,
    pub new_endpoint_id: Option<String>,
    pub default_remote_name: String,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize)]
pub struct AddRemoteGatewayPlan {
    pub endpoint: GatewayEndpoint,
    pub previous_endpoint: Option<GatewayEndpoint>,
    pub token_write: Option<GatewayAuthTokenWrite>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize)]
pub struct AddAndActivateRemoteGatewayRegistryPlan {
    pub registry: GatewayRegistry,
    pub endpoint: GatewayEndpoint,
    pub previous_endpoint: Option<GatewayEndpoint>,
    pub token_write: Option<GatewayAuthTokenWrite>,
    pub previous_active_gateway_id: Option<String>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanActivateGatewayRequest {
    pub registry: GatewayRegistry,
    pub gateway_id: String,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanSetGatewayWorkspaceRequest {
    pub registry: GatewayRegistry,
    pub gateway_id: String,
    #[serde(default)]
    pub workspace_id: Option<String>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanUpdateRemoteGatewayRequest {
    pub registry: GatewayRegistry,
    pub gateway_id: String,
    pub name: String,
    pub address: String,
    pub auth_token_update: GatewayAuthTokenUpdate,
    pub default_remote_name: String,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanDeleteRemoteGatewayRequest {
    pub registry: GatewayRegistry,
    pub gateway_id: String,
    #[serde(default)]
    pub local_gateway_id: Option<String>,
    #[serde(default)]
    pub local_gateway_has_auth_token: bool,
}

pub fn validate_remote_gateway_request(
    request: &RemoteGatewayValidationRequest,
) -> Result<RemoteGatewayValidation, RemoteGatewayValidationError> {
    if request.timeout_ms == 0 {
        return Err(RemoteGatewayValidationError::InvalidTimeout {
            timeout_ms: request.timeout_ms,
        });
    }

    validate_remote_gateway_connection(
        request.address.as_str(),
        request.auth_token.as_deref(),
        request.timeout_ms,
    )
}

pub fn plan_add_remote_gateway_request<F>(
    request: PlanAddRemoteGatewayRequest,
    auth_token_ref_for_endpoint: F,
) -> Result<AddRemoteGatewayPlan, GatewayProfileError>
where
    F: FnMut(&str) -> Result<String, GatewayProfileError>,
{
    let new_endpoint_id = request
        .new_endpoint_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(generated_remote_gateway_endpoint_id);

    let change = plan_add_remote_gateway(
        &request.registry,
        AddRemoteGatewayInput {
            name: request.name.as_str(),
            address: request.address.as_str(),
            auth_token: request.auth_token.as_deref(),
            new_endpoint_id,
            default_remote_name: request.default_remote_name,
        },
        auth_token_ref_for_endpoint,
    )?;

    Ok(AddRemoteGatewayPlan {
        endpoint: change.endpoint().clone(),
        previous_endpoint: change.previous_endpoint().cloned(),
        token_write: change.token_write().cloned(),
    })
}

pub fn plan_add_and_activate_remote_gateway_registry_request<F>(
    request: PlanAddRemoteGatewayRequest,
    auth_token_ref_for_endpoint: F,
) -> Result<AddAndActivateRemoteGatewayRegistryPlan, GatewayProfileError>
where
    F: FnMut(&str) -> Result<String, GatewayProfileError>,
{
    let new_endpoint_id = request
        .new_endpoint_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(generated_remote_gateway_endpoint_id);

    let mut registry = request.registry;
    let change = plan_add_remote_gateway(
        &registry,
        AddRemoteGatewayInput {
            name: request.name.as_str(),
            address: request.address.as_str(),
            auth_token: request.auth_token.as_deref(),
            new_endpoint_id,
            default_remote_name: request.default_remote_name,
        },
        auth_token_ref_for_endpoint,
    )?;
    let commit =
        change.apply_to_registry(&mut registry, AddRemoteGatewayApplyMode::ActivateEndpoint)?;

    Ok(AddAndActivateRemoteGatewayRegistryPlan {
        registry,
        endpoint: commit.endpoint,
        previous_endpoint: commit.previous_endpoint,
        token_write: commit.token_write,
        previous_active_gateway_id: commit.previous_active_gateway_id,
    })
}

pub fn plan_activate_gateway_registry_request(
    request: PlanActivateGatewayRequest,
) -> Result<ActivateGatewayRegistryPlan, GatewayProfileError> {
    plan_activate_gateway_registry(&request.registry, request.gateway_id.as_str())
}

pub fn plan_set_gateway_workspace_registry_request(
    request: PlanSetGatewayWorkspaceRequest,
) -> Result<SetGatewayWorkspaceRegistryPlan, GatewayProfileError> {
    plan_set_gateway_workspace_registry(
        &request.registry,
        request.gateway_id.as_str(),
        request.workspace_id,
    )
}

pub fn plan_update_remote_gateway_registry_request<F>(
    request: PlanUpdateRemoteGatewayRequest,
    auth_token_ref_for_endpoint: F,
) -> Result<UpdateRemoteGatewayRegistryPlan, GatewayProfileError>
where
    F: FnMut(&str) -> Result<String, GatewayProfileError>,
{
    let PlanUpdateRemoteGatewayRequest {
        registry,
        gateway_id,
        name,
        address,
        auth_token_update,
        default_remote_name,
    } = request;

    plan_update_remote_gateway_registry(
        &registry,
        UpdateRemoteGatewayRegistryInput {
            gateway_id: gateway_id.as_str(),
            name: name.as_str(),
            address: address.as_str(),
            auth_token_update,
            default_remote_name,
        },
        auth_token_ref_for_endpoint,
    )
}

pub fn plan_delete_remote_gateway_registry_request(
    request: PlanDeleteRemoteGatewayRequest,
) -> Result<DeleteRemoteGatewayRegistryPlan, GatewayProfileError> {
    plan_delete_remote_gateway_registry(
        &request.registry,
        DeleteRemoteGatewayRegistryInput {
            gateway_id: request.gateway_id.as_str(),
            local_gateway_id: request.local_gateway_id.as_deref(),
            local_gateway_has_auth_token: request.local_gateway_has_auth_token,
        },
    )
}
