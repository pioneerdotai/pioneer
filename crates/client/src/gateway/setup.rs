//! Shared gateway setup workflows.

use super::{
    connectivity::{GatewayAddressError, is_gateway_reachable, normalize_address},
    runtime::{
        AddRemoteGatewayProfilePlan, GatewayProfileError, activate_gateway,
        apply_add_remote_gateway_profile_plan, apply_delete_remote_gateway_profile_plan,
        apply_update_remote_gateway_profile_plan, endpoint_by_id, plan_add_remote_gateway_profile,
        plan_delete_remote_gateway_profile, plan_remote_candidate_connect_spec,
        plan_update_remote_gateway_profile, remote_delete_fallback_endpoint,
        rollback_add_remote_gateway_profile_plan,
    },
    secrets::{gateway_auth_token_label, normalize_gateway_auth_token},
    timings::{GatewayTimingError, GatewayWsTimings},
    types::{GatewayEndpoint, GatewayRegistry},
};
use crate::transport::ws::client::connect_websocket_once;
use pioneer_protocol::generate_id;
use serde::{Deserialize, Serialize};
use std::{error::Error, fmt, time::Duration};

const REMOTE_GATEWAY_VALIDATION_ENDPOINT_ID: &str = "remote-validation";
const REMOTE_GATEWAY_VALIDATION_ENDPOINT_NAME: &str = "Remote Gateway";
const REMOTE_GATEWAY_VALIDATION_PING_INTERVAL_MS: u64 = 10_000;
const REMOTE_GATEWAY_VALIDATION_PONG_TIMEOUT_MS: u64 = 30_000;
const REMOTE_GATEWAY_VALIDATION_RECONNECT_INITIAL_MS: u64 = 500;
const REMOTE_GATEWAY_VALIDATION_RECONNECT_MAX_MS: u64 = 10_000;
const REMOTE_GATEWAY_VALIDATION_RECONNECT_JITTER_PERCENT: u8 = 20;

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RemoteGatewayValidationRequest {
    pub address: String,
    #[serde(default)]
    pub auth_token: Option<String>,
    pub timeout_ms: u64,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum RemoteGatewayValidation {
    Reachable { address: String },
    Unreachable { address: String },
}

impl RemoteGatewayValidation {
    pub fn address(&self) -> &str {
        match self {
            Self::Reachable { address } | Self::Unreachable { address } => address.as_str(),
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
    InvalidAddress(GatewayAddressError),
    ResolveFailed {
        address: String,
        source: GatewayAddressError,
    },
    InvalidTimings(GatewayTimingError),
    ConnectionFailed {
        address: String,
        reason: String,
    },
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
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

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize)]
pub struct AddRemoteGatewayPlan {
    pub endpoint: GatewayEndpoint,
    pub previous_endpoint: Option<GatewayEndpoint>,
    pub token_write: Option<GatewayAuthTokenWrite>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize)]
pub struct AddAndActivateRemoteGatewayRegistryPlan {
    pub registry: GatewayRegistry,
    pub endpoint: GatewayEndpoint,
    pub previous_endpoint: Option<GatewayEndpoint>,
    pub token_write: Option<GatewayAuthTokenWrite>,
    pub previous_active_gateway_id: Option<String>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanActivateGatewayRequest {
    pub registry: GatewayRegistry,
    pub gateway_id: String,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize)]
pub struct ActivateGatewayRegistryPlan {
    pub registry: GatewayRegistry,
    pub endpoint: GatewayEndpoint,
    pub previous_active_gateway_id: Option<String>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum GatewayAuthTokenUpdate {
    Preserve,
    Replace { token: String },
    Clear,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
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

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize)]
pub struct UpdateRemoteGatewayRegistryPlan {
    pub registry: GatewayRegistry,
    pub endpoint: GatewayEndpoint,
    pub previous_endpoint: GatewayEndpoint,
    pub token_write: Option<GatewayAuthTokenWrite>,
    pub deleted_token_ref: Option<String>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
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

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize)]
pub struct DeleteRemoteGatewayRegistryPlan {
    pub registry: GatewayRegistry,
    pub endpoint: GatewayEndpoint,
    pub deleted_active: bool,
    pub previous_active_gateway_id: Option<String>,
    pub fallback_endpoint: Option<GatewayEndpoint>,
    pub deleted_token_ref: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AddRemoteGatewayInput<'a> {
    pub name: &'a str,
    pub address: &'a str,
    pub auth_token: Option<&'a str>,
    pub new_endpoint_id: String,
    pub default_remote_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateRemoteGatewayRegistryInput<'a> {
    pub gateway_id: &'a str,
    pub name: &'a str,
    pub address: &'a str,
    pub auth_token_update: GatewayAuthTokenUpdate,
    pub default_remote_name: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeleteRemoteGatewayRegistryInput<'a> {
    pub gateway_id: &'a str,
    pub local_gateway_id: Option<&'a str>,
    pub local_gateway_has_auth_token: bool,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GatewayAuthTokenWrite {
    pub token_ref: String,
    pub token: String,
    pub label: String,
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
    pub token_write: Option<GatewayAuthTokenWrite>,
    pub previous_active_gateway_id: Option<String>,
}

#[derive(Clone, Debug)]
pub struct AddRemoteGatewayChange {
    profile_plan: AddRemoteGatewayProfilePlan,
    token_write: Option<GatewayAuthTokenWrite>,
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

    pub fn token_write(&self) -> Option<&GatewayAuthTokenWrite> {
        self.token_write.as_ref()
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
            token_write: self.token_write.clone(),
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

pub fn validate_remote_gateway_address(
    address: &str,
    connect_timeout: Duration,
) -> Result<RemoteGatewayValidation, RemoteGatewayValidationError> {
    let address =
        normalize_address(address).map_err(RemoteGatewayValidationError::InvalidAddress)?;
    let reachable = is_gateway_reachable(address.as_str(), connect_timeout).map_err(|source| {
        RemoteGatewayValidationError::ResolveFailed {
            address: address.clone(),
            source,
        }
    })?;

    if reachable {
        Ok(RemoteGatewayValidation::Reachable { address })
    } else {
        Ok(RemoteGatewayValidation::Unreachable { address })
    }
}

pub fn validate_remote_gateway_connection(
    address: &str,
    auth_token: Option<&str>,
    connect_timeout_ms: u64,
) -> Result<RemoteGatewayValidation, RemoteGatewayValidationError> {
    let timings = GatewayWsTimings::from_millis(
        connect_timeout_ms,
        REMOTE_GATEWAY_VALIDATION_PING_INTERVAL_MS,
        REMOTE_GATEWAY_VALIDATION_PONG_TIMEOUT_MS,
        REMOTE_GATEWAY_VALIDATION_RECONNECT_INITIAL_MS,
        REMOTE_GATEWAY_VALIDATION_RECONNECT_MAX_MS,
        REMOTE_GATEWAY_VALIDATION_RECONNECT_JITTER_PERCENT,
    )
    .map_err(RemoteGatewayValidationError::InvalidTimings)?;

    validate_remote_gateway_connection_with_timings(address, auth_token, timings)
}

pub fn validate_remote_gateway_connection_with_timings(
    address: &str,
    auth_token: Option<&str>,
    timings: GatewayWsTimings,
) -> Result<RemoteGatewayValidation, RemoteGatewayValidationError> {
    let address =
        normalize_address(address).map_err(RemoteGatewayValidationError::InvalidAddress)?;
    let plan = plan_remote_candidate_connect_spec(
        REMOTE_GATEWAY_VALIDATION_ENDPOINT_ID.to_owned(),
        "",
        REMOTE_GATEWAY_VALIDATION_ENDPOINT_NAME.to_owned(),
        address.as_str(),
        auth_token.unwrap_or_default(),
        timings,
    );
    let spec = plan.into();

    connect_websocket_once(&spec).map_err(|error| {
        RemoteGatewayValidationError::ConnectionFailed {
            address: address.clone(),
            reason: format!("{error:#}"),
        }
    })?;

    Ok(RemoteGatewayValidation::Reachable { address })
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

pub fn plan_add_remote_gateway<F>(
    registry: &GatewayRegistry,
    input: AddRemoteGatewayInput<'_>,
    auth_token_ref_for_endpoint: F,
) -> Result<AddRemoteGatewayChange, GatewayProfileError>
where
    F: FnMut(&str) -> Result<String, GatewayProfileError>,
{
    let auth_token = input.auth_token.and_then(normalize_gateway_auth_token);

    let profile_plan = plan_add_remote_gateway_profile(
        registry,
        input.new_endpoint_id,
        input.name,
        input.address,
        auth_token.is_some(),
        auth_token_ref_for_endpoint,
        input.default_remote_name,
    )?;

    let token_write = auth_token.map(|token| {
        let token_ref = profile_plan
            .auth_token_ref()
            .expect("token ref should exist when auth token is present")
            .to_owned();
        GatewayAuthTokenWrite {
            token_ref,
            token,
            label: gateway_auth_token_label(
                profile_plan.endpoint().name.as_str(),
                profile_plan.endpoint().address.as_str(),
            ),
        }
    });

    Ok(AddRemoteGatewayChange {
        profile_plan,
        token_write,
    })
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

pub fn plan_update_remote_gateway_registry<F>(
    registry: &GatewayRegistry,
    input: UpdateRemoteGatewayRegistryInput<'_>,
    auth_token_ref_for_endpoint: F,
) -> Result<UpdateRemoteGatewayRegistryPlan, GatewayProfileError>
where
    F: FnMut(&str) -> Result<String, GatewayProfileError>,
{
    let existing = registry
        .remotes
        .iter()
        .find(|endpoint| endpoint.id == input.gateway_id)
        .ok_or_else(|| GatewayProfileError::EndpointNotFound {
            id: input.gateway_id.to_owned(),
        })?;
    let auth_token = match &input.auth_token_update {
        GatewayAuthTokenUpdate::Replace { token } => normalize_gateway_auth_token(token),
        GatewayAuthTokenUpdate::Preserve | GatewayAuthTokenUpdate::Clear => None,
    };
    let keeps_existing_auth_token =
        matches!(&input.auth_token_update, GatewayAuthTokenUpdate::Preserve)
            && existing.auth_token_ref.is_some();
    let has_auth_token = auth_token.is_some() || keeps_existing_auth_token;
    let mut next_registry = registry.clone();

    let mut profile_plan = plan_update_remote_gateway_profile(
        &next_registry,
        input.gateway_id,
        input.name,
        input.address,
        has_auth_token,
        auth_token_ref_for_endpoint,
        input.default_remote_name,
    )?;
    if matches!(&input.auth_token_update, GatewayAuthTokenUpdate::Preserve) {
        profile_plan.endpoint.auth_token_ref =
            profile_plan.previous_endpoint.auth_token_ref.clone();
    }

    let token_write = auth_token.map(|token| {
        let token_ref = profile_plan
            .auth_token_ref()
            .expect("token ref should exist when auth token is present")
            .to_owned();
        GatewayAuthTokenWrite {
            token_ref,
            token,
            label: gateway_auth_token_label(
                profile_plan.endpoint.name.as_str(),
                profile_plan.endpoint.address.as_str(),
            ),
        }
    });
    let deleted_token_ref = match &input.auth_token_update {
        GatewayAuthTokenUpdate::Clear => profile_plan.previous_endpoint.auth_token_ref.clone(),
        GatewayAuthTokenUpdate::Replace { .. } => profile_plan
            .previous_endpoint
            .auth_token_ref
            .clone()
            .filter(|previous_ref| {
                token_write
                    .as_ref()
                    .is_none_or(|write| write.token_ref != *previous_ref)
            }),
        GatewayAuthTokenUpdate::Preserve => None,
    };

    apply_update_remote_gateway_profile_plan(&mut next_registry, &profile_plan);

    Ok(UpdateRemoteGatewayRegistryPlan {
        registry: next_registry,
        endpoint: profile_plan.endpoint,
        previous_endpoint: profile_plan.previous_endpoint,
        token_write,
        deleted_token_ref,
    })
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

pub fn plan_delete_remote_gateway_registry(
    registry: &GatewayRegistry,
    input: DeleteRemoteGatewayRegistryInput<'_>,
) -> Result<DeleteRemoteGatewayRegistryPlan, GatewayProfileError> {
    let fallback_endpoint = if registry.active_gateway_id.as_deref() == Some(input.gateway_id) {
        remote_delete_fallback_endpoint(
            registry,
            input.gateway_id,
            input.local_gateway_id.unwrap_or_default(),
            input.local_gateway_has_auth_token,
        )
    } else {
        None
    };
    let mut next_registry = registry.clone();
    let profile_plan =
        plan_delete_remote_gateway_profile(&next_registry, input.gateway_id, fallback_endpoint)?;
    let deleted_token_ref = profile_plan.endpoint.auth_token_ref.clone();

    apply_delete_remote_gateway_profile_plan(&mut next_registry, &profile_plan);

    Ok(DeleteRemoteGatewayRegistryPlan {
        registry: next_registry,
        endpoint: profile_plan.endpoint,
        deleted_active: profile_plan.deleted_active,
        previous_active_gateway_id: profile_plan.previous_active_gateway_id,
        fallback_endpoint: profile_plan.fallback_endpoint,
        deleted_token_ref,
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
            Self::InvalidAddress(error) => write!(f, "{error}"),
            Self::ResolveFailed { address, source } => {
                write!(
                    f,
                    "failed to resolve remote gateway address `{address}`: {source}"
                )
            }
            Self::InvalidTimings(error) => write!(f, "{error}"),
            Self::ConnectionFailed { address, reason } => {
                write!(
                    f,
                    "failed to connect to remote gateway `{address}`: {reason}"
                )
            }
        }
    }
}

impl Error for RemoteGatewayValidationError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gateway::types::{GatewayEndpointKind, GatewayRegistry};
    use std::net::TcpListener;

    fn registry() -> GatewayRegistry {
        GatewayRegistry {
            version: 1,
            active_gateway_id: None,
            local: Some(GatewayEndpoint {
                id: "local".to_owned(),
                name: "Local".to_owned(),
                address: "127.0.0.1:17878".to_owned(),
                kind: GatewayEndpointKind::Local,
                auth_token_ref: None,
                workspace_id: None,
                service_name: None,
            }),
            remotes: Vec::new(),
        }
    }

    fn token_ref(endpoint_id: &str) -> Result<String, GatewayProfileError> {
        Ok(endpoint_id.to_owned())
    }

    #[test]
    fn add_remote_gateway_change_plans_endpoint_and_secret_write() {
        let mut registry = registry();
        let change = plan_add_remote_gateway(
            &registry,
            AddRemoteGatewayInput {
                name: " Remote ",
                address: "127.0.0.1:23000",
                auth_token: Some(" token "),
                new_endpoint_id: "remote-one".to_owned(),
                default_remote_name: "Remote 1".to_owned(),
            },
            token_ref,
        )
        .expect("plan add remote");

        assert_eq!(change.endpoint().id, "remote-one");
        assert_eq!(change.endpoint().name, "Remote");
        assert_eq!(
            change.endpoint().auth_token_ref.as_deref(),
            Some("remote-one")
        );
        assert_eq!(
            change.token_write(),
            Some(&GatewayAuthTokenWrite {
                token_ref: "remote-one".to_owned(),
                token: "token".to_owned(),
                label: "Remote (127.0.0.1:23000)".to_owned(),
            })
        );

        let commit = change
            .apply_to_registry(&mut registry, AddRemoteGatewayApplyMode::ProfileOnly)
            .expect("apply add remote");
        assert_eq!(registry.remotes.len(), 1);
        assert_eq!(commit.endpoint.id, "remote-one");
        assert_eq!(
            commit
                .token_write
                .as_ref()
                .map(|write| write.token.as_str()),
            Some("token")
        );

        change.rollback_commit(&mut registry, &commit);
        assert!(registry.remotes.is_empty());
    }

    #[test]
    fn add_remote_gateway_change_updates_existing_and_preserves_token_ref_without_new_token() {
        let mut registry = registry();
        registry.remotes.push(GatewayEndpoint {
            id: "remote-one".to_owned(),
            name: "Remote".to_owned(),
            address: "127.0.0.1:23000".to_owned(),
            kind: GatewayEndpointKind::Remote,
            auth_token_ref: Some("remote-one".to_owned()),
            workspace_id: Some("workspace".to_owned()),
            service_name: None,
        });

        let change = plan_add_remote_gateway(
            &registry,
            AddRemoteGatewayInput {
                name: "Renamed",
                address: "127.0.0.1:23000",
                auth_token: None,
                new_endpoint_id: "remote-unused".to_owned(),
                default_remote_name: "Remote 2".to_owned(),
            },
            token_ref,
        )
        .expect("plan update existing remote");

        assert_eq!(
            change
                .previous_endpoint()
                .map(|endpoint| endpoint.name.as_str()),
            Some("Remote")
        );
        assert_eq!(change.endpoint().id, "remote-one");
        assert_eq!(change.endpoint().name, "Renamed");
        assert_eq!(
            change.endpoint().auth_token_ref.as_deref(),
            Some("remote-one")
        );
        assert!(change.token_write().is_none());

        change
            .apply_to_registry(&mut registry, AddRemoteGatewayApplyMode::ProfileOnly)
            .expect("apply update existing");
        assert_eq!(registry.remotes[0].name, "Renamed");
        assert_eq!(
            registry.remotes[0].workspace_id.as_deref(),
            Some("workspace")
        );
    }

    #[test]
    fn add_remote_gateway_change_updates_existing_token_write_for_existing_ref() {
        let mut registry = registry();
        registry.remotes.push(GatewayEndpoint {
            id: "remote-one".to_owned(),
            name: "Remote".to_owned(),
            address: "127.0.0.1:23000".to_owned(),
            kind: GatewayEndpointKind::Remote,
            auth_token_ref: Some("remote-one".to_owned()),
            workspace_id: None,
            service_name: None,
        });

        let change = plan_add_remote_gateway(
            &registry,
            AddRemoteGatewayInput {
                name: "Remote",
                address: "127.0.0.1:23000",
                auth_token: Some("new-token"),
                new_endpoint_id: "remote-unused".to_owned(),
                default_remote_name: "Remote 2".to_owned(),
            },
            token_ref,
        )
        .expect("plan update token");

        assert_eq!(
            change.token_write(),
            Some(&GatewayAuthTokenWrite {
                token_ref: "remote-one".to_owned(),
                token: "new-token".to_owned(),
                label: "Remote (127.0.0.1:23000)".to_owned(),
            })
        );
    }

    #[test]
    fn add_remote_gateway_request_plans_with_shared_dto() {
        let plan = plan_add_remote_gateway_request(
            PlanAddRemoteGatewayRequest {
                registry: registry(),
                name: " Remote ".to_owned(),
                address: "127.0.0.1:23000".to_owned(),
                auth_token: Some(" token ".to_owned()),
                new_endpoint_id: Some("remote-one".to_owned()),
                default_remote_name: "Remote 1".to_owned(),
            },
            token_ref,
        )
        .expect("plan from request");

        assert_eq!(plan.endpoint.id, "remote-one");
        assert_eq!(plan.endpoint.name, "Remote");
        assert_eq!(plan.endpoint.auth_token_ref.as_deref(), Some("remote-one"));
        assert_eq!(
            plan.token_write,
            Some(GatewayAuthTokenWrite {
                token_ref: "remote-one".to_owned(),
                token: "token".to_owned(),
                label: "Remote (127.0.0.1:23000)".to_owned(),
            })
        );
    }

    #[test]
    fn add_and_activate_remote_gateway_registry_request_returns_next_registry() {
        let plan = plan_add_and_activate_remote_gateway_registry_request(
            PlanAddRemoteGatewayRequest {
                registry: registry(),
                name: " Remote ".to_owned(),
                address: "127.0.0.1:23000".to_owned(),
                auth_token: Some(" token ".to_owned()),
                new_endpoint_id: Some("remote-one".to_owned()),
                default_remote_name: "Remote 1".to_owned(),
            },
            token_ref,
        )
        .expect("plan registry update from request");

        assert_eq!(plan.endpoint.id, "remote-one");
        assert_eq!(plan.previous_active_gateway_id, None);
        assert_eq!(
            plan.registry.active_gateway_id.as_deref(),
            Some("remote-one")
        );
        assert_eq!(plan.registry.remotes.len(), 1);
        assert_eq!(plan.registry.remotes[0].id, "remote-one");
        assert_eq!(
            plan.token_write,
            Some(GatewayAuthTokenWrite {
                token_ref: "remote-one".to_owned(),
                token: "token".to_owned(),
                label: "Remote (127.0.0.1:23000)".to_owned(),
            })
        );
    }

    #[test]
    fn activate_gateway_registry_request_switches_active_gateway() {
        let mut registry = registry();
        registry.remotes.push(GatewayEndpoint {
            id: "remote-one".to_owned(),
            name: "Remote".to_owned(),
            address: "127.0.0.1:23000".to_owned(),
            kind: GatewayEndpointKind::Remote,
            auth_token_ref: None,
            workspace_id: None,
            service_name: None,
        });
        registry.active_gateway_id = Some("local".to_owned());

        let plan = plan_activate_gateway_registry_request(PlanActivateGatewayRequest {
            registry,
            gateway_id: "remote-one".to_owned(),
        })
        .expect("plan activation");

        assert_eq!(plan.endpoint.id, "remote-one");
        assert_eq!(plan.previous_active_gateway_id.as_deref(), Some("local"));
        assert_eq!(
            plan.registry.active_gateway_id.as_deref(),
            Some("remote-one")
        );
    }

    #[test]
    fn update_remote_gateway_registry_request_preserves_replaces_and_clears_token() {
        let mut registry = registry();
        registry.remotes.push(GatewayEndpoint {
            id: "remote-one".to_owned(),
            name: "Remote".to_owned(),
            address: "127.0.0.1:23000".to_owned(),
            kind: GatewayEndpointKind::Remote,
            auth_token_ref: Some("remote-one".to_owned()),
            workspace_id: Some("workspace".to_owned()),
            service_name: None,
        });

        let preserved = plan_update_remote_gateway_registry_request(
            PlanUpdateRemoteGatewayRequest {
                registry: registry.clone(),
                gateway_id: "remote-one".to_owned(),
                name: "Renamed".to_owned(),
                address: "127.0.0.1:23000".to_owned(),
                auth_token_update: GatewayAuthTokenUpdate::Preserve,
                default_remote_name: "Remote 1".to_owned(),
            },
            token_ref,
        )
        .expect("plan preserve token update");

        assert_eq!(preserved.endpoint.name, "Renamed");
        assert_eq!(
            preserved.endpoint.auth_token_ref.as_deref(),
            Some("remote-one")
        );
        assert_eq!(
            preserved.endpoint.workspace_id.as_deref(),
            Some("workspace")
        );
        assert!(preserved.token_write.is_none());
        assert!(preserved.deleted_token_ref.is_none());

        let replaced = plan_update_remote_gateway_registry_request(
            PlanUpdateRemoteGatewayRequest {
                registry: registry.clone(),
                gateway_id: "remote-one".to_owned(),
                name: "Remote".to_owned(),
                address: "127.0.0.1:24000".to_owned(),
                auth_token_update: GatewayAuthTokenUpdate::Replace {
                    token: " new-token ".to_owned(),
                },
                default_remote_name: "Remote 1".to_owned(),
            },
            token_ref,
        )
        .expect("plan replace token update");

        assert_eq!(replaced.endpoint.address, "127.0.0.1:24000");
        assert_eq!(
            replaced.token_write,
            Some(GatewayAuthTokenWrite {
                token_ref: "remote-one".to_owned(),
                token: "new-token".to_owned(),
                label: "Remote (127.0.0.1:24000)".to_owned(),
            })
        );

        let cleared = plan_update_remote_gateway_registry_request(
            PlanUpdateRemoteGatewayRequest {
                registry,
                gateway_id: "remote-one".to_owned(),
                name: "Remote".to_owned(),
                address: "127.0.0.1:23000".to_owned(),
                auth_token_update: GatewayAuthTokenUpdate::Clear,
                default_remote_name: "Remote 1".to_owned(),
            },
            token_ref,
        )
        .expect("plan clear token update");

        assert!(cleared.endpoint.auth_token_ref.is_none());
        assert_eq!(cleared.deleted_token_ref.as_deref(), Some("remote-one"));
    }

    #[test]
    fn delete_remote_gateway_registry_request_selects_fallback_for_deleted_active() {
        let mut registry = registry();
        registry.local = None;
        registry.remotes.push(GatewayEndpoint {
            id: "remote-one".to_owned(),
            name: "One".to_owned(),
            address: "127.0.0.1:23000".to_owned(),
            kind: GatewayEndpointKind::Remote,
            auth_token_ref: Some("remote-one".to_owned()),
            workspace_id: None,
            service_name: None,
        });
        registry.remotes.push(GatewayEndpoint {
            id: "remote-two".to_owned(),
            name: "Two".to_owned(),
            address: "127.0.0.1:24000".to_owned(),
            kind: GatewayEndpointKind::Remote,
            auth_token_ref: None,
            workspace_id: None,
            service_name: None,
        });
        registry.active_gateway_id = Some("remote-one".to_owned());

        let plan = plan_delete_remote_gateway_registry_request(PlanDeleteRemoteGatewayRequest {
            registry,
            gateway_id: "remote-one".to_owned(),
            local_gateway_id: None,
            local_gateway_has_auth_token: false,
        })
        .expect("plan delete active remote");

        assert!(plan.deleted_active);
        assert_eq!(plan.endpoint.id, "remote-one");
        assert_eq!(plan.deleted_token_ref.as_deref(), Some("remote-one"));
        assert_eq!(
            plan.fallback_endpoint
                .as_ref()
                .map(|endpoint| endpoint.id.as_str()),
            Some("remote-two")
        );
        assert_eq!(
            plan.registry.active_gateway_id.as_deref(),
            Some("remote-two")
        );
        assert_eq!(plan.registry.remotes.len(), 1);
        assert_eq!(plan.registry.remotes[0].id, "remote-two");
    }

    #[test]
    fn remote_gateway_validation_request_rejects_zero_timeout() {
        let error = validate_remote_gateway_request(&RemoteGatewayValidationRequest {
            address: "127.0.0.1:23000".to_owned(),
            auth_token: None,
            timeout_ms: 0,
        })
        .expect_err("zero timeout should fail");

        assert_eq!(
            error,
            RemoteGatewayValidationError::InvalidTimeout { timeout_ms: 0 }
        );
    }

    #[test]
    fn validate_remote_gateway_address_reports_reachable_and_unreachable() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind reachable listener");
        let reachable_address = listener.local_addr().expect("listener address").to_string();
        let reachable =
            validate_remote_gateway_address(reachable_address.as_str(), Duration::from_millis(100))
                .expect("validate reachable");

        assert!(reachable.is_reachable());
        assert_eq!(reachable.address(), reachable_address);

        let unreachable_address = {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind temporary listener");
            listener.local_addr().expect("listener address").to_string()
        };
        let unreachable = validate_remote_gateway_address(
            unreachable_address.as_str(),
            Duration::from_millis(100),
        )
        .expect("validate unreachable");

        assert_eq!(
            unreachable,
            RemoteGatewayValidation::Unreachable {
                address: unreachable_address,
            }
        );
    }

    #[test]
    fn add_remote_gateway_change_can_activate_and_rollback_previous_active_gateway() {
        let mut registry = registry();
        registry.remotes.push(GatewayEndpoint {
            id: "remote-current".to_owned(),
            name: "Current".to_owned(),
            address: "127.0.0.1:24000".to_owned(),
            kind: GatewayEndpointKind::Remote,
            auth_token_ref: None,
            workspace_id: None,
            service_name: None,
        });
        registry.active_gateway_id = Some("remote-current".to_owned());

        let change = plan_add_remote_gateway(
            &registry,
            AddRemoteGatewayInput {
                name: "Next",
                address: "127.0.0.1:25000",
                auth_token: None,
                new_endpoint_id: "remote-next".to_owned(),
                default_remote_name: "Remote 2".to_owned(),
            },
            token_ref,
        )
        .expect("plan next remote");
        let commit = change
            .apply_to_registry(&mut registry, AddRemoteGatewayApplyMode::ActivateEndpoint)
            .expect("apply and activate");

        assert_eq!(registry.active_gateway_id.as_deref(), Some("remote-next"));
        assert_eq!(
            commit.previous_active_gateway_id.as_deref(),
            Some("remote-current")
        );

        change.rollback_commit(&mut registry, &commit);

        assert_eq!(
            registry.active_gateway_id.as_deref(),
            Some("remote-current")
        );
        assert!(
            registry
                .remotes
                .iter()
                .all(|endpoint| endpoint.id != "remote-next")
        );
    }
}
