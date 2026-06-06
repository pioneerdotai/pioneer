//! Active gateway profile and operation epoch state.

use super::{
    connectivity::normalize_address,
    registry::remote_index_by_address,
    secrets::normalize_gateway_auth_token,
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
    InvalidAuthTokenRef { endpoint_id: String, reason: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GatewayConnectSpecPlan {
    pub endpoint_id: String,
    pub endpoint_name: String,
    pub endpoint_kind: GatewayEndpointKind,
    pub address: String,
    pub auth_token: Option<String>,
    pub timings: GatewayWsTimings,
}

#[derive(Clone, Debug)]
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

    pub fn auth_token_ref(&self) -> Option<&str> {
        self.endpoint().auth_token_ref.as_deref()
    }
}

#[derive(Clone, Debug)]
pub struct UpdateRemoteGatewayProfilePlan {
    pub index: usize,
    pub previous_endpoint: GatewayEndpoint,
    pub endpoint: GatewayEndpoint,
}

impl UpdateRemoteGatewayProfilePlan {
    pub fn auth_token_ref(&self) -> Option<&str> {
        self.endpoint.auth_token_ref.as_deref()
    }
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
    if registry.local.id == id {
        return Some(&registry.local);
    }

    registry.remotes.iter().find(|endpoint| endpoint.id == id)
}

pub fn endpoint_by_id_mut<'a>(
    registry: &'a mut GatewayRegistry,
    id: &str,
) -> Option<&'a mut GatewayEndpoint> {
    if registry.local.id == id {
        return Some(&mut registry.local);
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

pub fn local_gateway_is_selectable(
    registry: &GatewayRegistry,
    local_gateway_id: &str,
    local_gateway_has_auth_token: bool,
) -> bool {
    registry.active_gateway_id.as_deref() == Some(local_gateway_id.trim())
        || local_gateway_has_auth_token
}

pub fn selectable_gateway_endpoints(
    registry: &GatewayRegistry,
    local_gateway_id: &str,
    local_gateway_has_auth_token: bool,
) -> Vec<GatewayEndpoint> {
    let mut endpoints = Vec::with_capacity(registry.remotes.len() + 1);
    if local_gateway_is_selectable(registry, local_gateway_id, local_gateway_has_auth_token) {
        endpoints.push(registry.local.clone());
    }
    endpoints.extend(registry.remotes.clone());
    endpoints
}

pub fn remote_delete_fallback_endpoint(
    registry: &GatewayRegistry,
    deleted_id: &str,
    local_gateway_id: &str,
    local_gateway_has_auth_token: bool,
) -> Option<GatewayEndpoint> {
    if local_gateway_is_selectable(registry, local_gateway_id, local_gateway_has_auth_token) {
        return Some(registry.local.clone());
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

pub fn plan_gateway_connect_spec(
    endpoint: &GatewayEndpoint,
    auth_token: Option<String>,
    timings: GatewayWsTimings,
) -> GatewayConnectSpecPlan {
    GatewayConnectSpecPlan {
        endpoint_id: endpoint.id.clone(),
        endpoint_name: endpoint.name.clone(),
        endpoint_kind: endpoint.kind,
        address: endpoint.address.clone(),
        auth_token,
        timings,
    }
}

pub fn plan_remote_candidate_connect_spec(
    candidate_endpoint_id: String,
    name: &str,
    default_name: String,
    address: &str,
    token: &str,
    timings: GatewayWsTimings,
) -> GatewayConnectSpecPlan {
    let endpoint_name = if name.trim().is_empty() {
        default_name
    } else {
        name.trim().to_owned()
    };

    GatewayConnectSpecPlan {
        endpoint_id: candidate_endpoint_id,
        endpoint_name,
        endpoint_kind: GatewayEndpointKind::Remote,
        address: address.trim().to_owned(),
        auth_token: normalize_gateway_auth_token(token),
        timings,
    }
}

pub fn plan_add_remote_gateway_profile<F>(
    registry: &GatewayRegistry,
    new_endpoint_id: String,
    name: &str,
    address: &str,
    has_auth_token: bool,
    mut auth_token_ref_for_endpoint: F,
    default_remote_name: String,
) -> Result<AddRemoteGatewayProfilePlan, GatewayProfileError>
where
    F: FnMut(&str) -> Result<String, GatewayProfileError>,
{
    let address = normalize_remote_gateway_address(address)?;

    if let Some(existing_index) = remote_index_by_address(registry, address.as_str(), None) {
        let previous_endpoint = registry.remotes[existing_index].clone();
        let endpoint_name = remote_gateway_name_or_default(name, previous_endpoint.name.clone());
        let mut endpoint = previous_endpoint.clone();
        endpoint.name = endpoint_name;
        endpoint.address = address;
        if has_auth_token {
            endpoint.auth_token_ref = Some(
                auth_token_ref_for_endpoint(endpoint.id.as_str())
                    .map_err(|error| map_token_ref_error(error, endpoint.id.as_str()))?,
            );
        }

        return Ok(AddRemoteGatewayProfilePlan::UpdateExisting {
            index: existing_index,
            previous_endpoint,
            endpoint,
        });
    }

    let endpoint_name = remote_gateway_name_or_default(name, default_remote_name);
    let auth_token_ref = if has_auth_token {
        Some(
            auth_token_ref_for_endpoint(new_endpoint_id.as_str())
                .map_err(|error| map_token_ref_error(error, new_endpoint_id.as_str()))?,
        )
    } else {
        None
    };
    let endpoint = GatewayEndpoint {
        id: new_endpoint_id,
        name: endpoint_name,
        address,
        kind: GatewayEndpointKind::Remote,
        auth_token_ref,
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

pub fn plan_update_remote_gateway_profile<F>(
    registry: &GatewayRegistry,
    id: &str,
    name: &str,
    address: &str,
    has_auth_token: bool,
    mut auth_token_ref_for_endpoint: F,
    default_remote_name: String,
) -> Result<UpdateRemoteGatewayProfilePlan, GatewayProfileError>
where
    F: FnMut(&str) -> Result<String, GatewayProfileError>,
{
    let address = normalize_remote_gateway_address(address)?;
    let Some(existing_index) = registry.remotes.iter().position(|remote| remote.id == id) else {
        return Err(GatewayProfileError::EndpointNotFound { id: id.to_owned() });
    };

    if remote_index_by_address(registry, address.as_str(), Some(id)).is_some() {
        return Err(GatewayProfileError::DuplicateRemoteAddress { address });
    }

    let previous_endpoint = registry.remotes[existing_index].clone();
    let endpoint_name = remote_gateway_name_or_default(name, default_remote_name);
    let auth_token_ref = if has_auth_token {
        Some(auth_token_ref_for_endpoint(id).map_err(|error| map_token_ref_error(error, id))?)
    } else {
        None
    };

    let mut endpoint = previous_endpoint.clone();
    endpoint.name = endpoint_name;
    endpoint.address = address;
    endpoint.kind = GatewayEndpointKind::Remote;
    endpoint.auth_token_ref = auth_token_ref;
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
    if registry.local.id == id {
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

fn map_token_ref_error(error: GatewayProfileError, endpoint_id: &str) -> GatewayProfileError {
    match error {
        GatewayProfileError::InvalidAuthTokenRef { .. } => error,
        other => GatewayProfileError::InvalidAuthTokenRef {
            endpoint_id: endpoint_id.to_owned(),
            reason: other.to_string(),
        },
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
            Self::InvalidAuthTokenRef {
                endpoint_id,
                reason,
            } => {
                write!(
                    f,
                    "invalid gateway auth token ref for endpoint `{endpoint_id}`: {reason}"
                )
            }
        }
    }
}

impl Error for GatewayProfileError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn registry() -> GatewayRegistry {
        GatewayRegistry {
            version: 1,
            active_gateway_id: None,
            local: GatewayEndpoint {
                id: "local".to_owned(),
                name: "Local".to_owned(),
                address: "127.0.0.1:17878".to_owned(),
                kind: GatewayEndpointKind::Local,
                auth_token_ref: None,
                workspace_id: Some("ws-local".to_owned()),
                service_name: Some("com.pioneer.gateway".to_owned()),
            },
            remotes: vec![GatewayEndpoint {
                id: "remote".to_owned(),
                name: "Remote".to_owned(),
                address: "127.0.0.1:22000".to_owned(),
                kind: GatewayEndpointKind::Remote,
                auth_token_ref: None,
                workspace_id: Some("ws-remote".to_owned()),
                service_name: None,
            }],
        }
    }

    fn token_ref(endpoint_id: &str) -> Result<String, GatewayProfileError> {
        Ok(endpoint_id.to_owned())
    }

    #[test]
    fn gateway_operation_epoch_saturates_and_fences_stale_results() {
        assert_eq!(next_gateway_operation_epoch(41), 42);
        assert_eq!(next_gateway_operation_epoch(u64::MAX), u64::MAX);
        assert!(should_apply_gateway_operation_result(7, 7));
        assert!(!should_apply_gateway_operation_result(8, 7));
    }

    #[test]
    fn local_gateway_state_classifier_handles_running_stopped_and_conflict() {
        assert_eq!(
            classify_local_gateway_state(true, true),
            ActiveGatewayState::Connected
        );
        assert_eq!(
            classify_local_gateway_state(false, true),
            ActiveGatewayState::Unreachable
        );
        assert_eq!(
            classify_local_gateway_state(false, false),
            ActiveGatewayState::Unreachable
        );
        assert_eq!(
            classify_local_gateway_state(true, false),
            ActiveGatewayState::LocalAddressConflict
        );
        assert!(normalize_local_service_active(true, false));
        assert!(normalize_local_service_active(true, true));
        assert!(normalize_local_service_active(false, true));
        assert!(!normalize_local_service_active(false, false));
    }

    #[test]
    fn gateway_profile_activation_updates_active_gateway() {
        let mut registry = registry();

        activate_gateway(&mut registry, "remote").expect("activate remote");

        assert_eq!(active_gateway_id(&registry), Some("remote"));
        assert_eq!(
            active_gateway(&registry).map(|endpoint| endpoint.id.as_str()),
            Some("remote")
        );
        assert_eq!(active_workspace_id(&registry), Some("ws-remote"));
    }

    #[test]
    fn gateway_profile_workspace_preference_is_scoped_to_endpoint() {
        let mut registry = registry();

        set_gateway_workspace_id(&mut registry, "remote", Some("ws-updated".to_owned()))
            .expect("set workspace");

        assert_eq!(
            endpoint_by_id(&registry, "remote")
                .and_then(|endpoint| endpoint.workspace_id.as_deref()),
            Some("ws-updated")
        );
        assert_eq!(
            endpoint_by_id(&registry, "local")
                .and_then(|endpoint| endpoint.workspace_id.as_deref()),
            Some("ws-local")
        );
    }

    #[test]
    fn gateway_selectable_endpoints_hide_uncreated_local_gateway() {
        let mut registry = registry();
        registry.active_gateway_id = Some("remote".to_owned());

        let endpoints = selectable_gateway_endpoints(&registry, "local", false);

        assert_eq!(endpoints.len(), 1);
        assert_eq!(endpoints[0].id, "remote");
        assert_eq!(endpoints[0].kind, GatewayEndpointKind::Remote);
    }

    #[test]
    fn gateway_selectable_endpoints_include_local_after_token_or_active_selection() {
        let mut registry = registry();
        registry.active_gateway_id = Some("remote".to_owned());

        let endpoints = selectable_gateway_endpoints(&registry, "local", true);

        assert_eq!(endpoints.len(), 2);
        assert_eq!(endpoints[0].id, "local");
        assert_eq!(endpoints[0].kind, GatewayEndpointKind::Local);
        assert_eq!(endpoints[1].id, "remote");

        registry.active_gateway_id = Some("local".to_owned());
        let endpoints = selectable_gateway_endpoints(&registry, "local", false);

        assert_eq!(endpoints.len(), 2);
        assert_eq!(endpoints[0].id, "local");
        assert_eq!(endpoints[0].kind, GatewayEndpointKind::Local);
    }

    #[test]
    fn gateway_remote_delete_fallback_prefers_selectable_local_then_other_remote() {
        let mut registry = registry();
        registry.remotes.push(GatewayEndpoint {
            id: "remote-two".to_owned(),
            name: "Remote Two".to_owned(),
            address: "127.0.0.1:24000".to_owned(),
            kind: GatewayEndpointKind::Remote,
            auth_token_ref: Some("remote-two".to_owned()),
            workspace_id: None,
            service_name: None,
        });

        let fallback = remote_delete_fallback_endpoint(&registry, "remote", "local", true);

        assert_eq!(
            fallback.as_ref().map(|endpoint| endpoint.id.as_str()),
            Some("local")
        );

        let fallback = remote_delete_fallback_endpoint(&registry, "remote", "local", false);

        assert_eq!(
            fallback.as_ref().map(|endpoint| endpoint.id.as_str()),
            Some("remote-two")
        );
    }

    #[test]
    fn gateway_activation_predicates_are_shell_neutral() {
        assert!(gateway_activation_requires_local_start(Some(
            GatewayEndpointKind::Local
        )));
        assert!(!gateway_activation_requires_local_start(Some(
            GatewayEndpointKind::Remote
        )));
        assert!(gateway_activation_is_noop(Some("local"), "local", true));
        assert!(!gateway_activation_is_noop(Some("local"), "local", false));
        assert!(!gateway_activation_is_noop(Some("local"), "remote", true));
    }

    #[test]
    fn gateway_setup_required_waits_for_bootstrap_and_handles_missing_runtime() {
        assert!(!gateway_setup_required(false, None));
        assert!(!gateway_setup_required(false, Some(true)));
        assert!(gateway_setup_required(true, None));
        assert!(gateway_setup_required(true, Some(true)));
        assert!(!gateway_setup_required(true, Some(false)));
    }

    #[test]
    fn gateway_connect_spec_plan_copies_endpoint_inputs() {
        let registry = registry();
        let endpoint = endpoint_by_id(&registry, "remote").expect("remote endpoint");
        let timings =
            GatewayWsTimings::from_millis(100, 200, 300, 400, 500, 0).expect("valid timings");

        let plan = plan_gateway_connect_spec(endpoint, Some("token".to_owned()), timings);

        assert_eq!(plan.endpoint_id, "remote");
        assert_eq!(plan.endpoint_name, "Remote");
        assert_eq!(plan.endpoint_kind, GatewayEndpointKind::Remote);
        assert_eq!(plan.address, "127.0.0.1:22000");
        assert_eq!(plan.auth_token.as_deref(), Some("token"));
        assert_eq!(plan.timings, timings);
    }

    #[test]
    fn gateway_remote_candidate_plan_trims_inputs_and_token() {
        let timings =
            GatewayWsTimings::from_millis(100, 200, 300, 400, 500, 0).expect("valid timings");

        let plan = plan_remote_candidate_connect_spec(
            "candidate-123".to_owned(),
            "  ",
            "Remote Gateway 1".to_owned(),
            " 127.0.0.1:22000 ",
            " token ",
            timings,
        );

        assert_eq!(plan.endpoint_id, "candidate-123");
        assert_eq!(plan.endpoint_name, "Remote Gateway 1");
        assert_eq!(plan.endpoint_kind, GatewayEndpointKind::Remote);
        assert_eq!(plan.address, "127.0.0.1:22000");
        assert_eq!(plan.auth_token.as_deref(), Some("token"));
    }

    #[test]
    fn gateway_remote_add_plan_updates_existing_address_and_can_rollback() {
        let mut registry = registry();
        registry.remotes[0].auth_token_ref = Some("remote".to_owned());

        let plan = plan_add_remote_gateway_profile(
            &registry,
            "remote-new".to_owned(),
            "  Renamed  ",
            " 127.0.0.1:22000 ",
            false,
            token_ref,
            "Remote Gateway 2".to_owned(),
        )
        .expect("plan add existing");

        assert_eq!(plan.endpoint().id, "remote");
        assert_eq!(plan.endpoint().name, "Renamed");
        assert_eq!(plan.endpoint().auth_token_ref.as_deref(), Some("remote"));

        apply_add_remote_gateway_profile_plan(&mut registry, &plan);
        assert_eq!(registry.remotes.len(), 1);
        assert_eq!(registry.remotes[0].name, "Renamed");

        rollback_add_remote_gateway_profile_plan(&mut registry, &plan);
        assert_eq!(registry.remotes[0].name, "Remote");
        assert_eq!(
            registry.remotes[0].auth_token_ref.as_deref(),
            Some("remote")
        );
    }

    #[test]
    fn gateway_remote_update_plan_rejects_duplicate_address_and_clears_token() {
        let mut registry = registry();
        registry.remotes[0].auth_token_ref = Some("remote".to_owned());
        registry.remotes.push(GatewayEndpoint {
            id: "remote-two".to_owned(),
            name: "Remote Two".to_owned(),
            address: "127.0.0.1:24000".to_owned(),
            kind: GatewayEndpointKind::Remote,
            auth_token_ref: None,
            workspace_id: None,
            service_name: None,
        });

        let duplicate = plan_update_remote_gateway_profile(
            &registry,
            "remote",
            "Remote",
            "127.0.0.1:24000",
            true,
            token_ref,
            "Remote Gateway 1".to_owned(),
        )
        .expect_err("duplicate address should fail");
        assert!(matches!(
            duplicate,
            GatewayProfileError::DuplicateRemoteAddress { .. }
        ));

        let plan = plan_update_remote_gateway_profile(
            &registry,
            "remote",
            "  ",
            "127.0.0.1:23000",
            false,
            token_ref,
            "Remote Gateway 1".to_owned(),
        )
        .expect("plan update");
        assert_eq!(plan.endpoint.name, "Remote Gateway 1");
        assert_eq!(plan.endpoint.address, "127.0.0.1:23000");
        assert!(plan.endpoint.auth_token_ref.is_none());
    }

    #[test]
    fn gateway_remote_delete_plan_selects_supplied_fallback_and_can_rollback() {
        let mut registry = registry();
        registry.active_gateway_id = Some("remote".to_owned());
        registry.remotes.push(GatewayEndpoint {
            id: "remote-two".to_owned(),
            name: "Remote Two".to_owned(),
            address: "127.0.0.1:24000".to_owned(),
            kind: GatewayEndpointKind::Remote,
            auth_token_ref: Some("remote-two".to_owned()),
            workspace_id: None,
            service_name: None,
        });
        let fallback = registry.remotes[1].clone();

        let plan = plan_delete_remote_gateway_profile(&registry, "remote", Some(fallback.clone()))
            .expect("plan delete");
        assert!(plan.deleted_active);
        assert_eq!(
            plan.fallback_endpoint
                .as_ref()
                .map(|endpoint| endpoint.id.as_str()),
            Some("remote-two")
        );

        apply_delete_remote_gateway_profile_plan(&mut registry, &plan);
        assert_eq!(registry.active_gateway_id.as_deref(), Some("remote-two"));
        assert_eq!(registry.remotes.len(), 1);
        assert_eq!(registry.remotes[0].id, "remote-two");

        rollback_delete_remote_gateway_profile_plan(&mut registry, &plan);
        assert_eq!(registry.active_gateway_id.as_deref(), Some("remote"));
        assert_eq!(registry.remotes.len(), 2);
        assert_eq!(registry.remotes[0].id, "remote");
    }

    #[test]
    fn gateway_remote_ws_timings_apply_minimum_connect_timeout() {
        let timings =
            GatewayWsTimings::from_millis(100, 200, 300, 400, 500, 0).expect("valid timings");
        let remote = ws_timings_for_endpoint(
            timings,
            GatewayEndpointKind::Remote,
            Duration::from_millis(1_000),
        );
        let local = ws_timings_for_endpoint(
            timings,
            GatewayEndpointKind::Local,
            Duration::from_millis(1_000),
        );

        assert_eq!(remote.connect_timeout, Duration::from_millis(1_000));
        assert_eq!(local.connect_timeout, Duration::from_millis(100));
    }
}
