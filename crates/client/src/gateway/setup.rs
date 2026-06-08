//! Shared gateway setup workflows.

use super::{
    connectivity::{GatewayAddressError, is_gateway_reachable, normalize_address},
    runtime::{
        AddRemoteGatewayProfilePlan, GatewayProfileError, activate_gateway,
        apply_add_remote_gateway_profile_plan, plan_add_remote_gateway_profile,
        rollback_add_remote_gateway_profile_plan,
    },
    secrets::{gateway_auth_token_label, normalize_gateway_auth_token},
    types::{GatewayEndpoint, GatewayRegistry},
};
use pioneer_protocol::generate_id;
use std::time::Duration;

#[derive(Clone, Debug, PartialEq, Eq)]
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
    InvalidAddress(GatewayAddressError),
    ResolveFailed {
        address: String,
        source: GatewayAddressError,
    },
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gateway::types::{GatewayEndpointKind, GatewayRegistry};
    use std::net::TcpListener;

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
                workspace_id: None,
                service_name: None,
            },
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
