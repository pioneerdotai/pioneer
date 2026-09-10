//! Gateway form and operation ownership shared by retained native screens.
use super::{
    endpoint::GatewayBaseUrl,
    runtime::GatewaySetupAction,
    types::{GatewayEndpoint, GatewayRegistry},
};
pub use pioneer_protocol::{AuthSecretString, normalize_device_activation_code_input};
use serde::{Deserialize, Serialize};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum GatewaySetupMode {
    Initial {
        allow_local: bool,
    },
    AddGateway {
        allow_local: bool,
    },
    EditGateway {
        endpoint_id: String,
    },
    ReauthenticateGateway {
        endpoint_id: String,
        close_on_success: bool,
    },
}
impl Default for GatewaySetupMode {
    fn default() -> Self {
        Self::Initial { allow_local: false }
    }
}
impl GatewaySetupMode {
    pub fn allow_local(&self) -> bool {
        matches!(
            self,
            Self::Initial { allow_local: true } | Self::AddGateway { allow_local: true }
        )
    }
    fn endpoint_id(&self) -> Option<&str> {
        match self {
            Self::EditGateway { endpoint_id } | Self::ReauthenticateGateway { endpoint_id, .. } => {
                Some(endpoint_id)
            }
            _ => None,
        }
    }
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct GatewaySetupPublication {
    pub owner_generation: u64,
    pub mode: GatewaySetupMode,
    pub name: String,
    pub address: String,
    pub activation_valid: bool,
    pub name_error: Option<String>,
    pub address_error: Option<String>,
    pub activation_error: Option<String>,
    pub error: Option<String>,
    pub action: Option<GatewaySetupAction>,
    pub pending: bool,
    pub completed_endpoint: Option<String>,
    pub input_revision: u64,
    pub input_reset_generation: u64,
    pub revision: u64,
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum GatewaySetupIntent {
    Open {
        mode: GatewaySetupMode,
    },
    Prefill {
        address: String,
        activation: AuthSecretString,
        gateway_id: Option<pioneer_protocol::GatewayId>,
    },
    EditName {
        value: String,
    },
    EditAddress {
        value: String,
    },
    EditActivation {
        value: AuthSecretString,
    },
    EditNameForOwner {
        expected_owner: u64,
        value: String,
    },
    EditAddressForOwner {
        expected_owner: u64,
        value: String,
    },
    EditActivationForOwner {
        expected_owner: u64,
        value: AuthSecretString,
    },
    DeleteForOwner {
        expected_owner: u64,
    },
    SubmitForOwner {
        expected_owner: u64,
        local: bool,
    },
    Close {
        expected_owner: u64,
    },
    SubmitRemote,
    SubmitLocal,
    Delete,
    Cancel,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GatewaySetupTicket {
    pub owner_generation: u64,
    pub operation_generation: u64,
    pub registry_revision: u64,
    pub authorization_generation: u64,
}
/// Secret input is carried only in the private execution request, never a publication.
pub struct GatewaySetupRequest {
    pub ticket: GatewaySetupTicket,
    pub mode: GatewaySetupMode,
    pub action: GatewaySetupAction,
    pub name: String,
    pub address: Option<GatewayBaseUrl>,
    pub activation: Option<AuthSecretString>,
    pub expected_gateway_id: Option<pioneer_protocol::GatewayId>,
    pub recovery_endpoint: Option<String>,
}
#[derive(Default)]
pub struct GatewaySetupController {
    publication: GatewaySetupPublication,
    activation: Option<AuthSecretString>,
    expected_gateway_id: Option<pioneer_protocol::GatewayId>,
    recovery_endpoint: Option<String>,
    operation_generation: u64,
    pending: Option<GatewaySetupTicket>,
}
impl GatewaySetupController {
    pub(crate) fn retain_provisioned_endpoint(
        &mut self,
        ticket: GatewaySetupTicket,
        endpoint_id: String,
    ) {
        if self.pending == Some(ticket) {
            self.recovery_endpoint = Some(endpoint_id);
            self.activation = None;
        }
    }
    pub fn publication(&self) -> &GatewaySetupPublication {
        &self.publication
    }
    fn publish(&mut self, before: GatewaySetupPublication) {
        if self.publication != before {
            self.publication.revision = before.revision + 1;
        }
    }
    pub fn intent(
        &mut self,
        intent: GatewaySetupIntent,
        registry: &GatewayRegistry,
        registry_revision: u64,
        authorization_generation: u64,
    ) -> Option<GatewaySetupRequest> {
        let before = self.publication.clone();
        let result = self.reduce(
            intent,
            registry,
            registry_revision,
            authorization_generation,
        );
        self.publish(before);
        result
    }
    fn reduce(
        &mut self,
        intent: GatewaySetupIntent,
        registry: &GatewayRegistry,
        registry_revision: u64,
        authorization_generation: u64,
    ) -> Option<GatewaySetupRequest> {
        match intent {
            GatewaySetupIntent::EditNameForOwner {
                expected_owner,
                value,
            } => {
                if expected_owner != self.publication.owner_generation {
                    return None;
                }
                return self.reduce(
                    GatewaySetupIntent::EditName { value },
                    registry,
                    registry_revision,
                    authorization_generation,
                );
            }
            GatewaySetupIntent::EditAddressForOwner {
                expected_owner,
                value,
            } => {
                if expected_owner != self.publication.owner_generation {
                    return None;
                }
                return self.reduce(
                    GatewaySetupIntent::EditAddress { value },
                    registry,
                    registry_revision,
                    authorization_generation,
                );
            }
            GatewaySetupIntent::EditActivationForOwner {
                expected_owner,
                value,
            } => {
                if expected_owner != self.publication.owner_generation {
                    return None;
                }
                return self.reduce(
                    GatewaySetupIntent::EditActivation { value },
                    registry,
                    registry_revision,
                    authorization_generation,
                );
            }
            GatewaySetupIntent::DeleteForOwner { expected_owner } => {
                if expected_owner != self.publication.owner_generation {
                    return None;
                }
                return self.reduce(
                    GatewaySetupIntent::Delete,
                    registry,
                    registry_revision,
                    authorization_generation,
                );
            }
            GatewaySetupIntent::Close { expected_owner } => {
                if expected_owner == self.publication.owner_generation {
                    return self.reduce(
                        GatewaySetupIntent::Cancel,
                        registry,
                        registry_revision,
                        authorization_generation,
                    );
                }
                return None;
            }
            GatewaySetupIntent::SubmitForOwner {
                expected_owner,
                local,
            } => {
                if expected_owner == self.publication.owner_generation {
                    return self.reduce(
                        if local {
                            GatewaySetupIntent::SubmitLocal
                        } else {
                            GatewaySetupIntent::SubmitRemote
                        },
                        registry,
                        registry_revision,
                        authorization_generation,
                    );
                }
                return None;
            }
            GatewaySetupIntent::Open { mode } => {
                self.pending = None;
                self.activation = None;
                self.recovery_endpoint = None;
                self.expected_gateway_id = None;
                let endpoint = mode
                    .endpoint_id()
                    .and_then(|id| super::runtime::endpoint_by_id(registry, id));
                self.publication = GatewaySetupPublication {
                    owner_generation: self.publication.owner_generation + 1,
                    name: endpoint.map(|e| e.name.clone()).unwrap_or_default(),
                    address: endpoint
                        .map(|e| e.gateway_base_url.to_string())
                        .unwrap_or_default(),
                    error: (mode.endpoint_id().is_some() && endpoint.is_none())
                        .then(|| "gateway_not_found".into()),
                    input_reset_generation: self.publication.input_reset_generation + 1,
                    revision: self.publication.revision,
                    mode,
                    ..Default::default()
                };
            }
            GatewaySetupIntent::Cancel => {
                self.pending = None;
                self.activation = None;
                self.recovery_endpoint = None;
                self.expected_gateway_id = None;
                self.publication.owner_generation += 1;
                self.publication.pending = false;
                self.publication.action = None;
                self.publication.activation_valid = false;
                self.publication.input_reset_generation += 1;
            }
            GatewaySetupIntent::Prefill {
                address,
                activation,
                gateway_id,
            } => {
                if self.publication.pending
                    || (self.publication.address == address
                        && self.activation.as_ref() == Some(&activation)
                        && self.expected_gateway_id == gateway_id)
                {
                    return None;
                }
                self.recovery_endpoint = None;
                self.publication.address = address;
                self.publication.activation_valid =
                    normalize_device_activation_code_input(activation.expose_secret())
                        .is_ok_and(|value| value.len() == 8);
                self.activation = Some(activation);
                self.expected_gateway_id = gateway_id;
                self.publication.input_revision += 1;
            }
            GatewaySetupIntent::EditName { value } => {
                if self.publication.pending
                    || matches!(
                        self.publication.mode,
                        GatewaySetupMode::ReauthenticateGateway { .. }
                    )
                    || self.publication.name == value
                {
                    return None;
                }
                self.publication.name = value;
                self.publication.name_error = None;
                self.publication.error = None;
                self.publication.input_revision += 1;
            }
            GatewaySetupIntent::EditAddress { value } => {
                if self.publication.pending
                    || matches!(
                        self.publication.mode,
                        GatewaySetupMode::ReauthenticateGateway { .. }
                    )
                    || self.publication.address == value
                {
                    return None;
                }
                self.recovery_endpoint = None;
                self.publication.address = value;
                self.publication.address_error = None;
                self.publication.error = None;
                self.publication.input_revision += 1;
            }
            GatewaySetupIntent::EditActivation { value } => {
                if self.publication.pending || self.activation.as_ref() == Some(&value) {
                    return None;
                }
                self.publication.activation_valid =
                    pioneer_protocol::normalize_device_activation_code_input(value.expose_secret())
                        .is_ok_and(|code| code.len() == 8);
                self.recovery_endpoint = None;
                self.activation = Some(value);
                self.publication.activation_error = None;
                self.publication.error = None;
                self.publication.input_revision += 1;
            }
            GatewaySetupIntent::SubmitRemote
            | GatewaySetupIntent::SubmitLocal
            | GatewaySetupIntent::Delete => {
                if self.pending.is_some() {
                    return None;
                }
                let action = match intent {
                    GatewaySetupIntent::SubmitLocal => GatewaySetupAction::StartLocal,
                    GatewaySetupIntent::Delete => GatewaySetupAction::DeleteGateway,
                    _ => {
                        if matches!(self.publication.mode, GatewaySetupMode::EditGateway { .. }) {
                            GatewaySetupAction::SaveGateway
                        } else {
                            GatewaySetupAction::ConnectRemote
                        }
                    }
                };
                if action == GatewaySetupAction::StartLocal && !self.publication.mode.allow_local()
                {
                    return None;
                }
                if action == GatewaySetupAction::DeleteGateway
                    && !matches!(self.publication.mode, GatewaySetupMode::EditGateway { .. })
                {
                    return None;
                }
                if self
                    .publication
                    .mode
                    .endpoint_id()
                    .is_some_and(|id| super::runtime::endpoint_by_id(registry, id).is_none())
                {
                    self.publication.error = Some("gateway_not_found".into());
                    return None;
                }
                let address = if matches!(
                    action,
                    GatewaySetupAction::StartLocal | GatewaySetupAction::DeleteGateway
                ) {
                    None
                } else {
                    match GatewayBaseUrl::parse_presentation(&self.publication.address) {
                        Ok(address) => Some(address),
                        Err(_) => {
                            self.publication.address_error = Some("invalid_gateway_address".into());
                            return None;
                        }
                    }
                };
                if action == GatewaySetupAction::ConnectRemote
                    && self.recovery_endpoint.is_none()
                    && !self.publication.activation_valid
                {
                    // An already provisioned endpoint can be selected without issuing another session.
                    let existing = address.as_ref().and_then(|base| {
                        registry
                            .remotes
                            .iter()
                            .find(|endpoint| &endpoint.gateway_base_url == base)
                    });
                    if matches!(
                        self.publication.mode,
                        GatewaySetupMode::ReauthenticateGateway { .. }
                    ) || !existing.is_some_and(|endpoint| endpoint.session_ref.is_some())
                    {
                        self.publication.activation_error = Some("invalid_activation_code".into());
                        return None;
                    }
                }
                self.operation_generation += 1;
                let ticket = GatewaySetupTicket {
                    owner_generation: self.publication.owner_generation,
                    operation_generation: self.operation_generation,
                    registry_revision,
                    authorization_generation,
                };
                self.pending = Some(ticket);
                self.publication.pending = true;
                self.publication.action = Some(action);
                self.publication.error = None;
                self.publication.completed_endpoint = None;
                return Some(GatewaySetupRequest {
                    ticket,
                    mode: self.publication.mode.clone(),
                    action,
                    name: self.publication.name.clone(),
                    address,
                    activation: self.activation.clone(),
                    expected_gateway_id: self.expected_gateway_id.clone(),
                    recovery_endpoint: self.recovery_endpoint.clone(),
                });
            }
        }
        None
    }
    pub fn accepts(
        &self,
        ticket: GatewaySetupTicket,
        registry_revision: u64,
        authorization_generation: u64,
    ) -> bool {
        self.pending == Some(ticket)
            && ticket.owner_generation == self.publication.owner_generation
            && ticket.registry_revision == registry_revision
            && ticket.authorization_generation == authorization_generation
    }
    pub fn complete(
        &mut self,
        ticket: GatewaySetupTicket,
        registry_revision: u64,
        authorization_generation: u64,
        result: Result<Option<&GatewayEndpoint>, String>,
    ) -> bool {
        if !self.accepts(ticket, registry_revision, authorization_generation) {
            return false;
        }
        let before = self.publication.clone();
        self.pending = None;
        self.publication.pending = false;
        match result {
            Ok(endpoint) => {
                self.publication.completed_endpoint = endpoint.map(|endpoint| endpoint.id.clone());
                self.activation = None;
                self.expected_gateway_id = None;
                self.publication.activation_valid = false;
                self.publication.input_reset_generation += 1;
                self.publication.error = None;
            }
            Err(code) => self.publication.error = Some(code),
        }
        self.publish(before);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn replaced_form_rejects_late_fields_and_delete_without_publication() {
        let mut owner = GatewaySetupController::default();
        let registry = registry();
        owner.intent(
            GatewaySetupIntent::Open {
                mode: GatewaySetupMode::Initial { allow_local: false },
            },
            &registry,
            1,
            1,
        );
        let old = owner.publication.owner_generation;
        owner.intent(
            GatewaySetupIntent::Open {
                mode: GatewaySetupMode::AddGateway { allow_local: false },
            },
            &registry,
            1,
            1,
        );
        let current = owner.publication.clone();
        for intent in [
            GatewaySetupIntent::EditNameForOwner {
                expected_owner: old,
                value: "late".into(),
            },
            GatewaySetupIntent::EditAddressForOwner {
                expected_owner: old,
                value: "https://late.invalid".into(),
            },
            GatewaySetupIntent::EditActivationForOwner {
                expected_owner: old,
                value: AuthSecretString::new("K7M4P9Q2"),
            },
            GatewaySetupIntent::DeleteForOwner {
                expected_owner: old,
            },
        ] {
            assert!(owner.intent(intent, &registry, 1, 1).is_none());
            assert_eq!(owner.publication, current);
        }
    }

    fn registry() -> GatewayRegistry {
        super::super::registry::default_registry(&super::super::registry::GatewayRegistryConfig {
            local: None,
        })
    }
    #[test]
    fn setup_single_submit_retry_and_replaced_owner_reject_late_completion() {
        let registry = registry();
        let mut owner = GatewaySetupController::default();
        owner.intent(
            GatewaySetupIntent::Open {
                mode: GatewaySetupMode::Initial { allow_local: false },
            },
            &registry,
            1,
            2,
        );
        assert!(
            owner
                .intent(GatewaySetupIntent::SubmitRemote, &registry, 1, 2)
                .is_none()
        );
        assert!(owner.publication().address_error.is_some());
        owner.intent(
            GatewaySetupIntent::EditAddress {
                value: "https://gateway.invalid".into(),
            },
            &registry,
            1,
            2,
        );
        owner.intent(
            GatewaySetupIntent::EditActivation {
                value: AuthSecretString::new("K7M4P9Q2"),
            },
            &registry,
            1,
            2,
        );
        let request = owner
            .intent(GatewaySetupIntent::SubmitRemote, &registry, 1, 2)
            .unwrap();
        let before = owner.publication().clone();
        assert!(
            owner
                .intent(GatewaySetupIntent::SubmitRemote, &registry, 1, 2)
                .is_none()
        );
        assert_eq!(owner.publication(), &before);
        assert!(!owner.complete(request.ticket, 1, 3, Ok(None)));
        assert!(owner.complete(
            request.ticket,
            1,
            2,
            Err("synthetic_storage_failure".into())
        ));
        assert!(owner.publication().activation_valid);
        let retry = owner
            .intent(GatewaySetupIntent::SubmitRemote, &registry, 1, 2)
            .unwrap();
        owner.intent(GatewaySetupIntent::Cancel, &registry, 1, 2);
        let cancelled = owner.publication().clone();
        assert!(!owner.complete(retry.ticket, 1, 2, Ok(None)));
        assert_eq!(owner.publication(), &cancelled);
        assert!(
            !serde_json::to_string(owner.publication())
                .unwrap()
                .contains("K7M4")
        );
    }
    #[test]
    fn controlled_name_and_address_echo_does_not_advance_visible_revision() {
        let mut owner = GatewaySetupController::default();
        let registry = registry();
        owner.intent(
            GatewaySetupIntent::EditName {
                value: "Named gateway".into(),
            },
            &registry,
            0,
            0,
        );
        let before = owner.publication().clone();
        owner.intent(
            GatewaySetupIntent::EditName {
                value: before.name.clone(),
            },
            &registry,
            0,
            0,
        );
        owner.intent(
            GatewaySetupIntent::EditAddress {
                value: before.address.clone(),
            },
            &registry,
            0,
            0,
        );
        assert_eq!(owner.publication(), &before);
        assert!(
            owner
                .intent(GatewaySetupIntent::SubmitLocal, &registry, 0, 0)
                .is_none()
        );
    }
    #[test]
    fn failed_connection_retry_reuses_provisioned_endpoint_without_activation_replay() {
        let registry = registry();
        let mut owner = GatewaySetupController::default();
        owner.intent(
            GatewaySetupIntent::EditAddress {
                value: "https://gateway.invalid".into(),
            },
            &registry,
            1,
            1,
        );
        owner.intent(
            GatewaySetupIntent::EditActivation {
                value: AuthSecretString::new("K7M4P9Q2"),
            },
            &registry,
            1,
            1,
        );
        let request = owner
            .intent(GatewaySetupIntent::SubmitRemote, &registry, 1, 1)
            .unwrap();
        owner.retain_provisioned_endpoint(request.ticket, "durable".into());
        owner.complete(
            request.ticket,
            1,
            1,
            Err("gateway_connection_failed".into()),
        );
        let retry = owner
            .intent(GatewaySetupIntent::SubmitRemote, &registry, 2, 2)
            .unwrap();
        assert!(retry.activation.is_none());
        assert_eq!(retry.recovery_endpoint.as_deref(), Some("durable"));
        owner.complete(retry.ticket, 2, 2, Err("gateway_connection_failed".into()));
        owner.intent(
            GatewaySetupIntent::EditAddress {
                value: "https://replacement.invalid".into(),
            },
            &registry,
            2,
            2,
        );
        owner.intent(
            GatewaySetupIntent::EditActivation {
                value: AuthSecretString::new("P9Q2K7M4"),
            },
            &registry,
            2,
            2,
        );
        assert!(
            owner
                .intent(GatewaySetupIntent::SubmitRemote, &registry, 2, 2)
                .unwrap()
                .recovery_endpoint
                .is_none()
        );
    }
}
