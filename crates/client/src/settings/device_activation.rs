//! Pending-device creation and cleanup. Credentials have a direct-call-only accessor.
use crate::{
    core::*,
    gateway::{device_activation::DeviceActivationQrPresentation, endpoint::GatewayBaseUrl},
};
use pioneer_protocol::{AuthSessionId, AuthSessionRevokeParams, AuthSessionStatus};
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct DeviceActivationPublication {
    pub generation: u64,
    pub loading: bool,
    pub ready: bool,
    pub error: Option<String>,
}
#[derive(Default)]
pub(crate) struct DeviceActivationController {
    pub publication: DeviceActivationPublication,
    epoch: u64,
    connection: Option<u64>,
    presentation: Option<DeviceActivationQrPresentation>,
    pending: bool,
}
pub(crate) enum DeviceActivationWork {
    Create {
        generation: u64,
        epoch: u64,
        connection: Option<u64>,
        base: GatewayBaseUrl,
    },
    Cleanup {
        epoch: u64,
        connection: Option<u64>,
        session: AuthSessionId,
    },
}
impl DeviceActivationController {
    pub(crate) fn invalidate(&mut self) {
        self.publication.generation += 1;
        self.publication.loading = false;
        self.publication.ready = false;
        self.publication.error = None;
        self.presentation = None;
        self.pending = false;
    }
}
impl ClientCore {
    pub fn device_activation_publication(&self) -> DeviceActivationPublication {
        self.device_activation
            .lock()
            .expect("activation owner poisoned")
            .publication
            .clone()
    }
    pub fn device_activation_presentation(
        &self,
        generation: u64,
    ) -> Option<DeviceActivationQrPresentation> {
        let owner = self
            .device_activation
            .lock()
            .expect("activation owner poisoned");
        (owner.publication.generation == generation)
            .then(|| owner.presentation.clone())
            .flatten()
    }
    pub fn create_device_activation(&self, base: GatewayBaseUrl) -> ClientTransition {
        if self.current_auth().is_none() || self.authorization_snapshot(None, None).is_none() {
            return self.reject_intent();
        }
        let epoch = self.authorization_connection_generation();
        let identity = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        let access = self
            .transport_runtime()
            .ws_command_sender()
            .current_gateway_http_access()
            .ok();
        let connection = access.as_ref().map(|access| access.generation);
        if identity.authorization_epoch().0 != epoch
            || identity.policy_revision().is_none()
            || !identity.connection_matches(connection)
            || access
                .as_ref()
                .is_none_or(|access| access.gateway_base_url != base)
        {
            return self.reject_intent();
        }
        let mut owner = self
            .device_activation
            .lock()
            .expect("activation owner poisoned");
        if self.is_stopped() || owner.pending || owner.presentation.is_some() {
            return self.navigation_outcome(ClientTransitionOutcome::Noop);
        }
        owner.epoch = epoch;
        owner.connection = connection;
        owner.publication.generation += 1;
        owner.publication.loading = true;
        owner.publication.error = None;
        owner.pending = true;
        let work = DeviceActivationWork::Create {
            generation: owner.publication.generation,
            epoch,
            connection,
            base,
        };
        let transition =
            self.publish_settings_value(ClientScope::DeviceActivation, owner.publication.clone());
        drop(owner);
        self.queue_device_activation(work);
        transition
    }
    pub fn close_device_activation(&self, generation: u64) -> ClientTransition {
        let mut owner = self
            .device_activation
            .lock()
            .expect("activation owner poisoned");
        if owner.publication.generation != generation {
            return self.navigation_outcome(ClientTransitionOutcome::Stale);
        }
        let cleanup = owner
            .presentation
            .take()
            .map(|p| DeviceActivationWork::Cleanup {
                epoch: owner.epoch,
                connection: owner.connection,
                session: p.session_id,
            });
        owner.invalidate();
        let transition =
            self.publish_settings_value(ClientScope::DeviceActivation, owner.publication.clone());
        drop(owner);
        if let Some(work) = cleanup {
            self.queue_device_activation(work);
        }
        transition
    }
    pub(crate) fn execute_device_activation(&self, work: DeviceActivationWork) {
        match work {
            DeviceActivationWork::Cleanup {
                epoch,
                connection,
                session,
            } => {
                let identity = self
                    .identity_authorization
                    .lock()
                    .expect("identity owner poisoned");
                let allowed = !self.is_stopped()
                    && identity.authorization_epoch().0 == epoch
                    && identity.connection_matches(connection);
                drop(identity);
                if let Some(connection) = connection.filter(|_| allowed) {
                    let _ = crate::transport::ws::command_sender::auth_session_revoke(
                        &self
                            .transport_runtime()
                            .ws_command_sender()
                            .requests_for_connection(connection),
                        AuthSessionRevokeParams {
                            session_id: session,
                            expected_status: Some(AuthSessionStatus::Pending),
                        },
                    );
                }
            }
            DeviceActivationWork::Create {
                generation,
                epoch,
                connection,
                base,
            } => {
                {
                    let owner = self
                        .device_activation
                        .lock()
                        .expect("activation owner poisoned");
                    if !owner.pending
                        || owner.publication.generation != generation
                        || owner.epoch != epoch
                    {
                        return;
                    }
                }
                if self.is_stopped() || self.authorization_connection_generation() != epoch {
                    return;
                }
                let result = connection
                    .ok_or_else(|| anyhow::anyhow!("activation connection unavailable"))
                    .and_then(|connection| {
                        crate::transport::ws::command_sender::auth_device_create(
                            &self
                                .transport_runtime()
                                .ws_command_sender()
                                .requests_for_connection(connection),
                        )
                    });
                self.complete_device_activation(generation, epoch, connection, &base, result);
            }
        }
    }
}
impl ClientCore {
    pub fn create_current_device_activation(&self) -> ClientTransition {
        if self.current_auth().is_none() || self.authorization_snapshot(None, None).is_none() {
            return self.reject_intent();
        }
        let epoch = self.authorization_connection_generation();
        match self
            .transport_runtime()
            .ws_command_sender()
            .current_gateway_http_access()
        {
            Ok(access) => self.create_device_activation(access.gateway_base_url),
            Err(_) => {
                let identity = self
                    .identity_authorization
                    .lock()
                    .expect("identity owner poisoned");
                if identity.authorization_epoch().0 != epoch || identity.policy_revision().is_none()
                {
                    return self.reject_intent();
                }
                let mut owner = self
                    .device_activation
                    .lock()
                    .expect("activation owner poisoned");
                owner.publication.error = Some("gateway_not_connected".into());
                self.publish_settings_value(
                    ClientScope::DeviceActivation,
                    owner.publication.clone(),
                )
            }
        }
    }
}

impl ClientCore {
    fn complete_device_activation(
        &self,
        generation: u64,
        epoch: u64,
        connection: Option<u64>,
        base: &GatewayBaseUrl,
        result: anyhow::Result<pioneer_protocol::AuthDeviceCreateResponse>,
    ) {
        let created_session = result.as_ref().ok().map(|v| v.session_id.clone());
        let result = result
            .and_then(|created| DeviceActivationQrPresentation::from_created_device(base, created));
        let identity = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        let current_epoch = identity.authorization_epoch().0;
        let mut owner = self
            .device_activation
            .lock()
            .expect("activation owner poisoned");
        let accepted = !self.is_stopped()
            && current_epoch == epoch
            && identity.connection_matches(connection)
            && owner.connection == connection
            && owner.pending
            && owner.publication.generation == generation;
        if accepted {
            owner.pending = false;
            owner.publication.loading = false;
            match result {
                Ok(p) => {
                    owner.presentation = Some(p);
                    owner.publication.ready = true;
                }
                Err(_) => {
                    owner.publication.error = Some("device_activation_failed".into());
                }
            }
            self.publish_settings_value(ClientScope::DeviceActivation, owner.publication.clone());
        }
        let cleanup = created_session.filter(|session| {
            (!accepted || !owner.publication.ready)
                && !owner.presentation.as_ref().is_some_and(|presentation| {
                    presentation.session_id == *session
                        && owner.publication.generation == generation
                })
        });
        drop(owner);
        if let Some(session) = cleanup {
            self.queue_device_activation(DeviceActivationWork::Cleanup {
                epoch,
                connection,
                session,
            });
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn created() -> pioneer_protocol::AuthDeviceCreateResponse {
        use pioneer_protocol::*;
        AuthDeviceCreateResponse {
            device_id: DeviceId::new("D00000000000000000001").unwrap(),
            session_id: AuthSessionId::new("S00000000000000000001").unwrap(),
            activation_code: AuthSecretString::new("K7M4-P9Q2"),
            expires_at_unix: 1_800_000_000,
            gateway_id: GatewayId::new("G00000000000000000001").unwrap(),
        }
    }
    fn pending(core: &ClientCore) -> u64 {
        let mut owner = core.device_activation.lock().unwrap();
        owner.invalidate();
        owner.pending = true;
        owner.publication.loading = true;
        owner.publication.generation
    }
    #[test]
    fn activation_from_another_connection_cannot_publish_its_secret_or_complete_current_demand() {
        let core = ClientCore::new();
        let base = GatewayBaseUrl::parse_presentation("https://gateway.invalid").unwrap();
        let generation = pending(&core);
        let before = core.device_activation_publication();
        core.complete_device_activation(generation, 0, Some(9), &base, Ok(created()));
        assert_eq!(core.device_activation_publication(), before);
        assert!(core.device_activation_presentation(generation).is_none());
        assert_eq!(
            core.take_activation_cleanup_for_test(),
            Some(created().session_id)
        );
    }

    #[test]
    fn activation_completion_is_scoped_secret_free_and_duplicate_does_not_revoke_current_code() {
        let core = ClientCore::new();
        let base = GatewayBaseUrl::parse_presentation("https://gateway.invalid").unwrap();
        let generation = pending(&core);
        core.complete_device_activation(generation, 0, None, &base, Ok(created()));
        let first = core.snapshot(&ClientScope::DeviceActivation).unwrap();
        assert!(
            !serde_json::to_string(&core.device_activation_publication())
                .unwrap()
                .contains("K7M4")
        );
        assert!(core.device_activation_presentation(generation).is_some());
        core.complete_device_activation(generation, 0, None, &base, Ok(created()));
        assert_eq!(
            first.revisions(),
            core.snapshot(&ClientScope::DeviceActivation)
                .unwrap()
                .revisions()
        );
        assert!(core.take_activation_cleanup_for_test().is_none());
        core.close_device_activation(generation);
        assert!(core.device_activation_presentation(generation).is_none());
        assert_eq!(
            core.take_activation_cleanup_for_test(),
            Some(created().session_id)
        );
        let next = pending(&core);
        core.complete_device_activation(generation, 0, None, &base, Ok(created()));
        assert!(core.device_activation_publication().loading);
        assert_eq!(core.device_activation_publication().generation, next);
        assert_eq!(
            core.take_activation_cleanup_for_test(),
            Some(created().session_id)
        );
        core.complete_device_activation(
            next,
            0,
            None,
            &base,
            Err(anyhow::anyhow!("synthetic secret transport error")),
        );
        assert_eq!(
            core.device_activation_publication().error.as_deref(),
            Some("device_activation_failed")
        );
    }
}
