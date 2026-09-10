//! Invitation execution shares the onboarding registry and session handoff.
use super::{
    invitation_commits::InvitationRequestError, invitation_controller::*, invitation_persistence::*,
};
use crate::core::*;
use std::time::Duration;

pub(super) struct InvitationRecovery {
    pub owner: u64,
    pub registry: Option<InvitationRegistryRecovery>,
    pub endpoint_id: String,
}
impl ClientCore {
    pub(super) fn execute_onboarding_invitation(
        &self,
        request: InvitationRequest,
        mut environment: super::onboarding_effects::OnboardingEnvironment,
    ) {
        let authorization = self.authorization_connection_generation();
        let gateway = self.gateway_operation_epoch();
        if !self
            .onboarding
            .lock()
            .expect("onboarding owner poisoned")
            .invitation
            .accepts(request.ticket, authorization, gateway)
        {
            return;
        }
        let timeout = environment.timings.startup_timeout;
        match request.kind {
            InvitationRequestKind::Preview => {
                let result = self
                    .preview_invitation_request(&request.presentation, timeout)
                    .map_err(invitation_request_code);
                let authorization = self.authorization_connection_generation();
                let gateway = self.gateway_operation_epoch();
                let mut owner = self.onboarding.lock().expect("onboarding owner poisoned");
                owner
                    .invitation
                    .complete_preview(request.ticket, authorization, gateway, result);
                self.publish_onboarding(&owner);
            }
            InvitationRequestKind::Accept { params } => {
                let result = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|_| "invitation_unavailable".into())
                    .and_then(|runtime| {
                        runtime
                            .block_on(
                                crate::transport::ws::auth_exchange::AuthExchangeClient::new(
                                    timeout,
                                )
                                .accept_invitation(&request.presentation, params),
                            )
                            .map_err(|error| {
                                invitation_request_code(InvitationRequestError::Exchange(error))
                            })
                    });
                let cleanup = result.as_ref().ok().map(|response| response.grant.clone());
                let authorization = self.authorization_connection_generation();
                let gateway = self.gateway_operation_epoch();
                let completion = {
                    let mut owner = self.onboarding.lock().expect("onboarding owner poisoned");
                    let completion = owner.invitation.complete_accept(
                        request.ticket,
                        authorization,
                        gateway,
                        &environment.installation.installation_id,
                        result,
                    );
                    self.publish_onboarding(&owner);
                    completion
                };
                match completion {
                    InvitationAcceptCompletion::Stale(grant) => {
                        if let Some(grant) = grant {
                            cleanup_grant(&request.presentation, &grant, timeout);
                        }
                        return;
                    }
                    InvitationAcceptCompletion::InvalidGrant(grant) => {
                        cleanup_grant(&request.presentation, &grant, timeout);
                        return;
                    }
                    InvitationAcceptCompletion::Applied => {}
                }
                let (commit, name) = {
                    let mut owner = self.onboarding.lock().expect("onboarding owner poisoned");
                    let Some(commit) = owner.invitation.take_commit() else {
                        return;
                    };
                    let name = owner
                        .invitation
                        .publication()
                        .preview
                        .as_ref()
                        .and_then(|preview| preview.gateway_display_name.clone())
                        .unwrap_or_else(|| {
                            environment.remote_name(environment.registry.remotes.len() + 1)
                        });
                    (commit, name)
                };
                let storage = super::session_refresh::GatewaySessionPlatformStorage(self);
                let default_name = environment.remote_name(environment.registry.remotes.len() + 1);
                let result = commit_invitation_session(
                    &mut environment.registry,
                    &storage,
                    environment.installation.client_kind,
                    default_name,
                    |registry| self.persist_onboarding_registry(registry),
                    &request.presentation,
                    commit,
                    &name,
                );
                match result {
                    Ok(endpoint) => {
                        self.adopt_onboarding_registry(environment.registry.clone());
                        self.finish_invitation_connection(
                            request.ticket.owner_generation,
                            endpoint.id,
                            environment,
                            request.ticket.authorization_generation,
                            request.ticket.gateway_generation,
                        );
                    }
                    Err(InvitationCommitError::Registry(recovery)) => {
                        let mut owner = self.onboarding.lock().expect("onboarding owner poisoned");
                        owner.invitation_recovery = Some(InvitationRecovery {
                            owner: request.ticket.owner_generation,
                            endpoint_id: recovery.endpoint().id.clone(),
                            registry: Some(recovery),
                        });
                        owner.invitation.finish_commit(
                            request.ticket.owner_generation,
                            Err("invitation_registry_write_failed".into()),
                            true,
                        );
                        self.publish_onboarding(&owner);
                    }
                    Err(InvitationCommitError::SecureStorage(cleanup)) => {
                        let _ = super::provisioning::revoke_session_best_effort(
                            cleanup.gateway_base_url(),
                            cleanup.access_token(),
                            cleanup.session_id(),
                            timeout,
                        );
                        let mut owner = self.onboarding.lock().expect("onboarding owner poisoned");
                        owner.invitation.finish_commit(
                            request.ticket.owner_generation,
                            Err("invitation_secure_storage_failed".into()),
                            false,
                        );
                        self.publish_onboarding(&owner);
                    }
                    Err(InvitationCommitError::Invalid { .. }) => {
                        if let Some(grant) = cleanup {
                            cleanup_grant(&request.presentation, &grant, timeout);
                        }
                        let mut owner = self.onboarding.lock().expect("onboarding owner poisoned");
                        owner.invitation.finish_commit(
                            request.ticket.owner_generation,
                            Err("invalid_invitation_grant".into()),
                            false,
                        );
                        self.publish_onboarding(&owner);
                    }
                }
            }
        }
    }
    fn finish_invitation_connection(
        &self,
        generation: u64,
        endpoint_id: String,
        mut environment: super::onboarding_effects::OnboardingEnvironment,
        authorization_generation: u64,
        gateway_generation: u64,
    ) {
        let result = if self.authorization_connection_generation() == authorization_generation
            && self.gateway_operation_epoch() == gateway_generation
        {
            self.connect_onboarding_endpoint(&mut environment, &endpoint_id)
        } else {
            Err(anyhow::anyhow!("invitation handoff scope changed"))
        };
        self.adopt_onboarding_registry(environment.registry);
        let mut owner = self.onboarding.lock().expect("onboarding owner poisoned");
        let failed = result.is_err();
        if failed {
            owner.invitation_recovery = Some(InvitationRecovery {
                owner: generation,
                endpoint_id: endpoint_id.clone(),
                registry: None,
            });
        } else {
            owner.invitation_recovery = None;
        }
        owner.invitation.finish_commit(
            generation,
            result
                .map(|_| endpoint_id)
                .map_err(|_| "invitation_connection_failed".into()),
            failed,
        );
        self.publish_onboarding(&owner);
    }
    pub(super) fn retry_onboarding_invitation(
        &self,
        mut environment: super::onboarding_effects::OnboardingEnvironment,
    ) {
        let recovery = self
            .onboarding
            .lock()
            .expect("onboarding owner poisoned")
            .invitation_recovery
            .take();
        let Some(recovery) = recovery else { return };
        if let Some(registry) = &recovery.registry {
            if recover_invitation_registry(&mut environment.registry, registry, |registry| {
                self.persist_onboarding_registry(registry)
            })
            .is_err()
            {
                let mut owner = self.onboarding.lock().expect("onboarding owner poisoned");
                owner.invitation.finish_commit(
                    recovery.owner,
                    Err("invitation_registry_write_failed".into()),
                    true,
                );
                owner.invitation_recovery = Some(recovery);
                self.publish_onboarding(&owner);
                return;
            }
        }
        self.adopt_onboarding_registry(environment.registry.clone());
        self.finish_invitation_connection(
            recovery.owner,
            recovery.endpoint_id,
            environment,
            self.authorization_connection_generation(),
            self.gateway_operation_epoch(),
        );
    }
}
fn cleanup_grant(
    presentation: &super::invitation::InvitationQrPresentation,
    grant: &pioneer_protocol::AuthSessionGrant,
    timeout: Duration,
) {
    let _ = super::provisioning::revoke_session_best_effort(
        presentation.gateway_base_url(),
        grant.access_token.expose_secret(),
        &grant.session.id,
        timeout,
    );
}
fn invitation_request_code(error: InvitationRequestError) -> String {
    use crate::transport::ws::auth_exchange::InvitationExchangeErrorKind as Kind;
    match error {
        InvitationRequestError::Exchange(error) => match error.kind {
            Kind::InvalidProfile => "invalid_profile",
            Kind::NicknameUnavailable => "nickname_unavailable",
            Kind::AvatarInvalid => "avatar_invalid",
            Kind::Unavailable => "invitation_unavailable",
            _ => "invitation_request_failed",
        },
        _ => "invitation_request_failed",
    }
    .into()
}
