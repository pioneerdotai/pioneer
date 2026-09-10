//! Bounded custody of accepted invitation grants and their durable commit order.
use super::invitation::*;
use crate::{ClientError, ClientResult, core::ClientCore};
use std::collections::HashMap;
pub const MAX_OUTSTANDING_INVITATION_COMMITS: usize = 16;
#[derive(Default)]
pub(crate) struct InvitationCommits {
    sequence: u64,
    fence: u64,
    values: HashMap<String, InvitationSessionCommit>,
}
impl InvitationCommits {
    pub(crate) fn invalidate(&mut self) {
        self.fence = self
            .fence
            .checked_add(1)
            .expect("invitation fence exhausted");
        self.values.clear();
    }
}
impl ClientCore {
    pub fn invitation_commit_capacity_available(&self) -> bool {
        !self.is_stopped()
            && self
                .invitation_commits
                .lock()
                .is_ok_and(|owner| owner.values.len() < MAX_OUTSTANDING_INVITATION_COMMITS)
    }
    fn retain_invitation_commit(
        &self,
        commit: InvitationSessionCommit,
        fence: u64,
    ) -> Result<String, InvitationSessionCommit> {
        let Ok(mut owner) = self.invitation_commits.lock() else {
            return Err(commit);
        };
        if self.is_stopped()
            || owner.fence != fence
            || owner.values.len() >= MAX_OUTSTANDING_INVITATION_COMMITS
        {
            return Err(commit);
        }
        owner.sequence = owner
            .sequence
            .checked_add(1)
            .expect("invitation commit identity exhausted");
        let id = format!("invitation_commit_{}", owner.sequence);
        owner.values.insert(id.clone(), commit);
        Ok(id)
    }
    fn with_invitation_commit<T>(
        &self,
        id: &str,
        operation: impl FnOnce(&mut InvitationSessionCommit) -> ClientResult<T>,
    ) -> ClientResult<T> {
        let mut owner = self
            .invitation_commits
            .lock()
            .map_err(|_| ClientError::invalid_state("invitation commit unavailable"))?;
        let commit = owner
            .values
            .get_mut(id)
            .ok_or_else(|| ClientError::invalid_state("invitation commit unavailable"))?;
        operation(commit)
    }
    pub fn invitation_commit_take_refresh(
        &self,
        id: &str,
    ) -> ClientResult<InvitationRefreshEnvelope> {
        self.with_invitation_commit(id, InvitationSessionCommit::take_refresh_for_secure_storage)
    }
    pub fn invitation_commit_secure_storage_committed(
        &self,
        id: &str,
    ) -> ClientResult<InvitationRegistryBinding> {
        self.with_invitation_commit(id, InvitationSessionCommit::secure_storage_committed)
    }
    pub fn invitation_commit_registry_committed(
        &self,
        id: &str,
    ) -> ClientResult<InvitationAccessGrant> {
        let mut owner = self
            .invitation_commits
            .lock()
            .map_err(|_| ClientError::invalid_state("invitation commit unavailable"))?;
        let grant = owner
            .values
            .get_mut(id)
            .ok_or_else(|| ClientError::invalid_state("invitation commit unavailable"))?
            .registry_committed()?;
        owner.values.remove(id);
        Ok(grant)
    }
    pub fn invitation_commit_registry_failed(&self, id: &str) -> ClientResult<()> {
        let mut owner = self
            .invitation_commits
            .lock()
            .map_err(|_| ClientError::invalid_state("invitation commit unavailable"))?;
        owner
            .values
            .get_mut(id)
            .ok_or_else(|| ClientError::invalid_state("invitation commit unavailable"))?
            .registry_failed()?;
        owner.values.remove(id);
        Ok(())
    }
    pub fn invitation_commit_take_failed_storage_cleanup(
        &self,
        id: &str,
    ) -> ClientResult<InvitationSessionCleanup> {
        let mut owner = self
            .invitation_commits
            .lock()
            .map_err(|_| ClientError::invalid_state("invitation commit unavailable"))?;
        if owner.values.get(id).is_none_or(|commit| {
            commit.state() != InvitationSessionCommitState::AwaitingSecureStorage
        }) {
            return Err(ClientError::invalid_state("invitation commit unavailable"));
        }
        owner
            .values
            .remove(id)
            .expect("commit checked under same owner lock")
            .secure_storage_failed()
    }
}

pub enum InvitationRequestError {
    Runtime,
    Exchange(crate::transport::ws::auth_exchange::InvitationExchangeError),
    InvalidGrant,
    Unavailable,
}
impl ClientCore {
    pub fn preview_invitation_request(
        &self,
        presentation: &InvitationQrPresentation,
        timeout: std::time::Duration,
    ) -> Result<pioneer_protocol::InvitationPreviewResponse, InvitationRequestError> {
        if self.is_stopped() {
            return Err(InvitationRequestError::Unavailable);
        }
        let epoch = self.authorization_connection_generation();
        let gateway = self.gateway_operation_epoch();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| InvitationRequestError::Runtime)?;
        let result = runtime
            .block_on(
                crate::transport::ws::auth_exchange::AuthExchangeClient::new(timeout)
                    .preview_invitation(presentation),
            )
            .map_err(InvitationRequestError::Exchange)?;
        if self.is_stopped()
            || epoch != self.authorization_connection_generation()
            || gateway != self.gateway_operation_epoch()
        {
            return Err(InvitationRequestError::Unavailable);
        }
        Ok(result)
    }
    pub fn accept_invitation_request(
        &self,
        presentation: &InvitationQrPresentation,
        params: pioneer_protocol::InvitationAcceptParams,
        expected_installation_id: &str,
        timeout: std::time::Duration,
    ) -> Result<(String, InvitationSessionCommitState), InvitationRequestError> {
        if !self.invitation_commit_capacity_available() {
            return Err(InvitationRequestError::Unavailable);
        }
        let fence = self
            .invitation_commits
            .lock()
            .map_err(|_| InvitationRequestError::Unavailable)?
            .fence;
        if params.installation.installation_id != expected_installation_id {
            return Err(InvitationRequestError::InvalidGrant);
        }
        let epoch = self.authorization_connection_generation();
        let gateway = self.gateway_operation_epoch();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| InvitationRequestError::Runtime)?;
        let client = crate::transport::ws::auth_exchange::AuthExchangeClient::new(timeout);
        let accepted = runtime
            .block_on(client.accept_invitation(presentation, params))
            .map_err(InvitationRequestError::Exchange)?;
        let cleanup_grant = accepted.grant.clone();
        let commit =
            match InvitationSessionCommit::new(presentation, accepted, expected_installation_id) {
                Ok(commit) => commit,
                Err(_) => {
                    let _ = runtime.block_on(client.cleanup_session_once(
                        presentation.gateway_base_url(),
                        cleanup_grant.access_token.expose_secret(),
                        cleanup_grant.session.id,
                    ));
                    return Err(InvitationRequestError::InvalidGrant);
                }
            };
        let state = commit.state();
        if self.is_stopped()
            || epoch != self.authorization_connection_generation()
            || gateway != self.gateway_operation_epoch()
        {
            cleanup_untracked(&runtime, &client, commit);
            return Err(InvitationRequestError::Unavailable);
        }
        match self.retain_invitation_commit(commit, fence) {
            Ok(id) => Ok((id, state)),
            Err(commit) => {
                cleanup_untracked(&runtime, &client, commit);
                Err(InvitationRequestError::Unavailable)
            }
        }
    }
    pub fn cleanup_failed_invitation_storage(
        &self,
        id: &str,
        timeout: std::time::Duration,
    ) -> Result<(), InvitationRequestError> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| InvitationRequestError::Runtime)?;
        let cleanup = self
            .invitation_commit_take_failed_storage_cleanup(id)
            .map_err(|_| InvitationRequestError::Unavailable)?;
        runtime.block_on(
            crate::transport::ws::auth_exchange::AuthExchangeClient::new(timeout)
                .cleanup_invitation_session_best_effort(cleanup),
        );
        Ok(())
    }
}
fn cleanup_untracked(
    runtime: &tokio::runtime::Runtime,
    client: &crate::transport::ws::auth_exchange::AuthExchangeClient,
    mut commit: InvitationSessionCommit,
) {
    let Ok(refresh) = commit.take_refresh_for_secure_storage() else {
        return;
    };
    drop(refresh);
    if let Ok(cleanup) = commit.secure_storage_failed() {
        runtime.block_on(client.cleanup_invitation_session_best_effort(cleanup));
    }
}
