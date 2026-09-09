//! Process-local authority for coherent identity capability projections.

use crate::{
    authorization::{AuthorizationProjectionAcceptance, AuthorizationProjectionStore},
    core::*,
};
use pioneer_protocol::{
    AuthMeResponse, AuthSessionId, AuthSessionListItem, AuthSessionListResponse,
    AuthSessionRevokeParams, AuthSessionRevokeResponse, AuthorizationCapabilitySnapshot,
};
#[cfg(test)]
use std::sync::Arc;

/// A policy revision fences server data without ending the user's editing session.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum IdentityPublicationChange {
    Update,
    Revalidate,
    ResetSession,
}
impl IdentityPublicationChange {
    fn revision(changed: bool) -> Self {
        if changed {
            Self::Revalidate
        } else {
            Self::Update
        }
    }
    fn session(changed: bool) -> Self {
        if changed {
            Self::ResetSession
        } else {
            Self::Update
        }
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct AuthSessionsStore {
    pub sessions: Vec<AuthSessionListItem>,
    pub loading: bool,
    pub error: Option<String>,
    pub revoking: Option<AuthSessionId>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct IdentityAuthorizationPublication {
    pub endpoint_id: Option<String>,
    pub connection_id: Option<u64>,
    pub connection_generation: u64,
    pub authorization_change_sequence: u64,
    pub access_change: Option<pioneer_protocol::AccessChangedNotification>,
    pub policy_change: Option<pioneer_protocol::AuthorizationProjectionChangedNotification>,
    pub current_auth: Option<AuthMeResponse>,
    pub capabilities: AuthorizationProjectionStore,
    pub auth_sessions: AuthSessionsStore,
}

#[derive(Default)]
pub(crate) struct IdentityAuthorizationStore {
    connection_generation: u64,
    authorization_change_sequence: u64,
    access_change: Option<pioneer_protocol::AccessChangedNotification>,
    policy_change: Option<pioneer_protocol::AuthorizationProjectionChangedNotification>,
    epoch: Option<(String, u64)>,
    projections: AuthorizationProjectionStore,
    current_auth: Option<AuthMeResponse>,
    identity_request: u64,
    sessions: AuthSessionsStore,
    session_request: u64,
    session_request_connection: Option<u64>,
    pub(crate) settings: super::settings_store::GatewaySettingsStore,
    pub(crate) settings_request: u64,
    pub(crate) settings_notifications: [u64; 3],
    pub(crate) settings_request_notifications: [u64; 3],
    pub(crate) settings_request_connection: Option<u64>,
}

impl IdentityAuthorizationStore {
    pub(crate) fn stop(&mut self) {
        self.connection_generation = self
            .connection_generation
            .checked_add(1)
            .expect("authorization connection generation exhausted");
        self.epoch = None;
        self.access_change = None;
        self.policy_change = None;
        self.projections.clear_epoch();
        self.clear_sessions();
    }

    fn publication(&self) -> IdentityAuthorizationPublication {
        IdentityAuthorizationPublication {
            endpoint_id: self.epoch.as_ref().map(|(endpoint, _)| endpoint.clone()),
            connection_id: self.epoch.as_ref().map(|(_, connection)| *connection),
            connection_generation: self.connection_generation,
            authorization_change_sequence: self.authorization_change_sequence,
            access_change: self.access_change.clone(),
            policy_change: self.policy_change.clone(),
            current_auth: self.current_auth.clone(),
            capabilities: self.projections.clone(),
            auth_sessions: self.sessions.clone(),
        }
    }
    fn invalidate_policy_requests(&mut self) {
        // The verified subject remains the same. Capabilities have their own revision
        // fence; an absent projection must not look like a logout to either shell.
        let auth = self.current_auth.take();
        self.clear_sessions();
        self.current_auth = auth;
    }
    fn clear_sessions(&mut self) {
        self.current_auth = None;
        self.identity_request = self
            .identity_request
            .checked_add(1)
            .expect("identity request generation exhausted");
        self.session_request = self
            .session_request
            .checked_add(1)
            .expect("session request generation exhausted");
        self.sessions = AuthSessionsStore::default();
        self.settings_request = self
            .settings_request
            .checked_add(1)
            .expect("settings generation exhausted");
        self.settings = super::settings_store::GatewaySettingsStore::default();
    }
}

impl ClientCore {
    /// Projects accepted access invalidation onto the remaining shell-owned
    /// thread/workspace presentation. The context conveys selection and loaded
    /// keys only; it cannot grant access or advance authorization authority.
    /// Desktop uses the same `plan_access_changed` projection in its retained
    /// workspace/thread adapter until those feature owners move into Client.
    pub fn published_access_change_plan(
        &self,
        connection_generation: u64,
        change_sequence: u64,
        active_workspace_id: Option<&str>,
        active_thread_id: Option<&str>,
        known_threads: &[crate::authorization::ThreadAuthorizationScope],
    ) -> anyhow::Result<crate::authorization::AccessChangedPlan> {
        let owner = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        anyhow::ensure!(
            !self.is_stopped()
                && owner.connection_generation == connection_generation
                && owner.authorization_change_sequence == change_sequence,
            "authorization publication is stale"
        );
        let change = owner
            .access_change
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("access invalidation is unavailable"))?;
        Ok(crate::authorization::plan_access_changed(
            change,
            None,
            active_workspace_id,
            active_thread_id,
            known_threads,
        ))
    }

    pub fn authorization_connection_generation(&self) -> u64 {
        self.identity_authorization
            .lock()
            .expect("identity owner poisoned")
            .connection_generation
    }
    pub fn update_auth_profile(
        &self,
        params: pioneer_protocol::AuthProfileUpdateParams,
    ) -> anyhow::Result<pioneer_protocol::AuthProfileUpdateResponse> {
        let (generation, connection) = self.begin_identity_request()?;
        let response = self
            .compatibility_runtime()
            .ws_command_sender()
            .auth_profile_update(params)?;
        self.finish_auth_profile_update(generation, connection, response)
    }

    fn finish_auth_profile_update(
        &self,
        generation: u64,
        connection: Option<u64>,
        response: pioneer_protocol::AuthProfileUpdateResponse,
    ) -> anyhow::Result<pioneer_protocol::AuthProfileUpdateResponse> {
        let mut owner = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        anyhow::ensure!(
            !self.is_stopped()
                && owner.identity_request == generation
                && self.gateway_http_generation() == connection,
            "Gateway profile response is stale"
        );
        let auth = owner
            .current_auth
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("Gateway identity is unavailable"))?;
        anyhow::ensure!(
            auth.principal.id == response.principal.id,
            "Gateway profile principal does not match"
        );
        if auth.principal != response.principal {
            auth.principal = response.principal.clone();
            self.publish_identity_authorization(
                &owner.publication(),
                IdentityPublicationChange::Update,
            );
        }
        drop(owner);
        self.refresh_administration_member_pages();
        Ok(response)
    }

    pub fn current_auth(&self) -> Option<AuthMeResponse> {
        self.identity_authorization
            .lock()
            .expect("identity owner poisoned")
            .current_auth
            .clone()
    }

    pub fn current_auth_ticket(&self) -> (u64, Option<u64>) {
        let owner = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        (owner.identity_request, self.gateway_http_generation())
    }

    pub fn refresh_current_auth(&self) -> anyhow::Result<AuthMeResponse> {
        let (generation, connection) = self.begin_identity_request()?;
        let auth = self.compatibility_runtime().ws_command_sender().auth_me()?;
        self.finish_current_auth(generation, connection, auth)
    }

    fn begin_identity_request(&self) -> anyhow::Result<(u64, Option<u64>)> {
        let (generation, connection) = {
            let mut owner = self
                .identity_authorization
                .lock()
                .expect("identity owner poisoned");
            anyhow::ensure!(!self.is_stopped(), "Client runtime is stopped");
            owner.identity_request = owner
                .identity_request
                .checked_add(1)
                .expect("identity request generation exhausted");
            (owner.identity_request, self.gateway_http_generation())
        };
        Ok((generation, connection))
    }

    pub(crate) fn finish_current_auth(
        &self,
        generation: u64,
        connection: Option<u64>,
        auth: AuthMeResponse,
    ) -> anyhow::Result<AuthMeResponse> {
        let mut owner = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        anyhow::ensure!(
            !self.is_stopped()
                && owner.identity_request == generation
                && self.gateway_http_generation() == connection,
            "Gateway identity response is stale"
        );
        if owner.current_auth.as_ref() != Some(&auth) {
            owner.current_auth = Some(auth.clone());
            self.publish_identity_authorization(
                &owner.publication(),
                IdentityPublicationChange::Update,
            );
        }
        Ok(auth)
    }

    pub fn refresh_identity_authorization(
        &self,
        params: pioneer_protocol::AuthorizationCapabilitiesParams,
    ) -> Result<
        (AuthMeResponse, AuthorizationCapabilitySnapshot),
        (Option<AuthMeResponse>, anyhow::Error),
    > {
        let auth = self.refresh_current_auth().map_err(|error| (None, error))?;
        let (generation, connection) = {
            let owner = self
                .identity_authorization
                .lock()
                .expect("identity owner poisoned");
            (owner.identity_request, self.gateway_http_generation())
        };
        let snapshot = self
            .compatibility_runtime()
            .ws_command_sender()
            .authorization_capabilities(params.clone())
            .map_err(|error| (Some(auth.clone()), error))?;
        if !crate::authorization::authorization_capability_snapshot_is_compatible(
            &snapshot,
            &auth.principal.id,
            params.workspace_id.as_deref(),
            params.thread_id.as_deref(),
        ) {
            return Err((
                Some(auth),
                anyhow::anyhow!("Gateway returned an incompatible capability snapshot"),
            ));
        }
        let mut owner = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        if self.is_stopped()
            || owner.identity_request != generation
            || self.gateway_http_generation() != connection
            || owner.current_auth.as_ref() != Some(&auth)
        {
            return Err((None, anyhow::anyhow!("Gateway identity response is stale")));
        }
        let previous = owner.projections.accepted_revision();
        if owner.projections.accept(snapshot.clone()) != AuthorizationProjectionAcceptance::Accepted
        {
            return Err((
                Some(auth),
                anyhow::anyhow!("Gateway returned an incompatible capability snapshot"),
            ));
        }
        let changed = previous != owner.projections.accepted_revision();
        if changed {
            owner.invalidate_policy_requests();
        }
        owner.current_auth = Some(auth.clone());
        self.publish_identity_authorization(
            &owner.publication(),
            IdentityPublicationChange::revision(changed),
        );
        Ok((auth, snapshot))
    }

    pub(crate) fn invalidate_session_authorization(&self, endpoint: &str) {
        let mut owner = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        if self.is_stopped()
            || !owner
                .epoch
                .as_ref()
                .is_some_and(|(current, _)| current == endpoint)
        {
            return;
        }
        owner.epoch = None;
        owner.access_change = None;
        owner.policy_change = None;
        owner.connection_generation = owner
            .connection_generation
            .checked_add(1)
            .expect("authorization connection generation exhausted");
        owner.projections.clear_epoch();
        owner.clear_sessions();
        self.publish_identity_authorization(
            &owner.publication(),
            IdentityPublicationChange::ResetSession,
        );
    }

    pub fn begin_authorization_epoch(&self, epoch: Option<(String, u64)>) {
        let continuing_session = epoch
            .as_ref()
            .and_then(|(endpoint, _)| self.continuing_authorization_session(endpoint));
        let mut owner = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        if self.is_stopped() {
            return;
        }
        if epoch.is_some() && owner.epoch == epoch {
            return;
        }
        if let (Some((endpoint, _)), Some((previous_connection, metadata)), Some(auth)) = (
            epoch.as_ref(),
            continuing_session,
            owner.current_auth.as_ref(),
        ) && owner
            .epoch
            .as_ref()
            .is_some_and(|(previous_endpoint, connection)| {
                previous_endpoint == endpoint && *connection == previous_connection
            })
            && auth.gateway.id == metadata.gateway_id
            && auth.device.id == metadata.device_id
            && auth.session.id == metadata.session_id
            && auth.session.status == pioneer_protocol::AuthSessionStatus::Active
        {
            // Access-token rotation changes the transport, not the authorization subject.
            // Keep accepted capabilities, navigation and drafts until identity verification
            // succeeds (or the existing failure path invalidates the session). Fence older
            // auth requests; other RPCs also check the HTTP connection generation.
            owner.epoch = epoch;
            owner.identity_request = owner
                .identity_request
                .checked_add(1)
                .expect("identity request generation exhausted");
            self.publish_identity_authorization(
                &owner.publication(),
                IdentityPublicationChange::Update,
            );
            return;
        }
        owner.epoch = epoch;
        owner.access_change = None;
        owner.policy_change = None;
        owner.connection_generation = owner
            .connection_generation
            .checked_add(1)
            .expect("authorization connection generation exhausted");
        owner.projections.clear_epoch();
        owner.clear_sessions();
        self.publish_identity_authorization(
            &owner.publication(),
            IdentityPublicationChange::ResetSession,
        );
    }

    pub(crate) fn observe_authorization_connection(
        &self,
        event: &crate::transport::ws::GatewayWsEvent,
    ) {
        use crate::transport::ws::GatewayWsEvent;
        match event {
            GatewayWsEvent::Connected {
                endpoint_id,
                connection_id,
                ..
            } => {
                self.begin_authorization_epoch(Some((endpoint_id.clone(), *connection_id)));
            }
            GatewayWsEvent::Reconnecting { connection_id, .. }
            | GatewayWsEvent::Disconnected { connection_id, .. } => {
                let current = self
                    .identity_authorization
                    .lock()
                    .expect("identity owner poisoned")
                    .epoch
                    .as_ref()
                    .is_some_and(|(_, id)| id == connection_id);
                if current {
                    self.clear_authorization_projections();
                }
            }
            _ => {}
        }
    }

    pub fn clear_authorization_projections(&self) {
        let mut owner = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        if self.is_stopped() {
            return;
        }
        owner.connection_generation = owner
            .connection_generation
            .checked_add(1)
            .expect("authorization connection generation exhausted");
        owner.projections.clear_epoch();
        owner.clear_sessions();
        self.publish_identity_authorization(
            &owner.publication(),
            IdentityPublicationChange::ResetSession,
        );
    }

    pub(crate) fn observe_access_change(
        &self,
        change: &pioneer_protocol::AccessChangedNotification,
    ) {
        let mut owner = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        if self.is_stopped()
            || owner
                .projections
                .accepted_revision()
                .is_some_and(|revision| change.authorization_revision < revision)
            || owner.access_change.as_ref() == Some(change)
        {
            return;
        }
        let changed = owner
            .projections
            .accepted_revision()
            .is_none_or(|revision| change.authorization_revision > revision);
        owner
            .projections
            .invalidate_for_revision(change.authorization_revision);
        if changed {
            owner.invalidate_policy_requests();
        }
        owner.authorization_change_sequence = owner
            .authorization_change_sequence
            .checked_add(1)
            .expect("access change sequence exhausted");
        self.apply_thread_access_change(change);
        owner.access_change = Some(change.clone());
        owner.policy_change = None;
        self.publish_identity_authorization(
            &owner.publication(),
            IdentityPublicationChange::revision(changed),
        );
    }

    pub(crate) fn observe_policy_change(
        &self,
        change: &pioneer_protocol::AuthorizationProjectionChangedNotification,
    ) {
        let mut owner = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        let revision = change.policy_generation.get();
        if self.is_stopped()
            || owner
                .projections
                .accepted_revision()
                .is_some_and(|current| revision < current)
            || owner.policy_change.as_ref() == Some(change)
        {
            return;
        }
        let changed = owner
            .projections
            .accepted_revision()
            .is_none_or(|current| revision > current);
        owner.projections.invalidate_for_revision(revision);
        if changed {
            owner.invalidate_policy_requests();
        }
        owner.authorization_change_sequence = owner
            .authorization_change_sequence
            .checked_add(1)
            .expect("authorization change sequence exhausted");
        self.invalidate_threads_for_policy(change);
        owner.policy_change = Some(change.clone());
        owner.access_change = None;
        self.publish_identity_authorization(
            &owner.publication(),
            IdentityPublicationChange::revision(changed),
        );
    }

    pub fn invalidate_authorization_revision(&self, revision: u64) {
        let mut owner = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        if self.is_stopped() {
            return;
        }
        let changed = owner
            .projections
            .accepted_revision()
            .is_none_or(|current| revision > current);
        owner.projections.invalidate_for_revision(revision);
        if changed {
            owner.invalidate_policy_requests();
        }
        self.publish_identity_authorization(
            &owner.publication(),
            IdentityPublicationChange::revision(changed),
        );
    }

    pub fn authorization_revision(&self) -> Option<u64> {
        self.identity_authorization
            .lock()
            .expect("identity owner poisoned")
            .projections
            .accepted_revision()
    }

    pub fn authorization_snapshot(
        &self,
        workspace_id: Option<&str>,
        thread_id: Option<&str>,
    ) -> Option<AuthorizationCapabilitySnapshot> {
        self.identity_authorization
            .lock()
            .expect("identity owner poisoned")
            .projections
            .snapshot(workspace_id, thread_id)
    }

    pub fn accept_authorization_projection(
        &self,
        identity_generation: u64,
        connection_id: Option<u64>,
        snapshot: AuthorizationCapabilitySnapshot,
    ) -> AuthorizationProjectionAcceptance {
        let mut owner = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        if self.is_stopped()
            || owner.identity_request != identity_generation
            || self.gateway_http_generation() != connection_id
            || owner.epoch.as_ref().map(|(_, id)| *id) != connection_id
        {
            return AuthorizationProjectionAcceptance::Incompatible;
        }
        let previous_revision = owner.projections.accepted_revision();
        let accepted = owner.projections.accept(snapshot);
        if accepted == AuthorizationProjectionAcceptance::Accepted {
            let changed = previous_revision != owner.projections.accepted_revision();
            if changed {
                owner.invalidate_policy_requests();
            }
            self.publish_identity_authorization(
                &owner.publication(),
                IdentityPublicationChange::revision(changed),
            );
        }
        drop(owner);
        if accepted == AuthorizationProjectionAcceptance::Accepted { self.resume_administration_demand(); self.resume_provider_collection_demand(); self.resume_provider_runtime_demand(); }
        accepted
    }

    pub fn accept_authorization_projection_for_connection(
        &self,
        gateway_id: &str,
        connection_id: u64,
        snapshot: AuthorizationCapabilitySnapshot,
    ) -> AuthorizationProjectionAcceptance {
        let mut owner = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        if self.is_stopped()
            || !owner.epoch.as_ref().is_some_and(|(gateway, connection)| {
                gateway == gateway_id && *connection == connection_id
            })
        {
            return AuthorizationProjectionAcceptance::Incompatible;
        }
        let previous_revision = owner.projections.accepted_revision();
        let accepted = owner.projections.accept(snapshot);
        if accepted == AuthorizationProjectionAcceptance::Accepted {
            let changed = previous_revision != owner.projections.accepted_revision();
            if changed {
                owner.invalidate_policy_requests();
            }
            self.publish_identity_authorization(
                &owner.publication(),
                IdentityPublicationChange::revision(changed),
            );
        }
        drop(owner);
        if accepted == AuthorizationProjectionAcceptance::Accepted { self.resume_administration_demand(); self.resume_provider_collection_demand(); self.resume_provider_runtime_demand(); }
        accepted
    }
}

impl ClientCore {
    pub fn auth_sessions(&self) -> AuthSessionsStore {
        self.identity_authorization
            .lock()
            .expect("identity owner poisoned")
            .sessions
            .clone()
    }

    fn begin_auth_sessions_request(&self, revoking: Option<AuthSessionId>) -> anyhow::Result<u64> {
        let mut owner = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        anyhow::ensure!(!self.is_stopped(), "Client session runtime is stopped");
        anyhow::ensure!(
            owner.sessions.revoking.is_none(),
            "Session action is already pending"
        );
        owner.session_request = owner
            .session_request
            .checked_add(1)
            .expect("session request generation exhausted");
        owner.session_request_connection = self.gateway_http_generation();
        owner.sessions.loading = revoking.is_none();
        owner.sessions.revoking = revoking;
        owner.sessions.error = None;
        self.publish_identity_authorization(
            &owner.publication(),
            IdentityPublicationChange::Update,
        );
        Ok(owner.session_request)
    }

    fn finish_auth_sessions_request(
        &self,
        generation: u64,
        result: &anyhow::Result<AuthSessionListResponse>,
    ) -> bool {
        let mut owner = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        if self.is_stopped()
            || owner.session_request != generation
            || owner.session_request_connection != self.gateway_http_generation()
        {
            return false;
        }
        owner.sessions.loading = false;
        owner.sessions.revoking = None;
        match result {
            Ok(response) => {
                owner.sessions.sessions = response.sessions.clone();
                owner.sessions.error = None;
            }
            Err(error) => owner.sessions.error = Some(format!("{error:#}")),
        }
        self.publish_identity_authorization(
            &owner.publication(),
            IdentityPublicationChange::Update,
        );
        true
    }

    pub fn refresh_auth_sessions(&self) -> anyhow::Result<AuthSessionListResponse> {
        let generation = self.request_auth_sessions()?;
        self.load_auth_sessions(generation)
    }

    pub fn request_auth_sessions(&self) -> anyhow::Result<ClientGeneration> {
        self.begin_auth_sessions_request(None)
            .map(ClientGeneration::new)
    }

    pub fn load_auth_sessions(
        &self,
        generation: ClientGeneration,
    ) -> anyhow::Result<AuthSessionListResponse> {
        let generation = generation.get();
        {
            let owner = self
                .identity_authorization
                .lock()
                .expect("identity owner poisoned");
            anyhow::ensure!(
                !self.is_stopped() && owner.session_request == generation && owner.sessions.loading,
                "Session list request is no longer pending"
            );
        }
        let result = self
            .compatibility_runtime()
            .ws_command_sender()
            .auth_session_list();
        anyhow::ensure!(
            self.finish_auth_sessions_request(generation, &result),
            "Session list response belongs to a superseded authorization generation"
        );
        result
    }

    pub fn revoke_auth_session(
        &self,
        params: AuthSessionRevokeParams,
    ) -> anyhow::Result<AuthSessionRevokeResponse> {
        let session_id = params.session_id.clone();
        let generation = self.begin_auth_sessions_request(Some(session_id.clone()))?;
        let result = self
            .compatibility_runtime()
            .ws_command_sender()
            .auth_session_revoke(params);
        self.finish_auth_session_revoke(generation, &session_id, result)
    }

    fn finish_auth_session_revoke(
        &self,
        generation: u64,
        session_id: &AuthSessionId,
        result: anyhow::Result<AuthSessionRevokeResponse>,
    ) -> anyhow::Result<AuthSessionRevokeResponse> {
        let mut owner = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        anyhow::ensure!(
            !self.is_stopped()
                && owner.session_request == generation
                && owner.session_request_connection == self.gateway_http_generation(),
            "Session revoke response belongs to a superseded authorization generation"
        );
        let result = result.and_then(|response| {
            anyhow::ensure!(
                &response.session_id == session_id,
                "Session revoke response does not match its request"
            );
            Ok(response)
        });
        let clear_protected = result.as_ref().is_ok_and(|response| response.revoked)
            && (owner
                .sessions
                .sessions
                .iter()
                .any(|item| &item.session.id == session_id && item.current)
                || self
                    .compatibility_runtime()
                    .ws_command_sender()
                    .current_gateway_http_access()
                    .is_ok_and(|access| &access.session_id == session_id));
        owner.sessions.revoking = None;
        owner.sessions.loading = false;
        match &result {
            Ok(response) if response.revoked => {
                for item in &mut owner.sessions.sessions {
                    if &item.session.id == session_id {
                        item.session.status = pioneer_protocol::AuthSessionStatus::Revoked;
                    }
                }
            }
            Err(error) => owner.sessions.error = Some(format!("{error:#}")),
            _ => {}
        }
        if clear_protected {
            owner.connection_generation = owner
                .connection_generation
                .checked_add(1)
                .expect("authorization connection generation exhausted");
            owner.projections.clear_epoch();
            owner.clear_sessions();
        }
        self.publish_identity_authorization(
            &owner.publication(),
            IdentityPublicationChange::session(clear_protected),
        );
        result
    }
    pub fn logout_auth_session(&self) -> anyhow::Result<pioneer_protocol::AuthLogoutResponse> {
        let generation = self.begin_auth_sessions_request(None)?;
        let result = self
            .compatibility_runtime()
            .ws_command_sender()
            .auth_logout();
        let mut owner = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        anyhow::ensure!(
            !self.is_stopped()
                && owner.session_request == generation
                && owner.session_request_connection == self.gateway_http_generation(),
            "Logout response belongs to a superseded authorization generation"
        );
        match &result {
            Ok(_) => {
                owner.connection_generation = owner
                    .connection_generation
                    .checked_add(1)
                    .expect("authorization connection generation exhausted");
                owner.projections.clear_epoch();
                owner.clear_sessions();
            }
            Err(error) => {
                owner.sessions.loading = false;
                owner.sessions.error = Some(format!("{error:#}"));
            }
        }
        self.publish_identity_authorization(
            &owner.publication(),
            IdentityPublicationChange::session(result.is_ok()),
        );
        result
    }
}

#[cfg(test)]
mod session_tests {
    use super::*;
    fn session(current: bool) -> AuthSessionListItem {
        use pioneer_protocol::*;
        AuthSessionListItem {
            current,
            last_seen_at_unix: 100,
            device: AuthDeviceSnapshot {
                id: DeviceId::new("D00000000000000000001").unwrap(),
                installation_id: "synthetic".into(),
                display_name: "Test device".into(),
                client_kind: ClientKind::Desktop,
                status: DeviceStatus::Active,
            },
            session: AuthSessionSnapshot {
                id: AuthSessionId::new("S00000000000000000001").unwrap(),
                device_id: DeviceId::new("D00000000000000000001").unwrap(),
                token_family_id: TokenFamilyId::new("F00000000000000000001").unwrap(),
                status: AuthSessionStatus::Active,
                refresh_generation: 1,
                refresh_expires_at_unix: 1000,
            },
        }
    }
    fn auth_me() -> AuthMeResponse {
        let session = session(true);
        AuthMeResponse {
            gateway: pioneer_protocol::AuthGatewaySnapshot {
                id: pioneer_protocol::GatewayId::new("G00000000000000000001").unwrap(),
            },
            principal: pioneer_protocol::AuthPrincipalSnapshot {
                id: pioneer_protocol::PrincipalId::new("P00000000000000000001").unwrap(),
                kind: pioneer_protocol::PrincipalKind::Superuser,
                display_name: "Synthetic".into(),
                nickname: "synthetic".into(),
                avatar_revision: None,
            },
            device: session.device,
            session: session.session,
            role_key: None,
        }
    }

    #[test]
    fn policy_refresh_preserves_screen_thread_and_unsent_composer() {
        use crate::composer::{state_machine::ComposerDomainState, store::ComposerIntent};
        use crate::navigation::{AdministrationRoute, NavigationIntent, SemanticDestination};
        use pioneer_protocol::{
            AuthorizationChangeKind, AuthorizationChangeScope, PolicyGeneration,
        };

        for destination in [
            SemanticDestination::Providers {
                filter: crate::providers::selectors::ProviderFilter::Cli,
            },
            SemanticDestination::Mcp {
                server_id: Some("server".into()),
            },
            SemanticDestination::Skills { skill_id: None },
            SemanticDestination::Administration {
                route: AdministrationRoute::Members,
            },
            SemanticDestination::Administration {
                route: AdministrationRoute::Invitations,
            },
        ] {
            for affected in [
                AuthorizationChangeScope::Invitation {
                    invitation_id: pioneer_protocol::InvitationId::new("I00000000000000000001")
                        .unwrap(),
                },
                AuthorizationChangeScope::Workspace {
                    workspace_id: "workspace".into(),
                },
            ] {
                let workspace_policy =
                    matches!(affected, AuthorizationChangeScope::Workspace { .. });
                let core = ClientCore::new();
                core.finish_current_auth(0, None, auth_me()).unwrap();
                let thread = pioneer_protocol::Thread {
                    id: "thread".into(),
                    workspace_id: "workspace".into(),
                    name: None,
                    preview: String::new(),
                    preview_author: None,
                    mode: pioneer_protocol::ThreadMode::Chat,
                    model: "model".into(),
                    model_provider: "provider".into(),
                    reasoning_effort: None,
                    created_at: 1,
                    updated_at: 2,
                    status: pioneer_protocol::ThreadStatus::Idle,
                    origin_kind: pioneer_protocol::ThreadOriginKind::User,
                    sidebar_visibility: pioneer_protocol::ThreadSidebarVisibility::Visible,
                    agent_nickname: None,
                    agent_role: None,
                    visibility: None,
                    turns: Vec::new(),
                };
                core.upsert_thread(thread);
                core.activate_thread(Some("thread"), Some("workspace"));
                core.composer_intent(ComposerIntent::Open {
                    thread_id: "thread".into(),
                    defaults: ComposerDomainState::default(),
                });
                let draft = core.composer_snapshot("thread").unwrap();
                core.composer_intent(ComposerIntent::EditText {
                    thread_id: "thread".into(),
                    draft_id: draft.draft_id(),
                    text: "Unsent message".into(),
                });
                core.navigate(
                    NavigationIntent::Navigate {
                        destination: destination.clone(),
                    },
                    None,
                );
                let navigation = core.navigation_snapshot();
                let draft = core.composer_snapshot("thread").unwrap();
                core.observe_policy_change(
                    &pioneer_protocol::AuthorizationProjectionChangedNotification {
                        policy_generation: PolicyGeneration::new(7).unwrap(),
                        change: AuthorizationChangeKind::WorkspaceAcl,
                        affected,
                    },
                );
                assert_eq!(
                    core.navigation_snapshot(),
                    navigation,
                    "policy refresh changed {destination:?}"
                );
                assert_eq!(core.current_auth(), Some(auth_me()));
                assert_eq!(
                    core.composer_snapshot("thread").unwrap().draft(),
                    draft.draft()
                );
                assert!(
                    core.snapshot(&ClientScope::Composer {
                        thread_id: "thread".into()
                    })
                    .unwrap()
                    .typed::<crate::composer::store::ComposerPublication>()
                    .is_some()
                );
                assert!(
                    core.snapshot(&ClientScope::Thread {
                        thread_id: "thread".into()
                    })
                    .unwrap()
                    .snapshot()
                    .serialized_payload()
                    .is_null()
                );
                assert!(
                    core.authorization_snapshot(Some("workspace"), None)
                        .is_none()
                );
                if workspace_policy {
                    core.observe_access_change(&pioneer_protocol::AccessChangedNotification {
                        authorization_revision: 7,
                        workspace_id: "workspace".into(),
                        thread_id: None,
                        outcome: pioneer_protocol::AccessChangeOutcome::Revoked,
                        change: pioneer_protocol::AccessChangeKind::WorkspaceMembership,
                    });
                    assert!(core.active_thread_id().is_none());
                    assert!(core.navigation_snapshot().workspace_id().is_none());
                    assert!(core.composer_snapshot("thread").is_none());
                }
                core.begin_authorization_epoch(Some(("replacement".into(), 2)));
                assert_eq!(
                    *core.navigation_snapshot(),
                    crate::navigation::ClientNavigationState::default()
                );
                assert!(core.composer_snapshot("thread").is_none());
            }
        }
    }

    #[test]
    fn capability_callback_ticket_cannot_restore_a_revoked_or_replaced_epoch() {
        let core = ClientCore::new();
        let snapshot = pioneer_protocol::AuthorizationCapabilitySnapshot {
            schema_version: pioneer_protocol::AUTHORIZATION_CAPABILITY_SNAPSHOT_SCHEMA_VERSION,
            authorization_revision: 7,
            principal_id: auth_me().principal.id,
            role_key: "member".into(),
            role: pioneer_protocol::AuthorizationRolePresentation {
                key: "member".into(),
                display_name: "Synthetic".into(),
                description: "Synthetic".into(),
                built_in: false,
            },
            global: Default::default(),
            workspace: None,
            thread: None,
        };
        let ticket = core.current_auth_ticket();
        core.invalidate_authorization_revision(7);
        let scope = ClientScope::Administration { workspace_id: None };
        let before = core.snapshot(&scope).unwrap().snapshot();
        assert_eq!(
            core.accept_authorization_projection(ticket.0, ticket.1, snapshot.clone()),
            AuthorizationProjectionAcceptance::Incompatible
        );
        assert!(Arc::ptr_eq(
            &before,
            &core.snapshot(&scope).unwrap().snapshot()
        ));
        let ticket = core.current_auth_ticket();
        core.begin_authorization_epoch(Some(("replacement".into(), 9)));
        assert_eq!(
            core.accept_authorization_projection(ticket.0, ticket.1, snapshot),
            AuthorizationProjectionAcceptance::Incompatible
        );
        assert!(core.authorization_snapshot(None, None).is_none());
    }

    #[test]
    fn shutdown_releases_protected_current_values_and_is_idempotent() {
        let core = ClientCore::new();
        core.finish_current_auth(0, None, auth_me()).unwrap();
        let scope = ClientScope::Administration { workspace_id: None };
        assert!(core.snapshot(&scope).is_some());
        core.shutdown();
        let generation = core.authorization_connection_generation();
        assert!(core.snapshot(&scope).is_none());
        assert!(core.current_auth().is_none());
        core.shutdown();
        assert_eq!(core.authorization_connection_generation(), generation);
        assert!(core.finish_current_auth(0, None, auth_me()).is_err());
    }

    #[test]
    fn accepted_access_change_is_atomic_and_duplicate_or_stale_input_is_silent() {
        let core = ClientCore::new();
        let scope = ClientScope::Administration { workspace_id: None };
        core.finish_current_auth(0, None, auth_me()).unwrap();
        let change = pioneer_protocol::AccessChangedNotification {
            authorization_revision: 7,
            workspace_id: "synthetic-workspace".into(),
            thread_id: Some("synthetic-thread".into()),
            outcome: pioneer_protocol::AccessChangeOutcome::Revoked,
            change: pioneer_protocol::AccessChangeKind::ThreadParticipantRemoved,
        };
        core.observe_access_change(&change);
        let accepted = core.snapshot(&scope).unwrap();
        let value = accepted
            .typed::<IdentityAuthorizationPublication>()
            .unwrap();
        assert_eq!(value.payload().authorization_change_sequence, 1);
        let plan = core
            .published_access_change_plan(
                value.payload().connection_generation,
                1,
                Some("synthetic-workspace"),
                Some("synthetic-thread"),
                &[],
            )
            .unwrap();
        assert!(plan.clear_active_thread);
        assert!(!plan.clear_active_workspace);
        assert_eq!(plan.invalidate_thread_ids, ["synthetic-thread"]);
        assert!(
            core.published_access_change_plan(
                value.payload().connection_generation + 1,
                1,
                None,
                None,
                &[],
            )
            .is_err()
        );
        assert!(
            core.published_access_change_plan(
                value.payload().connection_generation,
                0,
                None,
                None,
                &[],
            )
            .is_err()
        );

        assert_eq!(value.payload().access_change.as_ref(), Some(&change));
        assert_eq!(value.payload().current_auth, Some(auth_me()));
        assert_eq!(core.authorization_revision(), Some(7));
        core.observe_access_change(&change);
        let mut stale = change.clone();
        stale.authorization_revision = 6;
        core.observe_access_change(&stale);
        assert!(Arc::ptr_eq(
            &accepted.snapshot(),
            &core.snapshot(&scope).unwrap().snapshot()
        ));
        let mut another_scope = change;
        another_scope.thread_id = Some("another-synthetic-thread".into());
        core.observe_access_change(&another_scope);
        assert_eq!(
            core.snapshot(&scope)
                .unwrap()
                .typed::<IdentityAuthorizationPublication>()
                .unwrap()
                .payload()
                .authorization_change_sequence,
            2
        );
        core.begin_authorization_epoch(Some(("next-endpoint".into(), 2)));
        assert!(
            core.snapshot(&scope)
                .unwrap()
                .typed::<IdentityAuthorizationPublication>()
                .unwrap()
                .payload()
                .access_change
                .is_none()
        );
    }

    #[test]
    fn observing_the_same_connection_cannot_erase_its_verified_identity() {
        let core = ClientCore::new();
        let epoch = Some(("synthetic".into(), 7));
        core.begin_authorization_epoch(epoch.clone());
        let (generation, connection) = core.current_auth_ticket();
        core.finish_current_auth(generation, connection, auth_me())
            .unwrap();
        let scope = ClientScope::Administration { workspace_id: None };
        let publication = core.snapshot(&scope).unwrap().snapshot();
        core.begin_authorization_epoch(epoch);
        assert!(Arc::ptr_eq(
            &publication,
            &core.snapshot(&scope).unwrap().snapshot()
        ));
        assert!(core.current_auth().is_some());
        core.begin_authorization_epoch(Some(("synthetic".into(), 8)));
        assert!(core.current_auth().is_none());
        assert!(
            core.finish_current_auth(generation, connection, auth_me())
                .is_err()
        );
    }

    #[test]
    fn profile_completion_updates_only_the_current_principal_and_rejects_revoked_work() {
        let core = ClientCore::new();
        let scope = ClientScope::Administration { workspace_id: None };
        let auth = auth_me();
        core.finish_current_auth(0, None, auth.clone()).unwrap();
        let mut response = pioneer_protocol::AuthProfileUpdateResponse {
            principal: auth.principal,
            changed: true,
        };
        response.principal.display_name = "Updated synthetic profile".into();
        core.finish_auth_profile_update(0, None, response.clone())
            .unwrap();
        assert_eq!(core.current_auth().unwrap().principal, response.principal);
        let publication = core.snapshot(&scope).unwrap().snapshot();
        core.finish_auth_profile_update(0, None, response.clone())
            .unwrap();
        assert!(Arc::ptr_eq(
            &publication,
            &core.snapshot(&scope).unwrap().snapshot()
        ));
        let mut wrong_principal = response.clone();
        wrong_principal.principal.id =
            pioneer_protocol::PrincipalId::new("P00000000000000000002").unwrap();
        assert!(
            core.finish_auth_profile_update(0, None, wrong_principal)
                .is_err()
        );
        assert!(
            core.finish_auth_profile_update(0, Some(99), response.clone())
                .is_err()
        );
        core.invalidate_authorization_revision(2);
        assert!(
            core.finish_auth_profile_update(0, None, response.clone())
                .is_err()
        );
        assert_eq!(core.current_auth().unwrap().principal, response.principal);
        core.clear_authorization_projections();
        assert!(core.current_auth().is_none());
        assert!(core.finish_auth_profile_update(0, None, response).is_err());
    }

    #[test]
    fn identity_completion_cannot_repopulate_a_revoked_epoch_and_equal_results_are_noops() {
        let core = ClientCore::new();
        let scope = ClientScope::Administration { workspace_id: None };
        let identity = auth_me();
        core.finish_current_auth(0, None, identity.clone()).unwrap();
        let publication = core.snapshot(&scope).unwrap().snapshot();
        core.finish_current_auth(0, None, identity.clone()).unwrap();
        assert!(Arc::ptr_eq(
            &publication,
            &core.snapshot(&scope).unwrap().snapshot()
        ));
        assert!(
            core.finish_current_auth(0, Some(99), identity.clone())
                .is_err()
        );
        core.invalidate_authorization_revision(2);
        assert_eq!(core.current_auth(), Some(identity.clone()));
        assert!(core.finish_current_auth(0, None, identity.clone()).is_err());
        assert_eq!(core.current_auth(), Some(identity.clone()));
        core.clear_authorization_projections();
        assert!(core.current_auth().is_none());
        assert!(core.finish_current_auth(1, None, identity.clone()).is_err());
        core.shutdown();
        assert!(core.finish_current_auth(1, None, identity).is_err());
    }

    #[test]
    fn current_revoke_publishes_identity_and_protected_settings_eviction_together() {
        let core = ClientCore::new();
        let current = session(true);
        let request = core.begin_auth_sessions_request(None).unwrap();
        assert!(core.finish_auth_sessions_request(
            request,
            &Ok(AuthSessionListResponse {
                sessions: vec![current.clone()]
            })
        ));
        core.request_gateway_settings().unwrap();
        let revoke = core
            .begin_auth_sessions_request(Some(current.session.id.clone()))
            .unwrap();
        assert!(core.begin_auth_sessions_request(None).is_err());
        core.finish_auth_session_revoke(
            revoke,
            &current.session.id,
            Ok(AuthSessionRevokeResponse {
                session_id: current.session.id.clone(),
                revoked: true,
            }),
        )
        .unwrap();
        assert!(core.auth_sessions().sessions.is_empty());
        assert!(!core.gateway_settings().loading);
        let identity = core
            .snapshot(&ClientScope::Administration { workspace_id: None })
            .unwrap();
        let settings = core.snapshot(&ClientScope::Settings).unwrap();
        assert_eq!(
            identity.snapshot().sequence(),
            settings.snapshot().sequence()
        );
    }
    #[test]
    fn mismatched_revoke_response_cannot_change_the_requested_session() {
        let core = ClientCore::new();
        let peer = session(false);
        let request = core.begin_auth_sessions_request(None).unwrap();
        core.finish_auth_sessions_request(
            request,
            &Ok(AuthSessionListResponse {
                sessions: vec![peer.clone()],
            }),
        );
        let revoke = core
            .begin_auth_sessions_request(Some(peer.session.id.clone()))
            .unwrap();
        assert!(
            core.finish_auth_session_revoke(
                revoke,
                &peer.session.id,
                Ok(AuthSessionRevokeResponse {
                    session_id: AuthSessionId::new("S00000000000000000002").unwrap(),
                    revoked: true
                })
            )
            .is_err()
        );
        let snapshot = core.auth_sessions();
        assert_eq!(snapshot.sessions, vec![peer]);
        assert!(snapshot.revoking.is_none());
        assert!(snapshot.error.is_some());
    }

    #[test]
    fn authorization_fence_rejects_inflight_list_without_resurrecting_loading_or_error() {
        let core = ClientCore::new();
        let request = core.begin_auth_sessions_request(None).unwrap();
        assert!(core.auth_sessions().loading);
        core.invalidate_authorization_revision(3);
        let fence = core
            .snapshot(&ClientScope::Administration { workspace_id: None })
            .unwrap();
        assert!(
            !core.finish_auth_sessions_request(request, &Err(anyhow::anyhow!("synthetic failure")))
        );
        assert_eq!(core.auth_sessions(), AuthSessionsStore::default());
        assert!(Arc::ptr_eq(
            &fence.snapshot(),
            &core
                .snapshot(&ClientScope::Administration { workspace_id: None })
                .unwrap()
                .snapshot()
        ));
    }
    #[test]
    fn newer_list_and_shutdown_reject_stale_completions() {
        let core = ClientCore::new();
        let old = core.begin_auth_sessions_request(None).unwrap();
        let current = core.begin_auth_sessions_request(None).unwrap();
        let empty = Ok(AuthSessionListResponse { sessions: vec![] });
        assert!(!core.finish_auth_sessions_request(old, &empty));
        assert!(core.auth_sessions().loading);
        assert!(core.finish_auth_sessions_request(current, &empty));
        assert!(!core.auth_sessions().loading);
        let pending = core.begin_auth_sessions_request(None).unwrap();
        core.shutdown();
        assert!(!core.finish_auth_sessions_request(pending, &empty));
        assert!(core.begin_auth_sessions_request(None).is_err());
    }
}
