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
    pub owner_generation: u64,
    pub sessions: Vec<AuthSessionListItem>,
    pub loading: bool,
    pub error: Option<String>,
    pub revoking: Option<AuthSessionId>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct CapabilityReadState {
    pub workspace_id: Option<String>,
    pub thread_id: Option<String>,
    pub loading: bool,
    pub error: Option<String>,
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
    pub capability_reads: Vec<CapabilityReadState>,
    pub identity_loading: bool,
    pub identity_error: Option<String>,
    pub workspace_snapshots: std::collections::BTreeMap<String, AuthorizationCapabilitySnapshot>,
    pub thread_snapshots: std::collections::BTreeMap<String, AuthorizationCapabilitySnapshot>,
}

#[derive(Default)]
pub(crate) struct IdentityAuthorizationStore {
    connection_generation: u64,
    authorization_change_sequence: u64,
    access_change: Option<pioneer_protocol::AccessChangedNotification>,
    policy_change: Option<pioneer_protocol::AuthorizationProjectionChangedNotification>,
    epoch: Option<(String, u64)>,
    projections: AuthorizationProjectionStore,
    pending_revision: Option<u64>,
    revalidation_after: Option<std::time::Instant>,
    current_auth: Option<AuthMeResponse>,
    identity_request: u64,
    identity_read_request: u64,
    profile_write_request: u64,
    capability_reads:
        std::collections::BTreeMap<(Option<String>, Option<String>), CapabilityReadState>,
    identity_loading: bool,
    identity_error: Option<String>,
    sessions: AuthSessionsStore,
    profile: crate::settings::profile::ProfileStore,
    session_request: u64,
    policy_generation: u64,
    session_request_connection: Option<u64>,
    pub(crate) settings: super::settings_store::GatewaySettingsStore,
    pub(crate) settings_request: u64,
    pub(crate) settings_notifications: [u64; 3],
    pub(crate) settings_request_notifications: [u64; 3],
    pub(crate) settings_request_connection: Option<u64>,
    pub(crate) settings_request_workspace: Option<String>,
}

impl IdentityAuthorizationStore {
    pub(crate) fn authorization_epoch(&self) -> (u64, u64) {
        (self.connection_generation, self.policy_generation)
    }
    pub(crate) fn connection_matches(&self, connection: Option<u64>) -> bool {
        self.epoch.as_ref().map(|(_, id)| *id) == connection
    }
    pub(crate) fn permissions_generation(&self) -> Option<u64> {
        self.projections
            .accepted_revision()
            .map(|_| self.authorization_change_sequence)
    }
    pub(crate) fn stop(&mut self) {
        self.connection_generation = self
            .connection_generation
            .checked_add(1)
            .expect("authorization connection generation exhausted");
        self.epoch = None;
        self.access_change = None;
        self.policy_change = None;
        self.projections.clear_epoch();
        self.pending_revision = None;
        self.revalidation_after = None;
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
            capability_reads: self.capability_reads.values().cloned().collect(),
            identity_loading: self.identity_loading,
            identity_error: self.identity_error.clone(),
            workspace_snapshots: self.projections.workspace_snapshots(),
            thread_snapshots: self.projections.thread_snapshots(),
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
        self.policy_generation = self
            .policy_generation
            .checked_add(1)
            .expect("authorization policy generation exhausted");
        self.current_auth = None;
        self.capability_reads.clear();
        self.identity_loading = false;
        self.identity_error = None;
        self.identity_request = self
            .identity_request
            .checked_add(1)
            .expect("identity request generation exhausted");
        self.session_request = self
            .session_request
            .checked_add(1)
            .expect("session request generation exhausted");
        self.sessions = AuthSessionsStore::default();
        self.profile.invalidate();
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
        self.update_auth_profile_scoped(params, None)
    }
    fn update_auth_profile_scoped(
        &self,
        params: pioneer_protocol::AuthProfileUpdateParams,
        expected: Option<(u64, u64)>,
    ) -> anyhow::Result<pioneer_protocol::AuthProfileUpdateResponse> {
        let (generation, connection, write) = {
            let mut owner = self
                .identity_authorization
                .lock()
                .expect("identity owner poisoned");
            anyhow::ensure!(
                !self.is_stopped()
                    && expected
                        .is_none_or(|(generation, epoch)| owner.connection_generation == epoch
                            && owner.profile.accepts(generation)),
                "Profile action scope was replaced"
            );
            let connection = self.gateway_http_generation();
            anyhow::ensure!(
                owner.connection_matches(connection),
                "Profile connection scope was replaced"
            );
            owner.profile_write_request = owner
                .profile_write_request
                .checked_add(1)
                .expect("profile write sequence exhausted");
            (
                owner.identity_request,
                connection,
                owner.profile_write_request,
            )
        };
        let connection_id =
            connection.ok_or_else(|| anyhow::anyhow!("Profile connection unavailable"))?;
        let transport = self
            .transport_runtime()
            .ws_command_sender()
            .requests_for_connection(connection_id);
        let response =
            crate::transport::ws::command_sender::auth_profile_update(&transport, params)?;
        self.finish_auth_profile_write(generation, connection, Some(write), response)
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn finish_auth_profile_update(
        &self,
        generation: u64,
        connection: Option<u64>,
        response: pioneer_protocol::AuthProfileUpdateResponse,
    ) -> anyhow::Result<pioneer_protocol::AuthProfileUpdateResponse> {
        self.finish_auth_profile_write(generation, connection, None, response)
    }

    fn finish_auth_profile_write(
        &self,
        generation: u64,
        connection: Option<u64>,
        write: Option<u64>,
        response: pioneer_protocol::AuthProfileUpdateResponse,
    ) -> anyhow::Result<pioneer_protocol::AuthProfileUpdateResponse> {
        let mut owner = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        anyhow::ensure!(
            !self.is_stopped()
                && owner.identity_request == generation
                && write.is_none_or(|write| owner.profile_write_request == write)
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
            owner.profile.synchronize(Some(&response.principal));
            // An identity read started before this write completed must not
            // restore the old profile after the successful mutation.
            owner.identity_read_request = owner
                .identity_read_request
                .checked_add(1)
                .expect("identity read sequence exhausted");
            owner.identity_loading = false;
            owner.identity_error = None;
            self.publish_settings_value(ClientScope::Profile, owner.profile.publication.clone());
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
        let (generation, read, connection) = self.begin_identity_request()?;
        let result = self.transport_runtime().ws_command_sender().auth_me();
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
        anyhow::ensure!(
            owner.identity_read_request == read,
            "identity_read_superseded"
        );
        owner.identity_loading = false;
        owner.identity_error = result
            .as_ref()
            .err()
            .map(|_| "identity_request_failed".into());
        if let Ok(auth) = &result {
            self.apply_current_auth(&mut owner, auth);
        }
        self.publish_identity_authorization(
            &owner.publication(),
            IdentityPublicationChange::Update,
        );
        result
    }

    fn begin_identity_request(&self) -> anyhow::Result<(u64, u64, Option<u64>)> {
        let mut owner = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        anyhow::ensure!(!self.is_stopped(), "Client runtime is stopped");
        // A background identity read supersedes older identity reads, not the
        // thread/catalog/action requests sharing the verified subject.
        owner.identity_read_request = owner
            .identity_read_request
            .checked_add(1)
            .expect("identity read sequence exhausted");
        owner.identity_loading = true;
        owner.identity_error = None;
        self.publish_identity_authorization(
            &owner.publication(),
            IdentityPublicationChange::Update,
        );
        Ok((
            owner.identity_request,
            owner.identity_read_request,
            self.gateway_http_generation(),
        ))
    }

    fn apply_current_auth(&self, owner: &mut IdentityAuthorizationStore, auth: &AuthMeResponse) {
        if owner.current_auth.as_ref() != Some(auth) {
            owner.profile.synchronize(Some(&auth.principal));
            owner.current_auth = Some(auth.clone());
            self.publish_identity_authorization(
                &owner.publication(),
                IdentityPublicationChange::Update,
            );
            self.publish_settings_value(ClientScope::Profile, owner.profile.publication.clone());
        }
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
        self.apply_current_auth(&mut owner, &auth);
        Ok(auth)
    }

    /// Serializes capability reads and owns their bounded recovery policy. Shells
    /// observe the accepted projection and request state through one publication.
    pub fn refresh_identity_authorization(
        &self,
        params: pioneer_protocol::AuthorizationCapabilitiesParams,
    ) -> Result<
        (AuthMeResponse, AuthorizationCapabilitySnapshot),
        (Option<AuthMeResponse>, anyhow::Error),
    > {
        self.refresh_identity_authorization_with_ports(
            params,
            || self.refresh_current_auth(),
            |params| {
                self.transport_runtime()
                    .ws_command_sender()
                    .authorization_capabilities(params)
            },
        )
    }

    fn refresh_identity_authorization_with_ports(
        &self,
        params: pioneer_protocol::AuthorizationCapabilitiesParams,
        mut authenticate: impl FnMut() -> anyhow::Result<AuthMeResponse>,
        mut read: impl FnMut(
            pioneer_protocol::AuthorizationCapabilitiesParams,
        ) -> anyhow::Result<AuthorizationCapabilitySnapshot>,
    ) -> Result<
        (AuthMeResponse, AuthorizationCapabilitySnapshot),
        (Option<AuthMeResponse>, anyhow::Error),
    > {
        let epoch = {
            let owner = self
                .identity_authorization
                .lock()
                .expect("identity owner poisoned");
            (
                owner.authorization_epoch(),
                owner.authorization_change_sequence,
            )
        };
        let _gate = self
            .capability_read_gate
            .lock()
            .expect("capability read gate poisoned");
        let key = (params.workspace_id.clone(), params.thread_id.clone());
        {
            let mut owner = self
                .identity_authorization
                .lock()
                .expect("identity owner poisoned");
            if self.is_stopped()
                || epoch
                    != (
                        owner.authorization_epoch(),
                        owner.authorization_change_sequence,
                    )
            {
                return Err((None, anyhow::anyhow!("capability_request_stale")));
            }
            if !owner.capability_reads.contains_key(&key) && owner.capability_reads.len() >= 64 {
                if let Some(retired) = owner
                    .capability_reads
                    .iter()
                    .find(|(_, read)| !read.loading)
                    .map(|(key, _)| key.clone())
                {
                    owner.capability_reads.remove(&retired);
                } else {
                    return Err((None, anyhow::anyhow!("capability_request_capacity")));
                }
            }
            owner.capability_reads.insert(
                key.clone(),
                CapabilityReadState {
                    workspace_id: params.workspace_id.clone(),
                    thread_id: params.thread_id.clone(),
                    loading: true,
                    error: None,
                },
            );
            self.publish_identity_authorization(
                &owner.publication(),
                IdentityPublicationChange::Update,
            );
        }
        let result = retry_capability_read(
            || {
                self.refresh_identity_authorization_once(
                    params.clone(),
                    &mut authenticate,
                    &mut read,
                )
            },
            || {
                let owner = self
                    .identity_authorization
                    .lock()
                    .expect("identity owner poisoned");
                !self.is_stopped()
                    && (
                        owner.authorization_epoch(),
                        owner.authorization_change_sequence,
                    ) == epoch
            },
            std::thread::sleep,
        );
        let mut owner = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        if !self.is_stopped()
            && epoch
                == (
                    owner.authorization_epoch(),
                    owner.authorization_change_sequence,
                )
        {
            owner.capability_reads.insert(
                key,
                CapabilityReadState {
                    workspace_id: params.workspace_id,
                    thread_id: params.thread_id,
                    loading: false,
                    error: result
                        .as_ref()
                        .err()
                        .map(|_| "capability_request_failed".into()),
                },
            );
            self.publish_identity_authorization(
                &owner.publication(),
                IdentityPublicationChange::Update,
            );
        }
        drop(owner);
        if result.is_ok() {
            self.resume_authorized_feature_demands();
        }
        result
    }

    fn refresh_identity_authorization_once(
        &self,
        params: pioneer_protocol::AuthorizationCapabilitiesParams,
        authenticate: &mut impl FnMut() -> anyhow::Result<AuthMeResponse>,
        read: &mut impl FnMut(
            pioneer_protocol::AuthorizationCapabilitiesParams,
        ) -> anyhow::Result<AuthorizationCapabilitySnapshot>,
    ) -> Result<
        (AuthMeResponse, AuthorizationCapabilitySnapshot),
        (Option<AuthMeResponse>, anyhow::Error),
    > {
        let auth = authenticate().map_err(|error| (None, error))?;
        let (generation, connection) = {
            let owner = self
                .identity_authorization
                .lock()
                .expect("identity owner poisoned");
            (owner.identity_request, self.gateway_http_generation())
        };
        let snapshot = read(params.clone()).map_err(|error| (Some(auth.clone()), error))?;
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
        let owner = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        if self.is_stopped()
            || owner.identity_request != generation
            || self.gateway_http_generation() != connection
        {
            return Err((None, anyhow::anyhow!("Gateway identity response is stale")));
        }
        if owner.current_auth.as_ref() != Some(&auth) {
            return Err((None, anyhow::anyhow!("identity_read_superseded")));
        }
        let previous = owner.projections.clone();
        let pending = owner.pending_revision;
        drop(owner);
        let revision = snapshot.authorization_revision;
        if pending.is_some_and(|minimum| revision < minimum) {
            return Err((Some(auth), anyhow::anyhow!("capability_request_stale")));
        }
        let advancing = previous
            .accepted_revision()
            .is_some_and(|old| revision > old);
        let mut next = if advancing {
            AuthorizationProjectionStore::default()
        } else {
            previous.clone()
        };
        if next.accept(snapshot.clone()) != AuthorizationProjectionAcceptance::Accepted {
            return Err((
                Some(auth),
                anyhow::anyhow!("Gateway returned an incompatible capability snapshot"),
            ));
        }
        if advancing {
            let scopes = previous
                .workspace_snapshots()
                .into_values()
                .chain(previous.thread_snapshots().into_values());
            for old in scopes {
                let scope = pioneer_protocol::AuthorizationCapabilitiesParams {
                    workspace_id: old.workspace.map(|w| w.workspace_id),
                    thread_id: old.thread.map(|t| t.thread_id),
                };
                if scope == params {
                    continue;
                }
                let replacement =
                    read(scope.clone()).map_err(|error| (Some(auth.clone()), error))?;
                if replacement.authorization_revision != revision {
                    return Err((Some(auth), anyhow::anyhow!("capability_request_stale")));
                }
                if !crate::authorization::authorization_capability_snapshot_is_compatible(
                    &replacement,
                    &auth.principal.id,
                    scope.workspace_id.as_deref(),
                    scope.thread_id.as_deref(),
                ) || next.accept(replacement) != AuthorizationProjectionAcceptance::Accepted
                {
                    return Err((
                        Some(auth),
                        anyhow::anyhow!("Gateway returned an incompatible capability snapshot"),
                    ));
                }
            }
        }
        let mut owner = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        if self.is_stopped()
            || owner.identity_request != generation
            || self.gateway_http_generation() != connection
            || owner.projections != previous
            || owner.current_auth.as_ref() != Some(&auth)
            || owner
                .pending_revision
                .is_some_and(|minimum| revision < minimum)
        {
            return Err((None, anyhow::anyhow!("capability_request_stale")));
        }
        let changed = previous.permissions_changed(&next);
        owner.projections = next;
        owner.pending_revision = None;
        owner.revalidation_after = None;
        if changed {
            owner.invalidate_policy_requests();
            owner.authorization_change_sequence = owner
                .authorization_change_sequence
                .checked_add(1)
                .expect("authorization change sequence exhausted");
            // The read can discover changes whose notification was missed.
            // Retire protected caches only now, after the effective comparison.
            self.invalidate_threads_for_policy(
                &pioneer_protocol::AuthorizationProjectionChangedNotification {
                    policy_generation: pioneer_protocol::PolicyGeneration::new(revision)
                        .expect("accepted authorization revision is nonzero"),
                    change: pioneer_protocol::AuthorizationChangeKind::RolePolicy,
                    affected: pioneer_protocol::AuthorizationChangeScope::Global,
                },
            );
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
        owner.pending_revision = None;
        owner.revalidation_after = None;
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
        owner.pending_revision = None;
        owner.revalidation_after = None;
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
                if current && !self.preserves_suspended_authorization(*connection_id) {
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
        owner.pending_revision = None;
        owner.revalidation_after = None;
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
        if change.outcome == pioneer_protocol::AccessChangeOutcome::Retained {
            let Some(policy_generation) =
                pioneer_protocol::PolicyGeneration::new(change.authorization_revision)
            else {
                return;
            };
            self.observe_policy_change(
                &pioneer_protocol::AuthorizationProjectionChangedNotification {
                    policy_generation,
                    change: pioneer_protocol::AuthorizationChangeKind::WorkspaceAcl,
                    affected: match &change.thread_id {
                        Some(thread_id) => pioneer_protocol::AuthorizationChangeScope::Thread {
                            workspace_id: change.workspace_id.clone(),
                            thread_id: thread_id.clone(),
                        },
                        None => pioneer_protocol::AuthorizationChangeScope::Workspace {
                            workspace_id: change.workspace_id.clone(),
                        },
                    },
                },
            );
            return;
        }
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
                .is_some_and(|old| revision <= old)
            || owner.pending_revision.is_some_and(|old| revision <= old)
        {
            return;
        }
        // A generation is a hint to re-read, not evidence that this principal's
        // permissions changed. Keep the published grants and feature owners until
        // a coherent replacement has been compared. Explicit revocations still
        // use observe_access_change and retire protected data immediately.
        owner.pending_revision = Some(revision);
        owner.revalidation_after = None;
        owner.policy_change = Some(change.clone());
    }

    pub(crate) fn observe_principal_data_change(
        &self,
        principal_id: &pioneer_protocol::PrincipalId,
    ) {
        let mut owner = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        if self.is_stopped()
            || owner
                .current_auth
                .as_ref()
                .is_none_or(|auth| &auth.principal.id != principal_id)
        {
            return;
        }
        // Refresh profile data on peer devices without fabricating a role change.
        let revision = owner.projections.accepted_revision().unwrap_or(0);
        owner.pending_revision = Some(owner.pending_revision.unwrap_or(0).max(revision));
        owner.revalidation_after = None;
    }

    pub(super) fn authorization_revalidation_due(&self) -> bool {
        let mut owner = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        if owner.pending_revision.is_none()
            || owner
                .revalidation_after
                .is_some_and(|at| at > std::time::Instant::now())
        {
            return false;
        }
        owner.revalidation_after =
            Some(std::time::Instant::now() + std::time::Duration::from_secs(2));
        true
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
            owner.authorization_change_sequence = owner
                .authorization_change_sequence
                .checked_add(1)
                .expect("authorization change sequence exhausted");
        }
        self.publish_identity_authorization(
            &owner.publication(),
            IdentityPublicationChange::revision(changed),
        );
    }

    /// Connection and effective-permission generation for retained consumers.
    /// Desktop bindings use this directly; mobile observes the same generation
    /// in IdentityAuthorizationPublication. Revalidation alone does not advance it.
    pub fn authorization_permissions_epoch(&self) -> Option<(u64, u64)> {
        let owner = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        owner.projections.snapshot(None, None)?;
        Some((
            owner.connection_generation,
            owner.authorization_change_sequence,
        ))
    }

    pub(crate) fn authorization_operation_epoch(&self) -> (u64, u64) {
        self.identity_authorization
            .lock()
            .expect("identity owner poisoned")
            .authorization_epoch()
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
        if owner
            .projections
            .accepted_revision()
            .is_some_and(|old| snapshot.authorization_revision > old)
            && let Some(auth) = owner.current_auth.clone()
        {
            // A thread-scoped response can discover a newer generation before
            // its notification. It must use the same coherent comparison path.
            let params = pioneer_protocol::AuthorizationCapabilitiesParams {
                workspace_id: snapshot.workspace.as_ref().map(|w| w.workspace_id.clone()),
                thread_id: snapshot.thread.as_ref().map(|t| t.thread_id.clone()),
            };
            drop(owner);
            let mut initial = Some(snapshot);
            return if self
                .refresh_identity_authorization_with_ports(
                    params,
                    || Ok(auth.clone()),
                    |params| match initial.take() {
                        Some(snapshot) => Ok(snapshot),
                        None => self
                            .transport_runtime()
                            .ws_command_sender()
                            .authorization_capabilities(params),
                    },
                )
                .is_ok()
            {
                AuthorizationProjectionAcceptance::Accepted
            } else {
                AuthorizationProjectionAcceptance::Stale
            };
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
        if accepted == AuthorizationProjectionAcceptance::Accepted {
            self.resume_authorized_feature_demands();
        }
        accepted
    }

    fn resume_authorized_feature_demands(&self) {
        self.resume_workspace_directory_demand();
        self.resume_administration_demand();
        self.resume_provider_collection_demand();
        self.resume_provider_runtime_demand();
        self.resume_mcp_demand();
        self.resume_skills_demand();
        self.resume_current_settings_demand();
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
        if accepted == AuthorizationProjectionAcceptance::Accepted {
            self.resume_authorized_feature_demands();
        }
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
        self.begin_auth_sessions_request_scoped(revoking, None)
    }
    fn begin_auth_sessions_request_scoped(
        &self,
        revoking: Option<AuthSessionId>,
        expected: Option<(&crate::settings::runtime::SettingsEpoch, Option<u64>)>,
    ) -> anyhow::Result<u64> {
        let mut owner = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        anyhow::ensure!(
            expected.is_none_or(|(epoch, owner_generation)| epoch
                .matches_owner(&owner, self.settings_workspace())
                && owner_generation.is_none_or(|generation| generation == owner.policy_generation)),
            "Session action scope was replaced"
        );
        anyhow::ensure!(!self.is_stopped(), "Client session runtime is stopped");
        anyhow::ensure!(
            owner.sessions.revoking.is_none(),
            "Session action is already pending"
        );
        let connection = self.gateway_http_generation();
        anyhow::ensure!(
            owner.connection_matches(connection),
            "Session connection scope was replaced"
        );
        owner.session_request = owner
            .session_request
            .checked_add(1)
            .expect("session request generation exhausted");
        owner.session_request_connection = connection;
        owner.sessions.owner_generation = owner.policy_generation;
        owner.sessions.loading = revoking.is_none();
        owner.sessions.revoking = revoking;
        owner.sessions.error = None;
        self.publish_auth_sessions(&owner.sessions);
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
            || (!owner.sessions.loading && owner.sessions.revoking.is_none())
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
            Err(_) => owner.sessions.error = Some("sessions_request_failed".into()),
        }
        self.publish_auth_sessions(&owner.sessions);
        true
    }

    pub fn refresh_auth_sessions(&self) -> anyhow::Result<AuthSessionListResponse> {
        let generation = self.request_auth_sessions()?;
        self.load_auth_sessions(generation)
    }

    pub(crate) fn refresh_auth_sessions_for_epoch(
        &self,
        epoch: &crate::settings::runtime::SettingsEpoch,
    ) -> anyhow::Result<AuthSessionListResponse> {
        let generation = self.begin_auth_sessions_request_scoped(None, Some((epoch, None)))?;
        self.load_auth_sessions(ClientGeneration::new(generation))
    }
    pub fn request_auth_sessions(&self) -> anyhow::Result<ClientGeneration> {
        self.begin_auth_sessions_request(None)
            .map(ClientGeneration::new)
    }

    pub fn load_auth_sessions(
        &self,
        generation: ClientGeneration,
    ) -> anyhow::Result<AuthSessionListResponse> {
        self.load_auth_sessions_with_reader(generation, |connection| {
            connection
                .ok_or_else(|| anyhow::anyhow!("Session connection unavailable"))
                .and_then(|connection| {
                    crate::transport::ws::command_sender::auth_session_list(
                        &self
                            .transport_runtime()
                            .ws_command_sender()
                            .requests_for_connection(connection),
                    )
                })
        })
    }
    pub(crate) fn load_auth_sessions_with_reader(
        &self,
        generation: ClientGeneration,
        read: impl FnOnce(Option<u64>) -> anyhow::Result<AuthSessionListResponse>,
    ) -> anyhow::Result<AuthSessionListResponse> {
        let generation = generation.get();
        let connection = {
            let owner = self
                .identity_authorization
                .lock()
                .expect("identity owner poisoned");
            anyhow::ensure!(
                !self.is_stopped() && owner.session_request == generation && owner.sessions.loading,
                "Session list request is no longer pending"
            );
            owner.session_request_connection
        };
        let result = read(connection);
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
        self.revoke_auth_session_scoped(params, None)
    }
    pub(crate) fn revoke_auth_session_for_epoch(
        &self,
        params: AuthSessionRevokeParams,
        epoch: &crate::settings::runtime::SettingsEpoch,
        owner: u64,
    ) -> anyhow::Result<AuthSessionRevokeResponse> {
        self.revoke_auth_session_scoped(params, Some((epoch, Some(owner))))
    }
    fn session_request_connection(&self, generation: u64) -> anyhow::Result<u64> {
        let owner = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        anyhow::ensure!(
            !self.is_stopped() && owner.session_request == generation,
            "Session action scope was replaced"
        );
        owner
            .session_request_connection
            .ok_or_else(|| anyhow::anyhow!("Session connection unavailable"))
    }
    fn revoke_auth_session_scoped(
        &self,
        params: AuthSessionRevokeParams,
        expected: Option<(&crate::settings::runtime::SettingsEpoch, Option<u64>)>,
    ) -> anyhow::Result<AuthSessionRevokeResponse> {
        let session_id = params.session_id.clone();
        let generation =
            self.begin_auth_sessions_request_scoped(Some(session_id.clone()), expected)?;
        let result = self
            .session_request_connection(generation)
            .and_then(|connection| {
                crate::transport::ws::command_sender::auth_session_revoke(
                    &self
                        .transport_runtime()
                        .ws_command_sender()
                        .requests_for_connection(connection),
                    params,
                )
            });
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
                    .transport_runtime()
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
            Err(_) => owner.sessions.error = Some("sessions_request_failed".into()),
            _ => {}
        }
        if clear_protected {
            owner.connection_generation = owner
                .connection_generation
                .checked_add(1)
                .expect("authorization connection generation exhausted");
            owner.projections.clear_epoch();
            owner.pending_revision = None;
            owner.revalidation_after = None;
            owner.clear_sessions();
        }
        if clear_protected {
            self.publish_identity_authorization(
                &owner.publication(),
                IdentityPublicationChange::ResetSession,
            );
        } else {
            self.publish_auth_sessions(&owner.sessions);
        }
        result
    }
    pub fn logout_auth_session(&self) -> anyhow::Result<pioneer_protocol::AuthLogoutResponse> {
        self.logout_auth_session_scoped(None)
    }
    pub(crate) fn logout_auth_session_for_epoch(
        &self,
        epoch: &crate::settings::runtime::SettingsEpoch,
        owner: u64,
    ) -> anyhow::Result<pioneer_protocol::AuthLogoutResponse> {
        self.logout_auth_session_scoped(Some((epoch, Some(owner))))
    }
    fn logout_auth_session_scoped(
        &self,
        expected: Option<(&crate::settings::runtime::SettingsEpoch, Option<u64>)>,
    ) -> anyhow::Result<pioneer_protocol::AuthLogoutResponse> {
        let generation = self.begin_auth_sessions_request_scoped(None, expected)?;
        let result = self
            .session_request_connection(generation)
            .and_then(|connection| {
                crate::transport::ws::command_sender::auth_logout(
                    &self
                        .transport_runtime()
                        .ws_command_sender()
                        .requests_for_connection(connection),
                )
            });
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
                owner.pending_revision = None;
                owner.revalidation_after = None;
                owner.clear_sessions();
            }
            Err(_) => {
                owner.sessions.loading = false;
                owner.sessions.error = Some("sessions_request_failed".into());
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
    fn stale_profile_save_cannot_allocate_a_request_for_the_replacement_principal() {
        let core = crate::catalog_test_support::settings_model_picker_client();
        let before = core.current_auth_ticket();
        let params = serde_json::from_value(
            serde_json::json!({"display_name":"Old draft","nickname":"old_draft"}),
        )
        .unwrap();
        assert!(
            core.update_auth_profile_scoped(
                params,
                Some((u64::MAX, core.authorization_connection_generation()))
            )
            .is_err()
        );
        assert_eq!(core.current_auth_ticket(), before);
    }
    #[test]
    fn profile_request_generation_does_not_invalidate_device_confirmation_owner() {
        let core = crate::catalog_test_support::settings_model_picker_client();
        let peer = session(false);
        let request = core.begin_auth_sessions_request(None).unwrap();
        core.finish_auth_sessions_request(
            request,
            &Ok(AuthSessionListResponse {
                sessions: vec![peer.clone()],
            }),
        );
        let owner = core.auth_sessions().owner_generation;
        core.begin_identity_request().unwrap();
        assert_eq!(
            core.settings_intent(crate::settings::runtime::SettingsIntent::RevokeSession {
                expected_owner: owner,
                session_id: peer.session.id,
                expected_status: Some(peer.session.status)
            })
            .outcome(),
            ClientTransitionOutcome::Changed
        );
    }
    #[test]
    fn session_confirmation_owner_changes_when_permissions_are_replaced_in_same_connection() {
        let core = crate::catalog_test_support::settings_model_picker_client();
        let peer = session(false);
        let first = core.begin_auth_sessions_request(None).unwrap();
        assert!(core.finish_auth_sessions_request(
            first,
            &Ok(AuthSessionListResponse {
                sessions: vec![peer.clone()]
            })
        ));
        let old = core.auth_sessions().owner_generation;
        let connection_generation = core.authorization_connection_generation();
        let mut capabilities = core.authorization_snapshot(None, None).unwrap();
        capabilities.authorization_revision += 1;
        core.invalidate_authorization_revision(capabilities.authorization_revision);
        let (generation, connection) = core.current_auth_ticket();
        core.accept_authorization_projection(generation, connection, capabilities);
        let next = core.begin_auth_sessions_request(None).unwrap();
        assert!(core.finish_auth_sessions_request(
            next,
            &Ok(AuthSessionListResponse {
                sessions: vec![peer.clone()]
            })
        ));
        assert_eq!(
            core.authorization_connection_generation(),
            connection_generation
        );
        assert_ne!(core.auth_sessions().owner_generation, old);
        let epoch = core.settings_epoch();
        let session_ticket = core.identity_authorization.lock().unwrap().session_request;
        assert!(
            core.revoke_auth_session_for_epoch(
                AuthSessionRevokeParams {
                    session_id: peer.session.id.clone(),
                    expected_status: Some(peer.session.status)
                },
                &epoch,
                old
            )
            .is_err()
        );
        assert_eq!(
            core.identity_authorization.lock().unwrap().session_request,
            session_ticket
        );
        assert_eq!(
            core.settings_intent(crate::settings::runtime::SettingsIntent::RevokeSession {
                expected_owner: old,
                session_id: peer.session.id,
                expected_status: Some(peer.session.status)
            })
            .outcome(),
            ClientTransitionOutcome::Rejected
        );
        assert!(core.auth_sessions().revoking.is_none());
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
                    !core
                        .snapshot(&ClientScope::Thread {
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

impl ClientCore {
    pub(crate) fn publish_auth_sessions(&self, sessions: &AuthSessionsStore) -> ClientTransition {
        self.publish_settings_value(ClientScope::AuthSessions, sessions.clone())
    }
    pub fn profile(&self) -> crate::settings::profile::ProfilePublication {
        self.identity_authorization
            .lock()
            .expect("identity owner poisoned")
            .profile
            .publication
            .clone()
    }
    pub fn profile_intent(
        &self,
        intent: crate::settings::profile::ProfileIntent,
    ) -> ClientTransition {
        let mut owner = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        if self.is_stopped() {
            return self.reject_intent();
        }
        let principal = owner.projections.snapshot(None, None).and_then(|_| {
            owner
                .current_auth
                .as_ref()
                .map(|auth| auth.principal.clone())
        });
        owner.profile.synchronize(principal.as_ref());
        let save = owner.profile.intent(intent);
        let transition =
            self.publish_settings_value(ClientScope::Profile, owner.profile.publication.clone());
        let epoch = owner.connection_generation;
        drop(owner);
        if let Some(save) = save {
            self.queue_profile_save(save, epoch);
        }
        transition
    }
    pub(crate) fn execute_profile_save(
        &self,
        save: crate::settings::profile::ProfileSave,
        epoch: u64,
    ) {
        self.execute_profile_save_with_writer(save, epoch, |params, expected| {
            self.update_auth_profile_scoped(params, Some(expected))
        });
    }
    pub(crate) fn execute_profile_save_with_writer(
        &self,
        save: crate::settings::profile::ProfileSave,
        epoch: u64,
        write: impl FnOnce(
            pioneer_protocol::AuthProfileUpdateParams,
            (u64, u64),
        ) -> anyhow::Result<pioneer_protocol::AuthProfileUpdateResponse>,
    ) {
        {
            let owner = self
                .identity_authorization
                .lock()
                .expect("identity owner poisoned");
            if self.is_stopped()
                || owner.connection_generation != epoch
                || !owner.profile.accepts(save.generation)
            {
                return;
            }
        }
        let result = write(save.params, (save.generation, epoch));
        let mut owner = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        if self.is_stopped() || owner.connection_generation != epoch {
            return;
        }
        // Server error codes, never arbitrary transport payloads, cross the profile boundary.
        let result = result
            .as_ref()
            .map(|response| &response.principal)
            .map_err(|error| {
                let message = error.to_string();
                ["nickname_unavailable", "avatar_invalid", "invalid_profile"]
                    .into_iter()
                    .find(|code| message.contains(code))
                    .unwrap_or("profile_save_failed")
                    .to_owned()
            });
        if owner.profile.complete(save.generation, result) {
            self.publish_settings_value(ClientScope::Profile, owner.profile.publication.clone());
        }
    }
}

impl ClientCore {
    pub(crate) fn record_session_cleanup_result(&self, epoch: u64, success: bool) {
        let mut owner = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        if self.is_stopped() || owner.connection_generation != epoch {
            return;
        }
        owner.sessions.error = (!success).then(|| "secure_storage_failed".into());
        self.publish_auth_sessions(&owner.sessions);
    }
}

fn capability_retry_allowed(attempt: usize, error: &anyhow::Error) -> bool {
    if attempt >= 4 {
        return false;
    }
    let code = crate::rpc::json_rpc_response_error(error).and_then(|error| error.machine_code());
    !matches!(
        code,
        Some(
            "gateway_identity_mismatch"
                | "invalid_capability_scope"
                | "invalid_credential"
                | "session_compromised"
                | "session_expired"
                | "session_revoked"
        )
    ) && !matches!(
        error.to_string().as_str(),
        "Gateway returned an incompatible capability snapshot"
            | "Gateway identity response is stale"
    )
}

fn retry_capability_read<T>(
    mut read: impl FnMut() -> Result<T, (Option<AuthMeResponse>, anyhow::Error)>,
    current: impl Fn() -> bool,
    mut wait: impl FnMut(std::time::Duration),
) -> Result<T, (Option<AuthMeResponse>, anyhow::Error)> {
    let mut result = read();
    for (attempt, delay) in [100, 250, 500, 1_000].into_iter().enumerate() {
        let Err((_, error)) = &result else { break };
        if !capability_retry_allowed(attempt, error) {
            break;
        }
        wait(std::time::Duration::from_millis(delay));
        if !current() {
            return Err((None, anyhow::anyhow!("capability_request_stale")));
        }
        result = read();
    }
    result
}

#[cfg(test)]
mod capability_retry_tests {
    use super::*;
    use std::cell::Cell;

    #[test]
    fn policy_revalidation_compares_grants_atomically_without_resetting_equal_presentations() {
        use crate::composer::{state_machine::ComposerDomainState, store::ComposerIntent};
        use pioneer_protocol::*;
        let core = crate::catalog_test_support::settings_client();
        let auth = core.current_auth().unwrap();
        let global = core.authorization_snapshot(None, None).unwrap();
        for workspace_id in ["one", "two"] {
            let resources = AuthorizationOperationalResourceProjection {
                fingerprint: "old-receipt".into(),
                ..Default::default()
            };
            let mut scoped = global.clone();
            scoped.workspace = Some(AuthorizationWorkspaceCapabilitySnapshot {
                workspace_id: workspace_id.into(),
                capabilities: Default::default(),
                operational_resources: resources.clone(),
                execution_draft_policy: AuthorizationExecutionDraftPolicyProjection {
                    fingerprint: "old-draft-receipt".into(),
                    resources,
                    permission_options: vec![],
                    can_attach_artifacts: false,
                    mcp_invocation_limits: Default::default(),
                },
            });
            let ticket = core.current_auth_ticket();
            assert_eq!(
                core.accept_authorization_projection(ticket.0, ticket.1, scoped),
                AuthorizationProjectionAcceptance::Accepted
            );
        }
        core.composer_intent(ComposerIntent::Open {
            thread_id: "draft".into(),
            defaults: ComposerDomainState::default(),
        });
        let scope = ClientScope::Composer {
            thread_id: "draft".into(),
        };
        let before = core.snapshot(&scope).unwrap().snapshot();
        let operation_epoch = core.authorization_operation_epoch();
        let before_sequence = core
            .identity_authorization
            .lock()
            .unwrap()
            .authorization_change_sequence;
        for (revision, changed) in [(2, false), (3, false), (4, true)] {
            let previous = core
                .identity_authorization
                .lock()
                .unwrap()
                .projections
                .clone();
            core.observe_policy_change(&AuthorizationProjectionChangedNotification {
                policy_generation: PolicyGeneration::new(revision).unwrap(),
                change: AuthorizationChangeKind::RoleAssignment,
                affected: AuthorizationChangeScope::Global,
            });
            assert!(Arc::ptr_eq(
                &before,
                &core.snapshot(&scope).unwrap().snapshot()
            ));
            assert_eq!(core.authorization_revision(), Some(revision - 1));
            let calls = Cell::new(0);
            core.refresh_identity_authorization_with_ports(
                AuthorizationCapabilitiesParams {
                    workspace_id: None,
                    thread_id: None,
                },
                || Ok(auth.clone()),
                |params| {
                    calls.set(calls.get() + 1);
                    // All old scopes remain visible until every response agrees.
                    assert_eq!(core.authorization_revision(), Some(revision - 1));
                    assert!(core.authorization_snapshot(Some("two"), None).is_some());
                    let mut next = previous
                        .snapshot(params.workspace_id.as_deref(), params.thread_id.as_deref())
                        .unwrap();
                    next.authorization_revision = revision;
                    if changed {
                        next.global.can_manage_capabilities = false;
                    }
                    if let Some(workspace) = next.workspace.as_mut() {
                        workspace.operational_resources.fingerprint = format!("receipt-{revision}");
                        workspace.execution_draft_policy.resources.fingerprint =
                            format!("receipt-{revision}");
                        workspace.execution_draft_policy.fingerprint = format!("draft-{revision}");
                    }
                    Ok(next)
                },
            )
            .unwrap();
            assert_eq!(calls.get(), 3);
            assert_eq!(core.authorization_revision(), Some(revision));
            assert_eq!(
                core.authorization_operation_epoch() == operation_epoch,
                !changed
            );
            assert_eq!(
                core.identity_authorization
                    .lock()
                    .unwrap()
                    .authorization_change_sequence,
                before_sequence + u64::from(changed)
            );
            assert_eq!(
                Arc::ptr_eq(&before, &core.snapshot(&scope).unwrap().snapshot()),
                !changed
            );
        }
    }

    #[test]
    fn identity_reads_do_not_retire_requests_owned_by_the_verified_subject() {
        let core = crate::catalog_test_support::settings_client();
        let ticket = core.current_auth_ticket();
        let epoch = core.authorization_operation_epoch();
        let first = core.begin_identity_request().unwrap();
        let second = core.begin_identity_request().unwrap();
        assert_ne!(
            first.1, second.1,
            "new reads supersede old identity replies"
        );
        assert_eq!(
            core.current_auth_ticket(),
            ticket,
            "background verification retired feature requests"
        );
        assert_eq!(core.authorization_operation_epoch(), epoch);
        core.clear_authorization_projections();
        assert_ne!(
            core.current_auth_ticket(),
            ticket,
            "session loss must retire feature requests"
        );
    }

    #[test]
    fn failed_policy_revalidation_keeps_last_complete_grants_and_remains_retryable() {
        use pioneer_protocol::*;
        let core = crate::catalog_test_support::settings_client();
        let auth = core.current_auth().unwrap();
        let before = core.authorization_snapshot(None, None).unwrap();
        core.observe_policy_change(&AuthorizationProjectionChangedNotification {
            policy_generation: PolicyGeneration::new(2).unwrap(),
            change: AuthorizationChangeKind::RoleAssignment,
            affected: AuthorizationChangeScope::Global,
        });
        let result = core.refresh_identity_authorization_once(
            AuthorizationCapabilitiesParams {
                workspace_id: None,
                thread_id: None,
            },
            &mut || Ok(auth.clone()),
            &mut |_| Err(anyhow::anyhow!("offline")),
        );
        assert!(result.is_err());
        assert_eq!(
            core.authorization_snapshot(None, None),
            Some(before.clone())
        );
        assert!(core.authorization_revalidation_due());
        assert!(!core.authorization_revalidation_due());
        let result = core.refresh_identity_authorization_once(
            AuthorizationCapabilitiesParams {
                workspace_id: None,
                thread_id: None,
            },
            &mut || Ok(auth.clone()),
            &mut |_| {
                core.observe_policy_change(&AuthorizationProjectionChangedNotification {
                    policy_generation: PolicyGeneration::new(3).unwrap(),
                    change: AuthorizationChangeKind::RoleAssignment,
                    affected: AuthorizationChangeScope::Global,
                });
                let mut stale = before.clone();
                stale.authorization_revision = 2;
                stale.global.can_manage_capabilities = false;
                Ok(stale)
            },
        );
        assert!(
            result.is_err(),
            "an obsolete response crossed a newer policy hint"
        );
        assert_eq!(core.authorization_snapshot(None, None), Some(before));
    }

    #[test]
    fn capability_rpc_completion_resumes_already_mounted_settings_and_provider_demands() {
        use crate::providers::store::{
            ProviderCollectionIntent, ProviderCollectionKey, ProviderLoadState,
        };
        use crate::settings::runtime::{SettingsIntent, SettingsPage};
        let core = crate::catalog_test_support::settings_client();
        let auth = core.current_auth().unwrap();
        let mut capabilities = core.authorization_snapshot(None, None).unwrap();
        capabilities.global.can_manage_gateway_settings = true;
        // Reproduce initial authentication, before the first capability response.
        core.clear_authorization_projections();
        let (generation, connection) = core.current_auth_ticket();
        core.finish_current_auth(generation, connection, auth.clone())
            .unwrap();
        let key = ProviderCollectionKey::catalog("workspace");
        core.provider_collection_intent(ProviderCollectionIntent::Observe { key: key.clone() });
        let _settings = core.acquire_settings_page(SettingsPage::General);
        assert_eq!(
            core.provider_collection_snapshot(&key).unwrap().request(),
            ProviderLoadState::Forbidden
        );
        assert_eq!(
            core.settings_intent(SettingsIntent::Refresh).outcome(),
            ClientTransitionOutcome::Rejected
        );
        let requests = Cell::new(0);
        core.refresh_identity_authorization_with_ports(
            pioneer_protocol::AuthorizationCapabilitiesParams {
                workspace_id: None,
                thread_id: None,
            },
            || Ok(auth.clone()),
            |_| {
                requests.set(requests.get() + 1);
                Ok(capabilities.clone())
            },
        )
        .unwrap();
        assert_eq!(requests.get(), 1);
        assert!(
            core.authorization_snapshot(None, None)
                .unwrap()
                .global
                .can_manage_gateway_settings
        );
        // No network worker is installed in this fixture: Failed proves that the
        // catalog request reached dispatch instead of remaining Forbidden/Cancelled.
        assert_eq!(
            core.provider_collection_snapshot(&key).unwrap().request(),
            ProviderLoadState::Failed
        );
        // Refresh is already queued for the retained demand; a second intent coalesces.
        assert_eq!(
            core.settings_intent(SettingsIntent::Refresh).outcome(),
            ClientTransitionOutcome::Noop
        );
    }

    #[test]
    fn transient_reads_are_bounded_and_success_ends_retry() {
        let calls = Cell::new(0);
        let mut delays = Vec::new();
        let result = retry_capability_read::<()>(
            || {
                calls.set(calls.get() + 1);
                Err((None, anyhow::anyhow!("temporary")))
            },
            || true,
            |delay| delays.push(delay.as_millis()),
        );
        assert!(result.is_err());
        assert_eq!(calls.get(), 5);
        assert_eq!(delays, [100, 250, 500, 1_000]);
        calls.set(0);
        let result = retry_capability_read(
            || {
                calls.set(calls.get() + 1);
                if calls.get() == 2 {
                    Ok(42)
                } else {
                    Err((None, anyhow::anyhow!("temporary")))
                }
            },
            || true,
            |_| {},
        );
        assert_eq!(result.unwrap(), 42);
        assert_eq!(calls.get(), 2);
    }
    #[test]
    fn terminal_and_replaced_scopes_cannot_retry() {
        for code in [
            "session_revoked",
            "session_compromised",
            "invalid_capability_scope",
            "gateway_identity_mismatch",
        ] {
            let result = retry_capability_read::<()>(
                || {
                    Err((
                        None,
                        anyhow::Error::new(crate::rpc::JsonRpcResponseError::server(
                            None,
                            "rejected",
                            Some(code.into()),
                        )),
                    ))
                },
                || true,
                |_| panic!("terminal reads cannot schedule retries"),
            );
            assert!(result.is_err());
        }
        let calls = Cell::new(0);
        let current = Cell::new(true);
        let result = retry_capability_read::<()>(
            || {
                calls.set(calls.get() + 1);
                Err((None, anyhow::anyhow!("temporary")))
            },
            || current.get(),
            |_| current.set(false),
        );
        assert_eq!(calls.get(), 1);
        assert_eq!(
            result.unwrap_err().1.to_string(),
            "capability_request_stale"
        );
    }
}
