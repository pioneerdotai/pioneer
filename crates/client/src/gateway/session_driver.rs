//! Session demand and recovery scheduling owned by the process-local Client.
use crate::core::{ClientCore, ClientTransition, ClientTransitionOutcome};
use serde::{Deserialize, Serialize};
use std::{
    sync::{Arc, mpsc},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionVisibility {
    Foreground,
    Inactive,
    Background,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionDemand {
    pub endpoint_id: Option<String>,
    pub visibility: SessionVisibility,
    pub network_available: bool,
    pub generation: u64,
}

#[derive(Default)]
pub(crate) struct SessionDriver {
    demand: Option<SessionDemand>,
    handoff: Option<(String, std::thread::ThreadId, usize)>,
    next_attempt: Option<Instant>,
    authorization_demand: Option<AuthorizationDemand>,
    wake: Option<mpsc::SyncSender<()>>,
    task: Option<std::thread::JoinHandle<()>>,
}

/// Deduplicates the active scope's intent, not its response or retry policy.
/// Capability projections and bounded request retries belong to identity_authorization.
#[derive(PartialEq, Eq)]
struct AuthorizationDemand {
    demand_generation: u64,
    authorization_epoch: (u64, u64),
    transport_revision: u64,
    workspace_id: Option<String>,
}
impl SessionDriver {
    pub(super) fn begin_handoff(&mut self, endpoint: &str) {
        let thread = std::thread::current().id();
        if let Some((id, owner, depth)) = &mut self.handoff
            && id == endpoint
            && *owner == thread
        {
            *depth += 1;
        } else {
            self.handoff = Some((endpoint.to_owned(), thread, 1));
        }
    }
    pub(super) fn end_handoff(&mut self, endpoint: &str) {
        if let Some((id, owner, depth)) = &mut self.handoff
            && id == endpoint
            && *owner == std::thread::current().id()
        {
            *depth -= 1;
            if *depth == 0 {
                self.handoff = None;
            }
        }
    }
    pub(crate) fn allows_connection(&self, endpoint: &str) -> bool {
        let available = self.demand.as_ref().is_none_or(|demand| {
            demand.visibility == SessionVisibility::Foreground && demand.network_available
        });
        if let Some((id, owner, _)) = &self.handoff {
            // Only the explicit onboarding operation may replace the current
            // transport. Background recovery must not reconnect the old endpoint.
            return available && id == endpoint && *owner == std::thread::current().id();
        }
        available
            && self
                .demand
                .as_ref()
                .is_none_or(|demand| demand.endpoint_id.as_deref() == Some(endpoint))
    }
    pub(crate) fn stop(&mut self) {
        self.demand = None;
        self.handoff = None;
        self.authorization_demand = None;
        self.wake.take();
    }
}
impl Drop for SessionDriver {
    fn drop(&mut self) {
        self.wake.take();
        if let Some(task) = self.task.take()
            && task.thread().id() != std::thread::current().id()
        {
            let _ = task.join();
        }
    }
}
impl ClientCore {
    pub fn session_demand(&self, demand: SessionDemand) -> ClientTransition {
        let mut owner = self.session_driver.lock().expect("session driver poisoned");
        if self.is_stopped()
            || demand
                .endpoint_id
                .as_ref()
                .is_some_and(|id| id.trim().is_empty() || id.len() > 1024)
        {
            return self.reject_intent();
        }
        if owner
            .demand
            .as_ref()
            .is_some_and(|old| demand.generation < old.generation)
        {
            return self.navigation_outcome(ClientTransitionOutcome::Stale);
        }
        if owner.demand.as_ref() == Some(&demand) {
            return self.navigation_outcome(ClientTransitionOutcome::Noop);
        }
        if owner
            .demand
            .as_ref()
            .is_some_and(|old| old.generation == demand.generation)
        {
            return self.navigation_outcome(ClientTransitionOutcome::Stale);
        }
        let mut previous_endpoints = owner
            .demand
            .as_ref()
            .and_then(|old| old.endpoint_id.as_ref())
            .map_or_else(
                || {
                    self.gateway_session()
                        .connections
                        .keys()
                        .cloned()
                        .collect::<Vec<_>>()
                },
                |endpoint| vec![endpoint.clone()],
            );
        // A new lifecycle demand supersedes an in-flight explicit switch too.
        if let Some((endpoint, _, _)) = owner.handoff.take()
            && !previous_endpoints.contains(&endpoint)
        {
            previous_endpoints.push(endpoint);
        }
        let retiring = previous_endpoints
            .into_iter()
            .filter_map(|endpoint| {
                let replacing = demand.endpoint_id.as_deref() != Some(endpoint.as_str());
                let suspending = demand.visibility == SessionVisibility::Background
                    && owner
                        .demand
                        .as_ref()
                        .is_none_or(|old| old.visibility != SessionVisibility::Background);
                (replacing || suspending)
                    .then_some((endpoint, replacing && demand.endpoint_id.is_some()))
            })
            .collect::<Vec<_>>();
        owner.demand = Some(demand);
        owner.next_attempt = None;
        for (endpoint, replacing) in retiring {
            self.retire_demand_session(&endpoint);
            if replacing {
                self.invalidate_session_authorization(&endpoint);
            }
        }
        if let Some(wake) = &owner.wake {
            let _ = wake.try_send(());
        }
        drop(owner);
        self.navigation_outcome(ClientTransitionOutcome::Changed)
    }
    pub(crate) fn start_session_driver(self: &Arc<Self>) {
        let (wake, received) = mpsc::sync_channel(1);
        let weak = Arc::downgrade(self);
        let task = std::thread::Builder::new()
            .name("client-session-demand".into())
            .spawn(move || {
                loop {
                    if let Err(mpsc::RecvTimeoutError::Disconnected) =
                        received.recv_timeout(Duration::from_millis(250))
                    {
                        break;
                    }
                    let Some(core) = weak.upgrade() else { break };
                    if core.is_stopped() {
                        break;
                    }
                    core.drive_session_demand();
                }
            })
            .expect("session demand worker unavailable");
        let mut owner = self.session_driver.lock().expect("session driver poisoned");
        owner.wake = Some(wake);
        owner.task = Some(task);
    }
    fn drive_session_demand(&self) {
        self.drive_session_demand_with_ports(
            |endpoint| self.verify_configured_gateway_session(endpoint),
            |params| {
                let _ = self.refresh_identity_authorization(params);
            },
        );
    }

    pub(super) fn drive_session_demand_with_ports(
        &self,
        verify_identity: impl FnOnce(
            &str,
        ) -> anyhow::Result<
            Option<super::session_lifecycle::SessionTerminalReason>,
        >,
        refresh_authorization: impl FnOnce(pioneer_protocol::AuthorizationCapabilitiesParams),
    ) {
        let demand = {
            let owner = self.session_driver.lock().expect("session driver poisoned");
            if owner.handoff.is_some() {
                return;
            }
            let Some(demand) = owner.demand.clone() else {
                return;
            };
            demand
        };
        if self.is_stopped()
            || demand.visibility != SessionVisibility::Foreground
            || !demand.network_available
        {
            return;
        }
        let Some(id) = demand.endpoint_id.as_deref() else {
            return;
        };
        let publication = self.gateway_session();
        if publication.terminal_reason(id).is_some() {
            return;
        }
        // Automatic transport reconnects require identity verification before
        // publishing readiness. A pending connection must not skip this step.
        if publication.startup.endpoint_id.as_deref() == Some(id) {
            if publication.startup.identity_pending {
                if verify_identity(id).is_err()
                    && let Some(connection) = publication.startup.connection_id
                {
                    self.reject_gateway_session_identity(
                        id,
                        connection,
                        super::session_connection::GatewaySessionConnectionFailure::Unavailable {
                            code: "gateway_identity_unavailable".into(),
                        },
                    );
                }
                return;
            }
            if publication.startup.transport_ready && self.current_auth().is_some() {
                let workspace_id = self.navigation_snapshot().workspace_id().map(str::to_owned);
                let intent = AuthorizationDemand {
                    demand_generation: demand.generation,
                    authorization_epoch: self
                        .identity_authorization
                        .lock()
                        .expect("identity owner poisoned")
                        .authorization_epoch(),
                    transport_revision: publication.startup.transport_revision,
                    workspace_id: workspace_id.clone(),
                };
                let missing = self
                    .authorization_snapshot(workspace_id.as_deref(), None)
                    .is_none();
                let dispatch = {
                    let mut owner = self.session_driver.lock().expect("session driver poisoned");
                    if owner.demand.as_ref() != Some(&demand) {
                        return;
                    }
                    let changed = owner.authorization_demand.as_ref() != Some(&intent);
                    // A verified reconnect can retain the same principal and
                    // projection for continuity, but may have missed policy events.
                    let reconnected = owner
                        .authorization_demand
                        .as_ref()
                        .is_some_and(|old| old.transport_revision != intent.transport_revision);
                    owner.authorization_demand = Some(intent);
                    (missing || reconnected) && changed
                };
                if dispatch {
                    refresh_authorization(pioneer_protocol::AuthorizationCapabilitiesParams {
                        workspace_id,
                        thread_id: None,
                    });
                    return;
                }
            }
        }
        let now = Instant::now();
        if self
            .session_driver
            .lock()
            .expect("session driver poisoned")
            .next_attempt
            .is_some_and(|at| at > now)
        {
            return;
        }
        let connection = publication.connections.get(id);
        let unix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let refresh_due = publication
            .refresh_delay(id, unix, 60)
            .is_some_and(|delay| delay.is_zero());
        if connection.is_some_and(|c| {
            c.pending || c.connected.is_some() && !c.refresh_requested && !refresh_due
        }) {
            return;
        }
        if self
            .gateway_registry()
            .as_ref()
            .and_then(|r| r.active_gateway_id.as_deref())
            != Some(id)
        {
            return;
        }
        let result = self.refresh_configured_gateway_session(id);
        let mut owner = self.session_driver.lock().expect("session driver poisoned");
        if owner.demand.as_ref() != Some(&demand) {
            return;
        }
        if result.is_err() {
            let delay = self
                .gateway_session()
                .connections
                .get(id)
                .and_then(|c| c.retry_delay_ms)
                .unwrap_or(1_000);
            owner.next_attempt = Some(Instant::now() + Duration::from_millis(delay.max(1)));
        } else {
            owner.next_attempt = None;
            let restored = self
                .gateway_session()
                .connections
                .get(id)
                .and_then(|state| {
                    state
                        .connected
                        .as_ref()
                        .map(|session| session.connection_id)
                });
            let previous = connection.and_then(|state| {
                state
                    .connected
                    .as_ref()
                    .map(|session| session.connection_id)
            });
            if restored.is_some() && restored != previous {
                self.resume_visible_thread_delivery();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_handoff_is_scoped_nested_and_superseded_by_background_demand() {
        let core = ClientCore::new();
        core.session_demand(SessionDemand {
            endpoint_id: Some("old".into()),
            visibility: SessionVisibility::Foreground,
            network_available: true,
            generation: 1,
        });
        {
            let mut driver = core.session_driver.lock().unwrap();
            assert!(driver.allows_connection("old"));
            assert!(!driver.allows_connection("new"));
            driver.begin_handoff("new");
            driver.begin_handoff("new");
            assert!(driver.allows_connection("new"));
            assert!(!driver.allows_connection("old"));
            driver.end_handoff("new");
            assert!(driver.allows_connection("new"));
            driver.end_handoff("new");
            assert!(!driver.allows_connection("new"));
            assert!(driver.allows_connection("old"));
            driver.begin_handoff("new");
        }
        std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    let driver = core.session_driver.lock().unwrap();
                    assert!(!driver.allows_connection("new"));
                    assert!(!driver.allows_connection("old"));
                })
                .join()
                .unwrap();
        });
        core.session_demand(SessionDemand {
            endpoint_id: Some("old".into()),
            visibility: SessionVisibility::Background,
            network_available: true,
            generation: 2,
        });
        {
            let driver = core.session_driver.lock().unwrap();
            assert!(driver.handoff.is_none());
            assert!(!driver.allows_connection("new"));
            assert!(!driver.allows_connection("old"));
        }
        assert!(
            core.gateway_session().connections["new"]
                .connected
                .is_none()
        );
        core.shutdown();
    }

    fn ready(core: &ClientCore) {
        core.session_demand(SessionDemand {
            endpoint_id: Some("endpoint".into()),
            visibility: SessionVisibility::Foreground,
            network_available: true,
            generation: 1,
        });
        let mut session = core.gateway_session.lock().unwrap();
        session.observe_transport(&crate::transport::ws::GatewayWsEvent::Connected {
            connection_id: 7,
            endpoint_id: "endpoint".into(),
            endpoint_name: "Synthetic".into(),
            gateway_base_url: pioneer_protocol::GatewayBaseUrl::parse_presentation(
                "https://synthetic.invalid",
            )
            .unwrap(),
        });
        core.publish_gateway_session(&session);
    }

    #[test]
    fn verified_session_drives_missing_authorization_without_a_shell_callback() {
        let core = crate::catalog_test_support::settings_client();
        let mut capabilities = core.authorization_snapshot(None, None).unwrap();
        capabilities.authorization_revision += 1;
        core.invalidate_authorization_revision(capabilities.authorization_revision);
        assert!(core.current_auth().is_some());
        assert!(core.authorization_snapshot(None, None).is_none());
        ready(&core);
        let calls = std::cell::Cell::new(0);
        core.drive_session_demand_with_ports(
            |_| panic!("verified transport must not repeat identity verification"),
            |params| {
                assert_eq!(params.workspace_id, None);
                assert_eq!(params.thread_id, None);
                calls.set(calls.get() + 1);
                let (generation, connection) = core.current_auth_ticket();
                core.accept_authorization_projection(generation, connection, capabilities);
            },
        );
        assert_eq!(
            calls.get(),
            1,
            "session startup must request capabilities without the deleted Desktop callback"
        );
        assert!(
            core.authorization_snapshot(None, None)
                .unwrap()
                .global
                .can_manage_capabilities
        );
        core.drive_session_demand_with_ports(
            |_| panic!("verified transport"),
            |_| panic!("accepted capabilities must not be fetched on every driver tick"),
        );
    }

    #[test]
    fn verified_reconnect_revalidates_retained_capabilities_once() {
        let core = crate::catalog_test_support::settings_client();
        ready(&core);
        core.drive_session_demand_with_ports(
            |_| panic!("verified transport"),
            |_| panic!("already supplied capabilities"),
        );
        {
            let mut session = core.gateway_session.lock().unwrap();
            session.observe_transport(&crate::transport::ws::GatewayWsEvent::Reconnecting {
                connection_id: 7,
                endpoint_id: "endpoint".into(),
                endpoint_name: "Synthetic".into(),
                attempt: 1,
                delay_ms: 100,
                reason: "synthetic disconnect".into(),
            });
            core.publish_gateway_session(&session);
        }
        core.drive_session_demand_with_ports(
            |_| panic!("transport has not reconnected"),
            |_| panic!("disconnected capability request"),
        );
        ready(&core);
        assert!(core.authorization_snapshot(None, None).is_some());
        let calls = std::cell::Cell::new(0);
        for _ in 0..2 {
            core.drive_session_demand_with_ports(
                |_| panic!("verified transport"),
                |_| calls.set(calls.get() + 1),
            );
        }
        assert_eq!(
            calls.get(),
            1,
            "a reconnect may have missed a policy change even with retained capabilities"
        );
    }

    #[test]
    fn workspace_authorization_from_session_driver_unblocks_existing_thread_selection() {
        use pioneer_protocol::*;
        let core = crate::catalog_test_support::settings_client();
        let mut capabilities = core.authorization_snapshot(None, None).unwrap();
        ready(&core);
        core.upsert_thread(
            serde_json::from_value(serde_json::json!({
                "id":"thread","workspace_id":"workspace","preview":"","mode":"Chat",
                "model":"model","model_provider":"provider","created_at":1,"updated_at":1,
                "status":"Idle","turns":[]
            }))
            .unwrap(),
        );
        core.navigate(
            crate::navigation::NavigationIntent::SelectWorkspace {
                workspace_id: Some("workspace".into()),
            },
            None,
        );
        let select = crate::workspaces::intents::WorkspaceIntent::SelectThread {
            workspace_id: "workspace".into(),
            thread_id: "thread".into(),
        };
        assert!(core.execute_workspace_intent(select.clone()).is_err());
        assert!(core.navigation_snapshot().active_thread_id().is_none());
        let resources = AuthorizationOperationalResourceProjection {
            fingerprint: "synthetic".into(),
            ..Default::default()
        };
        capabilities.workspace = Some(AuthorizationWorkspaceCapabilitySnapshot {
            workspace_id: "workspace".into(),
            capabilities: Default::default(),
            operational_resources: resources.clone(),
            execution_draft_policy: AuthorizationExecutionDraftPolicyProjection {
                fingerprint: "synthetic".into(),
                resources,
                permission_options: vec![],
                can_attach_artifacts: false,
                mcp_invocation_limits: Default::default(),
            },
        });
        core.drive_session_demand_with_ports(
            |_| panic!("verified transport"),
            |params| {
                assert_eq!(params.workspace_id.as_deref(), Some("workspace"));
                let (generation, connection) = core.current_auth_ticket();
                assert_eq!(
                    core.accept_authorization_projection(generation, connection, capabilities),
                    crate::authorization::AuthorizationProjectionAcceptance::Accepted
                );
            },
        );
        core.execute_workspace_intent(select).unwrap();
        assert_eq!(
            core.navigation_snapshot().active_thread_id(),
            Some("thread")
        );
    }

    #[test]
    fn workspace_and_policy_changes_request_exact_scopes_without_a_retry_loop() {
        let core = crate::catalog_test_support::settings_client();
        ready(&core);
        let requested = std::cell::RefCell::new(Vec::new());
        let tick = || {
            core.drive_session_demand_with_ports(
                |_| panic!("verified transport"),
                |params| {
                    assert!(params.thread_id.is_none());
                    // An exhausted bounded capability request leaves no projection.
                    requested.borrow_mut().push(params.workspace_id);
                },
            )
        };
        tick();
        assert!(
            requested.borrow().is_empty(),
            "the existing global projection is sufficient"
        );
        for workspace in ["first", "second", "first"] {
            core.navigate(
                crate::navigation::NavigationIntent::SelectWorkspace {
                    workspace_id: Some(workspace.into()),
                },
                None,
            );
            tick();
            tick();
        }
        assert_eq!(
            *requested.borrow(),
            vec![
                Some("first".into()),
                Some("second".into()),
                Some("first".into())
            ]
        );
        core.invalidate_authorization_revision(2);
        tick();
        tick();
        assert_eq!(
            requested.borrow().len(),
            4,
            "new policy retries once through the capability owner"
        );
        assert_eq!(requested.borrow()[3].as_deref(), Some("first"));
    }

    #[test]
    fn authorization_startup_respects_visibility_network_identity_and_shutdown() {
        for (visibility, online) in [
            (SessionVisibility::Inactive, true),
            (SessionVisibility::Background, true),
            (SessionVisibility::Foreground, false),
        ] {
            let core = crate::catalog_test_support::settings_client();
            ready(&core);
            core.invalidate_authorization_revision(2);
            core.session_demand(SessionDemand {
                endpoint_id: Some("endpoint".into()),
                visibility,
                network_available: online,
                generation: 2,
            });
            core.drive_session_demand_with_ports(
                |_| panic!("inactive identity request"),
                |_| panic!("inactive capability request"),
            );
        }
        let core = crate::catalog_test_support::settings_client();
        ready(&core);
        core.clear_authorization_projections();
        core.drive_session_demand_with_ports(
            |_| panic!("verified transport"),
            |_| panic!("a transport without verified auth is insufficient"),
        );
        core.shutdown();
        core.drive_session_demand_with_ports(
            |_| panic!("stopped identity request"),
            |_| panic!("stopped capability request"),
        );
    }
    #[test]
    fn demand_generations_are_exact_and_invalid_inputs_do_not_replace_the_owner() {
        let core = ClientCore::new();
        let demand = SessionDemand {
            endpoint_id: Some("endpoint".into()),
            visibility: SessionVisibility::Foreground,
            network_available: true,
            generation: 4,
        };
        assert_eq!(
            core.session_demand(demand.clone()).outcome(),
            ClientTransitionOutcome::Changed
        );
        let publication = core.gateway_session();
        assert_eq!(
            core.session_demand(demand.clone()).outcome(),
            ClientTransitionOutcome::Noop
        );
        assert_eq!(
            core.session_demand(SessionDemand {
                generation: 3,
                ..demand.clone()
            })
            .outcome(),
            ClientTransitionOutcome::Stale
        );
        assert_eq!(
            core.session_demand(SessionDemand {
                network_available: false,
                ..demand.clone()
            })
            .outcome(),
            ClientTransitionOutcome::Stale
        );
        assert_eq!(
            core.session_demand(SessionDemand {
                endpoint_id: Some(" ".into()),
                generation: 5,
                ..demand.clone()
            })
            .outcome(),
            ClientTransitionOutcome::Rejected
        );
        assert_eq!(
            core.session_driver.lock().unwrap().demand.as_ref(),
            Some(&demand)
        );
        assert_eq!(core.gateway_session(), publication);
        core.shutdown();
        assert_eq!(
            core.session_demand(demand).outcome(),
            ClientTransitionOutcome::Rejected
        );
    }
    #[test]
    fn only_foreground_online_demand_admits_a_matching_endpoint() {
        let core = ClientCore::new();
        for (index, (visibility, online, allowed)) in [
            (SessionVisibility::Foreground, true, true),
            (SessionVisibility::Inactive, true, false),
            (SessionVisibility::Background, true, false),
            (SessionVisibility::Foreground, false, false),
            (SessionVisibility::Foreground, true, true),
        ]
        .into_iter()
        .enumerate()
        {
            core.session_demand(SessionDemand {
                endpoint_id: Some("endpoint".into()),
                visibility,
                network_available: online,
                generation: index as u64 + 1,
            });
            let driver = core.session_driver.lock().unwrap();
            assert_eq!(driver.allows_connection("endpoint"), allowed);
            assert!(!driver.allows_connection("other"));
        }
    }
}
