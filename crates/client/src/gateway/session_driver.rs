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
    next_attempt: Option<Instant>,
    wake: Option<mpsc::SyncSender<()>>,
    task: Option<std::thread::JoinHandle<()>>,
}
impl SessionDriver {
    pub(crate) fn allows_connection(&self, endpoint: &str) -> bool {
        self.demand.as_ref().is_none_or(|demand| {
            demand.endpoint_id.as_deref() == Some(endpoint)
                && demand.visibility == SessionVisibility::Foreground
                && demand.network_available
        })
    }
    pub(crate) fn stop(&mut self) {
        self.demand = None;
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
        let previous_endpoints = owner
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
        let demand = {
            let owner = self.session_driver.lock().expect("session driver poisoned");
            let Some(demand) = owner.demand.clone() else {
                return;
            };
            demand
        };
        if demand.visibility != SessionVisibility::Foreground || !demand.network_available {
            return;
        }
        let Some(id) = demand.endpoint_id.as_deref() else {
            return;
        };
        let publication = self.gateway_session();
        if publication.terminal_reason(id).is_some() {
            return;
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
