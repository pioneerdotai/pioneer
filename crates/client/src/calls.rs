//! Thread call signalling on the authenticated Gateway connection.

use crate::{core::ClientCore, transport::ws::GatewayWsEvent};
use pioneer_protocol::{
    GatewayNotification,
    constants::{events, methods},
};
pub use pioneer_protocol::{
    ThreadCallAction, ThreadCallCredentials, ThreadCallEndReason, ThreadCallParams,
    ThreadCallPhase, ThreadCallResponse, ThreadCallSnapshot, ThreadCallTransition,
};
use serde::Serialize;
use std::{
    sync::{Condvar, Mutex},
    time::Duration,
};
use tokio::sync::watch;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const RECENT_CALL_LIMIT: usize = 128;

#[derive(Clone, Default, Serialize)]
pub struct CallUpdates {
    pub sequence: u64,
    pub connection_id: Option<u64>,
    pub calls: Vec<ThreadCallSnapshot>,
}

pub(crate) struct CallNotifications {
    updates: watch::Sender<CallUpdates>,
    lock: Mutex<()>,
    ready: Condvar,
}

impl Default for CallNotifications {
    fn default() -> Self {
        Self {
            updates: watch::channel(CallUpdates::default()).0,
            lock: Mutex::new(()),
            ready: Condvar::new(),
        }
    }
}

impl CallNotifications {
    pub(crate) fn clear(&self) {
        let _guard = self.lock.lock().expect("call notifications poisoned");
        self.updates.send_modify(|current| {
            current.connection_id = None;
            current.calls.clear();
            current.sequence += 1;
        });
        self.ready.notify_all();
    }

    fn apply(&self, connection_id: u64, incoming: ThreadCallSnapshot) {
        let _guard = self.lock.lock().expect("call notifications poisoned");
        let changed = self.updates.send_if_modified(|current| {
            if current.connection_id != Some(connection_id) {
                current.connection_id = Some(connection_id);
                current.calls.clear();
            }
            if let Some(index) = current
                .calls
                .iter()
                .position(|call| call.call_id == incoming.call_id)
            {
                if current.calls[index].revision >= incoming.revision {
                    return false;
                }
                current.calls.remove(index);
            }
            if current.calls.len() == RECENT_CALL_LIMIT {
                current.calls.remove(
                    current
                        .calls
                        .iter()
                        .position(|call| call.phase == ThreadCallPhase::Ended)
                        .unwrap_or(0),
                );
            }
            current.calls.push(incoming);
            current.sequence += 1;
            true
        });
        if changed {
            self.ready.notify_all();
        }
    }
}

impl ClientCore {
    pub fn request_thread_call(
        &self,
        connection_id: u64,
        params: &ThreadCallParams,
    ) -> anyhow::Result<ThreadCallResponse> {
        let ticket = self.current_auth_ticket();
        anyhow::ensure!(
            ticket.1 == Some(connection_id) && !self.is_stopped(),
            "Call connection changed"
        );
        let sender = self.transport_runtime().ws_command_sender();
        let transport = sender.requests_for_connection(connection_id);
        let response = crate::rpc::send_json_rpc_request_typed(
            &transport,
            methods::THREAD_CALL,
            params,
            REQUEST_TIMEOUT,
        )?;
        anyhow::ensure!(
            self.current_auth_ticket() == ticket && !self.is_stopped(),
            "Call connection changed"
        );
        Ok(response)
    }

    /// Resolve display names through the existing authorized workspace member directory.
    pub fn thread_call_names(
        self: &std::sync::Arc<Self>,
        connection_id: u64,
        workspace_id: &str,
    ) -> anyhow::Result<std::collections::HashMap<String, String>> {
        use crate::administration::pages::AdministrationPage;
        let ticket = self.current_auth_ticket();
        let current = || self.current_auth_ticket() == ticket && !self.is_stopped();
        anyhow::ensure!(
            ticket.1 == Some(connection_id) && current(),
            "Call connection changed"
        );
        let scope = AdministrationPage::WorkspaceMembers {
            workspace_id: pioneer_protocol::WorkspaceId::new(workspace_id.to_owned())?,
        };
        let page = self
            .read_administration_page(scope, false)?
            .wait_while(&current)?;
        anyhow::ensure!(current(), "Call connection changed");
        Ok(page
            .members
            .iter()
            .filter_map(|row| {
                let member = &row.member;
                let name = member.display_name.trim();
                let name = if name.is_empty() {
                    member.nickname.trim()
                } else {
                    name
                };
                (!name.is_empty()).then(|| (member.principal_id.to_string(), name.to_owned()))
            })
            .collect())
    }

    pub fn thread_call_updates(&self) -> watch::Receiver<CallUpdates> {
        self.calls.updates.subscribe()
    }

    pub fn wait_for_thread_call_updates(&self, after_sequence: u64) -> CallUpdates {
        let guard = self.calls.lock.lock().expect("call notifications poisoned");
        let (_guard, _) = self
            .calls
            .ready
            .wait_timeout_while(guard, Duration::from_secs(2), |_| {
                !self.is_stopped() && self.calls.updates.borrow().sequence <= after_sequence
            })
            .expect("call notification wait poisoned");
        let updates = self.calls.updates.borrow().clone();
        self.current_thread_call_updates(&updates)
    }

    pub fn current_thread_call_updates(&self, updates: &CallUpdates) -> CallUpdates {
        let connection_id = self.current_auth_ticket().1;
        if !self.is_stopped() && updates.connection_id == connection_id {
            return updates.clone();
        }
        CallUpdates {
            sequence: updates.sequence,
            connection_id: None,
            calls: Vec::new(),
        }
    }

    pub(crate) fn observe_call_transport(&self, event: &GatewayWsEvent) -> bool {
        let sender = self.transport_runtime().ws_command_sender();
        match event {
            GatewayWsEvent::Notification {
                connection_id,
                notification: GatewayNotification::Unknown(notification),
            } if notification.method == events::THREAD_CALL_CHANGED => {
                if sender.gateway_state_event_is_current(event)
                    && self.current_auth_ticket().1 == Some(*connection_id)
                    && let Ok(call) = serde_json::from_value(notification.params.clone())
                {
                    self.calls.apply(*connection_id, call);
                }
                true
            }
            GatewayWsEvent::Connected { .. }
            | GatewayWsEvent::Disconnected { .. }
            | GatewayWsEvent::Reconnecting { .. }
                if sender.gateway_state_event_is_current(event) =>
            {
                self.calls.clear();
                false
            }
            _ => false,
        }
    }
}

pub fn change_call(
    call: &ThreadCallSnapshot,
    transition: ThreadCallTransition,
) -> ThreadCallAction {
    ThreadCallAction::Change {
        call_id: call.call_id.clone(),
        revision: call.revision,
        transition,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CallMembership {
    Absent,
    Active,
    Held,
}

pub fn call_membership(call: Option<&ThreadCallSnapshot>, principal: &str) -> CallMembership {
    call.filter(|call| call.phase != ThreadCallPhase::Ended)
        .and_then(|call| {
            call.participants
                .iter()
                .find(|member| member.principal_id == principal)
        })
        .map_or(CallMembership::Absent, |member| {
            if member.held {
                CallMembership::Held
            } else {
                CallMembership::Active
            }
        })
}

/// Reconcile a CAS conflict or an unknown RPC outcome with one exact-ID read.
/// Keep ownership with the caller when the desired state cannot be verified.
pub fn settle_call_membership(
    principal: &str,
    transition: ThreadCallTransition,
    mut status: impl FnMut() -> anyhow::Result<Option<ThreadCallSnapshot>>,
    change: impl FnOnce(&ThreadCallSnapshot) -> anyhow::Result<Option<ThreadCallSnapshot>>,
) -> anyhow::Result<Option<ThreadCallSnapshot>> {
    let settled =
        |call: Option<&ThreadCallSnapshot>| match (transition, call_membership(call, principal)) {
            (ThreadCallTransition::Leave, CallMembership::Absent)
            | (ThreadCallTransition::Hold, CallMembership::Held | CallMembership::Absent)
            | (ThreadCallTransition::Resume, CallMembership::Active | CallMembership::Absent) => {
                true
            }
            _ => false,
        };
    let current = status()?;
    if settled(current.as_ref()) {
        return Ok(current);
    }
    let call = current
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Call is unavailable"))?;
    let error = match change(call) {
        Ok(Some(next)) if settled(Some(&next)) => return Ok(Some(next)),
        Ok(_) => anyhow::anyhow!("Call transition was not confirmed"),
        Err(error) => error,
    };
    if let Ok(next) = status()
        && settled(next.as_ref())
    {
        return Ok(next);
    }
    Err(error)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot(id: &str, revision: i64, phase: ThreadCallPhase) -> ThreadCallSnapshot {
        ThreadCallSnapshot {
            workspace_id: "workspace".into(),
            thread_id: "thread".into(),
            call_id: id.into(),
            revision,
            created_at_unix: 0,
            phase,
            initiator_id: "caller".into(),
            invitee_id: None,
            ring_until_unix: None,
            end_reason: None,
            participants: Vec::new(),
        }
    }

    #[test]
    fn a_late_ringing_update_cannot_reopen_an_ended_call() {
        let notifications = CallNotifications::default();
        notifications.apply(4, snapshot("call", 3, ThreadCallPhase::Ended));
        notifications.apply(4, snapshot("call", 2, ThreadCallPhase::Ringing));
        let updates = notifications.updates.borrow();
        assert_eq!(updates.sequence, 1);
        assert_eq!(updates.calls[0].phase, ThreadCallPhase::Ended);
    }

    #[test]
    fn switching_connections_clears_previous_calls() {
        let notifications = CallNotifications::default();
        notifications.apply(4, snapshot("previous", 5, ThreadCallPhase::Active));
        notifications.apply(7, snapshot("current", 1, ThreadCallPhase::Ringing));
        {
            let updates = notifications.updates.borrow();
            assert_eq!(updates.connection_id, Some(7));
            assert_eq!(updates.calls.len(), 1);
            assert_eq!(updates.calls[0].call_id, "current");
        }
        notifications.clear();
        assert!(notifications.updates.borrow().calls.is_empty());
        assert_eq!(notifications.updates.borrow().connection_id, None);
    }
    fn participating(revision: i64, held: bool) -> ThreadCallSnapshot {
        let mut call = snapshot("call", revision, ThreadCallPhase::Active);
        call.participants
            .push(pioneer_protocol::ThreadCallParticipant {
                principal_id: "caller".into(),
                media_identity: "media".into(),
                held,
            });
        call
    }

    #[test]
    fn resume_reconciles_lost_result_with_confirmed_active_membership() {
        let mut statuses = [participating(2, true), participating(3, false)].into_iter();
        let result = settle_call_membership(
            "caller",
            ThreadCallTransition::Resume,
            || Ok(statuses.next()),
            |call| {
                assert_eq!(call.revision, 2);
                anyhow::bail!("reply lost")
            },
        )
        .unwrap()
        .unwrap();
        assert_eq!(result.revision, 3);
        assert_eq!(
            call_membership(Some(&result), "caller"),
            CallMembership::Active
        );
        assert!(statuses.next().is_none());
    }

    #[test]
    fn resume_conflict_keeps_the_attempt_unconfirmed_and_refreshes_the_revision_on_retry() {
        let mut statuses = [participating(2, true), participating(3, true)].into_iter();
        let first = settle_call_membership(
            "caller",
            ThreadCallTransition::Resume,
            || Ok(statuses.next()),
            |call| {
                assert_eq!(call.revision, 2);
                anyhow::bail!("stale revision")
            },
        );
        assert!(first.is_err());
        assert!(statuses.next().is_none());

        let result = settle_call_membership(
            "caller",
            ThreadCallTransition::Resume,
            || Ok(Some(participating(3, true))),
            |call| {
                assert_eq!(call.revision, 3);
                Ok(Some(participating(4, false)))
            },
        )
        .unwrap()
        .unwrap();
        assert_eq!(result.revision, 4);
    }

    #[test]
    fn leave_requires_authoritative_absence_even_when_change_returns_success() {
        let result = settle_call_membership(
            "caller",
            ThreadCallTransition::Leave,
            || Ok(Some(participating(2, false))),
            |_| Ok(Some(participating(2, false))),
        );
        assert!(result.is_err());

        let mut statuses = [Some(participating(2, false)), None].into_iter();
        let confirmed = settle_call_membership(
            "caller",
            ThreadCallTransition::Leave,
            || Ok(statuses.next().unwrap()),
            |_| anyhow::bail!("reply lost"),
        )
        .unwrap();
        assert!(confirmed.is_none());
    }
}
