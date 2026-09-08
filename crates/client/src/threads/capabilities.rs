//! Thread capability request lifetime; accepted capabilities stay in the authorization owner.

use crate::{
    authorization::{
        AuthorizationProjectionAcceptance, authorization_capability_snapshot_is_compatible,
    },
    core::{ClientCore, ClientDemand, ClientMutationAuthority, ClientScope, ClientTransition},
};
use pioneer_protocol::{
    AuthorizationCapabilitiesParams, AuthorizationCapabilitySnapshot, PrincipalId,
};
use std::{
    collections::HashMap,
    sync::{Arc, mpsc},
    thread::JoinHandle,
};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ThreadCapabilityIntent {
    Observe { thread_id: String },
    Retry { thread_id: String },
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ThreadCapabilityRequestState {
    #[default]
    Idle,
    Loading,
    Ready,
    Failed {
        message: String,
    },
    Cancelled,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct ThreadCapabilityPublication {
    pub thread_id: String,
    pub workspace_id: String,
    pub revision: u64,
    pub generation: u64,
    pub request: ThreadCapabilityRequestState,
    pub snapshot: Option<AuthorizationCapabilitySnapshot>,
}

#[derive(Clone)]
struct CapabilityRequest {
    thread_id: String,
    workspace_id: String,
    principal_id: PrincipalId,
    generation: u64,
    auth_ticket: (u64, Option<u64>),
}

#[derive(Default)]
pub(crate) struct ThreadCapabilityController {
    generation: u64,
    publications: HashMap<String, Arc<ThreadCapabilityPublication>>,
    subscriptions: HashMap<String, usize>,
    sender: Option<mpsc::SyncSender<CapabilityRequest>>,
    task: Option<JoinHandle<()>>,
}
impl ThreadCapabilityController {
    fn next_generation(&mut self) -> u64 {
        self.generation = self
            .generation
            .checked_add(1)
            .expect("thread capability generation exhausted");
        self.generation
    }
    pub(crate) fn stop(&mut self) {
        self.sender.take();
        self.publications.clear();
        self.subscriptions.clear();
    }
}
impl Drop for ThreadCapabilityController {
    fn drop(&mut self) {
        self.stop();
        if let Some(task) = self.task.take() {
            if task.thread().id() != std::thread::current().id() {
                let _ = task.join();
            }
        }
    }
}

impl ClientCore {
    pub fn thread_capability_snapshot(
        &self,
        thread_id: &str,
    ) -> Option<Arc<ThreadCapabilityPublication>> {
        self.thread_capabilities
            .lock()
            .expect("thread capability owner poisoned")
            .publications
            .get(thread_id)
            .cloned()
    }

    fn publish_thread_capability(
        &self,
        owner: &mut ThreadCapabilityController,
        mut next: ThreadCapabilityPublication,
    ) -> ClientTransition {
        let scope = ClientScope::ThreadCapability {
            thread_id: next.thread_id.clone(),
        };
        next.revision = owner
            .publications
            .get(&next.thread_id)
            .map_or_else(
                || {
                    self.snapshot(&scope)
                        .map_or(0, |p| p.revisions().scoped().get())
                },
                |p| p.revision,
            )
            .checked_add(1)
            .expect("thread capability revision exhausted");
        let revision = next.revision;
        let next = Arc::new(next);
        owner
            .publications
            .insert(next.thread_id.clone(), next.clone());
        self.publish(
            &ClientMutationAuthority { _private: () },
            scope,
            super::registry::revisions(revision),
            next,
            vec![],
        )
    }

    pub fn thread_capability_intent(&self, intent: ThreadCapabilityIntent) -> ClientTransition {
        let (thread_id, retry) = match intent {
            ThreadCapabilityIntent::Observe { thread_id } => (thread_id, false),
            ThreadCapabilityIntent::Retry { thread_id } => (thread_id, true),
        };
        if self.is_stopped() {
            return self.reject_intent();
        }
        let Some(thread) = self.thread_coordinator_snapshot(&thread_id) else {
            return self.transition(&ClientMutationAuthority { _private: () }, vec![], vec![]);
        };
        let Some(auth) = self.current_auth() else {
            return self.transition(&ClientMutationAuthority { _private: () }, vec![], vec![]);
        };
        let ticket = self.current_auth_ticket();
        if ticket.1.is_none() {
            return self.transition(&ClientMutationAuthority { _private: () }, vec![], vec![]);
        }
        self.enqueue_thread_capability(
            thread_id,
            thread.workspace_id.clone(),
            auth.principal.id,
            ticket,
            retry,
        )
    }

    fn enqueue_thread_capability(
        &self,
        thread_id: String,
        workspace_id: String,
        principal_id: PrincipalId,
        ticket: (u64, Option<u64>),
        retry: bool,
    ) -> ClientTransition {
        if self.is_stopped() {
            return self.reject_intent();
        }
        let mut owner = self
            .thread_capabilities
            .lock()
            .expect("thread capability owner poisoned");
        if owner.publications.get(&thread_id).is_some_and(|p| {
            p.workspace_id == workspace_id
                && (p.request == ThreadCapabilityRequestState::Loading
                    || (!retry
                        && !matches!(
                            p.request,
                            ThreadCapabilityRequestState::Idle
                                | ThreadCapabilityRequestState::Cancelled
                        )))
        }) {
            return self.transition(&ClientMutationAuthority { _private: () }, vec![], vec![]);
        }
        let generation = owner.next_generation();
        let request = CapabilityRequest {
            thread_id: thread_id.clone(),
            workspace_id: workspace_id.clone(),
            principal_id,
            generation,
            auth_ticket: ticket,
        };
        let transition = self.publish_thread_capability(
            &mut owner,
            ThreadCapabilityPublication {
                thread_id,
                workspace_id: workspace_id.clone(),
                revision: 0,
                generation,
                request: ThreadCapabilityRequestState::Loading,
                snapshot: None,
            },
        );
        if owner
            .sender
            .as_ref()
            .is_none_or(|sender| sender.try_send(request.clone()).is_err())
        {
            drop(owner);
            return self.complete_thread_capability(
                &request,
                Err("Thread capability request unavailable".into()),
            );
        }
        transition
    }

    fn thread_capability_request_matches(&self, request: &CapabilityRequest) -> bool {
        !self.is_stopped()
            && self.current_auth_ticket() == request.auth_ticket
            && self
                .thread_capability_snapshot(&request.thread_id)
                .is_some_and(|p| {
                    p.generation == request.generation
                        && p.workspace_id == request.workspace_id
                        && p.request == ThreadCapabilityRequestState::Loading
                })
    }

    fn complete_thread_capability(
        &self,
        request: &CapabilityRequest,
        result: Result<AuthorizationCapabilitySnapshot, String>,
    ) -> ClientTransition {
        let mut owner = self
            .thread_capabilities
            .lock()
            .expect("thread capability owner poisoned");
        let Some(current) = owner.publications.get(&request.thread_id).filter(|p| {
            p.generation == request.generation
                && p.workspace_id == request.workspace_id
                && p.request == ThreadCapabilityRequestState::Loading
        }) else {
            return self.transition(&ClientMutationAuthority { _private: () }, vec![], vec![]);
        };
        if self.is_stopped() {
            return self.reject_intent();
        }
        let mut next = (**current).clone();
        match result {
            Ok(snapshot) => {
                next.snapshot = Some(snapshot);
                next.request = ThreadCapabilityRequestState::Ready;
            }
            Err(message) => {
                next.snapshot = None;
                next.request = ThreadCapabilityRequestState::Failed { message };
            }
        }
        self.publish_thread_capability(&mut owner, next)
    }

    pub(crate) fn invalidate_thread_capabilities(&self, thread_id: Option<&str>) {
        let mut owner = self
            .thread_capabilities
            .lock()
            .expect("thread capability owner poisoned");
        let entries = owner
            .publications
            .values()
            .filter(|p| thread_id.is_none_or(|id| id == p.thread_id))
            .cloned()
            .collect::<Vec<_>>();
        for input in entries {
            let mut next = (*input).clone();
            next.generation = owner.next_generation();
            next.snapshot = None;
            next.request = ThreadCapabilityRequestState::Cancelled;
            self.publish_thread_capability(&mut owner, next);
        }
    }

    pub(crate) fn thread_capability_demand_changed(
        &self,
        scope: &ClientScope,
        demand: ClientDemand,
    ) {
        let ClientScope::ThreadCapability { thread_id } = scope else {
            return;
        };
        if demand == ClientDemand::Suspended {
            self.invalidate_thread_capabilities(Some(thread_id));
        } else {
            self.thread_capability_intent(ThreadCapabilityIntent::Observe {
                thread_id: thread_id.clone(),
            });
        }
    }

    pub(crate) fn thread_capability_subscription_changed(&self, scope: &ClientScope, added: bool) {
        let ClientScope::ThreadCapability { thread_id } = scope else {
            return;
        };
        let mut owner = self
            .thread_capabilities
            .lock()
            .expect("thread capability owner poisoned");
        let count = owner.subscriptions.entry(thread_id.clone()).or_default();
        if added {
            *count += 1;
        } else {
            *count = count.saturating_sub(1);
        }
        let retire = *count == 0;
        if retire {
            owner.subscriptions.remove(thread_id);
        }
        drop(owner);
        if retire {
            self.invalidate_thread_capabilities(Some(thread_id));
            self.thread_capabilities
                .lock()
                .expect("thread capability owner poisoned")
                .publications
                .remove(thread_id);
        }
    }

    pub(crate) fn start_thread_capability_controller(self: &Arc<Self>) {
        let (sender, receiver) = mpsc::sync_channel::<CapabilityRequest>(64);
        let weak = Arc::downgrade(self);
        let task = std::thread::Builder::new()
            .name("client-thread-capabilities".into())
            .spawn(move || {
                while let Ok(request) = receiver.recv() {
                    let Some(core) = weak.upgrade() else {
                        return;
                    };
                    if !core.thread_capability_request_matches(&request) {
                        core.complete_thread_capability(
                            &request,
                            Err("Thread capability request cancelled".into()),
                        );
                        continue;
                    }
                    let sender = core.compatibility_runtime().ws_command_sender();
                    drop(core);
                    let result =
                        sender.authorization_capabilities(AuthorizationCapabilitiesParams {
                            workspace_id: Some(request.workspace_id.clone()),
                            thread_id: Some(request.thread_id.clone()),
                        });
                    let Some(core) = weak.upgrade() else {
                        return;
                    };
                    if !core.thread_capability_request_matches(&request) {
                        core.complete_thread_capability(
                            &request,
                            Err("Thread capability request cancelled".into()),
                        );
                        continue;
                    }
                    let result = result.and_then(|snapshot| {
                        anyhow::ensure!(
                            authorization_capability_snapshot_is_compatible(
                                &snapshot,
                                &request.principal_id,
                                Some(&request.workspace_id),
                                Some(&request.thread_id)
                            ),
                            "Gateway returned an incompatible thread capability snapshot"
                        );
                        anyhow::ensure!(
                            core.accept_authorization_projection(
                                request.auth_ticket.0,
                                request.auth_ticket.1,
                                snapshot.clone()
                            ) == AuthorizationProjectionAcceptance::Accepted,
                            "Thread capability authorization changed"
                        );
                        Ok(snapshot)
                    });
                    core.complete_thread_capability(
                        &request,
                        result.map_err(|error| format!("{error:#}")),
                    );
                }
            })
            .expect("thread capability worker could not start");
        let mut owner = self
            .thread_capabilities
            .lock()
            .expect("thread capability owner poisoned");
        owner.sender = Some(sender);
        owner.task = Some(task);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::ClientTransitionOutcome;
    fn request(core: &ClientCore, thread: &str, retry: bool) -> ClientTransition {
        core.enqueue_thread_capability(
            thread.into(),
            "workspace".into(),
            PrincipalId::new("P00000000000000000001").unwrap(),
            core.current_auth_ticket(),
            retry,
        )
    }
    #[test]
    fn persistent_failure_is_terminal_until_explicit_retry_and_rejects_old_generation() {
        let core = ClientCore::new();
        let (sender, receiver) = mpsc::sync_channel(1);
        core.thread_capabilities.lock().unwrap().sender = Some(sender);
        request(&core, "a", false);
        let first = receiver.try_recv().unwrap();
        for _ in 0..5 {
            assert_eq!(
                request(&core, "a", true).outcome(),
                ClientTransitionOutcome::Noop
            );
        }
        core.complete_thread_capability(&first, Err("persistent failure".into()));
        let failed = core.thread_capability_snapshot("a").unwrap();
        for _ in 0..20 {
            assert_eq!(
                request(&core, "a", false).outcome(),
                ClientTransitionOutcome::Noop
            );
        }
        assert!(receiver.try_recv().is_err());
        assert!(Arc::ptr_eq(
            &failed,
            &core.thread_capability_snapshot("a").unwrap()
        ));
        request(&core, "a", true);
        let retry = receiver.try_recv().unwrap();
        assert!(retry.generation > first.generation);
        let pending = core.thread_capability_snapshot("a").unwrap();
        core.complete_thread_capability(&first, Err("late error".into()));
        assert!(Arc::ptr_eq(
            &pending,
            &core.thread_capability_snapshot("a").unwrap()
        ));
        core.complete_thread_capability(&retry, Err("retry failed".into()));
        assert!(matches!(
            core.thread_capability_snapshot("a").unwrap().request,
            ThreadCapabilityRequestState::Failed { .. }
        ));
        assert!(receiver.try_recv().is_err());
    }
    #[test]
    fn scope_drop_and_access_loss_reject_late_publications_without_changing_other_threads() {
        let core = Arc::new(ClientCore::new());
        let (sender, receiver) = mpsc::sync_channel(2);
        core.thread_capabilities.lock().unwrap().sender = Some(sender);
        let subscription = core.subscribe(
            ClientScope::ThreadCapability {
                thread_id: "a".into(),
            },
            std::num::NonZeroUsize::new(8).unwrap(),
        );
        request(&core, "a", false);
        let a = receiver.try_recv().unwrap();
        request(&core, "b", false);
        let b = receiver.try_recv().unwrap();
        let before_b = core.thread_capability_snapshot("b").unwrap();
        drop(subscription);
        core.complete_thread_capability(&a, Err("late a".into()));
        assert!(core.thread_capability_snapshot("a").is_none());
        assert!(Arc::ptr_eq(
            &before_b,
            &core.thread_capability_snapshot("b").unwrap()
        ));
        core.invalidate_thread_capabilities(None);
        let cancelled = core.thread_capability_snapshot("b").unwrap();
        core.complete_thread_capability(&b, Err("late b".into()));
        assert!(Arc::ptr_eq(
            &cancelled,
            &core.thread_capability_snapshot("b").unwrap()
        ));
        assert_eq!(cancelled.request, ThreadCapabilityRequestState::Cancelled);
        assert!(cancelled.snapshot.is_none());
        core.shutdown();
        assert_eq!(
            request(&core, "b", true).outcome(),
            ClientTransitionOutcome::Rejected
        );
        assert!(core.thread_capability_snapshot("b").is_none());
    }
    #[test]
    fn unavailable_worker_is_a_bounded_failure_instead_of_a_permanent_loading_state() {
        let core = ClientCore::new();
        request(&core, "a", false);
        let before = core.thread_capability_snapshot("a").unwrap();
        assert!(matches!(
            before.request,
            ThreadCapabilityRequestState::Failed { .. }
        ));
        assert_eq!(
            request(&core, "a", false).outcome(),
            ClientTransitionOutcome::Noop
        );
        assert!(Arc::ptr_eq(
            &before,
            &core.thread_capability_snapshot("a").unwrap()
        ));
    }
}

#[cfg(any(test, feature = "test-support"))]
impl ClientMutationAuthority {
    /// Delivers a synthetic scoped response to the capability request owner.
    pub fn accept_thread_capabilities_for_test(
        &self,
        core: &ClientCore,
        snapshot: AuthorizationCapabilitySnapshot,
    ) {
        let scope = snapshot.thread.as_ref().expect("thread capability fixture");
        let mut owner = core.thread_capabilities.lock().unwrap();
        let generation = owner.next_generation();
        core.publish_thread_capability(
            &mut owner,
            ThreadCapabilityPublication {
                thread_id: scope.thread_id.clone(),
                workspace_id: scope.workspace_id.clone(),
                revision: 0,
                generation,
                request: ThreadCapabilityRequestState::Ready,
                snapshot: Some(snapshot),
            },
        );
    }
}
