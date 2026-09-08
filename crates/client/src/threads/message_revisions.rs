//! Message revision history requests shared by the thread dialog and native route.
use crate::{
    core::{ClientCore, ClientDemand, ClientMutationAuthority, ClientScope, ClientTransition},
    timeline::rows::{MessageRevisionPagePresentation, project_message_revision_page},
};
use std::{
    collections::BTreeMap,
    sync::{Arc, mpsc},
};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MessageRevisionIdentity {
    pub thread_id: String,
    pub turn_id: String,
    pub generation: u64,
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageRevisionReadState {
    Loading,
    Ready,
    Failed,
    Cancelled,
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct MessageRevisionPublication {
    pub identity: MessageRevisionIdentity,
    pub revision: u64,
    pub request_generation: u64,
    pub state: MessageRevisionReadState,
    pub page: Option<MessageRevisionPagePresentation>,
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MessageRevisionIntent {
    Open { thread_id: String, turn_id: String },
    More { identity: MessageRevisionIdentity },
    Retry { identity: MessageRevisionIdentity },
    Close { identity: MessageRevisionIdentity },
}
#[derive(Clone)]
struct RevisionRequest {
    identity: MessageRevisionIdentity,
    request_generation: u64,
    workspace_id: String,
    cursor: Option<String>,
    ticket: (u64, Option<u64>),
    incarnation: Option<super::registry::ThreadOperationToken>,
}
#[derive(Default)]
pub(crate) struct MessageRevisionController {
    generation: u64,
    publications: BTreeMap<String, Arc<MessageRevisionPublication>>,
    subscriptions: BTreeMap<String, usize>,
    sender: Option<mpsc::SyncSender<RevisionRequest>>,
    task: Option<std::thread::JoinHandle<()>>,
}
impl MessageRevisionController {
    fn generation(&mut self) -> u64 {
        self.generation = self
            .generation
            .checked_add(1)
            .expect("message revision generation exhausted");
        self.generation
    }
    pub(crate) fn stop(&mut self) {
        self.sender.take();
        self.publications.clear();
    }
}
impl Drop for MessageRevisionController {
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
    pub fn message_revision_snapshot(
        &self,
        thread_id: &str,
    ) -> Option<Arc<MessageRevisionPublication>> {
        self.message_revisions
            .lock()
            .expect("message revisions poisoned")
            .publications
            .get(thread_id)
            .cloned()
    }
    fn publish_message_revisions(
        &self,
        owner: &mut MessageRevisionController,
        mut next: MessageRevisionPublication,
    ) -> ClientTransition {
        let scope = ClientScope::MessageRevisions {
            thread_id: next.identity.thread_id.clone(),
        };
        next.revision = owner
            .publications
            .get(&next.identity.thread_id)
            .map(|p| p.revision)
            .unwrap_or_else(|| {
                self.snapshot(&scope)
                    .map_or(0, |p| p.revisions().scoped().get())
            })
            .checked_add(1)
            .expect("message revision publication exhausted");
        let revision = next.revision;
        let next = Arc::new(next);
        owner
            .publications
            .insert(next.identity.thread_id.clone(), next.clone());
        self.publish(
            &ClientMutationAuthority { _private: () },
            scope,
            super::registry::revisions(revision),
            next,
            vec![],
        )
    }
    pub fn message_revision_intent(&self, intent: MessageRevisionIntent) -> ClientTransition {
        if self.is_stopped() {
            return self.reject_intent();
        }
        let thread_id = match &intent {
            MessageRevisionIntent::Open { thread_id, .. } => thread_id.clone(),
            MessageRevisionIntent::More { identity }
            | MessageRevisionIntent::Retry { identity }
            | MessageRevisionIntent::Close { identity } => identity.thread_id.clone(),
        };
        let coordinator = self.thread_coordinator_snapshot(&thread_id);
        let ticket = self.current_auth_ticket();
        let incarnation = self.thread_operation_token(&thread_id);
        let mut owner = self
            .message_revisions
            .lock()
            .expect("message revisions poisoned");
        let current = owner.publications.get(&thread_id).cloned();
        let mut next = match intent {
            MessageRevisionIntent::Open { turn_id, .. } => {
                if thread_id.is_empty()
                    || turn_id.is_empty()
                    || coordinator.is_none()
                    || ticket.1.is_none()
                {
                    return self.reject_intent();
                }
                if current.as_ref().is_some_and(|p| {
                    p.identity.turn_id == turn_id && p.state == MessageRevisionReadState::Loading
                }) {
                    return self.transition(
                        &ClientMutationAuthority { _private: () },
                        vec![],
                        vec![],
                    );
                }
                if current.is_none() && owner.publications.len() >= 64 {
                    let removable = owner
                        .publications
                        .iter()
                        .find(|(id, p)| {
                            p.state != MessageRevisionReadState::Loading
                                && !owner.subscriptions.contains_key(*id)
                        })
                        .map(|(id, _)| id.clone());
                    if let Some(id) = removable {
                        owner.publications.remove(&id);
                    } else {
                        return self.reject_intent();
                    }
                }
                MessageRevisionPublication {
                    identity: MessageRevisionIdentity {
                        thread_id: thread_id.clone(),
                        turn_id,
                        generation: owner.generation(),
                    },
                    revision: 0,
                    request_generation: 0,
                    state: MessageRevisionReadState::Loading,
                    page: None,
                }
            }
            MessageRevisionIntent::Close { identity } => {
                let Some(current) = current.filter(|p| {
                    p.identity == identity && p.state != MessageRevisionReadState::Cancelled
                }) else {
                    return self.transition(
                        &ClientMutationAuthority { _private: () },
                        vec![],
                        vec![],
                    );
                };
                let mut next = (*current).clone();
                next.state = MessageRevisionReadState::Cancelled;
                next.page = None;
                return self.publish_message_revisions(&mut owner, next);
            }
            MessageRevisionIntent::More { identity }
            | MessageRevisionIntent::Retry { identity } => {
                let Some(current) = current.filter(|p| {
                    p.identity == identity
                        && (p.state == MessageRevisionReadState::Failed
                            || (p.state == MessageRevisionReadState::Ready
                                && p.page
                                    .as_ref()
                                    .is_some_and(|page| page.next_cursor.is_some())))
                }) else {
                    return self.transition(
                        &ClientMutationAuthority { _private: () },
                        vec![],
                        vec![],
                    );
                };
                if ticket.1.is_none() || coordinator.is_none() {
                    return self.reject_intent();
                }
                (*current).clone()
            }
        };
        next.request_generation = owner.generation();
        next.state = MessageRevisionReadState::Loading;
        let request = RevisionRequest {
            identity: next.identity.clone(),
            request_generation: next.request_generation,
            workspace_id: coordinator.expect("validated thread").workspace_id.clone(),
            cursor: next.page.as_ref().and_then(|page| page.next_cursor.clone()),
            ticket,
            incarnation,
        };
        let transition = self.publish_message_revisions(&mut owner, next);
        let sender = owner.sender.clone();
        drop(owner);
        if sender.is_none_or(|sender| sender.try_send(request.clone()).is_err()) {
            self.complete_message_revisions(&request, Err(()));
        }
        transition
    }
    fn revision_request_current(&self, request: &RevisionRequest) -> bool {
        !self.is_stopped()
            && self.current_auth_ticket() == request.ticket
            && self.thread_operation_token(&request.identity.thread_id) == request.incarnation
            && self
                .message_revision_snapshot(&request.identity.thread_id)
                .is_some_and(|p| {
                    p.identity == request.identity
                        && p.request_generation == request.request_generation
                        && p.state == MessageRevisionReadState::Loading
                })
    }
    fn complete_message_revisions(
        &self,
        request: &RevisionRequest,
        result: Result<MessageRevisionPagePresentation, ()>,
    ) -> bool {
        let current_authority = self.revision_request_current(request);
        let mut owner = self
            .message_revisions
            .lock()
            .expect("message revisions poisoned");
        let Some(current) = owner
            .publications
            .get(&request.identity.thread_id)
            .filter(|p| {
                p.identity == request.identity
                    && p.request_generation == request.request_generation
                    && p.state == MessageRevisionReadState::Loading
            })
            .cloned()
        else {
            return false;
        };
        let mut next = (*current).clone();
        if !current_authority {
            next.state = MessageRevisionReadState::Cancelled;
            next.page = None;
            self.publish_message_revisions(&mut owner, next);
            return true;
        }
        match result {
            Ok(mut page)
                if page.thread_id == request.identity.thread_id
                    && page.turn_id == request.identity.turn_id
                    && page.workspace_id == request.workspace_id
                    && page.revisions.len() <= 50
                    && (page.next_cursor.is_none() || page.next_cursor != request.cursor) =>
            {
                if request.cursor.is_some() {
                    if let Some(previous) = &next.page {
                        let mut revisions = previous.revisions.clone();
                        for revision in page.revisions {
                            if !revisions
                                .iter()
                                .any(|old| old.revision == revision.revision)
                            {
                                revisions.push(revision);
                            }
                        }
                        page.revisions = revisions;
                    }
                }
                next.page = Some(page);
                next.state = MessageRevisionReadState::Ready;
            }
            _ => next.state = MessageRevisionReadState::Failed,
        }
        self.publish_message_revisions(&mut owner, next);
        true
    }
    pub(crate) fn invalidate_message_revisions(&self, thread: Option<&str>) {
        let mut owner = self
            .message_revisions
            .lock()
            .expect("message revisions poisoned");
        let inputs = owner
            .publications
            .values()
            .filter(|p| thread.is_none_or(|id| id == p.identity.thread_id))
            .cloned()
            .collect::<Vec<_>>();
        for current in inputs {
            if current.state == MessageRevisionReadState::Cancelled && current.page.is_none() {
                continue;
            }
            let mut next = (*current).clone();
            next.page = None;
            next.state = MessageRevisionReadState::Cancelled;
            self.publish_message_revisions(&mut owner, next);
        }
    }
    pub(crate) fn message_revision_subscription_changed(&self, scope: &ClientScope, added: bool) {
        let ClientScope::MessageRevisions { thread_id } = scope else {
            return;
        };
        let mut owner = self
            .message_revisions
            .lock()
            .expect("message revisions poisoned");
        if added {
            *owner.subscriptions.entry(thread_id.clone()).or_default() += 1;
        } else if let Some(count) = owner.subscriptions.get_mut(thread_id) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                owner.subscriptions.remove(thread_id);
                drop(owner);
                self.invalidate_message_revisions(Some(thread_id));
            }
        }
    }
    pub(crate) fn message_revision_demand_changed(
        &self,
        scope: &ClientScope,
        demand: ClientDemand,
    ) {
        if let ClientScope::MessageRevisions { thread_id } = scope {
            if demand == ClientDemand::Suspended {
                self.invalidate_message_revisions(Some(thread_id));
            }
        }
    }
    pub(crate) fn start_message_revision_controller(self: &Arc<Self>) {
        let (sender, receiver) = mpsc::sync_channel::<RevisionRequest>(64);
        let weak = Arc::downgrade(self);
        let task = std::thread::Builder::new()
            .name("client-message-revisions".into())
            .spawn(move || {
                while let Ok(request) = receiver.recv() {
                    let Some(core) = weak.upgrade() else {
                        return;
                    };
                    if !core.revision_request_current(&request) {
                        core.complete_message_revisions(&request, Err(()));
                        continue;
                    }
                    let sender = core.compatibility_runtime().ws_command_sender();
                    let transport = sender
                        .requests_for_connection(request.ticket.1.expect("authenticated request"));
                    drop(core);
                    let result = crate::transport::ws::command_sender::turn_message_revisions_page(
                        &transport,
                        pioneer_protocol::TurnMessageRevisionsPageParams {
                            thread_id: request.identity.thread_id.clone(),
                            turn_id: request.identity.turn_id.clone(),
                            cursor: request.cursor.clone(),
                            limit: Some(50),
                        },
                    )
                    .map(project_message_revision_page)
                    .map_err(|_| ());
                    if let Some(core) = weak.upgrade() {
                        core.complete_message_revisions(&request, result);
                    }
                }
            })
            .expect("message revision worker");
        let mut owner = self
            .message_revisions
            .lock()
            .expect("message revisions poisoned");
        owner.sender = Some(sender);
        owner.task = Some(task);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn pending(
        core: &ClientCore,
        thread: &str,
        generation: u64,
        cursor: Option<&str>,
    ) -> RevisionRequest {
        let identity = MessageRevisionIdentity {
            thread_id: thread.into(),
            turn_id: "turn".into(),
            generation,
        };
        let request = RevisionRequest {
            identity: identity.clone(),
            request_generation: generation + 1,
            workspace_id: "ws".into(),
            cursor: cursor.map(str::to_owned),
            ticket: core.current_auth_ticket(),
            incarnation: core.thread_operation_token(thread),
        };
        let mut owner = core.message_revisions.lock().unwrap();
        let page = owner.publications.get(thread).and_then(|p| p.page.clone());
        core.publish_message_revisions(
            &mut owner,
            MessageRevisionPublication {
                identity,
                revision: 0,
                request_generation: generation + 1,
                state: MessageRevisionReadState::Loading,
                page,
            },
        );
        request
    }
    fn page(thread: &str, revision: u64, cursor: Option<&str>) -> MessageRevisionPagePresentation {
        MessageRevisionPagePresentation {
            workspace_id: "ws".into(),
            thread_id: thread.into(),
            turn_id: "turn".into(),
            revisions: vec![crate::timeline::rows::MessageRevisionPresentation {
                revision,
                change_kind: pioneer_protocol::TurnMessageRevisionChangeKind::Edit,
                changed_by: pioneer_protocol::PersistedActorRef::System,
                created_at: 1,
                text: Some("synthetic".into()),
                mentions: vec![],
                content_redacted: false,
            }],
            next_cursor: cursor.map(str::to_owned),
        }
    }
    #[test]
    fn matching_history_completion_is_scoped_and_duplicate_is_noop() {
        let core = ClientCore::new();
        let a = pending(&core, "a", 1, None);
        pending(&core, "b", 5, None);
        let b = core.message_revision_snapshot("b").unwrap();
        assert!(core.complete_message_revisions(&a, Ok(page("a", 3, Some("more")))));
        let accepted = core.message_revision_snapshot("a").unwrap();
        assert!(!core.complete_message_revisions(&a, Ok(page("a", 2, None))));
        assert!(Arc::ptr_eq(
            &accepted,
            &core.message_revision_snapshot("a").unwrap()
        ));
        assert!(Arc::ptr_eq(
            &b,
            &core.message_revision_snapshot("b").unwrap()
        ));
    }
    #[test]
    fn paging_deduplicates_revision_ids_and_rejects_repeated_cursor() {
        let core = ClientCore::new();
        let first = pending(&core, "a", 1, None);
        core.complete_message_revisions(&first, Ok(page("a", 3, Some("more"))));
        let next = pending(&core, "a", 3, Some("more"));
        let mut response = page("a", 3, Some("last"));
        response.revisions.extend(page("a", 2, None).revisions);
        core.complete_message_revisions(&next, Ok(response));
        assert_eq!(
            core.message_revision_snapshot("a")
                .unwrap()
                .page
                .as_ref()
                .unwrap()
                .revisions
                .iter()
                .map(|r| r.revision)
                .collect::<Vec<_>>(),
            vec![3, 2]
        );
        let last = pending(&core, "a", 5, Some("last"));
        core.complete_message_revisions(&last, Ok(page("a", 1, Some("last"))));
        let failed = core.message_revision_snapshot("a").unwrap();
        assert_eq!(failed.state, MessageRevisionReadState::Failed);
        assert_eq!(failed.page.as_ref().unwrap().revisions.len(), 2);
    }
    #[test]
    fn wrong_candidate_closed_and_replaced_request_cannot_publish() {
        let core = ClientCore::new();
        let old = pending(&core, "a", 1, None);
        let newer = pending(&core, "a", 3, None);
        let before = core.message_revision_snapshot("a").unwrap();
        assert!(!core.complete_message_revisions(&old, Ok(page("a", 1, None))));
        let mut wrong = newer.clone();
        wrong.identity.turn_id = "other".into();
        assert!(!core.complete_message_revisions(&wrong, Ok(page("a", 1, None))));
        assert!(Arc::ptr_eq(
            &before,
            &core.message_revision_snapshot("a").unwrap()
        ));
        core.message_revision_intent(MessageRevisionIntent::Close {
            identity: newer.identity.clone(),
        });
        assert!(!core.complete_message_revisions(&newer, Ok(page("a", 1, None))));
        assert_eq!(
            core.message_revision_snapshot("a").unwrap().state,
            MessageRevisionReadState::Cancelled
        );
    }
    #[test]
    fn malformed_scope_or_oversized_response_is_a_bounded_failure() {
        let core = ClientCore::new();
        let request = pending(&core, "a", 1, None);
        core.complete_message_revisions(&request, Ok(page("b", 1, None)));
        assert_eq!(
            core.message_revision_snapshot("a").unwrap().state,
            MessageRevisionReadState::Failed
        );
        let request = pending(&core, "a", 3, None);
        let mut response = page("a", 1, None);
        response.revisions = (0..51)
            .map(|revision| page("a", revision, None).revisions.remove(0))
            .collect();
        core.complete_message_revisions(&request, Ok(response));
        assert_eq!(
            core.message_revision_snapshot("a").unwrap().state,
            MessageRevisionReadState::Failed
        );
        assert!(core.message_revision_snapshot("a").unwrap().page.is_none());
    }
    #[test]
    fn unsubscription_and_access_loss_clear_history_and_reject_inflight_results() {
        let core = ClientCore::new();
        let scope = ClientScope::MessageRevisions {
            thread_id: "a".into(),
        };
        core.message_revision_subscription_changed(&scope, true);
        let old = pending(&core, "a", 1, None);
        core.message_revision_subscription_changed(&scope, false);
        assert!(!core.complete_message_revisions(&old, Ok(page("a", 1, None))));
        let current = pending(&core, "a", 3, None);
        core.invalidate_message_revisions(None);
        assert!(!core.complete_message_revisions(&current, Ok(page("a", 1, None))));
        assert!(core.message_revision_snapshot("a").unwrap().page.is_none());
    }
}
