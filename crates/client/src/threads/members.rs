//! Thread member, mention-directory and participant mutation ownership.

use crate::{
    composer::state_machine::{ComposerMentionCandidate, composer_workspace_mention_candidates},
    core::*,
    threads::scope::ThreadScopeAction,
};
use pioneer_protocol::*;
use std::{
    collections::HashMap,
    sync::{Arc, mpsc},
    thread::JoinHandle,
};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ThreadMemberIntent {
    Observe {
        thread_id: String,
    },
    Retry {
        thread_id: String,
    },
    Perform {
        thread_id: String,
        action: ThreadScopeAction,
    },
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ThreadMemberRequestState {
    Loading {
        action: ThreadScopeAction,
    },
    Ready,
    Failed {
        action: ThreadScopeAction,
        message: String,
    },
    Cancelled,
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ThreadMemberReadState {
    #[default]
    Idle,
    Loading,
    Ready,
    Failed {
        message: String,
    },
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct ThreadMemberPublication {
    pub thread_id: String,
    pub workspace_id: String,
    pub revision: u64,
    pub generation: u64,
    pub workspace_members: Vec<MemberSummary>,
    pub member_directory: Vec<MemberSummary>,
    pub participants: Vec<ThreadParticipantSummary>,
    pub mention_candidates: Vec<ComposerMentionCandidate>,
    pub workspace_request: ThreadMemberReadState,
    pub directory_request: ThreadMemberReadState,
    pub participants_request: ThreadMemberReadState,
    pub presentation: Option<super::scope::ThreadScopePresentation>,
    pub request: ThreadMemberRequestState,
}
impl ThreadMemberPublication {
    /// Existing member-panel disclosure and add-picker candidates, shared by shells.
    pub fn visible_members(&self, private: bool) -> Vec<MemberSummary> {
        self.workspace_members
            .iter()
            .filter(|member| {
                !private
                    || self
                        .participants
                        .iter()
                        .any(|participant| participant.principal_id == member.principal_id)
            })
            .cloned()
            .collect()
    }
    pub fn add_candidates(&self, private: bool) -> Vec<ComposerMentionCandidate> {
        if !private || self.participants_request == ThreadMemberReadState::Loading {
            return Vec::new();
        }
        crate::composer::state_machine::composer_mention_candidates(
            self.workspace_members
                .iter()
                .filter(|member| {
                    !self
                        .participants
                        .iter()
                        .any(|participant| participant.principal_id == member.principal_id)
                })
                .cloned(),
        )
    }
}
#[derive(Clone)]
struct MemberRequest {
    thread_id: String,
    workspace_id: String,
    generation: u64,
    auth_ticket: (u64, Option<u64>),
    principal_id: PrincipalId,
    action: ThreadScopeAction,
    private: bool,
    can_read_directory: bool,
}
#[derive(Clone, Default)]
struct MemberOutput {
    workspace_members: Option<Vec<MemberSummary>>,
    member_directory: Option<Vec<MemberSummary>>,
    participants: Option<Vec<ThreadParticipantSummary>>,
    thread: Option<Thread>,
    error: Option<String>,
    workspace_request: Option<ThreadMemberReadState>,
    directory_request: Option<ThreadMemberReadState>,
    participants_request: Option<ThreadMemberReadState>,
}
#[derive(Default)]
pub(crate) struct ThreadMemberController {
    generation: u64,
    publications: HashMap<String, Arc<ThreadMemberPublication>>,
    subscriptions: HashMap<String, usize>,
    contexts: HashMap<String, (bool, bool, (u64, Option<u64>))>,
    sender: Option<mpsc::SyncSender<MemberRequest>>,
    task: Option<JoinHandle<()>>,
}
impl ThreadMemberController {
    fn next_generation(&mut self) -> u64 {
        self.generation = self
            .generation
            .checked_add(1)
            .expect("thread member generation exhausted");
        self.generation
    }
    pub(crate) fn stop(&mut self) {
        self.sender.take();
        self.publications.clear();
        self.subscriptions.clear();
        self.contexts.clear();
    }
}
impl Drop for ThreadMemberController {
    fn drop(&mut self) {
        self.stop();
        if let Some(task) = self.task.take() {
            if task.thread().id() != std::thread::current().id() {
                let _ = task.join();
            }
        }
    }
}

pub fn thread_participant_summaries(
    response: ThreadParticipantsResponse,
) -> Vec<ThreadParticipantSummary> {
    if response.participants.is_empty() {
        response
            .participant_ids
            .into_iter()
            .map(|principal_id| ThreadParticipantSummary { principal_id })
            .collect()
    } else {
        response.participants
    }
}

impl ClientCore {
    pub fn thread_member_snapshot(&self, thread_id: &str) -> Option<Arc<ThreadMemberPublication>> {
        self.thread_members
            .lock()
            .expect("thread member owner poisoned")
            .publications
            .get(thread_id)
            .cloned()
    }
    fn publish_thread_members(
        &self,
        owner: &mut ThreadMemberController,
        mut next: ThreadMemberPublication,
    ) -> ClientTransition {
        let scope = ClientScope::ThreadMember {
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
            .expect("thread member revision exhausted");
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
    pub fn thread_member_intent(&self, intent: ThreadMemberIntent) -> ClientTransition {
        let (thread_id, action, retry) = match intent {
            ThreadMemberIntent::Observe { thread_id } => {
                (thread_id, ThreadScopeAction::ListParticipants, false)
            }
            ThreadMemberIntent::Retry { thread_id } => {
                (thread_id, ThreadScopeAction::ListParticipants, true)
            }
            ThreadMemberIntent::Perform { thread_id, action } => (thread_id, action, true),
        };
        if self.is_stopped() {
            return self.reject_intent();
        }
        let Some(coordinator) = self.thread_coordinator_snapshot(&thread_id) else {
            return self.transition(&ClientMutationAuthority { _private: () }, vec![], vec![]);
        };
        let Some(thread) = coordinator.thread() else {
            return self.transition(&ClientMutationAuthority { _private: () }, vec![], vec![]);
        };
        let Some(auth) = self.current_auth() else {
            return self.transition(&ClientMutationAuthority { _private: () }, vec![], vec![]);
        };
        let ticket = self.current_auth_ticket();
        if ticket.1.is_none() {
            return self.transition(&ClientMutationAuthority { _private: () }, vec![], vec![]);
        }
        let snapshot = self.authorization_snapshot(Some(&thread.workspace_id), Some(&thread_id));
        let capabilities = crate::authorization::thread_presentation_capabilities(
            snapshot
                .as_ref()
                .and_then(|s| s.thread.as_ref())
                .map(|t| &t.capabilities),
        );
        let allowed = match &action {
            ThreadScopeAction::ListParticipants => true,
            ThreadScopeAction::UpdateVisibility { .. } => capabilities.can_manage_thread,
            ThreadScopeAction::AddParticipant { .. }
            | ThreadScopeAction::RemoveParticipant { .. } => {
                thread.visibility == Some(ThreadVisibility::Private)
                    && capabilities.can_manage_private_participants
            }
        };
        if !allowed {
            return self.reject_intent();
        }
        let can_read_directory = snapshot.as_ref().is_some_and(|s| {
            crate::authorization::principal_presentation_capabilities(s).can_view_member_directory
        });
        self.enqueue_thread_member(
            MemberRequest {
                thread_id,
                workspace_id: thread.workspace_id.clone(),
                generation: 0,
                auth_ticket: ticket,
                principal_id: auth.principal.id,
                action,
                private: thread.visibility == Some(ThreadVisibility::Private),
                can_read_directory,
            },
            retry,
        )
    }
    fn enqueue_thread_member(&self, mut request: MemberRequest, retry: bool) -> ClientTransition {
        if self.is_stopped() {
            return self.reject_intent();
        }
        let mut owner = self
            .thread_members
            .lock()
            .expect("thread member owner poisoned");
        let current = owner.publications.get(&request.thread_id).cloned();
        let context = (
            request.private,
            request.can_read_directory,
            request.auth_ticket,
        );
        if current.as_ref().is_some_and(|p| {
            p.workspace_id == request.workspace_id
                && owner.contexts.get(&request.thread_id) == Some(&context)
                && ((matches!(p.request, ThreadMemberRequestState::Loading { .. })
                    && !(matches!(
                        p.request,
                        ThreadMemberRequestState::Loading {
                            action: ThreadScopeAction::ListParticipants
                        }
                    ) && !matches!(request.action, ThreadScopeAction::ListParticipants)))
                    || (!retry && p.request != ThreadMemberRequestState::Cancelled))
        }) {
            return self.transition(&ClientMutationAuthority { _private: () }, vec![], vec![]);
        }
        request.generation = owner.next_generation();
        owner.contexts.insert(request.thread_id.clone(), context);
        let mut next = current
            .as_ref()
            .filter(|p| p.workspace_id == request.workspace_id)
            .map(|p| (**p).clone())
            .unwrap_or_else(|| ThreadMemberPublication {
                thread_id: request.thread_id.clone(),
                workspace_id: request.workspace_id.clone(),
                revision: 0,
                generation: request.generation,
                workspace_members: vec![],
                member_directory: vec![],
                participants: vec![],
                mention_candidates: vec![],
                workspace_request: ThreadMemberReadState::Idle,
                directory_request: ThreadMemberReadState::Idle,
                participants_request: ThreadMemberReadState::Idle,
                presentation: None,
                request: ThreadMemberRequestState::Cancelled,
            });
        next.generation = request.generation;
        next.participants_request = ThreadMemberReadState::Loading;
        if matches!(
            request.action,
            ThreadScopeAction::ListParticipants | ThreadScopeAction::UpdateVisibility { .. }
        ) {
            next.workspace_request = ThreadMemberReadState::Loading;
            next.directory_request = ThreadMemberReadState::Loading;
        }
        next.request = ThreadMemberRequestState::Loading {
            action: request.action.clone(),
        };
        let transition = self.publish_thread_members(&mut owner, next);
        if owner
            .sender
            .as_ref()
            .is_none_or(|sender| sender.try_send(request.clone()).is_err())
        {
            drop(owner);
            return self.complete_thread_members(
                &request,
                Err("Thread member request unavailable".into()),
            );
        }
        transition
    }
    fn thread_member_request_matches(&self, request: &MemberRequest) -> bool {
        !self.is_stopped()
            && self.current_auth_ticket() == request.auth_ticket
            && self
                .thread_member_snapshot(&request.thread_id)
                .is_some_and(|p| {
                    p.generation == request.generation
                        && p.workspace_id == request.workspace_id
                        && matches!(p.request, ThreadMemberRequestState::Loading { .. })
                })
    }
    fn complete_thread_members(
        &self,
        request: &MemberRequest,
        result: Result<MemberOutput, String>,
    ) -> ClientTransition {
        self.apply_thread_member_output(request, result, true)
    }

    fn apply_thread_member_output(
        &self,
        request: &MemberRequest,
        result: Result<MemberOutput, String>,
        finished: bool,
    ) -> ClientTransition {
        let capabilities =
            self.authorization_snapshot(Some(&request.workspace_id), Some(&request.thread_id));
        let presentation_capabilities = crate::authorization::thread_presentation_capabilities(
            capabilities
                .as_ref()
                .and_then(|s| s.thread.as_ref())
                .map(|t| &t.capabilities),
        );
        // Lock the existing registry entry before the member owner. Retirement
        // invalidates the member generation before removing that entry, so a late
        // visibility response can neither recreate it nor overwrite a new request.
        let mut thread_mutation = if finished
            && result.as_ref().is_ok_and(|output| output.thread.is_some())
        {
            let Some(mutation) = self.existing_thread_mutation(&request.thread_id) else {
                return self.transition(&ClientMutationAuthority { _private: () }, vec![], vec![]);
            };
            if mutation.workspace_id != request.workspace_id {
                return self.transition(&ClientMutationAuthority { _private: () }, vec![], vec![]);
            }
            Some(mutation)
        } else {
            None
        };
        let current_thread = match thread_mutation.as_ref() {
            Some(mutation) => mutation.thread().cloned(),
            None => self
                .thread_coordinator_snapshot(&request.thread_id)
                .and_then(|c| c.thread().cloned()),
        };
        let mut owner = self
            .thread_members
            .lock()
            .expect("thread member owner poisoned");
        let Some(current) = owner.publications.get(&request.thread_id).filter(|p| {
            p.generation == request.generation
                && p.workspace_id == request.workspace_id
                && matches!(p.request, ThreadMemberRequestState::Loading { .. })
        }) else {
            return self.transition(&ClientMutationAuthority { _private: () }, vec![], vec![]);
        };
        if self.is_stopped() {
            return self.reject_intent();
        }
        let mut next = (**current).clone();
        let mut thread = None;
        match result {
            Ok(output) => {
                if let Some(state) = output.workspace_request {
                    next.workspace_request = state;
                }
                if let Some(state) = output.directory_request {
                    next.directory_request = state;
                }
                if let Some(state) = output.participants_request {
                    next.participants_request = state;
                }
                if let Some(members) = output.workspace_members {
                    next.workspace_members = members;
                }
                if let Some(members) = output.member_directory {
                    next.member_directory = members;
                }
                if let Some(participants) = output.participants {
                    next.participants = participants;
                }
                next.mention_candidates = composer_workspace_mention_candidates(
                    next.workspace_members.clone(),
                    next.member_directory.clone(),
                    Some(&request.principal_id),
                );
                if finished {
                    next.request =
                        output
                            .error
                            .map_or(ThreadMemberRequestState::Ready, |message| {
                                ThreadMemberRequestState::Failed {
                                    action: request.action.clone(),
                                    message,
                                }
                            });
                }
                next.presentation =
                    output
                        .thread
                        .as_ref()
                        .or(current_thread.as_ref())
                        .map(|thread| {
                            super::scope::thread_scope_presentation(
                                thread,
                                Some(&request.principal_id),
                                presentation_capabilities,
                                &next.participants,
                                &next.workspace_members,
                            )
                        });
                thread = output.thread;
            }
            Err(message) => {
                for state in [
                    &mut next.workspace_request,
                    &mut next.directory_request,
                    &mut next.participants_request,
                ] {
                    if *state == ThreadMemberReadState::Loading {
                        *state = ThreadMemberReadState::Failed {
                            message: message.clone(),
                        };
                    }
                }
                next.request = ThreadMemberRequestState::Failed {
                    action: request.action.clone(),
                    message,
                }
            }
        }
        let transition = self.publish_thread_members(&mut owner, next);
        if let (Some(mutation), Some(thread)) = (thread_mutation.as_mut(), thread) {
            if mutation
                .thread()
                .is_none_or(|previous| previous.updated_at <= thread.updated_at)
                && mutation.thread() != Some(&thread)
            {
                mutation.set_snapshot(thread);
            }
        }
        drop(owner);
        transition
    }
    pub(crate) fn invalidate_thread_members(&self, thread_id: Option<&str>) {
        let mut owner = self
            .thread_members
            .lock()
            .expect("thread member owner poisoned");
        let entries = owner
            .publications
            .values()
            .filter(|p| thread_id.is_none_or(|id| id == p.thread_id))
            .cloned()
            .collect::<Vec<_>>();
        for input in entries {
            let mut next = (*input).clone();
            next.generation = owner.next_generation();
            next.workspace_members.clear();
            next.member_directory.clear();
            next.participants.clear();
            next.mention_candidates.clear();
            next.workspace_request = ThreadMemberReadState::Idle;
            next.directory_request = ThreadMemberReadState::Idle;
            next.participants_request = ThreadMemberReadState::Idle;
            next.presentation = None;
            next.request = ThreadMemberRequestState::Cancelled;
            self.publish_thread_members(&mut owner, next);
        }
    }
    pub(crate) fn thread_member_demand_changed(&self, scope: &ClientScope, demand: ClientDemand) {
        let ClientScope::ThreadMember { thread_id } = scope else {
            return;
        };
        if demand == ClientDemand::Suspended {
            self.invalidate_thread_members(Some(thread_id));
        } else {
            self.thread_member_intent(ThreadMemberIntent::Observe {
                thread_id: thread_id.clone(),
            });
        }
    }
    pub(crate) fn thread_member_subscription_changed(&self, scope: &ClientScope, added: bool) {
        let ClientScope::ThreadMember { thread_id } = scope else {
            return;
        };
        let mut owner = self
            .thread_members
            .lock()
            .expect("thread member owner poisoned");
        let count = owner.subscriptions.entry(thread_id.clone()).or_default();
        if added {
            *count += 1;
        } else {
            *count = count.saturating_sub(1);
        }
        let retire = *count == 0;
        if retire {
            owner.subscriptions.remove(thread_id);
            owner.contexts.remove(thread_id);
        }
        drop(owner);
        if retire {
            self.invalidate_thread_members(Some(thread_id));
            self.thread_members
                .lock()
                .expect("thread member owner poisoned")
                .publications
                .remove(thread_id);
        }
    }
    pub(crate) fn observe_thread_member_notification(
        &self,
        notification: &GatewayNotification,
    ) -> bool {
        let affected = match notification {
            GatewayNotification::ThreadParticipantsChanged(change) => {
                Some(Some(change.thread_id.as_str()))
            }
            GatewayNotification::WorkspaceMembersChanged(_)
            | GatewayNotification::MemberChanged(_) => Some(None),
            _ => None,
        };
        let Some(thread) = affected else {
            return false;
        };
        let threads = self
            .thread_members
            .lock()
            .expect("thread member owner poisoned")
            .subscriptions
            .keys()
            .filter(|id| thread.is_none_or(|thread| id.as_str() == thread))
            .cloned()
            .collect::<Vec<_>>();
        for thread_id in threads {
            self.invalidate_thread_members(Some(&thread_id));
            self.thread_member_intent(ThreadMemberIntent::Observe { thread_id });
        }
        matches!(
            notification,
            GatewayNotification::ThreadParticipantsChanged(_)
        )
    }

    pub(crate) fn start_thread_member_controller(self: &Arc<Self>) {
        let (sender, receiver) = mpsc::sync_channel::<MemberRequest>(64);
        let weak = Arc::downgrade(self);
        let task = std::thread::Builder::new()
            .name("client-thread-members".into())
            .spawn(move || {
                while let Ok(request) = receiver.recv() {
                    let Some(core) = weak.upgrade() else {
                        return;
                    };
                    if !core.thread_member_request_matches(&request) {
                        core.complete_thread_members(
                            &request,
                            Err("Thread member request cancelled".into()),
                        );
                        continue;
                    }
                    let sender = core.compatibility_runtime().ws_command_sender();
                    drop(core);
                    let current = || {
                        weak.upgrade()
                            .is_some_and(|core| core.thread_member_request_matches(&request))
                    };
                    let result = load_thread_members(&request, &sender, current, |output| {
                        if let Some(core) = weak
                            .upgrade()
                            .filter(|core| core.thread_member_request_matches(&request))
                        {
                            core.apply_thread_member_output(&request, Ok(output), false);
                        }
                    }, |page| {
                        let core = weak.upgrade().ok_or_else(|| anyhow::anyhow!("administration_read_cancelled"))?;
                        let read = core.read_administration_page(page, true)?;
                        drop(core);
                        let publication = read.wait_while(&current)?;
                        Ok(publication.members.iter().map(|row| row.member.clone()).collect())
                    });
                    let Some(core) = weak.upgrade() else {
                        return;
                    };
                    if !core.thread_member_request_matches(&request) {
                        core.complete_thread_members(
                            &request,
                            Err("Thread member request cancelled".into()),
                        );
                        continue;
                    }
                    core.complete_thread_members(&request, result.map_err(|e| format!("{e:#}")));
                }
            })
            .expect("thread member worker could not start");
        let mut owner = self
            .thread_members
            .lock()
            .expect("thread member owner poisoned");
        owner.sender = Some(sender);
        owner.task = Some(task);
    }
}

fn load_thread_members(
    request: &MemberRequest,
    sender: &impl crate::rpc::JsonRpcRequestTransport,
    current: impl Fn() -> bool,
    progress: impl Fn(MemberOutput),
    directory: impl Fn(crate::administration::pages::AdministrationPage) -> anyhow::Result<Vec<MemberSummary>>,
) -> anyhow::Result<MemberOutput> {
    use crate::transport::ws::command_sender as commands;
    let mut output = MemberOutput::default();
    anyhow::ensure!(current(), "Thread member request cancelled");
    match &request.action {
        ThreadScopeAction::UpdateVisibility { visibility } => {
            let response = commands::thread_update(
                sender,
                ThreadUpdateParams {
                    workspace_id: request.workspace_id.clone(),
                    thread_id: request.thread_id.clone(),
                    name: None,
                    visibility: Some(*visibility),
                    archived: None,
                },
            )?;
            anyhow::ensure!(
                response.thread.id == request.thread_id
                    && response.thread.workspace_id == request.workspace_id,
                "Thread update response scope mismatch"
            );
            output.thread = Some(response.thread);
        }
        ThreadScopeAction::AddParticipant { principal_id }
        | ThreadScopeAction::RemoveParticipant { principal_id } => {
            let params = ThreadParticipantMutationParams {
                workspace_id: request.workspace_id.clone(),
                thread_id: request.thread_id.clone(),
                principal_id: principal_id.clone(),
            };
            let response = if matches!(request.action, ThreadScopeAction::AddParticipant { .. }) {
                commands::thread_participant_add(sender, params)?
            } else {
                commands::thread_participant_remove(sender, params)?
            };
            anyhow::ensure!(
                response.thread_id == request.thread_id
                    && response.workspace_id == request.workspace_id,
                "Thread participant response scope mismatch"
            );
            output.participants = Some(thread_participant_summaries(response));
            output.participants_request = Some(ThreadMemberReadState::Ready);
            return Ok(output);
        }
        ThreadScopeAction::ListParticipants => {}
    }
    anyhow::ensure!(current(), "Thread member request cancelled");
    let workspace_result = directory(crate::administration::pages::AdministrationPage::WorkspaceMembers { workspace_id: WorkspaceId::new(request.workspace_id.clone())? });
    output.workspace_request = Some(match &workspace_result {
        Ok(_) => ThreadMemberReadState::Ready,
        Err(error) => ThreadMemberReadState::Failed {
            message: format!("{error:#}"),
        },
    });
    match workspace_result {
        Ok(members) => output.workspace_members = Some(members),
        Err(error) => output.error = Some(format!("{error:#}")),
    }
    anyhow::ensure!(current(), "Thread member request cancelled");
    progress(output.clone());
    let directory_result = if request.can_read_directory { directory(crate::administration::pages::AdministrationPage::MemberDirectory) } else { Ok(Vec::new()) };
    output.directory_request = Some(match &directory_result {
        Ok(_) => ThreadMemberReadState::Ready,
        Err(error) => ThreadMemberReadState::Failed {
            message: format!("{error:#}"),
        },
    });
    match directory_result {
        Ok(members) => output.member_directory = Some(members),
        Err(error) => {
            output.error.get_or_insert_with(|| format!("{error:#}"));
        }
    }
    anyhow::ensure!(current(), "Thread member request cancelled");
    progress(output.clone());
    let participant_result: anyhow::Result<Vec<ThreadParticipantSummary>> = (|| {
        let private = output.thread.as_ref().map_or(request.private, |t| {
            t.visibility == Some(ThreadVisibility::Private)
        });
        if private {
            anyhow::ensure!(current(), "Thread member request cancelled");
            let response = commands::thread_participants_list(
                sender,
                ThreadParticipantsListParams {
                    workspace_id: request.workspace_id.clone(),
                    thread_id: request.thread_id.clone(),
                },
            )?;
            anyhow::ensure!(
                response.thread_id == request.thread_id
                    && response.workspace_id == request.workspace_id,
                "Thread participant response scope mismatch"
            );
            return Ok(thread_participant_summaries(response));
        }
        Ok(vec![])
    })();
    output.participants_request = Some(match &participant_result {
        Ok(_) => ThreadMemberReadState::Ready,
        Err(error) => ThreadMemberReadState::Failed {
            message: format!("{error:#}"),
        },
    });
    match participant_result {
        Ok(participants) => output.participants = Some(participants),
        Err(error) => {
            output.error.get_or_insert_with(|| format!("{error:#}"));
        }
    }
    anyhow::ensure!(current(), "Thread member request cancelled");
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::{Cell, RefCell};
    fn request(thread: &str) -> MemberRequest {
        MemberRequest {
            thread_id: thread.into(),
            workspace_id: "WAAAAAAAAAAAAAAAAAAAA".into(),
            generation: 0,
            auth_ticket: (0, None),
            principal_id: PrincipalId::new("PAAAAAAAAAAAAAAAAAAAA").unwrap(),
            action: ThreadScopeAction::ListParticipants,
            private: true,
            can_read_directory: true,
        }
    }
    fn member() -> MemberSummary {
        MemberSummary {
            principal_id: PrincipalId::new("PBBBBBBBBBBBBBBBBBBBB").unwrap(),
            kind: PrincipalKind::User,
            display_name: "Workspace Member".into(),
            nickname: "member".into(),
            role_key: None,
            role: AuthorizationRolePresentation {
                key: "member".into(),
                display_name: "Member".into(),
                description: String::new(),
                built_in: true,
            },
            lifecycle_managed: true,
            status: PrincipalStatus::Active,
            avatar_revision: None,
        }
    }
    struct Transport<'a> {
        requests: RefCell<Vec<String>>,
        on_workspace: Box<dyn Fn() + 'a>,
        repeated_cursor: bool,
    }
    impl crate::rpc::JsonRpcRequestTransport for Transport<'_> {
        fn send_json_rpc_request(
            &self,
            _: String,
            payload: String,
            reply: crate::rpc::JsonRpcResponseSender,
        ) -> Result<(), String> {
            let payload: serde_json::Value = serde_json::from_str(&payload).unwrap();
            let method = payload["method"].as_str().unwrap().to_owned();
            self.requests.borrow_mut().push(method.clone());
            let response = match method.as_str() {
                "workspace/member/list" => {
                    (self.on_workspace)();
                    serde_json::json!({"workspace_id": "WAAAAAAAAAAAAAAAAAAAA", "members": [member()], "next_cursor": self.repeated_cursor.then_some("same")})
                }
                "member/list" => return Err("synthetic directory failure".into()),
                "thread/participants/list" => {
                    serde_json::json!({"workspace_id": "WAAAAAAAAAAAAAAAAAAAA", "thread_id": "a", "participant_ids": [member().principal_id]})
                }
                method => panic!("unexpected request: {method}"),
            };
            reply.send(Ok(response)).map_err(|e| e.to_string())
        }
    }
    fn thread(id: &str) -> Thread {
        Thread {
            workspace_id: "WAAAAAAAAAAAAAAAAAAAA".into(),
            id: id.into(),
            name: None,
            preview: String::new(),
            preview_author: None,
            mode: ThreadMode::Chat,
            model: "model".into(),
            model_provider: "provider".into(),
            reasoning_effort: None,
            created_at: 1,
            updated_at: 2,
            status: ThreadStatus::Idle,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            visibility: Some(ThreadVisibility::Private),
            turns: vec![],
        }
    }

    #[test]
    fn visibility_completion_updates_only_a_retained_matching_request() {
        let core = ClientCore::new();
        let (sender, receiver) = mpsc::sync_channel(2);
        core.thread_members.lock().unwrap().sender = Some(sender);
        core.upsert_thread(thread("a"));
        let mut action = request("a");
        action.action = ThreadScopeAction::UpdateVisibility {
            visibility: ThreadVisibility::Workspace,
        };
        core.enqueue_thread_member(action.clone(), true);
        let initial = receiver.try_recv().unwrap();
        let changed = Thread {
            visibility: Some(ThreadVisibility::Workspace),
            updated_at: 3,
            ..thread("a")
        };
        core.complete_thread_members(
            &initial,
            Ok(MemberOutput {
                thread: Some(changed.clone()),
                ..Default::default()
            }),
        );
        assert_eq!(
            core.thread_coordinator_snapshot("a").unwrap().thread(),
            Some(&changed)
        );
        core.enqueue_thread_member(action, true);
        let retired = receiver.try_recv().unwrap();
        core.remove_thread_store("a");
        core.complete_thread_members(
            &retired,
            Ok(MemberOutput {
                thread: Some(changed.clone()),
                ..Default::default()
            }),
        );
        assert!(core.thread_coordinator_snapshot("a").is_none());
        core.upsert_thread(thread("a"));
        core.complete_thread_members(
            &retired,
            Ok(MemberOutput {
                thread: Some(changed),
                ..Default::default()
            }),
        );
        assert_eq!(
            core.thread_coordinator_snapshot("a").unwrap().thread(),
            Some(&thread("a"))
        );
    }

    #[test]
    fn mention_candidates_normalize_names_and_keep_domain_identity() {
        let first = MemberSummary {
            nickname: "  member  ".into(),
            ..member()
        };
        let blank = MemberSummary {
            principal_id: PrincipalId::new("PCCCCCCCCCCCCCCCCCCCC").unwrap(),
            nickname: "  ".into(),
            ..member()
        };
        let duplicate = MemberSummary {
            display_name: "Duplicate".into(),
            ..first.clone()
        };
        let candidates =
            composer_workspace_mention_candidates([first.clone(), blank, duplicate], [], None);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].principal_id, first.principal_id);
        assert_eq!(candidates[0].nickname, "member");
        assert_eq!(candidates[0].display_name, first.display_name);
    }

    #[test]
    fn directory_failure_preserves_workspace_mentions_and_explicit_retry_is_bounded() {
        let core = ClientCore::new();
        let (sender, receiver) = mpsc::sync_channel(1);
        core.thread_members.lock().unwrap().sender = Some(sender);
        core.enqueue_thread_member(request("a"), false);
        let initial = receiver.try_recv().unwrap();
        let transport = Transport {
            requests: Default::default(),
            on_workspace: Box::new(|| {}),
            repeated_cursor: false,
        };
        let output = load_thread_members(
            &initial,
            &transport,
            || true,
            |output| {
                core.apply_thread_member_output(&initial, Ok(output), false);
                let input = core.thread_member_snapshot("a").unwrap();
                assert_eq!(input.workspace_request, ThreadMemberReadState::Ready);
                assert_eq!(
                    input.mention_candidates[0].principal_id,
                    member().principal_id
                );
                assert!(matches!(
                    input.request,
                    ThreadMemberRequestState::Loading { .. }
                ));
            },
            |page| match page { crate::administration::pages::AdministrationPage::WorkspaceMembers { .. } => Ok(vec![member()]), _ => Err(anyhow::anyhow!("synthetic directory failure")) },
        )
        .unwrap();
        assert_eq!(&*transport.requests.borrow(), &["thread/participants/list"]);
        core.complete_thread_members(&initial, Ok(output));
        let input = core.thread_member_snapshot("a").unwrap();
        assert_eq!(input.workspace_members, vec![member()]);
        assert_eq!(input.participants[0].principal_id, member().principal_id);
        assert_eq!(
            input.mention_candidates[0].principal_id,
            member().principal_id
        );
        assert!(matches!(
            input.request,
            ThreadMemberRequestState::Failed { .. }
        ));
        for _ in 0..20 {
            core.enqueue_thread_member(request("a"), false);
        }
        assert!(receiver.try_recv().is_err());
        assert!(Arc::ptr_eq(
            &input,
            &core.thread_member_snapshot("a").unwrap()
        ));
        core.enqueue_thread_member(request("a"), true);
        let retry = receiver.try_recv().unwrap();
        assert!(retry.generation > initial.generation);
        let pending = core.thread_member_snapshot("a").unwrap();
        core.complete_thread_members(&initial, Err("late failure".into()));
        assert!(Arc::ptr_eq(
            &pending,
            &core.thread_member_snapshot("a").unwrap()
        ));
    }
    #[test]
    fn cancelled_directory_read_does_not_publish_or_request_participants() {
        let active = Cell::new(true);
        let transport = Transport { requests: Default::default(), on_workspace: Box::new(|| {}), repeated_cursor: false };
        assert!(load_thread_members(&request("a"), &transport, || active.get(), |_| panic!("cancelled progress"), |_| { active.set(false); Ok(vec![member()]) }).is_err());
        assert!(transport.requests.borrow().is_empty());
    }
    #[test]
    fn drop_and_access_loss_fence_old_thread_completion_and_policy_change_gets_a_new_generation() {
        let core = Arc::new(ClientCore::new());
        let subscription = core.subscribe(
            ClientScope::ThreadMember {
                thread_id: "a".into(),
            },
            std::num::NonZeroUsize::new(8).unwrap(),
        );
        let (sender, receiver) = mpsc::sync_channel(2);
        core.thread_members.lock().unwrap().sender = Some(sender);
        core.enqueue_thread_member(request("a"), false);
        let a = receiver.try_recv().unwrap();
        core.enqueue_thread_member(request("b"), false);
        let b = receiver.try_recv().unwrap();
        let before_b = core.thread_member_snapshot("b").unwrap();
        drop(subscription);
        core.complete_thread_members(&a, Ok(MemberOutput::default()));
        assert!(core.thread_member_snapshot("a").is_none());
        assert!(Arc::ptr_eq(
            &before_b,
            &core.thread_member_snapshot("b").unwrap()
        ));
        let mut changed_policy = request("b");
        changed_policy.can_read_directory = false;
        core.enqueue_thread_member(changed_policy, false);
        let new_b = receiver.try_recv().unwrap();
        assert!(new_b.generation > b.generation);
        core.invalidate_thread_members(None);
        let cancelled = core.thread_member_snapshot("b").unwrap();
        core.complete_thread_members(&new_b, Ok(MemberOutput::default()));
        assert!(Arc::ptr_eq(
            &cancelled,
            &core.thread_member_snapshot("b").unwrap()
        ));
        assert_eq!(cancelled.request, ThreadMemberRequestState::Cancelled);
        assert!(cancelled.workspace_members.is_empty());
    }
}
