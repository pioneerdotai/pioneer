//! Scoped member and invitation pages owned by the process-local Client.
use super::{AdministrationEvent, AdministrationEventTracker, InvitationListRow, MemberListRow};
use crate::{authorization::principal_presentation_capabilities, core::*};
use pioneer_protocol::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, mpsc},
};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AdministrationPage {
    Members,
    MemberDirectory,
    Invitations,
    WorkspaceMembers { workspace_id: WorkspaceId },
}
impl AdministrationPage {
    fn workspace(&self) -> Option<&str> {
        match self {
            Self::WorkspaceMembers { workspace_id } => Some(workspace_id.as_str()),
            _ => None,
        }
    }
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AdministrationPageIntent {
    Observe { page: AdministrationPage },
    Release { page: AdministrationPage },
    Refresh { page: AdministrationPage },
    Next { page: AdministrationPage },
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AdministrationLoadState {
    #[default]
    Idle,
    Loading,
    Ready,
    Failed,
    Forbidden,
    Cancelled,
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct AdministrationMemberRow {
    pub id: PrincipalId,
    pub revision: u64,
    pub member: MemberSummary,
    pub presentation: MemberListRow,
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct AdministrationInvitationRow {
    pub id: InvitationId,
    pub revision: u64,
    pub invitation: InvitationSummary,
    pub presentation: InvitationListRow,
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct AdministrationPagePublication {
    pub page: AdministrationPage,
    pub revision: u64,
    pub members: Vec<Arc<AdministrationMemberRow>>,
    pub invitations: Vec<Arc<AdministrationInvitationRow>>,
    pub next_cursor: Option<String>,
    pub request_cursor: Option<String>,
    pub request: AdministrationLoadState,
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct PageRequest {
    page: AdministrationPage,
    generation: u64,
    epoch: (u64, u64, Option<u64>),
    cursor: Option<String>,
}
struct PageState {
    publication: Arc<AdministrationPagePublication>,
    request: Option<PageRequest>,
    demand: usize,
    cursors: BTreeSet<String>,
    readers: BTreeMap<u64, mpsc::SyncSender<()>>,
}
#[derive(Default)]
pub(crate) struct AdministrationStore {
    pages: BTreeMap<AdministrationPage, PageState>,
    generation: u64,
    read_generation: u64,
    events: AdministrationEventTracker,
    sender: Option<mpsc::SyncSender<PageRequest>>,
    task: Option<std::thread::JoinHandle<()>>,
}
impl AdministrationStore {
    pub(crate) fn stop(&mut self) {
        self.sender.take();
        self.pages.clear();
    }
}
impl Drop for AdministrationStore {
    fn drop(&mut self) {
        self.stop();
        if let Some(task) = self.task.take() {
            if task.thread().id() != std::thread::current().id() {
                let _ = task.join();
            }
        }
    }
}
enum PageResult {
    Members(MemberListResponse),
    Invitations(InvitationListResponse),
    WorkspaceMembers(WorkspaceMemberListResponse),
}
impl ClientCore {
    #[cfg(test)]
    pub(crate) fn complete_administration_members_for_test(&self, members: Vec<MemberSummary>) {
        let (sender, receiver) = mpsc::sync_channel(64);
        self.administration_store.lock().unwrap().sender = Some(sender);
        self.administration_page_intent(AdministrationPageIntent::Observe {
            page: AdministrationPage::Members,
        });
        let request = receiver.try_recv().unwrap();
        self.complete_administration_page(
            &request,
            Ok(PageResult::Members(MemberListResponse {
                members,
                next_cursor: None,
            })),
        );
    }
    pub(crate) fn resume_administration_demand(&self) {
        let pages: Vec<_> = self
            .administration_store
            .lock()
            .expect("administration store poisoned")
            .pages
            .iter()
            .filter(|(_, state)| {
                state.demand > 0
                    && state.request.is_none()
                    && matches!(
                        state.publication.request,
                        AdministrationLoadState::Idle
                            | AdministrationLoadState::Forbidden
                            | AdministrationLoadState::Cancelled
                    )
            })
            .map(|(page, _)| page.clone())
            .collect();
        for page in pages {
            if self.administration_page_allowed(&page) {
                self.administration_page_intent(AdministrationPageIntent::Refresh { page });
            }
        }
    }
    pub(crate) fn refresh_administration_member_pages(&self) {
        let pages: Vec<_> = self
            .administration_store
            .lock()
            .expect("administration store poisoned")
            .pages
            .iter()
            .filter(|(page, state)| {
                state.demand > 0 && !matches!(page, AdministrationPage::Invitations)
            })
            .map(|(page, _)| page.clone())
            .collect();
        for page in pages {
            self.administration_page_intent(AdministrationPageIntent::Refresh { page });
        }
    }
    pub fn administration_workspace_pages(&self) -> Vec<Arc<AdministrationPagePublication>> {
        self.administration_store
            .lock()
            .expect("administration store poisoned")
            .pages
            .iter()
            .filter(|(page, _)| matches!(page, AdministrationPage::WorkspaceMembers { .. }))
            .map(|(_, state)| state.publication.clone())
            .collect()
    }
    pub(crate) fn administration_epoch(&self) -> (u64, u64, Option<u64>) {
        self.snapshot(&ClientScope::Administration { workspace_id: None })
            .and_then(|p| p.snapshot().payload::<crate::gateway::identity_authorization::IdentityAuthorizationPublication>())
            .map_or((0, 0, None), |p| (p.connection_generation, p.authorization_change_sequence, p.connection_id))
    }
    pub fn administration_page_snapshot(
        &self,
        page: &AdministrationPage,
    ) -> Option<Arc<AdministrationPagePublication>> {
        self.snapshot(&ClientScope::AdministrationPage { page: page.clone() })
            .and_then(|p| p.snapshot().payload())
    }
    fn administration_page_allowed(&self, page: &AdministrationPage) -> bool {
        self.authorization_snapshot(page.workspace(), None)
            .or_else(|| self.authorization_snapshot(None, None))
            .is_some_and(|p| {
                let capabilities = principal_presentation_capabilities(&p);
                match page {
                    AdministrationPage::Invitations => capabilities.can_view_invitations,
                    AdministrationPage::WorkspaceMembers { .. } => p
                        .workspace
                        .as_ref()
                        .map_or(capabilities.can_view_member_directory, |workspace| {
                            workspace.capabilities.can_list_members
                        }),
                    _ => capabilities.can_view_member_directory,
                }
            })
    }
    fn publish_administration_page(
        &self,
        state: &mut PageState,
        mut next: AdministrationPagePublication,
    ) -> ClientTransition {
        next.revision = state.publication.revision;
        if next == *state.publication {
            return self.navigation_outcome(ClientTransitionOutcome::Noop);
        }
        let scope = ClientScope::AdministrationPage {
            page: next.page.clone(),
        };
        next.revision = next
            .revision
            .max(
                self.snapshot(&scope)
                    .map_or(0, |p| p.revisions().scoped().get()),
            )
            .checked_add(1)
            .expect("administration page revision exhausted");
        state.publication = Arc::new(next);
        for reader in state.readers.values() {
            let _ = reader.try_send(());
        }
        self.publish(
            &ClientMutationAuthority { _private: () },
            scope,
            crate::threads::registry::revisions(state.publication.revision),
            state.publication.clone(),
            vec![],
        )
    }
    pub fn administration_page_intent(&self, intent: AdministrationPageIntent) -> ClientTransition {
        let (page, observe, release, append) = match intent {
            AdministrationPageIntent::Observe { page } => (page, true, false, false),
            AdministrationPageIntent::Release { page } => (page, false, true, false),
            AdministrationPageIntent::Refresh { page } => (page, false, false, false),
            AdministrationPageIntent::Next { page } => (page, false, false, true),
        };
        let allowed = self.administration_page_allowed(&page);
        let epoch = self.administration_epoch();
        let mut owner = self
            .administration_store
            .lock()
            .expect("administration store poisoned");
        if self.is_stopped() {
            return self.reject_intent();
        }
        if release && !owner.pages.contains_key(&page) {
            return self.navigation_outcome(ClientTransitionOutcome::Noop);
        }
        let state = owner
            .pages
            .entry(page.clone())
            .or_insert_with(|| PageState {
                publication: Arc::new(AdministrationPagePublication {
                    page: page.clone(),
                    revision: 0,
                    members: vec![],
                    invitations: vec![],
                    next_cursor: None,
                    request_cursor: None,
                    request: AdministrationLoadState::Idle,
                }),
                request: None,
                demand: 0,
                cursors: BTreeSet::new(),
                readers: BTreeMap::new(),
            });
        if observe {
            state.demand = state
                .demand
                .checked_add(1)
                .expect("administration demand exhausted");
        }
        if release {
            state.demand = state.demand.saturating_sub(1);
            if state.demand == 0 {
                self.cancel_administration_page_operations(&page);
            }
            if state.demand > 0 || state.request.is_none() {
                return self.navigation_outcome(ClientTransitionOutcome::Noop);
            }
            state.request = None;
            let mut next = (*state.publication).clone();
            next.request_cursor = None;
            next.request = AdministrationLoadState::Cancelled;
            return self.publish_administration_page(state, next);
        }
        if !allowed {
            state.request = None;
            state.cursors.clear();
            let mut next = (*state.publication).clone();
            next.members.clear();
            next.invitations.clear();
            next.next_cursor = None;
            next.request_cursor = None;
            next.request = AdministrationLoadState::Forbidden;
            return self.publish_administration_page(state, next);
        }
        if state.request.is_some()
            || (observe
                && matches!(
                    state.publication.request,
                    AdministrationLoadState::Ready | AdministrationLoadState::Failed
                ))
        {
            return self.navigation_outcome(ClientTransitionOutcome::Noop);
        }
        if append && state.demand == 0 {
            return self.navigation_outcome(ClientTransitionOutcome::Noop);
        }
        let cursor = if append {
            let Some(cursor) = state.publication.next_cursor.clone() else {
                return self.navigation_outcome(ClientTransitionOutcome::Noop);
            };
            Some(cursor)
        } else {
            None
        };
        owner.generation = owner
            .generation
            .checked_add(1)
            .expect("administration request generation exhausted");
        let request = PageRequest {
            page: page.clone(),
            generation: owner.generation,
            epoch,
            cursor,
        };
        let sender = owner.sender.clone();
        let state = owner.pages.get_mut(&page).unwrap();
        state.request = Some(request.clone());
        let mut next = (*state.publication).clone();
        next.request_cursor = request.cursor.clone();
        next.request = AdministrationLoadState::Loading;
        let transition = self.publish_administration_page(state, next);
        drop(owner);
        if sender.is_some_and(|sender| sender.try_send(request.clone()).is_ok()) {
            return transition;
        }
        self.complete_administration_page(&request, Err(()))
    }
    fn complete_administration_page(
        &self,
        request: &PageRequest,
        result: Result<PageResult, ()>,
    ) -> ClientTransition {
        let epoch = self.administration_epoch();
        let authorization = self
            .authorization_snapshot(request.page.workspace(), None)
            .or_else(|| self.authorization_snapshot(None, None));
        let principal = authorization
            .as_ref()
            .map(|snapshot| snapshot.principal_id.clone());
        let capabilities = authorization.map(|p| principal_presentation_capabilities(&p));
        let allowed = self.administration_page_allowed(&request.page);
        let mut owner = self
            .administration_store
            .lock()
            .expect("administration store poisoned");
        let Some(state) = owner.pages.get_mut(&request.page) else {
            return self.navigation_outcome(ClientTransitionOutcome::Noop);
        };
        if self.is_stopped() || epoch != request.epoch || state.request.as_ref() != Some(request) {
            return self.navigation_outcome(ClientTransitionOutcome::Noop);
        }
        if !allowed {
            state.request = None;
            state.cursors.clear();
            let mut next = (*state.publication).clone();
            next.members.clear();
            next.invitations.clear();
            next.next_cursor = None;
            next.request_cursor = None;
            next.request = AdministrationLoadState::Forbidden;
            return self.publish_administration_page(state, next);
        }
        let capabilities = capabilities.unwrap();
        let mut next = (*state.publication).clone();
        let page = match (result, &request.page) {
            (
                Ok(PageResult::Members(page)),
                AdministrationPage::Members | AdministrationPage::MemberDirectory,
            ) => Some((page.members, vec![], page.next_cursor)),
            (Ok(PageResult::Invitations(page)), AdministrationPage::Invitations) => {
                Some((vec![], page.invitations, page.next_cursor))
            }
            (
                Ok(PageResult::WorkspaceMembers(page)),
                AdministrationPage::WorkspaceMembers { workspace_id },
            ) if &page.workspace_id == workspace_id => {
                Some((page.members, vec![], page.next_cursor))
            }
            (Ok(_), _) => {
                state.request = None;
                state.cursors.clear();
                next.members.clear();
                next.invitations.clear();
                next.next_cursor = None;
                next.request_cursor = None;
                next.request = AdministrationLoadState::Forbidden;
                return self.publish_administration_page(state, next);
            }
            (Err(()), _) => None,
        };
        state.request = None;
        if let Some((members, invitations, cursor)) = page {
            if cursor.as_ref().is_some_and(|c| {
                Some(c) == request.cursor.as_ref()
                    || (request.cursor.is_some() && state.cursors.contains(c))
            }) {
                next.request = AdministrationLoadState::Failed;
                return self.publish_administration_page(state, next);
            }
            if request.cursor.is_none() {
                state.cursors.clear();
            }
            if let Some(cursor) = &request.cursor {
                state.cursors.insert(cursor.clone());
            }
            if request.cursor.is_none() {
                next.members.clear();
                next.invitations.clear();
            }
            for member in members {
                let previous = state
                    .publication
                    .members
                    .iter()
                    .find(|row| row.id == member.principal_id);
                let presentation = super::member_list_row(
                    &member,
                    principal.as_ref(),
                    capabilities,
                    matches!(request.page, AdministrationPage::WorkspaceMembers { .. }),
                );
                let row = if let Some(previous) =
                    previous.filter(|row| row.member == member && row.presentation == presentation)
                {
                    previous.clone()
                } else {
                    Arc::new(AdministrationMemberRow {
                        id: member.principal_id.clone(),
                        revision: previous.map_or(1, |row| {
                            row.revision
                                .checked_add(1)
                                .expect("member revision exhausted")
                        }),
                        member,
                        presentation,
                    })
                };
                if let Some(ix) = next.members.iter().position(|old| old.id == row.id) {
                    next.members[ix] = row;
                } else {
                    next.members.push(row);
                }
            }
            for invitation in invitations {
                let previous = state
                    .publication
                    .invitations
                    .iter()
                    .find(|row| row.id == invitation.invitation_id);
                let presentation = super::invitation_list_row(&invitation, capabilities);
                let row = if let Some(previous) = previous
                    .filter(|row| row.invitation == invitation && row.presentation == presentation)
                {
                    previous.clone()
                } else {
                    Arc::new(AdministrationInvitationRow {
                        id: invitation.invitation_id.clone(),
                        revision: previous.map_or(1, |row| {
                            row.revision
                                .checked_add(1)
                                .expect("invitation revision exhausted")
                        }),
                        invitation,
                        presentation,
                    })
                };
                if let Some(ix) = next.invitations.iter().position(|old| old.id == row.id) {
                    next.invitations[ix] = row;
                } else {
                    next.invitations.push(row);
                }
            }
            next.next_cursor = cursor;
            next.request = AdministrationLoadState::Ready;
        } else {
            next.request = AdministrationLoadState::Failed;
        }
        next.request_cursor = None;
        let continuation = matches!(
            request.page,
            AdministrationPage::WorkspaceMembers { .. } | AdministrationPage::MemberDirectory
        ) && state.demand > 0
            && next.next_cursor.is_some()
            && next.request == AdministrationLoadState::Ready;
        let transition = self.publish_administration_page(state, next);
        drop(owner);
        if continuation {
            self.administration_page_intent(AdministrationPageIntent::Next {
                page: request.page.clone(),
            });
        }
        transition
    }
    pub(crate) fn invalidate_administration(&self) {
        let mut owner = self
            .administration_store
            .lock()
            .expect("administration store poisoned");
        owner.events = AdministrationEventTracker::default();
        for state in owner.pages.values_mut() {
            state.request = None;
            state.cursors.clear();
            let mut next = (*state.publication).clone();
            next.members.clear();
            next.invitations.clear();
            next.next_cursor = None;
            next.request_cursor = None;
            next.request = AdministrationLoadState::Cancelled;
            state.publication = Arc::new(next);
        }
    }
    pub(crate) fn observe_administration_notification(&self, notification: &GatewayNotification) {
        let event = match notification {
            GatewayNotification::InvitationChanged(n) => {
                AdministrationEvent::InvitationChanged(n.clone())
            }
            GatewayNotification::MemberChanged(n) => AdministrationEvent::MemberChanged(n.clone()),
            GatewayNotification::WorkspaceMembersChanged(n) => {
                AdministrationEvent::WorkspaceMembersChanged(n.clone())
            }
            _ => return,
        };
        let mut owner = self
            .administration_store
            .lock()
            .expect("administration store poisoned");
        if !owner.events.apply_event(&event).apply {
            return;
        }
        let mut reload = Vec::new();
        for (page, state) in &mut owner.pages {
            let mut next = (*state.publication).clone();
            let affected = match (&event, page) {
                (AdministrationEvent::InvitationChanged(n), AdministrationPage::Invitations) => {
                    next.invitations.retain(|r| r.id != n.invitation_id);
                    true
                }
                (
                    AdministrationEvent::MemberChanged(n),
                    AdministrationPage::Members
                    | AdministrationPage::MemberDirectory
                    | AdministrationPage::WorkspaceMembers { .. },
                ) => {
                    next.members.retain(|r| r.id != n.principal_id);
                    true
                }
                (
                    AdministrationEvent::WorkspaceMembersChanged(_),
                    AdministrationPage::Members | AdministrationPage::MemberDirectory,
                ) => {
                    next.members.clear();
                    true
                }
                (
                    AdministrationEvent::WorkspaceMembersChanged(n),
                    AdministrationPage::WorkspaceMembers { workspace_id },
                ) if &n.workspace_id == workspace_id => {
                    next.members.clear();
                    true
                }
                _ => false,
            };
            if affected {
                state.request = None;
                state.cursors.clear();
                next.next_cursor = None;
                next.request = AdministrationLoadState::Idle;
                self.publish_administration_page(state, next);
                if state.demand > 0 {
                    reload.push(page.clone());
                }
            }
        }
        drop(owner);
        for page in reload {
            self.administration_page_intent(AdministrationPageIntent::Refresh { page });
        }
    }
    pub(crate) fn start_administration_controller(self: &Arc<Self>) {
        let (sender, receiver) = mpsc::sync_channel::<PageRequest>(64);
        let weak = Arc::downgrade(self);
        let task = std::thread::Builder::new()
            .name("client-administration-pages".into())
            .spawn(move || {
                while let Ok(request) = receiver.recv() {
                    let Some(core) = weak.upgrade() else {
                        return;
                    };
                    let current = core.administration_epoch() == request.epoch
                        && !core.is_stopped()
                        && core
                            .administration_store
                            .lock()
                            .expect("administration store poisoned")
                            .pages
                            .get(&request.page)
                            .is_some_and(|state| state.request.as_ref() == Some(&request));
                    if !current {
                        continue;
                    }
                    let sender = core.compatibility_runtime().ws_command_sender();
                    drop(core);
                    let result = match &request.page {
                        AdministrationPage::Members | AdministrationPage::MemberDirectory => sender
                            .member_list(MemberListParams {
                                cursor: request.cursor.clone(),
                                limit: Some(50),
                            })
                            .map(PageResult::Members),
                        AdministrationPage::Invitations => sender
                            .invitation_list(InvitationListParams {
                                cursor: request.cursor.clone(),
                                limit: Some(50),
                                ..Default::default()
                            })
                            .map(PageResult::Invitations),
                        AdministrationPage::WorkspaceMembers { workspace_id } => sender
                            .workspace_member_list(WorkspaceMemberListParams {
                                workspace_id: workspace_id.clone(),
                                cursor: request.cursor.clone(),
                                limit: Some(100),
                            })
                            .map(PageResult::WorkspaceMembers),
                    }
                    .map_err(|_| ());
                    let Some(core) = weak.upgrade() else {
                        return;
                    };
                    core.complete_administration_page(&request, result);
                }
            })
            .expect("administration page worker");
        let mut owner = self
            .administration_store
            .lock()
            .expect("administration store poisoned");
        owner.sender = Some(sender);
        owner.task = Some(task);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> (ClientCore, mpsc::Receiver<PageRequest>) {
        let core = ClientCore::new();
        let role = AuthorizationRolePresentation {
            key: "member".into(),
            display_name: "Member".into(),
            description: String::new(),
            built_in: false,
        };
        let accepted = core.accept_authorization_projection(
            0,
            None,
            AuthorizationCapabilitySnapshot {
                schema_version: AUTHORIZATION_CAPABILITY_SNAPSHOT_SCHEMA_VERSION,
                authorization_revision: 1,
                principal_id: PrincipalId::new("PAAAAAAAAAAAAAAAAAAAA").unwrap(),
                role_key: "member".into(),
                role,
                global: AuthorizationGlobalCapabilities {
                    can_view_member_directory: true,
                    can_view_invitations: true,
                    ..Default::default()
                },
                workspace: None,
                thread: None,
            },
        );
        assert_eq!(
            accepted,
            crate::authorization::AuthorizationProjectionAcceptance::Accepted
        );
        let (sender, receiver) = mpsc::sync_channel(64);
        core.administration_store.lock().unwrap().sender = Some(sender);
        (core, receiver)
    }
    fn member(id: &str) -> MemberSummary {
        serde_json::from_value(
            serde_json::json!({"principal_id":id,"kind":"user","display_name":id,"nickname":id,
            "role":{"key":"member","display_name":"Member","description":"","built_in":false},
            "lifecycle_managed":true,"status":"active"}),
        )
        .unwrap()
    }
    fn start(core: &ClientCore, receiver: &mpsc::Receiver<PageRequest>) -> PageRequest {
        core.administration_page_intent(AdministrationPageIntent::Observe {
            page: AdministrationPage::Members,
        });
        receiver.try_recv().unwrap()
    }
    fn members(ids: &[&str], cursor: Option<&str>) -> Result<PageResult, ()> {
        Ok(PageResult::Members(MemberListResponse {
            members: ids.iter().map(|id| member(id)).collect(),
            next_cursor: cursor.map(str::to_owned),
        }))
    }
    #[test]
    fn directory_traversal_is_client_owned_and_last_reader_cancels_the_next_page() {
        let (core, rx) = fixture();
        let core = Arc::new(core);
        let page = AdministrationPage::MemberDirectory;
        let read = core.read_administration_page(page.clone(), false).unwrap();
        let first = rx.try_recv().unwrap();
        core.complete_administration_page(
            &first,
            members(&["PBBBBBBBBBBBBBBBBBBBB"], Some("next")),
        );
        let second = rx.try_recv().unwrap();
        assert_eq!(second.cursor.as_deref(), Some("next"));
        let first_row = core.administration_page_snapshot(&page).unwrap().members[0].clone();
        drop(read);
        let cancelled = core.administration_page_snapshot(&page).unwrap();
        assert_eq!(cancelled.request, AdministrationLoadState::Cancelled);
        core.complete_administration_page(&second, members(&["PCCCCCCCCCCCCCCCCCCCC"], None));
        assert!(Arc::ptr_eq(
            &cancelled,
            &core.administration_page_snapshot(&page).unwrap()
        ));
        assert!(Arc::ptr_eq(&first_row, &cancelled.members[0]));
        assert!(rx.try_recv().is_err());
    }
    #[test]
    fn paging_deduplicates_by_identity_and_reuses_equal_rows_after_replace_and_reorder() {
        let (core, rx) = fixture();
        let first = start(&core, &rx);
        core.complete_administration_page(
            &first,
            members(&["PBBBBBBBBBBBBBBBBBBBB"], Some("cursor")),
        );
        let initial = core
            .administration_page_snapshot(&AdministrationPage::Members)
            .unwrap();
        core.administration_page_intent(AdministrationPageIntent::Next {
            page: AdministrationPage::Members,
        });
        let next = rx.try_recv().unwrap();
        assert_eq!(next.cursor.as_deref(), Some("cursor"));
        core.complete_administration_page(
            &next,
            members(&["PBBBBBBBBBBBBBBBBBBBB", "PCCCCCCCCCCCCCCCCCCCC"], None),
        );
        let appended = core
            .administration_page_snapshot(&AdministrationPage::Members)
            .unwrap();
        assert_eq!(appended.members.len(), 2);
        assert!(Arc::ptr_eq(&initial.members[0], &appended.members[0]));
        core.administration_page_intent(AdministrationPageIntent::Refresh {
            page: AdministrationPage::Members,
        });
        core.complete_administration_page(
            &rx.try_recv().unwrap(),
            members(&["PCCCCCCCCCCCCCCCCCCCC", "PBBBBBBBBBBBBBBBBBBBB"], None),
        );
        let reordered = core
            .administration_page_snapshot(&AdministrationPage::Members)
            .unwrap();
        assert!(Arc::ptr_eq(&appended.members[0], &reordered.members[1]));
        let duplicate =
            core.complete_administration_page(&first, members(&["PAAAAAAAAAAAAAAAAAAAA"], None));
        assert!(duplicate.changes().publications().is_empty());
        assert!(Arc::ptr_eq(
            &reordered,
            &core
                .administration_page_snapshot(&AdministrationPage::Members)
                .unwrap()
        ));
    }
    #[test]
    fn failed_pages_remain_explicit_until_retry_and_repeated_cursors_fail() {
        let (core, rx) = fixture();
        let work = start(&core, &rx);
        core.complete_administration_page(&work, Err(()));
        let failed = core
            .administration_page_snapshot(&AdministrationPage::Members)
            .unwrap();
        assert_eq!(failed.request, AdministrationLoadState::Failed);
        core.administration_page_intent(AdministrationPageIntent::Observe {
            page: AdministrationPage::Members,
        });
        assert!(rx.try_recv().is_err());
        assert!(Arc::ptr_eq(
            &failed,
            &core
                .administration_page_snapshot(&AdministrationPage::Members)
                .unwrap()
        ));
        core.administration_page_intent(AdministrationPageIntent::Refresh {
            page: AdministrationPage::Members,
        });
        core.complete_administration_page(&rx.try_recv().unwrap(), members(&[], Some("same")));
        core.administration_page_intent(AdministrationPageIntent::Next {
            page: AdministrationPage::Members,
        });
        core.complete_administration_page(&rx.try_recv().unwrap(), members(&[], Some("same")));
        assert_eq!(
            core.administration_page_snapshot(&AdministrationPage::Members)
                .unwrap()
                .request,
            AdministrationLoadState::Failed
        );
    }
    #[test]
    fn last_release_and_access_fence_reject_late_pages() {
        for revoke in [false, true] {
            let (core, rx) = fixture();
            let work = start(&core, &rx);
            if revoke {
                core.clear_authorization_projections();
            } else {
                core.administration_page_intent(AdministrationPageIntent::Release {
                    page: AdministrationPage::Members,
                });
            }
            let before = core.administration_page_snapshot(&AdministrationPage::Members);
            let late =
                core.complete_administration_page(&work, members(&["PBBBBBBBBBBBBBBBBBBBB"], None));
            assert!(late.changes().publications().is_empty());
            assert_eq!(
                before,
                core.administration_page_snapshot(&AdministrationPage::Members)
            );
        }
    }
    #[test]
    fn member_events_are_applied_once_and_invalidate_an_in_flight_cursor() {
        let (core, rx) = fixture();
        let work = start(&core, &rx);
        core.complete_administration_page(&work, members(&["PBBBBBBBBBBBBBBBBBBBB"], Some("old")));
        core.administration_page_intent(AdministrationPageIntent::Next {
            page: AdministrationPage::Members,
        });
        let stale = rx.try_recv().unwrap();
        let event = GatewayNotification::MemberChanged(MemberChangedNotification {
            revision: 2,
            principal_id: PrincipalId::new("PBBBBBBBBBBBBBBBBBBBB").unwrap(),
        });
        core.observe_administration_notification(&event);
        let refreshed = core
            .administration_page_snapshot(&AdministrationPage::Members)
            .unwrap();
        assert!(refreshed.members.is_empty());
        core.observe_administration_notification(&event);
        assert!(Arc::ptr_eq(
            &refreshed,
            &core
                .administration_page_snapshot(&AdministrationPage::Members)
                .unwrap()
        ));
        assert!(
            core.complete_administration_page(&stale, members(&["PBBBBBBBBBBBBBBBBBBBB"], None))
                .changes()
                .publications()
                .is_empty()
        );
        assert_ne!(stale.generation, rx.try_recv().unwrap().generation);
    }
}

/// A background read of an administration-owned page. The Client controller,
/// including its full-directory scope, owns all cursor traversal.
pub struct AdministrationRead {
    core: std::sync::Weak<ClientCore>,
    page: AdministrationPage,
    identity: u64,
    receiver: mpsc::Receiver<()>,
}
impl AdministrationRead {
    pub fn wait(self) -> anyhow::Result<Arc<AdministrationPagePublication>> {
        self.wait_while(|| true)
    }
    pub(crate) fn wait_while(
        &self,
        current: impl Fn() -> bool,
    ) -> anyhow::Result<Arc<AdministrationPagePublication>> {
        loop {
            anyhow::ensure!(current(), "administration_read_cancelled");
            let core = self
                .core
                .upgrade()
                .ok_or_else(|| anyhow::anyhow!("administration_read_cancelled"))?;
            anyhow::ensure!(!core.is_stopped(), "administration_read_cancelled");
            let snapshot = core
                .administration_page_snapshot(&self.page)
                .ok_or_else(|| anyhow::anyhow!("administration_read_cancelled"))?;
            drop(core);
            match snapshot.request {
                AdministrationLoadState::Ready
                    if snapshot.next_cursor.is_none()
                        || !matches!(
                            self.page,
                            AdministrationPage::MemberDirectory
                                | AdministrationPage::WorkspaceMembers { .. }
                        ) =>
                {
                    return Ok(snapshot);
                }
                AdministrationLoadState::Failed
                | AdministrationLoadState::Forbidden
                | AdministrationLoadState::Cancelled => {
                    anyhow::bail!("administration_page_unavailable")
                }
                _ => {}
            }
            match self
                .receiver
                .recv_timeout(std::time::Duration::from_millis(100))
            {
                Ok(()) | Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    anyhow::bail!("administration_read_cancelled")
                }
            }
        }
    }
}
impl Drop for AdministrationRead {
    fn drop(&mut self) {
        if let Some(core) = self.core.upgrade() {
            if let Some(state) = core
                .administration_store
                .lock()
                .expect("administration store poisoned")
                .pages
                .get_mut(&self.page)
            {
                state.readers.remove(&self.identity);
            }
            core.administration_page_intent(AdministrationPageIntent::Release {
                page: self.page.clone(),
            });
        }
    }
}
impl ClientCore {
    pub fn read_administration_page(
        self: &Arc<Self>,
        page: AdministrationPage,
        retry: bool,
    ) -> anyhow::Result<AdministrationRead> {
        self.administration_page_intent(AdministrationPageIntent::Observe { page: page.clone() });
        if retry {
            self.administration_page_intent(AdministrationPageIntent::Refresh {
                page: page.clone(),
            });
        }
        let mut owner = self
            .administration_store
            .lock()
            .expect("administration store poisoned");
        owner.read_generation = owner
            .read_generation
            .checked_add(1)
            .expect("administration read identity exhausted");
        let identity = owner.read_generation;
        let (sender, receiver) = mpsc::sync_channel(1);
        let Some(state) = owner.pages.get_mut(&page) else {
            anyhow::bail!("administration_page_unavailable")
        };
        if state.readers.len() >= 64 {
            state.demand = state.demand.saturating_sub(1);
            anyhow::bail!("administration_read_overloaded");
        }
        state.readers.insert(identity, sender);
        Ok(AdministrationRead {
            core: Arc::downgrade(self),
            page,
            identity,
            receiver,
        })
    }
}
