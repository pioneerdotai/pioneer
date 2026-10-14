//! Process-local thread ownership. A transaction can mutate only its named thread.

use super::{coordinator::ThreadCoordinator, start::ThreadStartCoordinator};
use crate::{
    cli_runtime::approvals::{PendingRequest, PendingRequestRegistry, PendingRequestsReduction},
    core::{
        ClientCore, ClientDemand, ClientMutationAuthority, ClientPublicationDraft, ClientRevisions,
        ClientScope, ContentRevision, DomainRevision, PresentationRevision, ScopedRevision,
    },
    timeline::semantic::{
        SemanticTimelineRequestAction, SemanticTimelineRequestKey, SemanticTimelineState,
    },
};
use pioneer_protocol::{CLIRuntimeThreadBinding, GatewayNotification, Thread};
use serde::Serialize;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    ops::{Deref, DerefMut},
    sync::{Arc, MutexGuard},
};

const INACTIVE_THREAD_LIMIT: usize = 32;

/// Immutable source snapshot; the contained coordinator has no mutable shell handle.
pub struct ThreadDomainSnapshot {
    thread_id: String,
    draft_thread_id: Option<String>,
    coordinator: Arc<ThreadCoordinator>,
    current_principal_id: Option<String>,
    semantic: Arc<SemanticTimelineState>,
    pending: Arc<Vec<PendingRequest>>,
    cli_binding: Option<CLIRuntimeThreadBinding>,
    revision: u64,
    timeline_revision: u64,
    cache_patch: crate::timeline::semantic::SemanticTimelineCachePatch,
    execution_revision: u64,
    placement: Option<pioneer_protocol::ThreadPlacement>,
    subscription_failed: bool,
}

fn same_coordinator_content(left: &ThreadCoordinator, right: &ThreadCoordinator) -> bool {
    left.workspace_id == right.workspace_id
        && left.thread() == right.thread()
        && left.last_known_turn() == right.last_known_turn()
        && left
            .conversation
            .projection()
            .same_content(right.conversation.projection())
        && left.resume == right.resume
        && left.history_loaded == right.history_loaded
        && left.history_loading == right.history_loading
}

impl PartialEq for ThreadDomainSnapshot {
    fn eq(&self, other: &Self) -> bool {
        self.thread_id == other.thread_id
            && self.draft_thread_id == other.draft_thread_id
            && self.revision == other.revision
            && self.timeline_revision == other.timeline_revision
            && self.current_principal_id == other.current_principal_id
            && (Arc::ptr_eq(&self.coordinator, &other.coordinator)
                || same_coordinator_content(&self.coordinator, &other.coordinator))
            && self.semantic == other.semantic
            && self.pending == other.pending
            && self.cli_binding == other.cli_binding
            && self.execution_revision == other.execution_revision
            && self.cache_patch == other.cache_patch
            && self.placement == other.placement
            && self.subscription_failed == other.subscription_failed
    }
}

impl ThreadDomainSnapshot {
    pub fn current_principal_id(&self) -> Option<&str> {
        self.current_principal_id.as_deref()
    }
    pub fn coordinator(&self) -> Arc<ThreadCoordinator> {
        self.coordinator.clone()
    }
    pub fn semantic(&self) -> Arc<SemanticTimelineState> {
        self.semantic.clone()
    }
    pub fn pending(&self) -> &[PendingRequest] {
        &self.pending
    }
    pub fn cli_binding(&self) -> Option<&CLIRuntimeThreadBinding> {
        self.cli_binding.as_ref()
    }
    pub fn subscription_failed(&self) -> bool {
        self.subscription_failed
    }
    pub fn placement(&self) -> Option<&pioneer_protocol::ThreadPlacement> {
        self.placement.as_ref()
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn timeline_revision(&self) -> u64 {
        self.timeline_revision
    }
    pub fn semantic_cache_patch(&self) -> crate::timeline::semantic::SemanticTimelineCachePatch {
        self.cache_patch.clone()
    }
}

impl Serialize for ThreadDomainSnapshot {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Source<'a> {
            thread_id: &'a str,
            draft_thread_id: Option<&'a str>,
            domain_revision: u64,
            timeline_revision: u64,
            active_turn_security_summary: Option<&'a crate::security::ClientTurnSecuritySummary>,
            active_turn_security_diagnostics: Vec<crate::security::ClientSecurityDiagnosticRow>,
            current_principal_id: Option<&'a str>,
            thread: Option<&'a Thread>,
            workspace_id: &'a str,
            projection: &'a crate::conversation::ConversationViewState,
            semantic: &'a SemanticTimelineState,
            pending_requests: &'a [PendingRequest],
            history_loaded: bool,
            history_loading: bool,
            cli_binding: Option<&'a CLIRuntimeThreadBinding>,
            execution_revision: u64,
            placement: Option<&'a pioneer_protocol::ThreadPlacement>,
            subscription_failed: bool,
        }
        let summary = self
            .coordinator
            .conversation
            .projection()
            .in_flight_turn_id
            .as_deref()
            .and_then(|id| {
                self.coordinator
                    .conversation
                    .projection()
                    .turn_security_summary(id)
            });
        Source {
            thread_id: &self.thread_id,
            draft_thread_id: self.draft_thread_id.as_deref(),
            domain_revision: self.revision,
            timeline_revision: self.timeline_revision,
            active_turn_security_summary: summary,
            active_turn_security_diagnostics: summary
                .map(crate::security::security_diagnostic_rows)
                .unwrap_or_default(),
            current_principal_id: self.current_principal_id.as_deref(),
            thread: self.coordinator.thread(),
            workspace_id: &self.coordinator.workspace_id,
            projection: self.coordinator.conversation.projection(),
            semantic: &self.semantic,
            pending_requests: &self.pending,
            history_loaded: self.coordinator.history_loaded,
            history_loading: self.coordinator.history_loading,
            cli_binding: self.cli_binding.as_ref(),
            execution_revision: self.execution_revision,
            placement: self.placement.as_ref(),
            subscription_failed: self.subscription_failed,
        }
        .serialize(serializer)
    }
}

/// Sidebar changes are independent of conversation and timeline content.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct SidebarSummaryChanged {
    thread_id: String,
    workspace_id: String,
    thread: Option<Thread>,
    pub placement: Option<pioneer_protocol::ThreadPlacement>,
}

fn confirm_admission_page(
    conversation: &mut crate::conversation::Conversation,
    page: &pioneer_protocol::ThreadTimelinePageResponse,
) {
    conversation.confirm_admission_timeline_blocks(page.blocks.iter().filter(|block| {
        block.workspace_id == page.workspace_id && block.thread_id == page.thread_id
    }));
}

struct ThreadDomainStore {
    coordinator: ThreadCoordinator,
    semantic: SemanticTimelineState,
    cli_binding: Option<CLIRuntimeThreadBinding>,
    snapshot: Option<Arc<ThreadDomainSnapshot>>,
    summary_revision: u64,
    presentation: Option<Arc<crate::timeline::presentation::ThreadPresentationSnapshot>>,
    presentation_blocked: bool,
    demand: ClientDemand,
    subscriptions: usize,
    last_used: u64,
    generation: u64,
    subscription_failed: bool,
}

impl ThreadDomainStore {
    fn set_snapshot(&mut self, thread: Thread) {
        if self.coordinator.thread() == Some(&thread) {
            return;
        }
        crate::timeline::semantic::apply_thread_context_to_semantic_timeline(
            &mut self.semantic,
            &thread,
        );
        self.coordinator.set_snapshot(thread);
    }

    fn new(id: &str, workspace: &str) -> Self {
        Self {
            coordinator: ThreadCoordinator::pending(id, workspace),
            semantic: Default::default(),
            cli_binding: None,
            snapshot: None,
            summary_revision: 0,
            presentation: None,
            presentation_blocked: false,
            demand: ClientDemand::Suspended,
            subscriptions: 0,
            last_used: 0,
            generation: 0,
            subscription_failed: false,
        }
    }
}

/// One owner for thread lifecycle, navigation mappings, and per-thread stores.
#[derive(Default)]
pub struct ThreadRegistry {
    pub(crate) timeline: crate::timeline::controller::ThreadTimelineController,
    current_principal_id: Option<String>,
    stores: HashMap<String, ThreadDomainStore>,
    pub(crate) directory: crate::workspaces::directory::ThreadDirectoryStore,
    revisions: HashMap<String, (u64, u64, u64)>,
    retired: HashSet<String>,
    retired_timelines: HashSet<String>,
    presentation_revisions: HashMap<String, u64>,
    subscription_counts: HashMap<String, usize>,
    demands: HashMap<ClientScope, ClientDemand>,
    timeline_subscription_counts: HashMap<String, usize>,
    pending_requests: PendingRequestRegistry,
    pub(crate) navigation: crate::navigation::ClientNavigationState,
    pub(crate) navigation_revision: u64,
    pub(crate) navigation_publication: Option<Arc<crate::navigation::ClientNavigationState>>,
    start: ThreadStartCoordinator,
    start_generation: u64,
    start_requested: bool,
    ready_resume: VecDeque<String>,
    ready_resume_set: HashSet<String>,
    clock: u64,
    pub(crate) session_revision: u64,
    invalidation_revision: Option<u64>,
    last_access: Option<(
        pioneer_protocol::AccessChangedNotification,
        crate::authorization::AccessChangedPlan,
    )>,
    last_policy: Option<pioneer_protocol::AuthorizationProjectionChangedNotification>,
}

impl ThreadRegistry {
    pub(crate) fn timeline_has_unread(&self, thread: &str) -> bool {
        self.directory.has_unread(thread)
    }
    pub(crate) fn timeline_revision_matches(&self, thread: &str, revision: u64) -> bool {
        self.stores.get(thread).is_some_and(|store| {
            !store.presentation_blocked
                && store
                    .snapshot
                    .as_ref()
                    .is_some_and(|snapshot| snapshot.timeline_revision() == revision)
        })
    }
    pub(crate) fn timeline_workspace_matches(&self, thread: &str, workspace: &str) -> bool {
        self.stores
            .get(thread)
            .is_some_and(|store| store.coordinator.workspace_id == workspace)
    }
}

impl ThreadRegistry {
    fn require(&mut self, id: &str, workspace: &str) -> bool {
        if id.is_empty() || workspace.is_empty() {
            return false;
        }
        if self
            .directory
            .threads
            .get(id)
            .is_some_and(|thread| thread.workspace_id != workspace)
        {
            return false;
        }
        if self
            .stores
            .get(id)
            .is_some_and(|store| store.coordinator.workspace_id != workspace)
        {
            return false;
        }
        self.clock = self.clock.saturating_add(1);
        let store = self.stores.entry(id.to_owned()).or_insert_with(|| {
            let mut store = ThreadDomainStore::new(id, workspace);
            if let Some(thread) = self.directory.threads.get(id) {
                store.set_snapshot(thread.clone());
            }
            store.generation = self.clock;
            store.subscriptions = self.subscription_counts.get(id).copied().unwrap_or(0);
            store.demand = timeline_demand(
                &self.demands,
                id,
                self.timeline_subscription_counts
                    .get(id)
                    .copied()
                    .unwrap_or(0),
            );
            store
        });
        store.last_used = self.clock;
        true
    }

    fn publish(&mut self, id: &str) -> Vec<ClientPublicationDraft> {
        let Some(store) = self.stores.get_mut(id) else {
            return Vec::new();
        };
        let pending = self
            .pending_requests
            .pending_for_scope(Some(&store.coordinator.workspace_id), Some(id));
        let was_blocked = store.presentation_blocked;
        store.presentation_blocked = false;
        let previous = store.snapshot.as_ref();
        let namespace = self.revisions.get(id).copied().unwrap_or_default();
        let old_revision = namespace.0;
        let old_timeline_revision = namespace.1;
        let projection_changed = previous.is_none_or(|p| {
            !p.coordinator
                .conversation
                .projection()
                .same_content(store.coordinator.conversation.projection())
        });
        let coordinator_changed =
            previous.is_none_or(|p| !same_coordinator_content(&p.coordinator, &store.coordinator));
        let semantic_changed = previous.is_none_or(|p| p.semantic.as_ref() != &store.semantic);
        let pending_changed = previous.is_none_or(|p| p.pending.as_ref() != &pending);
        let principal_changed =
            previous.is_none_or(|p| p.current_principal_id != self.current_principal_id);
        let placement = self.directory.placements.get(id);
        if !was_blocked
            && previous.is_some_and(|p| {
                !coordinator_changed
                    && !semantic_changed
                    && !pending_changed
                    && !principal_changed
                    && p.cli_binding == store.cli_binding
                    && p.placement.as_ref() == placement
                    && p.subscription_failed == store.subscription_failed
                    && p.draft_thread_id.as_ref()
                        == self.navigation.drafts.get(&store.coordinator.workspace_id)
            })
        {
            return Vec::new();
        }
        let timeline_changed = was_blocked
            || projection_changed
            || semantic_changed
            || pending_changed
            || principal_changed
            || previous.is_none_or(|p| {
                p.coordinator.thread().map(|t| &t.turns)
                    != store.coordinator.thread().map(|t| &t.turns)
            });
        let mut snapshot = ThreadDomainSnapshot {
            thread_id: id.to_owned(),
            draft_thread_id: self
                .navigation
                .drafts
                .get(&store.coordinator.workspace_id)
                .cloned(),
            current_principal_id: self.current_principal_id.clone(),
            coordinator: match previous.filter(|_| !coordinator_changed) {
                Some(p) => p.coordinator.clone(),
                None => Arc::new(store.coordinator.snapshot_copy()),
            },
            semantic: match previous.filter(|_| !semantic_changed) {
                Some(p) => p.semantic.clone(),
                None => Arc::new(store.semantic.clone()),
            },
            pending: match previous.filter(|_| !pending_changed) {
                Some(p) => p.pending.clone(),
                None => Arc::new(pending),
            },
            cli_binding: store.cli_binding.clone(),
            revision: old_revision,
            timeline_revision: old_timeline_revision,
            execution_revision: previous.map_or(0, |p| p.execution_revision)
                + u64::from(
                    previous.is_none_or(|p| p.coordinator.resume != store.coordinator.resume),
                ),
            placement: self.directory.placements.get(id).cloned(),
            subscription_failed: store.subscription_failed,
            cache_patch: Default::default(),
        };
        snapshot.revision += 1;
        if timeline_changed {
            snapshot.timeline_revision += 1;
        }
        let snapshot = Arc::new(snapshot);
        let authority = ClientMutationAuthority { _private: () };
        let mut drafts = vec![authority.typed_publication(
            ClientScope::Thread {
                thread_id: id.to_owned(),
            },
            ClientRevisions::new(
                DomainRevision::new(snapshot.revision),
                PresentationRevision::new(snapshot.timeline_revision),
                ContentRevision::new(snapshot.timeline_revision),
                ScopedRevision::new(snapshot.revision),
            ),
            snapshot.clone(),
        )];
        let thread_summary = store.coordinator.thread().cloned().map(|thread| {
            let mut summary = crate::workspaces::directory::thread_directory_summary(thread);
            if summary.turns.is_empty() {
                summary
                    .turns
                    .extend(store.coordinator.last_known_turn().cloned());
            }
            summary
        });
        let summary = SidebarSummaryChanged {
            thread_id: id.to_owned(),
            workspace_id: store.coordinator.workspace_id.clone(),
            thread: thread_summary.clone(),
            placement: self.directory.placements.get(id).cloned(),
        };
        if self.directory.summaries.get(id) != Some(&summary) {
            self.directory
                .dirty_workspaces
                .insert(summary.workspace_id.clone());
            store.summary_revision = namespace.2 + 1;
            drafts.push(authority.publication(
                ClientScope::SidebarSummary {
                    workspace_id: summary.workspace_id.clone(),
                    thread_id: id.to_owned(),
                },
                revisions(store.summary_revision),
                Arc::new(summary.clone()),
            ));
            self.directory.summaries.insert(id.to_owned(), summary);
        }
        if let Some(thread) = thread_summary {
            self.directory.threads.insert(id.to_owned(), thread);
        }
        self.retired.remove(id);
        self.revisions.insert(
            id.to_owned(),
            (
                snapshot.revision,
                snapshot.timeline_revision,
                store.summary_revision.max(namespace.2),
            ),
        );
        store.snapshot = Some(snapshot);
        drafts.extend(self.evict_inactive());
        drafts
    }
}

pub(crate) fn revisions(value: u64) -> ClientRevisions {
    ClientRevisions::new(
        DomainRevision::new(value),
        PresentationRevision::new(value),
        ContentRevision::new(value),
        ScopedRevision::new(value),
    )
}

/// A synchronous Client transaction scoped to a single coordinator.
/// It must be dropped before I/O or another Client transaction is started.
pub struct ThreadMutation<'a> {
    core: &'a ClientCore,
    registry: MutexGuard<'a, ThreadRegistry>,
    id: String,
}

impl Deref for ThreadMutation<'_> {
    type Target = ThreadCoordinator;
    fn deref(&self) -> &Self::Target {
        &self.registry.stores[&self.id].coordinator
    }
}
impl DerefMut for ThreadMutation<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self
            .registry
            .stores
            .get_mut(&self.id)
            .expect("retained thread")
            .coordinator
    }
}
impl Drop for ThreadMutation<'_> {
    fn drop(&mut self) {
        let drafts = self.registry.publish(&self.id);
        self.core.transition_directory(
            &mut self.registry,
            &ClientMutationAuthority { _private: () },
            drafts,
            Vec::new(),
        );
    }
}

/// Existing semantic reducers operate only on the named store's timeline.
pub struct ThreadTimelineMutation<'a> {
    core: &'a ClientCore,
    registry: MutexGuard<'a, ThreadRegistry>,
    id: String,
}
impl Deref for ThreadTimelineMutation<'_> {
    type Target = SemanticTimelineState;
    fn deref(&self) -> &Self::Target {
        &self.registry.stores[&self.id].semantic
    }
}
impl DerefMut for ThreadTimelineMutation<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self
            .registry
            .stores
            .get_mut(&self.id)
            .expect("retained thread")
            .semantic
    }
}
impl Drop for ThreadTimelineMutation<'_> {
    fn drop(&mut self) {
        self.registry
            .stores
            .get_mut(&self.id)
            .expect("retained thread")
            .semantic
            .threads_by_id
            .retain(|id, _| id == &self.id);
        let drafts = self.registry.publish(&self.id);
        self.core.transition_directory(
            &mut self.registry,
            &ClientMutationAuthority { _private: () },
            drafts,
            Vec::new(),
        );
    }
}

impl ClientCore {
    /// Model refreshes need only small selection candidates, never copies of
    /// every loaded conversation, event log, and turn payload in the process.
    pub(crate) fn resolved_composer_model_selection(
        &self,
        thread_id: &str,
    ) -> Option<crate::composer::model_selection::ComposerModelSelection> {
        use crate::composer::model_selection::{
            ComposerModelSelection, ComposerModelSelectionCandidate,
            resolve_composer_model_selection,
        };
        let registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        let workspace = registry
            .stores
            .get(thread_id)
            .map(|store| store.coordinator.workspace_id.as_str())
            .or_else(|| {
                registry
                    .directory
                    .threads
                    .get(thread_id)
                    .map(|thread| thread.workspace_id.as_str())
            })?;
        let directory = registry
            .directory
            .threads
            .iter()
            .filter(|(id, _)| !registry.stores.contains_key(*id))
            .map(|(id, thread)| ComposerModelSelectionCandidate {
                thread_id: id.clone(),
                workspace_id: thread.workspace_id.clone(),
                updated_at: thread.updated_at,
                has_turns: !thread.turns.is_empty(),
                selection: ComposerModelSelection::from_thread(thread),
            });
        let loaded = registry.stores.iter().filter_map(|(id, store)| {
            crate::state::selectors::composer_model_selection_candidate(id, &store.coordinator)
        });
        resolve_composer_model_selection(
            Some(thread_id),
            Some(workspace),
            directory.chain(loaded).collect(),
        )
    }

    pub fn thread_snapshot(&self, id: &str) -> Option<Arc<ThreadDomainSnapshot>> {
        self.thread_registry
            .lock()
            .expect("thread registry poisoned")
            .stores
            .get(id)
            .and_then(|store| store.snapshot.clone())
    }

    pub fn thread_mutation(&self, id: &str, workspace: &str) -> Option<ThreadMutation<'_>> {
        if self.is_stopped() {
            return None;
        }
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        if !registry.require(id, workspace) {
            return None;
        }
        Some(ThreadMutation {
            core: self,
            registry,
            id: id.to_owned(),
        })
    }

    pub fn thread_timeline_mutation(
        &self,
        id: &str,
        workspace: &str,
    ) -> Option<ThreadTimelineMutation<'_>> {
        if self.is_stopped() {
            return None;
        }
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        if !registry.require(id, workspace) {
            return None;
        }
        Some(ThreadTimelineMutation {
            core: self,
            registry,
            id: id.to_owned(),
        })
    }

    pub fn upsert_thread(&self, thread: Thread) {
        let id = thread.id.clone();
        let workspace = thread.workspace_id.clone();
        if let Some(mut mutation) = self.thread_mutation(&id, &workspace) {
            if mutation
                .thread()
                .is_some_and(|previous| previous.updated_at > thread.updated_at)
            {
                return;
            }
            if mutation.thread() != Some(&thread) {
                mutation.set_snapshot(thread);
            }
        }
    }

    pub fn thread_snapshots(&self) -> HashMap<String, Arc<ThreadDomainSnapshot>> {
        self.thread_registry
            .lock()
            .expect("thread registry poisoned")
            .stores
            .iter()
            .filter_map(|(id, store)| {
                store
                    .snapshot
                    .clone()
                    .map(|snapshot| (id.clone(), snapshot))
            })
            .collect()
    }

    pub fn pending_requests_for_scope(
        &self,
        workspace: Option<&str>,
        thread: Option<&str>,
    ) -> Vec<PendingRequest> {
        self.thread_registry
            .lock()
            .expect("thread registry poisoned")
            .pending_requests
            .pending_for_scope(workspace, thread)
    }

    pub(crate) fn pending_request_for_action(
        &self,
        thread: &str,
        request_id: &str,
    ) -> Option<(PendingRequest, u64)> {
        let registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        let workspace = &registry.stores.get(thread)?.coordinator.workspace_id;
        let request = registry
            .pending_requests
            .pending_for_scope(Some(workspace), Some(thread))
            .into_iter()
            .find(|request| request.request_id == request_id)?;
        let generation = registry.pending_requests.request_generation(request_id)?;
        Some((request, generation))
    }

    pub fn apply_pending_requests(&self, reduction: PendingRequestsReduction) -> bool {
        let changed = self.apply_pending_requests_matching(reduction, None);
        if changed {
            self.refresh_approval_actions();
        }
        changed
    }

    pub(crate) fn apply_pending_requests_matching(
        &self,
        reduction: PendingRequestsReduction,
        expected: Option<(&PendingRequest, u64)>,
    ) -> bool {
        if self.is_stopped() {
            return false;
        }
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        if let Some((request, generation)) = expected {
            if registry.pending_requests.request(&request.request_id) != Some(request)
                || registry
                    .pending_requests
                    .request_generation(&request.request_id)
                    != Some(generation)
            {
                return false;
            }
        }
        let before = registry.pending_requests.requests().to_vec();
        if !registry.pending_requests.apply(reduction) {
            return false;
        }
        let after = registry.pending_requests.requests();
        let mut scopes = HashSet::new();
        for request in before.iter().chain(after) {
            if before.iter().find(|r| r.request_id == request.request_id)
                == after.iter().find(|r| r.request_id == request.request_id)
            {
                continue;
            }
            scopes.insert((request.workspace_id.clone(), request.thread_id.clone()));
            for id in &request.visible_thread_ids {
                scopes.insert((request.workspace_id.clone(), Some(id.clone())));
            }
        }
        let mut drafts = Vec::new();
        for (workspace, thread) in scopes {
            if let Some(id) = &thread {
                if registry
                    .stores
                    .get(id)
                    .is_some_and(|s| s.coordinator.workspace_id == workspace)
                {
                    let removed = before
                        .iter()
                        .filter(|r| {
                            r.workspace_id == workspace
                                && !registry
                                    .pending_requests
                                    .requests()
                                    .iter()
                                    .any(|current| current.request_id == r.request_id)
                        })
                        .map(|r| r.request_id.clone())
                        .collect::<Vec<_>>();
                    if let Some(store) = registry.stores.get_mut(id) {
                        for request in removed {
                            crate::timeline::semantic::remove_pending_request_blocks(
                                &mut store.semantic,
                                &request,
                            );
                        }
                    }
                    drafts.extend(registry.publish(id));
                }
            }
            let scope = ClientScope::PendingRequest {
                workspace_id: Some(workspace.clone()),
                thread_id: thread.clone(),
            };
            let revision = self
                .snapshot(&scope)
                .map_or(1, |p| p.revisions().scoped().get() + 1);
            let projection = registry
                .pending_requests
                .pending_for_scope(Some(&workspace), thread.as_deref());
            drafts.push(ClientMutationAuthority { _private: () }.publication(
                scope,
                revisions(revision),
                Arc::new(projection),
            ));
        }
        self.transition_directory(
            &mut registry,
            &ClientMutationAuthority { _private: () },
            drafts,
            Vec::new(),
        );
        true
    }
}

pub struct ConversationMutation<'a>(ThreadMutation<'a>);
impl Deref for ConversationMutation<'_> {
    type Target = crate::conversation::Conversation;
    fn deref(&self) -> &Self::Target {
        &self.0.conversation
    }
}
impl DerefMut for ConversationMutation<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0.conversation
    }
}
pub struct ResumeMutation<'a>(ThreadMutation<'a>);
impl Deref for ResumeMutation<'_> {
    type Target = super::resume::ThreadResumeCoordinator;
    fn deref(&self) -> &Self::Target {
        &self.0.resume
    }
}
impl DerefMut for ResumeMutation<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0.resume
    }
}
impl<'a> ThreadMutation<'a> {
    /// Accept a snapshot through the shared desktop/mobile mutation boundary.
    pub fn set_snapshot(&mut self, thread: Thread) {
        self.registry
            .stores
            .get_mut(&self.id)
            .expect("retained thread")
            .set_snapshot(thread);
    }

    pub fn conversation_mutation(self) -> ConversationMutation<'a> {
        ConversationMutation(self)
    }
    pub fn resume_mutation(self) -> ResumeMutation<'a> {
        ResumeMutation(self)
    }
}

pub struct ThreadStartMutation<'a>(MutexGuard<'a, ThreadRegistry>);
impl Deref for ThreadStartMutation<'_> {
    type Target = ThreadStartCoordinator;
    fn deref(&self) -> &Self::Target {
        &self.0.start
    }
}
impl DerefMut for ThreadStartMutation<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0.start
    }
}

impl ClientCore {
    /// Owned read projection for existing catalog selectors. It is never retained as an owner.
    pub fn thread_coordinator_snapshots(&self) -> HashMap<String, ThreadCoordinator> {
        {
            let registry = self
                .thread_registry
                .lock()
                .expect("thread registry poisoned");
            let mut result: HashMap<_, _> = registry
                .directory
                .threads
                .iter()
                .map(|(id, t)| (id.clone(), ThreadCoordinator::new(t.clone())))
                .collect();
            result.extend(
                registry
                    .stores
                    .iter()
                    .map(|(id, store)| (id.clone(), store.coordinator.snapshot_copy())),
            );
            result
        }
    }
    pub fn existing_thread_mutation(&self, id: &str) -> Option<ThreadMutation<'_>> {
        if self.is_stopped() {
            return None;
        }
        let registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        if !registry.stores.contains_key(id) {
            return None;
        }
        Some(ThreadMutation {
            core: self,
            registry,
            id: id.to_owned(),
        })
    }
    pub fn thread_start_snapshot(&self) -> ThreadStartCoordinator {
        self.thread_registry
            .lock()
            .expect("thread registry poisoned")
            .start
            .clone()
    }
    /// Reserves the existing start coordinator's identity for an editable draft
    /// before workspace bootstrap/creation completes. This does not send a request
    /// or select a server thread; creation consumes the same identity.
    pub fn prepare_thread_draft(&self) -> Option<String> {
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        if self.is_stopped() {
            return None;
        }
        if let Some(draft) = registry
            .navigation
            .workspace_id()
            .and_then(|workspace| registry.navigation.draft(workspace))
        {
            return Some(draft.to_owned());
        }
        if registry.start.pending_thread_id.is_none() {
            super::start::ensure_pending_thread_start_id(
                &mut registry.start,
                super::start::generate_thread_start_id(),
            );
        }
        registry.start.pending_thread_id.clone()
    }
    pub fn thread_start_mutation(&self) -> ThreadStartMutation<'_> {
        ThreadStartMutation(
            self.thread_registry
                .lock()
                .expect("thread registry poisoned"),
        )
    }
    pub fn thread_start_requested(&self) -> bool {
        self.thread_registry
            .lock()
            .expect("thread registry poisoned")
            .start_requested
    }
    pub fn enqueue_thread_start(&self) {
        self.thread_registry
            .lock()
            .expect("thread registry poisoned")
            .start_requested = true;
    }
    pub fn take_thread_start(&self) -> bool {
        std::mem::take(
            &mut self
                .thread_registry
                .lock()
                .expect("thread registry poisoned")
                .start_requested,
        )
    }
    pub fn clear_thread_start_request(&self) {
        self.thread_registry
            .lock()
            .expect("thread registry poisoned")
            .start_requested = false;
    }
    pub fn enqueue_thread_resume(&self, id: String) {
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        if registry.ready_resume_set.insert(id.clone()) {
            registry.ready_resume.push_back(id);
        }
    }
    pub fn take_thread_resume(&self) -> Option<String> {
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        let id = registry.ready_resume.pop_front()?;
        registry.ready_resume_set.remove(&id);
        Some(id)
    }
    pub fn clear_thread_resume_queue(&self) {
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        registry.ready_resume.clear();
        registry.ready_resume_set.clear();
    }
    pub fn remove_thread_store(&self, id: &str) {
        self.evict_thread_store(id, true);
    }
    fn evict_thread_store(&self, id: &str, remove_navigation: bool) {
        self.cancel_artifact_downloads(Some(id));
        self.cancel_composer_requests_for_thread(id);
        self.invalidate_task_reviews(Some(id));
        self.invalidate_approval_actions(Some(id));
        self.invalidate_message_revisions(Some(id));
        self.invalidate_message_deletions(Some(id));
        self.invalidate_turn_cancellations(Some(id));
        self.invalidate_thread_capabilities(Some(id));
        self.invalidate_thread_members(Some(id));
        self.invalidate_artifacts(Some(id));
        let workspace = self
            .thread_coordinator_snapshot(id)
            .map(|c| c.workspace_id.clone());
        if let Some(workspace_id) = workspace {
            self.apply_pending_requests(PendingRequestsReduction::ThreadClosed {
                workspace_id,
                thread_id: id.to_owned(),
            });
        }
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        registry.timeline.invalidate(Some(id));
        if remove_navigation {
            registry.stores.remove(id);
        } else {
            registry.clock = registry.clock.saturating_add(1);
            let generation = registry.clock;
            if let Some(store) = registry.stores.get_mut(id) {
                let mut fresh = ThreadDomainStore::new(id, &store.coordinator.workspace_id);
                fresh.generation = generation;
                fresh.subscriptions = store.subscriptions;
                fresh.demand = store.demand;
                fresh.last_used = store.last_used;
                fresh.presentation_blocked = true;
                *store = fresh;
            }
        }
        if let Some(thread) = registry.directory.threads.remove(id) {
            registry
                .directory
                .dirty_workspaces
                .insert(thread.workspace_id);
        }
        registry.directory.placements.remove(id);
        if remove_navigation {
            registry.navigation.remove_thread(id);
        }
        registry.ready_resume.retain(|value| value != id);
        registry.ready_resume_set.remove(id);
        let mut drafts = registry.navigation_change().into_iter().collect::<Vec<_>>();
        drafts.extend(registry.retire(id));
        drafts.extend(registry.retire_summary(id));
        self.transition_directory(
            &mut registry,
            &ClientMutationAuthority { _private: () },
            drafts,
            Vec::new(),
        );
    }
    pub fn clear_thread_stores(&self) {
        self.cancel_artifact_downloads(None);
        self.cancel_composer_requests();
        self.invalidate_task_reviews(None);
        self.invalidate_approval_actions(None);
        self.invalidate_message_revisions(None);
        self.invalidate_message_deletions(None);
        self.invalidate_turn_cancellations(None);
        self.invalidate_thread_capabilities(None);
        self.invalidate_thread_members(None);
        self.invalidate_artifacts(None);
        let ids = {
            let registry = self
                .thread_registry
                .lock()
                .expect("thread registry poisoned");
            registry.revisions.keys().cloned().collect::<Vec<_>>()
        };
        self.apply_pending_requests(PendingRequestsReduction::ClearAll);
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        let mut drafts = Vec::new();
        for id in ids {
            drafts.extend(registry.retire(&id));
            drafts.extend(registry.retire_summary(&id));
        }
        let current_principal_id = registry.current_principal_id.clone();
        // Clear directory contents while retaining its publication/lifecycle fences.
        // Restarting at revision one would reject the next thread + tree transaction.
        let mut directory = std::mem::take(&mut registry.directory);
        directory.invalidate();
        let presentation_revisions = std::mem::take(&mut registry.presentation_revisions);
        let retired = std::mem::take(&mut registry.retired);
        let retired_timelines = std::mem::take(&mut registry.retired_timelines);
        let revisions = std::mem::take(&mut registry.revisions);
        let subscriptions = std::mem::take(&mut registry.subscription_counts);
        let demands = std::mem::take(&mut registry.demands);
        let timeline_subscription_counts =
            std::mem::take(&mut registry.timeline_subscription_counts);
        let clock = registry.clock.saturating_add(1);
        let session_revision = registry.session_revision.saturating_add(1);
        let navigation_revision = registry.navigation_revision;
        let navigation_publication = registry.navigation_publication.take();
        *registry = ThreadRegistry {
            current_principal_id,
            directory,
            presentation_revisions,
            clock,
            revisions,
            retired,
            retired_timelines,
            demands,
            timeline_subscription_counts,
            subscription_counts: subscriptions,
            session_revision,
            navigation_revision,
            navigation_publication,
            ..Default::default()
        };
        self.publish_navigation(&mut registry);
        self.transition_directory(
            &mut registry,
            &ClientMutationAuthority { _private: () },
            drafts,
            Vec::new(),
        );
    }
    pub fn tick_thread_conversations(&self) -> bool {
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        let ids = registry
            .stores
            .iter_mut()
            .filter_map(|(id, s)| s.coordinator.conversation.tick().then(|| id.clone()))
            .collect::<Vec<_>>();
        let changed = !ids.is_empty();
        let mut drafts = Vec::new();
        for id in ids {
            drafts.extend(registry.publish(&id));
        }
        self.transition_directory(
            &mut registry,
            &ClientMutationAuthority { _private: () },
            drafts,
            Vec::new(),
        );
        changed
    }
}

pub struct ConversationSnapshot(Arc<ThreadCoordinator>);
impl Deref for ConversationSnapshot {
    type Target = crate::conversation::Conversation;
    fn deref(&self) -> &Self::Target {
        &self.0.conversation
    }
}
impl ThreadDomainSnapshot {
    pub fn conversation(&self) -> ConversationSnapshot {
        ConversationSnapshot(self.coordinator.clone())
    }
}

impl ClientCore {
    pub fn thread_semantic_snapshot(&self, id: &str) -> Arc<SemanticTimelineState> {
        self.thread_snapshot(id)
            .map(|s| s.semantic())
            .unwrap_or_default()
    }
    pub fn existing_thread_timeline_mutation(
        &self,
        id: &str,
    ) -> Option<ThreadTimelineMutation<'_>> {
        if self.is_stopped() {
            return None;
        }
        let registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        if !registry.stores.contains_key(id) {
            return None;
        }
        Some(ThreadTimelineMutation {
            core: self,
            registry,
            id: id.to_owned(),
        })
    }
    pub fn apply_thread_timeline_page(
        &self,
        page: pioneer_protocol::ThreadTimelinePageResponse,
        mode: crate::timeline::semantic::TopLevelPageMergeMode,
    ) -> bool {
        let Some(mut state) = self.thread_timeline_mutation(&page.thread_id, &page.workspace_id)
        else {
            return false;
        };
        let store = state
            .registry
            .stores
            .get_mut(&state.id)
            .expect("retained thread");
        confirm_admission_page(&mut store.coordinator.conversation, &page);
        crate::timeline::semantic::apply_thread_timeline_page(&mut store.semantic, page, mode)
    }
    pub fn apply_turn_work_page(
        &self,
        page: pioneer_protocol::TurnWorkPageResponse,
        mode: crate::timeline::semantic::WorkPageMergeMode,
    ) -> bool {
        let Some(mut state) = self.thread_timeline_mutation(&page.thread_id, &page.workspace_id)
        else {
            return false;
        };
        crate::timeline::semantic::apply_turn_work_page(&mut state, page, mode)
    }
    pub fn apply_turn_work_items_get_response(
        &self,
        page: pioneer_protocol::TurnWorkItemsGetResponse,
    ) -> bool {
        let Some(mut state) = self.thread_timeline_mutation(&page.thread_id, &page.workspace_id)
        else {
            return false;
        };
        crate::timeline::semantic::apply_turn_work_items_get_response(&mut state, page)
    }
    pub fn apply_thread_semantic_live_update(
        &self,
        update: crate::timeline::semantic::SemanticTimelineLiveUpdate,
    ) -> bool {
        use crate::timeline::semantic::SemanticTimelineLiveUpdate::*;
        let (id, workspace) = match &update {
            ThreadTimelineBlocksChanged(n) => (&n.thread_id, &n.workspace_id),
            TurnWorkItemsChanged(n) => (&n.thread_id, &n.workspace_id),
            TurnWorkStateChanged(n) => (&n.thread_id, &n.workspace_id),
        };
        let Some(mut state) = self.thread_timeline_mutation(id, workspace) else {
            return false;
        };
        crate::timeline::semantic::apply_semantic_timeline_live_update(&mut state, update)
    }
    pub fn thread_semantic_in_flight(&self, id: &str) -> HashSet<SemanticTimelineRequestKey> {
        self.thread_registry
            .lock()
            .expect("thread registry poisoned")
            .timeline
            .in_flight
            .iter()
            .filter(|key| request_thread_id(key) == id)
            .cloned()
            .collect()
    }
    pub fn enqueue_thread_semantic_request(
        &self,
        action: SemanticTimelineRequestAction,
    ) -> Option<SemanticTimelineRequestAction> {
        let id = request_thread_id(crate::timeline::semantic::semantic_timeline_request_key(
            &action,
        ));
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        registry.stores.get(id)?;
        registry.timeline.begin(action)
    }
    pub fn finish_thread_semantic_request(
        &self,
        key: &SemanticTimelineRequestKey,
    ) -> Option<SemanticTimelineRequestAction> {
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        registry.timeline.finish(key, true, self.timeline_now_ms())
    }
}
fn request_thread_id(key: &SemanticTimelineRequestKey) -> &str {
    use SemanticTimelineRequestKey::*;
    match key {
        ThreadNewest { thread_id }
        | ThreadBefore { thread_id, .. }
        | ThreadAfter { thread_id, .. }
        | TurnWorkInitial { thread_id, .. }
        | TurnWorkBefore { thread_id, .. }
        | TurnWorkAfter { thread_id, .. }
        | TurnWorkItems { thread_id, .. } => thread_id,
    }
}

impl ClientCore {
    pub fn cancel_thread_requests(&self) {
        // Retire optimistic cancellation before advancing the thread incarnation.
        // Otherwise a disconnected request would leave the turn in Cancelling.
        self.invalidate_turn_cancellations(None);
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        registry.clock = registry.clock.saturating_add(1);
        let generation = registry.clock;
        registry.ready_resume.clear();
        registry.ready_resume_set.clear();
        let mut changed = Vec::new();
        let cancelled = registry.timeline.invalidate(None);
        for (id, store) in &mut registry.stores {
            store.generation = generation;
            if store.coordinator.history_loading {
                store.coordinator.history_loading = false;
                store.subscription_failed = true;
                changed.push(id.clone());
            }
            if store.coordinator.resume != Default::default() {
                changed.push(id.clone());
                super::resume::reset_thread_resume_coordinator(&mut store.coordinator.resume);
            }
            let keys = cancelled
                .iter()
                .filter(|key| request_thread_id(key) == id)
                .cloned()
                .collect::<Vec<_>>();
            if !keys.is_empty() {
                changed.push(id.clone());
            }
            for key in keys {
                set_request_status(
                    &mut store.semantic,
                    &key,
                    crate::timeline::semantic::TimelineRequestStatus::Idle,
                );
            }
        }
        changed.sort();
        changed.dedup();
        let drafts = changed
            .into_iter()
            .flat_map(|id| registry.publish(&id))
            .collect();
        self.transition_directory(
            &mut registry,
            &ClientMutationAuthority { _private: () },
            drafts,
            vec![],
        );
    }
    pub(crate) fn publish_timeline_cancellation(&self, keys: &[SemanticTimelineRequestKey]) {
        if keys.is_empty() {
            return;
        }
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        let mut ids = HashSet::new();
        for key in keys {
            if registry.timeline.in_flight.contains(key) {
                continue;
            }
            let id = request_thread_id(key);
            if let Some(store) = registry.stores.get_mut(id) {
                set_request_status(
                    &mut store.semantic,
                    key,
                    crate::timeline::semantic::TimelineRequestStatus::Idle,
                );
                ids.insert(id.to_owned());
            }
        }
        let drafts = ids
            .into_iter()
            .flat_map(|id| registry.publish(&id))
            .collect();
        self.transition_directory(
            &mut registry,
            &ClientMutationAuthority { _private: () },
            drafts,
            vec![],
        );
    }
}

impl ClientCore {
    pub fn set_thread_cli_binding(&self, id: &str, binding: Option<CLIRuntimeThreadBinding>) {
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        let Some(store) = registry.stores.get_mut(id) else {
            return;
        };
        store.cli_binding = binding;
        let drafts = registry.publish(id);
        self.transition_directory(
            &mut registry,
            &ClientMutationAuthority { _private: () },
            drafts,
            Vec::new(),
        );
        drop(registry);
        self.observe_composer_runtime(id, None, false);
    }
}

impl ClientCore {
    pub fn active_thread_id(&self) -> Option<String> {
        self.thread_registry
            .lock()
            .expect("thread registry poisoned")
            .navigation
            .active_thread_id
            .clone()
    }
    pub fn thread_session_revision(&self) -> u64 {
        self.thread_registry
            .lock()
            .expect("thread registry poisoned")
            .session_revision
    }
    pub fn activate_thread(&self, id: Option<&str>, workspace: Option<&str>) {
        self.navigate(
            crate::navigation::NavigationIntent::SelectThread {
                workspace_id: workspace.map(str::to_owned),
                thread_id: id.map(str::to_owned),
            },
            None,
        );
    }
    pub fn thread_workspace_draft(&self, workspace: &str) -> Option<String> {
        let registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        registry
            .navigation
            .drafts
            .get(workspace)
            .filter(|id| registry.stores.contains_key(*id))
            .cloned()
    }
    pub fn thread_workspace_last_active(&self, workspace: &str) -> Option<String> {
        self.navigation_snapshot()
            .last_active(workspace)
            .map(str::to_owned)
    }
    pub fn remember_thread_draft(&self, workspace: &str, id: Option<String>) {
        self.navigate(
            crate::navigation::NavigationIntent::RememberDraft {
                workspace_id: workspace.to_owned(),
                thread_id: id,
            },
            None,
        );
    }
    pub fn promote_thread(&self, id: &str) -> bool {
        self.navigate(
            crate::navigation::NavigationIntent::PromoteThread {
                thread_id: id.to_owned(),
            },
            None,
        )
        .outcome()
            == crate::core::ClientTransitionOutcome::Changed
    }
    pub fn apply_thread_conversation_event(
        &self,
        workspace: &str,
        event: crate::conversation::ConversationEvent,
        mode: Option<pioneer_protocol::ThreadComposerExecutionMode>,
    ) -> crate::timeline::semantic::SemanticTimelineCachePatch {
        if matches!(&event, crate::conversation::ConversationEvent::ItemDelta {delta,payload,markdown,..} if delta.is_empty() && payload.is_none() && markdown.is_none())
        {
            return Default::default();
        }
        let Some(id) = event.thread_id().map(str::to_owned) else {
            return Default::default();
        };
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        if self.is_stopped() || !registry.require(&id, workspace) {
            return Default::default();
        }
        let store = registry.stores.get_mut(&id).expect("retained thread");
        if let crate::conversation::ConversationEvent::ItemDelta {
            turn_id,
            delta,
            payload,
            markdown,
            ..
        } = &event
        {
            use crate::conversation::reducer::TurnPhase;
            if (delta.is_empty() && payload.is_none() && markdown.is_none())
                || store
                    .coordinator
                    .conversation
                    .projection()
                    .turns
                    .iter()
                    .any(|turn| {
                        turn.id == *turn_id
                            && matches!(
                                turn.phase,
                                TurnPhase::Completed
                                    | TurnPhase::Failed
                                    | TurnPhase::Cancelled
                                    | TurnPhase::Blocked
                            )
                    })
            {
                return Default::default();
            }
        }
        if store
            .coordinator
            .conversation
            .ignores_admission_response(&event)
        {
            return Default::default();
        }
        store.coordinator.conversation.apply(event.clone());
        let execution_mode = store
            .coordinator
            .thread()
            .map(|thread| thread.origin_kind.composer_execution_mode())
            .or(mode)
            .unwrap_or(pioneer_protocol::ThreadComposerExecutionMode::ForegroundTurn);
        let patch =
            crate::timeline::semantic::apply_local_composer_event_to_semantic_timeline_with_patch(
                &mut store.semantic,
                workspace,
                &event,
                execution_mode,
                crate::timeline::labels::now_unix_ms(),
            );
        let drafts = registry.publish(&id);
        self.transition_directory(
            &mut registry,
            &ClientMutationAuthority { _private: () },
            drafts,
            Vec::new(),
        );
        patch
    }
}

impl ClientCore {
    pub fn thread_coordinator_snapshot(&self, id: &str) -> Option<Arc<ThreadCoordinator>> {
        let registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        registry
            .stores
            .get(id)
            .and_then(|s| s.snapshot.as_ref())
            .map(|s| s.coordinator())
            .or_else(|| {
                registry
                    .directory
                    .threads
                    .get(id)
                    .map(|t| Arc::new(ThreadCoordinator::new(t.clone())))
            })
    }
    pub fn commit_prepared_thread_turn(
        &self,
        id: &str,
        workspace: &str,
        mode: pioneer_protocol::ThreadMode,
        update: &crate::composer::turn_prepare::PreparedComposerThreadSnapshotUpdate,
        event: crate::conversation::ConversationEvent,
        execution: pioneer_protocol::ThreadComposerExecutionMode,
    ) -> crate::timeline::semantic::SemanticTimelineCachePatch {
        if self
            .thread_coordinator_snapshot(id)
            .is_none_or(|c| c.workspace_id != workspace)
            || event.thread_id() != Some(id)
        {
            return Default::default();
        }
        self.activate_thread(Some(id), Some(workspace));
        self.promote_thread(id);
        {
            let Some(mut coordinator) = self.existing_thread_mutation(id) else {
                return Default::default();
            };
            if coordinator.workspace_id != workspace {
                return Default::default();
            }
            if let Some(thread) = coordinator.thread_mut() {
                crate::turns::start::apply_prepared_turn_to_thread_snapshot(
                    thread,
                    mode,
                    update.selected_model.as_deref(),
                    update.selected_provider.as_deref(),
                    update.selected_reasoning_effort.as_deref(),
                    &update.user_text,
                    update.updated_at_unix,
                );
            }
        }
        self.apply_thread_conversation_event(workspace, event, Some(execution))
    }
    pub fn apply_thread_access_change(
        &self,
        notification: &pioneer_protocol::AccessChangedNotification,
    ) -> crate::authorization::AccessChangedPlan {
        if let Some((previous, plan)) = &self
            .thread_registry
            .lock()
            .expect("thread registry poisoned")
            .last_access
        {
            if previous == notification {
                return plan.clone();
            }
        }
        let (active, workspace, revision) = {
            let registry = self
                .thread_registry
                .lock()
                .expect("thread registry poisoned");
            (
                registry.navigation.active_thread_id.clone(),
                registry.navigation.workspace_id.clone(),
                registry.invalidation_revision,
            )
        };
        let scopes = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned")
            .stores
            .iter()
            .map(
                |(id, store)| crate::authorization::ThreadAuthorizationScope {
                    thread_id: id.clone(),
                    workspace_id: store.coordinator.workspace_id.clone(),
                },
            )
            .collect::<Vec<_>>();
        let plan = crate::authorization::plan_access_changed(
            notification,
            // A policy event and its concrete access-loss event can share one revision.
            // Exact duplicate access events were already handled above.
            revision.filter(|revision| *revision != notification.authorization_revision),
            workspace.as_deref(),
            active.as_deref(),
            &scopes,
        );
        if plan.apply {
            self.thread_registry
                .lock()
                .expect("thread registry poisoned")
                .invalidation_revision = Some(plan.authorization_revision);
            for id in &plan.invalidate_thread_ids {
                self.remove_thread_store(id);
                self.forget_revoked_composer(id);
            }
            if plan.clear_active_thread {
                self.activate_thread(None, None);
            }
            if plan.change == pioneer_protocol::AccessChangeKind::WorkspaceMembership
                && notification.outcome == pioneer_protocol::AccessChangeOutcome::Revoked
            {
                self.apply_pending_requests(PendingRequestsReduction::ClearWorkspace {
                    workspace_id: plan.workspace_id.clone(),
                });
                let mut registry = self
                    .thread_registry
                    .lock()
                    .expect("thread registry poisoned");
                registry.navigation.remove_workspace(&plan.workspace_id);
                self.publish_navigation(&mut registry);
            }
        }
        if plan.apply {
            self.thread_registry
                .lock()
                .expect("thread registry poisoned")
                .last_access = Some((notification.clone(), plan.clone()));
        }
        plan
    }
    pub fn invalidate_threads_for_policy(
        &self,
        notification: &pioneer_protocol::AuthorizationProjectionChangedNotification,
    ) {
        {
            let mut registry = self
                .thread_registry
                .lock()
                .expect("thread registry poisoned");
            if registry.last_policy.as_ref() == Some(notification)
                || registry
                    .invalidation_revision
                    .is_some_and(|v| v > notification.policy_generation.get())
            {
                return;
            }
            registry.last_policy = Some(notification.clone());
            registry.invalidation_revision = Some(notification.policy_generation.get());
        }
        use pioneer_protocol::AuthorizationChangeScope::*;
        let ids = self
            .thread_snapshots()
            .into_iter()
            .filter_map(|(id, snapshot)| {
                let affected = match &notification.affected {
                    Global | Role { .. } | Principal { .. } => true,
                    Invitation { .. } => false,
                    PrincipalWorkspace { workspace_id, .. }
                    | Workspace { workspace_id }
                    | ResourceSelector { workspace_id, .. } => {
                        snapshot.coordinator.workspace_id == *workspace_id
                    }
                    PrincipalThread {
                        workspace_id,
                        thread_id,
                        ..
                    }
                    | Thread {
                        workspace_id,
                        thread_id,
                    } => snapshot.coordinator.workspace_id == *workspace_id && id == *thread_id,
                };
                affected.then_some(id)
            })
            .collect::<Vec<_>>();
        for id in ids {
            // A changed policy requires revalidation, not a user navigation action.
            self.evict_thread_store(&id, false);
        }
    }
}

fn notification_thread_id(notification: &GatewayNotification) -> Option<&str> {
    match notification {
        GatewayNotification::ThreadStarted(notification) => Some(notification.thread.id.as_str()),
        GatewayNotification::ThreadUpdated(notification) => Some(notification.thread.id.as_str()),
        GatewayNotification::ThreadClosed(notification) => Some(notification.thread_id.as_str()),
        GatewayNotification::ThreadTimelineBlocksChanged(notification) => {
            Some(notification.thread_id.as_str())
        }
        GatewayNotification::TurnStarted(notification) => Some(notification.thread_id.as_str()),
        GatewayNotification::TurnCompleted(notification) => Some(notification.thread_id.as_str()),
        GatewayNotification::TurnFailed(notification) => Some(notification.thread_id.as_str()),
        GatewayNotification::TurnBlocked(notification) => Some(notification.thread_id.as_str()),
        GatewayNotification::TurnWorkItemsChanged(notification) => {
            Some(notification.thread_id.as_str())
        }
        GatewayNotification::TurnWorkStateChanged(notification) => {
            Some(notification.thread_id.as_str())
        }
        GatewayNotification::ItemStarted(notification) => Some(notification.thread_id.as_str()),
        GatewayNotification::ItemDelta(notification) => Some(notification.thread_id.as_str()),
        GatewayNotification::ItemCompleted(notification) => Some(notification.thread_id.as_str()),
        GatewayNotification::ItemUpdated(notification) => Some(notification.thread_id.as_str()),
        GatewayNotification::ItemTimeoutDetected(notification) => {
            Some(notification.thread_id.as_str())
        }
        GatewayNotification::ItemRecoveryOpened(notification) => {
            Some(notification.thread_id.as_str())
        }
        GatewayNotification::ItemRecoveryAttached(notification) => {
            Some(notification.thread_id.as_str())
        }
        GatewayNotification::ItemRetryScheduled(notification) => {
            Some(notification.thread_id.as_str())
        }
        GatewayNotification::ItemRetryAttemptStarted(notification) => {
            Some(notification.thread_id.as_str())
        }
        GatewayNotification::ItemRecoverySucceeded(notification) => {
            Some(notification.thread_id.as_str())
        }
        GatewayNotification::ItemRecoveryExhausted(notification) => {
            Some(notification.thread_id.as_str())
        }
        GatewayNotification::ItemToolRetryScheduled(notification) => {
            Some(notification.thread_id.as_str())
        }
        GatewayNotification::ItemToolRetryResolved(notification) => {
            Some(notification.thread_id.as_str())
        }
        GatewayNotification::ItemToolRetryExhausted(notification) => {
            Some(notification.thread_id.as_str())
        }
        GatewayNotification::TurnToolLoopBudgetExceeded(notification) => {
            Some(notification.thread_id.as_str())
        }
        GatewayNotification::CLIRuntimeRequestOpened(notification) => {
            notification.thread_id.as_deref()
        }
        GatewayNotification::CLIRuntimeRequestResolved(notification) => {
            notification.thread_id.as_deref()
        }
        GatewayNotification::TurnPermissionRequestOpened(notification) => {
            Some(notification.request.thread_id.as_str())
        }
        GatewayNotification::TurnPermissionRequestResolved(notification) => {
            Some(notification.thread_id.as_str())
        }
        GatewayNotification::ThreadArtifactsChanged(notification) => {
            Some(notification.thread_id.as_str())
        }
        GatewayNotification::Unknown(notification) => notification.thread_id.as_deref(),
        _ => None,
    }
}
fn notification_workspace_id(notification: &GatewayNotification) -> Option<&str> {
    match notification {
        GatewayNotification::WorkspaceChanged(notification) => {
            Some(notification.workspace.id.as_str())
        }
        GatewayNotification::ThreadStarted(notification) => {
            Some(notification.thread.workspace_id.as_str())
        }
        GatewayNotification::ThreadUpdated(notification) => {
            Some(notification.thread.workspace_id.as_str())
        }
        GatewayNotification::ThreadClosed(notification) => Some(notification.workspace_id.as_str()),
        GatewayNotification::ThreadTreeChanged(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::ThreadAgentsDocChanged(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::TurnStarted(notification) => Some(notification.workspace_id.as_str()),
        GatewayNotification::TurnCompleted(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::TurnFailed(notification) => Some(notification.workspace_id.as_str()),
        GatewayNotification::TurnBlocked(notification) => Some(notification.workspace_id.as_str()),
        GatewayNotification::ItemStarted(notification) => Some(notification.workspace_id.as_str()),
        GatewayNotification::ItemDelta(notification) => Some(notification.workspace_id.as_str()),
        GatewayNotification::ItemCompleted(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::ItemUpdated(notification) => Some(notification.workspace_id.as_str()),
        GatewayNotification::ItemTimeoutDetected(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::ItemRecoveryOpened(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::ItemRecoveryAttached(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::ItemRetryScheduled(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::ItemRetryAttemptStarted(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::ItemRecoverySucceeded(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::ItemRecoveryExhausted(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::ItemToolRetryScheduled(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::ItemToolRetryResolved(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::ItemToolRetryExhausted(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::TurnToolLoopBudgetExceeded(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::CLIRuntimeRequestOpened(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::CLIRuntimeRequestResolved(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::TurnPermissionRequestOpened(notification) => {
            Some(notification.request.workspace_id.as_str())
        }
        GatewayNotification::TurnPermissionRequestResolved(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::ThreadArtifactsChanged(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::ArtifactCreated(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::ArtifactUpdated(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::ArtifactDeleted(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::ArtifactProjectionUpdated(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::ArtifactUploadProgress(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::SkillsChanged(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::McpChanged(notification) => Some(notification.workspace_id.as_str()),
        GatewayNotification::McpServerStatusChanged(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::McpServerCatalogChanged(notification) => {
            Some(notification.workspace_id.as_str())
        }
        GatewayNotification::Unknown(notification) => notification.workspace_id.as_deref(),
        _ => None,
    }
}
impl ClientCore {
    /// Sole transport ingress for thread and request domain state.
    pub fn apply_thread_notification(
        &self,
        notification: pioneer_protocol::GatewayNotification,
    ) -> bool {
        let startup_key = match &notification {
            pioneer_protocol::GatewayNotification::ItemDelta(n)
                if !n.delta.is_empty()
                    && (n.stream == Some(pioneer_protocol::ItemDeltaStream::AgentMessage)
                        || (n.stream == Some(pioneer_protocol::ItemDeltaStream::Generic)
                            && n.payload.as_ref().is_some_and(|p| {
                                matches!(
                                    p.get("startup_output_kind")
                                        .or_else(|| p.get("runtimeDeltaKind"))
                                        .and_then(|v| v.as_str()),
                                    Some(
                                        "reasoning"
                                            | "buffered_reasoning"
                                            | "reasoning_text"
                                            | "reasoning_summary"
                                    )
                                )
                            }))) =>
            {
                Some(n.turn_id.as_str())
            }
            pioneer_protocol::GatewayNotification::ItemStarted(n)
                if matches!(
                    &n.item,
                    pioneer_protocol::TurnItem::CommandExecution { .. }
                        | pioneer_protocol::TurnItem::FileChange { .. }
                        | pioneer_protocol::TurnItem::DynamicToolCall { .. }
                        | pioneer_protocol::TurnItem::WebSearch { .. }
                ) =>
            {
                Some(n.turn_id.as_str())
            }
            pioneer_protocol::GatewayNotification::ItemCompleted(n) if matches!(&n.item, pioneer_protocol::TurnItem::AgentMessage { text, .. } if !text.is_empty()) => {
                Some(n.turn_id.as_str())
            }
            pioneer_protocol::GatewayNotification::ItemCompleted(n) if matches!(&n.item, pioneer_protocol::TurnItem::Reasoning { summary, content, .. } if summary.iter().chain(content).any(|s| !s.is_empty())) => {
                Some(n.turn_id.as_str())
            }
            _ => None,
        };
        let _startup_apply =
            startup_key.and_then(pioneer_observability::turn_startup::client_apply);
        if self.is_stopped() {
            return true;
        }
        let active = self.active_thread_id();
        let workspace = active
            .as_deref()
            .and_then(|id| self.thread_coordinator_snapshot(id))
            .map(|c| c.workspace_id.clone());
        let start = self.thread_start_snapshot();
        let matches = match (
            notification_thread_id(&notification),
            notification_workspace_id(&notification),
        ) {
            (Some(id), Some(workspace)) => self
                .thread_coordinator_snapshot(id)
                .is_some_and(|c| c.workspace_id == workspace),
            _ => false,
        };
        let context = crate::runtime::ClientRuntimeNotificationContext {
            active_thread_id: active.as_deref(),
            active_workspace_id: workspace.as_deref(),
            pending_thread_id: start.pending_thread_id.as_deref(),
            notification_thread_workspace_matches: matches,
            ..Default::default()
        };
        use crate::runtime::ClientRuntimeNotification::*;
        match crate::runtime::reduce_gateway_notification(notification, context) {
            Some(ThreadStarted(r)) => {
                self.upsert_thread(r.thread);
                if let Some(id) = r.set_draft_thread_id {
                    self.remember_thread_draft(&r.workspace_id, Some(id));
                }
                if let Some(id) = r.set_active_thread_id {
                    self.activate_thread(Some(&id), Some(&r.workspace_id));
                }
                if r.reset_thread_start {
                    *self.thread_start_mutation() = Default::default();
                }
                if r.clear_thread_start_queue {
                    self.clear_thread_start_request();
                }
            }
            Some(ThreadUpdated(r)) => {
                let id = r.thread.id.clone();
                let workspace = r.thread.workspace_id.clone();
                if let Some(mut mutation) = self.thread_mutation(&id, &workspace) {
                    if mutation
                        .thread()
                        .is_some_and(|previous| previous.updated_at > r.thread.updated_at)
                    {
                        return true;
                    }
                    if let Some(placement) = r.placement {
                        if placement.thread_id == id
                            && placement.workspace_id == workspace
                            && mutation.registry.directory.placements.get(&id) != Some(&placement)
                        {
                            mutation.registry.directory.placements.insert(id, placement);
                            mutation
                                .registry
                                .directory
                                .dirty_workspaces
                                .insert(workspace);
                        }
                    }
                    if mutation.thread() != Some(&r.thread) {
                        mutation.set_snapshot(r.thread);
                    }
                }
            }
            Some(ThreadClosed(r)) => {
                if let Some(pending) = r.pending_requests {
                    self.apply_pending_requests(pending);
                }
                if r.remove_thread_conversation {
                    self.remove_thread_store(&r.thread_id);
                } else if r.clear_active_thread_if_matches
                    && self.active_thread_id().as_deref() == Some(&r.thread_id)
                {
                    self.activate_thread(None, None);
                }
            }
            Some(ConversationEvent(r)) => {
                self.apply_thread_conversation_event(&r.workspace_id, r.conversation_event, None);
            }
            Some(TurnLifecycle(r)) => {
                if r.promote_thread_from_draft {
                    self.promote_thread(&r.thread_id);
                }
                if let Some(mut coordinator) = self.thread_mutation(&r.thread_id, &r.workspace_id) {
                    if let (Some(status), Some(thread)) =
                        (r.thread_status, coordinator.thread_mut())
                    {
                        thread.status = status;
                    }
                    if r.reset_thread_resume {
                        super::resume::reset_thread_resume_coordinator(&mut coordinator.resume);
                    }
                }
                let reconciliation = crate::timeline::semantic::terminal_turn_work_reconciliation(
                    &self.thread_semantic_snapshot(&r.thread_id),
                    &r.conversation_event,
                );
                self.apply_thread_conversation_event(&r.workspace_id, r.conversation_event, None);
                if let Some(reconciliation) = reconciliation {
                    let thread_id = reconciliation.thread_id;
                    let turn_id = reconciliation.turn_id;
                    self.schedule_thread_semantic_request(
                        SemanticTimelineRequestAction::TurnWorkItemsGet {
                            key: SemanticTimelineRequestKey::TurnWorkItems {
                                thread_id: thread_id.clone(),
                                turn_id: turn_id.clone(),
                            },
                            params: pioneer_protocol::TurnWorkItemsGetParams {
                                thread_id,
                                turn_id,
                                work_item_ids: reconciliation.running_work_item_ids,
                            },
                        },
                    );
                }
                if r.tick_conversation {
                    if let Some(mut coordinator) = self.existing_thread_mutation(&r.thread_id) {
                        coordinator.conversation.tick();
                    }
                }
                if let Some(pending) = r.pending_requests {
                    self.apply_pending_requests(pending);
                }
            }
            Some(SemanticTimeline(update)) => {
                let requests = thread_reconcile_actions(&update);
                if self.apply_thread_semantic_live_update(update) {
                    for action in requests {
                        self.schedule_thread_semantic_request_inner(action, None, true);
                    }
                }
            }
            Some(CLIRuntimePendingRequests(r)) | Some(PendingRequests { reduction: r }) => {
                self.apply_pending_requests(r);
            }
            _ => return false,
        }
        true
    }
}

impl ThreadRegistry {
    fn retire_summary(&mut self, id: &str) -> Vec<ClientPublicationDraft> {
        let Some(summary) = self.directory.summaries.remove(id) else {
            return vec![];
        };
        self.directory
            .dirty_workspaces
            .insert(summary.workspace_id.clone());
        let revision = self.revisions.entry(id.to_owned()).or_default();
        revision.2 += 1;
        vec![ClientMutationAuthority { _private: () }.publication(
            ClientScope::SidebarSummary {
                workspace_id: summary.workspace_id,
                thread_id: id.to_owned(),
            },
            revisions(revision.2),
            Arc::new(serde_json::Value::Null),
        )]
    }
    fn retire(&mut self, id: &str) -> Vec<ClientPublicationDraft> {
        if !self.revisions.contains_key(id) || !self.retired.insert(id.to_owned()) {
            return Vec::new();
        }
        let Some(revisions) = self.revisions.get_mut(id) else {
            return Vec::new();
        };
        revisions.0 += 1;
        revisions.1 += 1;
        let authority = ClientMutationAuthority { _private: () };
        let mut drafts = vec![authority.publication(
            ClientScope::Thread {
                thread_id: id.to_owned(),
            },
            ClientRevisions::new(
                DomainRevision::new(revisions.0),
                PresentationRevision::new(revisions.1),
                ContentRevision::new(revisions.1),
                ScopedRevision::new(revisions.0),
            ),
            Arc::new(serde_json::Value::Null),
        )];
        // Recreating a source does not recreate its timeline. Repeated source
        // eviction must not advance the revision of an already absent timeline:
        // duplicate null publications are omitted by the publication store.
        if self.retired_timelines.insert(id.to_owned()) {
            let presentation_revision = self
                .presentation_revisions
                .entry(id.to_owned())
                .or_default();
            *presentation_revision += 1;
            drafts.push(authority.publication(
                ClientScope::Timeline {
                    thread_id: id.to_owned(),
                },
                crate::threads::registry::revisions(*presentation_revision),
                Arc::new(serde_json::Value::Null),
            ));
        }
        drafts
    }
    fn evict_inactive(&mut self) -> Vec<ClientPublicationDraft> {
        let mut candidates = self
            .stores
            .iter()
            .filter(|(id, store)| {
                self.navigation.active_thread_id.as_ref() != Some(id)
                    && !self.navigation.drafts.values().any(|draft| draft == *id)
                    && self.start.pending_thread_id.as_ref() != Some(id)
                    && !self.ready_resume_set.contains(*id)
                    && store.subscriptions == 0
                    && store.demand == ClientDemand::Suspended
                    && !self
                        .timeline
                        .in_flight
                        .iter()
                        .any(|key| request_thread_id(key) == id.as_str())
                    && !store.coordinator.history_loading
                    && !store.coordinator.resume.in_progress
                    && store.coordinator.conversation.in_flight_turn_id().is_none()
                    && self
                        .pending_requests
                        .pending_for_scope(Some(&store.coordinator.workspace_id), Some(id))
                        .is_empty()
            })
            .map(|(id, s)| (id.clone(), s.last_used))
            .collect::<Vec<_>>();
        candidates.sort_by_key(|(_, used)| *used);
        let count = candidates.len().saturating_sub(INACTIVE_THREAD_LIMIT);
        let mut drafts = Vec::new();
        for (id, _) in candidates.into_iter().take(count) {
            self.timeline.invalidate(Some(&id));
            self.stores.remove(&id);
            drafts.extend(self.retire(&id));
        }
        drafts
    }
}
impl ClientCore {
    pub(crate) fn thread_subscription_changed(&self, scope: &ClientScope, added: bool) {
        let (ClientScope::Thread { thread_id: id } | ClientScope::Timeline { thread_id: id }) =
            scope
        else {
            return;
        };
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        let count = registry.subscription_counts.entry(id.clone()).or_default();
        *count = if added {
            count.saturating_add(1)
        } else {
            count.saturating_sub(1)
        };
        let count = *count;
        if count == 0 {
            registry.subscription_counts.remove(id);
        }
        if matches!(scope, ClientScope::Timeline { .. }) {
            let timeline_count = registry
                .timeline_subscription_counts
                .entry(id.clone())
                .or_default();
            *timeline_count = if added {
                timeline_count.saturating_add(1)
            } else {
                timeline_count.saturating_sub(1)
            };
            if *timeline_count == 0 {
                registry.timeline_subscription_counts.remove(id);
            }
        }
        let demand = timeline_demand(
            &registry.demands,
            id,
            registry
                .timeline_subscription_counts
                .get(id)
                .copied()
                .unwrap_or(0),
        );
        if let Some(store) = registry.stores.get_mut(id) {
            store.subscriptions = count;
            store.demand = demand;
        }
        let drafts = registry.evict_inactive();
        self.transition_directory(
            &mut registry,
            &ClientMutationAuthority { _private: () },
            drafts,
            Vec::new(),
        );
        drop(registry);
        if added {
            self.request_thread_presentation(id);
        }
    }
    pub(crate) fn thread_demand_changed(&self, scope: &ClientScope, demand: ClientDemand) {
        let (ClientScope::Thread { thread_id: id } | ClientScope::Timeline { thread_id: id }) =
            scope
        else {
            return;
        };
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        registry.demands.insert(scope.clone(), demand);
        let combined = timeline_demand(
            &registry.demands,
            id,
            registry
                .timeline_subscription_counts
                .get(id)
                .copied()
                .unwrap_or(0),
        );
        if let Some(store) = registry.stores.get_mut(id) {
            store.demand = combined;
        }
        let drafts = registry.evict_inactive();
        self.transition_directory(
            &mut registry,
            &ClientMutationAuthority { _private: () },
            drafts,
            Vec::new(),
        );
        drop(registry);
        if combined != ClientDemand::Suspended {
            self.request_thread_presentation(id);
        }
    }
}

impl ClientCore {
    pub fn pending_request_snapshot(&self) -> Vec<PendingRequest> {
        self.thread_registry
            .lock()
            .expect("thread registry poisoned")
            .pending_requests
            .requests()
            .to_vec()
    }
    pub fn remember_thread_last_active(&self, workspace: &str, id: Option<String>) {
        self.navigate(
            crate::navigation::NavigationIntent::RememberLast {
                workspace_id: workspace.to_owned(),
                thread_id: id,
            },
            None,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conversation::ConversationEvent;
    use pioneer_protocol::*;
    use std::num::NonZeroUsize;
    fn composer_admission() -> Turn {
        let mut turn = running_turn("admission");
        turn.author = Some(TurnAuthorSnapshot {
            actor: PersistedActorRef::Principal(PrincipalId::new("P".repeat(21)).unwrap()),
            display_name: "User".into(),
            nickname: "user".into(),
            avatar_revision: None,
            agent: None,
        });
        turn
    }

    fn composer_occurrence() -> Turn {
        let mut turn = running_turn("run");
        turn.turn_kind = TurnKind::TaskRun;
        turn.origin = TurnOrigin::DetachedTask;
        turn.author = Some(TurnAuthorSnapshot {
            actor: PersistedActorRef::AgentExecution(
                AgentExecutionId::new("E".repeat(21)).unwrap(),
            ),
            display_name: "Codex".into(),
            nickname: "codex".into(),
            avatar_revision: Some("avatar".into()),
            agent: None,
        });
        turn
    }

    fn composer_anchor() -> TurnItem {
        TurnItem::Task {
            item: TaskTurnItem {
                id: "task_run_run".into(),
                task_id: "task".into(),
                run_id: Some("run".into()),
                created_by_turn_id: Some("admission".into()),
                parent_task_id: None,
                root_task_id: None,
                title: "Short title".into(),
                status: TaskStatus::Running,
                attachment: TaskAttachmentMode::Detached,
                trigger_kind: TaskTriggerKind::Immediate,
                executor_kind: TaskExecutorKind::Agent,
                child_thread_id: Some("child-thread".into()),
                child_turn_id: Some("child-turn".into()),
                agent_role: Some("executor".into()),
                depth: 0,
                max_depth: 3,
                next_fire_at: None,
                progress_preview: None,
                result_preview: None,
                error_preview: None,
                started_at: Some(2),
                created_at: 1,
                updated_at: 2,
            },
        }
    }

    // Read the same semantic renderer used by both shells. This assertion does
    // not mutate, hydrate, or reconcile the state under test.
    fn assert_composer_timeline(core: &ClientCore, users: usize, cards: usize, author_known: bool) {
        let snapshot = core.thread_snapshot("a").unwrap();
        let semantic = snapshot.semantic.thread("a").unwrap();
        let flattened = crate::timeline::semantic::flatten_thread_semantic_timeline(semantic);
        let rendered = crate::timeline::semantic_render::render_semantic_timeline_rows(
            &flattened.rows,
            snapshot.coordinator.conversation.projection().clone(),
        );
        assert!(rendered.rows.iter().all(|row| !matches!(
            row.kind,
            crate::timeline::rows::TimelineRowKind::RunningTurn(_)
        )));
        let blocks = semantic
            .top_level
            .ordered_block_ids
            .iter()
            .map(|id| semantic.top_level.block(id).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            blocks
                .iter()
                .filter(|b| matches!(b.kind, TimelineBlockKind::UserMessage { .. }))
                .count(),
            users
        );
        let tasks = blocks
            .iter()
            .filter(|b| matches!(b.kind, TimelineBlockKind::DetachedTaskRun { .. }))
            .collect::<Vec<_>>();
        assert_eq!(tasks.len(), cards);
        if cards == 1 {
            let TimelineBlockKind::DetachedTaskRun { task, author } = &tasks[0].kind else {
                unreachable!()
            };
            assert_eq!(task.task_id, "task");
            assert_eq!(task.run_id.as_deref(), Some("run"));
            assert_eq!(task.created_by_turn_id.as_deref(), Some("admission"));
            assert_eq!(task.child_thread_id.as_deref(), Some("child-thread"));
            assert_eq!(task.child_turn_id.as_deref(), Some("child-turn"));
            assert_eq!(task.agent_role.as_deref(), Some("executor"));
            if author_known {
                assert_eq!(author.as_ref(), composer_occurrence().author.as_ref());
                let row = rendered
                    .rows
                    .iter()
                    .find(|row| row.key == tasks[0].block_id)
                    .unwrap();
                assert_eq!(row.author.as_ref(), composer_occurrence().author.as_ref());
            }
            if users == 1 {
                assert!(matches!(
                    blocks[0].kind,
                    TimelineBlockKind::UserMessage { .. }
                ));
                assert!(matches!(
                    blocks[1].kind,
                    TimelineBlockKind::DetachedTaskRun { .. }
                ));
            }
        }
    }

    #[test]
    fn collaborative_timeline_sequence_is_consistent_for_local_other_client_and_restore() {
        for local_send in [false, true] {
            for item_first in [false, true] {
                for restore in [false, true] {
                    let core = core();
                    let mut snapshot = thread("a", "ws");
                    snapshot.origin_kind = ThreadOriginKind::Collaborative;
                    if restore {
                        snapshot.turns = vec![composer_admission(), composer_occurrence()];
                    }
                    core.upsert_thread(snapshot);
                    if local_send {
                        core.apply_thread_conversation_event(
                            "ws",
                            ConversationEvent::LocalTurnStartRequested {
                                thread_id: "a".into(),
                                turn_id: "admission".into(),
                                pending_request_id: "request".into(),
                                mode: ThreadMode::Agent,
                                user_text: "Full message, longer than Short title".into(),
                                attachments: vec![],
                            },
                            None,
                        );
                        assert_eq!(
                            core.thread_snapshot("a")
                                .unwrap()
                                .coordinator
                                .conversation
                                .pending_request_id(),
                            Some("request")
                        );
                        assert_composer_timeline(&core, 1, 0, false);
                    }
                    let message = TurnItem::UserMessage {
                        id: "user-admission".into(),
                        text: "Full message, longer than Short title".into(),
                        attachments: vec![],
                    };
                    for notification in [
                        GatewayNotification::ItemStarted(ItemStartedNotification {
                            workspace_id: "ws".into(),
                            thread_id: "a".into(),
                            turn_id: "admission".into(),
                            item: message.clone(),
                        }),
                        GatewayNotification::ItemCompleted(ItemCompletedNotification {
                            workspace_id: "ws".into(),
                            thread_id: "a".into(),
                            turn_id: "admission".into(),
                            item: message.clone(),
                        }),
                        GatewayNotification::TurnStarted(TurnStartedNotification {
                            workspace_id: "ws".into(),
                            thread_id: "a".into(),
                            turn: composer_admission(),
                        }),
                    ] {
                        core.apply_thread_notification(notification.clone());
                        core.apply_thread_notification(notification);
                        assert_composer_timeline(&core, 1, 0, false);
                    }
                    assert!(
                        core.thread_snapshot("a")
                            .unwrap()
                            .coordinator
                            .conversation
                            .can_submit_message()
                    );
                    assert!(
                        core.thread_snapshot("a")
                            .unwrap()
                            .coordinator
                            .conversation
                            .in_flight_turn_id()
                            .is_none()
                    );
                    if local_send {
                        let token = core.thread_operation_token("a").unwrap();
                        core.apply_thread_start_send_result(
                            token,
                            crate::turns::start::TurnStartSendReduction::Accepted {
                                events: vec![ConversationEvent::LocalTurnStartAccepted {
                                    thread_id: "a".into(),
                                    turn_id: "admission".into(),
                                    pending_request_id: "request".into(),
                                    mode: ThreadMode::Agent,
                                }],
                            },
                        );
                        assert_composer_timeline(&core, 1, 0, false);
                    }
                    let started = GatewayNotification::TurnStarted(TurnStartedNotification {
                        workspace_id: "ws".into(),
                        thread_id: "a".into(),
                        turn: composer_occurrence(),
                    });
                    let item = GatewayNotification::ItemStarted(ItemStartedNotification {
                        workspace_id: "ws".into(),
                        thread_id: "a".into(),
                        turn_id: "run".into(),
                        item: composer_anchor(),
                    });
                    let notifications = if item_first {
                        vec![item, started]
                    } else {
                        vec![started, item]
                    };
                    let mut cards = 0;
                    let mut known = restore;
                    for notification in notifications {
                        match &notification {
                            GatewayNotification::ItemStarted(_) => cards = 1,
                            GatewayNotification::TurnStarted(_) => known = true,
                            _ => unreachable!(),
                        }
                        core.apply_thread_notification(notification.clone());
                        assert_composer_timeline(&core, 1, cards, known);
                        core.apply_thread_notification(notification);
                        assert_composer_timeline(&core, 1, cards, known);
                    }
                    let before = core.thread_snapshot("a").unwrap();
                    let mut canonical = before
                        .semantic
                        .thread("a")
                        .unwrap()
                        .top_level
                        .blocks_by_id
                        .values()
                        .cloned()
                        .collect::<Vec<_>>();
                    for block in &mut canonical {
                        match &mut block.kind {
                            TimelineBlockKind::DetachedTaskRun { author, .. } => {
                                *author = composer_occurrence().author;
                                block.sort_key = "00000000000000001000:admission:100:detached-task-run:task_run_run".into();
                            }
                            TimelineBlockKind::UserMessage { .. } => {
                                block.sort_key = "00000000000000001000:admission:000:user".into()
                            }
                            _ => unreachable!(),
                        }
                    }
                    canonical.sort_by_key(|block| {
                        matches!(block.kind, TimelineBlockKind::UserMessage { .. })
                    });
                    // Card-only hydration followed by the parent page must also
                    // preserve causality while the parent still has a live key.
                    for block in canonical {
                        core.apply_thread_timeline_page(
                            ThreadTimelinePageResponse {
                                workspace_id: "ws".into(),
                                thread_id: "a".into(),
                                projection_version: 4,
                                blocks: vec![block],
                                page: Default::default(),
                            },
                            crate::timeline::semantic::TopLevelPageMergeMode::Merge,
                        );
                        assert_composer_timeline(&core, 1, 1, true);
                    }
                    let mut incomplete_turn = composer_occurrence();
                    incomplete_turn.author = None;
                    core.apply_thread_notification(GatewayNotification::TurnStarted(
                        TurnStartedNotification {
                            workspace_id: "ws".into(),
                            thread_id: "a".into(),
                            turn: incomplete_turn,
                        },
                    ));
                    assert_composer_timeline(&core, 1, 1, true);
                    core.apply_thread_notification(GatewayNotification::ItemCompleted(
                        ItemCompletedNotification {
                            workspace_id: "ws".into(),
                            thread_id: "a".into(),
                            turn_id: "run".into(),
                            item: composer_anchor(),
                        },
                    ));
                    assert_composer_timeline(&core, 1, 1, true);
                    let mut incomplete = composer_anchor();
                    if let TurnItem::Task { item } = &mut incomplete {
                        item.updated_at = 3;
                        item.created_by_turn_id = None;
                        item.run_id = None;
                        item.child_thread_id = None;
                        item.child_turn_id = None;
                        item.agent_role = None;
                    }
                    core.apply_thread_notification(GatewayNotification::ItemUpdated(
                        ItemUpdatedNotification {
                            workspace_id: "ws".into(),
                            thread_id: "a".into(),
                            turn_id: "run".into(),
                            item: incomplete,
                        },
                    ));
                    assert_composer_timeline(&core, 1, 1, true);
                    let mut parent = composer_admission();
                    parent.status = TurnStatus::Completed;
                    core.apply_thread_notification(GatewayNotification::TurnCompleted(
                        TurnCompletedNotification {
                            workspace_id: "ws".into(),
                            thread_id: "a".into(),
                            turn: parent,
                        },
                    ));
                    assert_composer_timeline(&core, 1, 1, true);
                    core.apply_thread_notification(GatewayNotification::TurnStarted(
                        TurnStartedNotification {
                            workspace_id: "ws".into(),
                            thread_id: "a".into(),
                            turn: composer_admission(),
                        },
                    ));
                    assert_composer_timeline(&core, 1, 1, true);
                    assert!(
                        core.thread_snapshot("a")
                            .unwrap()
                            .coordinator
                            .conversation
                            .can_submit_message()
                    );
                    // A terminal card cannot reopen when an older live item arrives.
                    let mut terminal = composer_anchor();
                    if let TurnItem::Task { item } = &mut terminal {
                        item.status = TaskStatus::Completed;
                        item.updated_at = 4;
                    }
                    for item in [terminal, composer_anchor()] {
                        core.apply_thread_notification(GatewayNotification::ItemUpdated(
                            ItemUpdatedNotification {
                                workspace_id: "ws".into(),
                                thread_id: "a".into(),
                                turn_id: "run".into(),
                                item,
                            },
                        ));
                        assert_composer_timeline(&core, 1, 1, true);
                        let state = core.thread_snapshot("a").unwrap();
                        let card = state
                            .semantic
                            .thread("a")
                            .unwrap()
                            .top_level
                            .block("turn:run:detached-task-run:task_run_run")
                            .unwrap();
                        assert!(
                            matches!(&card.kind, TimelineBlockKind::DetachedTaskRun { task, .. } if task.status == TaskStatus::Completed)
                        );
                    }
                    let restored = ClientCore::new();
                    let mut snapshot = thread("a", "ws");
                    snapshot.origin_kind = ThreadOriginKind::Collaborative;
                    snapshot.turns = vec![composer_admission(), composer_occurrence()];
                    restored.upsert_thread(snapshot);
                    let current = core.thread_snapshot("a").unwrap();
                    restored.apply_thread_timeline_page(
                        ThreadTimelinePageResponse {
                            workspace_id: "ws".into(),
                            thread_id: "a".into(),
                            projection_version: 4,
                            blocks: current
                                .semantic
                                .thread("a")
                                .unwrap()
                                .top_level
                                .ordered_blocks()
                                .cloned()
                                .collect(),
                            page: Default::default(),
                        },
                        crate::timeline::semantic::TopLevelPageMergeMode::Reset,
                    );
                    assert_composer_timeline(&restored, 1, 1, true);
                    assert!(
                        restored
                            .thread_snapshot("a")
                            .unwrap()
                            .coordinator
                            .conversation
                            .can_submit_message()
                    );
                }
            }
        }
    }

    #[test]
    fn detached_admission_acceptance_releases_composer_before_parent_terminal() {
        let core = core();
        let mut snapshot = thread("a", "ws");
        snapshot.origin_kind = ThreadOriginKind::Collaborative;
        core.upsert_thread(snapshot);
        for turn_id in ["admission", "next"] {
            core.apply_thread_conversation_event(
                "ws",
                ConversationEvent::LocalTurnStartRequested {
                    thread_id: "a".into(),
                    turn_id: turn_id.into(),
                    pending_request_id: turn_id.into(),
                    mode: ThreadMode::Agent,
                    user_text: "User input".into(),
                    attachments: vec![],
                },
                None,
            );
            let before = core.thread_snapshot("a").unwrap();
            assert!(!before.coordinator.conversation.can_submit_message());
            assert_eq!(
                before.coordinator.conversation.pending_request_id(),
                Some(turn_id)
            );
            if turn_id == "next" {
                let mut terminal = composer_admission();
                terminal.status = TurnStatus::Completed;
                core.apply_thread_notification(GatewayNotification::TurnCompleted(
                    TurnCompletedNotification {
                        workspace_id: "ws".into(),
                        thread_id: "a".into(),
                        turn: terminal,
                    },
                ));
                assert_eq!(
                    core.thread_snapshot("a")
                        .unwrap()
                        .coordinator
                        .conversation
                        .pending_request_id(),
                    Some("next")
                );
            }
            let mut admitted = composer_admission();
            admitted.id = turn_id.into();
            core.apply_thread_start_send_result(
                core.thread_operation_token("a").unwrap(),
                crate::turns::start::TurnStartSendReduction::Accepted {
                    events: vec![
                        ConversationEvent::LocalTurnStartAccepted {
                            thread_id: "a".into(),
                            turn_id: turn_id.into(),
                            pending_request_id: turn_id.into(),
                            mode: ThreadMode::Agent,
                        },
                        ConversationEvent::TurnStarted {
                            thread_id: "a".into(),
                            turn: admitted,
                        },
                    ],
                },
            );
            let accepted = core.thread_snapshot("a").unwrap();
            assert!(accepted.coordinator.conversation.can_submit_message());
            assert!(
                accepted
                    .coordinator
                    .conversation
                    .in_flight_turn_id()
                    .is_none()
            );
            assert!(
                accepted
                    .coordinator
                    .conversation
                    .pending_request_id()
                    .is_none()
            );
            assert!(
                accepted
                    .semantic
                    .thread("a")
                    .unwrap()
                    .top_level
                    .ordered_blocks()
                    .all(|block| !matches!(block.kind, TimelineBlockKind::TurnWork { .. }))
            );
            if turn_id == "admission" {
                core.apply_thread_notification(GatewayNotification::TurnStarted(
                    TurnStartedNotification {
                        workspace_id: "ws".into(),
                        thread_id: "a".into(),
                        turn: composer_occurrence(),
                    },
                ));
                core.apply_thread_notification(GatewayNotification::ItemStarted(
                    ItemStartedNotification {
                        workspace_id: "ws".into(),
                        thread_id: "a".into(),
                        turn_id: "run".into(),
                        item: composer_anchor(),
                    },
                ));
                assert_composer_timeline(&core, 1, 1, true);
            }
        }
    }

    #[test]
    fn foreground_and_agent_child_turns_keep_execution_ownership() {
        for origin in [
            ThreadOriginKind::User,
            ThreadOriginKind::DirectMessage,
            ThreadOriginKind::Collaborative,
        ] {
            for mode in [ThreadMode::Chat, ThreadMode::Agent, ThreadMode::Message] {
                let core = core();
                let mut snapshot = thread("a", "ws");
                snapshot.origin_kind = origin;
                core.upsert_thread(snapshot);
                let mut turn = if origin == ThreadOriginKind::Collaborative {
                    composer_occurrence()
                } else {
                    composer_admission()
                };
                turn.turn_kind = TurnKind::Conversation;
                turn.origin = TurnOrigin::User;
                turn.mode = mode;
                core.apply_thread_notification(GatewayNotification::TurnStarted(
                    TurnStartedNotification {
                        workspace_id: "ws".into(),
                        thread_id: "a".into(),
                        turn: turn.clone(),
                    },
                ));
                let snapshot = core.thread_snapshot("a").unwrap();
                assert_eq!(
                    snapshot.coordinator.conversation.in_flight_turn_id(),
                    (mode != ThreadMode::Message).then_some(turn.id.as_str())
                );
                let semantic = snapshot.semantic.thread("a").unwrap();
                let rows = crate::timeline::semantic::flatten_thread_semantic_timeline(semantic);
                let rendered = crate::timeline::semantic_render::render_semantic_timeline_rows(
                    &rows.rows,
                    snapshot.coordinator.conversation.projection().clone(),
                );
                assert_eq!(
                    rendered
                        .rows
                        .iter()
                        .filter(|row| matches!(
                            row.kind,
                            crate::timeline::rows::TimelineRowKind::RunningTurn(_)
                        ))
                        .count(),
                    usize::from(mode != ThreadMode::Message)
                );
            }
        }
    }

    #[test]
    fn attached_task_occurrence_keeps_foreground_work_across_snapshot_and_duplicates() {
        for origin in [TurnOrigin::AttachedTask, TurnOrigin::ScheduledTask] {
            let core = core();
            let mut snapshot = thread("a", "ws");
            snapshot.origin_kind = ThreadOriginKind::Collaborative;
            let mut turn = composer_occurrence();
            turn.origin = origin;
            snapshot.turns = vec![turn.clone()];
            core.upsert_thread(snapshot.clone());
            let started = GatewayNotification::TurnStarted(TurnStartedNotification {
                workspace_id: "ws".into(),
                thread_id: "a".into(),
                turn,
            });
            core.apply_thread_notification(started.clone());
            let mut item = composer_anchor();
            if let TurnItem::Task { item } = &mut item {
                item.attachment = TaskAttachmentMode::Attached;
            }
            core.apply_thread_notification(GatewayNotification::ItemStarted(
                ItemStartedNotification {
                    workspace_id: "ws".into(),
                    thread_id: "a".into(),
                    turn_id: "run".into(),
                    item,
                },
            ));
            core.apply_thread_notification(started);
            snapshot.updated_at += 1;
            core.upsert_thread(snapshot);
            let state = core.thread_snapshot("a").unwrap();
            let semantic = state.semantic.thread("a").unwrap();
            assert!(
                semantic
                    .top_level
                    .ordered_blocks()
                    .all(|block| !matches!(block.kind, TimelineBlockKind::DetachedTaskRun { .. }))
            );
            assert!(semantic.top_level.block("turn:run:work").is_some());
            assert_eq!(semantic.work_range("run").unwrap().items_by_id.len(), 1);
        }
    }

    #[test]
    fn occurrence_before_user_and_context_reuses_one_causal_card() {
        let core = core();
        let mut snapshot = thread("a", "ws");
        snapshot.origin_kind = ThreadOriginKind::Collaborative;
        core.upsert_thread(snapshot);
        core.apply_thread_notification(GatewayNotification::ItemStarted(ItemStartedNotification {
            workspace_id: "ws".into(),
            thread_id: "a".into(),
            turn_id: "run".into(),
            item: composer_anchor(),
        }));
        assert_composer_timeline(&core, 0, 1, false);
        core.apply_thread_notification(GatewayNotification::TurnStarted(TurnStartedNotification {
            workspace_id: "ws".into(),
            thread_id: "a".into(),
            turn: composer_occurrence(),
        }));
        assert_composer_timeline(&core, 0, 1, true);
        core.apply_thread_notification(GatewayNotification::ItemCompleted(
            ItemCompletedNotification {
                workspace_id: "ws".into(),
                thread_id: "a".into(),
                turn_id: "admission".into(),
                item: TurnItem::UserMessage {
                    id: "user".into(),
                    text: "Full message".into(),
                    attachments: vec![],
                },
            },
        ));
        assert_composer_timeline(&core, 1, 1, true);
    }

    fn rendered_thread(
        core: &ClientCore,
    ) -> crate::timeline::semantic_render::SemanticTimelineRenderModel {
        let source = core.thread_snapshot("a").unwrap();
        let rows = crate::timeline::semantic::flatten_thread_semantic_timeline(
            source.semantic.thread("a").unwrap(),
        );
        crate::timeline::semantic_render::render_semantic_timeline_rows(
            &rows.rows,
            source.coordinator.conversation.projection().clone(),
        )
    }

    fn run_work() -> TurnWorkBlock {
        TurnWorkBlock {
            html_publications: vec![],
            turn_id: "run".into(),
            presentation: TurnWorkPresentation::ExpandedLive,
            state: TurnWorkState::Running,
            agent_work_graph: None,
            author: composer_occurrence().author,
            started_at_unix_ms: Some(1_000),
            completed_at_unix_ms: None,
            elapsed_ms: None,
            work_count: 2,
            visible_work_count: 2,
            hidden_work_count: 0,
            has_more_before: true,
            has_more_after: false,
            before_cursor: Some(TimelineCursor {
                value: "old-anchor".into(),
            }),
            after_cursor: None,
            first_work_item_id: Some("old-anchor".into()),
            last_work_item_id: Some("action".into()),
        }
    }

    fn run_action() -> TurnWorkItem {
        TurnWorkItem {
            work_item_id: "turn:run:work:action".into(),
            item_id: "action".into(),
            turn_id: "run".into(),
            order_key: "00000000000000000020:action".into(),
            source_sequence: 20,
            source_updated_at_unix_micros: 20_000,
            item_type: TurnItemType::CommandExecution,
            status: TurnWorkItemStatus::Completed,
            started_at_unix_ms: Some(1_000),
            completed_at_unix_ms: Some(2_000),
            item: TurnItem::CommandExecution {
                id: "action".into(),
                tool_name: "exec_command".into(),
                arguments: serde_json::json!({"command":["echo", "result"]}),
                status: ToolCallStatus::Completed,
                recovery_policy: None,
                output_policy: ToolOutputPolicySnapshot::for_tool_name("exec_command"),
                display: ToolDisplayPayload::Shell {
                    stdout: Some("result".into()),
                    stderr: None,
                    aggregated_output: None,
                    exit_code: Some(0),
                    duration_ms: Some(1),
                    timed_out: None,
                    truncated: false,
                },
                storage: ToolStoragePayload::None,
                recovery: None,
                command: vec!["echo".into(), "result".into()],
                cwd: None,
                success: Some(true),
                outcome: None,
                observation: None,
            },
            metadata: None,
        }
    }

    fn canonical_run_block(kind: TimelineBlockKind, suffix: &str) -> TimelineBlock {
        TimelineBlock {
            workspace_id: "ws".into(),
            thread_id: "a".into(),
            block_id: format!("turn:run:{suffix}"),
            turn_id: Some("run".into()),
            sort_key: format!("00000000000000001000:run:100:{suffix}"),
            started_at_unix_ms: Some(1_000),
            updated_at_unix_ms: Some(2_000),
            kind,
        }
    }

    fn canonical_card() -> TimelineBlock {
        let TurnItem::Task { item } = composer_anchor() else {
            unreachable!()
        };
        canonical_run_block(
            TimelineBlockKind::DetachedTaskRun {
                task: item,
                author: None,
            },
            "detached-task-run:task_run_run",
        )
    }

    #[test]
    fn scheduled_attached_partial_work_survives_snapshot_and_lifecycle_replay() {
        use crate::timeline::semantic::{TopLevelPageMergeMode, WorkPageMergeMode};
        for restored_snapshot in [false, true] {
            let core = core();
            let mut scheduled = composer_occurrence();
            scheduled.origin = TurnOrigin::ScheduledTask;
            let mut snapshot = thread("a", "ws");
            snapshot.origin_kind = ThreadOriginKind::Collaborative;
            if restored_snapshot {
                snapshot.turns = vec![scheduled.clone()];
            }
            core.upsert_thread(snapshot.clone());
            let started = GatewayNotification::TurnStarted(TurnStartedNotification {
                workspace_id: "ws".into(),
                thread_id: "a".into(),
                turn: scheduled.clone(),
            });
            core.apply_thread_notification(started.clone());
            let work_block =
                canonical_run_block(TimelineBlockKind::TurnWork { work: run_work() }, "work");
            let page = ThreadTimelinePageResponse {
                workspace_id: "ws".into(),
                thread_id: "a".into(),
                projection_version: 4,
                blocks: vec![work_block.clone()],
                page: Default::default(),
            };
            core.apply_thread_timeline_page(page.clone(), TopLevelPageMergeMode::Reset);
            let actions = TurnWorkPageResponse {
                workspace_id: "ws".into(),
                thread_id: "a".into(),
                turn_id: "run".into(),
                projection_version: 4,
                source_high_watermark: 20,
                projection_updated_at_unix_micros: 20_000,
                work: Some(run_work()),
                items: vec![run_action()],
                page: TimelinePageInfo {
                    has_more_before: true,
                    ..Default::default()
                },
            };
            core.apply_turn_work_page(actions.clone(), WorkPageMergeMode::Reset);
            snapshot.turns = vec![scheduled.clone()];
            snapshot.updated_at += 1;
            for _ in 0..2 {
                core.apply_thread_notification(GatewayNotification::ThreadUpdated(
                    ThreadUpdatedNotification {
                        thread: snapshot.clone(),
                        placement: None,
                    },
                ));
                core.apply_thread_notification(started.clone());
                core.apply_thread_timeline_page(page.clone(), TopLevelPageMergeMode::Merge);
                core.apply_turn_work_page(actions.clone(), WorkPageMergeMode::Merge);
                let state = core.thread_snapshot("a").unwrap();
                let semantic = state.semantic.thread("a").unwrap();
                assert_eq!(semantic.top_level.block("turn:run:work"), Some(&work_block));
                assert_eq!(semantic.work_range("run").unwrap().items_by_id.len(), 1);
                assert_eq!(
                    semantic
                        .work_range("run")
                        .unwrap()
                        .item("turn:run:work:action"),
                    Some(&run_action())
                );
                let rendered = rendered_thread(&core);
                assert!(
                    rendered
                        .projection
                        .items
                        .iter()
                        .any(|item| item.id == "action")
                );
                assert_eq!(
                    rendered
                        .rows
                        .iter()
                        .filter(|row| matches!(
                            row.kind,
                            crate::timeline::rows::TimelineRowKind::RunningTurn(_)
                        ))
                        .count(),
                    1
                );
                assert!(
                    semantic.top_level.ordered_blocks().all(|block| !matches!(
                        block.kind,
                        TimelineBlockKind::DetachedTaskRun { .. }
                    ))
                );
            }
            // Pagination finally reaches the old anchor; it augments this same group.
            let mut anchor = composer_anchor();
            if let TurnItem::Task { item } = &mut anchor {
                item.attachment = TaskAttachmentMode::Attached;
                item.trigger_kind = TaskTriggerKind::Cron;
            }
            let anchor = TurnWorkItem {
                work_item_id: "turn:run:work:task_run_run".into(),
                item_id: "task_run_run".into(),
                turn_id: "run".into(),
                order_key: "00000000000000000001:task_run_run".into(),
                source_sequence: 1,
                source_updated_at_unix_micros: 1_000,
                item_type: TurnItemType::Task,
                status: TurnWorkItemStatus::Completed,
                started_at_unix_ms: Some(1_000),
                completed_at_unix_ms: Some(1_000),
                item: anchor,
                metadata: None,
            };
            core.apply_turn_work_page(
                TurnWorkPageResponse {
                    items: vec![anchor],
                    ..actions
                },
                WorkPageMergeMode::MergeBefore,
            );
            core.apply_thread_notification(started);
            snapshot.updated_at += 1;
            core.upsert_thread(snapshot);
            let state = core.thread_snapshot("a").unwrap();
            let semantic = state.semantic.thread("a").unwrap();
            assert_eq!(
                semantic
                    .top_level
                    .ordered_blocks()
                    .filter(|block| matches!(block.kind, TimelineBlockKind::TurnWork { .. }))
                    .count(),
                1
            );
            assert_eq!(semantic.work_range("run").unwrap().items_by_id.len(), 2);
            let rendered = rendered_thread(&core);
            assert!(
                rendered
                    .projection
                    .items
                    .iter()
                    .any(|item| item.id == "action")
            );
            assert_eq!(
                rendered
                    .rows
                    .iter()
                    .filter(|row| matches!(
                        row.kind,
                        crate::timeline::rows::TimelineRowKind::RunningTurn(_)
                    ))
                    .count(),
                1
            );
        }
    }

    #[test]
    fn scheduled_detached_occurrence_does_not_create_foreground_work_while_attachment_is_pending() {
        let core = core();
        let mut snapshot = thread("a", "ws");
        snapshot.origin_kind = ThreadOriginKind::Collaborative;
        core.upsert_thread(snapshot);
        let mut turn = composer_occurrence();
        turn.origin = TurnOrigin::ScheduledTask;
        core.apply_thread_notification(GatewayNotification::TurnStarted(TurnStartedNotification {
            workspace_id: "ws".into(),
            thread_id: "a".into(),
            turn,
        }));
        assert_composer_timeline(&core, 0, 0, false);
        core.apply_thread_notification(GatewayNotification::ItemStarted(ItemStartedNotification {
            workspace_id: "ws".into(),
            thread_id: "a".into(),
            turn_id: "run".into(),
            item: run_action().item,
        }));
        assert_composer_timeline(&core, 0, 0, false);
        let mut anchor = composer_anchor();
        if let TurnItem::Task { item } = &mut anchor {
            item.trigger_kind = TaskTriggerKind::Cron;
        }
        core.apply_thread_notification(GatewayNotification::ItemStarted(ItemStartedNotification {
            workspace_id: "ws".into(),
            thread_id: "a".into(),
            turn_id: "run".into(),
            item: anchor,
        }));
        assert_composer_timeline(&core, 0, 1, true);
    }

    fn assert_admission_error(core: &ClientCore, state: TurnWorkState, message: &str) {
        let source = core.thread_snapshot("a").unwrap();
        let semantic = source.semantic.thread("a").unwrap();
        assert!(
            matches!(&semantic.top_level.block("turn:admission:terminal-state").unwrap().kind,
            TimelineBlockKind::TurnState { state: actual, message: Some(text), .. } if *actual == state && text == message)
        );
        assert!(semantic.top_level.block("turn:admission:work").is_none());
        assert!(source.coordinator.conversation.can_submit_message());
        let rendered = rendered_thread(core);
        assert!(rendered.rows.iter().all(|row| !matches!(
            row.kind,
            crate::timeline::rows::TimelineRowKind::RunningTurn(_)
        )));
        assert!(
            rendered
                .projection
                .items
                .iter()
                .any(|item| matches!(&item.item,
            TurnItem::SystemEvent { message: text, .. } if text == message))
        );
        assert!(
            rendered
                .projection
                .items
                .iter()
                .all(|item| !matches!(item.item, TurnItem::Task { .. }))
        );
    }

    #[test]
    fn admission_errors_survive_live_snapshot_hydration_reconnect_and_late_events() {
        use crate::timeline::semantic::TopLevelPageMergeMode;
        for (status, state) in [
            (TurnStatus::Failed, TurnWorkState::Failed),
            (TurnStatus::Blocked, TurnWorkState::Blocked),
            (TurnStatus::Interrupted, TurnWorkState::Interrupted),
        ] {
            for local_send in [false, true] {
                let core = core();
                let mut snapshot = thread("a", "ws");
                snapshot.origin_kind = ThreadOriginKind::Collaborative;
                core.upsert_thread(snapshot.clone());
                if local_send {
                    core.apply_thread_conversation_event(
                        "ws",
                        ConversationEvent::LocalTurnStartRequested {
                            thread_id: "a".into(),
                            turn_id: "admission".into(),
                            pending_request_id: "request".into(),
                            mode: ThreadMode::Agent,
                            user_text: "Rejected submission".into(),
                            attachments: vec![],
                        },
                        None,
                    );
                }
                let mut terminal = composer_admission();
                terminal.status = status;
                terminal.error = Some("Admission refused before Task creation".into());
                let outcome = match status {
                    TurnStatus::Blocked => {
                        GatewayNotification::TurnBlocked(TurnBlockedNotification {
                            workspace_id: "ws".into(),
                            thread_id: "a".into(),
                            turn: terminal.clone(),
                            resume: None,
                        })
                    }
                    _ => GatewayNotification::TurnFailed(TurnFailedNotification {
                        workspace_id: "ws".into(),
                        thread_id: "a".into(),
                        turn: terminal.clone(),
                    }),
                };
                core.apply_thread_notification(outcome.clone());
                assert_admission_error(&core, state, "Admission refused before Task creation");
                let source = core.thread_snapshot("a").unwrap();
                match source.coordinator.conversation.turn_flow_state() {
                    crate::conversation::state_machine::TurnFlowState::Failed { .. } => {
                        assert_eq!(status, TurnStatus::Failed)
                    }
                    crate::conversation::state_machine::TurnFlowState::Blocked { .. } => {
                        assert_eq!(status, TurnStatus::Blocked)
                    }
                    crate::conversation::state_machine::TurnFlowState::Cancelled { .. } => {
                        assert_eq!(status, TurnStatus::Interrupted)
                    }
                    other => panic!("wrong admission outcome: {other:?}"),
                }
                snapshot.turns = vec![terminal.clone()];
                snapshot.updated_at += 1;
                core.apply_thread_notification(GatewayNotification::ThreadUpdated(
                    ThreadUpdatedNotification {
                        thread: snapshot.clone(),
                        placement: None,
                    },
                ));
                assert_admission_error(&core, state, "Admission refused before Task creation");
                let error = TimelineBlock {
                    workspace_id: "ws".into(),
                    thread_id: "a".into(),
                    block_id: "turn:admission:terminal-state".into(),
                    turn_id: Some("admission".into()),
                    sort_key: "00000000000000001000:admission:300:terminal-state".into(),
                    started_at_unix_ms: Some(1_000),
                    updated_at_unix_ms: Some(2_000),
                    kind: TimelineBlockKind::TurnState {
                        state,
                        message: Some("Canonical refusal".into()),
                        author: terminal.author.clone(),
                        route: None,
                    },
                };
                let page = ThreadTimelinePageResponse {
                    workspace_id: "ws".into(),
                    thread_id: "a".into(),
                    projection_version: 4,
                    blocks: vec![error.clone()],
                    page: Default::default(),
                };
                core.apply_thread_timeline_page(page.clone(), TopLevelPageMergeMode::Merge);
                assert_admission_error(&core, state, "Canonical refusal");
                // Replays, a stale success, and a work tombstone must retain the refusal.
                for notification in [
                    outcome.clone(),
                    GatewayNotification::TurnStarted(TurnStartedNotification {
                        workspace_id: "ws".into(),
                        thread_id: "a".into(),
                        turn: composer_admission(),
                    }),
                    GatewayNotification::TurnCompleted(TurnCompletedNotification {
                        workspace_id: "ws".into(),
                        thread_id: "a".into(),
                        turn: Turn {
                            status: TurnStatus::Completed,
                            ..composer_admission()
                        },
                    }),
                    GatewayNotification::ThreadUpdated(ThreadUpdatedNotification {
                        thread: snapshot.clone(),
                        placement: None,
                    }),
                ] {
                    core.apply_thread_notification(notification);
                    assert_admission_error(&core, state, "Canonical refusal");
                }
                core.apply_turn_work_page(
                    TurnWorkPageResponse {
                        workspace_id: "ws".into(),
                        thread_id: "a".into(),
                        turn_id: "admission".into(),
                        projection_version: 4,
                        source_high_watermark: 30,
                        projection_updated_at_unix_micros: 30_000,
                        work: None,
                        items: vec![],
                        page: Default::default(),
                    },
                    crate::timeline::semantic::WorkPageMergeMode::Reset,
                );
                assert_admission_error(&core, state, "Canonical refusal");
                // A fresh core needs no local-send history to restore terminal admission.
                let restored = ClientCore::new();
                let mut restored_snapshot = snapshot.clone();
                let historical_child = Turn {
                    id: "old-child".into(),
                    status: TurnStatus::Completed,
                    turn_kind: TurnKind::Conversation,
                    origin: TurnOrigin::User,
                    ..composer_occurrence()
                };
                restored_snapshot.turns.insert(0, historical_child);
                restored.upsert_thread(restored_snapshot);
                assert_admission_error(&restored, state, "Admission refused before Task creation");
                assert_eq!(
                    restored
                        .thread_snapshot("a")
                        .unwrap()
                        .coordinator
                        .conversation
                        .turn_flow_state(),
                    source.coordinator.conversation.turn_flow_state()
                );
                restored.apply_thread_timeline_page(page.clone(), TopLevelPageMergeMode::Reset);
                {
                    let mut mutation = restored.existing_thread_mutation("a").unwrap();
                    mutation.history_loading = true;
                }
                assert_admission_error(&restored, state, "Canonical refusal");
                let current = restored.thread_snapshot("a").unwrap();
                assert_eq!(
                    current
                        .semantic
                        .thread("a")
                        .unwrap()
                        .top_level
                        .block("turn:admission:terminal-state"),
                    Some(&error)
                );
                core.apply_thread_conversation_event(
                    "ws",
                    ConversationEvent::LocalTurnStartRequested {
                        thread_id: "a".into(),
                        turn_id: "next".into(),
                        pending_request_id: "next-request".into(),
                        mode: ThreadMode::Agent,
                        user_text: "Next submission".into(),
                        attachments: vec![],
                    },
                    None,
                );
                core.apply_thread_notification(outcome);
                snapshot.updated_at += 1;
                core.apply_thread_notification(GatewayNotification::ThreadUpdated(
                    ThreadUpdatedNotification {
                        thread: snapshot,
                        placement: None,
                    },
                ));
                let next = core.thread_snapshot("a").unwrap();
                assert_eq!(
                    next.coordinator.conversation.pending_request_id(),
                    Some("next-request")
                );
                assert_eq!(
                    next.coordinator.conversation.in_flight_turn_id(),
                    Some("next")
                );
                assert!(matches!(
                    next.coordinator.conversation.turn_flow_state(),
                    crate::conversation::state_machine::TurnFlowState::Starting { .. }
                ));
                assert!(rendered_thread(&core).rows.iter().all(|row| !matches!(
                    row.kind,
                    crate::timeline::rows::TimelineRowKind::RunningTurn(_)
                )));
            }
        }
    }

    #[test]
    fn first_canonical_card_uses_retained_exact_run_author_without_thread_snapshot_hydration() {
        use crate::timeline::semantic::TopLevelPageMergeMode;
        let core = core();
        let mut snapshot = thread("a", "ws");
        snapshot.origin_kind = ThreadOriginKind::Collaborative;
        core.upsert_thread(snapshot.clone());
        core.apply_thread_notification(GatewayNotification::TurnStarted(TurnStartedNotification {
            workspace_id: "ws".into(),
            thread_id: "a".into(),
            turn: composer_occurrence(),
        }));
        let page = ThreadTimelinePageResponse {
            workspace_id: "ws".into(),
            thread_id: "a".into(),
            projection_version: 4,
            blocks: vec![canonical_card()],
            page: Default::default(),
        };
        core.apply_thread_timeline_page(page.clone(), TopLevelPageMergeMode::Reset);
        assert_composer_timeline(&core, 0, 1, true);
        for _ in 0..2 {
            core.apply_thread_timeline_page(page.clone(), TopLevelPageMergeMode::Merge);
            assert_composer_timeline(&core, 0, 1, true);
            assert!(
                core.thread_snapshot("a")
                    .unwrap()
                    .coordinator
                    .thread()
                    .unwrap()
                    .turns
                    .is_empty()
            );
        }
        let mut partial = canonical_card();
        if let TimelineBlockKind::DetachedTaskRun { task, .. } = &mut partial.kind {
            task.updated_at += 1;
            task.run_id = None;
            task.created_by_turn_id = None;
            task.child_turn_id = None;
            task.child_thread_id = None;
        }
        core.apply_thread_timeline_page(
            ThreadTimelinePageResponse {
                blocks: vec![partial],
                ..page.clone()
            },
            TopLevelPageMergeMode::Merge,
        );
        assert_composer_timeline(&core, 0, 1, true);
        let mut incomplete_turn = composer_occurrence();
        incomplete_turn.author = None;
        core.apply_thread_notification(GatewayNotification::TurnStarted(TurnStartedNotification {
            workspace_id: "ws".into(),
            thread_id: "a".into(),
            turn: incomplete_turn,
        }));
        core.apply_thread_notification(GatewayNotification::ItemUpdated(ItemUpdatedNotification {
            workspace_id: "ws".into(),
            thread_id: "a".into(),
            turn_id: "run".into(),
            item: composer_anchor(),
        }));
        assert_composer_timeline(&core, 0, 1, true);
        // A card with another run identity cannot borrow this occurrence's author.
        let mut other = canonical_card();
        other.block_id = "turn:run:detached-task-run:other".into();
        if let TimelineBlockKind::DetachedTaskRun { task, .. } = &mut other.kind {
            task.id = "other".into();
            task.task_id = "other-task".into();
            task.run_id = Some("other-run".into());
        }
        core.apply_thread_timeline_page(
            ThreadTimelinePageResponse {
                blocks: vec![other],
                ..page
            },
            TopLevelPageMergeMode::Merge,
        );
        core.apply_thread_notification(GatewayNotification::TurnCompleted(
            TurnCompletedNotification {
                workspace_id: "ws".into(),
                thread_id: "a".into(),
                turn: Turn {
                    status: TurnStatus::Completed,
                    ..composer_occurrence()
                },
            },
        ));
        let rendered = rendered_thread(&core);
        assert!(
            rendered
                .rows
                .iter()
                .find(|row| row.key == "turn:run:detached-task-run:other")
                .unwrap()
                .author
                .is_none()
        );
        assert_eq!(
            rendered
                .rows
                .iter()
                .find(|row| row.key == "turn:run:detached-task-run:task_run_run")
                .unwrap()
                .author,
            composer_occurrence().author
        );
    }

    #[test]
    fn snapshot_boundaries_enrich_existing_card_and_unrelated_publications_leave_semantic_state_alone()
     {
        let core = core();
        let mut snapshot = thread("a", "ws");
        snapshot.origin_kind = ThreadOriginKind::Collaborative;
        core.upsert_thread(snapshot.clone());
        core.apply_thread_timeline_page(
            ThreadTimelinePageResponse {
                workspace_id: "ws".into(),
                thread_id: "a".into(),
                projection_version: 4,
                blocks: vec![canonical_card()],
                page: Default::default(),
            },
            crate::timeline::semantic::TopLevelPageMergeMode::Reset,
        );
        let before = core.thread_snapshot("a").unwrap();
        snapshot.turns = vec![composer_occurrence()];
        snapshot.updated_at += 1;
        core.apply_thread_notification(GatewayNotification::ThreadUpdated(
            ThreadUpdatedNotification {
                thread: snapshot.clone(),
                placement: None,
            },
        ));
        assert_composer_timeline(&core, 0, 1, true);
        let enriched = core.thread_snapshot("a").unwrap();
        assert!(!Arc::ptr_eq(&before.semantic, &enriched.semantic));
        {
            let mut mutation = core.existing_thread_mutation("a").unwrap();
            mutation.history_loading = true;
        }
        let unrelated = core.thread_snapshot("a").unwrap();
        assert!(Arc::ptr_eq(&enriched.semantic, &unrelated.semantic));
        core.apply_thread_notification(GatewayNotification::ThreadUpdated(
            ThreadUpdatedNotification {
                thread: snapshot,
                placement: None,
            },
        ));
        let duplicate = core.thread_snapshot("a").unwrap();
        assert!(Arc::ptr_eq(&unrelated.semantic, &duplicate.semantic));
        assert_composer_timeline(&core, 0, 1, true);
    }

    fn request_admission(core: &ClientCore, turn_id: &str, request_id: &str) {
        core.apply_thread_conversation_event(
            "ws",
            ConversationEvent::LocalTurnStartRequested {
                thread_id: "a".into(),
                turn_id: turn_id.into(),
                pending_request_id: request_id.into(),
                mode: ThreadMode::Agent,
                user_text: "Send through Composer".into(),
                attachments: vec![],
            },
            None,
        );
    }

    fn reject_admission(core: &ClientCore, turn_id: &str, request_id: &str) {
        let token = core.thread_operation_token("a").unwrap();
        let reduction = crate::turns::start::reduce_turn_start_send_failure(
            crate::turns::start::TurnStartSendContext {
                thread_id: "a".into(),
                turn_id: turn_id.into(),
                pending_request_id: request_id.into(),
                mode: ThreadMode::Agent,
            },
            "Local response timeout",
        );
        assert!(core.apply_thread_start_send_result(token, reduction));
    }

    fn admission_outcome_page(turn: &Turn) -> ThreadTimelinePageResponse {
        let state = match turn.status {
            TurnStatus::Failed => TurnWorkState::Failed,
            TurnStatus::Blocked => TurnWorkState::Blocked,
            TurnStatus::Interrupted => TurnWorkState::Interrupted,
            _ => panic!("expected refusal"),
        };
        ThreadTimelinePageResponse {
            workspace_id: "ws".into(),
            thread_id: "a".into(),
            projection_version: 4,
            blocks: vec![TimelineBlock {
                workspace_id: "ws".into(),
                thread_id: "a".into(),
                block_id: "turn:admission:terminal-state".into(),
                turn_id: Some(turn.id.clone()),
                sort_key: "00000000000000001000:admission:300:terminal-state".into(),
                started_at_unix_ms: Some(1_000),
                updated_at_unix_ms: Some(2_000),
                kind: TimelineBlockKind::TurnState {
                    state,
                    message: turn.error.clone(),
                    // Confirmation comes from canonical delivery, not from an avatar.
                    author: None,
                    route: None,
                },
            }],
            page: Default::default(),
        }
    }

    fn admission_outcome_notification(turn: &Turn) -> GatewayNotification {
        if turn.status == TurnStatus::Blocked {
            GatewayNotification::TurnBlocked(TurnBlockedNotification {
                workspace_id: "ws".into(),
                thread_id: "a".into(),
                turn: turn.clone(),
                resume: None,
            })
        } else {
            GatewayNotification::TurnFailed(TurnFailedNotification {
                workspace_id: "ws".into(),
                thread_id: "a".into(),
                turn: turn.clone(),
            })
        }
    }

    fn assert_admission_outcome(core: &ClientCore, status: TurnStatus, message: &str) {
        use crate::conversation::{reducer::TurnPhase, state_machine::TurnFlowState};
        let (state, phase, code) = match status {
            TurnStatus::Failed => (TurnWorkState::Failed, TurnPhase::Failed, "turn_failed"),
            TurnStatus::Blocked => (TurnWorkState::Blocked, TurnPhase::Blocked, "turn_blocked"),
            TurnStatus::Interrupted => (
                TurnWorkState::Interrupted,
                TurnPhase::Cancelled,
                "turn_cancelled",
            ),
            _ => panic!("expected refusal"),
        };
        assert_admission_error(core, state, message);
        let source = core.thread_snapshot("a").unwrap();
        let conversation = &source.coordinator.conversation;
        match (status, conversation.turn_flow_state()) {
            (TurnStatus::Failed, TurnFlowState::Failed { turn_id, error }) => {
                assert_eq!(turn_id, "admission");
                assert_eq!(error, message);
            }
            (TurnStatus::Blocked, TurnFlowState::Blocked { turn_id }) => {
                assert_eq!(turn_id, "admission");
            }
            (TurnStatus::Interrupted, TurnFlowState::Cancelled { turn_id, error }) => {
                assert_eq!(turn_id, "admission");
                assert_eq!(error.as_deref(), Some(message));
            }
            (_, flow) => panic!("unexpected admission flow: {flow:?}"),
        }
        let projection = conversation.projection();
        assert_eq!(
            projection
                .turns
                .iter()
                .filter(|turn| turn.id == "admission")
                .count(),
            1
        );
        let view = projection
            .turns
            .iter()
            .find(|turn| turn.id == "admission")
            .unwrap();
        assert_eq!(view.phase, phase);
        assert_eq!(view.error.as_deref(), Some(message));
        assert_eq!(projection.last_error.as_deref(), Some(message));
        assert_eq!(projection.pending_request_id, None);
        let rendered = rendered_thread(core);
        let outcomes = rendered
            .projection
            .items
            .iter()
            .filter(|item| matches!(&item.item, TurnItem::SystemEvent { .. }))
            .collect::<Vec<_>>();
        assert_eq!(outcomes.len(), 1);
        assert!(matches!(&outcomes[0].item,
            TurnItem::SystemEvent { code: Some(actual), message: actual_message, .. }
                if actual == code && actual_message == message));
    }

    #[test]
    fn canonical_admission_refusals_override_rpc_reject_in_either_order() {
        use crate::timeline::semantic::TopLevelPageMergeMode;
        for status in [
            TurnStatus::Failed,
            TurnStatus::Blocked,
            TurnStatus::Interrupted,
        ] {
            for reject_first in [false, true] {
                // Each entry point must reconcile the local outcome by itself.
                for confirmation in ["lifecycle", "snapshot", "page"] {
                    let core = core();
                    let mut snapshot = thread("a", "ws");
                    snapshot.origin_kind = ThreadOriginKind::Collaborative;
                    core.upsert_thread(snapshot.clone());
                    request_admission(&core, "admission", "request");
                    if reject_first {
                        reject_admission(&core, "admission", "request");
                        assert_admission_outcome(
                            &core,
                            TurnStatus::Failed,
                            "Local response timeout",
                        );
                    }
                    let mut terminal = composer_admission();
                    terminal.status = status;
                    terminal.error = Some("Server refused this admission".into());
                    snapshot.turns = vec![terminal.clone()];
                    snapshot.updated_at += 1;
                    match confirmation {
                        "lifecycle" => {
                            core.apply_thread_notification(admission_outcome_notification(
                                &terminal,
                            ));
                        }
                        "snapshot" => {
                            core.apply_thread_notification(GatewayNotification::ThreadUpdated(
                                ThreadUpdatedNotification {
                                    thread: snapshot.clone(),
                                    placement: None,
                                },
                            ));
                        }
                        "page" => {
                            core.apply_thread_timeline_page(
                                admission_outcome_page(&terminal),
                                TopLevelPageMergeMode::Merge,
                            );
                        }
                        _ => unreachable!(),
                    }
                    assert_admission_outcome(&core, status, "Server refused this admission");
                    for _ in 0..2 {
                        reject_admission(&core, "admission", "request");
                        assert_admission_outcome(&core, status, "Server refused this admission");
                        core.apply_thread_notification(admission_outcome_notification(&terminal));
                        assert_admission_outcome(&core, status, "Server refused this admission");
                        core.apply_thread_timeline_page(
                            admission_outcome_page(&terminal),
                            TopLevelPageMergeMode::Merge,
                        );
                        assert_admission_outcome(&core, status, "Server refused this admission");
                        core.apply_thread_notification(GatewayNotification::ThreadUpdated(
                            ThreadUpdatedNotification {
                                thread: snapshot.clone(),
                                placement: None,
                            },
                        ));
                        assert_admission_outcome(&core, status, "Server refused this admission");
                    }
                    for old_status in [TurnStatus::InProgress, TurnStatus::Completed] {
                        let old = Turn {
                            status: old_status,
                            error: None,
                            ..composer_admission()
                        };
                        let notification = if old_status == TurnStatus::InProgress {
                            GatewayNotification::TurnStarted(TurnStartedNotification {
                                workspace_id: "ws".into(),
                                thread_id: "a".into(),
                                turn: old.clone(),
                            })
                        } else {
                            GatewayNotification::TurnCompleted(TurnCompletedNotification {
                                workspace_id: "ws".into(),
                                thread_id: "a".into(),
                                turn: old.clone(),
                            })
                        };
                        core.apply_thread_notification(notification);
                        assert_admission_outcome(&core, status, "Server refused this admission");
                        let mut stale = snapshot.clone();
                        stale.updated_at += 1;
                        stale.turns = vec![old];
                        core.apply_thread_notification(GatewayNotification::ThreadUpdated(
                            ThreadUpdatedNotification {
                                thread: stale,
                                placement: None,
                            },
                        ));
                        assert_admission_outcome(&core, status, "Server refused this admission");
                    }
                    for state in [TurnWorkState::Running, TurnWorkState::Completed] {
                        let mut stale_page = admission_outcome_page(&terminal);
                        let TimelineBlockKind::TurnState {
                            state: incoming,
                            message,
                            ..
                        } = &mut stale_page.blocks[0].kind
                        else {
                            unreachable!()
                        };
                        *incoming = state;
                        *message = None;
                        core.apply_thread_timeline_page(stale_page, TopLevelPageMergeMode::Merge);
                        assert_admission_outcome(&core, status, "Server refused this admission");
                    }
                    let restored = ClientCore::new();
                    restored.upsert_thread(snapshot);
                    restored.apply_thread_timeline_page(
                        admission_outcome_page(&terminal),
                        TopLevelPageMergeMode::Reset,
                    );
                    assert_admission_outcome(&restored, status, "Server refused this admission");
                }
            }
        }
    }

    fn assert_successful_admission(core: &ClientCore) {
        use crate::conversation::{reducer::TurnPhase, state_machine::TurnFlowState};
        let source = core.thread_snapshot("a").unwrap();
        let conversation = &source.coordinator.conversation;
        assert!(
            matches!(conversation.turn_flow_state(), TurnFlowState::Completed { turn_id } if turn_id == "admission")
        );
        assert!(conversation.can_submit_message());
        let projection = conversation.projection();
        assert_eq!(projection.last_error, None);
        assert_eq!(projection.pending_request_id, None);
        assert_eq!(
            projection
                .turns
                .iter()
                .filter(|turn| turn.id == "admission")
                .count(),
            1
        );
        let turn = projection
            .turns
            .iter()
            .find(|turn| turn.id == "admission")
            .unwrap();
        assert_eq!(turn.phase, TurnPhase::Completed);
        assert_eq!(turn.error, None);
        assert!(
            source
                .semantic
                .thread("a")
                .unwrap()
                .top_level
                .block("turn:admission:terminal-state")
                .is_none()
        );
        let rendered = rendered_thread(core);
        assert!(rendered.rows.iter().all(|row| !matches!(
            row.kind,
            crate::timeline::rows::TimelineRowKind::RunningTurn(_)
        )));
        assert!(
            rendered
                .projection
                .items
                .iter()
                .all(|item| !matches!(item.item, TurnItem::SystemEvent { .. }))
        );
    }

    #[test]
    fn local_timeout_is_cleared_by_confirmed_successful_admission() {
        for confirmation in ["lifecycle", "snapshot", "rpc"] {
            let core = core();
            let mut snapshot = thread("a", "ws");
            snapshot.origin_kind = ThreadOriginKind::Collaborative;
            core.upsert_thread(snapshot.clone());
            request_admission(&core, "admission", "request");
            reject_admission(&core, "admission", "request");
            assert_admission_outcome(&core, TurnStatus::Failed, "Local response timeout");
            let admission = composer_admission();
            match confirmation {
                "lifecycle" => {
                    core.apply_thread_notification(GatewayNotification::TurnStarted(
                        TurnStartedNotification {
                            workspace_id: "ws".into(),
                            thread_id: "a".into(),
                            turn: admission.clone(),
                        },
                    ));
                }
                "snapshot" => {
                    snapshot.turns = vec![admission.clone()];
                    snapshot.updated_at += 1;
                    core.apply_thread_notification(GatewayNotification::ThreadUpdated(
                        ThreadUpdatedNotification {
                            thread: snapshot.clone(),
                            placement: None,
                        },
                    ));
                }
                "rpc" => {
                    let token = core.thread_operation_token("a").unwrap();
                    let reduction = crate::turns::start::reduce_turn_start_send_success(
                        crate::turns::start::TurnStartSendContext {
                            thread_id: "a".into(),
                            turn_id: "admission".into(),
                            pending_request_id: "request".into(),
                            mode: ThreadMode::Agent,
                        },
                        TurnStartResponse {
                            turn: admission.clone(),
                        },
                    );
                    assert!(core.apply_thread_start_send_result(token, reduction));
                }
                _ => unreachable!(),
            }
            assert_successful_admission(&core);
            // Subsequent Task delivery cannot resurrect the timeout or foreground work.
            core.apply_thread_notification(GatewayNotification::TurnStarted(
                TurnStartedNotification {
                    workspace_id: "ws".into(),
                    thread_id: "a".into(),
                    turn: composer_occurrence(),
                },
            ));
            core.apply_thread_conversation_event(
                "ws",
                ConversationEvent::ItemStarted {
                    thread_id: "a".into(),
                    turn_id: "run".into(),
                    item: composer_anchor(),
                },
                None,
            );
            assert_successful_admission(&core);
            for _ in 0..2 {
                reject_admission(&core, "admission", "request");
                assert_successful_admission(&core);
                core.apply_thread_notification(GatewayNotification::TurnCompleted(
                    TurnCompletedNotification {
                        workspace_id: "ws".into(),
                        thread_id: "a".into(),
                        turn: Turn {
                            status: TurnStatus::Completed,
                            ..admission.clone()
                        },
                    },
                ));
                assert_successful_admission(&core);
                core.apply_thread_timeline_page(
                    ThreadTimelinePageResponse {
                        workspace_id: "ws".into(),
                        thread_id: "a".into(),
                        projection_version: 4,
                        blocks: vec![canonical_card()],
                        page: Default::default(),
                    },
                    crate::timeline::semantic::TopLevelPageMergeMode::Merge,
                );
                assert_successful_admission(&core);
                assert_composer_timeline(&core, 1, 1, true);
            }
        }
    }

    #[test]
    fn unconfirmed_rpc_reject_remains_visible_and_releases_composer() {
        let core = core();
        let mut snapshot = thread("a", "ws");
        snapshot.origin_kind = ThreadOriginKind::Collaborative;
        core.upsert_thread(snapshot.clone());
        request_admission(&core, "admission", "request");
        for _ in 0..2 {
            reject_admission(&core, "admission", "request");
            assert_admission_outcome(&core, TurnStatus::Failed, "Local response timeout");
            snapshot.updated_at += 1;
            core.apply_thread_notification(GatewayNotification::ThreadUpdated(
                ThreadUpdatedNotification {
                    thread: snapshot.clone(),
                    placement: None,
                },
            ));
            assert_admission_outcome(&core, TurnStatus::Failed, "Local response timeout");
        }
    }

    #[test]
    fn previous_admission_rpc_reject_cannot_change_new_starting_request() {
        use crate::conversation::{reducer::TurnPhase, state_machine::TurnFlowState};
        for confirmed in [false, true] {
            let core = core();
            let mut snapshot = thread("a", "ws");
            snapshot.origin_kind = ThreadOriginKind::Collaborative;
            core.upsert_thread(snapshot.clone());
            request_admission(&core, "admission", "request");
            reject_admission(&core, "admission", "request");
            if confirmed {
                let terminal = Turn {
                    status: TurnStatus::Blocked,
                    error: Some("Canonical refusal".into()),
                    ..composer_admission()
                };
                core.apply_thread_notification(admission_outcome_notification(&terminal));
                assert_admission_outcome(&core, TurnStatus::Blocked, "Canonical refusal");
            }
            request_admission(&core, "next", "next-request");
            let before = core.thread_snapshot("a").unwrap();
            for _ in 0..2 {
                // Both an obsolete Turn and a wrong request for the current Turn are ignored.
                reject_admission(&core, "admission", "request");
                reject_admission(&core, "next", "wrong-request");
                let current = core.thread_snapshot("a").unwrap();
                let conversation = &current.coordinator.conversation;
                assert!(
                    matches!(conversation.turn_flow_state(), TurnFlowState::Starting { turn_id, pending_request_id }
                    if turn_id == "next" && pending_request_id == "next-request")
                );
                assert_eq!(conversation.pending_request_id(), Some("next-request"));
                assert!(!conversation.can_submit_message());
                let projection = conversation.projection();
                assert_eq!(
                    projection.pending_request_id.as_deref(),
                    Some("next-request")
                );
                assert_eq!(projection.last_error, None);
                let turn = projection
                    .turns
                    .iter()
                    .find(|turn| turn.id == "next")
                    .unwrap();
                assert_eq!(turn.phase, TurnPhase::Starting);
                assert_eq!(turn.error, None);
                assert_eq!(current.semantic, before.semantic);
                assert!(projection.same_content(before.coordinator.conversation.projection()));
                assert!(rendered_thread(&core).rows.iter().all(|row| !matches!(
                    row.kind,
                    crate::timeline::rows::TimelineRowKind::RunningTurn(_)
                )));
            }
            let terminal = Turn {
                status: TurnStatus::Blocked,
                error: Some("Canonical refusal".into()),
                ..composer_admission()
            };
            snapshot.turns = vec![terminal.clone()];
            snapshot.updated_at += 1;
            for notification in [
                admission_outcome_notification(&terminal),
                GatewayNotification::ThreadUpdated(ThreadUpdatedNotification {
                    thread: snapshot,
                    placement: None,
                }),
            ] {
                core.apply_thread_notification(notification);
                reject_admission(&core, "admission", "request");
                let current = core.thread_snapshot("a").unwrap();
                let conversation = &current.coordinator.conversation;
                assert!(
                    matches!(conversation.turn_flow_state(), TurnFlowState::Starting { turn_id, pending_request_id }
                    if turn_id == "next" && pending_request_id == "next-request")
                );
                assert_eq!(conversation.projection().last_error, None);
                assert_eq!(
                    conversation.projection().pending_request_id.as_deref(),
                    Some("next-request")
                );
                let view = conversation
                    .projection()
                    .turns
                    .iter()
                    .find(|turn| turn.id == "next")
                    .unwrap();
                assert_eq!(view.phase, TurnPhase::Starting);
                assert_eq!(view.error, None);
                assert!(rendered_thread(&core).rows.iter().all(|row| !matches!(
                    row.kind,
                    crate::timeline::rows::TimelineRowKind::RunningTurn(_)
                )));
                assert!(rendered_thread(&core).projection.items.iter().any(|item| matches!(&item.item,
                    TurnItem::SystemEvent { code: Some(code), message, .. } if code == "turn_blocked" && message == "Canonical refusal")));
            }
        }
    }

    #[test]
    fn historical_admission_does_not_skip_current_agent_terminal_snapshot() {
        use crate::conversation::{reducer::TurnPhase, state_machine::TurnFlowState};
        for cancelling in [false, true] {
            for status in [
                TurnStatus::Completed,
                TurnStatus::Failed,
                TurnStatus::Blocked,
                TurnStatus::Interrupted,
            ] {
                for another_running in [false, true] {
                    let core = core();
                    let mut snapshot = thread("a", "ws");
                    snapshot.origin_kind = ThreadOriginKind::Collaborative;
                    let admission = Turn {
                        status: TurnStatus::Completed,
                        ..composer_admission()
                    };
                    snapshot.turns = vec![admission];
                    core.upsert_thread(snapshot.clone());
                    let mut agent = Turn {
                        id: "current-agent".into(),
                        turn_kind: TurnKind::Conversation,
                        origin: TurnOrigin::User,
                        mode: ThreadMode::Agent,
                        ..composer_occurrence()
                    };
                    core.apply_thread_notification(GatewayNotification::TurnStarted(
                        TurnStartedNotification {
                            workspace_id: "ws".into(),
                            thread_id: "a".into(),
                            turn: agent.clone(),
                        },
                    ));
                    if cancelling {
                        let (_, params) = core.begin_thread_turn_cancellation("a", None).unwrap();
                        assert_eq!(params.turn_id, "current-agent");
                    }
                    let before = core.thread_snapshot("a").unwrap();
                    assert_eq!(
                        before.coordinator.conversation.in_flight_turn_id(),
                        Some("current-agent")
                    );
                    assert!(before.coordinator.conversation.projection().composer_locked);
                    assert!(
                        matches!(
                            before.coordinator.conversation.turn_flow_state(),
                            TurnFlowState::Cancelling { .. }
                        ) == cancelling
                    );
                    agent.status = status;
                    agent.error =
                        (status != TurnStatus::Completed).then(|| "Canonical agent outcome".into());
                    agent.permission_profile =
                        system_turn_permission_profile_snapshot(TurnPermissionMode::FullAccess);
                    snapshot.turns.push(agent.clone());
                    if another_running {
                        snapshot.turns.push(Turn {
                            id: "another-agent".into(),
                            status: TurnStatus::InProgress,
                            error: None,
                            ..agent.clone()
                        });
                    }
                    for _ in 0..2 {
                        snapshot.updated_at += 1; // Reapply the snapshot boundary, not only a publication duplicate.
                        core.apply_thread_notification(GatewayNotification::ThreadUpdated(
                            ThreadUpdatedNotification {
                                thread: snapshot.clone(),
                                placement: None,
                            },
                        ));
                        let source = core.thread_snapshot("a").unwrap();
                        let conversation = &source.coordinator.conversation;
                        let expected_phase = match status {
                            TurnStatus::Completed => TurnPhase::Completed,
                            TurnStatus::Failed => TurnPhase::Failed,
                            TurnStatus::Blocked => TurnPhase::Blocked,
                            TurnStatus::Interrupted => TurnPhase::Cancelled,
                            _ => unreachable!(),
                        };
                        let view = conversation
                            .projection()
                            .turns
                            .iter()
                            .find(|turn| turn.id == "current-agent")
                            .unwrap();
                        assert_eq!(view.phase, expected_phase);
                        assert_eq!(view.error, agent.error);
                        assert_eq!(
                            view.permission_profile.as_ref(),
                            Some(&agent.permission_profile)
                        );
                        assert_eq!(
                            conversation.in_flight_turn_id(),
                            another_running.then_some("another-agent")
                        );
                        assert_eq!(
                            conversation.projection().in_flight_turn_id.as_deref(),
                            another_running.then_some("another-agent")
                        );
                        assert_eq!(conversation.projection().composer_locked, another_running);
                        assert_eq!(conversation.can_submit_message(), !another_running);
                        assert_eq!(conversation.pending_request_id(), None);
                        let expected_flow = if another_running {
                            TurnFlowState::Running {
                                turn_id: "another-agent".into(),
                            }
                        } else {
                            match status {
                                TurnStatus::Completed => TurnFlowState::Completed {
                                    turn_id: "current-agent".into(),
                                },
                                TurnStatus::Failed => TurnFlowState::Failed {
                                    turn_id: "current-agent".into(),
                                    error: "Canonical agent outcome".into(),
                                },
                                TurnStatus::Blocked => TurnFlowState::Blocked {
                                    turn_id: "current-agent".into(),
                                },
                                TurnStatus::Interrupted => TurnFlowState::Cancelled {
                                    turn_id: "current-agent".into(),
                                    error: Some("Canonical agent outcome".into()),
                                },
                                _ => unreachable!(),
                            }
                        };
                        assert_eq!(conversation.turn_flow_state(), &expected_flow);
                        let rendered = rendered_thread(&core);
                        assert_eq!(rendered.projection.composer_locked, another_running);
                        assert_eq!(
                            rendered.projection.in_flight_turn_id.as_deref(),
                            another_running.then_some("another-agent")
                        );
                        let rendered_turn = rendered
                            .projection
                            .turns
                            .iter()
                            .find(|turn| turn.id == "current-agent")
                            .unwrap();
                        assert_eq!(rendered_turn.phase, expected_phase);
                        assert_eq!(rendered_turn.error, agent.error);
                        assert_eq!(
                            rendered_turn.permission_profile.as_ref(),
                            Some(&agent.permission_profile)
                        );
                        assert!(rendered.rows.iter().all(|row| !matches!(&row.kind,
                            crate::timeline::rows::TimelineRowKind::RunningTurn(display) if display.turn_id == "admission")));
                    }
                }
            }
        }
    }

    #[test]
    fn historical_agent_child_cannot_release_new_starting_admission() {
        use crate::conversation::{reducer::TurnPhase, state_machine::TurnFlowState};
        let core = core();
        let mut snapshot = thread("a", "ws");
        snapshot.origin_kind = ThreadOriginKind::Collaborative;
        snapshot.turns = vec![
            Turn {
                status: TurnStatus::Completed,
                ..composer_admission()
            },
            Turn {
                id: "historical-child".into(),
                status: TurnStatus::Completed,
                turn_kind: TurnKind::Conversation,
                origin: TurnOrigin::User,
                ..composer_occurrence()
            },
        ];
        core.upsert_thread(snapshot.clone());
        request_admission(&core, "next", "next-request");
        for _ in 0..2 {
            snapshot.updated_at += 1;
            core.apply_thread_notification(GatewayNotification::ThreadUpdated(
                ThreadUpdatedNotification {
                    thread: snapshot.clone(),
                    placement: None,
                },
            ));
            let source = core.thread_snapshot("a").unwrap();
            let conversation = &source.coordinator.conversation;
            assert_eq!(
                conversation.turn_flow_state(),
                &TurnFlowState::Starting {
                    turn_id: "next".into(),
                    pending_request_id: "next-request".into(),
                }
            );
            assert_eq!(conversation.pending_request_id(), Some("next-request"));
            assert_eq!(conversation.in_flight_turn_id(), Some("next"));
            assert!(conversation.projection().composer_locked);
            let view = conversation
                .projection()
                .turns
                .iter()
                .find(|turn| turn.id == "next")
                .unwrap();
            assert_eq!(view.phase, TurnPhase::Starting);
            assert_eq!(view.error, None);
            assert_eq!(conversation.projection().last_error, None);
            assert!(rendered_thread(&core).rows.iter().all(|row| !matches!(
                row.kind,
                crate::timeline::rows::TimelineRowKind::RunningTurn(_)
            )));
        }
    }

    #[test]
    fn historical_admission_confirmations_cannot_take_local_terminal_flow() {
        use crate::timeline::semantic::TopLevelPageMergeMode;
        use crate::turns::start::{TurnStartSendContext, reduce_turn_start_send_success};
        for status in [
            TurnStatus::Completed,
            TurnStatus::Failed,
            TurnStatus::Blocked,
            TurnStatus::Interrupted,
        ] {
            let core = core();
            let mut snapshot = thread("a", "ws");
            snapshot.origin_kind = ThreadOriginKind::Collaborative;
            core.upsert_thread(snapshot.clone());
            let previous = Turn {
                id: "previous".into(),
                status: TurnStatus::Completed,
                ..composer_admission()
            };
            request_admission(&core, "previous", "previous-request");
            let previous_response = || {
                reduce_turn_start_send_success(
                    TurnStartSendContext {
                        thread_id: "a".into(),
                        turn_id: "previous".into(),
                        pending_request_id: "previous-request".into(),
                        mode: ThreadMode::Agent,
                    },
                    TurnStartResponse {
                        turn: previous.clone(),
                    },
                )
            };
            assert!(core.apply_thread_start_send_result(
                core.thread_operation_token("a").unwrap(),
                previous_response()
            ));
            snapshot.turns = vec![previous.clone()];
            snapshot.updated_at += 1;
            core.apply_thread_notification(GatewayNotification::ThreadUpdated(
                ThreadUpdatedNotification {
                    thread: snapshot.clone(),
                    placement: None,
                },
            ));
            request_admission(&core, "admission", "request");
            reject_admission(&core, "admission", "request");
            assert_admission_outcome(&core, TurnStatus::Failed, "Local response timeout");
            let page = ThreadTimelinePageResponse {
                workspace_id: "ws".into(),
                thread_id: "a".into(),
                projection_version: 4,
                blocks: vec![TimelineBlock {
                    workspace_id: "ws".into(),
                    thread_id: "a".into(),
                    turn_id: Some("previous".into()),
                    block_id: "turn:previous:terminal-state".into(),
                    sort_key: "00000000000000001000:previous:300:terminal-state".into(),
                    started_at_unix_ms: Some(1_000),
                    updated_at_unix_ms: Some(2_000),
                    kind: TimelineBlockKind::TurnState {
                        state: TurnWorkState::Completed,
                        message: None,
                        author: previous.author.clone(),
                        route: None,
                    },
                }],
                page: Default::default(),
            };
            for _ in 0..2 {
                snapshot.updated_at += 1;
                snapshot.turns[0].permission_profile =
                    system_turn_permission_profile_snapshot(TurnPermissionMode::FullAccess);
                core.apply_thread_notification(GatewayNotification::ThreadUpdated(
                    ThreadUpdatedNotification {
                        thread: snapshot.clone(),
                        placement: None,
                    },
                ));
                assert_admission_outcome(&core, TurnStatus::Failed, "Local response timeout");
                let source = core.thread_snapshot("a").unwrap();
                let history = source
                    .coordinator
                    .conversation
                    .projection()
                    .turns
                    .iter()
                    .find(|turn| turn.id == "previous")
                    .unwrap();
                assert_eq!(
                    history.permission_profile.as_ref(),
                    Some(&snapshot.turns[0].permission_profile)
                );
                core.apply_thread_notification(GatewayNotification::TurnCompleted(
                    TurnCompletedNotification {
                        workspace_id: "ws".into(),
                        thread_id: "a".into(),
                        turn: previous.clone(),
                    },
                ));
                assert_admission_outcome(&core, TurnStatus::Failed, "Local response timeout");
                core.apply_thread_timeline_page(page.clone(), TopLevelPageMergeMode::Merge);
                assert_admission_outcome(&core, TurnStatus::Failed, "Local response timeout");
                reject_admission(&core, "previous", "previous-request");
                assert_admission_outcome(&core, TurnStatus::Failed, "Local response timeout");
                assert!(core.apply_thread_start_send_result(
                    core.thread_operation_token("a").unwrap(),
                    previous_response()
                ));
                assert_admission_outcome(&core, TurnStatus::Failed, "Local response timeout");
            }
            let mut own = composer_admission();
            own.status = status;
            own.error =
                (status != TurnStatus::Completed).then(|| "Canonical own admission outcome".into());
            if status == TurnStatus::Completed {
                core.apply_thread_notification(GatewayNotification::TurnCompleted(
                    TurnCompletedNotification {
                        workspace_id: "ws".into(),
                        thread_id: "a".into(),
                        turn: own.clone(),
                    },
                ));
                assert_successful_admission(&core);
            } else {
                core.apply_thread_notification(admission_outcome_notification(&own));
                assert_admission_outcome(&core, status, "Canonical own admission outcome");
            }
            for _ in 0..2 {
                // Both old RPC outcomes must remain scoped to the previous submission,
                // even though the new one has already released the composer.
                reject_admission(&core, "previous", "previous-request");
                assert!(core.apply_thread_start_send_result(
                    core.thread_operation_token("a").unwrap(),
                    previous_response()
                ));
                snapshot.updated_at += 1;
                core.apply_thread_notification(GatewayNotification::ThreadUpdated(
                    ThreadUpdatedNotification {
                        thread: snapshot.clone(),
                        placement: None,
                    },
                ));
                core.apply_thread_timeline_page(page.clone(), TopLevelPageMergeMode::Merge);
                if status == TurnStatus::Completed {
                    assert_successful_admission(&core);
                } else {
                    assert_admission_outcome(&core, status, "Canonical own admission outcome");
                }
            }
            let restored = ClientCore::new();
            snapshot.turns.push(own);
            restored.upsert_thread(snapshot);
            if status == TurnStatus::Completed {
                assert_successful_admission(&restored);
            } else {
                assert_admission_outcome(&restored, status, "Canonical own admission outcome");
            }
        }
    }

    #[test]
    fn historical_refusal_page_enriches_its_turn_without_changing_new_timeout() {
        use crate::conversation::{reducer::TurnPhase, state_machine::TurnFlowState};
        let core = core();
        let mut snapshot = thread("a", "ws");
        snapshot.origin_kind = ThreadOriginKind::Collaborative;
        let previous = Turn {
            id: "previous".into(),
            status: TurnStatus::Blocked,
            error: Some("Historical canonical refusal".into()),
            ..composer_admission()
        };
        snapshot.turns = vec![previous.clone()];
        core.upsert_thread(snapshot.clone());
        request_admission(&core, "admission", "request");
        reject_admission(&core, "admission", "request");
        let mut page = admission_outcome_page(&previous);
        page.blocks[0].block_id = "turn:previous:terminal-state".into();
        page.blocks[0].sort_key = "00000000000000001000:previous:300:terminal-state".into();
        for _ in 0..2 {
            snapshot.updated_at += 1;
            snapshot.turns[0].permission_profile =
                system_turn_permission_profile_snapshot(TurnPermissionMode::FullAccess);
            core.apply_thread_notification(GatewayNotification::ThreadUpdated(
                ThreadUpdatedNotification {
                    thread: snapshot.clone(),
                    placement: None,
                },
            ));
            core.apply_thread_notification(admission_outcome_notification(&previous));
            core.apply_thread_timeline_page(
                page.clone(),
                crate::timeline::semantic::TopLevelPageMergeMode::Merge,
            );
            let source = core.thread_snapshot("a").unwrap();
            let conversation = &source.coordinator.conversation;
            assert_eq!(
                conversation.turn_flow_state(),
                &TurnFlowState::Failed {
                    turn_id: "admission".into(),
                    error: "Local response timeout".into(),
                }
            );
            assert_eq!(
                conversation.projection().last_error.as_deref(),
                Some("Local response timeout")
            );
            assert_eq!(conversation.in_flight_turn_id(), None);
            assert_eq!(conversation.pending_request_id(), None);
            assert!(conversation.can_submit_message());
            assert!(!conversation.projection().composer_locked);
            let view = conversation
                .projection()
                .turns
                .iter()
                .find(|turn| turn.id == "admission")
                .unwrap();
            assert_eq!(view.phase, TurnPhase::Failed);
            assert_eq!(view.error.as_deref(), Some("Local response timeout"));
            let historical = conversation
                .projection()
                .turns
                .iter()
                .find(|turn| turn.id == "previous")
                .unwrap();
            assert_eq!(historical.phase, TurnPhase::Blocked);
            assert_eq!(
                historical.error.as_deref(),
                Some("Historical canonical refusal")
            );
            assert_admission_error(&core, TurnWorkState::Failed, "Local response timeout");
            assert!(rendered_thread(&core).projection.items.iter().any(
                |item| matches!(&item.item,
                TurnItem::SystemEvent { code: Some(code), message, .. }
                    if code == "turn_blocked" && message == "Historical canonical refusal")
            ));
        }
    }

    #[test]
    fn late_unconfirmed_rpc_reject_cannot_replace_new_terminal_submission() {
        let core = core();
        let mut snapshot = thread("a", "ws");
        snapshot.origin_kind = ThreadOriginKind::Collaborative;
        core.upsert_thread(snapshot);
        request_admission(&core, "previous", "previous-request");
        reject_admission(&core, "previous", "previous-request");
        request_admission(&core, "admission", "request");
        reject_admission(&core, "admission", "request");
        for successful in [false, true] {
            if successful {
                core.apply_thread_notification(GatewayNotification::TurnCompleted(
                    TurnCompletedNotification {
                        workspace_id: "ws".into(),
                        thread_id: "a".into(),
                        turn: Turn {
                            status: TurnStatus::Completed,
                            ..composer_admission()
                        },
                    },
                ));
            }
            let before = core.thread_snapshot("a").unwrap();
            for _ in 0..2 {
                reject_admission(&core, "previous", "previous-request");
                let source = core.thread_snapshot("a").unwrap();
                assert_eq!(
                    source.coordinator.conversation.turn_flow_state(),
                    before.coordinator.conversation.turn_flow_state()
                );
                assert!(
                    source
                        .coordinator
                        .conversation
                        .projection()
                        .same_content(before.coordinator.conversation.projection())
                );
                assert_eq!(source.semantic, before.semantic);
                assert_eq!(source.coordinator.conversation.in_flight_turn_id(), None);
                assert!(source.coordinator.conversation.can_submit_message());
                assert!(rendered_thread(&core).rows.iter().all(|row| !matches!(
                    row.kind,
                    crate::timeline::rows::TimelineRowKind::RunningTurn(_)
                )));
            }
        }
    }

    #[test]
    fn admission_user_item_delivery_keeps_projection_starting_until_confirmation() {
        use crate::conversation::{reducer::TurnPhase, state_machine::TurnFlowState};
        let core = core();
        let mut snapshot = thread("a", "ws");
        snapshot.origin_kind = ThreadOriginKind::Collaborative;
        core.upsert_thread(snapshot);
        request_admission(&core, "admission", "request");
        let item = TurnItem::UserMessage {
            id: "user-admission".into(),
            text: "Send through Composer".into(),
            attachments: vec![],
        };
        let deliveries = [
            GatewayNotification::ItemStarted(ItemStartedNotification {
                workspace_id: "ws".into(),
                thread_id: "a".into(),
                turn_id: "admission".into(),
                item: item.clone(),
            }),
            GatewayNotification::ItemCompleted(ItemCompletedNotification {
                workspace_id: "ws".into(),
                thread_id: "a".into(),
                turn_id: "admission".into(),
                item: item.clone(),
            }),
            GatewayNotification::ItemUpdated(ItemUpdatedNotification {
                workspace_id: "ws".into(),
                thread_id: "a".into(),
                turn_id: "admission".into(),
                item,
            }),
        ];
        for delivery in std::iter::once(None).chain(
            deliveries
                .into_iter()
                .flat_map(|delivery| [Some(delivery.clone()), Some(delivery)]),
        ) {
            if let Some(delivery) = delivery {
                core.apply_thread_notification(delivery);
            }
            let snapshot = core.thread_snapshot("a").unwrap();
            let conversation = &snapshot.coordinator.conversation;
            assert_eq!(
                conversation.turn_flow_state(),
                &TurnFlowState::Starting {
                    turn_id: "admission".into(),
                    pending_request_id: "request".into()
                }
            );
            assert_eq!(conversation.pending_request_id(), Some("request"));
            assert!(conversation.projection().composer_locked);
            let rendered = rendered_thread(&core);
            for projection in [conversation.projection(), &rendered.projection] {
                let view = projection
                    .turns
                    .iter()
                    .find(|turn| turn.id == "admission")
                    .unwrap();
                assert_eq!(view.phase, TurnPhase::Starting);
                assert_eq!(view.error, None);
                assert_eq!(projection.pending_request_id.as_deref(), Some("request"));
            }
            assert_composer_timeline(&core, 1, 0, false);
        }
        core.apply_thread_notification(GatewayNotification::TurnStarted(TurnStartedNotification {
            workspace_id: "ws".into(),
            thread_id: "a".into(),
            turn: composer_admission(),
        }));
        assert_successful_admission(&core);
    }

    #[test]
    fn foreground_user_item_delivery_still_claims_running_flow() {
        use crate::conversation::{reducer::TurnPhase, state_machine::TurnFlowState};
        for origin in [ThreadOriginKind::User, ThreadOriginKind::DirectMessage] {
            let core = core();
            let mut snapshot = thread("a", "ws");
            snapshot.origin_kind = origin;
            core.upsert_thread(snapshot);
            request_admission(&core, "foreground", "request");
            core.apply_thread_notification(GatewayNotification::ItemStarted(
                ItemStartedNotification {
                    workspace_id: "ws".into(),
                    thread_id: "a".into(),
                    turn_id: "foreground".into(),
                    item: TurnItem::UserMessage {
                        id: "user-foreground".into(),
                        text: "Send through Composer".into(),
                        attachments: vec![],
                    },
                },
            ));
            let source = core.thread_snapshot("a").unwrap();
            let conversation = &source.coordinator.conversation;
            assert_eq!(
                conversation.turn_flow_state(),
                &TurnFlowState::Running {
                    turn_id: "foreground".into()
                }
            );
            assert_eq!(conversation.pending_request_id(), None);
            assert_eq!(conversation.in_flight_turn_id(), Some("foreground"));
            assert!(conversation.projection().composer_locked);
            let rendered = rendered_thread(&core);
            let view = rendered
                .projection
                .turns
                .iter()
                .find(|turn| turn.id == "foreground")
                .unwrap();
            assert_eq!(view.phase, TurnPhase::Running);
            assert_eq!(rendered.projection.pending_request_id, None);
        }
    }

    #[test]
    fn admission_principal_refusal_keeps_exact_author_in_shared_renderer() {
        for status in [
            TurnStatus::Failed,
            TurnStatus::Blocked,
            TurnStatus::Interrupted,
        ] {
            let core = core();
            let mut snapshot = thread("a", "ws");
            snapshot.origin_kind = ThreadOriginKind::Collaborative;
            core.upsert_thread(snapshot.clone());
            let mut terminal = composer_admission();
            terminal.status = status;
            terminal.error = Some("Admission refused".into());
            core.apply_thread_notification(admission_outcome_notification(&terminal));
            for hydrate in [false, true] {
                if hydrate {
                    snapshot.turns = vec![terminal.clone()];
                    snapshot.updated_at += 1;
                    core.apply_thread_notification(GatewayNotification::ThreadUpdated(
                        ThreadUpdatedNotification {
                            thread: snapshot.clone(),
                            placement: None,
                        },
                    ));
                    let mut page = admission_outcome_page(&terminal);
                    if let TimelineBlockKind::TurnState { author, .. } = &mut page.blocks[0].kind {
                        *author = terminal.author.clone();
                    }
                    core.apply_thread_timeline_page(
                        page.clone(),
                        crate::timeline::semantic::TopLevelPageMergeMode::Merge,
                    );
                    core.apply_thread_timeline_page(
                        page,
                        crate::timeline::semantic::TopLevelPageMergeMode::Merge,
                    );
                }
                assert_admission_outcome(&core, status, "Admission refused");
                let rendered = rendered_thread(&core);
                let row = rendered
                    .rows
                    .iter()
                    .find(|row| row.key == "turn:admission:terminal-state")
                    .unwrap();
                assert_eq!(row.author, terminal.author);
            }
        }
    }

    #[test]
    fn restored_session_resumes_only_visible_thread_subscription_and_timeline() {
        let core = ClientCore::new();
        core.upsert_thread(thread("visible", "workspace"));
        core.upsert_thread(thread("hidden", "workspace"));
        core.thread_demand_changed(
            &ClientScope::Timeline {
                thread_id: "visible".into(),
            },
            ClientDemand::Visible,
        );
        let (sender, receiver) = std::sync::mpsc::channel();
        *core.thread_request_sender.lock().unwrap() = Some(sender);
        core.resume_visible_thread_delivery();
        let requests: Vec<_> = receiver.try_iter().collect();
        assert_eq!(requests.len(), 1);
        assert!(
            matches!(&requests[0], ThreadControllerRequest::Subscribe { id, .. } if id == "visible")
        );
        core.shutdown();
        core.resume_visible_thread_delivery();
        assert_eq!(receiver.try_iter().count(), 0);
    }

    fn thread(thread_id: &str, workspace_id: &str) -> Thread {
        Thread {
            workspace_id: workspace_id.to_owned(),
            id: thread_id.to_owned(),
            name: None,
            preview: String::new(),
            preview_author: None,
            mode: ThreadMode::Chat,
            model: "gpt-5.4".to_owned(),
            model_provider: "openai".to_owned(),
            reasoning_effort: None,
            created_at: 1,
            updated_at: 2,
            status: ThreadStatus::Idle,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            visibility: None,
            turns: Vec::new(),
        }
    }

    fn reconciliation_history() -> Vec<TimelineBlock> {
        (0..280)
            .map(|ix| {
                let mut block = presentation_page(&[("placeholder", "cached")])
                    .blocks
                    .remove(0);
                block.block_id = format!("block-{ix}");
                block.turn_id = Some(format!("turn-{ix}"));
                block.sort_key = format!("{ix:05}:a");
                block
            })
            .collect()
    }

    fn reconciliation_result(ix: usize) -> TimelineBlock {
        let mut block = reconciliation_history().remove(ix);
        block.block_id = format!("result-{ix}");
        block.sort_key = format!("{ix:05}:b");
        block.kind = TimelineBlockKind::AssistantMessage {
            html_publications: Vec::new(),
            item_id: format!("task-result-{ix}"),
            text: "child result".into(),
            status: TurnWorkItemStatus::Completed,
            markdown: None,
            author: None,
            route: None,
        };
        block
    }

    // Test cursors model the server's opaque boundary tokens. Only the fake
    // transport decodes them; production receives and forwards them unchanged.
    fn reconciliation_cursor(key: &str) -> TimelineCursor {
        TimelineCursor {
            value: format!("issued:{key}"),
        }
    }

    fn reconciliation_page(
        blocks: Vec<TimelineBlock>,
        before: bool,
        after: bool,
    ) -> ThreadTimelinePageResponse {
        ThreadTimelinePageResponse {
            page: TimelinePageInfo {
                before_cursor: blocks
                    .first()
                    .map(|block| reconciliation_cursor(&block.sort_key)),
                after_cursor: blocks
                    .last()
                    .map(|block| reconciliation_cursor(&block.sort_key)),
                has_more_before: before,
                has_more_after: after,
            },
            blocks,
            ..presentation_page(&[])
        }
    }

    fn reconciliation_server_page(
        history: &[TimelineBlock],
        params: &ThreadTimelinePageParams,
    ) -> ThreadTimelinePageResponse {
        let limit = params.limit.unwrap() as usize;
        let (start, end) = match &params.anchor {
            TimelinePageAnchor::Newest => (history.len().saturating_sub(limit), history.len()),
            TimelinePageAnchor::Around { cursor } => {
                let key = cursor.value.strip_prefix("issued:").unwrap();
                let at = history.partition_point(|block| block.sort_key.as_str() < key);
                let start = at.saturating_sub(limit / 2);
                (start, (start + limit).min(history.len()))
            }
            TimelinePageAnchor::After { cursor } => {
                let key = cursor.value.strip_prefix("issued:").unwrap();
                let start = history.partition_point(|block| block.sort_key.as_str() <= key);
                (start, (start + limit).min(history.len()))
            }
            _ => panic!("unexpected reconciliation anchor"),
        };
        reconciliation_page(history[start..end].to_vec(), start > 0, end < history.len())
    }

    struct ReconciliationTransport<F> {
        calls: std::cell::RefCell<Vec<ThreadTimelinePageParams>>,
        reply: F,
    }
    impl<F> crate::rpc::JsonRpcRequestTransport for ReconciliationTransport<F>
    where
        F: Fn(usize, &ThreadTimelinePageParams) -> Result<ThreadTimelinePageResponse, String>,
    {
        fn send_json_rpc_request(
            &self,
            _: String,
            payload: String,
            response: crate::rpc::JsonRpcResponseSender,
        ) -> Result<(), String> {
            let request: serde_json::Value = serde_json::from_str(&payload).unwrap();
            assert_eq!(request["method"], "thread/timeline/page");
            let params: ThreadTimelinePageParams =
                serde_json::from_value(request["params"].clone()).unwrap();
            let ix = self.calls.borrow().len();
            self.calls.borrow_mut().push(params.clone());
            let page = (self.reply)(ix, &params)?;
            response
                .send(Ok(serde_json::to_value(page).unwrap()))
                .unwrap();
            Ok(())
        }
    }

    fn reconciliation_core() -> (
        Arc<ClientCore>,
        std::sync::mpsc::Receiver<ThreadControllerRequest>,
    ) {
        let core = core();
        core.upsert_thread(thread("a", "ws"));
        core.apply_thread_timeline_page(
            reconciliation_page(reconciliation_history()[80..=210].to_vec(), true, true),
            crate::timeline::semantic::TopLevelPageMergeMode::Reset,
        );
        let (sender, receiver) = std::sync::mpsc::channel();
        *core.thread_request_sender.lock().unwrap() = Some(sender);
        (core, receiver)
    }

    fn reconciliation_request(
        receiver: &std::sync::mpsc::Receiver<ThreadControllerRequest>,
    ) -> ThreadSemanticRequest {
        let ThreadControllerRequest::Semantic(request) = receiver.try_recv().unwrap() else {
            panic!("expected timeline request")
        };
        request
    }

    fn reconciliation_notification(ids: &[&str]) -> GatewayNotification {
        GatewayNotification::ThreadTimelineBlocksChanged(ThreadTimelineBlocksChangedNotification {
            workspace_id: "ws".into(),
            thread_id: "a".into(),
            changed_block_ids: ids.iter().map(|id| (*id).into()).collect(),
            removed_block_ids: vec![],
            turn_work_tombstones: vec![],
            before_cursor: None,
            after_cursor: None,
            reason: TimelineChangeReason::LiveEvent,
        })
    }

    #[test]
    fn changed_old_parent_range_discovers_unknown_and_boundary_results_without_duplicates() {
        let (core, receiver) = reconciliation_core();
        let _lease = core.subscribe(
            ClientScope::Timeline {
                thread_id: "a".into(),
            },
            NonZeroUsize::new(16).unwrap(),
        );
        core.materialize_demanded_timelines();
        let anchor = core
            .thread_presentation_snapshot("a")
            .unwrap()
            .timeline()
            .rows()[60]
            .id()
            .clone();
        core.set_thread_turn_work_expanded("a", "turn-140", true);
        let before = core.thread_semantic_snapshot("a");
        assert!(
            before
                .thread("a")
                .unwrap()
                .top_level
                .block("result-140")
                .is_none()
        );
        core.apply_thread_notification(reconciliation_notification(&[
            "result-80",
            "result-140",
            "result-210",
        ]));
        core.apply_thread_notification(reconciliation_notification(&[
            "result-80",
            "result-140",
            "result-210",
        ]));
        let request = reconciliation_request(&receiver);
        assert_eq!(receiver.try_iter().count(), 0);
        let owner = request.request_generation;
        let mut history = reconciliation_history();
        history.extend([
            reconciliation_result(80),
            reconciliation_result(140),
            reconciliation_result(210),
        ]);
        history.sort_by(|a, b| a.sort_key.cmp(&b.sort_key));
        let transport = ReconciliationTransport {
            calls: Default::default(),
            reply: |ix: usize, params: &ThreadTimelinePageParams| {
                assert_eq!(core.thread_semantic_in_flight("a").len(), 1);
                let page = reconciliation_server_page(&history, params);
                if ix == 0 {
                    assert_eq!(page.blocks.len(), 12);
                    assert!(
                        !page
                            .blocks
                            .iter()
                            .any(|block| block.block_id.starts_with("result-"))
                    );
                }
                if ix == 1 {
                    // Repeated notifications during a walk coalesce into one trailing
                    // operation. The current continuation keeps its original owner.
                    core.apply_thread_notification(reconciliation_notification(&["block-120"]));
                    core.apply_thread_notification(reconciliation_notification(&["block-120"]));
                    let mut live = reconciliation_history().remove(140);
                    live.updated_at_unix_ms = Some(100);
                    if let TimelineBlockKind::UserMessage { text, revision, .. } = &mut live.kind {
                        *text = "newer live edit".into();
                        *revision = 1;
                    }
                    core.apply_thread_timeline_page(
                        reconciliation_page(vec![live], true, true),
                        crate::timeline::semantic::TopLevelPageMergeMode::Merge,
                    );
                }
                if ix < 4 {
                    let registry = core.thread_registry.lock().unwrap();
                    assert!(registry.timeline.accepts(
                        &SemanticTimelineRequestKey::ThreadNewest {
                            thread_id: "a".into()
                        },
                        owner
                    ));
                }
                Ok(page)
            },
        };
        core.execute_thread_semantic_request(&transport, request);
        let calls = transport.calls.borrow();
        assert!(matches!(calls[0].anchor, TimelinePageAnchor::Newest));
        assert!(
            matches!(calls[1].anchor, TimelinePageAnchor::Around { ref cursor } if cursor == &before.thread("a").unwrap().top_level.loaded_range.after_cursor.clone().unwrap())
        );
        assert!(
            matches!(calls[2].anchor, TimelinePageAnchor::Around { ref cursor } if cursor == &before.thread("a").unwrap().top_level.loaded_range.before_cursor.clone().unwrap())
        );
        assert!(matches!(calls[3].anchor, TimelinePageAnchor::After { .. }));
        assert!(
            matches!(calls[4].anchor, TimelinePageAnchor::Newest),
            "the first walk stops at the frozen cached boundary, before the refreshed tail"
        );
        assert_eq!(
            calls
                .iter()
                .filter(|params| params.anchor == TimelinePageAnchor::Newest)
                .count(),
            2
        );
        assert!(calls.len() <= 10);
        let semantic = core.thread_semantic_snapshot("a");
        let cached = semantic.thread("a").unwrap();
        for id in ["result-80", "result-140", "result-210"] {
            assert_eq!(
                cached
                    .top_level
                    .ordered_block_ids
                    .iter()
                    .filter(|block_id| block_id.as_str() == id)
                    .count(),
                1
            );
        }
        assert!(
            matches!(&cached.top_level.block("block-140").unwrap().kind, TimelineBlockKind::UserMessage { text, .. } if text == "newer live edit")
        );
        assert!(cached.expansion.expanded_turn_work.contains("turn-140"));
        assert_eq!(
            cached.top_level.request_status,
            crate::timeline::semantic::TimelineRequestStatus::Ready
        );
        assert!(core.thread_semantic_in_flight("a").is_empty());
        core.materialize_demanded_timelines();
        assert!(
            core.thread_presentation_snapshot("a")
                .unwrap()
                .timeline()
                .rows()
                .iter()
                .any(|row| row.id() == &anchor)
        );
    }

    #[test]
    fn canonical_pages_accept_lower_server_timestamps_and_preserve_live_changes_after_begin() {
        use crate::timeline::semantic::{TimelineRequestStatus, TopLevelPageMergeMode};
        let (core, receiver) = reconciliation_core();
        let mut completed = reconciliation_result(140);
        completed.block_id = "turn:turn-140:assistant:task-result-140".into();
        completed.sort_key = "00140:turn-140:200:task-result-140".into();
        if let TimelineBlockKind::AssistantMessage { text, .. } = &mut completed.kind {
            *text = "full canonical completed answer".into();
        }
        let mut concurrent = reconciliation_result(150);
        concurrent.block_id = "turn:turn-150:assistant:task-result-150".into();
        concurrent.sort_key = "00150:turn-150:200:task-result-150".into();
        let partial = |block: &TimelineBlock| {
            let mut cached = block.clone();
            cached.updated_at_unix_ms = Some(1_000);
            if let TimelineBlockKind::AssistantMessage { text, status, .. } = &mut cached.kind {
                *text = "partial".into();
                *status = TurnWorkItemStatus::Running;
            }
            cached
        };
        let mut cached_user = reconciliation_history().remove(130);
        cached_user.updated_at_unix_ms = Some(1_000);
        if let TimelineBlockKind::UserMessage { revision, .. } = &mut cached_user.kind {
            *revision = 1;
        }
        core.apply_thread_timeline_page(
            reconciliation_page(
                vec![partial(&completed), partial(&concurrent), cached_user],
                true,
                true,
            ),
            TopLevelPageMergeMode::Merge,
        );
        let delta = |turn_id: &str, item_id: &str, text: &str| ConversationEvent::ItemDelta {
            thread_id: "a".into(),
            turn_id: turn_id.into(),
            item_id: item_id.into(),
            delta: text.into(),
            stream: Some(ItemDeltaStream::AgentMessage),
            payload: None,
            markdown: None,
            markdown_version: None,
        };
        // A delayed intermediate event uses the client's clock, although the
        // server has already persisted the complete answer below.
        core.apply_thread_conversation_event(
            "ws",
            delta("turn-140", "task-result-140", " delayed"),
            None,
        );
        let before = core.thread_semantic_snapshot("a");
        assert!(
            before
                .thread("a")
                .unwrap()
                .top_level
                .block(&completed.block_id)
                .unwrap()
                .updated_at_unix_ms
                > completed.updated_at_unix_ms
        );
        let mut history = reconciliation_history();
        if let TimelineBlockKind::UserMessage { revision, text, .. } = &mut history[130].kind {
            *revision = 2;
            *text = "canonical edit".into();
        }
        history.extend([completed.clone(), concurrent.clone()]);
        history.sort_by(|a, b| a.sort_key.cmp(&b.sort_key));
        core.apply_thread_notification(reconciliation_notification(&[
            &completed.block_id,
            "block-130",
        ]));
        let request = reconciliation_request(&receiver);
        let transport = ReconciliationTransport {
            calls: Default::default(),
            reply: |ix: usize, params: &ThreadTimelinePageParams| {
                let page = reconciliation_server_page(&history, params);
                if ix == 0 {
                    core.apply_thread_conversation_event(
                        "ws",
                        delta("turn-150", "task-result-150", " during read"),
                        None,
                    );
                }
                Ok(page)
            },
        };
        core.execute_thread_semantic_request(&transport, request);
        let after = core.thread_semantic_snapshot("a");
        let cached = &after.thread("a").unwrap().top_level;
        assert_eq!(cached.block(&completed.block_id), Some(&completed));
        assert!(matches!(&cached.block(&concurrent.block_id).unwrap().kind,
            TimelineBlockKind::AssistantMessage { text, status: TurnWorkItemStatus::Running, .. }
                if text == "partial during read"));
        let user = cached.block("block-130").unwrap();
        assert_eq!(user.updated_at_unix_ms, Some(2));
        assert!(
            matches!(&user.kind, TimelineBlockKind::UserMessage { revision: 2, text, .. } if text == "canonical edit")
        );
        assert_eq!(cached.request_status, TimelineRequestStatus::Ready);
        assert!(core.thread_semantic_in_flight("a").is_empty());
        assert_eq!(receiver.try_iter().count(), 0);
    }

    #[test]
    fn missed_result_is_read_only_after_subscription_ack_with_cached_or_initial_timeline() {
        for cached in [false, true] {
            let (core, receiver) = reconciliation_core();
            if !cached {
                core.apply_thread_timeline_page(
                    presentation_page(&[]),
                    crate::timeline::semantic::TopLevelPageMergeMode::Reset,
                );
            }
            struct AckTransport<'a> {
                receiver: &'a std::sync::mpsc::Receiver<ThreadControllerRequest>,
                fail: bool,
            }
            impl crate::rpc::JsonRpcRequestTransport for AckTransport<'_> {
                fn send_json_rpc_request(
                    &self,
                    _: String,
                    _: String,
                    response: crate::rpc::JsonRpcResponseSender,
                ) -> Result<(), String> {
                    assert_eq!(
                        self.receiver.try_iter().count(),
                        0,
                        "no read before acknowledgement"
                    );
                    if self.fail {
                        return Err("subscription failed".into());
                    }
                    response
                        .send(Ok(serde_json::to_value(ThreadStartResponse {
                            thread: thread("a", "ws"),
                            sandbox: SandboxPolicy::from_mode(SandboxMode::FullAccess),
                        })
                        .unwrap()))
                        .unwrap();
                    Ok(())
                }
            }
            let token = || ThreadOperationToken {
                id: "a".into(),
                generation: core.thread_registry.lock().unwrap().stores["a"].generation,
            };
            if cached {
                // A new confirmed connection can recover even when notifications
                // have been stopped by a previous failed refresh.
                core.apply_thread_notification(reconciliation_notification(&["result-140"]));
                let request = reconciliation_request(&receiver);
                core.execute_thread_semantic_request(
                    &ReconciliationTransport {
                        calls: Default::default(),
                        reply: |_: usize, _: &ThreadTimelinePageParams| {
                            Err("previous connection read failed".into())
                        },
                    },
                    request,
                );
                core.apply_thread_notification(reconciliation_notification(&["block-130"]));
                assert_eq!(receiver.try_iter().count(), 0);
                assert!(matches!(
                    core.thread_semantic_snapshot("a")
                        .thread("a")
                        .unwrap()
                        .top_level
                        .request_status,
                    crate::timeline::semantic::TimelineRequestStatus::Failed { .. }
                ));
            }
            assert!(
                core.refresh_opened_thread_subscription(
                    &AckTransport {
                        receiver: &receiver,
                        fail: true
                    },
                    token(),
                    "ws"
                )
                .is_err()
            );
            assert_eq!(receiver.try_iter().count(), 0);
            core.refresh_opened_thread_subscription(
                &AckTransport {
                    receiver: &receiver,
                    fail: false,
                },
                token(),
                "ws",
            )
            .unwrap();
            let request = reconciliation_request(&receiver);
            let result_ix = if cached { 140 } else { 279 };
            let mut history = reconciliation_history();
            history.push(reconciliation_result(result_ix));
            history.sort_by(|a, b| a.sort_key.cmp(&b.sort_key));
            let transport = ReconciliationTransport {
                calls: Default::default(),
                reply: |_: usize, params: &ThreadTimelinePageParams| {
                    Ok(reconciliation_server_page(&history, params))
                },
            };
            core.execute_thread_semantic_request(&transport, request);
            assert!(
                core.thread_semantic_snapshot("a")
                    .thread("a")
                    .unwrap()
                    .top_level
                    .block(&format!("result-{result_ix}"))
                    .is_some()
            );
            assert_eq!(transport.calls.borrow().len(), if cached { 4 } else { 1 });
            assert_eq!(
                core.thread_semantic_snapshot("a")
                    .thread("a")
                    .unwrap()
                    .top_level
                    .request_status,
                crate::timeline::semantic::TimelineRequestStatus::Ready
            );
        }
    }

    #[test]
    fn cached_range_failures_stop_and_existing_retry_schedules_a_new_owned_walk() {
        use crate::timeline::{
            controller::{TimelineDemand, TimelineIntent},
            semantic::TimelineRequestStatus,
        };
        for failure in ["rpc", "cursor", "sort", "budget"] {
            let (core, receiver) = reconciliation_core();
            let _lease = core.subscribe(
                ClientScope::Timeline {
                    thread_id: "a".into(),
                },
                NonZeroUsize::new(16).unwrap(),
            );
            core.materialize_demanded_timelines();
            let timeline = core.thread_presentation_snapshot("a").unwrap().timeline();
            let mut demand = TimelineDemand {
                thread_id: "a".into(),
                consumer_id: "review".into(),
                generation: 1,
                source_revision: timeline.source_revision(),
                row_ids: vec![timeline.rows()[60].id().as_str().into()],
                threshold: 0,
                before: false,
                after: false,
                work: false,
                presented_rows: false,
                scroll_generation: 17,
                latest_user_turn_id: None,
                viewed_through_turn_id: None,
                read_requires_unread: false,
                prefetch_on_visibility: false,
                boundary_request_limit: 1,
            };
            core.plan_timeline_intent(
                TimelineIntent::Update {
                    demand: demand.clone(),
                },
                0,
            );
            core.apply_thread_notification(reconciliation_notification(&["result-140"]));
            let request = reconciliation_request(&receiver);
            let owner = request.request_generation;
            let history = reconciliation_history();
            let transport = ReconciliationTransport {
                calls: Default::default(),
                reply: |ix: usize, params: &ThreadTimelinePageParams| {
                    if ix < 2 {
                        return Ok(reconciliation_server_page(&history, params));
                    }
                    if ix == 2 {
                        core.apply_thread_notification(reconciliation_notification(&["block-120"]));
                        core.apply_thread_notification(reconciliation_notification(&["block-120"]));
                    }
                    if failure == "rpc" {
                        return Err("range read failed".into());
                    }
                    let mut block = history[80].clone();
                    block.sort_key = if failure == "sort" {
                        "00081:a".into()
                    } else {
                        format!("{:05}:a", 80 + ix)
                    };
                    let mut page = reconciliation_page(vec![block], true, true);
                    if failure == "cursor" {
                        page.page.after_cursor = Some(reconciliation_cursor("stuck"));
                    }
                    Ok(page)
                },
            };
            core.execute_thread_semantic_request(&transport, request);
            let expected = match failure {
                "rpc" => 3,
                "cursor" | "sort" => 4,
                _ => CACHED_TIMELINE_REQUEST_BUDGET,
            };
            assert_eq!(transport.calls.borrow().len(), expected);
            assert!(core.thread_semantic_in_flight("a").is_empty());
            assert_eq!(receiver.try_iter().count(), 0);
            assert!(matches!(
                core.thread_semantic_snapshot("a")
                    .thread("a")
                    .unwrap()
                    .top_level
                    .request_status,
                TimelineRequestStatus::Failed { .. }
            ));
            // A notification or unrelated successful pagination cannot acknowledge
            // recovery of the interval that failed.
            let failed = core
                .thread_semantic_snapshot("a")
                .thread("a")
                .unwrap()
                .top_level
                .request_status
                .clone();
            for id in ["block-130", "block-131", "result-150"] {
                core.apply_thread_notification(reconciliation_notification(&[id]));
                assert_eq!(
                    receiver.try_iter().count(),
                    0,
                    "notifications cannot restart a failed walk"
                );
                assert!(core.thread_semantic_in_flight("a").is_empty());
                let semantic = core.thread_semantic_snapshot("a");
                let cached = &semantic.thread("a").unwrap().top_level;
                assert!(
                    cached.stale_block_ids.contains(id),
                    "invalidation is retained"
                );
                assert_eq!(cached.request_status, failed);
            }
            let request = core
                .begin_thread_semantic_request(SemanticTimelineRequestAction::ThreadTimelinePage {
                    key: SemanticTimelineRequestKey::ThreadAfter {
                        thread_id: "a".into(),
                        cursor: "issued:00279:a".into(),
                    },
                    params: ThreadTimelinePageParams {
                        thread_id: "a".into(),
                        anchor: TimelinePageAnchor::After {
                            cursor: reconciliation_cursor("00279:a"),
                        },
                        limit: Some(12),
                    },
                })
                .unwrap();
            // begin also shares the top-level status, so a boundary request must
            // preserve the failed refresh before and after its response.
            core.execute_thread_semantic_request(
                &ReconciliationTransport {
                    calls: Default::default(),
                    reply: |_: usize, _: &ThreadTimelinePageParams| {
                        Ok(reconciliation_page(vec![], true, false))
                    },
                },
                request,
            );
            assert!(matches!(
                core.thread_semantic_snapshot("a")
                    .thread("a")
                    .unwrap()
                    .top_level
                    .request_status,
                TimelineRequestStatus::Failed { .. }
            ));
            core.materialize_demanded_timelines();
            demand.source_revision = core
                .thread_presentation_snapshot("a")
                .unwrap()
                .timeline()
                .source_revision();
            core.plan_timeline_intent(TimelineIntent::Update { demand }, 1);
            let retry = core.plan_timeline_intent(
                TimelineIntent::Retry {
                    thread_id: "a".into(),
                    consumer_id: "review".into(),
                    generation: 1,
                },
                2,
            );
            assert_eq!(retry.pages.len(), 1);
            assert!(retry.pages[0].generation > owner);
            core.schedule_planned_timeline_request(retry.pages[0].clone());
            let request = reconciliation_request(&receiver);
            assert_eq!(receiver.try_iter().count(), 0, "Retry queues one walk");
            let mut history = reconciliation_history();
            history.extend([reconciliation_result(140), reconciliation_result(150)]);
            history.sort_by(|a, b| a.sort_key.cmp(&b.sort_key));
            core.execute_thread_semantic_request(
                &ReconciliationTransport {
                    calls: Default::default(),
                    reply: |_: usize, params: &ThreadTimelinePageParams| {
                        Ok(reconciliation_server_page(&history, params))
                    },
                },
                request,
            );
            assert_eq!(
                core.thread_semantic_snapshot("a")
                    .thread("a")
                    .unwrap()
                    .top_level
                    .request_status,
                TimelineRequestStatus::Ready
            );
            assert!(
                core.thread_semantic_snapshot("a")
                    .thread("a")
                    .unwrap()
                    .top_level
                    .block("result-140")
                    .is_some()
            );
            assert!(
                core.thread_semantic_snapshot("a")
                    .thread("a")
                    .unwrap()
                    .top_level
                    .block("result-150")
                    .is_some()
            );
        }
    }

    #[test]
    fn replacement_owner_rejects_late_range_response_and_its_continuation() {
        for retire_at in [0, 2] {
            let (core, receiver) = reconciliation_core();
            core.apply_thread_notification(reconciliation_notification(&["previous-result"]));
            let failed_request = reconciliation_request(&receiver);
            core.execute_thread_semantic_request(
                &ReconciliationTransport {
                    calls: Default::default(),
                    reply: |_: usize, _: &ThreadTimelinePageParams| {
                        Err("old owner read failed".into())
                    },
                },
                failed_request,
            );
            // Failure belongs to the old store. A new owner's notification can
            // start recovery normally, before testing another late response.
            core.remove_thread_store("a");
            core.upsert_thread(thread("a", "ws"));
            core.apply_thread_timeline_page(
                reconciliation_page(reconciliation_history()[80..=210].to_vec(), true, true),
                crate::timeline::semantic::TopLevelPageMergeMode::Reset,
            );
            core.apply_thread_notification(reconciliation_notification(&["result-140"]));
            let request = reconciliation_request(&receiver);
            let replacement = std::cell::RefCell::new(None);
            let history = reconciliation_history();
            let transport = ReconciliationTransport {
                calls: Default::default(),
                reply: |ix: usize, params: &ThreadTimelinePageParams| {
                    let mut page = reconciliation_server_page(&history, params);
                    if ix == retire_at {
                        core.remove_thread_store("a");
                        core.upsert_thread(thread("a", "ws"));
                        core.refresh_thread_timeline("a");
                        *replacement.borrow_mut() = Some(reconciliation_request(&receiver));
                        page.blocks.push(reconciliation_result(140));
                    }
                    Ok(page)
                },
            };
            core.execute_thread_semantic_request(&transport, request);
            assert_eq!(transport.calls.borrow().len(), retire_at + 1);
            assert!(
                core.thread_semantic_snapshot("a")
                    .thread("a")
                    .unwrap()
                    .top_level
                    .block("result-140")
                    .is_none()
            );
            assert_eq!(
                core.thread_semantic_in_flight("a").len(),
                1,
                "replacement owner is still active"
            );
            core.execute_thread_semantic_request(
                &ReconciliationTransport {
                    calls: Default::default(),
                    reply: |_: usize, params: &ThreadTimelinePageParams| {
                        Ok(reconciliation_server_page(&history, params))
                    },
                },
                replacement.borrow_mut().take().unwrap(),
            );
            assert!(core.thread_semantic_in_flight("a").is_empty());
        }
    }

    struct CancelTransport<'a>(&'a ClientCore);
    impl crate::rpc::JsonRpcRequestTransport for CancelTransport<'_> {
        fn send_json_rpc_request(
            &self,
            _: String,
            _: String,
            response: crate::rpc::JsonRpcResponseSender,
        ) -> Result<(), String> {
            self.0.remove_thread_store("a");
            response.send(Ok(serde_json::json!({"workspaceId":"ws","threadId":"a","projectionVersion":1,"blocks":[],"page":{"hasMoreBefore":false,"hasMoreAfter":false}}))).unwrap();
            Ok(())
        }
    }
    struct WrongTurnTransport;
    impl crate::rpc::JsonRpcRequestTransport for WrongTurnTransport {
        fn send_json_rpc_request(
            &self,
            _: String,
            _: String,
            response: crate::rpc::JsonRpcResponseSender,
        ) -> Result<(), String> {
            response.send(Ok(serde_json::json!({"workspaceId":"ws","threadId":"a","turnId":"other","projectionVersion":1,"items":[],"page":{"hasMoreBefore":false,"hasMoreAfter":false}}))).unwrap();
            Ok(())
        }
    }
    #[test]
    fn exact_work_refresh_batches_ids_without_changing_scope_or_reducing_in_the_shell() {
        struct Transport(std::cell::RefCell<Vec<usize>>);
        impl crate::rpc::JsonRpcRequestTransport for Transport {
            fn send_json_rpc_request(
                &self,
                _: String,
                payload: String,
                response: crate::rpc::JsonRpcResponseSender,
            ) -> Result<(), String> {
                let request: serde_json::Value = serde_json::from_str(&payload).unwrap();
                self.0
                    .borrow_mut()
                    .push(request["params"]["workItemIds"].as_array().unwrap().len());
                response.send(Ok(serde_json::json!({"workspaceId":"ws", "threadId":"a", "turnId":"turn", "projectionVersion":1, "items":[]}))).unwrap();
                Ok(())
            }
        }
        let core = core();
        core.upsert_thread(thread("a", "ws"));
        let request = core
            .begin_thread_semantic_request(SemanticTimelineRequestAction::TurnWorkItemsGet {
                key: SemanticTimelineRequestKey::TurnWorkItems {
                    thread_id: "a".into(),
                    turn_id: "turn".into(),
                },
                params: TurnWorkItemsGetParams {
                    thread_id: "a".into(),
                    turn_id: "turn".into(),
                    work_item_ids: (0..405).map(|ix| format!("item-{ix}")).collect(),
                },
            })
            .unwrap();
        let transport = Transport(Default::default());
        core.execute_thread_semantic_request(&transport, request);
        assert_eq!(*transport.0.borrow(), [200, 200, 5]);
        assert!(core.thread_semantic_in_flight("a").is_empty());
    }

    #[test]
    fn semantic_worker_rejects_another_turn_in_the_same_thread() {
        for items in [false, true] {
            let core = core();
            core.upsert_thread(thread("a", "ws"));
            let action = if items {
                SemanticTimelineRequestAction::TurnWorkItemsGet {
                    key: SemanticTimelineRequestKey::TurnWorkItems {
                        thread_id: "a".into(),
                        turn_id: "expected".into(),
                    },
                    params: TurnWorkItemsGetParams {
                        thread_id: "a".into(),
                        turn_id: "expected".into(),
                        work_item_ids: vec!["item".into()],
                    },
                }
            } else {
                SemanticTimelineRequestAction::TurnWorkPage {
                    key: SemanticTimelineRequestKey::TurnWorkInitial {
                        thread_id: "a".into(),
                        turn_id: "expected".into(),
                    },
                    params: TurnWorkPageParams {
                        thread_id: "a".into(),
                        turn_id: "expected".into(),
                        anchor: TimelinePageAnchor::Newest,
                        limit: None,
                    },
                }
            };
            let request = core.begin_thread_semantic_request(action).unwrap();
            let before = core.thread_snapshot("a").unwrap();
            core.execute_thread_semantic_request(&WrongTurnTransport, request);
            let snapshot = core.thread_snapshot("a").unwrap();
            let semantic = snapshot.semantic.thread("a").unwrap();
            assert!(!semantic.work_ranges_by_turn.contains_key("other"));
            assert_eq!(
                semantic.work_ranges_by_turn["expected"].request_status,
                before.semantic.thread("a").unwrap().work_ranges_by_turn["expected"].request_status
            );
            assert_eq!(snapshot.revision(), before.revision());
            assert_eq!(core.thread_semantic_in_flight("a").len(), 1);
        }
    }
    #[test]
    fn late_page_response_cannot_resurrect_removed_store() {
        let core = core();
        core.upsert_thread(thread("a", "ws"));
        let result = core.fetch_thread_timeline_page(
            &CancelTransport(&core),
            ThreadTimelinePageParams {
                thread_id: "a".into(),
                anchor: TimelinePageAnchor::Newest,
                limit: None,
            },
        );
        assert!(result.unwrap_err().to_string().contains("cancelled"));
        assert!(core.thread_snapshot("a").is_none());
    }
    #[test]
    fn cancellation_restores_idle_and_invalidates_queued_request() {
        let core = core();
        core.upsert_thread(thread("a", "ws"));
        let action = SemanticTimelineRequestAction::ThreadTimelinePage {
            key: SemanticTimelineRequestKey::ThreadNewest {
                thread_id: "a".into(),
            },
            params: ThreadTimelinePageParams {
                thread_id: "a".into(),
                anchor: TimelinePageAnchor::Newest,
                limit: None,
            },
        };
        let request = core.begin_thread_semantic_request(action).unwrap();
        core.cancel_thread_requests();
        // This transport removes a if called; a cancelled queued request must never call it.
        core.execute_thread_semantic_request(&CancelTransport(&core), request);
        let snapshot = core.thread_snapshot("a").unwrap();
        assert_eq!(
            snapshot
                .semantic
                .thread("a")
                .unwrap()
                .top_level
                .request_status,
            crate::timeline::semantic::TimelineRequestStatus::Idle
        );
        assert!(core.thread_semantic_in_flight("a").is_empty());
    }
    #[test]
    fn cold_store_rejects_other_workspace_and_republishes_monotonic_revisions() {
        let core = core();
        core.upsert_thread(thread("a", "ws"));
        for index in 0..4 {
            let mut t = thread("a", "ws");
            t.preview = index.to_string();
            core.upsert_thread(t);
        }
        for index in 0..40 {
            core.upsert_thread(thread(&format!("cold-{index}"), "ws"));
        }
        assert!(core.thread_snapshot("a").is_none());
        assert!(core.thread_mutation("a", "other").is_none());
        let before = core
            .snapshot(&ClientScope::Thread {
                thread_id: "a".into(),
            })
            .unwrap()
            .revisions();
        core.upsert_thread(thread("a", "ws"));
        let after = core
            .snapshot(&ClientScope::Thread {
                thread_id: "a".into(),
            })
            .unwrap()
            .revisions();
        assert!(after.domain() > before.domain());
        assert!(after.presentation() > before.presentation());
    }

    fn running_turn(turn_id: &str) -> Turn {
        Turn {
            id: turn_id.to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: TurnKind::Conversation,
            origin: TurnOrigin::User,
            mode: Default::default(),
            author: None,
            reply_to_turn_id: None,
            mentions: Vec::new(),
            message_revision: 0,
            message_deleted: false,
            error: None,
            prompt_manifest: None,
            permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
        }
    }

    fn recovered_running_thread() -> Thread {
        let mut snapshot = thread("a", "ws");
        snapshot.status = ThreadStatus::Active;
        let mut later = running_turn("later-terminal");
        later.status = TurnStatus::Completed;
        snapshot.turns = vec![running_turn("original"), later];
        snapshot
    }

    #[test]
    fn recovered_turn_snapshot_completed_agent_child_restores_original_control_immediately() {
        let core = core();
        // Concurrent Agent-authored child Turns are ordinary Conversation
        // Turns in the parent's thread and can finish before the parent.
        let mut original = running_turn("original");
        original.mode = ThreadMode::Agent;
        let mut child = running_turn("child");
        child.mode = ThreadMode::Agent;
        child.author = Some(TurnAuthorSnapshot {
            actor: PersistedActorRef::System,
            display_name: "Child Agent".into(),
            nickname: "child-agent".into(),
            avatar_revision: None,
            agent: None,
        });
        let mut snapshot = thread("a", "ws");
        snapshot.mode = ThreadMode::Agent;
        snapshot.status = ThreadStatus::Active;
        snapshot.turns = vec![original, child];
        core.upsert_thread(snapshot.clone());
        assert_eq!(
            core.thread_coordinator_snapshot("a")
                .unwrap()
                .conversation
                .in_flight_turn_id(),
            Some("child")
        );
        snapshot.turns[1].status = TurnStatus::Completed;
        snapshot.turns[1].permission_profile =
            system_turn_permission_profile_snapshot(TurnPermissionMode::FullAccess);
        let update = GatewayNotification::ThreadUpdated(ThreadUpdatedNotification {
            thread: snapshot.clone(),
            placement: None,
        });
        core.apply_thread_notification(update.clone());
        let coordinator = core.thread_coordinator_snapshot("a").unwrap();
        assert_eq!(coordinator.thread(), Some(&snapshot));
        let active = crate::state::snapshot::ActiveThreadSnapshot::from_coordinator(
            Some("a"),
            Some(&coordinator),
            false,
        );
        assert_eq!(active.in_flight_turn_id.as_deref(), Some("original"));
        assert!(active.can_request_turn_cancel(true));
        assert!(!active.can_request_turn_cancel(false));
        let completed = coordinator
            .conversation
            .projection()
            .turns
            .iter()
            .find(|turn| turn.id == "child")
            .unwrap();
        assert_eq!(completed.phase, crate::conversation::TurnPhase::Completed);
        assert_eq!(
            completed.permission_profile.as_ref(),
            Some(&snapshot.turns[1].permission_profile)
        );

        // The first accepted snapshot already permits cancelling the original;
        // no later event is needed to restore the cancellation target.
        let (token, params) = core.begin_thread_turn_cancellation("a", None).unwrap();
        assert_eq!(params.thread_id, "a");
        assert_eq!(params.turn_id, "original");
        core.apply_thread_notification(update);
        let coordinator = core.thread_coordinator_snapshot("a").unwrap();
        assert_eq!(
            coordinator.conversation.in_flight_turn_id(),
            Some("original")
        );
        assert!(coordinator.conversation.is_cancelling_turn());
        assert_eq!(
            coordinator
                .conversation
                .projection()
                .turns
                .iter()
                .find(|turn| turn.id == "child")
                .unwrap()
                .phase,
            crate::conversation::TurnPhase::Completed
        );
        assert!(!core.finish_thread_turn_cancellation(&token, "child", None, true));
    }

    #[test]
    fn recovered_turn_snapshot_terminal_conversation_does_not_hide_cancel_target() {
        let core = core();
        let mut snapshot = recovered_running_thread();
        core.upsert_thread(snapshot.clone());
        assert_eq!(
            core.thread_coordinator_snapshot("a")
                .unwrap()
                .conversation
                .in_flight_turn_id(),
            Some("original")
        );
        // Lose Conversation while retaining the thread snapshot, then deliver
        // a differing ThreadUpdated through the production notification reducer.
        core.thread_mutation("a", "ws")
            .unwrap()
            .conversation_mutation()
            .reset();
        assert!(
            core.thread_coordinator_snapshot("a")
                .unwrap()
                .conversation
                .in_flight_turn_id()
                .is_none()
        );
        snapshot.preview = "recovered execution".into();
        core.apply_thread_notification(GatewayNotification::ThreadUpdated(
            ThreadUpdatedNotification {
                thread: snapshot,
                placement: None,
            },
        ));
        let (_, params) = core.begin_thread_turn_cancellation("a", None).unwrap();
        assert_eq!(params.thread_id, "a");
        assert_eq!(params.turn_id, "original");
    }

    #[test]
    fn recovered_turn_snapshot_preserves_pending_cancellation_and_accepts_response() {
        for rejected in [false, true] {
            let core = core();
            let mut snapshot = recovered_running_thread();
            core.upsert_thread(snapshot.clone());
            let (token, params) = core.begin_thread_turn_cancellation("a", None).unwrap();
            snapshot.preview = "activity during cancellation".into();
            core.apply_thread_notification(GatewayNotification::ThreadUpdated(
                ThreadUpdatedNotification {
                    thread: snapshot.clone(),
                    placement: None,
                },
            ));
            let coordinator = core.thread_coordinator_snapshot("a").unwrap();
            assert_eq!(
                coordinator.thread().unwrap().preview,
                snapshot.preview,
                "the differing running snapshot must actually be accepted"
            );
            assert!(coordinator.conversation.is_cancelling_turn());
            assert_eq!(
                coordinator.conversation.in_flight_turn_id(),
                Some("original")
            );
            assert!(core.begin_thread_turn_cancellation("a", None).is_none());
            assert!(!core.finish_thread_turn_cancellation(&token, "later-terminal", None, true));
            let mut wrong_generation = token.clone();
            wrong_generation.generation += 1;
            assert!(!core.finish_thread_turn_cancellation(
                &wrong_generation,
                &params.turn_id,
                None,
                true
            ));
            let event = if rejected {
                crate::turns::cancel::local_turn_cancel_rejected_event(
                    "a",
                    "original",
                    "RPC failed",
                )
            } else {
                let mut cancelled = running_turn("original");
                cancelled.status = TurnStatus::Interrupted;
                crate::turns::cancel::turn_cancel_response_event(TurnCancelResponse {
                    workspace_id: "ws".into(),
                    thread_id: "a".into(),
                    turn: cancelled,
                })
                .unwrap()
            };
            assert!(core.finish_thread_turn_cancellation(
                &token,
                &params.turn_id,
                Some(event),
                false
            ));
            let coordinator = core.thread_coordinator_snapshot("a").unwrap();
            assert!(!coordinator.conversation.is_cancelling_turn());
            if rejected {
                assert_eq!(
                    coordinator.conversation.in_flight_turn_id(),
                    Some("original")
                );
                assert!(core.begin_thread_turn_cancellation("a", None).is_some());
            } else {
                assert_eq!(coordinator.conversation.in_flight_turn_id(), None);
                assert!(coordinator.conversation.can_submit_message());
            }
        }
    }

    #[test]
    fn recovered_turn_snapshot_terminalizes_pending_cancellation() {
        for status in [TurnStatus::Interrupted, TurnStatus::Completed] {
            let core = core();
            let mut snapshot = recovered_running_thread();
            core.upsert_thread(snapshot.clone());
            let (token, params) = core.begin_thread_turn_cancellation("a", None).unwrap();
            snapshot.turns[0].status = status;
            core.apply_thread_notification(GatewayNotification::ThreadUpdated(
                ThreadUpdatedNotification {
                    thread: snapshot,
                    placement: None,
                },
            ));
            let coordinator = core.thread_coordinator_snapshot("a").unwrap();
            assert_eq!(coordinator.conversation.in_flight_turn_id(), None);
            assert!(!coordinator.conversation.is_cancelling_turn());
            assert!(coordinator.conversation.can_submit_message());
            assert_eq!(
                coordinator.conversation.status_label(),
                if status == TurnStatus::Interrupted {
                    "cancelled"
                } else {
                    "completed"
                }
            );
            assert!(!core.finish_thread_turn_cancellation(&token, &params.turn_id, None, true));
        }
    }

    struct ResumeTransport;
    impl crate::rpc::JsonRpcRequestTransport for ResumeTransport {
        fn send_json_rpc_request(
            &self,
            _: String,
            payload: String,
            response: crate::rpc::JsonRpcResponseSender,
        ) -> Result<(), String> {
            let request: serde_json::Value = serde_json::from_str(&payload).unwrap();
            let result = match request["method"].as_str().unwrap() {
                "turn/get" => {
                    let mut turn = running_turn("turn-b");
                    turn.status = TurnStatus::Completed;
                    serde_json::to_value(TurnGetResponse {
                        thread_id: "b".into(),
                        workspace_id: "ws".into(),
                        turn,
                    })
                    .unwrap()
                }
                "turn/items/page" => serde_json::to_value(TurnItemsResponse {
                    thread_id: "b".into(),
                    workspace_id: "ws".into(),
                    turn_id: "turn-b".into(),
                    events: vec![],
                    last_sequence: 0,
                    has_more: false,
                    next_cursor: None,
                })
                .unwrap(),
                method => panic!("unexpected synthetic RPC {method}"),
            };
            response.send(Ok(result)).unwrap();
            Ok(())
        }
    }
    #[test]
    fn resume_recovers_terminal_b_without_touching_a() {
        let core = core();
        core.upsert_thread(thread("a", "ws"));
        core.upsert_thread(thread("b", "ws"));
        core.activate_thread(Some("a"), Some("ws"));
        core.apply_thread_conversation_event(
            "ws",
            ConversationEvent::TurnStarted {
                thread_id: "b".into(),
                turn: running_turn("turn-b"),
            },
            None,
        );
        let a = core.thread_snapshot("a").unwrap();
        core.execute_thread_resume(&ResumeTransport, "b");
        assert!(Arc::ptr_eq(&a, &core.thread_snapshot("a").unwrap()));
        let b = core.thread_snapshot("b").unwrap();
        assert!(b.coordinator.conversation.in_flight_turn_id().is_none());
        assert!(!b.coordinator.resume.in_progress);
    }

    #[test]
    fn stale_terminal_delta_and_duplicate_page_publish_nothing() {
        let core = core();
        core.upsert_thread(thread("a", "ws"));
        let mut turn = running_turn("terminal");
        turn.status = TurnStatus::Completed;
        core.apply_thread_conversation_event(
            "ws",
            ConversationEvent::TurnCompleted {
                thread_id: "a".into(),
                turn,
            },
            None,
        );
        let page = ThreadTimelinePageResponse {
            workspace_id: "ws".into(),
            thread_id: "a".into(),
            projection_version: 4,
            blocks: vec![],
            page: Default::default(),
        };
        core.apply_thread_timeline_page(
            page.clone(),
            crate::timeline::semantic::TopLevelPageMergeMode::Merge,
        );
        let before = core.thread_snapshot("a").unwrap();
        let subscription = core.subscribe(scope("a"), NonZeroUsize::new(8).unwrap());
        core.apply_thread_timeline_page(
            page,
            crate::timeline::semantic::TopLevelPageMergeMode::Merge,
        );
        core.apply_thread_conversation_event(
            "ws",
            ConversationEvent::ItemDelta {
                thread_id: "a".into(),
                turn_id: "terminal".into(),
                item_id: "late".into(),
                delta: "late".into(),
                stream: None,
                payload: None,
                markdown: None,
                markdown_version: None,
            },
            None,
        );
        assert!(Arc::ptr_eq(&before, &core.thread_snapshot("a").unwrap()));
        assert!(subscription.try_next().is_none());
    }
    struct DraftTransport(std::sync::atomic::AtomicUsize);
    impl crate::rpc::JsonRpcRequestTransport for DraftTransport {
        fn send_json_rpc_request(
            &self,
            _: String,
            payload: String,
            response: crate::rpc::JsonRpcResponseSender,
        ) -> Result<(), String> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let request: serde_json::Value = serde_json::from_str(&payload).unwrap();
            let params: ThreadStartParams =
                serde_json::from_value(request["params"].clone()).unwrap();
            response
                .send(Ok(serde_json::to_value(ThreadStartResponse {
                    thread: thread(&params.thread_id, &params.workspace_id),
                    sandbox: SandboxPolicy::from_mode(SandboxMode::FullAccess),
                })
                .unwrap()))
                .unwrap();
            Ok(())
        }
    }
    #[test]
    fn draft_reply_after_thread_notification_hides_draft_and_promotion_restores_it() {
        struct NotifyingTransport(Arc<ClientCore>);
        impl crate::rpc::JsonRpcRequestTransport for NotifyingTransport {
            fn send_json_rpc_request(
                &self,
                _: String,
                payload: String,
                response: crate::rpc::JsonRpcResponseSender,
            ) -> Result<(), String> {
                let request: serde_json::Value = serde_json::from_str(&payload).unwrap();
                let params: ThreadStartParams =
                    serde_json::from_value(request["params"].clone()).unwrap();
                let thread = thread(&params.thread_id, &params.workspace_id);
                self.0.upsert_thread(thread.clone());
                assert!(
                    self.0
                        .workspace_tree("ws")
                        .unwrap()
                        .snapshot()
                        .threads_by_id
                        .contains_key(&params.thread_id)
                );
                response
                    .send(Ok(serde_json::to_value(ThreadStartResponse {
                        thread,
                        sandbox: SandboxPolicy::from_mode(SandboxMode::FullAccess),
                    })
                    .unwrap()))
                    .unwrap();
                Ok(())
            }
        }
        let core = core();
        core.activate_thread(None, Some("ws"));
        core.upsert_thread(thread("other", "other-workspace"));
        let other = core.workspace_tree("other-workspace").unwrap();
        let id = core.prepare_thread_draft().unwrap();
        core.create_workspace_thread_draft(
            &NotifyingTransport(core.clone()),
            "ws",
            ThreadVisibility::Private,
        )
        .unwrap();
        assert_eq!(
            core.thread_workspace_draft("ws").as_deref(),
            Some(id.as_str())
        );
        assert!(
            !core
                .workspace_tree("ws")
                .unwrap()
                .snapshot()
                .threads_by_id
                .contains_key(&id)
        );
        let scope = ClientScope::WorkspaceTree {
            workspace_id: Some("ws".into()),
        };
        let published = core
            .snapshot(&scope)
            .unwrap()
            .typed::<crate::workspaces::directory::ThreadTreePublication>()
            .unwrap()
            .payload();
        assert!(!published.snapshot().threads_by_id.contains_key(&id));
        assert!(core.promote_thread(&id));
        assert!(
            core.workspace_tree("ws")
                .unwrap()
                .snapshot()
                .threads_by_id
                .contains_key(&id)
        );
        let before = core.workspace_tree("ws").unwrap();
        assert!(!core.promote_thread(&id));
        assert!(Arc::ptr_eq(&before, &core.workspace_tree("ws").unwrap()));
        assert!(Arc::ptr_eq(
            &other,
            &core.workspace_tree("other-workspace").unwrap()
        ));
    }

    #[test]
    fn prepared_draft_keeps_identity_and_text_through_creation() {
        use crate::composer::store::ComposerIntent;
        let core = core();
        let navigation = core.subscribe(ClientScope::Navigation, NonZeroUsize::new(8).unwrap());
        let id = core.prepare_thread_draft().unwrap();
        assert_eq!(core.prepare_thread_draft().as_deref(), Some(id.as_str()));
        assert!(core.navigation_snapshot().active_thread_id().is_none());
        assert!(navigation.try_next().is_none());
        assert!(!core.thread_start_snapshot().in_progress);
        assert!(!core.thread_start_requested());
        core.composer_intent(ComposerIntent::Activate {
            thread_id: id.clone(),
        });
        let draft = core.composer_snapshot(&id).unwrap().draft_id();
        core.composer_intent(ComposerIntent::EditText {
            thread_id: id.clone(),
            draft_id: draft,
            text: "typed before bootstrap".into(),
        });
        core.activate_thread(None, Some("ws"));
        let transport = DraftTransport(Default::default());
        assert_eq!(
            core.create_workspace_thread_draft(&transport, "ws", ThreadVisibility::Private)
                .unwrap(),
            id
        );
        assert_eq!(
            core.navigation_snapshot().active_thread_id(),
            Some(id.as_str())
        );
        assert_eq!(core.prepare_thread_draft().as_deref(), Some(id.as_str()));
        let composer = core.composer_snapshot(&id).unwrap();
        assert_eq!(composer.draft_id(), draft);
        assert_eq!(composer.draft().text, "typed before bootstrap");
        assert_eq!(transport.0.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    #[test]
    fn prepared_draft_survives_failed_creation_for_explicit_retry() {
        struct FailedTransport;
        impl crate::rpc::JsonRpcRequestTransport for FailedTransport {
            fn send_json_rpc_request(
                &self,
                _: String,
                _: String,
                _: crate::rpc::JsonRpcResponseSender,
            ) -> Result<(), String> {
                Err("synthetic unavailable transport".into())
            }
        }
        let core = core();
        core.activate_thread(None, Some("ws"));
        let id = core.prepare_thread_draft().unwrap();
        assert!(
            core.create_workspace_thread_draft(&FailedTransport, "ws", ThreadVisibility::Private)
                .is_err()
        );
        assert!(!core.thread_start_snapshot().in_progress);
        assert!(!core.thread_start_requested());
        assert_eq!(core.prepare_thread_draft().as_deref(), Some(id.as_str()));
        let transport = DraftTransport(Default::default());
        assert_eq!(
            core.create_workspace_thread_draft(&transport, "ws", ThreadVisibility::Private)
                .unwrap(),
            id
        );
    }

    #[test]
    fn draft_creation_reuses_single_owner_and_promotion_preserves_thread() {
        let core = core();
        let transport = DraftTransport(Default::default());
        let id = core
            .create_workspace_thread_draft(&transport, "ws", ThreadVisibility::Private)
            .unwrap();
        assert_eq!(
            core.create_workspace_thread_draft(&transport, "ws", ThreadVisibility::Private)
                .unwrap(),
            id
        );
        assert_eq!(transport.0.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(
            core.thread_workspace_draft("ws").as_deref(),
            Some(id.as_str())
        );
        let snapshot = core.thread_snapshot(&id).unwrap();
        assert!(core.promote_thread(&id));
        assert!(Arc::ptr_eq(&snapshot, &core.thread_snapshot(&id).unwrap()));
        assert!(core.thread_workspace_draft("ws").is_none());
        assert!(!core.thread_start_snapshot().in_progress);
    }

    #[test]
    fn delayed_turn_start_result_cannot_mutate_recreated_thread() {
        let core = core();
        core.upsert_thread(thread("a", "ws"));
        core.apply_thread_conversation_event("ws", local("a"), None);
        let token = core.thread_operation_token("a").unwrap();
        core.remove_thread_store("a");
        core.upsert_thread(thread("a", "ws"));
        let before = core.thread_snapshot("a").unwrap();
        assert!(!core.apply_thread_start_send_result(
            token,
            crate::turns::start::TurnStartSendReduction::Accepted {
                events: vec![ConversationEvent::LocalTurnStartAccepted {
                    thread_id: "a".into(),
                    turn_id: "turn".into(),
                    pending_request_id: "local".into(),
                    mode: ThreadMode::Chat
                }]
            }
        ));
        assert!(Arc::ptr_eq(&before, &core.thread_snapshot("a").unwrap()));
    }

    fn pending_request(request_id: &str, workspace_id: &str, thread_id: &str) -> PendingRequest {
        pending_request_visible_in(request_id, workspace_id, thread_id, &[])
    }

    fn pending_request_visible_in(
        request_id: &str,
        workspace_id: &str,
        thread_id: &str,
        visible_thread_ids: &[&str],
    ) -> PendingRequest {
        PendingRequest::from_native_permission_request(TurnPermissionApprovalRequest {
            request_id: request_id.to_owned(),
            workspace_id: workspace_id.to_owned(),
            thread_id: thread_id.to_owned(),
            turn_id: "turn".to_owned(),
            visible_thread_ids: visible_thread_ids
                .iter()
                .map(|thread_id| (*thread_id).to_owned())
                .collect(),
            tool_name: "exec_command".to_owned(),
            action: pioneer_protocol::TurnPermissionActionKind::ShellCommand,
            scope_hash: format!("{request_id}_scope"),
            reason: pioneer_protocol::TurnPermissionDecisionReason::PolicyRequiresApproval,
            summary: None,
            details: Vec::new(),
        })
    }

    fn presentation_page(texts: &[(&str, &str)]) -> ThreadTimelinePageResponse {
        ThreadTimelinePageResponse {
            workspace_id: "ws".into(),
            thread_id: "a".into(),
            projection_version: 4,
            blocks: texts
                .iter()
                .enumerate()
                .map(|(ix, (id, text))| TimelineBlock {
                    workspace_id: "ws".into(),
                    thread_id: "a".into(),
                    block_id: (*id).into(),
                    turn_id: Some(format!("turn-{id}")),
                    sort_key: format!("{ix:04}"),
                    started_at_unix_ms: Some(1),
                    updated_at_unix_ms: Some(2),
                    kind: TimelineBlockKind::UserMessage {
                        item_id: Some(format!("item-{id}")),
                        inputs: vec![],
                        text: (*text).into(),
                        attachments: vec![],
                        mode: ThreadMode::Message,
                        author: None,
                        route: None,
                        reply: None,
                        mentions: vec![],
                        revision: 0,
                        edited: false,
                        deleted: false,
                    },
                })
                .collect(),
            page: Default::default(),
        }
    }

    #[test]
    fn repeated_thread_updates_release_previous_source_snapshots() {
        let core = core();
        core.upsert_thread(thread("a", "ws"));
        let mut sources = Vec::new();
        for revision in 0..40 {
            let text = format!("{revision}:{}", "history".repeat(16_384));
            core.apply_thread_timeline_page(
                presentation_page(&[("large-message", &text)]),
                crate::timeline::semantic::TopLevelPageMergeMode::Reset,
            );
            let snapshot = core
                .snapshot(&scope("a"))
                .unwrap()
                .typed::<ThreadDomainSnapshot>()
                .unwrap()
                .payload();
            sources.push(Arc::downgrade(&snapshot));
        }
        assert_eq!(
            sources
                .iter()
                .filter(|source| source.strong_count() > 0)
                .count(),
            1
        );
    }

    #[test]
    fn draining_last_delivery_releases_obsolete_rows_without_another_update() {
        let core = core();
        core.upsert_thread(thread("a", "ws"));
        let timeline_scope = ClientScope::Timeline {
            thread_id: "a".into(),
        };
        let subscription = core.subscribe(timeline_scope.clone(), NonZeroUsize::new(16).unwrap());
        let mut rows = Vec::new();
        for text in ["first", "second", "last"] {
            core.apply_thread_timeline_page(
                presentation_page(&[("message", text)]),
                crate::timeline::semantic::TopLevelPageMergeMode::Reset,
            );
            core.materialize_demanded_timelines();
            rows.push(Arc::downgrade(
                &core
                    .thread_presentation_snapshot("a")
                    .unwrap()
                    .timeline()
                    .rows()[0],
            ));
        }
        while subscription.try_next().is_some() {}
        let sequence = core
            .snapshot(&timeline_scope)
            .unwrap()
            .snapshot()
            .sequence();
        let idle = core.wait_for_publications(sequence);
        assert!(idle.changes.is_empty());
        assert_eq!(rows.iter().filter(|row| row.strong_count() > 0).count(), 1);
    }

    #[test]
    fn semantic_updates_share_unchanged_coordinator_and_duplicate_pages_do_not_publish() {
        let core = core();
        core.upsert_thread(thread("a", "ws"));
        let first = core
            .snapshot(&scope("a"))
            .unwrap()
            .typed::<ThreadDomainSnapshot>()
            .unwrap()
            .payload();
        let page = presentation_page(&[("message", "body")]);
        core.apply_thread_timeline_page(
            page.clone(),
            crate::timeline::semantic::TopLevelPageMergeMode::Reset,
        );
        let next = core
            .snapshot(&scope("a"))
            .unwrap()
            .typed::<ThreadDomainSnapshot>()
            .unwrap()
            .payload();
        assert!(Arc::ptr_eq(&first.coordinator, &next.coordinator));
        assert!(!Arc::ptr_eq(&first.semantic, &next.semantic));
        core.apply_thread_timeline_page(
            page,
            crate::timeline::semantic::TopLevelPageMergeMode::Reset,
        );
        let duplicate = core
            .snapshot(&scope("a"))
            .unwrap()
            .typed::<ThreadDomainSnapshot>()
            .unwrap()
            .payload();
        assert!(Arc::ptr_eq(&next, &duplicate));
    }

    #[test]
    fn presentation_reinserted_identity_never_reuses_a_retired_row_revision() {
        let core = core();
        core.upsert_thread(thread("a", "ws"));
        core.apply_thread_timeline_page(
            presentation_page(&[("x", "before")]),
            crate::timeline::semantic::TopLevelPageMergeMode::Reset,
        );
        let _lease = core.subscribe(
            ClientScope::Timeline {
                thread_id: "a".into(),
            },
            NonZeroUsize::new(16).unwrap(),
        );
        let before = core.thread_presentation_snapshot("a").unwrap().timeline();
        core.apply_thread_timeline_page(
            presentation_page(&[]),
            crate::timeline::semantic::TopLevelPageMergeMode::Reset,
        );
        core.materialize_demanded_timelines();
        core.apply_thread_timeline_page(
            presentation_page(&[("x", "after")]),
            crate::timeline::semantic::TopLevelPageMergeMode::Reset,
        );
        core.materialize_demanded_timelines();
        let after = core.thread_presentation_snapshot("a").unwrap().timeline();
        assert_eq!(before.rows()[0].id(), after.rows()[0].id());
        assert!(after.rows()[0].revision() > before.rows()[0].revision());
    }

    #[test]
    fn presentation_authorization_fence_rejects_late_domain_requests_and_old_content() {
        struct PageTransport;
        impl crate::rpc::JsonRpcRequestTransport for PageTransport {
            fn send_json_rpc_request(
                &self,
                _: String,
                _: String,
                response: crate::rpc::JsonRpcResponseSender,
            ) -> Result<(), String> {
                response
                    .send(Ok(serde_json::to_value(presentation_page(&[(
                        "x",
                        "protected-before-fence",
                    )]))
                    .unwrap()))
                    .unwrap();
                Ok(())
            }
        }
        let core = core();
        core.upsert_thread(thread("a", "ws"));
        core.apply_thread_timeline_page(
            presentation_page(&[("x", "protected-before-fence")]),
            crate::timeline::semantic::TopLevelPageMergeMode::Reset,
        );
        let _lease = core.subscribe(
            ClientScope::Timeline {
                thread_id: "a".into(),
            },
            NonZeroUsize::new(16).unwrap(),
        );
        let request = core
            .begin_thread_semantic_request(SemanticTimelineRequestAction::ThreadTimelinePage {
                key: SemanticTimelineRequestKey::ThreadNewest {
                    thread_id: "a".into(),
                },
                params: pioneer_protocol::ThreadTimelinePageParams {
                    thread_id: "a".into(),
                    anchor: pioneer_protocol::TimelinePageAnchor::Newest,
                    limit: Some(50),
                },
            })
            .unwrap();
        core.begin_authorization_epoch(Some(("synthetic-endpoint".into(), 1)));
        core.execute_thread_semantic_request(&PageTransport, request);
        core.materialize_demanded_timelines();
        assert!(core.thread_presentation_snapshot("a").is_none());
        core.upsert_thread(thread("a", "ws"));
        core.materialize_demanded_timelines();
        assert!(
            core.thread_presentation_snapshot("a")
                .unwrap()
                .timeline()
                .rows()
                .is_empty()
        );
    }

    #[test]
    fn presentation_empty_page_and_refresh_keep_loaded_state() {
        let core = core();
        core.upsert_thread(thread("a", "ws"));
        let _lease = core.subscribe(
            ClientScope::Timeline {
                thread_id: "a".into(),
            },
            NonZeroUsize::new(16).unwrap(),
        );
        assert_eq!(
            serde_json::to_value(core.thread_presentation_snapshot("a").unwrap().timeline())
                .unwrap()["has_loaded_page"],
            false
        );
        core.apply_thread_timeline_page(
            presentation_page(&[]),
            crate::timeline::semantic::TopLevelPageMergeMode::Reset,
        );
        core.materialize_demanded_timelines();
        assert_eq!(
            serde_json::to_value(core.thread_presentation_snapshot("a").unwrap().timeline())
                .unwrap()["has_loaded_page"],
            true
        );
        let request = core
            .begin_thread_semantic_request(SemanticTimelineRequestAction::ThreadTimelinePage {
                key: SemanticTimelineRequestKey::ThreadNewest {
                    thread_id: "a".into(),
                },
                params: pioneer_protocol::ThreadTimelinePageParams {
                    thread_id: "a".into(),
                    anchor: pioneer_protocol::TimelinePageAnchor::Newest,
                    limit: Some(50),
                },
            })
            .unwrap();
        core.materialize_demanded_timelines();
        let loading =
            serde_json::to_value(core.thread_presentation_snapshot("a").unwrap().timeline())
                .unwrap();
        assert_eq!(loading["has_loaded_page"], true);
        assert!(loading["status"].get("Loading").is_some());
        drop(request);
    }

    #[test]
    fn presentation_coalesces_dormant_sources_and_preserves_row_identity() {
        let core = core();
        core.upsert_thread(thread("a", "ws"));
        core.upsert_thread(thread("b", "ws"));
        for text in ["one", "two", "latest"] {
            core.apply_thread_timeline_page(
                presentation_page(&[("x", text), ("y", "unchanged")]),
                crate::timeline::semantic::TopLevelPageMergeMode::Reset,
            );
            core.materialize_demanded_timelines();
            assert!(core.thread_presentation_snapshot("a").is_none());
        }
        let subscription = core.subscribe(
            ClientScope::Timeline {
                thread_id: "a".into(),
            },
            NonZeroUsize::new(8).unwrap(),
        );
        let first = core.thread_presentation_snapshot("a").unwrap().timeline();
        let after = core
            .snapshot(&ClientScope::Timeline {
                thread_id: "a".into(),
            })
            .unwrap()
            .snapshot()
            .sequence();
        assert_eq!(first.revision(), 1);
        assert_eq!(first.rows().len(), 2);
        assert_eq!(first.rows()[0].item().unwrap().partial_text, "latest");
        core.materialize_demanded_timelines();
        assert!(Arc::ptr_eq(
            &first,
            &core.thread_presentation_snapshot("a").unwrap().timeline()
        ));
        core.apply_thread_timeline_page(
            presentation_page(&[("x", "changed"), ("y", "unchanged")]),
            crate::timeline::semantic::TopLevelPageMergeMode::Reset,
        );
        core.materialize_demanded_timelines();
        let second = core.thread_presentation_snapshot("a").unwrap().timeline();
        assert!(!Arc::ptr_eq(&first.rows()[0], &second.rows()[0]));
        assert!(Arc::ptr_eq(&first.rows()[1], &second.rows()[1]));
        let changes = core
            .wait_for_publications(after)
            .changes
            .last()
            .unwrap()
            .timeline_changes()
            .to_vec();
        assert_eq!(changes.len(), 1);
        assert_eq!((changes[0].from_revision, changes[0].to_revision), (1, 2));
        assert_eq!(changes[0].replaced.len(), 1);
        assert!(changes[0].inserted.is_empty());
        assert!(changes[0].removed.is_empty());
        assert!(changes[0].order.is_none());
        let mut b = thread("b", "ws");
        b.preview = "summary changed".into();
        core.upsert_thread(b);
        core.materialize_demanded_timelines();
        assert!(Arc::ptr_eq(
            &second,
            &core.thread_presentation_snapshot("a").unwrap().timeline()
        ));
        assert!(core.thread_presentation_snapshot("b").is_none());
        drop(subscription);
    }

    #[test]
    fn presentation_reorder_insert_remove_have_exact_ranges() {
        let core = core();
        core.upsert_thread(thread("a", "ws"));
        core.apply_thread_timeline_page(
            presentation_page(&[("x", "x"), ("y", "y")]),
            crate::timeline::semantic::TopLevelPageMergeMode::Reset,
        );
        let _subscription = core.subscribe(
            ClientScope::Timeline {
                thread_id: "a".into(),
            },
            NonZeroUsize::new(8).unwrap(),
        );
        let first = core.thread_presentation_snapshot("a").unwrap().timeline();
        let after = core
            .snapshot(&ClientScope::Timeline {
                thread_id: "a".into(),
            })
            .unwrap()
            .snapshot()
            .sequence();
        core.apply_thread_timeline_page(
            presentation_page(&[("y", "y"), ("x", "x"), ("z", "z")]),
            crate::timeline::semantic::TopLevelPageMergeMode::Reset,
        );
        core.materialize_demanded_timelines();
        let second = core.thread_presentation_snapshot("a").unwrap().timeline();
        assert!(Arc::ptr_eq(&first.rows()[0], &second.rows()[1]));
        assert!(Arc::ptr_eq(&first.rows()[1], &second.rows()[0]));
        let changes = core
            .wait_for_publications(after)
            .changes
            .last()
            .unwrap()
            .timeline_changes()
            .to_vec();
        assert_eq!(changes[0].inserted.len(), 1);
        assert!(changes[0].replaced.is_empty());
        assert_eq!(
            changes[0]
                .order
                .as_ref()
                .unwrap()
                .iter()
                .map(|id| id.as_str())
                .collect::<Vec<_>>(),
            second
                .rows()
                .iter()
                .map(|row| row.id().as_str())
                .collect::<Vec<_>>()
        );
        let after = core
            .snapshot(&ClientScope::Timeline {
                thread_id: "a".into(),
            })
            .unwrap()
            .snapshot()
            .sequence();
        core.apply_thread_timeline_page(
            presentation_page(&[("z", "z")]),
            crate::timeline::semantic::TopLevelPageMergeMode::Reset,
        );
        core.materialize_demanded_timelines();
        let changes = core
            .wait_for_publications(after)
            .changes
            .last()
            .unwrap()
            .timeline_changes()
            .to_vec();
        assert_eq!(changes[0].removed.len(), 2);
        assert_eq!((changes[0].from_revision, changes[0].to_revision), (2, 3));
    }

    #[test]
    fn presentation_rejects_old_source_and_incarnation_results() {
        let core = core();
        core.upsert_thread(thread("a", "ws"));
        core.apply_thread_timeline_page(
            presentation_page(&[("x", "old")]),
            crate::timeline::semantic::TopLevelPageMergeMode::Reset,
        );
        let _subscription = core.subscribe(
            ClientScope::Timeline {
                thread_id: "a".into(),
            },
            NonZeroUsize::new(8).unwrap(),
        );
        let source = core.thread_snapshot("a").unwrap();
        let old = core.thread_presentation_snapshot("a").unwrap().timeline();
        let (late, delta) = crate::timeline::presentation::project(
            "a",
            old.generation(),
            &source,
            Some(&old),
            2,
            100,
        );
        core.apply_thread_timeline_page(
            presentation_page(&[("x", "latest")]),
            crate::timeline::semantic::TopLevelPageMergeMode::Reset,
        );
        assert!(!core.accept_thread_timeline(
            "a",
            source.timeline_revision(),
            old.generation(),
            2,
            late.clone(),
            delta.clone()
        ));
        core.remove_thread_store("a");
        core.upsert_thread(thread("a", "ws"));
        assert!(!core.accept_thread_timeline(
            "a",
            core.thread_snapshot("a").unwrap().timeline_revision(),
            old.generation(),
            2,
            late,
            delta
        ));
    }

    #[test]
    fn presentation_demand_and_summary_have_independent_scopes() {
        let core = core();
        core.upsert_thread(thread("a", "ws"));
        let _metadata = core.subscribe(scope("a"), NonZeroUsize::new(8).unwrap());
        core.apply_thread_timeline_page(
            presentation_page(&[("x", "first")]),
            crate::timeline::semantic::TopLevelPageMergeMode::Reset,
        );
        core.materialize_demanded_timelines();
        assert!(core.thread_presentation_snapshot("a").is_none());
        let timeline_scope = ClientScope::Timeline {
            thread_id: "a".into(),
        };
        core.thread_demand_changed(&timeline_scope, ClientDemand::Suspended);
        let _timeline = core.subscribe(timeline_scope.clone(), NonZeroUsize::new(8).unwrap());
        assert!(
            core.thread_presentation_snapshot("a").is_none(),
            "explicit dormant demand survives subscriptions"
        );
        core.thread_demand_changed(&timeline_scope, ClientDemand::Prefetch);
        let first = core.thread_presentation_snapshot("a").unwrap().timeline();
        core.thread_demand_changed(&timeline_scope, ClientDemand::Visible);
        assert!(Arc::ptr_eq(
            &first,
            &core.thread_presentation_snapshot("a").unwrap().timeline()
        ));
        let mut summary = thread("a", "ws");
        summary.preview = "new summary".into();
        core.upsert_thread(summary);
        core.materialize_demanded_timelines();
        assert!(Arc::ptr_eq(
            &first,
            &core.thread_presentation_snapshot("a").unwrap().timeline()
        ));
        core.thread_demand_changed(&timeline_scope, ClientDemand::Suspended);
        for text in ["one", "two", "latest"] {
            core.apply_thread_timeline_page(
                presentation_page(&[("x", text)]),
                crate::timeline::semantic::TopLevelPageMergeMode::Reset,
            );
            core.materialize_demanded_timelines();
            assert!(Arc::ptr_eq(
                &first,
                &core.thread_presentation_snapshot("a").unwrap().timeline()
            ));
        }
        core.thread_demand_changed(&timeline_scope, ClientDemand::Visible);
        let next = core.thread_presentation_snapshot("a").unwrap().timeline();
        assert_eq!(next.revision(), first.revision() + 1);
        assert_eq!(
            next.source_revision(),
            core.thread_snapshot("a").unwrap().timeline_revision()
        );
        assert_eq!(next.rows()[0].item().unwrap().partial_text, "latest");
    }

    #[test]
    fn presentation_pending_positions_scope_and_running_start_are_stable() {
        use crate::timeline::presentation::TimelineRenderRow;
        use crate::timeline::rows::TimelineRowKind;
        for running_first in [true, false] {
            let core = core();
            core.upsert_thread(thread("a", "ws"));
            core.upsert_thread(thread("b", "ws"));
            let mut page = presentation_page(&[("user", "text")]);
            page.blocks.push(TimelineBlock {
                workspace_id: "ws".into(),
                thread_id: "a".into(),
                block_id: "running".into(),
                turn_id: Some("running-turn".into()),
                sort_key: if running_first { "0000" } else { "9999" }.into(),
                started_at_unix_ms: None,
                updated_at_unix_ms: None,
                kind: TimelineBlockKind::TurnState {
                    state: pioneer_protocol::TurnWorkState::Running,
                    message: None,
                    author: None,
                    route: None,
                },
            });
            core.apply_thread_timeline_page(
                page,
                crate::timeline::semantic::TopLevelPageMergeMode::Reset,
            );
            core.apply_pending_requests(PendingRequestsReduction::Opened(pending_request(
                "visible", "ws", "a",
            )));
            core.apply_pending_requests(PendingRequestsReduction::Opened(pending_request(
                "other", "ws", "b",
            )));
            let _subscription = core.subscribe(
                ClientScope::Timeline {
                    thread_id: "a".into(),
                },
                NonZeroUsize::new(8).unwrap(),
            );
            let first = core.thread_presentation_snapshot("a").unwrap().timeline();
            let running_ix = first.rows().iter().position(|row| matches!(row.value(), TimelineRenderRow::Timeline(row) if matches!(row.kind, TimelineRowKind::RunningTurn(_)))).unwrap();
            assert_eq!(
                first.rows()[running_ix - 1].id().as_str(),
                "timeline-pending-request::visible"
            );
            assert!(
                !first
                    .rows()
                    .iter()
                    .any(|row| row.id().as_str().contains("other"))
            );
            let started = match first.rows()[running_ix].value() {
                TimelineRenderRow::Timeline(row) => match &row.kind {
                    TimelineRowKind::RunningTurn(row) => row.started_at_unix_ms.unwrap(),
                    _ => unreachable!(),
                },
                _ => unreachable!(),
            };
            let source = core.thread_snapshot("a").unwrap();
            let (later, _) = crate::timeline::presentation::project(
                "a",
                first.generation(),
                &source,
                Some(&first),
                first.revision() + 1,
                started + 100_000,
            );
            let running = later
                .rows()
                .iter()
                .find(|row| row.id() == first.rows()[running_ix].id())
                .unwrap();
            assert!(Arc::ptr_eq(running, &first.rows()[running_ix]));
            core.apply_thread_timeline_page(
                presentation_page(&[("user", "text")]),
                crate::timeline::semantic::TopLevelPageMergeMode::Reset,
            );
            core.materialize_demanded_timelines();
            let last = core.thread_presentation_snapshot("a").unwrap().timeline();
            assert_eq!(
                last.rows().last().unwrap().id().as_str(),
                "timeline-pending-request::visible"
            );
        }
    }

    #[test]
    fn presentation_visible_burst_and_authorship_use_exact_subrevisions() {
        let core = core();
        core.upsert_thread(thread("a", "ws"));
        let mut page = presentation_page(&[("x", "initial")]);
        core.apply_thread_timeline_page(
            page.clone(),
            crate::timeline::semantic::TopLevelPageMergeMode::Reset,
        );
        let _subscription = core.subscribe(
            ClientScope::Timeline {
                thread_id: "a".into(),
            },
            NonZeroUsize::new(16).unwrap(),
        );
        let first = core.thread_presentation_snapshot("a").unwrap().timeline();
        for text in ["one", "two", "latest"] {
            page = presentation_page(&[("x", text)]);
            core.apply_thread_timeline_page(
                page.clone(),
                crate::timeline::semantic::TopLevelPageMergeMode::Reset,
            );
        }
        core.materialize_demanded_timelines();
        let last = core.thread_presentation_snapshot("a").unwrap().timeline();
        assert_eq!(last.revision(), first.revision() + 1);
        assert_eq!(last.rows()[0].item().unwrap().partial_text, "latest");
        if let TimelineBlockKind::UserMessage { author, .. } = &mut page.blocks[0].kind {
            *author = Some(pioneer_protocol::TurnAuthorSnapshot {
                actor: pioneer_protocol::PersistedActorRef::Principal(
                    pioneer_protocol::PrincipalId::new("PAAAAAAAAAAAAAAAAAAAA").unwrap(),
                ),
                display_name: "Alice".into(),
                nickname: "alice".into(),
                avatar_revision: Some("avatar-2".into()),
                agent: None,
            });
        }
        core.apply_thread_timeline_page(
            page,
            crate::timeline::semantic::TopLevelPageMergeMode::Reset,
        );
        core.materialize_demanded_timelines();
        let authored = core.thread_presentation_snapshot("a").unwrap().timeline();
        assert_eq!(last.rows()[0].id(), authored.rows()[0].id());
        assert_eq!(
            last.rows()[0].content_revision(),
            authored.rows()[0].content_revision()
        );
        assert_eq!(
            last.rows()[0].metadata_revision() + 1,
            authored.rows()[0].metadata_revision()
        );
    }

    #[test]
    fn presentation_paging_and_prefetch_use_published_identity_and_source_fences() {
        use crate::timeline::semantic::*;
        let core = core();
        core.upsert_thread(thread("a", "ws"));
        let mut page = presentation_page(&[("last", "last")]);
        page.page.has_more_before = true;
        page.page.before_cursor = Some(pioneer_protocol::TimelineCursor {
            value: "older".into(),
        });
        core.apply_thread_timeline_page(page, TopLevelPageMergeMode::Reset);
        let _subscription = core.subscribe(
            ClientScope::Timeline {
                thread_id: "a".into(),
            },
            NonZeroUsize::new(16).unwrap(),
        );
        let first = core.thread_presentation_snapshot("a").unwrap().timeline();
        let after = core
            .snapshot(&ClientScope::Timeline {
                thread_id: "a".into(),
            })
            .unwrap()
            .snapshot()
            .sequence();
        let ids = vec![first.rows()[0].id().as_str().to_owned()];
        for presented in [false, true] {
            let actions = core.plan_thread_timeline_viewport(
                "a",
                first.source_revision(),
                &ids,
                6,
                true,
                false,
                false,
                presented,
            );
            assert!(
                matches!(actions.as_slice(), [SemanticTimelineRequestAction::ThreadTimelinePage { key:SemanticTimelineRequestKey::ThreadBefore { cursor, .. }, .. }] if cursor == "older")
            );
            assert!(
                core.plan_thread_timeline_viewport(
                    "a",
                    first.source_revision() - 1,
                    &ids,
                    6,
                    true,
                    true,
                    true,
                    presented
                )
                .is_empty()
            );
        }
        core.apply_thread_timeline_page(
            presentation_page(&[("first", "first")]),
            TopLevelPageMergeMode::MergeBefore,
        );
        core.materialize_demanded_timelines();
        let next = core.thread_presentation_snapshot("a").unwrap().timeline();
        assert_eq!(next.rows().len(), 2);
        assert!(
            next.rows()
                .iter()
                .any(|row| Arc::ptr_eq(row, &first.rows()[0]))
        );
        let changes = core
            .wait_for_publications(after)
            .changes
            .last()
            .unwrap()
            .timeline_changes()
            .to_vec();
        assert_eq!((changes[0].from_revision, changes[0].to_revision), (1, 2));
        assert_eq!(changes[0].inserted.len(), 1);
        assert!(changes[0].replaced.is_empty());
    }

    #[test]
    fn presentation_identity_groups_optimistic_and_persisted_self_messages() {
        let core = core();
        core.upsert_thread(thread("a", "ws"));
        let principal = "PAAAAAAAAAAAAAAAAAAAA";
        let mut page = presentation_page(&[("x", "optimistic"), ("y", "persisted")]);
        if let TimelineBlockKind::UserMessage { item_id, .. } = &mut page.blocks[0].kind {
            *item_id = Some("user_turn-x".into());
        }
        if let TimelineBlockKind::UserMessage { author, .. } = &mut page.blocks[1].kind {
            *author = Some(pioneer_protocol::TurnAuthorSnapshot {
                actor: pioneer_protocol::PersistedActorRef::Principal(
                    pioneer_protocol::PrincipalId::new(principal).unwrap(),
                ),
                display_name: "Self".into(),
                nickname: "self".into(),
                avatar_revision: None,
                agent: None,
            });
        }
        core.apply_thread_timeline_page(
            page,
            crate::timeline::semantic::TopLevelPageMergeMode::Reset,
        );
        let _subscription = core.subscribe(
            ClientScope::Timeline {
                thread_id: "a".into(),
            },
            NonZeroUsize::new(8).unwrap(),
        );
        let before = core.thread_presentation_snapshot("a").unwrap().timeline();
        assert_eq!(before.groups().len(), 2);
        core.update_thread_presentation_identity(Some(principal.into()));
        core.materialize_demanded_timelines();
        let next = core.thread_presentation_snapshot("a").unwrap().timeline();
        assert_eq!(next.groups().len(), 1);
        assert!(next.groups()[0].current_principal);
        assert!(Arc::ptr_eq(&before.rows()[0], &next.rows()[0]));
        assert!(Arc::ptr_eq(&before.rows()[1], &next.rows()[1]));
        assert!(next.source_revision() > before.source_revision());
    }

    #[test]
    fn timeline_loads_after_repeated_eviction_before_open() {
        let core = core();
        let scope = ClientScope::Timeline {
            thread_id: "a".into(),
        };
        for _ in 0..3 {
            core.upsert_thread(thread("a", "ws"));
            core.remove_thread_store("a");
        }
        let retired_revision = core.snapshot(&scope).unwrap().revisions().scoped().get();
        core.upsert_thread(thread("a", "ws"));
        core.apply_thread_timeline_page(
            presentation_page(&[("x", "restored")]),
            crate::timeline::semantic::TopLevelPageMergeMode::Reset,
        );
        let _subscription = core.subscribe(scope.clone(), NonZeroUsize::new(16).unwrap());
        let timeline = core
            .thread_presentation_snapshot("a")
            .expect("history must load after eviction")
            .timeline();
        assert_eq!(timeline.rows().len(), 1);
        assert_eq!(timeline.revision(), retired_revision + 1);
        assert_eq!(
            core.snapshot(&scope).unwrap().revisions().scoped().get(),
            timeline.revision()
        );
    }

    #[test]
    fn timeline_reopens_after_fence_and_repeated_cache_clears() {
        let core = core();
        let scope = ClientScope::Timeline {
            thread_id: "a".into(),
        };
        core.upsert_thread(thread("a", "ws"));
        let subscription = core.subscribe(scope.clone(), NonZeroUsize::new(16).unwrap());
        assert!(core.thread_presentation_snapshot("a").is_some());
        core.begin_authorization_epoch(Some(("synthetic-endpoint".into(), 1)));
        drop(subscription);
        let fence_revision = core.snapshot(&scope).unwrap().revisions().scoped().get();
        core.clear_thread_stores();
        for _ in 0..3 {
            core.upsert_thread(thread("a", "ws"));
            core.clear_thread_stores();
        }
        assert_eq!(
            core.snapshot(&scope).unwrap().revisions().scoped().get(),
            fence_revision
        );
        core.upsert_thread(thread("a", "ws"));
        assert!(
            core.snapshot(&ClientScope::Thread {
                thread_id: "a".into()
            })
            .unwrap()
            .typed::<ThreadDomainSnapshot>()
            .is_some()
        );
        core.apply_thread_timeline_page(
            presentation_page(&[("x", "fresh history")]),
            crate::timeline::semantic::TopLevelPageMergeMode::Reset,
        );
        let subscription = core.subscribe(scope.clone(), NonZeroUsize::new(16).unwrap());
        let timeline = core
            .thread_presentation_snapshot("a")
            .expect("fresh timeline after fence")
            .timeline();
        assert_eq!(timeline.rows().len(), 1);
        assert_eq!(timeline.revision(), fence_revision + 1);
        drop(subscription);
        core.remove_thread_store("a");
        let retired = core.snapshot(&scope).unwrap();
        assert!(retired.snapshot().serialized_payload().is_null());
        assert_eq!(retired.revisions().scoped().get(), timeline.revision() + 1);
    }

    #[test]
    fn presentation_cannot_cross_protected_eviction_or_reuse_retired_revisions() {
        let core = core();
        core.upsert_thread(thread("a", "ws"));
        core.apply_thread_timeline_page(
            presentation_page(&[("x", "private")]),
            crate::timeline::semantic::TopLevelPageMergeMode::Reset,
        );
        let scope = ClientScope::Timeline {
            thread_id: "a".into(),
        };
        let _subscription = core.subscribe(scope.clone(), NonZeroUsize::new(16).unwrap());
        let before = core.thread_presentation_snapshot("a").unwrap().timeline();
        let source = core.thread_snapshot("a").unwrap();
        let (late, change) = crate::timeline::presentation::project(
            "a",
            before.generation(),
            &source,
            Some(&before),
            before.revision() + 1,
            1000,
        );
        core.begin_authorization_epoch(Some(("synthetic-endpoint".into(), 1)));
        assert!(core.thread_presentation_snapshot("a").is_none());
        assert!(
            core.snapshot(&scope)
                .unwrap()
                .snapshot()
                .serialized_payload()
                .is_null()
        );
        assert!(!core.accept_thread_timeline(
            "a",
            source.timeline_revision(),
            before.generation(),
            late.revision(),
            late,
            change
        ));
        core.materialize_demanded_timelines();
        assert!(core.thread_presentation_snapshot("a").is_none());
        let fence = core.snapshot(&scope).unwrap().revisions().scoped().get();
        core.upsert_thread(thread("a", "ws"));
        core.materialize_demanded_timelines();
        let refreshed = core.thread_presentation_snapshot("a").unwrap().timeline();
        assert_eq!(refreshed.revision(), fence + 1);
        assert_eq!(
            core.snapshot(&scope).unwrap().revisions().scoped().get(),
            refreshed.revision()
        );
        core.clear_thread_stores();
        core.upsert_thread(thread("a", "ws"));
        core.materialize_demanded_timelines();
        let recreated = core.thread_presentation_snapshot("a").unwrap().timeline();
        assert!(recreated.revision() > refreshed.revision());
        assert_ne!(recreated.generation(), refreshed.generation());
    }

    fn core() -> Arc<ClientCore> {
        Arc::new(ClientCore::new())
    }
    fn scope(id: &str) -> ClientScope {
        ClientScope::Thread {
            thread_id: id.to_owned(),
        }
    }
    fn local(id: &str) -> ConversationEvent {
        ConversationEvent::LocalTurnStartRequested {
            thread_id: id.to_owned(),
            turn_id: "turn".into(),
            pending_request_id: "local".into(),
            mode: ThreadMode::Chat,
            user_text: "synthetic".into(),
            attachments: vec![],
        }
    }
    #[test]
    fn thread_b_preserves_a_snapshot_revisions_and_subscriptions() {
        let core = core();
        core.upsert_thread(thread("a", "ws"));
        core.upsert_thread(thread("b", "ws"));
        let a = core.thread_snapshot("a").unwrap();
        let subscriber = core.subscribe(scope("a"), NonZeroUsize::new(8).unwrap());
        let b_subscriber = core.subscribe(scope("b"), NonZeroUsize::new(8).unwrap());
        let summary = core.subscribe(
            ClientScope::SidebarSummary {
                workspace_id: "ws".into(),
                thread_id: "b".into(),
            },
            NonZeroUsize::new(8).unwrap(),
        );
        core.apply_thread_conversation_event("ws", local("b"), None);
        assert!(Arc::ptr_eq(&a, &core.thread_snapshot("a").unwrap()));
        assert!(subscriber.try_next().is_none());
        assert!(b_subscriber.try_next().is_some());
        assert!(
            summary.try_next().is_none(),
            "content alone does not change the displayed summary"
        );
        let mut renamed = thread("b", "ws");
        renamed.name = Some("new name".into());
        core.upsert_thread(renamed);
        assert!(summary.try_next().is_some());
        assert!(subscriber.try_next().is_none());
    }
    #[test]
    fn no_op_delta_and_equal_snapshot_preserve_publication_identity() {
        let core = core();
        core.upsert_thread(thread("a", "ws"));
        let before = core.thread_snapshot("a").unwrap();
        let subscriber = core.subscribe(scope("a"), NonZeroUsize::new(8).unwrap());
        core.upsert_thread(thread("a", "ws"));
        core.apply_thread_conversation_event(
            "ws",
            ConversationEvent::ItemDelta {
                thread_id: "a".into(),
                turn_id: "missing".into(),
                item_id: "missing".into(),
                delta: String::new(),
                stream: None,
                payload: None,
                markdown: None,
                markdown_version: None,
            },
            None,
        );
        assert!(Arc::ptr_eq(&before, &core.thread_snapshot("a").unwrap()));
        assert!(subscriber.try_next().is_none());
    }
    #[test]
    fn requests_have_one_workspace_aware_owner_with_visible_projections() {
        let core = core();
        for (id, ws) in [("a", "ws"), ("b", "ws"), ("other", "other-ws")] {
            core.upsert_thread(thread(id, ws));
        }
        let other = core.thread_snapshot("other").unwrap();
        let request = pending_request_visible_in("request", "ws", "b", &["a"]);
        assert!(core.apply_pending_requests(PendingRequestsReduction::Opened(request.clone())));
        assert!(!core.apply_pending_requests(PendingRequestsReduction::Opened(request)));
        assert_eq!(core.pending_request_snapshot().len(), 1);
        assert_eq!(core.thread_snapshot("a").unwrap().pending().len(), 1);
        assert_eq!(core.thread_snapshot("b").unwrap().pending().len(), 1);
        assert!(Arc::ptr_eq(&other, &core.thread_snapshot("other").unwrap()));
        assert!(
            !core.apply_pending_requests(PendingRequestsReduction::ResolvedInWorkspace {
                workspace_id: "other-ws".into(),
                request_id: "request".into()
            })
        );
        let mut threadless = pending_request("global", "ws", "b");
        threadless.thread_id = None;
        threadless.visible_thread_ids.clear();
        core.apply_pending_requests(PendingRequestsReduction::Opened(threadless));
        assert_eq!(core.pending_requests_for_scope(Some("ws"), None).len(), 1);
        assert!(
            core.pending_requests_for_scope(Some("other-ws"), None)
                .is_empty()
        );
        assert_eq!(core.thread_snapshot("a").unwrap().pending().len(), 1);
        core.apply_pending_requests(PendingRequestsReduction::ResolvedInWorkspace {
            workspace_id: "ws".into(),
            request_id: "request".into(),
        });
        assert!(core.thread_snapshot("a").unwrap().pending().is_empty());
        assert_eq!(core.pending_request_snapshot().len(), 1);
    }
    #[test]
    fn inactive_stores_are_bounded_without_discarding_running_work_or_drafts() {
        let core = core();
        core.upsert_thread(thread("running", "ws"));
        core.apply_thread_conversation_event("ws", local("running"), None);
        core.upsert_thread(thread("draft", "ws"));
        core.remember_thread_draft("ws", Some("draft".into()));
        core.upsert_thread(thread("subscribed", "ws"));
        let subscription = core.subscribe(scope("subscribed"), NonZeroUsize::new(4).unwrap());
        for i in 0..80 {
            core.upsert_thread(thread(&format!("idle-{i}"), "ws"));
        }
        let registry = core.thread_registry.lock().unwrap();
        assert!(registry.stores.len() <= INACTIVE_THREAD_LIMIT + 3);
        assert!(registry.stores.contains_key("running"));
        assert!(registry.stores.contains_key("draft"));
        assert!(registry.stores.contains_key("subscribed"));
        assert_eq!(registry.directory.threads.len(), 83);
        drop(registry);
        drop(subscription);
        assert!(core.thread_snapshot("subscribed").is_none());
        assert!(
            core.thread_coordinator_snapshot("subscribed").is_some(),
            "sidebar metadata survives cache eviction"
        );
        core.upsert_thread(thread("subscribed", "ws"));
        assert!(
            core.snapshot(&scope("subscribed"))
                .unwrap()
                .typed::<ThreadDomainSnapshot>()
                .is_some()
        );
    }
    #[test]
    fn promotion_preserves_source_and_resume_ownership() {
        let core = core();
        core.upsert_thread(thread("draft", "ws"));
        core.remember_thread_draft("ws", Some("draft".into()));
        core.activate_thread(Some("draft"), Some("ws"));
        core.apply_thread_conversation_event("ws", local("draft"), None);
        core.existing_thread_mutation("draft")
            .unwrap()
            .resume
            .in_progress = true;
        core.enqueue_thread_resume("draft".into());
        let before = core.thread_snapshot("draft").unwrap();
        assert!(core.promote_thread("draft"));
        assert!(!core.promote_thread("draft"));
        assert!(Arc::ptr_eq(
            &before,
            &core.thread_snapshot("draft").unwrap()
        ));
        assert!(core.thread_workspace_draft("ws").is_none());
        assert_eq!(core.take_thread_resume().as_deref(), Some("draft"));
        assert!(core.take_thread_resume().is_none());
        assert!(
            core.thread_coordinator_snapshot("draft")
                .unwrap()
                .resume
                .in_progress
        );
    }
}

/// An accepted request is tied to the incarnation of its source store.
pub struct ThreadSemanticRequest {
    id: String,
    generation: u64,
    request_generation: u64,
    action: SemanticTimelineRequestAction,
    reconciliation: Option<CachedTimelineReconciliation>,
}

// One controller request owns the tail refresh and the entire cached-range walk.
// Cursors are opaque, server-issued boundaries; never manufacture one from an ID.
const CACHED_TIMELINE_PAGE_LIMIT: u32 = 100;
const CACHED_TIMELINE_REQUEST_BUDGET: usize = 32;

struct CachedTimelineReconciliation {
    first_cursor: Option<pioneer_protocol::TimelineCursor>,
    last_cursor: Option<pioneer_protocol::TimelineCursor>,
    last_sort_key: String,
    anchor: pioneer_protocol::TimelinePageAnchor,
    remaining: usize,
    previous_sort_key: Option<String>,
    visited_cursors: HashSet<String>,
    read_last_boundary: bool,
    baseline: Arc<SemanticTimelineState>,
}

impl CachedTimelineReconciliation {
    fn capture(store: &ThreadDomainStore, action: &SemanticTimelineRequestAction) -> Option<Self> {
        use pioneer_protocol::TimelinePageAnchor;
        let SemanticTimelineRequestAction::ThreadTimelinePage {
            key: SemanticTimelineRequestKey::ThreadNewest { thread_id },
            params,
        } = action
        else {
            return None;
        };
        if params.anchor != TimelinePageAnchor::Newest {
            return None;
        }
        let cache = &store.semantic.thread(thread_id)?.top_level;
        if !cache.has_loaded_page || cache.is_empty() {
            return None;
        }
        Some(Self {
            first_cursor: cache.loaded_range.before_cursor.clone(),
            last_cursor: cache.loaded_range.after_cursor.clone(),
            last_sort_key: cache.ordered_blocks().last()?.sort_key.clone(),
            anchor: TimelinePageAnchor::Newest,
            remaining: CACHED_TIMELINE_REQUEST_BUDGET,
            previous_sort_key: None,
            visited_cursors: HashSet::new(),
            read_last_boundary: false,
            baseline: store.snapshot.as_ref()?.semantic.clone(),
        })
    }

    fn advance(
        &mut self,
        page: &pioneer_protocol::ThreadTimelinePageResponse,
    ) -> anyhow::Result<bool> {
        use pioneer_protocol::TimelinePageAnchor;
        self.remaining = self.remaining.saturating_sub(1);
        let next = match &self.anchor {
            TimelinePageAnchor::Newest => {
                // Read the far boundary separately so a result just after the last
                // cached block is included even when a forward page ends on it.
                TimelinePageAnchor::Around {
                    cursor: self.last_cursor.clone().ok_or_else(|| {
                        anyhow::anyhow!("Cached timeline reconciliation boundary cursor missing")
                    })?,
                }
            }
            TimelinePageAnchor::Around { .. } if !self.read_last_boundary => {
                self.read_last_boundary = true;
                TimelinePageAnchor::Around {
                    cursor: self.first_cursor.clone().ok_or_else(|| {
                        anyhow::anyhow!("Cached timeline reconciliation boundary cursor missing")
                    })?,
                }
            }
            _ => {
                let last = page.blocks.iter().map(|block| &block.sort_key).max();
                if last.is_some_and(|key| key >= &self.last_sort_key) || !page.page.has_more_after {
                    return Ok(false);
                }
                let last = last.ok_or_else(|| {
                    anyhow::anyhow!("Cached timeline reconciliation made no progress")
                })?;
                anyhow::ensure!(
                    self.previous_sort_key
                        .as_ref()
                        .is_none_or(|previous| last > previous),
                    "Cached timeline reconciliation made no progress"
                );
                let cursor = page.page.after_cursor.clone().ok_or_else(|| {
                    anyhow::anyhow!("Cached timeline reconciliation cursor missing")
                })?;
                anyhow::ensure!(
                    self.visited_cursors.insert(cursor.value.clone()),
                    "Cached timeline reconciliation cursor did not advance"
                );
                self.previous_sort_key = Some(last.clone());
                TimelinePageAnchor::After { cursor }
            }
        };
        anyhow::ensure!(
            self.remaining > 0,
            "Cached timeline reconciliation request budget exhausted"
        );
        self.anchor = next;
        Ok(true)
    }
}

impl ClientCore {
    pub fn begin_thread_semantic_request(
        &self,
        action: SemanticTimelineRequestAction,
    ) -> Option<ThreadSemanticRequest> {
        self.accept_timeline_request(action, None, None, false)
    }

    fn accept_timeline_request(
        &self,
        action: SemanticTimelineRequestAction,
        reserved: Option<u64>,
        expected_store: Option<u64>,
        notification: bool,
    ) -> Option<ThreadSemanticRequest> {
        if self.is_stopped() {
            return None;
        }
        let id = request_thread_id(crate::timeline::semantic::semantic_timeline_request_key(
            &action,
        ))
        .to_owned();
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        let store = registry.stores.get(&id)?;
        if expected_store.is_some_and(|generation| store.generation != generation) {
            return None;
        }
        // Invalidation still applies, but notifications cannot restart a failed
        // refresh. Check under the same lock as begin and the status transition.
        // Explicit Retry and confirmed subscriptions use the other entry paths.
        if notification
            && matches!(
                crate::timeline::semantic::semantic_timeline_request_key(&action),
                SemanticTimelineRequestKey::ThreadNewest { .. }
            )
            && registry.timeline.has_failed_refresh(&id)
        {
            return None;
        }
        let action = if let Some(generation) = reserved {
            if !registry.timeline.accepts(
                crate::timeline::semantic::semantic_timeline_request_key(&action),
                generation,
            ) {
                return None;
            }
            action
        } else {
            registry.timeline.retain_domain_request(
                crate::timeline::semantic::semantic_timeline_request_key(&action),
            );
            registry.timeline.begin(action)?
        };
        let request_generation = registry.timeline.request_generation(
            crate::timeline::semantic::semantic_timeline_request_key(&action),
        );
        let key = crate::timeline::semantic::semantic_timeline_request_key(&action);
        let preserve_refresh_status = matches!(
            key,
            SemanticTimelineRequestKey::ThreadBefore { .. }
                | SemanticTimelineRequestKey::ThreadAfter { .. }
        ) && (registry.timeline.in_flight.contains(
            &SemanticTimelineRequestKey::ThreadNewest {
                thread_id: id.clone(),
            },
        ) || registry.timeline.has_failed_refresh(&id));
        let store = registry.stores.get_mut(&id)?;
        let generation = store.generation;
        let reconciliation = CachedTimelineReconciliation::capture(store, &action);
        if !preserve_refresh_status {
            set_request_status(
                &mut store.semantic,
                crate::timeline::semantic::semantic_timeline_request_key(&action),
                crate::timeline::semantic::TimelineRequestStatus::Loading {
                    request_key: format!(
                        "{:?}",
                        crate::timeline::semantic::semantic_timeline_request_key(&action)
                    ),
                },
            );
        }
        let drafts = registry.publish(&id);
        self.transition_directory(
            &mut registry,
            &ClientMutationAuthority { _private: () },
            drafts,
            vec![],
        );
        Some(ThreadSemanticRequest {
            id,
            generation,
            request_generation,
            action,
            reconciliation,
        })
    }
    pub fn schedule_thread_semantic_request(&self, action: SemanticTimelineRequestAction) {
        self.schedule_thread_semantic_request_generation(action, None);
    }

    fn schedule_thread_semantic_request_generation(
        &self,
        action: SemanticTimelineRequestAction,
        expected_store: Option<u64>,
    ) {
        self.schedule_thread_semantic_request_inner(action, expected_store, false);
    }

    fn schedule_thread_semantic_request_inner(
        &self,
        action: SemanticTimelineRequestAction,
        expected_store: Option<u64>,
        notification: bool,
    ) {
        let sender = self
            .thread_request_sender
            .lock()
            .expect("thread request sender poisoned")
            .clone();
        let Some(sender) = sender else {
            return;
        };
        if let Some(request) =
            self.accept_timeline_request(action, None, expected_store, notification)
        {
            let _ = sender.send(ThreadControllerRequest::Semantic(request));
        }
    }

    pub(crate) fn schedule_planned_timeline_request(
        &self,
        page: crate::timeline::controller::TimelinePageRequest,
    ) {
        let Some(request) =
            self.accept_timeline_request(page.action, Some(page.generation), None, false)
        else {
            return;
        };
        let sender = self
            .thread_request_sender
            .lock()
            .expect("thread request sender poisoned")
            .clone();
        if let Some(sender) = sender {
            let _ = sender.send(ThreadControllerRequest::Semantic(request));
        } else {
            let key =
                crate::timeline::semantic::semantic_timeline_request_key(&request.action).clone();
            let mut registry = self
                .thread_registry
                .lock()
                .expect("thread registry poisoned");
            registry
                .timeline
                .finish(&key, false, self.timeline_now_ms());
            if let Some(store) = registry.stores.get_mut(&request.id) {
                set_request_status(
                    &mut store.semantic,
                    &key,
                    crate::timeline::semantic::TimelineRequestStatus::Failed {
                        message: "Timeline transport unavailable".into(),
                    },
                );
            }
            let drafts = registry.publish(&request.id);
            self.transition_directory(
                &mut registry,
                &ClientMutationAuthority { _private: () },
                drafts,
                vec![],
            );
        }
    }
    pub fn execute_thread_semantic_request(
        &self,
        transport: &impl crate::rpc::JsonRpcRequestTransport,
        mut request: ThreadSemanticRequest,
    ) {
        use crate::timeline::semantic::*;
        use crate::transport::ws::command_sender as commands;
        enum Page {
            Thread(
                pioneer_protocol::ThreadTimelinePageResponse,
                TopLevelPageMergeMode,
            ),
            Work(pioneer_protocol::TurnWorkPageResponse, WorkPageMergeMode),
            Items(Vec<pioneer_protocol::TurnWorkItemsGetResponse>),
        }
        loop {
            if self.is_stopped() {
                return;
            }
            let key = semantic_timeline_request_key(&request.action).clone();
            {
                let registry = self
                    .thread_registry
                    .lock()
                    .expect("thread registry poisoned");
                if !registry.stores.get(&request.id).is_some_and(|store| {
                    store.generation == request.generation
                        && registry.timeline.accepts(&key, request.request_generation)
                }) {
                    return;
                }
            }
            let expected_turn = match &request.action {
                SemanticTimelineRequestAction::ThreadTimelinePage { .. } => None,
                SemanticTimelineRequestAction::TurnWorkPage { params, .. } => {
                    Some(params.turn_id.clone())
                }
                SemanticTimelineRequestAction::TurnWorkItemsGet { params, .. } => {
                    Some(params.turn_id.clone())
                }
            };
            let result = match request.action.clone() {
                SemanticTimelineRequestAction::ThreadTimelinePage { key, mut params } => {
                    if let Some(reconciliation) = &request.reconciliation {
                        params.anchor = reconciliation.anchor.clone();
                        if params.anchor != pioneer_protocol::TimelinePageAnchor::Newest {
                            params.limit = Some(CACHED_TIMELINE_PAGE_LIMIT);
                        }
                    }
                    let mode = match key {
                        SemanticTimelineRequestKey::ThreadBefore { .. } => {
                            TopLevelPageMergeMode::MergeBefore
                        }
                        SemanticTimelineRequestKey::ThreadAfter { .. } => {
                            TopLevelPageMergeMode::MergeAfter
                        }
                        _ => TopLevelPageMergeMode::Merge,
                    };
                    commands::thread_timeline_page(transport, params).map(|p| Page::Thread(p, mode))
                }
                SemanticTimelineRequestAction::TurnWorkPage { key, params } => {
                    let mode = match key {
                        SemanticTimelineRequestKey::TurnWorkBefore { .. } => {
                            WorkPageMergeMode::MergeBefore
                        }
                        _ => WorkPageMergeMode::MergeAfter,
                    };
                    commands::turn_work_page(transport, params).map(|p| Page::Work(p, mode))
                }
                SemanticTimelineRequestAction::TurnWorkItemsGet { params, .. } => params
                    .work_item_ids
                    .chunks(crate::timeline::controller::WORK_ITEM_CHUNK_LIMIT)
                    .map(|ids| {
                        anyhow::ensure!(
                            !self.is_stopped()
                                && self
                                    .thread_registry
                                    .lock()
                                    .expect("thread registry poisoned")
                                    .timeline
                                    .accepts(&key, request.request_generation),
                            "Timeline request retired"
                        );
                        commands::turn_work_items_get(
                            transport,
                            pioneer_protocol::TurnWorkItemsGetParams {
                                thread_id: params.thread_id.clone(),
                                turn_id: params.turn_id.clone(),
                                work_item_ids: ids.to_vec(),
                            },
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map(Page::Items),
            };
            let mut registry = self
                .thread_registry
                .lock()
                .expect("thread registry poisoned");
            if !registry.timeline.accepts(&key, request.request_generation) {
                return;
            }
            let refresh_key = SemanticTimelineRequestKey::ThreadNewest {
                thread_id: request.id.clone(),
            };
            let is_refresh = matches!(&request.action,
                SemanticTimelineRequestAction::ThreadTimelinePage { key: SemanticTimelineRequestKey::ThreadNewest { .. }, params }
                    if params.anchor == pioneer_protocol::TimelinePageAnchor::Newest);
            let preserve_refresh_status = !is_refresh
                && (registry.timeline.in_flight.contains(&refresh_key)
                    || registry.timeline.has_failed_refresh(&request.id));
            let Some(store) = registry.stores.get_mut(&request.id) else {
                return;
            };
            if self.is_stopped() || store.generation != request.generation {
                return;
            }
            let valid_scope = |workspace: &str, id: &str| {
                workspace == store.coordinator.workspace_id && id == request.id
            };
            let mut succeeded = result.is_ok();
            let mut continue_reconciliation = false;
            match result {
                Ok(Page::Thread(mut page, mode))
                    if valid_scope(&page.workspace_id, &page.thread_id) =>
                {
                    let progress = request
                        .reconciliation
                        .as_mut()
                        .map(|walk| walk.advance(&page));
                    if let Some(walk) = &request.reconciliation {
                        // Preserve blocks changed since this operation began, including
                        // same-millisecond live updates and explicit live removals.
                        page.blocks.retain(|incoming| {
                            let cached = store
                                .semantic
                                .thread(&request.id)
                                .and_then(|thread| thread.top_level.block(&incoming.block_id));
                            let baseline = walk
                                .baseline
                                .thread(&request.id)
                                .and_then(|thread| thread.top_level.block(&incoming.block_id));
                            cached == baseline || cached == Some(incoming)
                        });
                    }
                    let refresh_status = preserve_refresh_status.then(|| {
                        store
                            .semantic
                            .thread(&request.id)
                            .unwrap()
                            .top_level
                            .request_status
                            .clone()
                    });
                    confirm_admission_page(&mut store.coordinator.conversation, &page);
                    apply_thread_timeline_page(&mut store.semantic, page, mode);
                    if let Some(status) = refresh_status {
                        set_request_status(&mut store.semantic, &refresh_key, status);
                    }
                    match progress {
                        Some(Ok(true)) => {
                            continue_reconciliation = true;
                            set_request_status(
                                &mut store.semantic,
                                &key,
                                TimelineRequestStatus::Loading {
                                    request_key: format!("{key:?}"),
                                },
                            );
                        }
                        Some(Err(error)) => {
                            succeeded = false;
                            set_request_status(
                                &mut store.semantic,
                                &key,
                                TimelineRequestStatus::Failed {
                                    message: format!("{error:#}"),
                                },
                            );
                        }
                        _ => {}
                    }
                }
                Ok(Page::Work(page, mode))
                    if valid_scope(&page.workspace_id, &page.thread_id)
                        && expected_turn.as_deref() == Some(page.turn_id.as_str()) =>
                {
                    apply_turn_work_page(&mut store.semantic, page, mode);
                }
                Ok(Page::Items(pages))
                    if pages.iter().all(|page| {
                        valid_scope(&page.workspace_id, &page.thread_id)
                            && expected_turn.as_deref() == Some(page.turn_id.as_str())
                    }) =>
                {
                    for page in pages {
                        apply_turn_work_items_get_response(&mut store.semantic, page);
                    }
                }
                Ok(_) => return,
                Err(error) => set_request_status(
                    &mut store.semantic,
                    &key,
                    TimelineRequestStatus::Failed {
                        message: format!("{error:#}"),
                    },
                ),
            }
            if continue_reconciliation {
                let drafts = registry.publish(&request.id);
                self.transition_directory(
                    &mut registry,
                    &ClientMutationAuthority { _private: () },
                    drafts,
                    vec![],
                );
                drop(registry);
                continue;
            }
            let next = registry
                .timeline
                .finish(&key, succeeded, self.timeline_now_ms())
                .and_then(|action| registry.timeline.begin(action));
            let next_generation = registry.timeline.request_generation(&key);
            if next.is_some() && is_refresh {
                set_request_status(
                    &mut registry.stores.get_mut(&request.id).unwrap().semantic,
                    &key,
                    TimelineRequestStatus::Loading {
                        request_key: format!("{key:?}"),
                    },
                );
            }
            let drafts = registry.publish(&request.id);
            let next_reconciliation = next.as_ref().and_then(|action| {
                CachedTimelineReconciliation::capture(&registry.stores[&request.id], action)
            });
            self.transition_directory(
                &mut registry,
                &ClientMutationAuthority { _private: () },
                drafts,
                vec![],
            );
            drop(registry);
            let Some(action) = next else {
                return;
            };
            request.action = action;
            request.request_generation = next_generation;
            request.reconciliation = next_reconciliation;
        }
    }
}
fn set_request_status(
    state: &mut SemanticTimelineState,
    key: &SemanticTimelineRequestKey,
    status: crate::timeline::semantic::TimelineRequestStatus,
) {
    use SemanticTimelineRequestKey::*;
    let thread = state.thread_mut(request_thread_id(key));
    match key {
        ThreadNewest { .. } | ThreadBefore { .. } | ThreadAfter { .. } => {
            thread.top_level.request_status = status
        }
        TurnWorkInitial { turn_id, .. }
        | TurnWorkBefore { turn_id, .. }
        | TurnWorkAfter { turn_id, .. }
        | TurnWorkItems { turn_id, .. } => {
            thread.work_range_mut(turn_id.clone()).request_status = status
        }
    }
}

fn thread_reconcile_actions(
    update: &crate::timeline::semantic::SemanticTimelineLiveUpdate,
) -> Vec<SemanticTimelineRequestAction> {
    use crate::timeline::semantic::*;
    match update {
        SemanticTimelineLiveUpdate::ThreadTimelineBlocksChanged(n)
            if !n.changed_block_ids.is_empty() =>
        {
            vec![SemanticTimelineRequestAction::ThreadTimelinePage {
                key: SemanticTimelineRequestKey::ThreadNewest {
                    thread_id: n.thread_id.clone(),
                },
                params: pioneer_protocol::ThreadTimelinePageParams {
                    thread_id: n.thread_id.clone(),
                    anchor: pioneer_protocol::TimelinePageAnchor::Newest,
                    limit: Some(DEFAULT_TOP_LEVEL_PAGE_LIMIT),
                },
            }]
        }
        SemanticTimelineLiveUpdate::TurnWorkItemsChanged(n)
            if !n.changed_work_item_ids.is_empty() =>
        {
            let mut ids = n.changed_work_item_ids.clone();
            ids.sort();
            ids.dedup();
            vec![SemanticTimelineRequestAction::TurnWorkItemsGet {
                key: SemanticTimelineRequestKey::TurnWorkItems {
                    thread_id: n.thread_id.clone(),
                    turn_id: n.turn_id.clone(),
                },
                params: pioneer_protocol::TurnWorkItemsGetParams {
                    thread_id: n.thread_id.clone(),
                    turn_id: n.turn_id.clone(),
                    work_item_ids: ids,
                },
            }]
        }
        _ => vec![],
    }
}

impl ClientCore {
    fn fetch_thread_page<T>(
        &self,
        action: SemanticTimelineRequestAction,
        fetch: impl FnOnce() -> anyhow::Result<T>,
        apply: impl FnOnce(
            &mut SemanticTimelineState,
            &mut crate::conversation::Conversation,
            &str,
            &str,
            &T,
        ) -> anyhow::Result<()>,
    ) -> anyhow::Result<T> {
        let request = self
            .begin_thread_semantic_request(action)
            .ok_or_else(|| anyhow::anyhow!("Timeline request cancelled or already pending"))?;
        let result = fetch();
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        let key = crate::timeline::semantic::semantic_timeline_request_key(&request.action);
        anyhow::ensure!(
            registry.timeline.accepts(key, request.request_generation),
            "Timeline request cancelled"
        );
        let store = registry
            .stores
            .get_mut(&request.id)
            .ok_or_else(|| anyhow::anyhow!("Timeline request cancelled"))?;
        anyhow::ensure!(
            !self.is_stopped() && store.generation == request.generation,
            "Timeline request cancelled"
        );
        let result = result.and_then(|page| {
            apply(
                &mut store.semantic,
                &mut store.coordinator.conversation,
                &store.coordinator.workspace_id,
                &request.id,
                &page,
            )?;
            Ok(page)
        });
        if let Err(error) = &result {
            set_request_status(
                &mut store.semantic,
                key,
                crate::timeline::semantic::TimelineRequestStatus::Failed {
                    message: format!("{error:#}"),
                },
            );
        }
        let next = registry
            .timeline
            .finish(key, result.is_ok(), self.timeline_now_ms());
        let drafts = registry.publish(&request.id);
        self.transition_directory(
            &mut registry,
            &ClientMutationAuthority { _private: () },
            drafts,
            vec![],
        );
        drop(registry);
        if let Some(next) = next {
            self.schedule_thread_semantic_request(next);
        }
        result
    }
    pub fn fetch_thread_timeline_page(
        &self,
        transport: &impl crate::rpc::JsonRpcRequestTransport,
        params: pioneer_protocol::ThreadTimelinePageParams,
    ) -> anyhow::Result<pioneer_protocol::ThreadTimelinePageResponse> {
        use crate::timeline::semantic::*;
        let id = params.thread_id.clone();
        let (key, mode) = match &params.anchor {
            pioneer_protocol::TimelinePageAnchor::Newest
            | pioneer_protocol::TimelinePageAnchor::Oldest
            | pioneer_protocol::TimelinePageAnchor::Around { .. } => (
                SemanticTimelineRequestKey::ThreadNewest { thread_id: id },
                TopLevelPageMergeMode::Merge,
            ),
            pioneer_protocol::TimelinePageAnchor::Before { cursor } => (
                SemanticTimelineRequestKey::ThreadBefore {
                    thread_id: id,
                    cursor: cursor.value.clone(),
                },
                TopLevelPageMergeMode::MergeBefore,
            ),
            pioneer_protocol::TimelinePageAnchor::After { cursor } => (
                SemanticTimelineRequestKey::ThreadAfter {
                    thread_id: id,
                    cursor: cursor.value.clone(),
                },
                TopLevelPageMergeMode::MergeAfter,
            ),
        };
        self.fetch_thread_page(
            SemanticTimelineRequestAction::ThreadTimelinePage {
                key,
                params: params.clone(),
            },
            || crate::transport::ws::command_sender::thread_timeline_page(transport, params),
            |state, conversation, workspace, id, page| {
                anyhow::ensure!(
                    page.workspace_id == workspace && page.thread_id == id,
                    "Timeline response scope mismatch"
                );
                confirm_admission_page(conversation, page);
                apply_thread_timeline_page(state, page.clone(), mode);
                Ok(())
            },
        )
    }
    pub fn fetch_turn_work_page(
        &self,
        transport: &impl crate::rpc::JsonRpcRequestTransport,
        params: pioneer_protocol::TurnWorkPageParams,
    ) -> anyhow::Result<pioneer_protocol::TurnWorkPageResponse> {
        use crate::timeline::semantic::*;
        let id = params.thread_id.clone();
        let turn = params.turn_id.clone();
        let (key, mode) = match &params.anchor {
            pioneer_protocol::TimelinePageAnchor::Oldest => (
                SemanticTimelineRequestKey::TurnWorkInitial {
                    thread_id: id,
                    turn_id: turn.clone(),
                },
                WorkPageMergeMode::MergeBefore,
            ),
            pioneer_protocol::TimelinePageAnchor::Around { .. } => (
                SemanticTimelineRequestKey::TurnWorkInitial {
                    thread_id: id,
                    turn_id: turn.clone(),
                },
                WorkPageMergeMode::Reset,
            ),
            pioneer_protocol::TimelinePageAnchor::Newest => (
                SemanticTimelineRequestKey::TurnWorkInitial {
                    thread_id: id,
                    turn_id: turn.clone(),
                },
                WorkPageMergeMode::MergeAfter,
            ),
            pioneer_protocol::TimelinePageAnchor::Before { cursor } => (
                SemanticTimelineRequestKey::TurnWorkBefore {
                    thread_id: id,
                    turn_id: turn.clone(),
                    cursor: cursor.value.clone(),
                },
                WorkPageMergeMode::MergeBefore,
            ),
            pioneer_protocol::TimelinePageAnchor::After { cursor } => (
                SemanticTimelineRequestKey::TurnWorkAfter {
                    thread_id: id,
                    turn_id: turn.clone(),
                    cursor: cursor.value.clone(),
                },
                WorkPageMergeMode::MergeAfter,
            ),
        };
        self.fetch_thread_page(
            SemanticTimelineRequestAction::TurnWorkPage {
                key,
                params: params.clone(),
            },
            || crate::transport::ws::command_sender::turn_work_page(transport, params),
            |state, _conversation, workspace, id, page| {
                anyhow::ensure!(
                    page.workspace_id == workspace && page.thread_id == id && page.turn_id == turn,
                    "Timeline response scope mismatch"
                );
                apply_turn_work_page(state, page.clone(), mode);
                Ok(())
            },
        )
    }
    pub fn fetch_turn_work_items(
        &self,
        transport: &impl crate::rpc::JsonRpcRequestTransport,
        params: pioneer_protocol::TurnWorkItemsGetParams,
    ) -> anyhow::Result<pioneer_protocol::TurnWorkItemsGetResponse> {
        use crate::timeline::semantic::*;
        let key = SemanticTimelineRequestKey::TurnWorkItems {
            thread_id: params.thread_id.clone(),
            turn_id: params.turn_id.clone(),
        };
        let turn = params.turn_id.clone();
        self.fetch_thread_page(
            SemanticTimelineRequestAction::TurnWorkItemsGet {
                key,
                params: params.clone(),
            },
            || crate::transport::ws::command_sender::turn_work_items_get(transport, params),
            |state, _conversation, workspace, id, page| {
                anyhow::ensure!(
                    page.workspace_id == workspace && page.thread_id == id && page.turn_id == turn,
                    "Timeline response scope mismatch"
                );
                apply_turn_work_items_get_response(state, page.clone());
                Ok(())
            },
        )
    }
}

pub(crate) enum ThreadControllerRequest {
    Read(crate::timeline::controller::TimelineReadRequest),
    Semantic(ThreadSemanticRequest),
    Resume,
    Subscribe {
        id: String,
        workspace: String,
        generation: u64,
    },
    Binding {
        id: String,
        workspace: String,
        generation: u64,
    },
}

impl ClientCore {
    pub fn schedule_thread_resume(&self, id: &str) {
        self.enqueue_thread_resume(id.to_owned());
        if let Some(sender) = self
            .thread_request_sender
            .lock()
            .expect("thread request sender poisoned")
            .as_ref()
        {
            let _ = sender.send(ThreadControllerRequest::Resume);
        }
    }
    pub(crate) fn next_thread_resume_delay(&self) -> Option<std::time::Duration> {
        let registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        if !registry.ready_resume.is_empty() {
            return Some(std::time::Duration::ZERO);
        }
        registry
            .stores
            .values()
            .filter(|s| !s.coordinator.resume.in_progress)
            .filter_map(|s| s.coordinator.resume.next_attempt_at)
            .min()
            .map(|at| at.saturating_duration_since(std::time::Instant::now()))
    }
    pub(crate) fn drive_due_thread_resumes(&self) {
        let ids = {
            let mut registry = self
                .thread_registry
                .lock()
                .expect("thread registry poisoned");
            let mut ids = registry.ready_resume.drain(..).collect::<HashSet<_>>();
            registry.ready_resume_set.clear();
            ids.extend(
                registry
                    .stores
                    .iter()
                    .filter(|(_, s)| {
                        !s.coordinator.resume.in_progress
                            && s.coordinator
                                .resume
                                .next_attempt_at
                                .is_some_and(|at| at <= std::time::Instant::now())
                    })
                    .map(|(id, _)| id.clone()),
            );
            ids
        };
        for id in ids {
            self.execute_thread_resume(&self.transport_runtime().ws_command_sender(), &id);
        }
    }
    pub fn execute_thread_resume(
        &self,
        transport: &impl crate::rpc::JsonRpcRequestTransport,
        id: &str,
    ) {
        use super::resume::*;
        let (turn_id, generation, workspace) = {
            let mut registry = self
                .thread_registry
                .lock()
                .expect("thread registry poisoned");
            let Some(store) = registry.stores.get_mut(id) else {
                return;
            };
            if self.is_stopped() || store.coordinator.resume.in_progress {
                return;
            }
            let Some(turn) = store
                .coordinator
                .conversation
                .in_flight_turn_id()
                .map(str::to_owned)
            else {
                reset_thread_resume_coordinator(&mut store.coordinator.resume);
                return;
            };
            begin_turn_resume_attempt(&mut store.coordinator.resume);
            (
                turn,
                store.generation,
                store.coordinator.workspace_id.clone(),
            )
        };
        let result = (|| -> anyhow::Result<TurnResumeSnapshotReduction> {
            let snapshot = crate::transport::ws::command_sender::turn_get(
                transport,
                turn_resume_turn_params(id.to_owned(), turn_id.clone()),
            )?;
            anyhow::ensure!(
                snapshot.workspace_id == workspace,
                "Resume response workspace mismatch"
            );
            if !turn_snapshot_matches_scope(id, &turn_id, &snapshot) {
                return Ok(reduce_turn_resume_turn_snapshot(id, &turn_id, snapshot));
            }
            let mut after = None;
            loop {
                {
                    let registry = self
                        .thread_registry
                        .lock()
                        .expect("thread registry poisoned");
                    anyhow::ensure!(
                        !self.is_stopped()
                            && registry
                                .stores
                                .get(id)
                                .is_some_and(|s| s.generation == generation),
                        "Resume cancelled"
                    );
                }
                let page = crate::transport::ws::command_sender::turn_items_page(
                    transport,
                    turn_resume_items_page_params(id.to_owned(), turn_id.clone(), after),
                )?;
                let page = reduce_turn_resume_items_page(&snapshot, after, page)?;
                let mut registry = self
                    .thread_registry
                    .lock()
                    .expect("thread registry poisoned");
                let Some(store) = registry.stores.get_mut(id) else {
                    anyhow::bail!("Resume cancelled");
                };
                anyhow::ensure!(
                    !self.is_stopped()
                        && store.generation == generation
                        && page.workspace_id == workspace,
                    "Resume cancelled"
                );
                for event in page.replay_events {
                    store.coordinator.conversation.apply(event);
                }
                let drafts = registry.publish(id);
                self.transition_directory(
                    &mut registry,
                    &ClientMutationAuthority { _private: () },
                    drafts,
                    vec![],
                );
                let Some(next) = page.next_cursor else {
                    break;
                };
                after = Some(next);
            }
            Ok(reduce_turn_resume_turn_snapshot(id, &turn_id, snapshot))
        })();
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        let Some(store) = registry.stores.get_mut(id) else {
            return;
        };
        if self.is_stopped() || store.generation != generation {
            return;
        }
        finish_turn_resume_attempt(&mut store.coordinator.resume);
        match result {
            Ok(TurnResumeSnapshotReduction::Apply(reduction)) => {
                for event in reduction.replay_events {
                    store.coordinator.conversation.apply(event);
                }
                if let Some(event) = reduction.terminal_event {
                    store.coordinator.conversation.apply(event);
                    if reduction.tick_conversation_after_terminal_event {
                        store.coordinator.conversation.tick();
                    }
                }
                if let Some(delay) = reduction.schedule_after {
                    schedule_turn_resume_after_state(
                        &mut store.coordinator.resume,
                        delay,
                        std::time::Instant::now(),
                    );
                }
                if reduction.reset_thread_resume {
                    reset_thread_resume_coordinator(&mut store.coordinator.resume);
                }
            }
            Ok(TurnResumeSnapshotReduction::ScopeMismatch { retry_after, .. }) => {
                schedule_turn_resume_after_state(
                    &mut store.coordinator.resume,
                    retry_after,
                    std::time::Instant::now(),
                );
            }
            Err(_) => {
                apply_turn_resume_retry(
                    Some(&mut store.coordinator.resume),
                    std::time::Instant::now(),
                );
            }
        }
        let drafts = registry.publish(id);
        self.transition_directory(
            &mut registry,
            &ClientMutationAuthority { _private: () },
            drafts,
            vec![],
        );
    }
}

impl ClientCore {
    pub fn refresh_thread_subscription(
        &self,
        transport: &impl crate::rpc::JsonRpcRequestTransport,
        id: &str,
        workspace: &str,
    ) -> anyhow::Result<()> {
        self.refresh_thread_subscription_generation(transport, id, workspace, None)
    }
    fn refresh_thread_subscription_generation(
        &self,
        transport: &impl crate::rpc::JsonRpcRequestTransport,
        id: &str,
        workspace: &str,
        expected: Option<u64>,
    ) -> anyhow::Result<()> {
        let generation = {
            let mut registry = self
                .thread_registry
                .lock()
                .expect("thread registry poisoned");
            if let Some(expected) = expected {
                anyhow::ensure!(
                    registry
                        .stores
                        .get(id)
                        .is_some_and(|store| store.generation == expected),
                    "Thread subscription cancelled"
                );
            }
            anyhow::ensure!(
                !self.is_stopped() && registry.require(id, workspace),
                "Thread subscription cancelled"
            );
            registry.stores[id].generation
        };
        let response = crate::transport::ws::command_sender::thread_start(
            transport,
            super::start::thread_start_params(id.to_owned(), workspace.to_owned()),
        )?;
        let reduction = super::start::reduce_thread_start_subscription_success(response);
        anyhow::ensure!(
            reduction.thread.id == id && reduction.thread.workspace_id == workspace,
            "Thread subscription response scope mismatch"
        );
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        let store = registry
            .stores
            .get_mut(id)
            .ok_or_else(|| anyhow::anyhow!("Thread subscription cancelled"))?;
        anyhow::ensure!(
            !self.is_stopped() && store.generation == generation,
            "Thread subscription cancelled"
        );
        store.set_snapshot(reduction.thread);
        let drafts = registry.publish(id);
        self.transition_directory(
            &mut registry,
            &ClientMutationAuthority { _private: () },
            drafts,
            vec![],
        );
        drop(registry);
        // Owned subscriptions are the opened/reconnected-thread path. Composer
        // subscriptions use the metadata-only public entry point.
        if expected.is_some() {
            self.refresh_thread_timeline_generation(id, Some(generation));
        }
        Ok(())
    }
}

impl ClientCore {
    pub fn create_workspace_thread_draft(
        &self,
        transport: &impl crate::rpc::JsonRpcRequestTransport,
        workspace: &str,
        visibility: pioneer_protocol::ThreadVisibility,
    ) -> anyhow::Result<String> {
        self.create_workspace_thread_draft_with_clock(
            transport,
            workspace,
            visibility,
            std::time::Instant::now,
        )
    }
    pub(crate) fn create_workspace_thread_draft_with_clock(
        &self,
        transport: &impl crate::rpc::JsonRpcRequestTransport,
        workspace: &str,
        visibility: pioneer_protocol::ThreadVisibility,
        now: impl FnOnce() -> std::time::Instant,
    ) -> anyhow::Result<String> {
        let connection = self.gateway_http_generation();
        let authorization = self.authorization_connection_generation();
        let workspace_generation = self.workspace_operation_generation(workspace);
        let (id, generation, navigation_revision, start_generation) = {
            let mut registry = self
                .thread_registry
                .lock()
                .expect("thread registry poisoned");
            anyhow::ensure!(!self.is_stopped(), "Thread creation cancelled");
            if let Some(id) = registry.navigation.drafts.get(workspace).cloned() {
                return Ok(id);
            }
            let plan = super::start::begin_thread_start_attempt(
                &mut registry.start,
                super::start::generate_thread_start_id(),
                workspace.to_owned(),
            )
            .ok_or_else(|| anyhow::anyhow!("Thread creation is already in progress"))?;
            let id = plan.requested_thread_id;
            registry.start_generation += 1;
            anyhow::ensure!(
                registry.require(&id, workspace),
                "Thread creation scope mismatch"
            );
            let generation = registry.stores[&id].generation;
            (
                id,
                generation,
                registry.navigation_revision,
                registry.start_generation,
            )
        };
        let result = crate::transport::ws::command_sender::thread_start(
            transport,
            super::start::thread_create_params(id.clone(), workspace.to_owned(), visibility),
        )
        .and_then(|response| {
            anyhow::ensure!(
                response.thread.id == id && response.thread.workspace_id == workspace,
                "Thread creation response scope mismatch"
            );
            Ok(response)
        });
        let scope_current = self.gateway_http_generation() == connection
            && self.authorization_connection_generation() == authorization
            && self.workspace_operation_generation(workspace) == workspace_generation;
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        if self.is_stopped()
            || !scope_current
            || registry.start_generation != start_generation
            || !registry
                .stores
                .get(&id)
                .is_some_and(|store| store.generation == generation)
        {
            if registry.start_generation == start_generation
                && registry.start.pending_thread_id.as_deref() == Some(&id)
            {
                super::start::finish_thread_start_attempt(&mut registry.start);
                registry.start.next_attempt_at = None;
            }
            anyhow::bail!("Thread creation cancelled");
        }
        let response = match result {
            Ok(response) => response,
            Err(error) => {
                if registry.start.pending_thread_id.as_deref() == Some(&id) {
                    super::start::finish_thread_start_attempt(&mut registry.start);
                }
                if registry
                    .stores
                    .get(&id)
                    .is_some_and(|s| s.snapshot.is_none())
                {
                    registry.timeline.invalidate(Some(&id));
                    registry.stores.remove(&id);
                }
                let retry = matches!(
                    super::start::plan_thread_start_bootstrap_failure(&id, &format!("{error:#}")),
                    super::start::ThreadStartBootstrapFailurePlan::Retry { .. }
                )
                .then(|| super::start::apply_thread_start_retry(&mut registry.start, &id, now()));
                drop(registry);
                if let Some(plan) = retry {
                    self.queue_thread_draft_retry(
                        crate::workspaces::controller::ThreadDraftRetry {
                            workspace: workspace.to_owned(),
                            thread: id,
                            connection,
                            authorization,
                            workspace_generation,
                            plan,
                        },
                    );
                }
                return Err(error);
            }
        };
        let reduction = super::start::reduce_thread_start_bootstrap_success(
            workspace.to_owned(),
            response,
            None,
        );
        if registry.start.pending_thread_id.as_deref() == Some(&id) {
            registry.start = Default::default();
        }
        registry
            .stores
            .get_mut(&id)
            .expect("retained draft")
            .set_snapshot(reduction.thread);
        registry
            .navigation
            .apply(crate::navigation::NavigationIntent::RememberDraft {
                workspace_id: workspace.to_owned(),
                thread_id: Some(id.clone()),
            });
        if registry.navigation_revision == navigation_revision
            && registry.navigation.workspace_id() == Some(workspace)
            && registry.navigation.active_thread_id().is_none()
        {
            registry
                .navigation
                .apply(crate::navigation::NavigationIntent::SelectThread {
                    workspace_id: Some(workspace.to_owned()),
                    thread_id: Some(id.clone()),
                });
        }
        self.publish_navigation(&mut registry);
        let drafts = registry.publish(&id);
        self.transition_directory(
            &mut registry,
            &ClientMutationAuthority { _private: () },
            drafts,
            vec![],
        );
        Ok(id)
    }
}

impl ClientCore {
    pub(crate) fn admit_opened_thread(
        &self,
        thread: Thread,
        navigation: &crate::navigation::ClientNavigationState,
    ) -> anyhow::Result<ThreadOperationToken> {
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        anyhow::ensure!(
            !self.is_stopped() && &registry.navigation == navigation,
            "thread_open_stale"
        );
        let id = thread.id.clone();
        let workspace = thread.workspace_id.clone();
        anyhow::ensure!(registry.require(&id, &workspace), "thread_open_stale");
        let store = registry.stores.get_mut(&id).expect("admitted thread");
        if store
            .coordinator
            .thread()
            .is_none_or(|previous| previous.updated_at <= thread.updated_at)
        {
            store.set_snapshot(thread);
        }
        let token = ThreadOperationToken {
            id: id.clone(),
            generation: store.generation,
        };
        registry
            .navigation
            .apply(crate::navigation::NavigationIntent::SelectThread {
                workspace_id: Some(workspace),
                thread_id: Some(id.clone()),
            });
        let mut drafts = registry.publish(&id);
        drafts.extend(registry.navigation_change());
        self.transition_directory(
            &mut registry,
            &ClientMutationAuthority { _private: () },
            drafts,
            vec![],
        );
        Ok(token)
    }

    pub(crate) fn refresh_opened_thread_subscription(
        &self,
        transport: &impl crate::rpc::JsonRpcRequestTransport,
        token: ThreadOperationToken,
        workspace: &str,
    ) -> anyhow::Result<()> {
        self.refresh_thread_subscription_generation(
            transport,
            &token.id,
            workspace,
            Some(token.generation),
        )
    }

    pub(crate) fn resume_visible_thread_delivery(&self) {
        let _identity = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        let visible = {
            let registry = self
                .thread_registry
                .lock()
                .expect("thread registry poisoned");
            registry
                .stores
                .iter()
                .filter(|(_, store)| store.demand == ClientDemand::Visible)
                .map(|(id, store)| (id.clone(), store.coordinator.workspace_id.clone()))
                .collect::<Vec<_>>()
        };
        for (id, workspace) in visible {
            self.schedule_thread_subscription(&id, &workspace);
        }
    }

    pub fn schedule_thread_subscription(&self, id: &str, workspace: &str) {
        self.schedule_thread_connection_request(id, workspace, false);
    }
    pub fn schedule_thread_cli_binding(&self, id: &str, workspace: &str) {
        self.schedule_thread_connection_request(id, workspace, true);
    }
    fn schedule_thread_connection_request(&self, id: &str, workspace: &str, binding: bool) {
        let sender = self
            .thread_request_sender
            .lock()
            .expect("thread request sender poisoned")
            .clone();
        let Some(sender) = sender else {
            return;
        };
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        if self.is_stopped() || !registry.require(id, workspace) {
            return;
        }
        let store = registry.stores.get_mut(id).expect("retained thread");
        if !binding {
            store.coordinator.history_loading = true;
            store.subscription_failed = false;
        }
        let generation = store.generation;
        let drafts = registry.publish(id);
        self.transition_directory(
            &mut registry,
            &ClientMutationAuthority { _private: () },
            drafts,
            vec![],
        );
        drop(registry);
        let request = if binding {
            ThreadControllerRequest::Binding {
                id: id.into(),
                workspace: workspace.into(),
                generation,
            }
        } else {
            ThreadControllerRequest::Subscribe {
                id: id.into(),
                workspace: workspace.into(),
                generation,
            }
        };
        let _ = sender.send(request);
    }
    pub(crate) fn execute_thread_connection_request(
        &self,
        id: &str,
        workspace: &str,
        generation: u64,
        binding: bool,
    ) {
        {
            let registry = self
                .thread_registry
                .lock()
                .expect("thread registry poisoned");
            if self.is_stopped()
                || !registry
                    .stores
                    .get(id)
                    .is_some_and(|s| s.generation == generation)
            {
                return;
            }
        }
        if binding {
            let result = crate::transport::ws::command_sender::cli_runtime_thread_binding_get(
                &self.transport_runtime().ws_command_sender(),
                pioneer_protocol::CLIRuntimeThreadBindingGetParams {
                    workspace_id: workspace.into(),
                    thread_id: id.into(),
                },
            );
            let mut registry = self
                .thread_registry
                .lock()
                .expect("thread registry poisoned");
            let Some(store) = registry.stores.get_mut(id) else {
                return;
            };
            if self.is_stopped() || store.generation != generation {
                return;
            }
            store.cli_binding = result
                .ok()
                .and_then(|r| r.binding)
                .filter(|binding| binding.thread_id == id && binding.workspace_id == workspace);
            let drafts = registry.publish(id);
            self.transition_directory(
                &mut registry,
                &ClientMutationAuthority { _private: () },
                drafts,
                vec![],
            );
            drop(registry);
            self.observe_composer_runtime(id, None, false);
        } else {
            let result = self.refresh_thread_subscription_generation(
                &self.transport_runtime().ws_command_sender(),
                id,
                workspace,
                Some(generation),
            );
            let mut registry = self
                .thread_registry
                .lock()
                .expect("thread registry poisoned");
            let Some(store) = registry.stores.get_mut(id) else {
                return;
            };
            if self.is_stopped() || store.generation != generation {
                return;
            }
            store.coordinator.history_loading = false;
            store.subscription_failed = result.is_err();
            let drafts = registry.publish(id);
            self.transition_directory(
                &mut registry,
                &ClientMutationAuthority { _private: () },
                drafts,
                vec![],
            );
        }
    }
}

/// Identifies an operation's original store incarnation across asynchronous I/O.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ThreadOperationToken {
    id: String,
    generation: u64,
}
impl ClientCore {
    pub fn thread_operation_token(&self, id: &str) -> Option<ThreadOperationToken> {
        self.thread_registry
            .lock()
            .expect("thread registry poisoned")
            .stores
            .get(id)
            .map(|store| ThreadOperationToken {
                id: id.into(),
                generation: store.generation,
            })
    }
    pub fn apply_thread_start_send_result(
        &self,
        token: ThreadOperationToken,
        reduction: crate::turns::start::TurnStartSendReduction,
    ) -> bool {
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        let Some(store) = registry.stores.get_mut(&token.id) else {
            return false;
        };
        if self.is_stopped() || store.generation != token.generation {
            return false;
        }
        let events = match reduction {
            crate::turns::start::TurnStartSendReduction::Accepted { events } => events,
            crate::turns::start::TurnStartSendReduction::Rejected { event } => vec![event],
        };
        if events
            .iter()
            .any(|event| event.thread_id() != Some(token.id.as_str()))
        {
            return false;
        }
        for event in events {
            if store
                .coordinator
                .conversation
                .ignores_admission_response(&event)
            {
                continue;
            }

            store.coordinator.conversation.apply(event.clone());
            crate::timeline::semantic::apply_local_composer_event_to_semantic_timeline(
                &mut store.semantic,
                &store.coordinator.workspace_id,
                &event,
                store
                    .coordinator
                    .thread()
                    .map(|thread| thread.origin_kind.composer_execution_mode())
                    .unwrap_or(pioneer_protocol::ThreadComposerExecutionMode::ForegroundTurn),
                crate::timeline::labels::now_unix_ms(),
            );
        }
        let drafts = registry.publish(&token.id);
        self.transition_directory(
            &mut registry,
            &ClientMutationAuthority { _private: () },
            drafts,
            vec![],
        );
        true
    }
}

impl ClientCore {
    fn request_thread_presentation(&self, id: &str) {
        if self
            .presentation_sender
            .lock()
            .expect("presentation sender poisoned")
            .is_some()
        {
            self.wake_thread_presentation();
        } else {
            self.materialize_thread_timeline(id);
        }
    }

    pub(crate) fn wake_thread_presentation(&self) {
        if let Some(sender) = self
            .presentation_sender
            .lock()
            .expect("presentation sender poisoned")
            .as_ref()
        {
            let _ = sender.try_send(());
        }
    }

    /// Read-only access never materializes or advances a revision.
    pub fn thread_presentation_snapshot(
        &self,
        id: &str,
    ) -> Option<Arc<crate::timeline::presentation::ThreadPresentationSnapshot>> {
        self.thread_registry
            .lock()
            .expect("thread registry poisoned")
            .stores
            .get(id)
            .filter(|store| !store.presentation_blocked)
            .and_then(|store| store.presentation.clone())
    }

    pub(crate) fn materialize_demanded_timelines(&self) {
        let ids = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned")
            .stores
            .iter()
            .filter(|(_, store)| store.demand != ClientDemand::Suspended)
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        for id in ids {
            self.materialize_thread_timeline(&id);
        }
    }

    pub(crate) fn materialize_thread_timeline(&self, id: &str) {
        let (source, previous, generation, revision) = {
            let registry = self
                .thread_registry
                .lock()
                .expect("thread registry poisoned");
            let Some(store) = registry.stores.get(id) else {
                return;
            };
            if store.presentation_blocked
                || store.demand == ClientDemand::Suspended
                || self.is_stopped()
            {
                return;
            }
            let Some(source) = store.snapshot.clone() else {
                return;
            };
            let previous = store
                .presentation
                .as_ref()
                .map(|snapshot| snapshot.timeline());
            if previous
                .as_ref()
                .is_some_and(|p| p.source_revision() == source.timeline_revision())
            {
                return;
            }
            (
                source,
                previous,
                store.generation,
                registry
                    .presentation_revisions
                    .get(id)
                    .copied()
                    .unwrap_or(0)
                    + 1,
            )
        };
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(i64::MAX as u128) as i64;
        let (snapshot, changes) = crate::timeline::presentation::project(
            id,
            generation,
            &source,
            previous.as_deref(),
            revision,
            now,
        );
        self.accept_thread_timeline(
            id,
            source.timeline_revision(),
            generation,
            revision,
            snapshot,
            changes,
        );
    }

    fn accept_thread_timeline(
        &self,
        id: &str,
        source_revision: u64,
        generation: u64,
        revision: u64,
        snapshot: Arc<crate::timeline::presentation::TimelineSnapshot>,
        changes: crate::timeline::presentation::TimelineChangeSet,
    ) -> bool {
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        let Some(store) = registry.stores.get_mut(id) else {
            return false;
        };
        if self.is_stopped()
            || store.presentation_blocked
            || store.demand == ClientDemand::Suspended
            || store.generation != generation
            || store
                .snapshot
                .as_ref()
                .is_none_or(|current| current.timeline_revision() != source_revision)
            || store
                .presentation
                .as_ref()
                .is_some_and(|current| current.timeline().revision() >= revision)
        {
            self.wake_thread_presentation();
            return false;
        }
        let initial_work = snapshot
            .semantic_rows()
            .request_hints
            .iter()
            .filter_map(|hint| match hint {
                crate::timeline::semantic::SemanticTimelineRequestHint::TurnWorkInitial {
                    thread_id,
                    turn_id,
                } => Some(SemanticTimelineRequestAction::TurnWorkPage {
                    key: SemanticTimelineRequestKey::TurnWorkInitial {
                        thread_id: thread_id.clone(),
                        turn_id: turn_id.clone(),
                    },
                    params: pioneer_protocol::TurnWorkPageParams {
                        thread_id: thread_id.clone(),
                        turn_id: turn_id.clone(),
                        anchor: pioneer_protocol::TimelinePageAnchor::Newest,
                        limit: Some(crate::timeline::semantic::DEFAULT_TURN_WORK_PAGE_LIMIT),
                    },
                }),
                _ => None,
            })
            .collect::<Vec<_>>();
        let authority = ClientMutationAuthority { _private: () };
        let transition = self.transition_directory(
            &mut registry,
            &authority,
            vec![
                authority
                    .timeline_publication(snapshot.clone())
                    .with_timeline_change(changes),
            ],
            Vec::new(),
        );
        if transition.outcome() != crate::core::ClientTransitionOutcome::Changed {
            return false;
        }
        registry
            .stores
            .get_mut(id)
            .expect("accepted thread")
            .presentation = Some(Arc::new(
            crate::timeline::presentation::ThreadPresentationSnapshot::new(snapshot),
        ));
        registry
            .presentation_revisions
            .insert(id.to_owned(), revision);
        registry.retired_timelines.remove(id);
        drop(registry);
        for action in initial_work {
            self.schedule_thread_semantic_request(action);
        }
        true
    }
}

impl ClientCore {
    pub fn set_thread_timeline_expansion(
        &self,
        id: &str,
        row_ids: &[String],
        collapsed_row_ids: &[String],
    ) {
        for (ids, expanded) in [(row_ids, true), (collapsed_row_ids, false)] {
            for row in ids {
                if let Some(turn) = row
                    .strip_prefix(crate::timeline::semantic_render::SEMANTIC_TURN_WORK_GROUP_PREFIX)
                {
                    self.set_thread_turn_work_expanded(id, turn, expanded);
                }
            }
        }
    }

    pub fn set_thread_turn_work_expanded(&self, id: &str, turn: &str, expanded: bool) -> bool {
        let Some(mut state) = self.existing_thread_timeline_mutation(id) else {
            return false;
        };
        let changed = if expanded {
            crate::timeline::semantic::expand_turn_work(&mut state, id.to_owned(), turn.to_owned())
        } else {
            crate::timeline::semantic::collapse_turn_work(
                &mut state,
                id.to_owned(),
                turn.to_owned(),
            )
        };
        let stale = if expanded {
            state
                .thread(id)
                .and_then(|thread| thread.work_range(turn))
                .map(|range| {
                    range
                        .stale_work_item_ids()
                        .into_iter()
                        .map(str::to_owned)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default()
        } else {
            Vec::new()
        };
        drop(state);
        if !stale.is_empty() {
            self.schedule_thread_semantic_request(
                SemanticTimelineRequestAction::TurnWorkItemsGet {
                    key: SemanticTimelineRequestKey::TurnWorkItems {
                        thread_id: id.to_owned(),
                        turn_id: turn.to_owned(),
                    },
                    params: pioneer_protocol::TurnWorkItemsGetParams {
                        thread_id: id.to_owned(),
                        turn_id: turn.to_owned(),
                        work_item_ids: stale,
                    },
                },
            );
        }
        changed
    }

    pub fn refresh_thread_timeline(&self, id: &str) {
        self.refresh_thread_timeline_generation(id, None);
    }

    fn refresh_thread_timeline_generation(&self, id: &str, expected_store: Option<u64>) {
        use crate::timeline::semantic::*;
        if self.thread_snapshot(id).is_none() {
            return;
        }
        self.schedule_thread_semantic_request_generation(
            SemanticTimelineRequestAction::ThreadTimelinePage {
                key: SemanticTimelineRequestKey::ThreadNewest {
                    thread_id: id.to_owned(),
                },
                params: pioneer_protocol::ThreadTimelinePageParams {
                    thread_id: id.to_owned(),
                    anchor: pioneer_protocol::TimelinePageAnchor::Newest,
                    limit: Some(DEFAULT_TOP_LEVEL_PAGE_LIMIT),
                },
            },
            expected_store,
        );
        let semantic = self.thread_semantic_snapshot(id);
        let Some(thread) = semantic.thread(id) else {
            return;
        };
        for (turn_id, range) in &thread.work_ranges_by_turn {
            if thread
                .cached_turn_work_block(turn_id)
                .is_some_and(|work| resolve_work_expanded(work, &thread.expansion))
            {
                self.schedule_thread_semantic_request_generation(
                    SemanticTimelineRequestAction::TurnWorkPage {
                        key: SemanticTimelineRequestKey::TurnWorkInitial {
                            thread_id: id.to_owned(),
                            turn_id: turn_id.clone(),
                        },
                        params: pioneer_protocol::TurnWorkPageParams {
                            thread_id: id.to_owned(),
                            turn_id: turn_id.clone(),
                            anchor: pioneer_protocol::TimelinePageAnchor::Newest,
                            limit: Some(DEFAULT_TURN_WORK_PAGE_LIMIT),
                        },
                    },
                    expected_store,
                );
            }
            let mut work_item_ids: Vec<_> = range
                .stale_work_item_ids()
                .into_iter()
                .map(str::to_owned)
                .collect();
            work_item_ids.extend(range.running_work_item_ids());
            work_item_ids.sort();
            work_item_ids.dedup();
            if !work_item_ids.is_empty() {
                self.schedule_thread_semantic_request_generation(
                    SemanticTimelineRequestAction::TurnWorkItemsGet {
                        key: SemanticTimelineRequestKey::TurnWorkItems {
                            thread_id: id.to_owned(),
                            turn_id: turn_id.clone(),
                        },
                        params: pioneer_protocol::TurnWorkItemsGetParams {
                            thread_id: id.to_owned(),
                            turn_id: turn_id.clone(),
                            work_item_ids,
                        },
                    },
                    expected_store,
                );
            }
        }
    }

    /// Viewport geometry remains in the shell; Client resolves identities, ranges and request policy.
    pub fn plan_thread_timeline_viewport(
        &self,
        id: &str,
        source_revision: u64,
        row_ids: &[String],
        threshold: usize,
        before: bool,
        after: bool,
        work: bool,
        presented_rows: bool,
    ) -> Vec<SemanticTimelineRequestAction> {
        self.plan_thread_timeline_demand(
            id,
            source_revision,
            row_ids,
            threshold,
            before,
            after,
            work,
            presented_rows,
        )
        .into_iter()
        .filter(|action| {
            !self.thread_semantic_in_flight(id).contains(
                crate::timeline::semantic::semantic_timeline_request_key(action),
            )
        })
        .collect()
    }
    pub(crate) fn plan_thread_timeline_demand(
        &self,
        id: &str,
        source_revision: u64,
        row_ids: &[String],
        threshold: usize,
        before: bool,
        after: bool,
        work: bool,
        presented_rows: bool,
    ) -> Vec<SemanticTimelineRequestAction> {
        use crate::timeline::semantic::*;
        let Some(presentation) = self.thread_presentation_snapshot(id) else {
            return Vec::new();
        };
        let timeline = presentation.timeline();
        if timeline.source_revision() != source_revision {
            return Vec::new();
        }
        let semantic_ids = timeline.semantic_row_ids();
        let positions = timeline
            .rows()
            .iter()
            .enumerate()
            .filter(|(_, row)| row_ids.iter().any(|id| id == row.id().as_str()))
            .map(|(ix, _)| ix)
            .collect::<Vec<_>>();
        if positions.is_empty() {
            return Vec::new();
        }
        let visible_rows = row_ids
            .iter()
            .filter_map(|id| semantic_ids.get(id).cloned())
            .map(|row_id| SemanticTimelineVisibleRow {
                row_id,
                top_offset_px: 0,
            })
            .collect();
        plan_semantic_timeline_requests_from_rows(
            &timeline.semantic_rows(),
            &SemanticTimelineRequestPlannerInput {
                visible_rows: if presented_rows {
                    timeline
                        .semantic_rows()
                        .rows
                        .iter()
                        .map(|row| SemanticTimelineVisibleRow {
                            row_id: row.id.clone(),
                            top_offset_px: 0,
                        })
                        .collect()
                } else {
                    visible_rows
                },
                leading_threshold_rows: if presented_rows {
                    usize::MAX
                } else {
                    threshold
                },
                trailing_threshold_rows: if presented_rows {
                    usize::MAX
                } else {
                    threshold
                },
                top_level_limit: DEFAULT_TOP_LEVEL_PAGE_LIMIT,
                turn_work_limit: DEFAULT_TURN_WORK_PAGE_LIMIT,
                in_flight: Default::default(),
            },
        )
        .actions
        .into_iter()
        .filter(|action| {
            !presented_rows
                || timeline.presented_boundary_allows(
                    semantic_timeline_request_key(action),
                    &positions,
                    threshold,
                )
        })
        .filter(|action| match semantic_timeline_request_key(action) {
            SemanticTimelineRequestKey::ThreadBefore { .. } => before,
            SemanticTimelineRequestKey::ThreadAfter { .. } => after,
            SemanticTimelineRequestKey::TurnWorkBefore { .. }
            | SemanticTimelineRequestKey::TurnWorkAfter { .. } => work,
            _ => true,
        })
        .collect()
    }
}

fn timeline_demand(
    demands: &HashMap<ClientScope, ClientDemand>,
    id: &str,
    timeline_subscriptions: usize,
) -> ClientDemand {
    demands
        .get(&ClientScope::Timeline {
            thread_id: id.to_owned(),
        })
        .copied()
        .unwrap_or(if timeline_subscriptions > 0 {
            ClientDemand::Visible
        } else {
            ClientDemand::Suspended
        })
}

impl ClientCore {
    // Identity is a domain input to authorship grouping. Protected-publication fences
    // invalidate derived outputs until the thread owner accepts fresh source state.
    pub(crate) fn update_thread_presentation_identity(&self, principal: Option<String>) {
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        if registry.current_principal_id == principal {
            return;
        }
        registry.current_principal_id = principal;
        let ids = registry
            .stores
            .iter()
            .filter(|(_, store)| !store.presentation_blocked)
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        let mut drafts = Vec::new();
        for id in ids {
            drafts.extend(registry.publish(&id));
        }
        self.transition_directory(
            &mut registry,
            &ClientMutationAuthority { _private: () },
            drafts,
            Vec::new(),
        );
    }
}

impl ThreadRegistry {
    pub(crate) fn fence_presentations(&mut self, principal: Option<String>) {
        super::start::finish_thread_start_attempt(&mut self.start);
        self.start.next_attempt_at = None;
        self.timeline.invalidate(None);
        self.current_principal_id = principal;
        self.pending_requests
            .apply(PendingRequestsReduction::ClearAll);
        self.directory.invalidate();
        self.ready_resume.clear();
        self.ready_resume_set.clear();
        for (id, store) in &mut self.stores {
            // A protected-publication fence also retires the source incarnation:
            // neither an old RPC completion nor retained semantic pages can revive it.
            self.clock = self.clock.saturating_add(1);
            let mut fresh = ThreadDomainStore::new(id, &store.coordinator.workspace_id);
            fresh.generation = self.clock;
            fresh.subscriptions = store.subscriptions;
            fresh.demand = store.demand;
            fresh.last_used = store.last_used;
            fresh.presentation_blocked = true;
            *store = fresh;
        }
    }
    pub(crate) fn synchronize_publication_revisions(
        &mut self,
        publications: &[crate::core::ClientPublicationReference],
    ) {
        for publication in publications {
            match publication.scope() {
                ClientScope::WorkspaceTree {
                    workspace_id: Some(workspace),
                } => {
                    self.directory
                        .synchronize_revision(workspace, publication.revisions().scoped().get());
                }
                ClientScope::Thread { thread_id } => {
                    let revisions = self.revisions.entry(thread_id.clone()).or_default();
                    revisions.0 = publication.revisions().domain().get();
                    revisions.1 = publication.revisions().presentation().get();
                    if publication.typed::<ThreadDomainSnapshot>().is_some() {
                        self.retired.remove(thread_id);
                    } else {
                        self.retired.insert(thread_id.clone());
                    }
                }
                ClientScope::Timeline { thread_id } => {
                    self.presentation_revisions
                        .insert(thread_id.clone(), publication.revisions().scoped().get());
                    if publication
                        .typed::<crate::timeline::presentation::TimelineSnapshot>()
                        .is_some()
                    {
                        self.retired_timelines.remove(thread_id);
                    } else {
                        self.retired_timelines.insert(thread_id.clone());
                    }
                }
                ClientScope::SidebarSummary { thread_id, .. } => {
                    self.revisions.entry(thread_id.clone()).or_default().2 =
                        publication.revisions().scoped().get();
                    self.directory.summaries.remove(thread_id);
                }
                _ => {}
            }
        }
    }
}

impl ClientCore {
    pub(crate) fn transition_directory(
        &self,
        registry: &mut ThreadRegistry,
        authority: &ClientMutationAuthority,
        mut drafts: Vec<ClientPublicationDraft>,
        effects: Vec<crate::core::ClientEffectPlan>,
    ) -> crate::core::ClientTransition {
        for workspace in std::mem::take(&mut registry.directory.dirty_workspaces) {
            let draft = registry.navigation.drafts.get(&workspace).cloned();
            if let Some(publication) =
                registry
                    .directory
                    .project(&workspace, draft.as_deref(), false, None)
            {
                drafts.push(authority.publication(
                    ClientScope::WorkspaceTree {
                        workspace_id: Some(workspace),
                    },
                    revisions(publication.revision()),
                    publication,
                ));
            }
        }
        self.transition(authority, drafts, effects)
    }
}

impl ClientCore {
    pub(crate) fn begin_thread_turn_cancellation(
        &self,
        id: &str,
        reason: Option<String>,
    ) -> Option<(ThreadOperationToken, pioneer_protocol::TurnCancelParams)> {
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        let store = registry.stores.get_mut(id)?;
        let turn = store
            .coordinator
            .conversation
            .in_flight_turn_id()?
            .to_owned();
        let request = crate::turns::cancel::plan_turn_cancel_request(
            id,
            turn,
            store.coordinator.conversation.is_cancelling_turn(),
            reason,
        )?;
        let token = ThreadOperationToken {
            id: id.into(),
            generation: store.generation,
        };
        store
            .coordinator
            .conversation
            .apply(request.requested_event);
        let drafts = registry.publish(id);
        self.transition_directory(
            &mut registry,
            &ClientMutationAuthority { _private: () },
            drafts,
            vec![],
        );
        Some((token, request.params))
    }
    pub(crate) fn finish_thread_turn_cancellation(
        &self,
        token: &ThreadOperationToken,
        turn: &str,
        event: Option<crate::conversation::events::ConversationEvent>,
        abandoned: bool,
    ) -> bool {
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        let Some(store) = registry
            .stores
            .get_mut(&token.id)
            .filter(|s| s.generation == token.generation)
        else {
            return false;
        };
        if store.coordinator.conversation.in_flight_turn_id() != Some(turn)
            || !store.coordinator.conversation.is_cancelling_turn()
        {
            return false;
        }
        if abandoned {
            store
                .coordinator
                .conversation
                .abandon_turn_cancellation(turn);
        } else if let Some(event) = event {
            store.coordinator.conversation.apply(event.clone());
            crate::timeline::semantic::apply_conversation_event_to_semantic_timeline(
                &mut store.semantic,
                &store.coordinator.workspace_id,
                &event,
                crate::timeline::labels::now_unix_ms(),
            );
        }
        let drafts = registry.publish(&token.id);
        self.transition_directory(
            &mut registry,
            &ClientMutationAuthority { _private: () },
            drafts,
            vec![],
        );
        true
    }
}
