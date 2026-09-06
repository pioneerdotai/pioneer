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
    coordinator: Arc<ThreadCoordinator>,
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

impl ThreadDomainSnapshot {
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
        Source {
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

struct ThreadDomainStore {
    coordinator: ThreadCoordinator,
    semantic: SemanticTimelineState,
    cli_binding: Option<CLIRuntimeThreadBinding>,
    in_flight: HashSet<SemanticTimelineRequestKey>,
    pending: HashMap<SemanticTimelineRequestKey, SemanticTimelineRequestAction>,
    snapshot: Option<Arc<ThreadDomainSnapshot>>,
    summary_revision: u64,
    demand: ClientDemand,
    subscriptions: usize,
    last_used: u64,
    generation: u64,
    subscription_failed: bool,
}

impl ThreadDomainStore {
    fn new(id: &str, workspace: &str) -> Self {
        Self {
            coordinator: ThreadCoordinator::pending(id, workspace),
            semantic: Default::default(),
            cli_binding: None,
            in_flight: Default::default(),
            pending: Default::default(),
            snapshot: None,
            summary_revision: 0,
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
    stores: HashMap<String, ThreadDomainStore>,
    catalog: HashMap<String, Thread>,
    placements: HashMap<String, pioneer_protocol::ThreadPlacement>,
    revisions: HashMap<String, (u64, u64, u64)>,
    retired: HashSet<String>,
    summaries: HashMap<String, SidebarSummaryChanged>,
    subscription_counts: HashMap<String, usize>,
    demands: HashMap<ClientScope, ClientDemand>,
    pending_requests: PendingRequestRegistry,
    active_thread_id: Option<String>,
    drafts: HashMap<String, String>,
    last_active: HashMap<String, String>,
    start: ThreadStartCoordinator,
    start_requested: bool,
    ready_resume: VecDeque<String>,
    ready_resume_set: HashSet<String>,
    clock: u64,
    session_revision: u64,
    invalidation_revision: Option<u64>,
    last_access: Option<(
        pioneer_protocol::AccessChangedNotification,
        crate::authorization::AccessChangedPlan,
    )>,
    last_policy: Option<pioneer_protocol::AuthorizationProjectionChangedNotification>,
}

impl ThreadRegistry {
    fn require(&mut self, id: &str, workspace: &str) -> bool {
        if id.is_empty() || workspace.is_empty() {
            return false;
        }
        if self
            .catalog
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
            if let Some(thread) = self.catalog.get(id) { store.coordinator.set_snapshot(thread.clone()); }
            store.generation = self.clock;
            store.subscriptions = self.subscription_counts.get(id).copied().unwrap_or(0);
            store.demand = self.demands.iter().filter(|(scope,_)| matches!(scope,ClientScope::Thread {thread_id} | ClientScope::Timeline {thread_id} if thread_id == id)).map(|(_,d)|*d).find(|d|*d != ClientDemand::Suspended).unwrap_or(ClientDemand::Suspended);
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
        let previous = store.snapshot.as_ref();
        let namespace = self.revisions.get(id).copied().unwrap_or_default();
        let old_revision = namespace.0;
        let old_timeline_revision = namespace.1;
        let mut snapshot = ThreadDomainSnapshot {
            coordinator: Arc::new(store.coordinator.snapshot_copy()),
            semantic: Arc::new(store.semantic.clone()),
            pending: Arc::new(pending),
            cli_binding: store.cli_binding.clone(),
            revision: old_revision,
            timeline_revision: old_timeline_revision,
            execution_revision: previous.map_or(0, |p| p.execution_revision)
                + u64::from(
                    previous.is_none_or(|p| p.coordinator.resume != store.coordinator.resume),
                ),
            placement: self.placements.get(id).cloned(),
            subscription_failed: store.subscription_failed,
            cache_patch: semantic_cache_projection(
                id,
                &store.coordinator.workspace_id,
                &store.semantic,
                previous.map(AsRef::as_ref),
            ),
        };
        let mut current_value =
            serde_json::to_value(&snapshot).expect("thread snapshot serializes");
        current_value["projection"]["revision"] = serde_json::Value::from(0);
        if previous.is_some_and(|previous| {
            let mut previous_value =
                serde_json::to_value(previous.as_ref()).expect("thread snapshot serializes");
            previous_value["projection"]["revision"] = serde_json::Value::from(0);
            current_value == previous_value
        }) {
            return Vec::new();
        }
        let timeline_changed = previous.is_none_or(|previous| {
            if previous.semantic.as_ref() != &store.semantic || previous.pending != snapshot.pending
            {
                return true;
            }
            let mut old = serde_json::to_value(previous.coordinator.conversation.projection())
                .expect("projection serializes");
            let mut new = serde_json::to_value(snapshot.coordinator.conversation.projection())
                .expect("projection serializes");
            old["revision"] = 0.into();
            new["revision"] = 0.into();
            old != new
        });
        snapshot.revision += 1;
        if timeline_changed {
            snapshot.timeline_revision += 1;
        }
        let snapshot = Arc::new(snapshot);
        let authority = ClientMutationAuthority { _private: () };
        let mut drafts = vec![authority.publication(
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
        if timeline_changed {
            drafts.push(authority.publication(
                ClientScope::Timeline {
                    thread_id: id.to_owned(),
                },
                revisions(snapshot.timeline_revision),
                snapshot.clone(),
            ));
        }
        let mut thread_summary = store.coordinator.thread().cloned();
        if let Some(thread) = &mut thread_summary {
            thread.turns.clear();
        }
        let summary = SidebarSummaryChanged {
            thread_id: id.to_owned(),
            workspace_id: store.coordinator.workspace_id.clone(),
            thread: thread_summary,
            placement: self.placements.get(id).cloned(),
        };
        if self.summaries.get(id) != Some(&summary) {
            store.summary_revision = namespace.2 + 1;
            drafts.push(authority.publication(
                ClientScope::SidebarSummary {
                    workspace_id: summary.workspace_id.clone(),
                    thread_id: id.to_owned(),
                },
                revisions(store.summary_revision),
                Arc::new(summary.clone()),
            ));
            self.summaries.insert(id.to_owned(), summary);
        }
        if let Some(mut thread) = store.coordinator.thread().cloned() {
            thread.turns.clear();
            self.catalog.insert(id.to_owned(), thread);
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

fn revisions(value: u64) -> ClientRevisions {
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
        self.core.transition(
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
        self.core.transition(
            &ClientMutationAuthority { _private: () },
            drafts,
            Vec::new(),
        );
    }
}

impl ClientCore {
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

    pub fn apply_pending_requests(&self, reduction: PendingRequestsReduction) -> bool {
        if self.is_stopped() {
            return false;
        }
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
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
        self.transition(
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
                .catalog
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
        registry.stores.remove(id);
        registry.catalog.remove(id);
        registry.placements.remove(id);
        registry.drafts.retain(|_, value| value != id);
        registry.last_active.retain(|_, value| value != id);
        registry.ready_resume.retain(|value| value != id);
        registry.ready_resume_set.remove(id);
        if registry.active_thread_id.as_deref() == Some(id) {
            registry.active_thread_id = None;
            registry.session_revision += 1;
        }
        let mut drafts = registry.retire(id);
        drafts.extend(registry.retire_summary(id));
        self.transition(
            &ClientMutationAuthority { _private: () },
            drafts,
            Vec::new(),
        );
    }
    pub fn clear_thread_stores(&self) {
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
        let retired = std::mem::take(&mut registry.retired);
        let revisions = std::mem::take(&mut registry.revisions);
        let subscriptions = std::mem::take(&mut registry.subscription_counts);
        let demands = std::mem::take(&mut registry.demands);
        let clock = registry.clock.saturating_add(1);
        let session_revision = registry.session_revision.saturating_add(1);
        *registry = ThreadRegistry {
            clock,
            revisions,
            retired,
            demands,
            subscription_counts: subscriptions,
            session_revision,
            ..Default::default()
        };
        self.transition(
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
        self.transition(
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
        crate::timeline::semantic::apply_thread_timeline_page(&mut state, page, mode)
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
            .stores
            .get(id)
            .map(|s| s.in_flight.clone())
            .unwrap_or_default()
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
        let store = registry.stores.get_mut(id)?;
        crate::timeline::semantic::enqueue_semantic_timeline_request(
            &mut store.in_flight,
            &mut store.pending,
            action,
        )
    }
    pub fn finish_thread_semantic_request(
        &self,
        key: &SemanticTimelineRequestKey,
    ) -> Option<SemanticTimelineRequestAction> {
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        let store = registry.stores.get_mut(request_thread_id(key))?;
        crate::timeline::semantic::finish_semantic_timeline_request(
            &mut store.in_flight,
            &mut store.pending,
            key,
        )
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
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        registry.clock = registry.clock.saturating_add(1);
        let generation = registry.clock;
        registry.ready_resume.clear();
        registry.ready_resume_set.clear();
        let mut changed = Vec::new();
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
            let keys = store.in_flight.drain().collect::<Vec<_>>();
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
            store.pending.clear();
        }
        changed.sort();
        changed.dedup();
        let drafts = changed
            .into_iter()
            .flat_map(|id| registry.publish(&id))
            .collect();
        self.transition(&ClientMutationAuthority { _private: () }, drafts, vec![]);
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
        self.transition(
            &ClientMutationAuthority { _private: () },
            drafts,
            Vec::new(),
        );
    }
}

impl ClientCore {
    pub fn active_thread_id(&self) -> Option<String> {
        self.thread_registry
            .lock()
            .expect("thread registry poisoned")
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
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        let next = id.map(str::to_owned);
        if registry.active_thread_id != next {
            registry.active_thread_id = next.clone();
            registry.session_revision += 1;
        }
        if let (Some(id), Some(workspace)) = (next, workspace) {
            if registry.last_active.get(workspace) != Some(&id) {
                registry.last_active.insert(workspace.to_owned(), id);
                registry.session_revision += 1;
            }
        }
    }
    pub fn thread_workspace_draft(&self, workspace: &str) -> Option<String> {
        let registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        registry
            .drafts
            .get(workspace)
            .filter(|id| registry.stores.contains_key(*id))
            .cloned()
    }
    pub fn thread_workspace_last_active(&self, workspace: &str) -> Option<String> {
        self.thread_registry
            .lock()
            .expect("thread registry poisoned")
            .last_active
            .get(workspace)
            .cloned()
    }
    pub fn remember_thread_draft(&self, workspace: &str, id: Option<String>) {
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        if super::session::remember_thread_for_workspace(&mut registry.drafts, workspace, id) {
            registry.session_revision += 1;
        }
    }
    pub fn promote_thread(&self, id: &str) -> bool {
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        let changed = super::session::clear_thread_markers(&mut registry.drafts, id);
        if changed {
            registry.session_revision += 1;
        }
        changed
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
        store.coordinator.conversation.apply(event.clone());
        let patch = match mode {
            Some(mode) => crate::timeline::semantic::apply_local_composer_event_to_semantic_timeline_with_patch(&mut store.semantic, workspace, &event, mode, crate::timeline::labels::now_unix_ms()),
            None => crate::timeline::semantic::apply_conversation_event_to_semantic_timeline_with_patch(&mut store.semantic, workspace, &event, crate::timeline::labels::now_unix_ms()),
        };
        let drafts = registry.publish(&id);
        self.transition(
            &ClientMutationAuthority { _private: () },
            drafts,
            Vec::new(),
        );
        patch
    }
    pub fn request_thread_cancel(
        &self,
        id: &str,
        reason: Option<String>,
    ) -> Option<(String, pioneer_protocol::TurnCancelParams)> {
        let mut coordinator = self.existing_thread_mutation(id)?;
        let turn = coordinator.conversation.in_flight_turn_id()?.to_owned();
        let request = crate::turns::cancel::plan_turn_cancel_request(
            id.to_owned(),
            turn.clone(),
            coordinator.conversation.is_cancelling_turn(),
            reason,
        )?;
        coordinator.conversation.apply(request.requested_event);
        Some((turn, request.params))
    }
    pub fn reject_thread_cancel(&self, id: &str, turn: &str, error: &str) {
        if let Some(mut coordinator) = self.existing_thread_mutation(id) {
            coordinator
                .conversation
                .apply(crate::turns::cancel::local_turn_cancel_rejected_event(
                    id.to_owned(),
                    turn.to_owned(),
                    error.to_owned(),
                ));
        }
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
                    .catalog
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
        let (active, revision) = {
            let registry = self
                .thread_registry
                .lock()
                .expect("thread registry poisoned");
            (
                registry.active_thread_id.clone(),
                registry.invalidation_revision,
            )
        };
        let workspace = active
            .as_deref()
            .and_then(|id| self.thread_coordinator_snapshot(id))
            .map(|c| c.workspace_id.clone());
        let scopes = self
            .thread_coordinator_snapshots()
            .into_iter()
            .map(|(id, s)| crate::authorization::ThreadAuthorizationScope {
                thread_id: id,
                workspace_id: s.workspace_id.clone(),
            })
            .collect::<Vec<_>>();
        let plan = crate::authorization::plan_access_changed(
            notification,
            revision,
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
                registry.drafts.remove(&plan.workspace_id);
                registry.last_active.remove(&plan.workspace_id);
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
            self.remove_thread_store(&id);
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
                if let Some(placement) = r.placement {
                    if placement.thread_id == r.thread.id
                        && placement.workspace_id == r.thread.workspace_id
                    {
                        self.thread_registry
                            .lock()
                            .expect("thread registry poisoned")
                            .placements
                            .insert(placement.thread_id.clone(), placement);
                    }
                }
                self.upsert_thread(r.thread);
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
                        self.schedule_thread_semantic_request(action);
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

fn semantic_cache_projection(
    id: &str,
    workspace: &str,
    state: &SemanticTimelineState,
    previous: Option<&ThreadDomainSnapshot>,
) -> crate::timeline::semantic::SemanticTimelineCachePatch {
    let mut patch = previous.map(|s| s.cache_patch.clone()).unwrap_or_default();
    patch.workspace_id = workspace.to_owned();
    patch.thread_id = id.to_owned();
    let current = state.thread(id);
    let mut removed_blocks: HashSet<_> = patch.removed_block_ids.into_iter().collect();
    let mut removed_items: HashSet<_> = patch
        .removed_work_items
        .into_iter()
        .map(|item| (item.turn_id, item.work_item_id))
        .collect();
    if let Some(old) = previous.and_then(|s| s.semantic.thread(id)) {
        for key in old.top_level.blocks_by_id.keys() {
            if current.is_none_or(|t| !t.top_level.blocks_by_id.contains_key(key)) {
                removed_blocks.insert(key.clone());
            }
        }
        for (turn, range) in &old.work_ranges_by_turn {
            for key in range.items_by_id.keys() {
                if current
                    .and_then(|t| t.work_range(turn))
                    .is_none_or(|r| !r.items_by_id.contains_key(key))
                {
                    removed_items.insert((turn.clone(), key.clone()));
                }
            }
        }
    }
    patch.changed_blocks = current
        .into_iter()
        .flat_map(|t| t.top_level.ordered_blocks())
        .cloned()
        .collect();
    patch.changed_work_items = current
        .into_iter()
        .flat_map(|t| t.work_ranges_by_turn.values())
        .flat_map(|r| r.ordered_items())
        .cloned()
        .collect();
    for block in &patch.changed_blocks {
        removed_blocks.remove(&block.block_id);
    }
    for item in &patch.changed_work_items {
        removed_items.remove(&(item.turn_id.clone(), item.work_item_id.clone()));
    }
    patch.removed_block_ids = removed_blocks.into_iter().collect();
    patch.removed_block_ids.sort();
    let mut removed = removed_items.into_iter().collect::<Vec<_>>();
    removed.sort();
    patch.removed_work_items = removed
        .into_iter()
        .map(
            |(turn_id, work_item_id)| crate::timeline::semantic::SemanticTimelineRemovedWorkItem {
                turn_id,
                work_item_id,
            },
        )
        .collect();
    patch.changed_work_items.sort_by(|a, b| {
        (&a.turn_id, &a.order_key, &a.work_item_id).cmp(&(
            &b.turn_id,
            &b.order_key,
            &b.work_item_id,
        ))
    });
    patch
}

impl ThreadRegistry {
    fn retire_summary(&mut self, id: &str) -> Vec<ClientPublicationDraft> {
        let Some(summary) = self.summaries.remove(id) else {
            return vec![];
        };
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
        vec![
            authority.publication(
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
            ),
            authority.publication(
                ClientScope::Timeline {
                    thread_id: id.to_owned(),
                },
                crate::threads::registry::revisions(revisions.1),
                Arc::new(serde_json::Value::Null),
            ),
        ]
    }
    fn evict_inactive(&mut self) -> Vec<ClientPublicationDraft> {
        let mut candidates = self
            .stores
            .iter()
            .filter(|(id, store)| {
                self.active_thread_id.as_ref() != Some(id)
                    && !self.drafts.values().any(|draft| draft == *id)
                    && self.start.pending_thread_id.as_ref() != Some(id)
                    && !self.ready_resume_set.contains(*id)
                    && store.subscriptions == 0
                    && store.demand == ClientDemand::Suspended
                    && store.in_flight.is_empty()
                    && store.pending.is_empty()
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
        if let Some(store) = registry.stores.get_mut(id) {
            store.subscriptions = count;
        }
        let drafts = registry.evict_inactive();
        self.transition(
            &ClientMutationAuthority { _private: () },
            drafts,
            Vec::new(),
        );
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
        if demand == ClientDemand::Suspended {
            registry.demands.remove(scope);
        } else {
            registry.demands.insert(scope.clone(), demand);
        }
        let combined = registry.demands.iter().filter(|(scope,_)| matches!(scope,ClientScope::Thread {thread_id} | ClientScope::Timeline {thread_id} if thread_id == id)).map(|(_,d)|*d).find(|d|*d != ClientDemand::Suspended).unwrap_or(ClientDemand::Suspended);
        if let Some(store) = registry.stores.get_mut(id) {
            store.demand = combined;
        }
        let drafts = registry.evict_inactive();
        self.transition(
            &ClientMutationAuthority { _private: () },
            drafts,
            Vec::new(),
        );
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
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        if super::session::remember_thread_for_workspace(&mut registry.last_active, workspace, id) {
            registry.session_revision += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conversation::ConversationEvent;
    use pioneer_protocol::*;
    use std::num::NonZeroUsize;
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
            core.execute_thread_semantic_request(&WrongTurnTransport, request);
            let snapshot = core.thread_snapshot("a").unwrap();
            let semantic = snapshot.semantic.thread("a").unwrap();
            assert!(!semantic.work_ranges_by_turn.contains_key("other"));
            assert_eq!(
                semantic.work_ranges_by_turn["expected"].request_status,
                crate::timeline::semantic::TimelineRequestStatus::Failed {
                    message: "Timeline response scope mismatch".into()
                }
            );
            assert!(core.thread_semantic_in_flight("a").is_empty());
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
        assert_eq!(registry.catalog.len(), 83);
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
    action: SemanticTimelineRequestAction,
}
impl ClientCore {
    pub fn begin_thread_semantic_request(
        &self,
        action: SemanticTimelineRequestAction,
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
        let store = registry.stores.get_mut(&id)?;
        let action = crate::timeline::semantic::enqueue_semantic_timeline_request(
            &mut store.in_flight,
            &mut store.pending,
            action,
        )?;
        let generation = store.generation;
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
        let drafts = registry.publish(&id);
        self.transition(&ClientMutationAuthority { _private: () }, drafts, vec![]);
        Some(ThreadSemanticRequest {
            id,
            generation,
            action,
        })
    }
    pub fn schedule_thread_semantic_request(&self, action: SemanticTimelineRequestAction) {
        let sender = self
            .thread_request_sender
            .lock()
            .expect("thread request sender poisoned")
            .clone();
        let Some(sender) = sender else {
            return;
        };
        if let Some(request) = self.begin_thread_semantic_request(action) {
            let _ = sender.send(ThreadControllerRequest::Semantic(request));
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
            Items(pioneer_protocol::TurnWorkItemsGetResponse),
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
                    store.generation == request.generation && store.in_flight.contains(&key)
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
            let result = match request.action {
                SemanticTimelineRequestAction::ThreadTimelinePage { key, params } => {
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
                SemanticTimelineRequestAction::TurnWorkItemsGet { params, .. } => {
                    commands::turn_work_items_get(transport, params).map(Page::Items)
                }
            };
            let mut registry = self
                .thread_registry
                .lock()
                .expect("thread registry poisoned");
            let Some(store) = registry.stores.get_mut(&request.id) else {
                return;
            };
            if self.is_stopped()
                || store.generation != request.generation
                || !store.in_flight.contains(&key)
            {
                return;
            }
            let valid_scope = |workspace: &str, id: &str| {
                workspace == store.coordinator.workspace_id && id == request.id
            };
            match result {
                Ok(Page::Thread(page, mode))
                    if valid_scope(&page.workspace_id, &page.thread_id) =>
                {
                    apply_thread_timeline_page(&mut store.semantic, page, mode);
                }
                Ok(Page::Work(page, mode))
                    if valid_scope(&page.workspace_id, &page.thread_id)
                        && expected_turn.as_deref() == Some(page.turn_id.as_str()) =>
                {
                    apply_turn_work_page(&mut store.semantic, page, mode);
                }
                Ok(Page::Items(page))
                    if valid_scope(&page.workspace_id, &page.thread_id)
                        && expected_turn.as_deref() == Some(page.turn_id.as_str()) =>
                {
                    apply_turn_work_items_get_response(&mut store.semantic, page);
                }
                Ok(_) => set_request_status(
                    &mut store.semantic,
                    &key,
                    TimelineRequestStatus::Failed {
                        message: "Timeline response scope mismatch".into(),
                    },
                ),
                Err(error) => set_request_status(
                    &mut store.semantic,
                    &key,
                    TimelineRequestStatus::Failed {
                        message: format!("{error:#}"),
                    },
                ),
            }
            let next =
                finish_semantic_timeline_request(&mut store.in_flight, &mut store.pending, &key)
                    .and_then(|action| {
                        enqueue_semantic_timeline_request(
                            &mut store.in_flight,
                            &mut store.pending,
                            action,
                        )
                    });
            let drafts = registry.publish(&request.id);
            self.transition(&ClientMutationAuthority { _private: () }, drafts, vec![]);
            drop(registry);
            let Some(action) = next else {
                return;
            };
            request.action = action;
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
        apply: impl FnOnce(&mut SemanticTimelineState, &str, &str, &T) -> anyhow::Result<()>,
    ) -> anyhow::Result<T> {
        let request = self
            .begin_thread_semantic_request(action)
            .ok_or_else(|| anyhow::anyhow!("Timeline request cancelled or already pending"))?;
        let result = fetch();
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        let store = registry
            .stores
            .get_mut(&request.id)
            .ok_or_else(|| anyhow::anyhow!("Timeline request cancelled"))?;
        let key = crate::timeline::semantic::semantic_timeline_request_key(&request.action);
        anyhow::ensure!(
            !self.is_stopped()
                && store.generation == request.generation
                && store.in_flight.contains(key),
            "Timeline request cancelled"
        );
        let result = result.and_then(|page| {
            apply(
                &mut store.semantic,
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
        let next = crate::timeline::semantic::finish_semantic_timeline_request(
            &mut store.in_flight,
            &mut store.pending,
            key,
        );
        let drafts = registry.publish(&request.id);
        self.transition(&ClientMutationAuthority { _private: () }, drafts, vec![]);
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
            |state, workspace, id, page| {
                anyhow::ensure!(
                    page.workspace_id == workspace && page.thread_id == id,
                    "Timeline response scope mismatch"
                );
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
            |state, workspace, id, page| {
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
            |state, workspace, id, page| {
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
            self.execute_thread_resume(&self.compatibility_runtime().ws_command_sender(), &id);
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
                self.transition(&ClientMutationAuthority { _private: () }, drafts, vec![]);
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
        self.transition(&ClientMutationAuthority { _private: () }, drafts, vec![]);
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
        store.coordinator.set_snapshot(reduction.thread);
        let drafts = registry.publish(id);
        self.transition(&ClientMutationAuthority { _private: () }, drafts, vec![]);
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
        let (id, generation) = {
            let mut registry = self
                .thread_registry
                .lock()
                .expect("thread registry poisoned");
            anyhow::ensure!(!self.is_stopped(), "Thread creation cancelled");
            if let Some(id) = registry.drafts.get(workspace).cloned() {
                return Ok(id);
            }
            let plan = super::start::begin_thread_start_attempt(
                &mut registry.start,
                super::start::generate_thread_start_id(),
                workspace.to_owned(),
            )
            .ok_or_else(|| anyhow::anyhow!("Thread creation is already in progress"))?;
            let id = plan.requested_thread_id;
            anyhow::ensure!(
                registry.require(&id, workspace),
                "Thread creation scope mismatch"
            );
            let generation = registry.stores[&id].generation;
            (id, generation)
        };
        let result = crate::transport::ws::command_sender::thread_start(
            transport,
            super::start::thread_create_params(id.clone(), workspace.to_owned(), visibility),
        );
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        anyhow::ensure!(
            !self.is_stopped()
                && registry
                    .stores
                    .get(&id)
                    .is_some_and(|s| s.generation == generation),
            "Thread creation cancelled"
        );
        if registry.start.pending_thread_id.as_deref() == Some(&id) {
            registry.start = Default::default();
        }
        let response = match result {
            Ok(response) => response,
            Err(error) => {
                if registry
                    .stores
                    .get(&id)
                    .is_some_and(|s| s.snapshot.is_none())
                {
                    registry.stores.remove(&id);
                }
                return Err(error);
            }
        };
        let reduction = super::start::reduce_thread_start_bootstrap_success(
            workspace.to_owned(),
            response,
            None,
        );
        anyhow::ensure!(
            reduction.thread.id == id && reduction.thread.workspace_id == workspace,
            "Thread creation response scope mismatch"
        );
        registry
            .stores
            .get_mut(&id)
            .expect("retained draft")
            .coordinator
            .set_snapshot(reduction.thread);
        registry.drafts.insert(workspace.to_owned(), id.clone());
        registry
            .last_active
            .insert(workspace.to_owned(), id.clone());
        registry.active_thread_id = Some(id.clone());
        registry.session_revision += 1;
        let drafts = registry.publish(&id);
        self.transition(&ClientMutationAuthority { _private: () }, drafts, vec![]);
        Ok(id)
    }
}

impl ClientCore {
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
        self.transition(&ClientMutationAuthority { _private: () }, drafts, vec![]);
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
                &self.compatibility_runtime().ws_command_sender(),
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
            self.transition(&ClientMutationAuthority { _private: () }, drafts, vec![]);
        } else {
            let result = self.refresh_thread_subscription_generation(
                &self.compatibility_runtime().ws_command_sender(),
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
            self.transition(&ClientMutationAuthority { _private: () }, drafts, vec![]);
        }
    }
}

/// Identifies an operation's original store incarnation across asynchronous I/O.
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
            store.coordinator.conversation.apply(event.clone());
            crate::timeline::semantic::apply_conversation_event_to_semantic_timeline(
                &mut store.semantic,
                &store.coordinator.workspace_id,
                &event,
                crate::timeline::labels::now_unix_ms(),
            );
        }
        let drafts = registry.publish(&token.id);
        self.transition(&ClientMutationAuthority { _private: () }, drafts, vec![]);
        true
    }
}
