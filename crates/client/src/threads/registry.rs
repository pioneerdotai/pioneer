//! Process-local thread ownership. A transaction can mutate only its named thread.

use super::{coordinator::ThreadCoordinator, start::ThreadStartCoordinator};
use crate::{
    cli_runtime::approvals::{PendingRequest, PendingRequestRegistry, PendingRequestsReduction},
    core::{
        ClientCore, ClientDemand, ClientMutationAuthority, ClientPublicationDraft, ClientRevisions,
        ClientScope, ContentRevision, DomainRevision, PresentationRevision, ScopedRevision,
    },
    timeline::controller::is_newest_refresh,
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

// Request-local evidence: Thread metadata alone can lag live Conversation state.
struct ThreadSubscriptionBaseline {
    thread: Option<Thread>,
    turn_phase_markers: HashMap<String, Arc<crate::conversation::reducer::TurnPhase>>,
}

struct ThreadDomainStore {
    resume_attempt: u64,
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
    subscription_receipt: Option<ThreadSubscriptionReceipt>,
    subscription_request: Option<ThreadSubscriptionRequest>,
    subscription_retry_at: Option<std::time::Instant>,
}

impl Drop for ThreadDomainStore {
    fn drop(&mut self) {
        self.retire_subscription();
    }
}

impl ThreadDomainStore {
    fn retire_subscription(&self) {
        if let Some(receipt) = &self.subscription_receipt {
            receipt.retire();
        }
        if let Some(request) = &self.subscription_request {
            request.retire();
        }
    }
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

    fn subscription_baseline(&self) -> ThreadSubscriptionBaseline {
        ThreadSubscriptionBaseline {
            thread: self.coordinator.thread().cloned(),
            turn_phase_markers: self
                .coordinator
                .conversation
                .projection()
                .turns
                .iter()
                .filter_map(|turn| {
                    self.coordinator
                        .conversation
                        .turn_phase_marker(&turn.id)
                        .map(|marker| (turn.id.clone(), marker))
                })
                .collect(),
        }
    }

    fn reconcile_subscription_snapshot(
        &mut self,
        before: Option<&ThreadSubscriptionBaseline>,
        mut thread: Thread,
    ) {
        let current = self.coordinator.thread();
        let older = current.is_some_and(|current| thread.updated_at < current.updated_at);
        if let Some(current) = current {
            // updated_at has second precision. Compare request-local values as well,
            // so a local send/live update in that same second wins field conflicts.
            // ThreadStarted has no request identity: keep existing metadata when
            // its relation to local changes is unknown. The RPC can resolve it.
            let metadata = before.and_then(|before| before.thread.as_ref());
            macro_rules! preserve_changes {
                ($($($field:ident),+);+ $(;)?) => {$({
                    if older || metadata.is_none_or(|old| $(old.$field != current.$field)||+) {
                        $(thread.$field = current.$field.clone();)+
                    }
                })+};
            }
            preserve_changes! {
                name;
                preview, preview_author;
                mode, model, model_provider, reasoning_effort;
                status;
                origin_kind;
                sidebar_visibility;
                agent_nickname;
                agent_role;
                visibility;
            }
            thread.updated_at = thread.updated_at.max(current.updated_at);
        }
        let accepts_turn =
            |turn: &pioneer_protocol::Turn, baseline: Option<&ThreadSubscriptionBaseline>| {
                use crate::conversation::reducer::TurnPhase;
                use pioneer_protocol::TurnStatus;
                let Some(view) = self
                    .coordinator
                    .conversation
                    .projection()
                    .turns
                    .iter()
                    .find(|view| view.id == turn.id)
                else {
                    return true;
                };
                let unchanged = !older
                    && baseline.is_some_and(|before| {
                        before.turn_phase_markers.get(&turn.id).is_some_and(|old| {
                            self.coordinator
                                .conversation
                                .turn_phase_marker(&turn.id)
                                .is_some_and(|current| Arc::ptr_eq(old, &current))
                        })
                    });
                match view.phase {
                    // Completion animation still owns this turn until tick().
                    TurnPhase::Completing => false,
                    TurnPhase::Completed => turn.status == TurnStatus::Completed,
                    TurnPhase::Failed => turn.status == TurnStatus::Failed,
                    TurnPhase::Cancelled => turn.status == TurnStatus::Interrupted,
                    // Blocked is resumable. Only an RPC begun in this same state can
                    // confirm resumption; an unversioned echo cannot undo a live block.
                    TurnPhase::Blocked => turn.status == TurnStatus::Blocked || unchanged,
                    TurnPhase::Starting | TurnPhase::Running => match turn.status {
                        TurnStatus::InProgress => true,
                        TurnStatus::Blocked => unchanged,
                        // An RPC can supply a missing terminal outcome. An echo
                        // without causal evidence must leave live flow intact.
                        _ => baseline.is_some(),
                    },
                }
            };
        // Stored Thread turns can themselves lag live events. Do not feed stale
        // retained evidence back into Conversation while merging a response.
        let mut turns = current
            .into_iter()
            .flat_map(|thread| &thread.turns)
            .filter(|turn| accepts_turn(turn, None))
            .cloned()
            .collect::<Vec<_>>();
        for incoming in std::mem::take(&mut thread.turns) {
            if !accepts_turn(&incoming, before) {
                continue;
            }
            if let Some(turn) = turns.iter_mut().find(|turn| turn.id == incoming.id) {
                *turn = incoming;
            } else {
                turns.push(incoming);
            }
        }
        thread.turns = turns;
        if self.coordinator.thread() == Some(&thread) {
            // A live block may have left Thread unchanged. Its fresh subscription
            // still needs to restore Conversation even when metadata is equal.
            self.coordinator.conversation.sync_thread_snapshot(&thread);
        } else {
            self.set_snapshot(thread);
        }
    }

    fn new(id: &str, workspace: &str) -> Self {
        Self {
            resume_attempt: 0,
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
            subscription_receipt: None,
            subscription_request: None,
            subscription_retry_at: None,
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
    open_intent: Option<(String, u64)>,
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
    pub(crate) fn next_timeline_request_deadline(&self) -> Option<u64> {
        [
            self.timeline.next_read_deadline(),
            self.timeline.next_refresh_deadline_where(|id| {
                self.stores.get(id).is_some_and(|store| {
                    !store.presentation_blocked && store.demand != ClientDemand::Suspended
                })
            }),
        ]
        .into_iter()
        .flatten()
        .min()
    }

    pub(crate) fn due_timeline_refreshes(
        &mut self,
        now_ms: u64,
    ) -> Vec<crate::timeline::controller::TimelinePageRequest> {
        self.timeline.due_refreshes(now_ms, |id| {
            self.stores.get(id).is_some_and(|store| {
                !store.presentation_blocked && store.demand != ClientDemand::Suspended
            })
        })
    }

    pub(crate) fn timeline_has_unread(&self, thread: &str) -> bool {
        self.directory.has_unread(thread)
    }
    pub(crate) fn timeline_read_is_confirmed(
        &self,
        thread: &str,
        turn: &str,
        position: Option<&pioneer_protocol::ThreadReadPosition>,
    ) -> bool {
        let sort_key = position
            .map(|position| position.sort_key.as_str())
            .or_else(|| {
                // A legacy read acknowledges only the prompt, never a later answer
                // in the same Turn. If it is not loaded, keep the read eligible.
                self.stores
                    .get(thread)?
                    .semantic
                    .thread(thread)?
                    .top_level
                    .ordered_blocks()
                    .find(|block| {
                        block.turn_id.as_deref() == Some(turn)
                            && matches!(
                                &block.kind,
                                pioneer_protocol::TimelineBlockKind::UserMessage { .. }
                            )
                    })
                    .map(|block| block.sort_key.as_str())
            });
        sort_key.is_some_and(|key| self.directory.read_position_is_confirmed(thread, key))
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
    pub(crate) fn timeline_store_generation(&self, thread: &str) -> u64 {
        self.stores.get(thread).map_or(0, |store| store.generation)
    }

    pub(crate) fn timeline_workspace_matches(&self, thread: &str, workspace: &str) -> bool {
        self.stores
            .get(thread)
            .is_some_and(|store| store.coordinator.workspace_id == workspace)
    }

    pub(crate) fn timeline_operation_matches(&self, token: &ThreadOperationToken) -> bool {
        self.stores
            .get(&token.id)
            .is_some_and(|store| store.generation == token.generation)
    }
}

impl ThreadRegistry {
    pub(crate) fn has_thread_store(&self, id: &str) -> bool {
        self.stores.contains_key(id)
    }

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

    /// Scoped compact metadata is part of the caller's atomic workspace batch.
    /// Never require/create a detailed store for a sidebar-only identity.
    pub(crate) fn apply_push_summaries(
        &mut self,
        threads: &[pioneer_protocol::ThreadMetadataPatch],
    ) -> Vec<ClientPublicationDraft> {
        let mut drafts = Vec::new();
        for patch in threads {
            let summary = &patch.thread;
            if self
                .stores
                .get(&summary.id)
                .and_then(|store| store.coordinator.thread())
                .is_some_and(|previous| previous.updated_at > summary.updated_at)
            {
                continue;
            }
            if self
                .directory
                .threads
                .get(&summary.id)
                .is_some_and(|previous| previous.updated_at > summary.updated_at)
            {
                continue;
            }
            self.directory.upsert_thread_summary(summary.clone());
            let Some(store) = self.stores.get_mut(&summary.id) else {
                continue;
            };
            if store.coordinator.workspace_id != summary.workspace_id {
                continue;
            }
            let mut thread = summary.clone();
            let Some(previous) = store.coordinator.thread() else {
                continue; // A compact summary cannot complete Thread/get bootstrap.
            };
            if patch.preview_truncated
                && previous.preview.len() > thread.preview.len()
                && previous.preview.starts_with(&thread.preview)
            {
                thread.preview = previous.preview.clone();
            }
            store.set_snapshot(thread);
            drafts.extend(self.publish(&summary.id));
        }
        drafts
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
            self.directory.upsert_thread_summary(thread);
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
        self.core.wake_thread_timeline_requests();
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

    /// Directory refresh must never create coordinators. Inspect the owner,
    /// including pending stores with no published snapshot, under the same lock
    /// used for the update (an eviction between selection and update is valid).
    pub(crate) fn upsert_existing_directory_thread(&self, mut thread: Thread) {
        if self.is_stopped() {
            return;
        }
        let registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        if registry.directory.threads.get(&thread.id) != Some(&thread) {
            return; // A live metadata update won the race after selection.
        }
        let Some(store) = registry.stores.get(&thread.id) else {
            return;
        };
        if store.coordinator.workspace_id != thread.workspace_id {
            return;
        }
        if let Some(previous) = store.coordinator.thread() {
            if previous.updated_at > thread.updated_at {
                return;
            }
            // A compact directory prefix must not replace the detailed preview
            // already loaded by Thread/get or live metadata.
            if previous.preview.len() > thread.preview.len() {
                let mut detailed = thread.clone();
                detailed.preview = previous.preview.clone();
                if detailed.into_directory_summary().preview == thread.preview {
                    thread.preview = previous.preview.clone();
                }
            }
        }
        let id = thread.id.clone();
        let mut mutation = ThreadMutation {
            core: self,
            registry,
            id,
        };
        if mutation.thread() != Some(&thread) {
            mutation.set_snapshot(thread);
        }
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
    pub fn enqueue_thread_resume(&self, id: String) -> anyhow::Result<()> {
        anyhow::ensure!(id.len() <= 32 * 1024, "Resume identifier capacity exceeded");
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        anyhow::ensure!(registry.stores.contains_key(&id), "Resume source retired");
        if registry.ready_resume.len() >= super::history_scheduler::MAX_JOBS
            && !registry.ready_resume_set.contains(&id)
        {
            // Durable overflow stays in its existing store coordinator, rather
            // than another queue of IDs. Do not reset an existing retry budget.
            registry
                .stores
                .get_mut(&id)
                .unwrap()
                .coordinator
                .resume
                .next_attempt_at
                .get_or_insert(std::time::Instant::now());
        } else if registry.ready_resume_set.insert(id.clone()) {
            registry.ready_resume.push_back(id);
        }
        Ok(())
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
        if let Some(store) = registry.stores.get(id) {
            store.retire_subscription();
        }
        self.notify_send_readiness();
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
        registry.directory.remove_thread(id);
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
        drop(registry);
        self.retire_thread_controller_jobs();
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
        for store in registry.stores.values() {
            store.retire_subscription();
        }
        self.notify_send_readiness();
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
        drop(registry);
        self.retire_thread_controller_jobs();
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
        drop(registry);
        self.retire_thread_controller_jobs();
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
        self.reduce_thread_semantic_live_update(update)
            .map_or(false, |r| r.0)
    }

    fn reduce_thread_semantic_live_update(
        &self,
        update: crate::timeline::semantic::SemanticTimelineLiveUpdate,
    ) -> Option<(bool, u64, Vec<SemanticTimelineRequestAction>)> {
        use crate::timeline::semantic::SemanticTimelineLiveUpdate::*;
        let (id, workspace) = match &update {
            ThreadTimelineBlocksChanged(n) => (&n.thread_id, &n.workspace_id),
            TurnWorkItemsChanged(n) => (&n.thread_id, &n.workspace_id),
            TurnWorkStateChanged(n) => (&n.thread_id, &n.workspace_id),
        };
        let mut state = self.thread_timeline_mutation(id, workspace)?;
        let generation = state.registry.stores[id].generation;
        // Record accepted-scope removals even when removing an absent value
        // leaves semantic state unchanged. Preserve the reducer's turn guards.
        match &update {
            ThreadTimelineBlocksChanged(n) => {
                state
                    .registry
                    .timeline
                    .record_removals(id, None, &n.removed_block_ids)
            }
            TurnWorkItemsChanged(n)
                if crate::timeline::semantic::accepts_turn_work_items_change(
                    &state, id, &n.turn_id,
                ) =>
            {
                state.registry.timeline.record_removals(
                    id,
                    Some(&n.turn_id),
                    &n.removed_work_item_ids,
                );
            }
            _ => {}
        }
        let changed = crate::timeline::semantic::apply_semantic_timeline_live_update(
            &mut state,
            update.clone(),
        );
        let retired = state.registry.retire_unauthorized_targeted_requests(id);
        // Read intent survives a duplicate invalidation, but only for IDs accepted
        // into the stale sets by the semantic reducer (including tombstone fences).
        let requests = thread_reconcile_actions(&state, &update);
        let store = &state.registry.stores[id];
        // Any accepted change for this timeline can signal recovery, including
        // duplicates and removal-only updates that produce no ordinary request.
        let rearmed = !store.presentation_blocked
            && store.demand == ClientDemand::Visible
            && state.registry.timeline.rearm_exhausted_refresh(id);
        drop(state);
        if retired {
            self.retire_thread_controller_jobs();
        }
        if rearmed {
            // Preserve the wake even when admission below returns no request.
            self.wake_thread_timeline_requests();
        }
        Some((changed, generation, requests))
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
            store.retire_subscription();
            store.subscription_receipt = None;
            store.subscription_request = None;
            store.subscription_retry_at = None;
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
        drop(registry);
        self.retire_thread_controller_jobs();
        self.notify_send_readiness();
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
        drop(registry);
        self.retire_thread_controller_jobs();
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
        drop(registry);
        self.retire_thread_controller_jobs();
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
                if let Some(mut mutation) = self.thread_mutation(&r.thread_id, &r.workspace_id) {
                    mutation
                        .registry
                        .stores
                        .get_mut(&r.thread_id)
                        .expect("retained thread")
                        .reconcile_subscription_snapshot(None, r.thread);
                }
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
                self.resume_created_thread_delivery(&r.thread_id, &r.workspace_id);
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
                            mutation.registry.directory.upsert_placement(placement);
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
                    let _ = self.schedule_thread_semantic_request_inner(
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
                        None,
                        true,
                        false,
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
                if let Some((_, generation, requests)) =
                    self.reduce_thread_semantic_live_update(update)
                {
                    for action in requests {
                        let _ = self.schedule_thread_semantic_request_inner(
                            action,
                            Some(generation),
                            true,
                            false,
                        );
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
            if let Some(store) = self.stores.remove(&id) {
                store.retire_subscription();
            }
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
        let restored_workspace = registry
            .stores
            .get(id)
            .filter(|store| {
                store.demand != ClientDemand::Visible && demand == ClientDemand::Visible
            })
            .map(|store| store.coordinator.workspace_id.clone());
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
            if let Some(workspace) = restored_workspace {
                self.schedule_thread_subscription(id, &workspace);
                self.recover_visible_thread_timeline(id);
            }
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
        let became_visible = registry.stores.get(id).is_some_and(|store| {
            store.demand != ClientDemand::Visible && combined == ClientDemand::Visible
        });
        let resumed_prefetch = registry.stores.get(id).is_some_and(|store| {
            store.demand == ClientDemand::Suspended
                && combined == ClientDemand::Prefetch
                && !store.presentation_blocked
        }) && registry
            .timeline
            .next_refresh_deadline_where(|thread| thread == id)
            .is_some();
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
        let workspace = registry
            .stores
            .get(id)
            .map(|store| store.coordinator.workspace_id.clone());
        drop(registry);
        if combined != ClientDemand::Suspended {
            self.request_thread_presentation(id);
        }
        if became_visible && let Some(workspace) = workspace {
            self.schedule_thread_subscription(id, &workspace);
            self.recover_visible_thread_timeline(id);
        }
        if resumed_prefetch {
            // Prefetch resumes an existing budget; only Visible may rearm it.
            self.wake_thread_timeline_requests();
        }
    }

    fn recover_visible_thread_timeline(&self, id: &str) {
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        if self.is_stopped()
            || !registry.stores.get(id).is_some_and(|store| {
                !store.presentation_blocked && store.demand == ClientDemand::Visible
            })
        {
            return;
        }
        registry.timeline.rearm_exhausted_refresh(id);
        // Also wake an unexhausted Backoff that was paused while Suspended.
        let wake = registry
            .timeline
            .next_refresh_deadline_where(|thread| thread == id)
            .is_some();
        drop(registry);
        if wake {
            self.wake_thread_timeline_requests();
        }
    }

    pub(crate) fn wake_thread_timeline_requests(&self) {
        if let Some(sender) = self
            .thread_request_sender
            .lock()
            .expect("thread request sender poisoned")
            .as_ref()
        {
            let _ = sender.send(ThreadControllerRequest::Resume);
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
    // Deterministic owner handoff cuts, absent from production.
    thread_local! {
        pub(super) static RESUME_CAPTURE_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = Default::default();
        pub(super) static SUBSCRIPTION_DUE_CAPTURE_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = Default::default();
        pub(super) static SUBSCRIPTION_PROMOTION_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = Default::default();
    }

    // Existing result-focused suites ask for a complete logical operation.
    // Advance only its returned continuation, with a fixed test bound; scheduler
    // ordering and one-quantum assertions use execute directly below.
    fn complete_semantic_operation(
        core: &ClientCore,
        transport: &impl crate::rpc::JsonRpcRequestTransport,
        request: ThreadSemanticRequest,
    ) {
        let mut next = Some(request);
        for _ in 0..(CACHED_TIMELINE_REQUEST_BUDGET * 4) {
            let Some(request) = next else {
                return;
            };
            next = core.execute_thread_semantic_request(transport, request);
        }
        panic!("logical semantic operation exceeded its continuation budget");
    }

    use super::*;
    use crate::conversation::ConversationEvent;
    use pioneer_protocol::*;
    use std::num::NonZeroUsize;
    use std::time::{Duration, Instant};
    fn composer_admission() -> Turn {
        let mut turn = running_turn("admission");
        turn.work_owner = TurnWorkOwner::DetachedTask;
        turn.author = Some(TurnAuthorSnapshot {
            cli_runtime_kind: None,
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
            cli_runtime_kind: None,
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
            let TimelineBlockKind::DetachedTaskRun { task, author, .. } = &tasks[0].kind else {
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
    fn directory_refresh_preserves_detailed_preview_but_accepts_shorter_and_empty_edits() {
        let core = ClientCore::new();
        let mut full: Thread = serde_json::from_value(serde_json::json!({
            "id":"preview", "workspace_id":"w", "name":"Named", "preview":"", "mode":"Chat", "model":"model", "model_provider":"provider", "created_at":1, "updated_at":1, "status":"Idle", "turns":[]
        })).unwrap();
        full.preview = "First 😀 message ".repeat(200);
        core.upsert_thread(full.clone());
        core.activate_thread(Some("preview"), Some("w"));
        let compact = full.clone().into_directory_summary();
        core.upsert_existing_directory_thread(compact.clone());
        assert_eq!(
            core.thread_snapshot("preview")
                .unwrap()
                .coordinator()
                .thread()
                .unwrap()
                .preview,
            full.preview
        );
        assert_eq!(
            core.workspace_tree("w").unwrap().snapshot().threads_by_id["preview"],
            compact
        );
        for text in ["First", ""] {
            let mut edited = full.clone();
            edited.preview = text.into();
            edited.updated_at += 1;
            core.thread_registry
                .lock()
                .unwrap()
                .directory
                .threads
                .insert("preview".into(), edited.clone());
            core.upsert_existing_directory_thread(edited);
            assert_eq!(
                core.thread_snapshot("preview")
                    .unwrap()
                    .coordinator()
                    .thread()
                    .unwrap()
                    .preview,
                text
            );
            assert_eq!(
                core.workspace_tree("w").unwrap().snapshot().threads_by_id["preview"].preview,
                text
            );
        }
    }

    #[test]
    fn directory_refresh_updates_pending_and_retained_stores_without_creating_others() {
        let core = ClientCore::new();
        let make_thread = |id: &str| {
            serde_json::from_value::<Thread>(serde_json::json!({
            "id": id, "workspace_id": "w", "preview": "", "mode": "Chat", "model": "new", "model_provider": "p", "created_at": 1, "updated_at": 2, "status": "Idle", "turns": []
        })).unwrap()
        };
        {
            let mut registry = core.thread_registry.lock().unwrap();
            registry
                .stores
                .insert("pending".into(), ThreadDomainStore::new("pending", "w"));
            for id in ["pending", "unopened"] {
                registry
                    .directory
                    .threads
                    .insert(id.into(), make_thread(id));
            }
            assert!(registry.stores["pending"].snapshot.is_none());
        }
        for id in ["pending", "unopened"] {
            core.upsert_existing_directory_thread(make_thread(id));
        }
        let registry = core.thread_registry.lock().unwrap();
        assert!(registry.stores["pending"].snapshot.is_some());
        assert_eq!(
            registry.stores["pending"]
                .coordinator
                .thread()
                .unwrap()
                .model,
            "new"
        );
        assert_eq!(registry.stores.len(), 1);
        assert_eq!(registry.directory.threads.len(), 2);
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
                turn.work_owner = TurnWorkOwner::Turn;
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
                codex_retry_notices: Vec::new(),
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
    fn visible_demand_and_scope_reacquisition_wake_the_saved_subscription_retry() {
        use std::time::{Duration, Instant};
        struct Requests(std::sync::mpsc::Sender<(String, crate::rpc::JsonRpcResponseSender)>);
        impl crate::rpc::JsonRpcRequestTransport for Requests {
            fn send_json_rpc_request(
                &self,
                _: String,
                payload: String,
                response: crate::rpc::JsonRpcResponseSender,
            ) -> Result<(), String> {
                let request: pioneer_protocol::JsonRpcRequest =
                    serde_json::from_str(&payload).unwrap();
                self.0.send((request.method, response)).unwrap();
                Ok(())
            }
        }
        for reacquire_scope in [false, true] {
            let core = crate::catalog_test_support::settings_client();
            core.upsert_thread(thread("parent", "workspace"));
            connect_subscription_test(&core);
            {
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
            let scope = ClientScope::Timeline {
                thread_id: "parent".into(),
            };
            let (sender, receiver) = crate::threads::history_scheduler::channel();

            *core.thread_request_sender.lock().unwrap() = Some(sender);
            let initial_scope = if reacquire_scope {
                Some(core.subscribe(scope.clone(), std::num::NonZeroUsize::new(8).unwrap()))
            } else {
                core.thread_demand_changed(&scope, ClientDemand::Visible);
                None
            };
            let ThreadControllerRequest::Subscribe(first) =
                receiver.recv_timeout(Duration::from_secs(5)).unwrap().value
            else {
                panic!()
            };
            core.finish_thread_subscription_request(
                &first,
                Err(anyhow::anyhow!("temporary failure")),
            );
            drop(initial_scope);
            if !reacquire_scope {
                core.thread_demand_changed(&scope, ClientDemand::Suspended);
            }
            let saved = {
                let mut registry = core.thread_registry.lock().unwrap();
                let store = registry.stores.get_mut("parent").unwrap();
                assert_eq!(store.demand, ClientDemand::Suspended);
                // Simulate time spent hidden without sleeping or spending a
                // retry. The real worker must ignore this deadline until Visible.
                store.subscription_retry_at = Some(Instant::now());
                let saved = store.subscription_request.clone().unwrap();
                assert_eq!(saved.attempt, 1);
                saved
            };
            let (requests_tx, requests_rx) = std::sync::mpsc::channel();
            *core
                .transport_runtime()
                .ws_command_sender()
                .test_requests
                .lock()
                .unwrap() = Some(Arc::new(Requests(requests_tx)));
            let (waiting_tx, waiting_rx) = std::sync::mpsc::channel();
            core.start_thread_request_controller(Some(waiting_tx));
            // This is the production worker's computed recv() boundary. It has
            // no deadline and no queued request; no unrelated wake is sent below.
            assert_eq!(
                waiting_rx.recv_timeout(Duration::from_secs(5)).unwrap(),
                None
            );
            assert!(requests_rx.try_recv().is_err());
            let restored_scope = if reacquire_scope {
                Some(core.subscribe(scope.clone(), std::num::NonZeroUsize::new(8).unwrap()))
            } else {
                core.thread_demand_changed(&scope, ClientDemand::Visible);
                None
            };
            let (method, response) = requests_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("Visible must wake the saved retry");
            assert_eq!(method, pioneer_protocol::constants::methods::THREAD_START);
            {
                let registry = core.thread_registry.lock().unwrap();
                let store = &registry.stores["parent"];
                assert_eq!(
                    store.subscription_request.as_ref(),
                    Some(&saved),
                    "same attempt, budget and fences"
                );
                assert!(
                    store.subscription_retry_at.is_none(),
                    "worker consumed the due retry"
                );
            }
            assert!(requests_rx.try_recv().is_err(), "no duplicate RPC");
            response
                .send(Err(crate::rpc::JsonRpcResponseError::transport(
                    "temporary failure",
                )))
                .unwrap();
            loop {
                let _next_boundary = waiting_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                if core.thread_registry.lock().unwrap().stores["parent"]
                    .subscription_request
                    .as_ref()
                    .unwrap()
                    .attempt
                    == 2
                {
                    break;
                }
            }
            assert_eq!(
                core.thread_registry.lock().unwrap().stores["parent"]
                    .subscription_request
                    .as_ref()
                    .unwrap()
                    .attempt,
                2
            );
            core.shutdown();
            drop(restored_scope);
        }
    }

    #[test]
    fn restored_session_resumes_only_visible_thread_subscription_and_timeline() {
        let core = crate::catalog_test_support::settings_client();
        core.upsert_thread(thread("visible", "workspace"));
        core.upsert_thread(thread("parent", "workspace"));
        let connected = || {
            connect_subscription_test(&core);
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
        };
        let (sender, receiver) = crate::threads::history_scheduler::channel();

        *core.thread_request_sender.lock().unwrap() = Some(sender);
        connected();
        core.thread_demand_changed(
            &ClientScope::Timeline {
                thread_id: "visible".into(),
            },
            ClientDemand::Visible,
        );
        let ThreadControllerRequest::Subscribe(first) = receiver.try_recv().unwrap().value else {
            panic!()
        };
        core.resume_visible_thread_delivery();
        for item in receiver.try_iter() {
            assert!(
                matches!(item.value, ThreadControllerRequest::Resume),
                "resuming a pending flight must not queue another subscription"
            );
        }
        let mut attempt = first.clone();
        for index in 0..=2 {
            core.finish_thread_subscription_request(
                &attempt,
                Err(anyhow::anyhow!("temporary failure")),
            );
            let registry = core.thread_registry.lock().unwrap();
            let store = &registry.stores["visible"];
            assert_eq!(store.subscription_receipt, None, "no acknowledgement yet");
            assert_eq!(store.subscription_retry_at.is_some(), index < 2);
            if index < 2 {
                attempt = store.subscription_request.clone().unwrap();
            }
        }
        core.cancel_thread_requests();
        {
            let mut session = core.gateway_session.lock().unwrap();
            session.observe_transport(&crate::transport::ws::GatewayWsEvent::Reconnecting {
                connection_id: 7,
                endpoint_id: "endpoint".into(),
                endpoint_name: "Synthetic".into(),
                attempt: 1,
                delay_ms: 100,
                reason: "network lost".into(),
            });
            core.publish_gateway_session(&session);
        }
        connected(); // the automatic reconnect retains local ID 7
        core.resume_visible_thread_delivery();
        let ThreadControllerRequest::Subscribe(current) = receiver.try_recv().unwrap().value else {
            panic!()
        };
        assert_ne!(current.transport_revision, first.transport_revision);
        core.finish_thread_subscription_request(&first, Ok(()));
        assert_eq!(
            core.thread_registry.lock().unwrap().stores["visible"].subscription_receipt,
            None
        );
        struct Ack;
        impl crate::rpc::JsonRpcRequestTransport for Ack {
            fn send_json_rpc_request(
                &self,
                _: String,
                _: String,
                tx: crate::rpc::JsonRpcResponseSender,
            ) -> Result<(), String> {
                tx.send(Ok(serde_json::to_value(ThreadStartResponse {
                    thread: thread("visible", "workspace"),
                    sandbox: SandboxPolicy::from_mode(SandboxMode::FullAccess),
                })
                .unwrap()))
                    .unwrap();
                Ok(())
            }
        }
        core.execute_thread_subscription_request_using(&current, &Ack);
        assert_eq!(
            core.thread_registry.lock().unwrap().stores["visible"]
                .subscription_receipt
                .as_ref()
                .map(|receipt| receipt.scope.transport_revision),
            Some(current.transport_revision)
        );
        let _timeline = receiver.try_iter().collect::<Vec<_>>();
        core.thread_demand_changed(
            &ClientScope::Timeline {
                thread_id: "parent".into(),
            },
            ClientDemand::Visible,
        );
        assert!(
            matches!(receiver.try_recv().map(|item| item.value), Ok(ThreadControllerRequest::Subscribe(request)) if request.id == "parent")
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
        crate::threads::history_scheduler::ThreadControllerReceiver,
    ) {
        reconciliation_core_with(core())
    }

    fn reconciliation_core_with(
        core: Arc<ClientCore>,
    ) -> (
        Arc<ClientCore>,
        crate::threads::history_scheduler::ThreadControllerReceiver,
    ) {
        core.upsert_thread(thread("a", "ws"));
        core.apply_thread_timeline_page(
            reconciliation_page(reconciliation_history()[80..=210].to_vec(), true, true),
            crate::timeline::semantic::TopLevelPageMergeMode::Reset,
        );
        let (sender, receiver) = crate::threads::history_scheduler::channel();

        *core.thread_request_sender.lock().unwrap() = Some(sender);
        (core, receiver)
    }

    fn reconciliation_request(
        receiver: &crate::threads::history_scheduler::ThreadControllerReceiver,
    ) -> ThreadSemanticRequest {
        loop {
            match receiver.try_recv().unwrap().value {
                ThreadControllerRequest::Semantic(request) => return request,
                ThreadControllerRequest::Resume => continue,
                _ => panic!("expected timeline request"),
            }
        }
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

    // This fixture advances exactly one scheduler choice and one RPC. It
    // deliberately does not drain a returned continuation.
    fn run_scheduled_semantic_quantum(
        core: &ClientCore,
        receiver: &crate::threads::history_scheduler::ThreadControllerReceiver,
        transport: &impl crate::rpc::JsonRpcRequestTransport,
    ) -> String {
        use pioneer_observability::work::{Boundary, ExecutorRole, WorkItem, WorkKind};
        let item = {
            let registry = core.thread_registry.lock().unwrap();
            receiver
                .take_by(|request| registry.controller_request_interactive(request))
                .unwrap()
        };
        let queue = item.queue.as_ref().map(|(queue, _)| queue.clone());
        let (request, work) = item.received(
            Boundary::StdThread,
            WorkKind::ClientCommand,
            ExecutorRole::ClientController,
        );
        let ThreadControllerRequest::Semantic(request) = request else {
            panic!("expected semantic job")
        };
        let id = request.id.clone();
        let next = work.run(|| core.execute_thread_semantic_request(transport, request));
        receiver.finish(next.map(|request| {
            let item =
                WorkItem::from_carrier(ThreadControllerRequest::Semantic(request), work, None);
            if let Some(queue) = queue {
                item.observed(
                    queue,
                    pioneer_observability::queues::QueueBytes::Unsupported,
                )
            } else {
                item
            }
        }));
        id
    }

    struct HeldHistory(
        std::sync::mpsc::Sender<(serde_json::Value, crate::rpc::JsonRpcResponseSender)>,
    );
    impl crate::rpc::JsonRpcRequestTransport for HeldHistory {
        fn send_json_rpc_request(
            &self,
            _: String,
            payload: String,
            response: crate::rpc::JsonRpcResponseSender,
        ) -> Result<(), String> {
            self.0
                .send((serde_json::from_str(&payload).unwrap(), response))
                .unwrap();
            Ok(())
        }
    }

    fn history_owner_fixture() -> (
        Arc<ClientCore>,
        std::sync::mpsc::Receiver<(serde_json::Value, crate::rpc::JsonRpcResponseSender)>,
        std::sync::mpsc::Receiver<Option<std::time::Duration>>,
    ) {
        let core = crate::catalog_test_support::settings_client();
        connect_subscription_test(&core);
        for id in ["a", "b", "c"] {
            core.upsert_thread(thread(id, "ws"));
        }
        let (tx, rx) = std::sync::mpsc::channel();
        *core
            .transport_runtime()
            .ws_command_sender()
            .test_requests
            .lock()
            .unwrap() = Some(Arc::new(HeldHistory(tx)));
        let (waiting_tx, waiting_rx) = std::sync::mpsc::channel();
        core.start_thread_request_controller(Some(waiting_tx));
        (core, rx, waiting_rx)
    }
    fn queue_history(core: &ClientCore, id: &str) {
        core.schedule_thread_semantic_request(SemanticTimelineRequestAction::ThreadTimelinePage {
            key: SemanticTimelineRequestKey::ThreadNewest {
                thread_id: id.into(),
            },
            params: pioneer_protocol::ThreadTimelinePageParams {
                thread_id: id.into(),
                anchor: pioneer_protocol::TimelinePageAnchor::Newest,
                limit: Some(100),
            },
        })
        .unwrap();
    }
    fn empty_history(id: &str) -> serde_json::Value {
        let mut page = reconciliation_page(vec![], false, false);
        page.thread_id = id.into();
        serde_json::to_value(page).unwrap()
    }

    fn wait_history_owner_exit(waiting: &std::sync::mpsc::Receiver<Option<std::time::Duration>>) {
        loop {
            match waiting.recv_timeout(std::time::Duration::from_secs(5)) {
                Ok(_) => {}
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                Err(error) => panic!("history owner did not exit: {error}"),
            }
        }
    }

    #[test]
    fn history_owner_serializes_same_thread_quanta_but_advances_other_thread() {
        let (core, requests, waiting) = history_owner_fixture();
        let watchdog = std::time::Duration::from_secs(5);
        queue_history(&core, "a");
        let (_, a) = requests.recv_timeout(watchdog).unwrap();
        core.schedule_thread_semantic_request(SemanticTimelineRequestAction::ThreadTimelinePage {
            key: SemanticTimelineRequestKey::ThreadBefore {
                thread_id: "a".into(),
                cursor: "older".into(),
            },
            params: pioneer_protocol::ThreadTimelinePageParams {
                thread_id: "a".into(),
                anchor: pioneer_protocol::TimelinePageAnchor::Before {
                    cursor: pioneer_protocol::TimelineCursor {
                        value: "older".into(),
                    },
                },
                limit: Some(100),
            },
        })
        .unwrap();
        queue_history(&core, "b");
        let (request, b) = requests.recv_timeout(watchdog).unwrap();
        assert_eq!(
            request["params"]["threadId"], "b",
            "same-thread quantum must wait for A"
        );
        a.send(Ok(empty_history("a"))).unwrap();
        let (request, next_a) = requests.recv_timeout(watchdog).unwrap();
        assert_eq!(request["params"]["threadId"], "a");
        assert!(b.is_active());
        core.shutdown();
        wait_history_owner_exit(&waiting);
        assert!(!next_a.is_active());
    }

    #[test]
    fn canonical_history_scheduler_enforces_two_active_semantic_reservations() {
        let core = crate::catalog_test_support::settings_client();
        let (sender, receiver) = crate::threads::history_scheduler::channel();
        let mut third = None;
        for id in ["a", "b", "c"] {
            core.upsert_thread(thread(id, "ws"));
            let request = core
                .begin_thread_semantic_request(SemanticTimelineRequestAction::ThreadTimelinePage {
                    key: SemanticTimelineRequestKey::ThreadNewest {
                        thread_id: id.into(),
                    },
                    params: pioneer_protocol::ThreadTimelinePageParams {
                        thread_id: id.into(),
                        anchor: pioneer_protocol::TimelinePageAnchor::Newest,
                        limit: Some(100),
                    },
                })
                .unwrap()
                .unwrap();
            if id == "c" {
                third = Some(request);
            } else {
                sender
                    .send(ThreadControllerRequest::Semantic(request))
                    .unwrap();
            }
        }
        let receive = |item: pioneer_observability::work::WorkItem<ThreadControllerRequest>| {
            use pioneer_observability::work::{Boundary, ExecutorRole, WorkKind};
            let queue = item.queue.as_ref().map(|(q, _)| q.clone());
            let (request, _) = item.received(
                Boundary::StdThread,
                WorkKind::ClientCommand,
                ExecutorRole::ClientController,
            );
            if let Some(queue) = queue {
                queue.processing().processed();
            }
            request
        };
        let (a, item) = receiver.take_ready_by(|_| false).unwrap();
        assert!(matches!(receive(item), ThreadControllerRequest::Semantic(r) if r.id == "a"));
        let (b, item) = receiver.take_ready_by(|_| false).unwrap();
        assert!(matches!(receive(item), ThreadControllerRequest::Semantic(r) if r.id == "b"));
        sender
            .send(ThreadControllerRequest::Semantic(third.unwrap()))
            .unwrap();
        assert!(receiver.take_ready_by(|_| false).is_none());
        receiver.finish_active(b, None);
        let (c, item) = receiver.take_ready_by(|_| false).unwrap();
        assert!(matches!(receive(item), ThreadControllerRequest::Semantic(r) if r.id == "c"));
        receiver.finish_active(c, None);
        receiver.finish_active(a, None);
    }

    #[test]
    fn history_owner_runs_b_while_a_waits_and_third_waits_for_a_slot() {
        let (core, requests, waiting) = history_owner_fixture();
        let watchdog = std::time::Duration::from_secs(5);
        queue_history(&core, "a");
        let (a_request, a) = requests.recv_timeout(watchdog).unwrap();
        assert_eq!(a_request["params"]["threadId"], "a");
        queue_history(&core, "b");
        let (b_request, b) = requests
            .recv_timeout(watchdog)
            .expect("B starts without a reply to A");
        assert_eq!(b_request["params"]["threadId"], "b");
        queue_history(&core, "c");
        // Hard bound is proved by canonical scheduler selection separately;
        // this test observes progress after B while A remains blocked.
        b.send(Ok(empty_history("b"))).unwrap();
        let (c_request, c) = requests
            .recv_timeout(watchdog)
            .expect("B completion releases a slot");
        assert_eq!(c_request["params"]["threadId"], "c");
        assert!(core.thread_semantic_in_flight("b").is_empty());
        assert!(!core.thread_semantic_in_flight("a").is_empty());
        assert!(a.is_active());
        core.shutdown();
        // Waiting channel closes only after the actual owner has exited.
        wait_history_owner_exit(&waiting);
        assert!(!a.is_active());
        assert!(!c.is_active());
    }

    #[test]
    fn inline_history_completion_before_send_returns_releases_slot_and_wakes_owner() {
        struct InlineReply(HeldHistory);
        impl crate::rpc::JsonRpcRequestTransport for InlineReply {
            fn send_json_rpc_request(
                &self,
                id: String,
                payload: String,
                response: crate::rpc::JsonRpcResponseSender,
            ) -> Result<(), String> {
                let request: serde_json::Value = serde_json::from_str(&payload).unwrap();
                if request["params"]["threadId"] == "b" {
                    response.send(Ok(empty_history("b"))).unwrap();
                    Ok(())
                } else {
                    crate::rpc::JsonRpcRequestTransport::send_json_rpc_request(
                        &self.0, id, payload, response,
                    )
                }
            }
        }
        let (core, _, waiting) = history_owner_fixture();
        let (tx, requests) = std::sync::mpsc::channel();
        *core
            .transport_runtime()
            .ws_command_sender()
            .test_requests
            .lock()
            .unwrap() = Some(Arc::new(InlineReply(HeldHistory(tx))));
        queue_history(&core, "a");
        let (_, a) = requests
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        queue_history(&core, "b");
        queue_history(&core, "c");
        let (request, _) = requests
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        assert_eq!(request["params"]["threadId"], "c");
        assert!(core.thread_semantic_in_flight("b").is_empty());
        assert!(a.is_active());
        core.shutdown();
        wait_history_owner_exit(&waiting);
    }

    #[test]
    fn history_waiter_is_owned_by_all_consumers_and_durable_demand() {
        use crate::timeline::controller::{TimelineDemand, TimelineIntent};
        for durable in [false, true] {
            let (core, requests, waiting) = history_owner_fixture();
            let action = SemanticTimelineRequestAction::ThreadTimelinePage {
                key: SemanticTimelineRequestKey::ThreadNewest {
                    thread_id: "a".into(),
                },
                params: pioneer_protocol::ThreadTimelinePageParams {
                    thread_id: "a".into(),
                    anchor: pioneer_protocol::TimelinePageAnchor::Newest,
                    limit: Some(100),
                },
            };
            if durable {
                queue_history(&core, "a");
            }
            // Feed the real timeline owner immutable page plans. This isolates
            // logical ownership from the unrelated viewport geometry planner.
            for consumer in ["one", "two"] {
                let demand = TimelineDemand {
                    thread_id: "a".into(),
                    consumer_id: consumer.into(),
                    generation: 1,
                    source_revision: 0,
                    row_ids: vec![],
                    threshold: 0,
                    before: false,
                    after: false,
                    work: false,
                    presented_rows: true,
                    scroll_generation: 0,
                    latest_user_turn_id: None,
                    viewed_through_turn_id: None,
                    viewed_row_ids: vec![],
                    viewed_through_block: None,
                    read_requires_unread: false,
                    prefetch_on_visibility: true,
                    boundary_request_limit: 1,
                };
                let plan = core.thread_registry.lock().unwrap().timeline.update(
                    demand,
                    vec![action.clone()],
                    0,
                );
                for page in plan.pages {
                    core.schedule_planned_timeline_request(page).unwrap();
                }
            }
            let (_, response) = requests
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap();
            for consumer in ["one", "two"] {
                core.timeline_intent(TimelineIntent::Exit {
                    thread_id: "a".into(),
                    consumer_id: consumer.into(),
                    generation: 1,
                });
                if consumer == "one" || durable {
                    assert!(response.is_active());
                } else {
                    assert!(
                        !response.is_active(),
                        "last consumer retires exact RPC waiter"
                    );
                }
            }
            core.shutdown();
            wait_history_owner_exit(&waiting);
            assert!(!response.is_active());
        }
    }

    #[test]
    fn foreground_subscription_readiness_bypasses_full_history_slots() {
        let (core, requests, waiting) = history_owner_fixture();
        let watchdog = std::time::Duration::from_secs(5);
        queue_history(&core, "a");
        let (_, a) = requests.recv_timeout(watchdog).unwrap();
        queue_history(&core, "b");
        let (_, b) = requests.recv_timeout(watchdog).unwrap();
        let sending = core.clone();
        let foreground = std::thread::spawn(move || {
            let budget = crate::rpc::RequestBudget::new(
                std::time::Instant::now() + std::time::Duration::from_secs(5),
            );
            sending
                .ensure_thread_subscription("c", "ws", &budget)
                .unwrap()
        });
        let (request, response) = requests.recv_timeout(watchdog).unwrap();
        assert_eq!(request["method"], "thread/start");
        response
            .send(Ok(serde_json::to_value(
                pioneer_protocol::ThreadStartResponse {
                    thread: thread("c", "ws"),
                    sandbox: SandboxPolicy::from_mode(SandboxMode::FullAccess),
                },
            )
            .unwrap()))
            .unwrap();
        assert!(foreground.join().unwrap().is_current());
        // The warm Send prerequisite reuses readiness with history still blocked.
        let budget = crate::rpc::RequestBudget::new(std::time::Instant::now() + watchdog);
        assert!(
            core.ensure_thread_subscription("c", "ws", &budget)
                .unwrap()
                .is_current()
        );
        assert!(requests.try_recv().is_err());
        assert!(a.is_active() && b.is_active());
        core.shutdown();
        wait_history_owner_exit(&waiting);
    }

    fn rotate_history_authority(core: &ClientCore) {
        let before = core.current_auth_ticket();
        // Actual continuing-session token-rotation branch: Update increments
        // identity_request, keeps the store, and wakes history. The socket need
        // not have disappeared for the captured logical authority to be stale.
        core.begin_authorization_epoch(Some(("endpoint".into(), 8)));
        let after = core.current_auth_ticket();
        assert_ne!(after.0, before.0);
        assert_eq!(after.1, before.1);
        assert!(core.current_auth().is_some());
    }

    #[test]
    fn continuing_session_rotation_finishes_each_exact_history_obligation() {
        use crate::timeline::controller::TimelineDemand;
        let watchdog = std::time::Duration::from_secs(5);
        for kind in ["semantic", "read", "resume"] {
            let (core, requests, waiting) = history_owner_fixture();
            core.prepare_history_rotation_session_for_test();
            for id in ["a", "b", "c"] {
                core.upsert_thread(thread(id, "ws"));
            }
            let token = core.thread_operation_token("a").unwrap();
            let read = match kind {
                "semantic" => {
                    queue_history(&core, "a");
                    None
                }
                "resume" => {
                    core.apply_thread_conversation_event(
                        "ws",
                        ConversationEvent::TurnStarted {
                            thread_id: "a".into(),
                            turn: running_turn("old"),
                        },
                        None,
                    );
                    core.schedule_thread_resume("a").unwrap();
                    None
                }
                "read" => {
                    let demand = TimelineDemand {
                        thread_id: "a".into(),
                        consumer_id: "reader".into(),
                        generation: 1,
                        source_revision: 0,
                        row_ids: vec![],
                        threshold: 0,
                        before: false,
                        after: false,
                        work: false,
                        presented_rows: true,
                        scroll_generation: 0,
                        latest_user_turn_id: Some("viewed".into()),
                        viewed_through_turn_id: Some("viewed".into()),
                        viewed_row_ids: vec![],
                        viewed_through_block: None,
                        read_requires_unread: false,
                        prefetch_on_visibility: false,
                        boundary_request_limit: 1,
                    };
                    let read = core
                        .thread_registry
                        .lock()
                        .unwrap()
                        .timeline
                        .update(demand, vec![], 0)
                        .reads
                        .pop()
                        .unwrap();
                    core.thread_request_sender
                        .lock()
                        .unwrap()
                        .as_ref()
                        .unwrap()
                        .send(ThreadControllerRequest::Read(read.clone()))
                        .unwrap();
                    Some(read)
                }
                _ => unreachable!(),
            };
            let (_, old) = requests.recv_timeout(watchdog).unwrap();
            let semantic_key = SemanticTimelineRequestKey::ThreadNewest {
                thread_id: "a".into(),
            };
            let semantic_generation = core
                .thread_registry
                .lock()
                .unwrap()
                .timeline
                .request_generation(&semantic_key);
            // A serial job is a deterministic progress marker: it cannot begin
            // until the held flight's logical finalization and release finish.
            let generation = core.thread_operation_token("b").unwrap().generation;
            core.thread_request_sender
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .send(ThreadControllerRequest::Binding {
                    id: "b".into(),
                    workspace: "ws".into(),
                    generation,
                })
                .unwrap();
            rotate_history_authority(&core);
            let (next, _) = requests
                .recv_timeout(watchdog)
                .expect("continuing-session rotation releases the barrier");
            assert_eq!(next["method"], "cli_runtime/thread_binding/get");
            assert!(!old.is_active());
            assert_eq!(
                core.thread_operation_token("a").unwrap(),
                token,
                "rotation preserves the store incarnation"
            );
            match kind {
                "semantic" => assert!(
                    !core
                        .thread_registry
                        .lock()
                        .unwrap()
                        .timeline
                        .accepts(&semantic_key, semantic_generation)
                ),
                "read" => {
                    let registry = core.thread_registry.lock().unwrap();
                    assert!(!registry.timeline.accepts_read(read.as_ref().unwrap()));
                    // A due retry may already own a newer generation; the old
                    // Loading must be gone in either case.
                }
                "resume" => {
                    let state = core.thread_coordinator_snapshot("a").unwrap();
                    assert!(!state.resume.in_progress);
                    assert!(state.resume.next_attempt_at.is_some());
                }
                _ => unreachable!(),
            }
            assert!(old.send(Ok(serde_json::Value::Null)).is_err());
            core.shutdown();
            wait_history_owner_exit(&waiting);
        }
    }

    #[test]
    fn terminal_cancellation_wakes_history_after_queued_job_reaches_blocked_wait() {
        struct GatedResume {
            held: HeldHistory,
            release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
        }
        impl crate::rpc::JsonRpcRequestTransport for GatedResume {
            fn send_json_rpc_request(
                &self,
                id: String,
                payload: String,
                response: crate::rpc::JsonRpcResponseSender,
            ) -> Result<(), String> {
                let rpc: serde_json::Value = serde_json::from_str(&payload).unwrap();
                let gate = rpc["method"] == "turn/get";
                crate::rpc::JsonRpcRequestTransport::send_json_rpc_request(
                    &self.held, id, payload, response,
                )?;
                if gate {
                    self.release
                        .lock()
                        .unwrap()
                        .recv_timeout(std::time::Duration::from_secs(5))
                        .map_err(|_| "test did not release Resume send".to_owned())?;
                }
                Ok(())
            }
        }
        let (core, _, waiting) = history_owner_fixture();
        let watchdog = std::time::Duration::from_secs(5);
        let (tx, requests) = std::sync::mpsc::channel();
        let (release, released) = std::sync::mpsc::channel();
        *core
            .transport_runtime()
            .ws_command_sender()
            .test_requests
            .lock()
            .unwrap() = Some(Arc::new(GatedResume {
            held: HeldHistory(tx),
            release: std::sync::Mutex::new(released),
        }));
        core.apply_thread_conversation_event(
            "ws",
            ConversationEvent::TurnStarted {
                thread_id: "a".into(),
                turn: running_turn("A"),
            },
            None,
        );
        core.schedule_thread_resume("a").unwrap();
        let (rpc, old) = requests.recv_timeout(watchdog).unwrap();
        assert_eq!(rpc["method"], "turn/get");
        assert_eq!(rpc["params"]["turn_id"], "A");
        let (token, params) = core.begin_thread_turn_cancellation("a", None).unwrap();
        assert_eq!(params.turn_id, "A");
        queue_history(&core, "b");
        // Owner is still inside send A, so draining cannot race with a new
        // waiting notification. Every later signal belongs to the B phase.
        for _ in waiting.try_iter() {}
        release.send(()).unwrap();
        // The first wait consumes the coalesced enqueue wake. The second has
        // exhausted that wake and the eligible work, with A's barrier held.
        for _ in 0..2 {
            assert!(
                waiting
                    .recv_timeout(watchdog)
                    .unwrap()
                    .is_some_and(|d| !d.is_zero())
            );
        }
        assert!(old.is_active());
        assert!(std::time::Instant::now() < old.deadline());
        let mut cancelled = running_turn("A");
        cancelled.status = TurnStatus::Interrupted;
        let event = crate::turns::cancel::turn_cancel_response_event(TurnCancelResponse {
            workspace_id: "ws".into(),
            thread_id: "a".into(),
            turn: cancelled,
        });
        assert!(core.finish_thread_turn_cancellation(&token, "A", event, false));
        assert_eq!(
            core.thread_coordinator_snapshot("a")
                .unwrap()
                .conversation
                .in_flight_turn_id(),
            None
        );
        let (rpc, b) = requests
            .recv_timeout(
                watchdog.min(
                    old.deadline()
                        .saturating_duration_since(std::time::Instant::now()),
                ),
            )
            .expect("terminal cancellation alone wakes history and releases the serial barrier");
        assert!(
            std::time::Instant::now() < old.deadline(),
            "progress must precede A's timeout"
        );
        assert_eq!(rpc["method"], "thread/timeline/page");
        assert_eq!(rpc["params"]["threadId"], "b");
        assert!(!old.is_active());
        assert!(old.send(Ok(serde_json::Value::Null)).is_err());
        assert!(b.is_active());
        core.shutdown();
        wait_history_owner_exit(&waiting);
    }

    #[test]
    fn terminal_or_moved_resume_retires_snapshot_and_items_without_waiting_for_reply() {
        let watchdog = std::time::Duration::from_secs(5);
        for page in [false, true] {
            for moved in [false, true] {
                let (core, requests, waiting) = history_owner_fixture();
                core.apply_thread_conversation_event(
                    "ws",
                    ConversationEvent::TurnStarted {
                        thread_id: "a".into(),
                        turn: running_turn("old"),
                    },
                    None,
                );
                core.schedule_thread_resume("a").unwrap();
                let (_, mut old) = requests.recv_timeout(watchdog).unwrap();
                if page {
                    old.send(Ok(serde_json::to_value(
                        pioneer_protocol::TurnGetResponse {
                            thread_id: "a".into(),
                            workspace_id: "ws".into(),
                            turn: running_turn("old"),
                        },
                    )
                    .unwrap()))
                        .unwrap();
                    let (request, items) = requests.recv_timeout(watchdog).unwrap();
                    assert_eq!(request["method"], "turn/items/page");
                    old = items;
                }
                queue_history(&core, "b");
                if moved {
                    // One registry-owned lifecycle transition: history cannot
                    // retire the held RPC in the gap between terminal A and B.
                    let mut mutation = core.thread_mutation("a", "ws").unwrap();
                    let mut cancelled = running_turn("old");
                    cancelled.status = TurnStatus::Interrupted;
                    mutation.conversation.apply(ConversationEvent::TurnFailed {
                        thread_id: "a".into(),
                        turn: cancelled,
                    });
                    mutation.conversation.apply(ConversationEvent::TurnStarted {
                        thread_id: "a".into(),
                        turn: running_turn("new"),
                    });
                    assert_eq!(mutation.conversation.in_flight_turn_id(), Some("new"));
                } else {
                    let mut turn = running_turn("old");
                    turn.status = TurnStatus::Completed;
                    core.apply_thread_conversation_event(
                        "ws",
                        ConversationEvent::TurnCompleted {
                            thread_id: "a".into(),
                            turn,
                        },
                        None,
                    );
                    core.tick_thread_conversations();
                }
                let (request, b) = requests
                    .recv_timeout(watchdog)
                    .expect("B must bypass retired Resume without reply or timeout");
                assert_eq!(request["params"]["threadId"], "b");
                assert!(!old.is_active());
                if !moved {
                    core.apply_thread_conversation_event(
                        "ws",
                        ConversationEvent::TurnStarted {
                            thread_id: "a".into(),
                            turn: running_turn("new"),
                        },
                        None,
                    );
                    core.schedule_thread_resume("a").unwrap();
                }
                b.send(Ok(empty_history("b"))).unwrap();
                let (request, new) = requests.recv_timeout(watchdog).unwrap();
                assert_eq!(request["method"], "turn/get");
                assert_eq!(request["params"]["turn_id"], "new");
                assert!(old.send(Ok(serde_json::Value::Null)).is_err());
                assert!(new.is_active());
                // The rejected old reply must leave the new Resume usable.
                new.send(Ok(serde_json::to_value(
                    pioneer_protocol::TurnGetResponse {
                        thread_id: "a".into(),
                        workspace_id: "ws".into(),
                        turn: running_turn("new"),
                    },
                )
                .unwrap()))
                    .unwrap();
                let (request, items) = requests.recv_timeout(watchdog).unwrap();
                assert_eq!(request["method"], "turn/items/page");
                assert_eq!(request["params"]["turn_id"], "new");
                assert!(items.is_active());
                core.shutdown();
                wait_history_owner_exit(&waiting);
            }
        }
    }

    #[test]
    fn buffered_history_success_cannot_apply_to_recreated_loading_request() {
        struct BufferedReplacement {
            core: std::sync::Weak<ClientCore>,
            first: std::sync::atomic::AtomicBool,
            held: HeldHistory,
        }
        impl crate::rpc::JsonRpcRequestTransport for BufferedReplacement {
            fn send_json_rpc_request(
                &self,
                id: String,
                payload: String,
                response: crate::rpc::JsonRpcResponseSender,
            ) -> Result<(), String> {
                if self.first.swap(false, std::sync::atomic::Ordering::SeqCst) {
                    // The actual owner is inside this send: completion is buffered
                    // successfully, but cannot be polled/applied until we return.
                    response
                        .send(Ok(serde_json::to_value(presentation_page(&[(
                            "stale",
                            "old payload",
                        )]))
                        .unwrap()))
                        .unwrap();
                    let core = self.core.upgrade().unwrap();
                    let old = core.thread_operation_token("a").unwrap();
                    core.remove_thread_store("a");
                    core.upsert_thread(thread("a", "ws"));
                    assert_ne!(core.thread_operation_token("a").unwrap(), old);
                    queue_history(&core, "a");
                    Ok(())
                } else {
                    crate::rpc::JsonRpcRequestTransport::send_json_rpc_request(
                        &self.held, id, payload, response,
                    )
                }
            }
        }
        let (core, _, waiting) = history_owner_fixture();
        let (tx, requests) = std::sync::mpsc::channel();
        *core
            .transport_runtime()
            .ws_command_sender()
            .test_requests
            .lock()
            .unwrap() = Some(Arc::new(BufferedReplacement {
            core: Arc::downgrade(&core),
            first: std::sync::atomic::AtomicBool::new(true),
            held: HeldHistory(tx),
        }));
        queue_history(&core, "a");
        let (_, new) = requests
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        let snapshot = core.thread_semantic_snapshot("a");
        let top = &snapshot.thread("a").unwrap().top_level;
        assert!(top.block("stale").is_none());
        assert!(matches!(
            top.request_status,
            crate::timeline::semantic::TimelineRequestStatus::Loading { .. }
        ));
        assert!(new.is_active());
        new.send(Ok(serde_json::to_value(presentation_page(&[(
            "fresh",
            "new payload",
        )]))
        .unwrap()))
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            assert!(
                std::time::Instant::now() < deadline,
                "new request did not complete"
            );
            if core.thread_semantic_in_flight("a").is_empty() {
                break;
            }
            waiting
                .recv_timeout(deadline.saturating_duration_since(std::time::Instant::now()))
                .unwrap();
        }
        let snapshot = core.thread_semantic_snapshot("a");
        let top = &snapshot.thread("a").unwrap().top_level;
        assert!(top.block("stale").is_none());
        assert!(top.block("fresh").is_some());
        core.shutdown();
        wait_history_owner_exit(&waiting);
    }

    #[test]
    fn authority_retirement_after_preliminary_check_fences_actual_semantic_apply() {
        let core = crate::catalog_test_support::settings_client();
        connect_subscription_test(&core);
        core.prepare_history_rotation_session_for_test();
        for id in ["a", "b"] {
            core.upsert_thread(thread(id, "ws"));
        }
        let begin = |id: &str| {
            core.begin_thread_semantic_request(SemanticTimelineRequestAction::ThreadTimelinePage {
                key: SemanticTimelineRequestKey::ThreadNewest {
                    thread_id: id.into(),
                },
                params: pioneer_protocol::ThreadTimelinePageParams {
                    thread_id: id.into(),
                    anchor: pioneer_protocol::TimelinePageAnchor::Newest,
                    limit: Some(100),
                },
            })
            .unwrap()
            .unwrap()
        };
        let authority = crate::core::HistoryAuthority::capture(&core);
        let prepared = core.prepare_thread_semantic_request(begin("a")).unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let response = crate::rpc::JsonRpcResponseSender::new(
            tx,
            std::time::Instant::now() + std::time::Duration::from_secs(5),
        );
        response
            .send(Ok(serde_json::to_value(presentation_page(&[(
                "stale",
                "old authority",
            )]))
            .unwrap()))
            .unwrap();
        let buffered = rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap()
            .map_err(anyhow::Error::new);
        assert!(authority.is_current(&core)); // same preliminary predicate as the real owner
        let token = core.thread_operation_token("a");
        rotate_history_authority(&core);
        assert_eq!(core.thread_operation_token("a"), token);
        let new = begin("b");
        let key = crate::timeline::semantic::semantic_timeline_request_key(&new.action).clone();
        let generation = new.request_generation;
        assert!(
            core.complete_thread_semantic_request(prepared, buffered, Some(&authority))
                .is_none()
        );
        assert!(
            core.thread_semantic_snapshot("a")
                .thread("a")
                .unwrap()
                .top_level
                .block("stale")
                .is_none()
        );
        assert!(core.thread_semantic_in_flight("a").is_empty());
        assert!(
            core.thread_registry
                .lock()
                .unwrap()
                .timeline
                .accepts(&key, generation)
        );
        let next = core.prepare_thread_semantic_request(new).unwrap();
        assert!(
            core.complete_thread_semantic_request(next, Ok(empty_history("b")), None)
                .is_none()
        );
        assert!(core.thread_semantic_in_flight("b").is_empty());
    }

    struct ResumeSnapshotPort(std::cell::RefCell<Vec<(String, String)>>);
    impl crate::rpc::JsonRpcRequestTransport for ResumeSnapshotPort {
        fn send_json_rpc_request(
            &self,
            _: String,
            payload: String,
            response: crate::rpc::JsonRpcResponseSender,
        ) -> Result<(), String> {
            let rpc: serde_json::Value = serde_json::from_str(&payload).unwrap();
            let method = rpc["method"].as_str().unwrap().to_owned();
            let turn = rpc["params"]["turn_id"].as_str().unwrap().to_owned();
            self.0.borrow_mut().push((method.clone(), turn.clone()));
            assert_eq!(method, "turn/get", "stale A items must never be sent");
            response
                .send(Ok(serde_json::to_value(
                    pioneer_protocol::TurnGetResponse {
                        thread_id: "a".into(),
                        workspace_id: "ws".into(),
                        turn: running_turn(&turn),
                    },
                )
                .unwrap()))
                .unwrap();
            Ok(())
        }
    }

    #[test]
    fn queued_resume_continuation_cannot_consume_new_turn_ready_intent() {
        for reset_after_sweep in [false, true] {
            let core = crate::catalog_test_support::settings_client();
            core.upsert_thread(thread("a", "ws"));
            let (sender, receiver) = crate::threads::history_scheduler::channel();
            *core.thread_request_sender.lock().unwrap() = Some(sender);
            core.apply_thread_conversation_event(
                "ws",
                ConversationEvent::TurnStarted {
                    thread_id: "a".into(),
                    turn: running_turn("A"),
                },
                None,
            );
            core.schedule_thread_resume("a").unwrap();
            core.drive_due_thread_resumes();
            use pioneer_observability::work::{Boundary, ExecutorRole, WorkItem, WorkKind};
            let item = receiver.take_by(|_| false).unwrap();
            let (request, carrier) = item.received(
                Boundary::StdThread,
                WorkKind::ClientCommand,
                ExecutorRole::ClientController,
            );
            let ThreadControllerRequest::ResumeJob(request) = request else {
                panic!()
            };
            let port = ResumeSnapshotPort(Default::default());
            let continuation = core.execute_thread_resume_quantum(&port, request).unwrap();
            let c = continuation.continuation.as_ref().unwrap();
            let stale = ThreadResumeRequest {
                id: continuation.id.clone(),
                generation: continuation.generation,
                continuation: Some(ThreadResumeContinuation {
                    attempt: c.attempt,
                    snapshot: c.snapshot.clone(),
                    after: c.after,
                }),
            };
            receiver.finish(Some(WorkItem::from_carrier(
                ThreadControllerRequest::ResumeJob(continuation),
                carrier,
                None,
            )));
            if reset_after_sweep {
                core.retire_thread_controller_jobs();
            }
            {
                let mut mutation = core.thread_mutation("a", "ws").unwrap();
                crate::threads::resume::reset_thread_resume_coordinator(&mut mutation.resume);
                let mut cancelled = running_turn("A");
                cancelled.status = TurnStatus::Interrupted;
                mutation.conversation.apply(ConversationEvent::TurnFailed {
                    thread_id: "a".into(),
                    turn: cancelled,
                });
                mutation.conversation.apply(ConversationEvent::TurnStarted {
                    thread_id: "a".into(),
                    turn: running_turn("B"),
                });
                assert_eq!(mutation.conversation.in_flight_turn_id(), Some("B"));
            }
            if !reset_after_sweep {
                core.retire_thread_controller_jobs();
            }
            core.schedule_thread_resume("a").unwrap();
            core.drive_due_thread_resumes();
            // With the after-sweep ordering the stale continuation may still be
            // queued. Its prepare must neither issue items nor eat B's ready job.
            let mut started = false;
            for _ in 0..2 {
                let item = receiver
                    .take_by(|_| false)
                    .expect("B remains scheduled without another event");
                let (request, _) = item.received(
                    Boundary::StdThread,
                    WorkKind::ClientCommand,
                    ExecutorRole::ClientController,
                );
                let ThreadControllerRequest::ResumeJob(request) = request else {
                    panic!()
                };
                let next = core.execute_thread_resume_quantum(&port, request);
                receiver.finish(None);
                if next.is_some() {
                    started = true;
                    break;
                }
            }
            assert!(
                started,
                "new turn B must begin within the two queued selections"
            );
            assert_eq!(
                *port.0.borrow(),
                [
                    ("turn/get".into(), "A".into()),
                    ("turn/get".into(), "B".into())
                ]
            );
            assert!(core.execute_thread_resume_quantum(&port, stale).is_none());
            assert!(
                core.thread_coordinator_snapshot("a")
                    .unwrap()
                    .resume
                    .in_progress
            );
        }
    }

    #[test]
    fn queued_resume_cleanup_recovers_after_cancellation_or_move_without_reset() {
        use pioneer_observability::work::{Boundary, ExecutorRole, WorkItem, WorkKind};
        let copy_continuation = |request: &ThreadResumeRequest| {
            let c = request.continuation.as_ref().unwrap();
            ThreadResumeRequest {
                id: request.id.clone(),
                generation: request.generation,
                continuation: Some(ThreadResumeContinuation {
                    attempt: c.attempt,
                    snapshot: c.snapshot.clone(),
                    after: c.after,
                }),
            }
        };
        for moved in [false, true] {
            let core = crate::catalog_test_support::settings_client();
            core.upsert_thread(thread("a", "ws"));
            let (sender, receiver) = crate::threads::history_scheduler::channel();
            *core.thread_request_sender.lock().unwrap() = Some(sender);
            let start = |turn: &str| {
                let mut mutation = core.thread_mutation("a", "ws").unwrap();
                mutation.conversation.apply(ConversationEvent::TurnStarted {
                    thread_id: "a".into(),
                    turn: running_turn(turn),
                });
                assert_eq!(mutation.conversation.in_flight_turn_id(), Some(turn));
            };
            let cancel = |turn: &str| {
                let (token, params) = core.begin_thread_turn_cancellation("a", None).unwrap();
                assert_eq!(params.turn_id, turn);
                let mut cancelled = running_turn(turn);
                cancelled.status = TurnStatus::Interrupted;
                let event = crate::turns::cancel::turn_cancel_response_event(TurnCancelResponse {
                    workspace_id: "ws".into(),
                    thread_id: "a".into(),
                    turn: cancelled,
                });
                assert!(core.finish_thread_turn_cancellation(&token, turn, event, false));
            };
            let port = ResumeSnapshotPort(Default::default());
            start("A");
            core.schedule_thread_resume("a").unwrap();
            core.drive_due_thread_resumes();
            let (request, carrier) = receiver.take_by(|_| false).unwrap().received(
                Boundary::StdThread,
                WorkKind::ClientCommand,
                ExecutorRole::ClientController,
            );
            let ThreadControllerRequest::ResumeJob(request) = request else {
                panic!()
            };
            let continuation = core.execute_thread_resume_quantum(&port, request).unwrap();
            let old_running = copy_continuation(&continuation);
            let old_missing = copy_continuation(&continuation);
            receiver.finish(Some(WorkItem::from_carrier(
                ThreadControllerRequest::ResumeJob(continuation),
                carrier,
                None,
            )));

            // Cancellation removes A without resetting Resume. Only then
            // may a live TurnStarted move the conversation to B.
            cancel("A");
            if moved {
                start("B");
                core.schedule_thread_resume("a").unwrap();
            } else {
                assert_eq!(
                    core.thread_coordinator_snapshot("a")
                        .unwrap()
                        .conversation
                        .in_flight_turn_id(),
                    None
                );
            }
            assert!(
                core.thread_coordinator_snapshot("a")
                    .unwrap()
                    .resume
                    .in_progress
            );
            core.retire_thread_controller_jobs();
            core.drive_due_thread_resumes();
            if core
                .thread_coordinator_snapshot("a")
                .unwrap()
                .resume
                .in_progress
            {
                let (request, _) = receiver
                    .take_by(|_| false)
                    .expect("unfinished attempt must retain a cleanup owner after sweep")
                    .received(
                        Boundary::StdThread,
                        WorkKind::ClientCommand,
                        ExecutorRole::ClientController,
                    );
                let ThreadControllerRequest::ResumeJob(request) = request else {
                    panic!()
                };
                assert!(core.execute_thread_resume_quantum(&port, request).is_none());
                receiver.finish(None);
            }
            let coordinator = core.thread_coordinator_snapshot("a").unwrap();
            let resume = &coordinator.resume;
            assert!(
                !resume.in_progress,
                "no orphaned logical attempt after prepare"
            );
            assert_eq!(resume.next_attempt_at.is_some(), moved);
            if !moved {
                start("B");
                core.schedule_thread_resume("a").unwrap();
            }
            // No further event/reset/rotation or timeout: discovery alone must
            // now admit B using the ready intent or existing recovery deadline.
            core.drive_due_thread_resumes();
            let (request, carrier) = receiver
                .take_by(|_| false)
                .expect("B begins without an additional recovery trigger")
                .received(
                    Boundary::StdThread,
                    WorkKind::ClientCommand,
                    ExecutorRole::ClientController,
                );
            let ThreadControllerRequest::ResumeJob(request) = request else {
                panic!()
            };
            let continuation = core.execute_thread_resume_quantum(&port, request).unwrap();
            receiver.finish(Some(WorkItem::from_carrier(
                ThreadControllerRequest::ResumeJob(continuation),
                carrier,
                None,
            )));
            assert_eq!(
                *port.0.borrow(),
                [
                    ("turn/get".into(), "A".into()),
                    ("turn/get".into(), "B".into()),
                ]
            );
            assert!(core.prepare_thread_resume_quantum(old_running).is_none());
            assert!(
                core.thread_coordinator_snapshot("a")
                    .unwrap()
                    .resume
                    .in_progress
            );
            // Missing turn must also check the attempt receipt before reset.
            cancel("B");
            assert!(core.prepare_thread_resume_quantum(old_missing).is_none());
            assert!(
                core.thread_coordinator_snapshot("a")
                    .unwrap()
                    .resume
                    .in_progress
            );
            core.retire_thread_controller_jobs();
            if core
                .thread_coordinator_snapshot("a")
                .unwrap()
                .resume
                .in_progress
            {
                let (request, _) = receiver.take_by(|_| false).unwrap().received(
                    Boundary::StdThread,
                    WorkKind::ClientCommand,
                    ExecutorRole::ClientController,
                );
                let ThreadControllerRequest::ResumeJob(request) = request else {
                    panic!()
                };
                assert!(core.prepare_thread_resume_quantum(request).is_none());
                receiver.finish(None);
            }
            assert!(
                !core
                    .thread_coordinator_snapshot("a")
                    .unwrap()
                    .resume
                    .in_progress
            );
        }
    }

    #[test]
    fn history_owner_retirement_cancels_exact_local_waiter_and_fences_recreated_store() {
        for retirement in ["remove", "reconnect", "clear", "shutdown", "drop"] {
            let (core, requests, waiting) = history_owner_fixture();
            let watchdog = std::time::Duration::from_secs(5);
            queue_history(&core, "a");
            let (_, response) = requests.recv_timeout(watchdog).unwrap();
            assert!(response.is_active());
            match retirement {
                "remove" => {
                    core.remove_thread_store("a");
                    core.upsert_thread(thread("a", "ws"));
                }
                "reconnect" => core.cancel_thread_requests(),
                "clear" => core.clear_thread_stores(),
                "shutdown" => core.shutdown(),
                "drop" => {}
                _ => unreachable!(),
            }
            if retirement != "drop" && retirement != "shutdown" {
                assert!(
                    !response.is_active(),
                    "retirement must cancel without waiting for RPC timeout"
                );
                assert!(response.send(Ok(empty_history("a"))).is_err());
                assert!(core.thread_semantic_in_flight("a").is_empty());
            }
            if retirement != "drop" {
                core.shutdown();
            }
            drop(core);
            wait_history_owner_exit(&waiting);
            assert!(!response.is_active());
            assert!(response.send(Ok(empty_history("a"))).is_err());
        }
    }

    #[test]
    fn current_demand_preempts_between_pages_and_recovery_advances_under_visible_pressure() {
        let (core, receiver) = reconciliation_core();
        core.upsert_thread(thread("b", "ws"));
        core.thread_demand_changed(
            &ClientScope::Timeline {
                thread_id: "a".into(),
            },
            ClientDemand::Prefetch,
        );
        core.thread_demand_changed(
            &ClientScope::Timeline {
                thread_id: "b".into(),
            },
            ClientDemand::Prefetch,
        );
        let action = |id: &str| SemanticTimelineRequestAction::ThreadTimelinePage {
            key: SemanticTimelineRequestKey::ThreadNewest {
                thread_id: id.into(),
            },
            params: ThreadTimelinePageParams {
                thread_id: id.into(),
                anchor: TimelinePageAnchor::Newest,
                limit: Some(100),
            },
        };
        let history = reconciliation_history();
        let transport = ReconciliationTransport {
            calls: Default::default(),
            reply: |_: usize, params: &ThreadTimelinePageParams| {
                if params.thread_id == "a" {
                    Ok(reconciliation_server_page(&history, params))
                } else {
                    let mut page = presentation_page(&[]);
                    page.thread_id = params.thread_id.clone();
                    Ok(page)
                }
            },
        };
        core.schedule_thread_semantic_request(action("a")).unwrap();
        assert_eq!(
            run_scheduled_semantic_quantum(&core, &receiver, &transport),
            "a"
        );
        assert_eq!(transport.calls.borrow().len(), 1);
        assert!(!core.thread_semantic_in_flight("a").is_empty());

        // A's continuation is already ahead of B. Demand changes only after
        // B was enqueued, so an enqueue-time priority snapshot would choose A.
        core.schedule_thread_semantic_request(action("b")).unwrap();
        core.thread_demand_changed(
            &ClientScope::Timeline {
                thread_id: "b".into(),
            },
            ClientDemand::Visible,
        );
        assert_eq!(
            run_scheduled_semantic_quantum(&core, &receiver, &transport),
            "b"
        );
        for _ in 0..3 {
            core.schedule_thread_semantic_request(action("b")).unwrap();
            assert_eq!(
                run_scheduled_semantic_quantum(&core, &receiver, &transport),
                "b"
            );
        }
        core.schedule_thread_semantic_request(action("b")).unwrap();
        assert_eq!(
            run_scheduled_semantic_quantum(&core, &receiver, &transport),
            "a"
        );
        assert_eq!(
            transport
                .calls
                .borrow()
                .iter()
                .map(|params| params.thread_id.as_str())
                .collect::<Vec<_>>(),
            ["a", "b", "b", "b", "b", "a"]
        );
        assert!(matches!(
            transport.calls.borrow()[0].anchor,
            TimelinePageAnchor::Newest
        ));
        assert!(matches!(
            transport.calls.borrow()[5].anchor,
            TimelinePageAnchor::Around { .. }
        ));
    }

    #[test]
    fn visible_job_dispatches_between_work_item_chunks_of_one_collection() {
        struct Transport(std::cell::RefCell<Vec<(String, usize)>>);
        impl crate::rpc::JsonRpcRequestTransport for Transport {
            fn send_json_rpc_request(
                &self,
                _: String,
                payload: String,
                response: crate::rpc::JsonRpcResponseSender,
            ) -> Result<(), String> {
                let request: serde_json::Value = serde_json::from_str(&payload).unwrap();
                let params = &request["params"];
                let id = params["threadId"].as_str().unwrap();
                let work_ids = params["workItemIds"].as_array();
                self.0
                    .borrow_mut()
                    .push((id.into(), work_ids.map_or(0, Vec::len)));
                let page = if work_ids.is_some() {
                    serde_json::json!({"workspaceId":"ws", "threadId":id, "turnId":"turn", "projectionVersion":1, "items":[]})
                } else {
                    let mut page = presentation_page(&[]);
                    page.thread_id = id.into();
                    serde_json::to_value(page).unwrap()
                };
                response.send(Ok(page)).unwrap();
                Ok(())
            }
        }
        let core = core();
        core.upsert_thread(thread("a", "ws"));
        core.upsert_thread(thread("b", "ws"));
        core.thread_demand_changed(
            &ClientScope::Timeline {
                thread_id: "a".into(),
            },
            ClientDemand::Prefetch,
        );
        core.thread_demand_changed(
            &ClientScope::Timeline {
                thread_id: "b".into(),
            },
            ClientDemand::Prefetch,
        );
        let (sender, receiver) = crate::threads::history_scheduler::channel();
        *core.thread_request_sender.lock().unwrap() = Some(sender);
        core.schedule_thread_semantic_request(SemanticTimelineRequestAction::TurnWorkItemsGet {
            key: SemanticTimelineRequestKey::TurnWorkItems {
                thread_id: "a".into(),
                turn_id: "turn".into(),
            },
            params: TurnWorkItemsGetParams {
                thread_id: "a".into(),
                turn_id: "turn".into(),
                work_item_ids: (0..405).map(|index| format!("item-{index}")).collect(),
            },
        })
        .unwrap();
        let transport = Transport(Default::default());
        assert_eq!(
            run_scheduled_semantic_quantum(&core, &receiver, &transport),
            "a"
        );
        core.schedule_thread_semantic_request(SemanticTimelineRequestAction::ThreadTimelinePage {
            key: SemanticTimelineRequestKey::ThreadNewest {
                thread_id: "b".into(),
            },
            params: ThreadTimelinePageParams {
                thread_id: "b".into(),
                anchor: TimelinePageAnchor::Newest,
                limit: Some(100),
            },
        })
        .unwrap();
        core.thread_demand_changed(
            &ClientScope::Timeline {
                thread_id: "b".into(),
            },
            ClientDemand::Visible,
        );
        assert_eq!(
            run_scheduled_semantic_quantum(&core, &receiver, &transport),
            "b"
        );
        assert_eq!(
            run_scheduled_semantic_quantum(&core, &receiver, &transport),
            "a"
        );
        assert_eq!(
            run_scheduled_semantic_quantum(&core, &receiver, &transport),
            "a"
        );
        assert_eq!(
            *transport.0.borrow(),
            [
                (String::from("a"), 200),
                (String::from("b"), 0),
                (String::from("a"), 200),
                (String::from("a"), 5)
            ]
        );
        assert!(core.thread_semantic_in_flight("a").is_empty());
    }

    #[test]
    fn full_scheduler_finishes_mandatory_loading_and_preserves_subscription_retry_policy() {
        let core = crate::catalog_test_support::settings_client();
        connect_subscription_test(&core);
        core.upsert_thread(thread("a", "ws"));
        core.thread_demand_changed(
            &ClientScope::Timeline {
                thread_id: "a".into(),
            },
            ClientDemand::Visible,
        );
        for index in 0..crate::threads::history_scheduler::MAX_ORDINARY_JOBS {
            core.upsert_thread(thread(&format!("held-{index}"), "ws"));
        }
        let (sender, receiver) = crate::threads::history_scheduler::channel();
        *core.thread_request_sender.lock().unwrap() = Some(sender.clone());
        for index in 0..crate::threads::history_scheduler::MAX_ORDINARY_JOBS {
            let id = format!("held-{index}");
            let generation = core.thread_operation_token(&id).unwrap().generation;
            sender
                .send(ThreadControllerRequest::Binding {
                    id,
                    workspace: "ws".into(),
                    generation,
                })
                .unwrap();
        }
        // Every queued key belongs to a current store, so retiring stale work
        // cannot manufacture capacity for the required semantic collection.
        assert!(
            core.schedule_thread_semantic_request(initial_work_action())
                .is_err()
        );
        assert!(core.thread_semantic_in_flight("a").is_empty());
        let semantic = core.thread_semantic_snapshot("a");
        let range = semantic.thread("a").unwrap().work_range("turn").unwrap();
        assert!(matches!(
            range.initial_request_status,
            crate::timeline::semantic::TimelineRequestStatus::Failed { .. }
        ));
        assert!(matches!(
            range.request_status,
            crate::timeline::semantic::TimelineRequestStatus::Failed { .. }
        ));

        // Cross the shell-neutral dispatch boundary used by FFI as well: a
        // refused read is Rejected, rather than a successful Changed receipt.
        core.materialize_demanded_timelines();
        let source_revision = core
            .thread_presentation_snapshot("a")
            .unwrap()
            .timeline()
            .source_revision();
        let transition = core.dispatch(crate::core::ClientIntent::Timeline {
            intent: crate::timeline::controller::TimelineIntent::Update {
                demand: crate::timeline::controller::TimelineDemand {
                    thread_id: "a".into(),
                    consumer_id: "full-reader".into(),
                    generation: 1,
                    source_revision,
                    row_ids: vec![],
                    threshold: 0,
                    before: false,
                    after: false,
                    work: false,
                    presented_rows: true,
                    scroll_generation: 0,
                    latest_user_turn_id: Some("turn".into()),
                    viewed_through_turn_id: Some("turn".into()),
                    viewed_row_ids: vec![],
                    viewed_through_block: None,
                    read_requires_unread: false,
                    prefetch_on_visibility: false,
                    boundary_request_limit: 1,
                },
            },
        });
        assert_eq!(
            transition.outcome(),
            crate::core::ClientTransitionOutcome::Rejected
        );

        let (starts, _responses) = std::sync::mpsc::channel();
        let transport = Arc::new(SubscriptionRecorder {
            starts,
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        *core
            .transport_runtime()
            .ws_command_sender()
            .test_requests
            .lock()
            .unwrap() = Some(transport.clone());
        core.schedule_thread_subscription("a", "ws");
        let registry = core.thread_registry.lock().unwrap();
        let store = &registry.stores["a"];
        assert!(!store.coordinator.history_loading);
        assert!(store.subscription_failed);
        assert!(store.subscription_retry_at.is_some());
        let retry = store.subscription_request.as_ref().unwrap();
        assert_eq!(retry.attempt, 1);
        let flight = retry.flight.lock().unwrap();
        assert!(!flight.queued);
        assert!(flight.response.is_none());
        assert!(
            flight.result.is_none(),
            "the retry remains dormant until its existing backoff expires"
        );
        let saved = retry.clone();
        let deadline = flight.deadline;
        drop(flight);
        drop(registry);
        let due_at = Instant::now();
        core.thread_registry
            .lock()
            .unwrap()
            .stores
            .get_mut("a")
            .unwrap()
            .subscription_retry_at = Some(due_at);
        core.drive_thread_subscriptions();
        {
            let registry = core.thread_registry.lock().unwrap();
            let store = &registry.stores["a"];
            assert_eq!(store.subscription_request.as_ref(), Some(&saved));
            assert_eq!(
                store.subscription_request.as_ref().unwrap().attempt,
                saved.attempt
            );
            assert_eq!(store.subscription_retry_at, Some(due_at));
            assert!(!store.coordinator.history_loading);
            let flight = saved.flight.lock().unwrap();
            assert_eq!(flight.deadline, deadline);
            assert!(!flight.queued);
            assert!(flight.response.is_none());
            assert!(flight.result.is_none());
        }
        assert_eq!(transport.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        core.apply_thread_conversation_event(
            "ws",
            ConversationEvent::TurnStarted {
                thread_id: "a".into(),
                turn: running_turn("recover"),
            },
            None,
        );
        core.schedule_thread_resume("a").unwrap();
        core.drive_due_thread_resumes();
        let admitted = receiver.try_iter().collect::<Vec<_>>();
        assert_eq!(admitted.len(), crate::threads::history_scheduler::MAX_JOBS);
        assert!(
            admitted
                .iter()
                .any(|item| matches!(&item.value, ThreadControllerRequest::ResumeJob(_))),
            "continuous ordinary admission cannot take the durable recovery reservation"
        );

        // Permanent command refusal on an empty, open scheduler must consume
        // the existing finite failure policy, unlike the transient full above.
        let (core, transport, responses, receiver) = shared_subscription_core();
        assert!(
            !core
                .thread_request_sender
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .is_closed()
        );
        let id = "x".repeat(crate::threads::history_scheduler::MAX_COMMAND_BYTES);
        core.upsert_thread(thread(&id, "ws"));
        core.thread_demand_changed(
            &ClientScope::Timeline {
                thread_id: id.clone(),
            },
            ClientDemand::Visible,
        );
        core.schedule_thread_subscription(&id, "ws");
        assert!(
            receiver.try_recv().is_err(),
            "oversized Subscribe never queues"
        );
        for attempt in [1, 2] {
            let due_at = Instant::now();
            let saved = {
                let mut registry = core.thread_registry.lock().unwrap();
                let store = registry.stores.get_mut(&id).unwrap();
                assert!(!store.coordinator.history_loading);
                assert!(store.subscription_failed);
                let saved = store.subscription_request.clone().unwrap();
                assert_eq!(saved.attempt, attempt);
                assert!(store.subscription_retry_at.is_some());
                store.subscription_retry_at = Some(due_at);
                saved
            };
            core.drive_thread_subscriptions();
            assert!(receiver.try_recv().is_err(), "empty scheduler stays empty");
            assert_eq!(transport.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
            let registry = core.thread_registry.lock().unwrap();
            let store = &registry.stores[&id];
            assert!(!store.coordinator.history_loading);
            assert!(store.subscription_failed);
            assert!(store.subscription_receipt.is_none());
            let flight = saved.flight.lock().unwrap();
            assert!(!flight.queued);
            assert!(flight.response.is_none());
            assert!(
                flight
                    .result
                    .as_ref()
                    .unwrap()
                    .as_ref()
                    .unwrap_err()
                    .message
                    .contains("capacity exceeded")
            );
            if attempt == 1 {
                let next = store.subscription_request.as_ref().unwrap();
                assert_ne!(next, &saved);
                assert_eq!(next.attempt, 2);
                assert!(store.subscription_retry_at.unwrap() > due_at);
            } else {
                assert!(store.subscription_request.is_none());
                assert!(store.subscription_retry_at.is_none());
            }
        }
        assert_eq!(core.next_thread_subscription_delay(), None);
        core.drive_thread_subscriptions();
        assert!(receiver.try_recv().is_err());
        assert_eq!(transport.calls.load(std::sync::atomic::Ordering::SeqCst), 0);

        // The opposite cut: foreground passes the real current check, then
        // capacity finalization replaces its flight before registry promotion.
        core.schedule_thread_subscription(&id, "ws");
        let saved = {
            let mut registry = core.thread_registry.lock().unwrap();
            let store = registry.stores.get_mut(&id).unwrap();
            store.subscription_retry_at = Some(Instant::now());
            let saved = store.subscription_request.clone().unwrap();
            assert_eq!(saved.attempt, 1);
            saved
        };
        let (replacement_tx, replacement_rx) = std::sync::mpsc::channel();
        let due_core = core.clone();
        let due_id = id.clone();
        let captured = saved.clone();
        SUBSCRIPTION_PROMOTION_HOOK.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(move || {
                assert_eq!(
                    due_core.thread_registry.lock().unwrap().stores[&due_id]
                        .subscription_request
                        .as_ref(),
                    Some(&captured)
                );
                due_core.drive_thread_subscriptions();
                let (next, retry_at, deadline) = {
                    let registry = due_core.thread_registry.lock().unwrap();
                    let store = &registry.stores[&due_id];
                    let next = store.subscription_request.clone().unwrap();
                    assert_ne!(next, captured);
                    assert_eq!(next.attempt, 2);
                    let retry_at = store.subscription_retry_at.unwrap();
                    assert!(retry_at > Instant::now());
                    let deadline = next.flight.lock().unwrap().deadline;
                    (next, retry_at, deadline)
                };
                let publication = due_core.thread_snapshot(&due_id).unwrap();
                replacement_tx
                    .send((next, retry_at, deadline, publication))
                    .unwrap();
            }));
        });
        let error = core
            .ensure_thread_subscription(
                &id,
                "ws",
                &crate::rpc::RequestBudget::new(Instant::now() + Duration::from_secs(2)),
            )
            .err()
            .expect("capacity claim completed the old flight");
        assert!(format!("{error:#}").contains("capacity exceeded"));
        let (next, retry_at, deadline, publication) = replacement_rx.try_recv().unwrap();
        assert!(Arc::ptr_eq(
            &publication,
            &core.thread_snapshot(&id).unwrap()
        ));
        assert!(!*saved.live.lock().unwrap());
        assert!(*next.live.lock().unwrap());
        assert!(receiver.try_recv().is_err());
        assert!(responses.try_recv().is_err());
        assert_eq!(transport.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        {
            let registry = core.thread_registry.lock().unwrap();
            let store = &registry.stores[&id];
            assert_eq!(store.subscription_request.as_ref(), Some(&next));
            assert_eq!(store.subscription_request.as_ref().unwrap().attempt, 2);
            assert_eq!(store.subscription_retry_at, Some(retry_at));
            assert!(!store.coordinator.history_loading);
            assert!(store.subscription_failed);
            assert!(store.subscription_receipt.is_none());
            let flight = next.flight.lock().unwrap();
            assert_eq!(flight.deadline, deadline);
            assert!(!flight.queued);
            assert!(flight.baseline.is_none());
            assert!(flight.response.is_none());
            assert!(flight.result.is_none());
        }
        core.thread_registry
            .lock()
            .unwrap()
            .stores
            .get_mut(&id)
            .unwrap()
            .subscription_retry_at = Some(Instant::now());
        core.drive_thread_subscriptions();
        {
            let registry = core.thread_registry.lock().unwrap();
            let store = &registry.stores[&id];
            assert!(store.subscription_request.is_none());
            assert!(store.subscription_retry_at.is_none());
            assert!(!store.coordinator.history_loading);
            assert!(store.subscription_failed);
            assert!(
                next.flight
                    .lock()
                    .unwrap()
                    .result
                    .as_ref()
                    .unwrap()
                    .as_ref()
                    .unwrap_err()
                    .message
                    .contains("capacity exceeded")
            );
        }
        assert_eq!(core.next_thread_subscription_delay(), None);
        assert!(receiver.try_recv().is_err());
        assert!(responses.try_recv().is_err());
        assert_eq!(transport.calls.load(std::sync::atomic::Ordering::SeqCst), 0);

        // Capture a fresh dormant attempt, then let the real foreground owner
        // promote that same flight before the worker claims capacity failure.
        core.schedule_thread_subscription(&id, "ws");
        let saved = {
            let mut registry = core.thread_registry.lock().unwrap();
            let store = registry.stores.get_mut(&id).unwrap();
            store.subscription_retry_at = Some(Instant::now());
            let saved = store.subscription_request.clone().unwrap();
            assert_eq!(saved.attempt, 1);
            saved
        };
        let expected_scope = saved.scope().unwrap();
        let (promoted_tx, promoted_rx) = std::sync::mpsc::channel();
        let foreground_core = core.clone();
        let foreground_id = id.clone();
        SUBSCRIPTION_DUE_CAPTURE_HOOK.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(move || {
                let core = foreground_core.clone();
                let id = foreground_id.clone();
                let watchdog = Duration::from_secs(2);
                let waiter = std::thread::spawn(move || {
                    core.ensure_thread_subscription(
                        &id,
                        "ws",
                        &crate::rpc::RequestBudget::new(Instant::now() + watchdog),
                    )
                });
                let response = responses.recv_timeout(watchdog).unwrap();
                let publication = foreground_core.thread_snapshot(&foreground_id).unwrap();
                promoted_tx.send((waiter, response, publication)).unwrap();
            }));
        });
        core.drive_thread_subscriptions();
        let (waiter, response, promoted_publication) = promoted_rx.try_recv().unwrap();
        assert!(
            response.is_active(),
            "scheduled refusal cannot cancel foreground RPC"
        );
        assert!(*saved.live.lock().unwrap());
        assert!(Arc::ptr_eq(
            &promoted_publication,
            &core.thread_snapshot(&id).unwrap()
        ));
        {
            let registry = core.thread_registry.lock().unwrap();
            let store = &registry.stores[&id];
            assert_eq!(store.subscription_request.as_ref(), Some(&saved));
            assert_eq!(store.subscription_request.as_ref().unwrap().attempt, 1);
            assert!(store.subscription_retry_at.is_none());
            let flight = saved.flight.lock().unwrap();
            assert!(flight.queued);
            assert!(flight.response.is_some());
            assert!(flight.result.is_none());
        }
        response
            .send(Ok(serde_json::to_value(ThreadStartResponse {
                thread: thread(&id, "ws"),
                sandbox: SandboxPolicy::from_mode(SandboxMode::FullAccess),
            })
            .unwrap()))
            .unwrap();
        let binding = waiter.join().unwrap().unwrap();
        assert!(binding.is_current());
        assert_eq!(binding.scope().as_ref(), Some(&expected_scope.socket));
        assert_eq!(transport.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        let registry = core.thread_registry.lock().unwrap();
        let store = &registry.stores[&id];
        let receipt = store.subscription_receipt.as_ref().unwrap();
        assert_eq!(&receipt.scope, &expected_scope);
        assert!(Arc::ptr_eq(&receipt.live, &saved.live));
        assert!(*receipt.live.lock().unwrap());
        assert!(!store.coordinator.history_loading);
        assert!(!store.subscription_failed);
        assert!(store.subscription_request.is_none());
        assert!(
            saved
                .flight
                .lock()
                .unwrap()
                .result
                .as_ref()
                .unwrap()
                .is_ok()
        );
    }

    fn viewed_read_plan(
        core: &ClientCore,
        ids: &[&str],
    ) -> crate::timeline::controller::TimelineEffectPlan {
        use crate::timeline::controller::{TimelineDemand, TimelineIntent};
        let timeline = core.thread_presentation_snapshot("a").unwrap().timeline();
        core.plan_timeline_intent(
            TimelineIntent::Update {
                demand: TimelineDemand {
                    thread_id: "a".into(),
                    consumer_id: "read-viewport".into(),
                    generation: 1,
                    source_revision: timeline.source_revision(),
                    row_ids: ids.iter().map(|id| (*id).into()).collect(),
                    viewed_row_ids: ids.iter().map(|id| (*id).into()).collect(),
                    viewed_through_block: None,
                    latest_user_turn_id: None,
                    viewed_through_turn_id: None,
                    threshold: 0,
                    before: false,
                    after: false,
                    work: false,
                    presented_rows: true,
                    scroll_generation: 0,
                    read_requires_unread: false,
                    prefetch_on_visibility: false,
                    boundary_request_limit: 1,
                },
            },
            0,
        )
    }

    #[test]
    fn legacy_work_view_uses_confirmed_prompt_position_and_suppresses_peer_retry() {
        use crate::timeline::controller::{TimelineDemand, TimelineIntent};
        let core = core();
        core.upsert_thread(thread("a", "ws"));
        let _lease = core.subscribe(
            ClientScope::Timeline {
                thread_id: "a".into(),
            },
            NonZeroUsize::new(16).unwrap(),
        );
        let mut page = presentation_page(&[
            ("prompt", "Prompt"),
            ("work", ""),
            ("other-answer", "Completed answer"),
            ("answer", "Completed answer"),
        ]);
        let mut work = run_work();
        work.turn_id = "turn-prompt".into();
        page.blocks[1].turn_id = Some(work.turn_id.clone());
        page.blocks[1].kind = TimelineBlockKind::TurnWork { work };
        for (index, turn) in [(2, "turn-earlier"), (3, "turn-prompt")] {
            page.blocks[index].turn_id = Some(turn.into());
            page.blocks[index].kind = TimelineBlockKind::AssistantMessage {
                item_id: page.blocks[index].block_id.clone(),
                text: "Completed answer".into(),
                html_publications: Vec::new(),
                status: TurnWorkItemStatus::Completed,
                markdown: None,
                author: None,
                route: None,
            };
        }
        core.apply_thread_timeline_page(
            page.clone(),
            crate::timeline::semantic::TopLevelPageMergeMode::Reset,
        );
        core.materialize_demanded_timelines();
        let plan = |generation, turn: &str| {
            let timeline = core.thread_presentation_snapshot("a").unwrap().timeline();
            core.plan_timeline_intent(
                TimelineIntent::Update {
                    demand: TimelineDemand {
                        thread_id: "a".into(),
                        consumer_id: "legacy-work".into(),
                        generation,
                        source_revision: timeline.source_revision(),
                        row_ids: vec!["work".into()],
                        viewed_row_ids: vec!["work".into()],
                        viewed_through_block: None,
                        latest_user_turn_id: Some(turn.into()),
                        viewed_through_turn_id: Some(turn.into()),
                        threshold: 0,
                        before: false,
                        after: false,
                        work: false,
                        presented_rows: true,
                        scroll_generation: 0,
                        read_requires_unread: false,
                        prefetch_on_visibility: false,
                        boundary_request_limit: 1,
                    },
                },
                0,
            )
        };
        let exit = |generation| {
            core.plan_timeline_intent(
                TimelineIntent::Exit {
                    thread_id: "a".into(),
                    consumer_id: "legacy-work".into(),
                    generation,
                },
                0,
            )
        };
        let first = plan(1, "turn-prompt").reads.remove(0);
        assert_eq!(first.through_turn_id, "turn-prompt");
        assert_eq!(first.through_block, None, "unseen answer is not an anchor");
        exit(1);
        let unconfirmed = plan(2, "turn-prompt").reads.remove(0);
        assert_ne!(first.generation, unconfirmed.generation);
        core.thread_registry
            .lock()
            .unwrap()
            .timeline
            .finish_read(&unconfirmed, false, 0);
        let retry = core
            .thread_registry
            .lock()
            .unwrap()
            .timeline
            .due_reads(u64::MAX)
            .remove(0);
        assert!(retry.through_block.is_none());
        assert!(core.observe_workspace_notification(
            &GatewayNotification::ThreadReadCursorChanged(ThreadReadCursorChangedNotification {
                workspace_id: "ws".into(),
                thread_id: "a".into(),
                cursor: ThreadReadCursor {
                    sort_key: "0000".into(),
                    through_turn_id: "turn-prompt".into(),
                },
                unread_count: 0,
            })
        ));
        core.execute_timeline_read(retry.clone());
        let mut registry = core.thread_registry.lock().unwrap();
        assert!(!registry.timeline.accepts_read(&retry));
        assert!(registry.timeline.due_reads(u64::MAX).is_empty());
        drop(registry);
        exit(2);
        assert!(plan(3, "turn-prompt").reads.is_empty());
        // A cursor in a different Turn also covers the prompt by sort_key,
        // independently of the lexical order of the Turn identifiers.
        core.apply_directory_read(
            "ws",
            "a",
            &ThreadReadCursor {
                sort_key: "0002".into(),
                through_turn_id: "turn-earlier".into(),
            },
            0,
        );
        assert!(plan(3, "turn-prompt").reads.is_empty());
        assert_eq!(
            plan(3, "unloaded-prompt").reads.len(),
            1,
            "without a loaded prompt position the legacy read stays eligible"
        );
        assert!(plan(3, "turn-prompt").reads.is_empty());
        let answer = viewed_read_plan(&core, &["answer"]).reads.remove(0);
        assert_eq!(answer.through_block.as_ref().unwrap().sort_key, "0003");
        core.apply_directory_read(
            "ws",
            "a",
            &ThreadReadCursor {
                sort_key: "0003".into(),
                through_turn_id: "turn-prompt".into(),
            },
            0,
        );
        page.blocks[3].sort_key = "0004".into();
        core.apply_thread_timeline_page(
            page,
            crate::timeline::semantic::TopLevelPageMergeMode::Reset,
        );
        core.materialize_demanded_timelines();
        let late = viewed_read_plan(&core, &["answer"]).reads.remove(0);
        assert_eq!(late.through_block.as_ref().unwrap().sort_key, "0004");
        assert!(
            !core
                .thread_registry
                .lock()
                .unwrap()
                .timeline_has_unread("a")
        );
    }

    #[test]
    fn visible_agent_authored_message_is_an_exact_read_anchor_after_a_final_answer() {
        let core = core();
        core.upsert_thread(thread("a", "ws"));
        let _lease = core.subscribe(
            ClientScope::Timeline {
                thread_id: "a".into(),
            },
            NonZeroUsize::new(16).unwrap(),
        );
        let mut page =
            presentation_page(&[("answer", "Finished"), ("agent-message", "Agent message")]);
        page.blocks[0].kind = TimelineBlockKind::AssistantMessage {
            item_id: "answer".into(),
            text: "Finished".into(),
            html_publications: Vec::new(),
            status: TurnWorkItemStatus::Completed,
            markdown: None,
            author: None,
            route: None,
        };
        if let TimelineBlockKind::UserMessage { author, .. } = &mut page.blocks[1].kind {
            *author = composer_occurrence().author;
        }
        core.apply_thread_timeline_page(
            page,
            crate::timeline::semantic::TopLevelPageMergeMode::Reset,
        );
        core.materialize_demanded_timelines();
        core.apply_directory_read(
            "ws",
            "a",
            &ThreadReadCursor {
                sort_key: "-1".into(),
                through_turn_id: "earlier".into(),
            },
            1,
        );
        let request = viewed_read_plan(&core, &["answer", "agent-message"])
            .reads
            .remove(0);
        let position = request.through_block.unwrap();
        assert_eq!(position.block_id, "agent-message");
        assert_eq!(request.through_turn_id, "turn-agent-message");
        // The server accepts this exact message position independently of its
        // author's unread eligibility (covered by the CRUD read regression).
        core.apply_directory_read(
            "ws",
            "a",
            &ThreadReadCursor {
                sort_key: position.sort_key,
                through_turn_id: request.through_turn_id,
            },
            0,
        );
        assert!(
            !core
                .thread_registry
                .lock()
                .unwrap()
                .timeline_has_unread("a")
        );
        assert!(
            viewed_read_plan(&core, &["answer", "agent-message"])
                .reads
                .is_empty()
        );
    }

    #[test]
    fn confirmed_cursor_suppresses_history_reads_but_allows_new_and_late_answers() {
        let core = core();
        core.upsert_thread(thread("a", "ws"));
        let _lease = core.subscribe(
            ClientScope::Timeline {
                thread_id: "a".into(),
            },
            NonZeroUsize::new(16).unwrap(),
        );
        let mut page = presentation_page(&[
            ("old", "Old"),
            ("middle", "Middle"),
            ("answer", ""),
            ("next", ""),
        ]);
        for block in &mut page.blocks[2..] {
            block.turn_id = Some("same-turn".into());
            block.kind = TimelineBlockKind::AssistantMessage {
                item_id: block.block_id.clone(),
                text: String::new(),
                html_publications: Vec::new(),
                status: TurnWorkItemStatus::Completed,
                markdown: None,
                author: None,
                route: None,
            };
        }
        core.apply_thread_timeline_page(
            page.clone(),
            crate::timeline::semantic::TopLevelPageMergeMode::Reset,
        );
        core.materialize_demanded_timelines();
        assert!(
            !core
                .thread_registry
                .lock()
                .unwrap()
                .timeline_has_unread("a")
        );
        let first = viewed_read_plan(&core, &["answer"]).reads.remove(0);
        assert!(viewed_read_plan(&core, &["answer"]).reads.is_empty());
        assert!(viewed_read_plan(&core, &[]).reads.is_empty());
        let unconfirmed = viewed_read_plan(&core, &["answer"]).reads.remove(0);
        assert_ne!(
            first.generation, unconfirmed.generation,
            "a local request is not a confirmed cursor when returning to the row"
        );
        // Successful RPCs use this same authoritative directory reduction.
        core.thread_registry
            .lock()
            .unwrap()
            .timeline
            .finish_read(&unconfirmed, true, 0);
        core.apply_directory_read(
            "ws",
            "a",
            &ThreadReadCursor {
                sort_key: "0002".into(),
                through_turn_id: "same-turn".into(),
            },
            0,
        );
        for ids in [
            &["old"][..],
            &["middle"][..],
            &[][..],
            &["answer"][..],
            &["old", "middle"][..],
            &["middle"][..],
        ] {
            assert!(
                viewed_read_plan(&core, ids).reads.is_empty(),
                "confirmed history never queues another read"
            );
        }
        let next = viewed_read_plan(&core, &["next"]).reads.remove(0);
        assert_eq!(first.through_turn_id, next.through_turn_id);
        assert_ne!(first.through_block, next.through_block);
        core.thread_registry
            .lock()
            .unwrap()
            .timeline
            .finish_read(&next, false, 0);
        assert!(core.observe_workspace_notification(
            &GatewayNotification::ThreadReadCursorChanged(ThreadReadCursorChangedNotification {
                workspace_id: "ws".into(),
                thread_id: "a".into(),
                cursor: ThreadReadCursor {
                    sort_key: "0003".into(),
                    through_turn_id: "same-turn".into()
                },
                unread_count: 0,
            })
        ));
        // An already queued retry must also consult the other session's cursor
        // before it touches the transport or starts a server transaction.
        let retry = core
            .thread_registry
            .lock()
            .unwrap()
            .timeline
            .due_reads(u64::MAX)
            .remove(0);
        core.execute_timeline_read(retry.clone());
        assert!(
            !core
                .thread_registry
                .lock()
                .unwrap()
                .timeline
                .accepts_read(&retry)
        );
        assert!(
            core.thread_registry
                .lock()
                .unwrap()
                .timeline
                .due_reads(u64::MAX)
                .is_empty()
        );
        assert!(viewed_read_plan(&core, &["next"]).reads.is_empty());
        page.blocks[2].sort_key = "0004".into();
        core.apply_thread_timeline_page(
            page,
            crate::timeline::semantic::TopLevelPageMergeMode::Reset,
        );
        core.materialize_demanded_timelines();
        let late = viewed_read_plan(&core, &["answer"]).reads.remove(0);
        assert_eq!(
            late.through_block.as_ref().unwrap().block_id,
            first.through_block.as_ref().unwrap().block_id
        );
        assert_eq!(late.through_block.as_ref().unwrap().sort_key, "0004");
        assert!(
            !core
                .thread_registry
                .lock()
                .unwrap()
                .timeline_has_unread("a"),
            "lagging zero unread is not a read gate"
        );
    }

    #[test]
    fn actual_viewed_rows_resolve_completed_html_answers_in_the_working_read_path() {
        use crate::timeline::controller::{TimelineDemand, TimelineIntent};
        use pioneer_protocol::{TimelineBlockKind, TurnWorkItemStatus};
        let core = core();
        core.upsert_thread(thread("a", "ws"));
        let _lease = core.subscribe(
            ClientScope::Timeline {
                thread_id: "a".into(),
            },
            NonZeroUsize::new(16).unwrap(),
        );
        let mut page = presentation_page(&[("answer", "")]);
        page.blocks[0].kind = TimelineBlockKind::AssistantMessage {
            html_publications: Vec::new(),
            item_id: "answer".into(),
            text: String::new(),
            status: TurnWorkItemStatus::Running,
            markdown: None,
            author: None,
            route: None,
        };
        core.apply_thread_timeline_page(
            page.clone(),
            crate::timeline::semantic::TopLevelPageMergeMode::Reset,
        );
        core.materialize_demanded_timelines();
        let plan = |viewed: Vec<String>, generation: u64| {
            let timeline = core.thread_presentation_snapshot("a").unwrap().timeline();
            core.plan_timeline_intent(
                TimelineIntent::Update {
                    demand: TimelineDemand {
                        thread_id: "a".into(),
                        consumer_id: "viewport".into(),
                        generation,
                        source_revision: timeline.source_revision(),
                        row_ids: vec!["answer".into()],
                        viewed_row_ids: viewed,
                        viewed_through_block: None,
                        latest_user_turn_id: None,
                        viewed_through_turn_id: None,
                        threshold: 0,
                        before: false,
                        after: false,
                        work: false,
                        presented_rows: true,
                        scroll_generation: 0,
                        read_requires_unread: false,
                        prefetch_on_visibility: false,
                        boundary_request_limit: 1,
                    },
                },
                0,
            )
        };
        assert!(
            plan(vec!["answer".into()], 1).reads.is_empty(),
            "streaming final block is not yet a read position"
        );
        if let TimelineBlockKind::AssistantMessage { status, .. } = &mut page.blocks[0].kind {
            *status = TurnWorkItemStatus::Completed;
        }
        core.apply_thread_timeline_page(
            page,
            crate::timeline::semantic::TopLevelPageMergeMode::Reset,
        );
        core.materialize_demanded_timelines();
        assert!(
            plan(Vec::new(), 1).reads.is_empty(),
            "prefetch row ids are not viewability"
        );
        let read = plan(vec!["answer".into()], 1).reads;
        assert_eq!(
            read.len(),
            1,
            "empty-text canonical final answer remains readable"
        );
        assert_eq!(
            read[0]
                .through_block
                .as_ref()
                .map(|position| position.block_id.as_str()),
            Some("answer")
        );
        assert!(plan(vec!["answer".into()], 1).reads.is_empty());
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
        complete_semantic_operation(&core, &transport, request);
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
    fn refresh_worker_is_bounded_and_recovers_on_later_timeline_triggers() {
        use crate::timeline::semantic::TimelineRequestStatus;
        for trigger in [
            "automatic",
            "notification",
            "removal",
            "visible",
            "reacquire",
        ] {
            let exhaust = trigger != "automatic";
            let (core, receiver) =
                reconciliation_core_with(crate::catalog_test_support::settings_client());
            {
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
            let revision = core.gateway_session().startup.transport_revision;
            connect_subscription_test(&core);
            acknowledge_subscription(&core, "a", "ws");
            let _completed = receiver.try_iter().collect::<Vec<_>>();
            let auth = core.current_auth_ticket();
            let scope = ClientScope::Timeline {
                thread_id: "a".into(),
            };
            let mut lease = Some(core.subscribe(scope.clone(), NonZeroUsize::new(16).unwrap()));
            core.apply_thread_notification(reconciliation_notification(&["result-140"]));
            let mut request = reconciliation_request(&receiver);
            let request_store_generation = request.generation;
            let retired = ThreadSemanticRequest {
                id: request.id.clone(),
                generation: request.generation,
                request_generation: request.request_generation,
                explicit_intent: request.explicit_intent,
                recovery: request.recovery,
                action: request.action.clone(),
                reconciliation: None,
                work_items: WorkItemsContinuation::default(),
            };
            let mut history = reconciliation_history();
            history.push(reconciliation_result(140));
            history.sort_by(|a, b| a.sort_key.cmp(&b.sort_key));
            let history = std::cell::RefCell::new(history);
            let invalidated = std::cell::Cell::new(false);
            for attempt in 1..=3 {
                let owner = request.request_generation;
                let transport = ReconciliationTransport {
                    calls: Default::default(),
                    reply: |ix: usize, params: &ThreadTimelinePageParams| {
                        if exhaust || attempt == 1 {
                            return Err("temporary read failure".into());
                        }
                        let page = reconciliation_server_page(&history.borrow(), params);
                        if page
                            .blocks
                            .iter()
                            .any(|block| block.block_id == "result-140")
                            && !invalidated.replace(true)
                        {
                            assert_eq!(ix, 3, "last old block response in automatic recovery");
                            assert!(
                                core.thread_registry
                                    .lock()
                                    .unwrap()
                                    .timeline
                                    .has_failed_refresh("a")
                            );
                            let mut history = history.borrow_mut();
                            let result = history
                                .iter_mut()
                                .find(|block| block.block_id == "result-140")
                                .unwrap();
                            if let TimelineBlockKind::AssistantMessage { text, .. } =
                                &mut result.kind
                            {
                                *text = "recovered latest child result".into();
                            }
                            core.apply_thread_notification(reconciliation_notification(&[
                                "result-140",
                            ]));
                            core.apply_thread_notification(reconciliation_notification(&[
                                "result-140",
                            ]));
                        }
                        Ok(page)
                    },
                };
                complete_semantic_operation(&core, &transport, request);
                if !exhaust && attempt == 2 {
                    assert!(invalidated.get());
                    assert_eq!(
                        transport
                            .calls
                            .borrow()
                            .iter()
                            .filter(|params| params.anchor == TimelinePageAnchor::Newest)
                            .count(),
                        2
                    );
                }
                assert!(core.thread_semantic_in_flight("a").is_empty());
                if attempt == 3 || (!exhaust && attempt == 2) {
                    break;
                }
                let deadline = core
                    .thread_registry
                    .lock()
                    .unwrap()
                    .timeline
                    .next_refresh_deadline()
                    .unwrap();
                assert!(core.next_timeline_request_delay().is_some());
                if exhaust {
                    core.apply_thread_notification(reconciliation_notification(&["result-140"]));
                }
                assert_eq!(
                    core.thread_registry
                        .lock()
                        .unwrap()
                        .timeline
                        .next_refresh_deadline(),
                    Some(deadline)
                );
                core.drive_timeline_requests_at(deadline - 1);
                assert_eq!(receiver.try_iter().count(), 0);
                core.drive_timeline_requests_at(deadline);
                request = reconciliation_request(&receiver);
                assert!(request.request_generation > owner);
                core.drive_timeline_requests_at(u64::MAX);
                assert_eq!(receiver.try_iter().count(), 0, "one owner per key");
            }
            assert_eq!(core.next_timeline_request_delay(), None);
            core.drive_timeline_requests_at(u64::MAX);
            assert_eq!(receiver.try_iter().count(), 0);
            core.materialize_demanded_timelines();
            let semantic = core.thread_semantic_snapshot("a");
            let cached = &semantic.thread("a").unwrap().top_level;
            let published = core.snapshot(&scope).unwrap().snapshot();
            if exhaust {
                assert!(matches!(
                    cached.request_status,
                    TimelineRequestStatus::Failed { .. }
                ));
                assert!(
                    published.serialized_payload()["status"]
                        .get("Failed")
                        .is_some()
                );
                // Without a later event, exhaustion has no deadline and does not poll.
                if trigger == "reacquire" {
                    drop(lease.take());
                } else if trigger == "visible" {
                    core.thread_demand_changed(&scope, ClientDemand::Suspended);
                }
                for _ in 0..10 {
                    let notification = if trigger == "removal" {
                        history
                            .borrow_mut()
                            .retain(|block| block.block_id != "block-100");
                        let GatewayNotification::ThreadTimelineBlocksChanged(mut update) =
                            reconciliation_notification(&[])
                        else {
                            unreachable!()
                        };
                        update.removed_block_ids = vec!["block-100".into()];
                        GatewayNotification::ThreadTimelineBlocksChanged(update)
                    } else {
                        reconciliation_notification(&["result-140"])
                    };
                    core.apply_thread_notification(notification);
                }
                if trigger == "visible" || trigger == "reacquire" {
                    assert_eq!(core.next_timeline_request_delay(), None);
                    core.drive_timeline_requests_at(u64::MAX);
                    assert_eq!(
                        receiver.try_iter().count(),
                        0,
                        "hidden parent does not poll"
                    );
                    if trigger == "reacquire" {
                        lease = Some(core.subscribe(scope.clone(), NonZeroUsize::new(16).unwrap()));
                    } else {
                        core.thread_demand_changed(&scope, ClientDemand::Visible);
                        core.thread_demand_changed(&scope, ClientDemand::Visible);
                    }
                }
                assert!(receiver.try_recv().is_err(), "wake has no payload");
                assert_eq!(
                    receiver.try_iter().count(),
                    0,
                    "one wake for the saved obligation"
                );
                let deadline = core
                    .thread_registry
                    .lock()
                    .unwrap()
                    .timeline
                    .next_refresh_deadline()
                    .unwrap();
                core.drive_timeline_requests_at(deadline - 1);
                assert_eq!(
                    receiver.try_iter().count(),
                    0,
                    "cooldown survives the trigger"
                );
                assert_eq!(
                    core.thread_semantic_snapshot("a")
                        .thread("a")
                        .unwrap()
                        .top_level
                        .request_status,
                    cached.request_status,
                    "rearming cannot acknowledge stale data"
                );
                // Hiding after the trigger preserves the deferred obligation too.
                core.thread_demand_changed(&scope, ClientDemand::Suspended);
                assert_eq!(core.next_timeline_request_delay(), None);
                core.drive_timeline_requests_at(u64::MAX);
                assert_eq!(receiver.try_iter().count(), 0);
                core.thread_demand_changed(&scope, ClientDemand::Visible);
                assert!(receiver.try_recv().is_err(), "wake has no payload");
                assert_eq!(
                    core.thread_registry
                        .lock()
                        .unwrap()
                        .timeline
                        .next_refresh_deadline(),
                    Some(deadline)
                );
                core.drive_timeline_requests_at(deadline);
                let request = reconciliation_request(&receiver);
                assert!(request.request_generation > retired.request_generation);
                let transport = ReconciliationTransport {
                    calls: Default::default(),
                    reply: |_: usize, params: &ThreadTimelinePageParams| {
                        Ok(reconciliation_server_page(&history.borrow(), params))
                    },
                };
                core.fail_timeline_dispatch(&retired);
                complete_semantic_operation(&core, &transport, retired);
                assert!(
                    transport.calls.borrow().is_empty(),
                    "retired request cannot read or finish"
                );
                assert_eq!(core.thread_semantic_in_flight("a").len(), 1);
                complete_semantic_operation(&core, &transport, request);
                assert_eq!(receiver.try_iter().count(), 0);
                core.materialize_demanded_timelines();
                assert_eq!(
                    core.thread_semantic_snapshot("a")
                        .thread("a")
                        .unwrap()
                        .top_level
                        .request_status,
                    TimelineRequestStatus::Ready
                );
                assert!(
                    core.snapshot(&scope)
                        .unwrap()
                        .snapshot()
                        .serialized_payload()
                        .to_string()
                        .contains("child result")
                );
                let registry = core.thread_registry.lock().unwrap();
                assert_eq!(
                    registry.stores["a"]
                        .subscription_receipt
                        .as_ref()
                        .map(|receipt| receipt.scope.transport_revision),
                    Some(revision)
                );
                assert_eq!(registry.stores["a"].generation, request_store_generation);
                drop(registry);
                assert_eq!(core.gateway_session().startup.transport_revision, revision);
                assert_eq!(core.current_auth_ticket(), auth);
            } else {
                assert_eq!(cached.request_status, TimelineRequestStatus::Ready);
                assert!(cached.block("result-140").is_some());
                assert!(
                    published
                        .serialized_payload()
                        .to_string()
                        .contains("recovered latest child result")
                );
            }
            drop(lease);
        }
    }

    #[test]
    fn prefetch_wakes_suspended_refresh_without_replenishing_its_budget() {
        let (core, receiver) = reconciliation_core();
        let scope = ClientScope::Timeline {
            thread_id: "a".into(),
        };
        let _lease = core.subscribe(scope.clone(), NonZeroUsize::new(16).unwrap());
        core.apply_thread_notification(reconciliation_notification(&["result-140"]));
        let mut request = reconciliation_request(&receiver);
        let action = request.action.clone();
        let transport = ReconciliationTransport {
            calls: Default::default(),
            reply: |_: usize, _: &ThreadTimelinePageParams| Err("temporary read failure".into()),
        };
        for attempt in 1..=3 {
            complete_semantic_operation(&core, &transport, request);
            if attempt == 3 {
                break;
            }
            let deadline = core
                .thread_registry
                .lock()
                .unwrap()
                .timeline
                .next_refresh_deadline()
                .unwrap();
            if attempt == 1 {
                core.thread_demand_changed(&scope, ClientDemand::Suspended);
                assert_eq!(core.next_timeline_request_delay(), None);
                core.drive_timeline_requests_at(u64::MAX);
                assert!(receiver.try_recv().is_err());
                core.thread_demand_changed(&scope, ClientDemand::Prefetch);
                assert!(receiver.try_recv().is_err(), "wake has no payload");
                core.thread_demand_changed(&scope, ClientDemand::Prefetch);
                assert!(
                    receiver.try_recv().is_err(),
                    "duplicate demand does not wake"
                );
                assert_eq!(
                    core.thread_registry
                        .lock()
                        .unwrap()
                        .next_timeline_request_deadline(),
                    Some(deadline)
                );
            }
            core.drive_timeline_requests_at(deadline - 1);
            assert!(receiver.try_recv().is_err());
            core.drive_timeline_requests_at(deadline);
            request = reconciliation_request(&receiver);
            assert_eq!(
                request.action, action,
                "retry preserves the exact Newest action"
            );
            core.drive_timeline_requests_at(u64::MAX);
            assert!(receiver.try_recv().is_err(), "one active request per key");
        }
        assert_eq!(transport.calls.borrow().len(), 3, "original attempt budget");
        assert_eq!(core.next_timeline_request_delay(), None);
        core.thread_demand_changed(&scope, ClientDemand::Suspended);
        core.thread_demand_changed(&scope, ClientDemand::Prefetch);
        core.thread_demand_changed(&scope, ClientDemand::Prefetch);
        core.drive_timeline_requests_at(u64::MAX);
        assert!(
            receiver.try_recv().is_err(),
            "Prefetch cannot rearm exhaustion"
        );
        assert!(core.thread_semantic_in_flight("a").is_empty());
        assert!(
            core.thread_registry
                .lock()
                .unwrap()
                .timeline
                .has_failed_refresh("a")
        );
    }

    #[test]
    fn duplicate_invalidation_after_last_block_read_gets_one_fresh_follow_up() {
        let (core, receiver) = reconciliation_core();
        let scope = ClientScope::Timeline {
            thread_id: "a".into(),
        };
        let _lease = core.subscribe(scope.clone(), NonZeroUsize::new(16).unwrap());
        let mut history = reconciliation_history();
        history.push(reconciliation_result(140));
        history.sort_by(|a, b| a.sort_key.cmp(&b.sort_key));
        let history = std::cell::RefCell::new(history);
        let invalidated = std::cell::Cell::new(false);
        core.apply_thread_notification(reconciliation_notification(&["result-140"]));
        let request = reconciliation_request(&receiver);
        let transport = ReconciliationTransport {
            calls: Default::default(),
            reply: |ix: usize, params: &ThreadTimelinePageParams| {
                let page = reconciliation_server_page(&history.borrow(), params);
                if page
                    .blocks
                    .iter()
                    .any(|block| block.block_id == "result-140")
                    && !invalidated.replace(true)
                {
                    assert_eq!(ix, 3, "last read of this block in the first walk");
                    let mut history = history.borrow_mut();
                    let result = history
                        .iter_mut()
                        .find(|block| block.block_id == "result-140")
                        .unwrap();
                    if let TimelineBlockKind::AssistantMessage { text, .. } = &mut result.kind {
                        *text = "updated child result".into();
                    }
                    let mut rejected = reconciliation_notification(&["result-140"]);
                    if let GatewayNotification::ThreadTimelineBlocksChanged(n) = &mut rejected {
                        n.workspace_id = "other-workspace".into();
                    }
                    core.apply_thread_notification(rejected);
                    assert!(
                        core.thread_registry
                            .lock()
                            .unwrap()
                            .timeline
                            .pending
                            .is_empty()
                    );
                    core.apply_thread_notification(reconciliation_notification(&["result-140"]));
                    core.apply_thread_notification(reconciliation_notification(&["result-140"]));
                    // The frozen response still contains the first version.
                }
                Ok(page)
            },
        };
        complete_semantic_operation(&core, &transport, request);
        assert_eq!(
            transport
                .calls
                .borrow()
                .iter()
                .filter(|p| p.anchor == TimelinePageAnchor::Newest)
                .count(),
            2
        );
        assert!(core.thread_semantic_in_flight("a").is_empty());
        assert_eq!(receiver.try_iter().count(), 0);
        core.materialize_demanded_timelines();
        assert!(
            core.snapshot(&scope)
                .unwrap()
                .snapshot()
                .serialized_payload()
                .to_string()
                .contains("updated child result")
        );
    }

    #[test]
    fn other_history_anchors_preserve_the_exact_newest_recovery() {
        use crate::timeline::semantic::TimelineRequestStatus;
        for queued in [true, false] {
            for anchor in [
                TimelinePageAnchor::Oldest,
                TimelinePageAnchor::Around {
                    cursor: reconciliation_cursor("00010:a"),
                },
            ] {
                let (core, receiver) = reconciliation_core();
                let _lease = core.subscribe(
                    ClientScope::Timeline {
                        thread_id: "a".into(),
                    },
                    NonZeroUsize::new(16).unwrap(),
                );
                let mut history = reconciliation_history();
                history.push(reconciliation_result(140));
                history.sort_by(|a, b| a.sort_key.cmp(&b.sort_key));
                let newest_calls = std::cell::Cell::new(0);
                let transport = ReconciliationTransport {
                    calls: Default::default(),
                    reply: |_: usize, params: &ThreadTimelinePageParams| {
                        if params.anchor == TimelinePageAnchor::Newest {
                            newest_calls.set(newest_calls.get() + 1);
                            if newest_calls.get() < 3 {
                                return Err("temporary read failure".into());
                            }
                        }
                        Ok(if params.anchor == TimelinePageAnchor::Oldest {
                            reconciliation_page(
                                history[..params.limit.unwrap() as usize].to_vec(),
                                false,
                                true,
                            )
                        } else {
                            reconciliation_server_page(&history, params)
                        })
                    },
                };
                let params = ThreadTimelinePageParams {
                    thread_id: "a".into(),
                    anchor,
                    limit: Some(17),
                };
                core.apply_thread_notification(reconciliation_notification(&["result-140"]));
                let request = reconciliation_request(&receiver);
                let newest = request.action.clone();
                if queued {
                    // Public fetch accepts the intent into pending while Newest owns the key.
                    assert!(
                        core.fetch_thread_timeline_page(&transport, params.clone())
                            .is_err()
                    );
                    assert!(transport.calls.borrow().is_empty());
                }
                complete_semantic_operation(&core, &transport, request);
                if queued {
                    // A different explicit anchor needs ordinary scheduler admission
                    // after the recovery quantum releases its reserved slot.
                    let other = reconciliation_request(&receiver);
                    assert!(matches!(
                        &other.action,
                        SemanticTimelineRequestAction::ThreadTimelinePage { params: other, .. }
                            if other == &params
                    ));
                    complete_semantic_operation(&core, &transport, other);
                }
                let deadline = core
                    .thread_registry
                    .lock()
                    .unwrap()
                    .timeline
                    .next_refresh_deadline()
                    .unwrap();
                if !queued {
                    core.fetch_thread_timeline_page(&transport, params.clone())
                        .unwrap();
                    assert!(receiver.try_recv().is_err(), "wake has no payload");
                }
                assert_eq!(
                    transport
                        .calls
                        .borrow()
                        .iter()
                        .filter(|call| **call == params)
                        .count(),
                    1
                );
                let semantic = core.thread_semantic_snapshot("a");
                let cached = &semantic.thread("a").unwrap().top_level;
                assert!(cached.block("block-10").is_some());
                assert!(matches!(
                    cached.request_status,
                    TimelineRequestStatus::Failed { .. }
                ));
                {
                    let registry = core.thread_registry.lock().unwrap();
                    assert_eq!(registry.timeline.next_refresh_deadline(), Some(deadline));
                    let key = crate::timeline::semantic::semantic_timeline_request_key(&newest);
                    assert!(!registry.timeline.page_retry_allowed(key, deadline - 1));
                }
                core.drive_timeline_requests_at(deadline - 1);
                assert_eq!(receiver.try_iter().count(), 0);
                for _ in 0..2 {
                    let deadline = core
                        .thread_registry
                        .lock()
                        .unwrap()
                        .timeline
                        .next_refresh_deadline()
                        .unwrap();
                    core.drive_timeline_requests_at(deadline);
                    let request = reconciliation_request(&receiver);
                    assert_eq!(
                        request.action, newest,
                        "recovery preserves anchor and limit"
                    );
                    complete_semantic_operation(&core, &transport, request);
                }
                assert_eq!(newest_calls.get(), 3);
                assert!(core.thread_semantic_in_flight("a").is_empty());
                assert_eq!(core.next_timeline_request_delay(), None);
                assert!(
                    !core
                        .thread_registry
                        .lock()
                        .unwrap()
                        .timeline
                        .has_failed_refresh("a")
                );
                let semantic = core.thread_semantic_snapshot("a");
                assert_eq!(
                    semantic.thread("a").unwrap().top_level.request_status,
                    TimelineRequestStatus::Ready
                );
                assert_eq!(receiver.try_iter().count(), 0);
            }
        }
    }

    #[test]
    fn captured_unadmitted_refusal_cannot_overwrite_completed_same_key_generation() {
        use crate::timeline::controller::{TimelineDemand, TimelineIntent};
        use crate::timeline::semantic::TimelineRequestStatus;
        let (core, _receiver) = reconciliation_core();
        core.thread_demand_changed(
            &ClientScope::Timeline {
                thread_id: "a".into(),
            },
            ClientDemand::Visible,
        );
        for index in 0..crate::timeline::controller::HISTORY_KEY_LIMIT {
            let id = format!("held-{index}");
            core.upsert_thread(thread(&id, "ws"));
            core.begin_thread_semantic_request(SemanticTimelineRequestAction::ThreadTimelinePage {
                key: SemanticTimelineRequestKey::ThreadNewest {
                    thread_id: id.clone(),
                },
                params: ThreadTimelinePageParams {
                    thread_id: id,
                    anchor: TimelinePageAnchor::Newest,
                    limit: Some(12),
                },
            })
            .unwrap()
            .unwrap();
        }
        core.materialize_demanded_timelines();
        let timeline = core.thread_presentation_snapshot("a").unwrap().timeline();
        let plan = core.plan_timeline_intent(
            TimelineIntent::Update {
                demand: TimelineDemand {
                    thread_id: "a".into(),
                    consumer_id: "refused".into(),
                    generation: 1,
                    source_revision: timeline.source_revision(),
                    row_ids: vec![timeline.rows()[0].id().as_str().to_owned()],
                    threshold: 0,
                    before: true,
                    after: false,
                    work: false,
                    presented_rows: true,
                    scroll_generation: 1,
                    latest_user_turn_id: None,
                    viewed_through_turn_id: None,
                    viewed_row_ids: vec![],
                    viewed_through_block: None,
                    read_requires_unread: false,
                    prefetch_on_visibility: true,
                    boundary_request_limit: 1,
                },
            },
            0,
        );
        assert!(plan.pages.is_empty());
        assert_eq!(plan.rejected.len(), 1);
        let refused = &plan.rejected[0];
        assert_eq!(refused.request_generation, 0);
        for index in 0..crate::timeline::controller::HISTORY_KEY_LIMIT {
            core.remove_thread_store(&format!("held-{index}"));
        }
        assert_eq!(
            core.thread_operation_token("a").unwrap().generation,
            refused.store_generation,
            "free capacity without replacing the target store"
        );
        let new = core
            .begin_thread_semantic_request(refused.action.clone())
            .unwrap()
            .unwrap();
        let block = reconciliation_history().remove(0);
        let transport = ReconciliationTransport {
            calls: Default::default(),
            reply: |_: usize, _: &ThreadTimelinePageParams| {
                Ok(reconciliation_page(vec![block.clone()], false, false))
            },
        };
        assert!(
            core.execute_thread_semantic_request(&transport, new)
                .is_none()
        );
        let key = crate::timeline::semantic::semantic_timeline_request_key(&refused.action);
        assert_eq!(
            core.thread_registry
                .lock()
                .unwrap()
                .timeline
                .request_generation(key),
            0,
            "new operation has completed, not merely become active"
        );
        assert_eq!(
            core.thread_operation_token("a").unwrap().generation,
            refused.store_generation
        );
        let ready = core.thread_snapshot("a").unwrap();
        assert_eq!(
            ready.semantic.thread("a").unwrap().top_level.request_status,
            TimelineRequestStatus::Ready
        );
        assert_eq!(
            ready
                .semantic
                .thread("a")
                .unwrap()
                .top_level
                .block(&block.block_id),
            Some(&block)
        );
        core.fail_timeline_admission(
            &refused.action,
            refused.store_generation,
            refused.request_generation,
        );
        assert!(
            Arc::ptr_eq(&ready, &core.thread_snapshot("a").unwrap()),
            "late refusal must not publish Failed or alter newer data"
        );
    }

    #[test]
    fn semantic_send_failure_finishes_only_its_owner_without_retry() {
        use crate::timeline::semantic::TimelineRequestStatus;
        for planned in [false, true] {
            let (core, receiver) = reconciliation_core();
            drop(receiver);
            let action = SemanticTimelineRequestAction::ThreadTimelinePage {
                key: SemanticTimelineRequestKey::ThreadNewest {
                    thread_id: "a".into(),
                },
                params: ThreadTimelinePageParams {
                    thread_id: "a".into(),
                    anchor: TimelinePageAnchor::Newest,
                    limit: Some(12),
                },
            };
            if planned {
                let generation = {
                    let mut registry = core.thread_registry.lock().unwrap();
                    registry.timeline.begin(action.clone()).unwrap();
                    registry.timeline.request_generation(
                        crate::timeline::semantic::semantic_timeline_request_key(&action),
                    )
                };
                core.schedule_planned_timeline_request(
                    crate::timeline::controller::TimelinePageRequest {
                        action: action.clone(),
                        generation,
                    },
                );
            } else {
                core.apply_thread_notification(reconciliation_notification(&["result-140"]));
            }
            assert!(core.thread_semantic_in_flight("a").is_empty());
            assert_eq!(core.next_timeline_request_delay(), None);
            assert!(matches!(
                core.thread_semantic_snapshot("a")
                    .thread("a")
                    .unwrap()
                    .top_level
                    .request_status,
                TimelineRequestStatus::Failed { .. }
            ));
            let old = core
                .begin_thread_semantic_request(action.clone())
                .unwrap()
                .unwrap();
            core.fail_timeline_dispatch(&old);
            let new = core.begin_thread_semantic_request(action).unwrap().unwrap();
            assert_eq!(old.generation, new.generation);
            assert_ne!(old.request_generation, new.request_generation);
            let before = core.thread_snapshot("a").unwrap();
            core.fail_timeline_dispatch(&old);
            core.fail_timeline_admission(&old.action, old.generation, old.request_generation);
            complete_semantic_operation(&core, &CancelTransport(&core), old);
            assert!(Arc::ptr_eq(&before, &core.thread_snapshot("a").unwrap()));
            assert_eq!(core.thread_semantic_in_flight("a").len(), 1);
            core.fail_timeline_dispatch(&new);
            assert!(core.thread_semantic_in_flight("a").is_empty());
            assert_eq!(core.next_timeline_request_delay(), None);
        }
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
        complete_semantic_operation(&core, &transport, request);
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
        for (cached, notification_first, finish_live_completion) in [
            (false, false, false),
            (false, true, false),
            (true, false, false),
            (true, true, false),
            (true, false, true),
            (true, true, true),
        ] {
            let echo = std::cell::RefCell::new(None);
            let (core, receiver) =
                reconciliation_core_with(crate::catalog_test_support::settings_client());
            connect_subscription_test(&core);
            if !cached {
                core.remove_thread_store("a");
                // No history page has loaded yet. An empty page would mark the
                // cache loaded, leaving the later local send without range cursors.
                drop(core.thread_mutation("a", "ws").unwrap());
            }
            struct AckTransport<'a> {
                core: &'a ClientCore,
                receiver: &'a crate::threads::history_scheduler::ThreadControllerReceiver,
                fail: bool,
                terminal_before_response: bool,
                notification_first: bool,
                finish_live_completion: bool,
                echo: &'a std::cell::RefCell<Option<Thread>>,
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
                    // Advance metadata after the request began, without advancing
                    // its second-resolution timestamp. The empty-source case also
                    // receives a live snapshot before this old response arrives.
                    let mut live = thread("a", "ws");
                    live.name = Some("Live title".into());
                    live.preview_author = composer_admission().author;
                    self.core.upsert_thread(live);
                    self.core.remember_thread_draft("ws", Some("a".into()));
                    self.core.commit_prepared_thread_turn(
                        "a",
                        "ws",
                        ThreadMode::Chat,
                        &crate::composer::turn_prepare::PreparedComposerThreadSnapshotUpdate {
                            selected_model: Some("local-model".into()),
                            selected_provider: Some("local-provider".into()),
                            selected_reasoning_effort: Some("high".into()),
                            user_text: "First local message".into(),
                            updated_at_unix: 2,
                        },
                        local("a"),
                        ThreadComposerExecutionMode::ForegroundTurn,
                    );
                    let mut snapshot = thread("a", "ws");
                    snapshot.name = Some("Server title".into());
                    let mut terminal = running_turn("turn");
                    terminal.status = TurnStatus::Completed;
                    if self.terminal_before_response {
                        self.core.apply_thread_conversation_event(
                            "ws",
                            ConversationEvent::TurnCompleted {
                                thread_id: "a".into(),
                                turn: terminal,
                            },
                            None,
                        );
                        if self.finish_live_completion {
                            assert!(self.core.tick_thread_conversations());
                        }
                        snapshot.turns.push(running_turn("turn"));
                    } else {
                        snapshot.turns.push(terminal);
                    }
                    *self.echo.borrow_mut() = Some(snapshot.clone());
                    if self.notification_first {
                        self.core
                            .apply_thread_notification(GatewayNotification::ThreadStarted(
                                ThreadStartedNotification {
                                    thread: snapshot.clone(),
                                },
                            ));
                    }
                    response
                        .send(Ok(serde_json::to_value(ThreadStartResponse {
                            thread: snapshot,
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
                complete_semantic_operation(
                    &core,
                    &ReconciliationTransport {
                        calls: Default::default(),
                        reply: |_: usize, _: &ThreadTimelinePageParams| {
                            Err("previous connection read failed".into())
                        },
                    },
                    request,
                );
                core.apply_thread_notification(reconciliation_notification(&["block-130"]));
                assert_eq!(
                    receiver
                        .try_iter()
                        .filter(|item| !matches!(item.value, ThreadControllerRequest::Resume))
                        .count(),
                    0
                );
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
                refresh_owned_subscription_using(
                    &core,
                    &AckTransport {
                        core: &core,
                        receiver: &receiver,
                        fail: true,
                        terminal_before_response: false,
                        notification_first,
                        finish_live_completion,
                        echo: &echo,
                    },
                    token(),
                    "ws"
                )
                .is_err()
            );
            assert_eq!(
                receiver
                    .try_iter()
                    .filter(|item| !matches!(item.value, ThreadControllerRequest::Resume))
                    .count(),
                0
            );
            refresh_owned_subscription_using(
                &core,
                &AckTransport {
                    core: &core,
                    receiver: &receiver,
                    fail: false,
                    terminal_before_response: cached,
                    notification_first,
                    finish_live_completion,
                    echo: &echo,
                },
                token(),
                "ws",
            )
            .unwrap();
            if !notification_first {
                core.apply_thread_notification(GatewayNotification::ThreadStarted(
                    ThreadStartedNotification {
                        thread: echo.borrow_mut().take().unwrap(),
                    },
                ));
            }
            if cached && !finish_live_completion {
                assert_eq!(
                    core.thread_snapshot("a")
                        .unwrap()
                        .coordinator()
                        .conversation
                        .status_label(),
                    "completing"
                );
                assert!(
                    core.tick_thread_conversations(),
                    "live completion can still finish"
                );
            }
            let accepted = core
                .snapshot(&scope("a"))
                .unwrap()
                .typed::<ThreadDomainSnapshot>()
                .unwrap()
                .payload();
            let coordinator = accepted.coordinator();
            let metadata = coordinator.thread().unwrap();
            assert_eq!(metadata.preview, "First local message");
            assert_eq!(metadata.model, "local-model");
            assert_eq!(metadata.model_provider, "local-provider");
            assert_eq!(metadata.reasoning_effort.as_deref(), Some("high"));
            assert_eq!(metadata.name.as_deref(), Some("Live title"));
            assert_eq!(metadata.preview_author, composer_admission().author);
            assert_eq!(coordinator.conversation.status_label(), "completed");
            assert!(core.thread_workspace_draft("ws").is_none());
            assert!(
                coordinator.conversation.in_flight_turn_id().is_none(),
                "terminal response still applies"
            );
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
            complete_semantic_operation(&core, &transport, request);
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
    fn subscription_snapshot_resumes_blocked_turn_without_reapplying_a_stale_block() {
        for (live_resume, live_reblock, snapshot_resume, snapshot_reblock) in [
            (false, false, false, false),
            (true, false, false, false),
            (true, true, false, false),
            (true, true, true, true),
            (true, true, true, false),
        ] {
            let core = core();
            let mut blocked = running_turn("resumable");
            blocked.status = TurnStatus::Blocked;
            let mut snapshot = thread("a", "ws");
            // Include both forms of lag: a running Thread with a live blocked
            // Conversation, and a blocked Thread followed by a live resumption.
            snapshot.turns = vec![if live_resume && !live_reblock {
                blocked.clone()
            } else {
                running_turn("resumable")
            }];
            core.upsert_thread(snapshot.clone());
            core.apply_thread_conversation_event(
                "ws",
                ConversationEvent::TurnBlocked {
                    thread_id: "a".into(),
                    turn: blocked.clone(),
                    resume: None,
                },
                None,
            );
            assert_eq!(
                core.thread_snapshot("a")
                    .unwrap()
                    .coordinator()
                    .conversation
                    .status_label(),
                "blocked"
            );
            struct ResumeTransport<'a> {
                core: &'a ClientCore,
                snapshot: Thread,
                live_resume: bool,
                live_reblock: bool,
                snapshot_resume: bool,
                snapshot_reblock: bool,
            }
            impl crate::rpc::JsonRpcRequestTransport for ResumeTransport<'_> {
                fn send_json_rpc_request(
                    &self,
                    _: String,
                    _: String,
                    response: crate::rpc::JsonRpcResponseSender,
                ) -> Result<(), String> {
                    if !self.live_resume {
                        // Repeating the current Blocked phase via a snapshot must
                        // still allow the fresh RPC to confirm resumption.
                        let mut thread = self.snapshot.clone();
                        thread.turns[0].status = TurnStatus::Blocked;
                        self.core
                            .apply_thread_notification(GatewayNotification::ThreadUpdated(
                                ThreadUpdatedNotification {
                                    thread,
                                    placement: None,
                                },
                            ));
                    }
                    if self.live_resume {
                        let turn = running_turn("resumable");
                        self.core
                            .apply_thread_notification(if self.snapshot_resume {
                                let mut thread = self.snapshot.clone();
                                thread.turns = vec![turn];
                                GatewayNotification::ThreadUpdated(ThreadUpdatedNotification {
                                    thread,
                                    placement: None,
                                })
                            } else {
                                GatewayNotification::TurnStarted(TurnStartedNotification {
                                    workspace_id: "ws".into(),
                                    thread_id: "a".into(),
                                    turn,
                                })
                            });
                    }
                    if self.live_reblock {
                        let mut turn = running_turn("resumable");
                        turn.status = TurnStatus::Blocked;
                        self.core
                            .apply_thread_notification(if self.snapshot_reblock {
                                let mut thread = self.snapshot.clone();
                                thread.turns = vec![turn];
                                GatewayNotification::ThreadUpdated(ThreadUpdatedNotification {
                                    thread,
                                    placement: None,
                                })
                            } else {
                                GatewayNotification::TurnBlocked(TurnBlockedNotification {
                                    workspace_id: "ws".into(),
                                    thread_id: "a".into(),
                                    turn,
                                    resume: None,
                                })
                            });
                    }
                    response
                        .send(Ok(serde_json::to_value(ThreadStartResponse {
                            thread: self.snapshot.clone(),
                            sandbox: SandboxPolicy::from_mode(SandboxMode::FullAccess),
                        })
                        .unwrap()))
                        .unwrap();
                    Ok(())
                }
            }
            if live_resume {
                snapshot.name = Some("Resumed remotely".into());
            }
            core.refresh_thread_metadata(
                &ResumeTransport {
                    core: &core,
                    snapshot: snapshot.clone(),
                    live_resume,
                    live_reblock,
                    snapshot_resume,
                    snapshot_reblock,
                },
                "a",
            )
            .unwrap();
            let acknowledged = core
                .snapshot(&scope("a"))
                .unwrap()
                .typed::<ThreadDomainSnapshot>()
                .unwrap()
                .payload();
            assert_eq!(
                acknowledged.coordinator().conversation.status_label(),
                if live_reblock { "blocked" } else { "running" }
            );
            if live_reblock {
                assert!(
                    acknowledged
                        .coordinator()
                        .conversation
                        .in_flight_turn_id()
                        .is_none()
                );
                assert!(core.begin_thread_turn_cancellation("a", None).is_none());
            }
            assert_eq!(
                acknowledged.coordinator().thread().unwrap().name,
                snapshot.name
            );
            // Deliver the matching opening echo after the RPC as well.
            core.apply_thread_notification(GatewayNotification::ThreadStarted(
                ThreadStartedNotification { thread: snapshot },
            ));
            let source = core
                .snapshot(&scope("a"))
                .unwrap()
                .typed::<ThreadDomainSnapshot>()
                .unwrap()
                .payload();
            assert_eq!(
                source.coordinator().conversation.status_label(),
                if live_reblock { "blocked" } else { "running" }
            );
            assert_eq!(
                source.coordinator().conversation.in_flight_turn_id(),
                if live_reblock {
                    None
                } else {
                    Some("resumable")
                }
            );
            if live_reblock {
                assert!(core.begin_thread_turn_cancellation("a", None).is_none());
            } else {
                let (_, cancel) = core.begin_thread_turn_cancellation("a", None).unwrap();
                assert_eq!(cancel.turn_id, "resumable");
            }
            assert_eq!(
                core.thread_registry.lock().unwrap().stores["a"].subscription_receipt,
                None,
                "metadata refresh cannot acknowledge a subscription"
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
                viewed_row_ids: Vec::new(),
                viewed_through_block: None,
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
            complete_semantic_operation(&core, &transport, request);
            let expected = match failure {
                "rpc" => 3,
                "cursor" | "sort" => 4,
                _ => CACHED_TIMELINE_REQUEST_BUDGET,
            };
            assert_eq!(transport.calls.borrow().len(), expected);
            assert_eq!(
                core.next_timeline_request_delay().is_some(),
                failure == "rpc"
            );
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
                .unwrap()
                .unwrap();
            // begin also shares the top-level status, so a boundary request must
            // preserve the failed refresh before and after its response.
            complete_semantic_operation(
                &core,
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
            complete_semantic_operation(
                &core,
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
            complete_semantic_operation(
                &core,
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
            complete_semantic_operation(&core, &transport, request);
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
            complete_semantic_operation(
                &core,
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

    fn initial_work_action() -> SemanticTimelineRequestAction {
        SemanticTimelineRequestAction::TurnWorkPage {
            key: SemanticTimelineRequestKey::TurnWorkInitial {
                thread_id: "a".into(),
                turn_id: "turn".into(),
            },
            params: TurnWorkPageParams {
                thread_id: "a".into(),
                turn_id: "turn".into(),
                anchor: TimelinePageAnchor::Newest,
                limit: Some(50),
            },
        }
    }

    fn work_delta(text: &str) -> ConversationEvent {
        ConversationEvent::ItemDelta {
            thread_id: "a".into(),
            turn_id: "turn".into(),
            item_id: "comment".into(),
            delta: text.into(),
            stream: Some(ItemDeltaStream::AgentMessage),
            payload: None,
            markdown: None,
            markdown_version: None,
        }
    }

    fn empty_work_page(core: &ClientCore) -> TurnWorkPageResponse {
        TurnWorkPageResponse {
            workspace_id: "ws".into(),
            thread_id: "a".into(),
            turn_id: "turn".into(),
            projection_version: 1,
            source_high_watermark: 1,
            projection_updated_at_unix_micros: 1,
            work: core
                .thread_semantic_snapshot("a")
                .thread("a")
                .unwrap()
                .work_range("turn")
                .unwrap()
                .work
                .clone(),
            items: vec![],
            page: Default::default(),
        }
    }

    fn retry_initial_work(core: &ClientCore) -> crate::timeline::controller::TimelineEffectPlan {
        use crate::timeline::controller::{TimelineDemand, TimelineIntent};
        let snapshot = core.thread_presentation_snapshot("a").unwrap();
        let demand = TimelineDemand {
            thread_id: "a".into(),
            consumer_id: "review-test".into(),
            generation: 1,
            source_revision: snapshot.timeline().source_revision(),
            row_ids: snapshot
                .timeline()
                .rows()
                .iter()
                .map(|row| row.id().as_str().to_owned())
                .collect(),
            threshold: 3,
            before: false,
            after: false,
            work: true,
            presented_rows: true,
            scroll_generation: 1,
            latest_user_turn_id: None,
            viewed_row_ids: Vec::new(),
            viewed_through_block: None,
            viewed_through_turn_id: None,
            read_requires_unread: false,
            prefetch_on_visibility: true,
            boundary_request_limit: 1,
        };
        let first = core.plan_timeline_intent(TimelineIntent::Update { demand }, 0);
        assert!(
            first.pages.is_empty(),
            "failure backoff applies to controller admission"
        );
        let retry = core.plan_timeline_intent(
            TimelineIntent::Retry {
                thread_id: "a".into(),
                consumer_id: "review-test".into(),
                generation: 1,
            },
            u64::MAX,
        );
        assert_eq!(retry.pages.len(), 1);
        retry
    }

    #[test]
    fn initial_work_page_collision_preserves_delta_and_retry_hydrates_canonical_body() {
        #[derive(Clone, Copy)]
        enum Scenario {
            Unchanged,
            Conflict,
            Matching,
            Superseded,
        }
        struct Transport<'a> {
            core: &'a ClientCore,
            page: TurnWorkPageResponse,
            scenario: Scenario,
            calls: std::cell::Cell<usize>,
        }
        impl crate::rpc::JsonRpcRequestTransport for Transport<'_> {
            fn send_json_rpc_request(
                &self,
                _: String,
                _: String,
                response: crate::rpc::JsonRpcResponseSender,
            ) -> Result<(), String> {
                let call = self.calls.get() + 1;
                self.calls.set(call);
                assert!(call <= 2, "no automatic reread loop");
                let mut prepared = self.page.clone();
                if call == 1 && !matches!(self.scenario, Scenario::Unchanged) {
                    if matches!(self.scenario, Scenario::Matching) {
                        let TurnItem::AgentMessage { text, .. } = &mut prepared.items[0].item
                        else {
                            panic!()
                        };
                        *text = "AB".into();
                    }
                    // The response is fixed before the same Running item's delta arrives.
                    self.core
                        .apply_thread_conversation_event("ws", work_delta("B"), None);
                    if matches!(self.scenario, Scenario::Superseded) {
                        let mut newer = prepared.items[0].clone();
                        let TurnItem::AgentMessage { text, .. } = &mut newer.item else {
                            panic!()
                        };
                        *text = "prefix AB".into();
                        newer.source_sequence = 5;
                        newer.source_updated_at_unix_micros = 5;
                        self.core
                            .apply_turn_work_items_get_response(TurnWorkItemsGetResponse {
                                workspace_id: "ws".into(),
                                thread_id: "a".into(),
                                turn_id: "turn".into(),
                                projection_version: 1,
                                source_high_watermark: 5,
                                projection_updated_at_unix_micros: 5,
                                items: vec![newer],
                                removed_work_item_ids: vec![],
                            });
                    }
                } else if call == 2 {
                    let TurnItem::AgentMessage { text, .. } = &mut prepared.items[0].item else {
                        panic!()
                    };
                    *text = "prefix AB".into();
                    prepared.items[0].source_sequence = 2;
                    prepared.source_high_watermark = 2;
                }
                response
                    .send(Ok(serde_json::to_value(prepared).unwrap()))
                    .unwrap();
                Ok(())
            }
        }
        for synchronous in [false, true] {
            for scenario in [
                Scenario::Unchanged,
                Scenario::Conflict,
                Scenario::Matching,
                Scenario::Superseded,
            ] {
                let collide = matches!(scenario, Scenario::Conflict);
                let core = core();
                core.upsert_thread(thread("a", "ws"));
                core.apply_thread_timeline_page(
                    presentation_page(&[]),
                    crate::timeline::semantic::TopLevelPageMergeMode::Reset,
                );
                core.apply_thread_conversation_event("ws", work_delta("A"), None);
                let _lease = core.subscribe(
                    ClientScope::Timeline {
                        thread_id: "a".into(),
                    },
                    NonZeroUsize::new(16).unwrap(),
                );
                let mut page = empty_work_page(&core);
                let mut canonical = core
                    .thread_semantic_snapshot("a")
                    .thread("a")
                    .unwrap()
                    .work_range("turn")
                    .unwrap()
                    .ordered_items()
                    .next()
                    .unwrap()
                    .clone();
                let TurnItem::AgentMessage { text, .. } = &mut canonical.item else {
                    panic!()
                };
                *text = "prefix A".into();
                canonical.source_sequence = 1;
                canonical.source_updated_at_unix_micros = 1;
                canonical.metadata = None;
                page.items.push(canonical);
                let (sender, receiver) = crate::threads::history_scheduler::channel();

                *core.thread_request_sender.lock().unwrap() = Some(sender);
                let transport = Transport {
                    core: &core,
                    page,
                    scenario,
                    calls: Default::default(),
                };
                if synchronous {
                    let SemanticTimelineRequestAction::TurnWorkPage { params, .. } =
                        initial_work_action()
                    else {
                        panic!()
                    };
                    assert_eq!(
                        core.fetch_turn_work_page(&transport, params).is_err(),
                        collide
                    );
                } else {
                    let request = core
                        .begin_thread_semantic_request(initial_work_action())
                        .unwrap()
                        .unwrap();
                    complete_semantic_operation(&core, &transport, request);
                }
                core.materialize_demanded_timelines();
                assert_eq!(transport.calls.get(), 1);
                assert!(core.thread_semantic_in_flight("a").is_empty());
                assert_eq!(receiver.try_iter().count(), 0);
                if collide {
                    let semantic = core.thread_semantic_snapshot("a");
                    let range = semantic.thread("a").unwrap().work_range("turn").unwrap();
                    assert!(!range.has_loaded_page);
                    assert_eq!(range.items_by_id.len(), 1);
                    assert_eq!(range.ordered_item_ids.len(), 1);
                    assert!(matches!(&range.ordered_items().next().unwrap().item,
                        TurnItem::AgentMessage { text, .. } if text == "AB"));
                    let public = core
                        .thread_presentation_snapshot("a")
                        .unwrap()
                        .timeline()
                        .serialized_header();
                    assert!(
                        public["status"]["Failed"]["message"]
                            .as_str()
                            .unwrap()
                            .contains("Work history changed")
                    );
                    let retry = retry_initial_work(&core);
                    core.schedule_planned_timeline_request(retry.pages[0].clone());
                    let ThreadControllerRequest::Semantic(request) =
                        receiver.try_recv().unwrap().value
                    else {
                        panic!()
                    };
                    complete_semantic_operation(&core, &transport, request);
                    core.materialize_demanded_timelines();
                    assert_eq!(receiver.try_iter().count(), 0);
                    assert_eq!(transport.calls.get(), 2);
                }
                let semantic = core.thread_semantic_snapshot("a");
                let range = semantic.thread("a").unwrap().work_range("turn").unwrap();
                assert!(range.has_loaded_page);
                assert_eq!(
                    range.initial_request_status,
                    crate::timeline::semantic::TimelineRequestStatus::Ready
                );
                assert_eq!(range.items_by_id.len(), 1);
                assert_eq!(range.ordered_item_ids.len(), 1);
                assert_eq!(
                    range.ordered_items().next().unwrap().status,
                    TurnWorkItemStatus::Running
                );
                let expected = match scenario {
                    Scenario::Unchanged => "prefix A",
                    Scenario::Matching => "AB",
                    Scenario::Conflict | Scenario::Superseded => "prefix AB",
                };
                assert!(matches!(&range.ordered_items().next().unwrap().item,
                    TurnItem::AgentMessage { text, .. } if text == expected));
                assert!(core.thread_semantic_in_flight("a").is_empty());
                assert_eq!(
                    core.thread_presentation_snapshot("a")
                        .unwrap()
                        .timeline()
                        .serialized_header()["status"],
                    "Ready"
                );
            }
        }
    }

    #[test]
    fn explicit_refresh_during_initial_work_loading_coalesces_one_follow_up() {
        struct Transport<'a> {
            core: &'a ClientCore,
            page: TurnWorkPageResponse,
            calls: std::cell::Cell<usize>,
        }
        impl crate::rpc::JsonRpcRequestTransport for Transport<'_> {
            fn send_json_rpc_request(
                &self,
                _: String,
                _: String,
                response: crate::rpc::JsonRpcResponseSender,
            ) -> Result<(), String> {
                let call = self.calls.get() + 1;
                self.calls.set(call);
                assert!(call <= 2);
                let semantic = self.core.thread_semantic_snapshot("a");
                assert!(matches!(
                    semantic
                        .thread("a")
                        .unwrap()
                        .work_range("turn")
                        .unwrap()
                        .initial_request_status,
                    crate::timeline::semantic::TimelineRequestStatus::Loading { .. }
                ));
                if call == 1 {
                    for _ in 0..3 {
                        self.core.refresh_thread_timeline("a");
                    }
                    let registry = self.core.thread_registry.lock().unwrap();
                    let action = initial_work_action();
                    let key = crate::timeline::semantic::semantic_timeline_request_key(&action);
                    assert!(registry.timeline.in_flight.contains(key));
                    assert!(registry.timeline.pending.contains_key(key));
                }
                response
                    .send(Ok(serde_json::to_value(&self.page).unwrap()))
                    .unwrap();
                Ok(())
            }
        }
        let core = core();
        core.upsert_thread(thread("a", "ws"));
        core.apply_thread_conversation_event("ws", work_delta("live"), None);
        let page = empty_work_page(&core);
        let request = core
            .begin_thread_semantic_request(initial_work_action())
            .unwrap()
            .unwrap();
        let (sender, receiver) = crate::threads::history_scheduler::channel();

        *core.thread_request_sender.lock().unwrap() = Some(sender);
        let transport = Transport {
            core: &core,
            page,
            calls: Default::default(),
        };
        complete_semantic_operation(&core, &transport, request);
        assert_eq!(transport.calls.get(), 2);
        let action = initial_work_action();
        let key = crate::timeline::semantic::semantic_timeline_request_key(&action);
        assert!(!core.thread_semantic_in_flight("a").contains(key));
        assert!(
            !core
                .thread_registry
                .lock()
                .unwrap()
                .timeline
                .pending
                .contains_key(key)
        );
        assert!(receiver.try_iter().all(|request| !matches!(request.value,
            ThreadControllerRequest::Semantic(request)
                if crate::timeline::semantic::semantic_timeline_request_key(&request.action) == key)));
    }

    #[test]
    fn automatic_initial_work_hydration_coalesces_and_failure_uses_existing_retry() {
        struct Transport(Result<TurnWorkPageResponse, String>);
        impl crate::rpc::JsonRpcRequestTransport for Transport {
            fn send_json_rpc_request(
                &self,
                _: String,
                _: String,
                response: crate::rpc::JsonRpcResponseSender,
            ) -> Result<(), String> {
                match &self.0 {
                    Ok(page) => {
                        response
                            .send(Ok(serde_json::to_value(page).unwrap()))
                            .unwrap();
                        Ok(())
                    }
                    Err(error) => Err(error.clone()),
                }
            }
        }
        for failed in [false, true] {
            let core = core();
            core.upsert_thread(thread("a", "ws"));
            core.apply_thread_timeline_page(
                presentation_page(&[]),
                crate::timeline::semantic::TopLevelPageMergeMode::Reset,
            );
            core.apply_thread_conversation_event("ws", work_delta("fragment"), None);
            let _lease = core.subscribe(
                ClientScope::Timeline {
                    thread_id: "a".into(),
                },
                NonZeroUsize::new(16).unwrap(),
            );
            let semantic = core.thread_semantic_snapshot("a");
            let range = semantic.thread("a").unwrap().work_range("turn").unwrap();
            assert_eq!(
                range.initial_request_status,
                crate::timeline::semantic::TimelineRequestStatus::Idle
            );
            assert!(core.thread_semantic_in_flight("a").is_empty());
            let (sender, receiver) = crate::threads::history_scheduler::channel();

            *core.thread_request_sender.lock().unwrap() = Some(sender);
            core.apply_thread_conversation_event("ws", work_delta(" more"), None);
            core.materialize_demanded_timelines();
            let ThreadControllerRequest::Semantic(request) = receiver.try_recv().unwrap().value
            else {
                panic!("expected initial work request");
            };
            assert!(matches!(
                &request.action,
                SemanticTimelineRequestAction::TurnWorkPage {
                    key: SemanticTimelineRequestKey::TurnWorkInitial { .. },
                    ..
                }
            ));
            for _ in 0..3 {
                core.apply_thread_conversation_event("ws", work_delta(" live"), None);
                core.materialize_demanded_timelines();
                assert_eq!(receiver.try_iter().count(), 0);
                assert!(
                    core.accept_timeline_request(
                        initial_work_action(),
                        None,
                        Some(core.thread_operation_token("a").unwrap().generation),
                        false,
                        true,
                    )
                    .unwrap()
                    .is_none()
                );
            }
            let page = empty_work_page(&core);
            complete_semantic_operation(
                &core,
                &Transport(if failed {
                    Err("synthetic page failure".into())
                } else {
                    Ok(page.clone())
                }),
                request,
            );
            assert!(core.thread_semantic_in_flight("a").is_empty());
            for _ in 0..3 {
                core.apply_thread_conversation_event("ws", work_delta(" later"), None);
                core.materialize_demanded_timelines();
                assert_eq!(receiver.try_iter().count(), 0);
            }
            let semantic = core.thread_semantic_snapshot("a");
            let range = semantic.thread("a").unwrap().work_range("turn").unwrap();
            assert_eq!(range.has_loaded_page, !failed);
            let public = core
                .thread_presentation_snapshot("a")
                .unwrap()
                .timeline()
                .serialized_header();
            assert_eq!(public["has_loaded_page"], true);
            if failed {
                assert!(matches!(
                    range.request_status,
                    crate::timeline::semantic::TimelineRequestStatus::Failed { .. }
                ));
                assert!(
                    public["status"]["Failed"]["message"]
                        .as_str()
                        .unwrap()
                        .contains("synthetic page failure")
                );
                core.set_thread_turn_work_expanded("a", "turn", false);
                core.materialize_demanded_timelines();
                assert_eq!(
                    core.thread_presentation_snapshot("a")
                        .unwrap()
                        .timeline()
                        .serialized_header()["status"],
                    "Ready"
                );
                core.set_thread_turn_work_expanded("a", "turn", true);
                core.materialize_demanded_timelines();
                let retry = retry_initial_work(&core);
                core.schedule_planned_timeline_request(retry.pages[0].clone());
                let ThreadControllerRequest::Semantic(request) = receiver.try_recv().unwrap().value
                else {
                    panic!()
                };
                complete_semantic_operation(&core, &Transport(Ok(page)), request);
                let semantic = core.thread_semantic_snapshot("a");
                assert!(
                    semantic
                        .thread("a")
                        .unwrap()
                        .work_range("turn")
                        .unwrap()
                        .has_loaded_page
                );
                assert!(core.thread_semantic_in_flight("a").is_empty());
                core.materialize_demanded_timelines();
                assert_eq!(
                    core.thread_presentation_snapshot("a")
                        .unwrap()
                        .timeline()
                        .serialized_header()["status"],
                    "Ready"
                );
            } else {
                assert_eq!(public["status"], "Ready");
            }
        }
    }

    #[test]
    fn initial_work_failure_survives_neighbor_updates_until_page_retry() {
        use crate::timeline::semantic::{SemanticTimelineLiveUpdate, TimelineRequestStatus};
        struct Transport<'a> {
            result: Result<serde_json::Value, String>,
            calls: &'a std::cell::Cell<usize>,
        }
        impl crate::rpc::JsonRpcRequestTransport for Transport<'_> {
            fn send_json_rpc_request(
                &self,
                _: String,
                _: String,
                response: crate::rpc::JsonRpcResponseSender,
            ) -> Result<(), String> {
                self.calls.set(self.calls.get() + 1);
                match &self.result {
                    Ok(value) => {
                        response.send(Ok(value.clone())).unwrap();
                        Ok(())
                    }
                    Err(error) => Err(error.clone()),
                }
            }
        }
        for items_get in [false, true] {
            let core = core();
            core.upsert_thread(thread("a", "ws"));
            core.apply_thread_timeline_page(
                presentation_page(&[]),
                crate::timeline::semantic::TopLevelPageMergeMode::Reset,
            );
            core.apply_thread_conversation_event("ws", work_delta("A"), None);
            let mut removed = work_delta("removed");
            let ConversationEvent::ItemDelta { item_id, .. } = &mut removed else {
                panic!()
            };
            *item_id = "removed".into();
            core.apply_thread_conversation_event("ws", removed, None);
            let _lease = core.subscribe(
                ClientScope::Timeline {
                    thread_id: "a".into(),
                },
                NonZeroUsize::new(16).unwrap(),
            );
            let semantic = core.thread_semantic_snapshot("a");
            let range = semantic.thread("a").unwrap().work_range("turn").unwrap();
            let mut updated = range
                .ordered_items()
                .find(|item| item.item_id == "comment")
                .unwrap()
                .clone();
            let removed_id = range
                .ordered_items()
                .find(|item| item.item_id == "removed")
                .unwrap()
                .work_item_id
                .clone();
            let work_item_id = updated.work_item_id.clone();
            let TurnItem::AgentMessage { text, .. } = &mut updated.item else {
                panic!()
            };
            *text = "neighbor canonical".into();
            updated.source_sequence = 5;
            updated.source_updated_at_unix_micros = 5;
            updated.metadata = None;
            let (sender, receiver) = crate::threads::history_scheduler::channel();

            *core.thread_request_sender.lock().unwrap() = Some(sender);
            let calls = std::cell::Cell::new(0);
            let request = core
                .begin_thread_semantic_request(initial_work_action())
                .unwrap()
                .unwrap();
            complete_semantic_operation(
                &core,
                &Transport {
                    result: Err("initial history unavailable".into()),
                    calls: &calls,
                },
                request,
            );
            let failure = core
                .thread_semantic_snapshot("a")
                .thread("a")
                .unwrap()
                .work_range("turn")
                .unwrap()
                .initial_request_status
                .clone();
            assert!(
                matches!(&failure, TimelineRequestStatus::Failed { message } if message.contains("initial history unavailable"))
            );
            let public_failure = serde_json::to_value(&failure).unwrap();
            core.set_thread_turn_work_expanded("a", "turn", false);
            core.materialize_demanded_timelines();
            assert_eq!(
                core.thread_presentation_snapshot("a")
                    .unwrap()
                    .timeline()
                    .serialized_header()["status"],
                "Ready"
            );
            let semantic = core.thread_semantic_snapshot("a");
            let range = semantic.thread("a").unwrap().work_range("turn").unwrap();
            assert!(!range.has_loaded_page);
            assert_eq!(range.initial_request_status, failure);
            core.set_thread_turn_work_expanded("a", "turn", true);
            core.materialize_demanded_timelines();
            assert_eq!(
                core.thread_presentation_snapshot("a")
                    .unwrap()
                    .timeline()
                    .serialized_header()["status"],
                public_failure
            );
            if items_get {
                let request = core
                    .begin_thread_semantic_request(
                        SemanticTimelineRequestAction::TurnWorkItemsGet {
                            key: SemanticTimelineRequestKey::TurnWorkItems {
                                thread_id: "a".into(),
                                turn_id: "turn".into(),
                            },
                            params: TurnWorkItemsGetParams {
                                thread_id: "a".into(),
                                turn_id: "turn".into(),
                                work_item_ids: vec![work_item_id.clone(), removed_id.clone()],
                            },
                        },
                    )
                    .unwrap()
                    .unwrap();
                let response = TurnWorkItemsGetResponse {
                    workspace_id: "ws".into(),
                    thread_id: "a".into(),
                    turn_id: "turn".into(),
                    projection_version: 1,
                    source_high_watermark: 5,
                    projection_updated_at_unix_micros: 5,
                    items: vec![updated.clone()],
                    removed_work_item_ids: vec![removed_id.clone()],
                };
                complete_semantic_operation(
                    &core,
                    &Transport {
                        result: Ok(serde_json::to_value(response).unwrap()),
                        calls: &calls,
                    },
                    request,
                );
            } else {
                core.apply_thread_semantic_live_update(
                    SemanticTimelineLiveUpdate::TurnWorkItemsChanged(
                        TurnWorkItemsChangedNotification {
                            workspace_id: "ws".into(),
                            thread_id: "a".into(),
                            turn_id: "turn".into(),
                            changed_work_item_ids: vec![work_item_id.clone()],
                            removed_work_item_ids: vec![removed_id.clone()],
                            before_cursor: None,
                            after_cursor: None,
                            reason: TimelineChangeReason::LiveEvent,
                        },
                    ),
                );
            }
            let semantic = core.thread_semantic_snapshot("a");
            let range = semantic.thread("a").unwrap().work_range("turn").unwrap();
            assert!(!range.has_loaded_page);
            assert_eq!(range.initial_request_status, failure);
            assert_eq!(range.request_status, TimelineRequestStatus::Ready);
            assert!(range.item(&removed_id).is_none());
            if items_get {
                assert_eq!(range.item(&work_item_id), Some(&updated));
            } else {
                assert!(range.stale_work_item_ids.contains(&work_item_id));
            }
            for _ in 0..3 {
                core.apply_thread_conversation_event("ws", work_delta(" later"), None);
                core.materialize_demanded_timelines();
                let public = core
                    .thread_presentation_snapshot("a")
                    .unwrap()
                    .timeline()
                    .serialized_header();
                assert_eq!(public["status"], public_failure);
                let semantic = core.thread_semantic_snapshot("a");
                let range = semantic.thread("a").unwrap().work_range("turn").unwrap();
                assert!(!range.has_loaded_page);
                assert_eq!(range.initial_request_status, failure);
                assert_eq!(receiver.try_iter().count(), 0);
            }
            let retry = retry_initial_work(&core);
            core.schedule_planned_timeline_request(retry.pages[0].clone());
            let semantic = core.thread_semantic_snapshot("a");
            let range = semantic.thread("a").unwrap().work_range("turn").unwrap();
            assert!(!range.has_loaded_page);
            assert!(matches!(
                range.initial_request_status,
                TimelineRequestStatus::Loading { .. }
            ));
            let mut page = empty_work_page(&core);
            page.source_high_watermark = 10;
            page.projection_updated_at_unix_micros = 10;
            let ThreadControllerRequest::Semantic(request) = receiver.try_recv().unwrap().value
            else {
                panic!()
            };
            complete_semantic_operation(
                &core,
                &Transport {
                    result: Ok(serde_json::to_value(page).unwrap()),
                    calls: &calls,
                },
                request,
            );
            core.materialize_demanded_timelines();
            let semantic = core.thread_semantic_snapshot("a");
            let range = semantic.thread("a").unwrap().work_range("turn").unwrap();
            assert!(range.has_loaded_page);
            assert_eq!(range.initial_request_status, TimelineRequestStatus::Ready);
            assert_eq!(range.items_by_id.len(), 1);
            assert!(range.item(&removed_id).is_none());
            let expected = if items_get {
                "neighbor canonical later later later"
            } else {
                "A later later later"
            };
            assert!(
                matches!(&range.item(&work_item_id).unwrap().item, TurnItem::AgentMessage { text, .. } if text == expected)
            );
            assert_eq!(
                core.thread_presentation_snapshot("a")
                    .unwrap()
                    .timeline()
                    .serialized_header()["status"],
                "Ready"
            );
            assert_eq!(calls.get(), if items_get { 3 } else { 2 });
            assert_eq!(receiver.try_iter().count(), 0);
            assert!(core.thread_semantic_in_flight("a").is_empty());
        }
    }

    #[test]
    fn late_initial_work_page_cannot_hydrate_a_replacement_owner() {
        struct ReplacingTransport<'a> {
            core: &'a ClientCore,
            page: TurnWorkPageResponse,
        }
        impl crate::rpc::JsonRpcRequestTransport for ReplacingTransport<'_> {
            fn send_json_rpc_request(
                &self,
                _: String,
                _: String,
                response: crate::rpc::JsonRpcResponseSender,
            ) -> Result<(), String> {
                self.core.remove_thread_store("a");
                self.core.upsert_thread(thread("a", "ws"));
                self.core
                    .apply_thread_conversation_event("ws", work_delta("replacement"), None);
                response
                    .send(Ok(serde_json::to_value(&self.page).unwrap()))
                    .unwrap();
                Ok(())
            }
        }
        let core = core();
        core.upsert_thread(thread("a", "ws"));
        core.apply_thread_conversation_event("ws", work_delta("old"), None);
        let page = empty_work_page(&core);
        let request = core
            .begin_thread_semantic_request(initial_work_action())
            .unwrap()
            .unwrap();
        let old_generation = request.generation;
        complete_semantic_operation(&core, &ReplacingTransport { core: &core, page }, request);
        assert_ne!(
            core.thread_operation_token("a").unwrap().generation,
            old_generation
        );
        let semantic = core.thread_semantic_snapshot("a");
        let range = semantic.thread("a").unwrap().work_range("turn").unwrap();
        assert!(!range.has_loaded_page);
        assert_eq!(
            range.request_status,
            crate::timeline::semantic::TimelineRequestStatus::Idle
        );
        assert_eq!(
            range.initial_request_status,
            crate::timeline::semantic::TimelineRequestStatus::Idle
        );
        assert!(matches!(&range.ordered_items().next().unwrap().item,
            TurnItem::AgentMessage { text, .. } if text == "replacement"));
        core.thread_demand_changed(
            &ClientScope::Timeline {
                thread_id: "a".into(),
            },
            ClientDemand::Visible,
        );
        assert!(
            core.accept_timeline_request(
                initial_work_action(),
                None,
                Some(old_generation),
                false,
                true
            )
            .unwrap()
            .is_none()
        );
        let owner = core.thread_operation_token("a").unwrap().generation;
        core.set_thread_turn_work_expanded("a", "turn", false);
        assert!(
            core.accept_timeline_request(initial_work_action(), None, Some(owner), false, true)
                .unwrap()
                .is_none()
        );
        core.set_thread_turn_work_expanded("a", "turn", true);
        assert!(
            core.accept_timeline_request(initial_work_action(), None, Some(owner), false, true)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn exact_work_refresh_batches_ids_without_changing_scope_or_reducing_in_the_shell() {
        struct Transport<'a>(&'a ClientCore, std::cell::RefCell<Vec<usize>>);
        impl crate::rpc::JsonRpcRequestTransport for Transport<'_> {
            fn send_json_rpc_request(
                &self,
                _: String,
                payload: String,
                response: crate::rpc::JsonRpcResponseSender,
            ) -> Result<(), String> {
                let request: serde_json::Value = serde_json::from_str(&payload).unwrap();
                let first = self.1.borrow().is_empty();
                self.1
                    .borrow_mut()
                    .push(request["params"]["workItemIds"].as_array().unwrap().len());
                if first {
                    for ids in [vec!["item-0"], vec!["item-0"], vec!["new-item"]] {
                        self.0.apply_thread_notification(
                            GatewayNotification::TurnWorkItemsChanged(
                                TurnWorkItemsChangedNotification {
                                    workspace_id: "ws".into(),
                                    thread_id: "a".into(),
                                    turn_id: "turn".into(),
                                    changed_work_item_ids: ids
                                        .into_iter()
                                        .map(str::to_owned)
                                        .collect(),
                                    removed_work_item_ids: vec![],
                                    before_cursor: None,
                                    after_cursor: None,
                                    reason: TimelineChangeReason::LiveEvent,
                                },
                            ),
                        );
                    }
                } else if self.1.borrow().len() == 4 {
                    assert_eq!(
                        request["params"]["workItemIds"],
                        serde_json::json!(["item-0", "new-item"])
                    );
                }
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
            .unwrap()
            .unwrap();
        let (sender, receiver) = crate::threads::history_scheduler::channel();

        *core.thread_request_sender.lock().unwrap() = Some(sender);
        let transport = Transport(&core, Default::default());
        let request = core
            .execute_thread_semantic_request(&transport, request)
            .unwrap();
        assert_eq!(*transport.1.borrow(), [200]);
        assert_eq!(core.thread_semantic_in_flight("a").len(), 1);
        let request = core
            .execute_thread_semantic_request(&transport, request)
            .unwrap();
        assert_eq!(*transport.1.borrow(), [200, 200]);
        let request = core
            .execute_thread_semantic_request(&transport, request)
            .unwrap();
        assert_eq!(*transport.1.borrow(), [200, 200, 5]);
        // The required trailing IDs remain one logical operation and obtain a
        // fresh scheduler selection before their own network step.
        assert!(
            core.execute_thread_semantic_request(&transport, request)
                .is_none()
        );
        assert_eq!(*transport.1.borrow(), [200, 200, 5, 2]);
        assert_eq!(receiver.try_iter().count(), 0);
        assert!(core.thread_semantic_in_flight("a").is_empty());
    }

    #[test]
    fn work_chunks_preserve_aggregate_failure_capacity_and_live_authority_between_quanta() {
        struct Transport<'a> {
            replies: std::cell::RefCell<Vec<Result<TurnWorkItemsGetResponse, String>>>,
            calls: std::cell::Cell<usize>,
            before_last_reply: Option<Box<dyn Fn() + 'a>>,
        }
        impl crate::rpc::JsonRpcRequestTransport for Transport<'_> {
            fn send_json_rpc_request(
                &self,
                _: String,
                _: String,
                response: crate::rpc::JsonRpcResponseSender,
            ) -> Result<(), String> {
                self.calls.set(self.calls.get() + 1);
                let page = self.replies.borrow_mut().remove(0)?;
                if self.calls.get() == 2
                    && let Some(callback) = &self.before_last_reply
                {
                    callback();
                }
                response
                    .send(Ok(serde_json::to_value(page).unwrap()))
                    .unwrap();
                Ok(())
            }
        }
        for outcome in [
            "success",
            "rpc",
            "capacity",
            "cancel",
            "replace",
            "live-edit",
            "live-remove",
            "absent",
            "absent-remove",
            "absent-new-generation",
            "absent-replace",
            "absent-evidence-full",
            "absent-tombstone",
            "absent-tombstone-rpc",
            "absent-tombstone-dispatch",
            "absent-tombstone-cancel",
            "absent-tombstone-reauthorized",
            "absent-tombstone-inflight-reauthorized",
            "absent-tombstone-work-page-reauthorized",
            "absent-tombstone-replace",
        ] {
            let core = core();
            core.upsert_thread(thread("a", "ws"));
            let mut original = run_action();
            original.work_item_id = "item-0".into();
            original.turn_id = "turn".into();
            original.metadata = Some(serde_json::json!("original"));
            let page = |items| TurnWorkItemsGetResponse {
                workspace_id: "ws".into(),
                thread_id: "a".into(),
                turn_id: "turn".into(),
                projection_version: 1,
                source_high_watermark: 20,
                projection_updated_at_unix_micros: 20_000,
                items,
                removed_work_item_ids: vec![],
            };
            let absent = outcome.starts_with("absent");
            let tombstoned = outcome.starts_with("absent-tombstone");
            let in_flight_reauthorization = outcome == "absent-tombstone-inflight-reauthorized";
            let work_page_reauthorization = outcome == "absent-tombstone-work-page-reauthorized";
            let reauthorized_before_completion =
                in_flight_reauthorization || work_page_reauthorization;
            core.apply_turn_work_items_get_response(page(if absent {
                vec![]
            } else {
                vec![original.clone()]
            }));
            let ids = if outcome == "absent-evidence-full" {
                std::iter::once("item-0".to_owned())
                    .chain((1..201).map(|ix| format!("item-{ix}-{}", "x".repeat(1024))))
                    .collect::<Vec<_>>()
            } else {
                (0..201).map(|ix| format!("item-{ix}")).collect()
            };
            let request = core
                .begin_thread_semantic_request(SemanticTimelineRequestAction::TurnWorkItemsGet {
                    key: SemanticTimelineRequestKey::TurnWorkItems {
                        thread_id: "a".into(),
                        turn_id: "turn".into(),
                    },
                    params: TurnWorkItemsGetParams {
                        thread_id: "a".into(),
                        turn_id: "turn".into(),
                        work_item_ids: ids.clone(),
                    },
                })
                .unwrap()
                .unwrap();
            let key =
                crate::timeline::semantic::semantic_timeline_request_key(&request.action).clone();
            let generation = request.request_generation;
            let mut incoming = original.clone();
            incoming.metadata = Some(serde_json::json!(if outcome == "capacity" {
                "x".repeat(SEMANTIC_CONTINUATION_BYTE_LIMIT / 2)
            } else {
                "fetched".into()
            }));
            let mut peer = run_action();
            peer.work_item_id = "item-99".into();
            peer.item_id = "peer-source".into();
            peer.turn_id = "turn".into();
            let mut second = if matches!(outcome, "rpc" | "absent-tombstone-rpc") {
                Err("late chunk failure".into())
            } else if outcome == "capacity" {
                Ok(page(vec![incoming.clone()]))
            } else {
                Ok(page(if absent && outcome != "absent-evidence-full" {
                    vec![peer.clone()]
                } else {
                    vec![]
                }))
            };
            if in_flight_reauthorization && let Ok(page) = &mut second {
                page.source_high_watermark = 32;
                page.projection_updated_at_unix_micros = 32_000;
            }
            let remove_range = || {
                if work_page_reauthorization {
                    struct RemovalPage(pioneer_protocol::TurnWorkPageResponse);
                    impl crate::rpc::JsonRpcRequestTransport for RemovalPage {
                        fn send_json_rpc_request(
                            &self,
                            _: String,
                            _: String,
                            response: crate::rpc::JsonRpcResponseSender,
                        ) -> Result<(), String> {
                            response
                                .send(Ok(serde_json::to_value(&self.0).unwrap()))
                                .unwrap();
                            Ok(())
                        }
                    }
                    let page_request = core
                        .begin_thread_semantic_request(
                            SemanticTimelineRequestAction::TurnWorkPage {
                                key: SemanticTimelineRequestKey::TurnWorkInitial {
                                    thread_id: "a".into(),
                                    turn_id: "turn".into(),
                                },
                                params: TurnWorkPageParams {
                                    thread_id: "a".into(),
                                    turn_id: "turn".into(),
                                    anchor: TimelinePageAnchor::Newest,
                                    limit: Some(12),
                                },
                            },
                        )
                        .unwrap()
                        .unwrap();
                    let page = RemovalPage(TurnWorkPageResponse {
                        workspace_id: "ws".into(),
                        thread_id: "a".into(),
                        turn_id: "turn".into(),
                        projection_version: 1,
                        source_high_watermark: 30,
                        projection_updated_at_unix_micros: 30_000,
                        work: None,
                        items: vec![],
                        page: Default::default(),
                    });
                    assert!(
                        core.execute_thread_semantic_request(&page, page_request)
                            .is_none()
                    );
                } else {
                    let GatewayNotification::ThreadTimelineBlocksChanged(mut notification) =
                        reconciliation_notification(&[])
                    else {
                        unreachable!()
                    };
                    notification.removed_block_ids = vec!["turn:turn:work".into()];
                    notification.turn_work_tombstones = vec![pioneer_protocol::TurnWorkTombstone {
                        turn_id: "turn".into(),
                        block_id: "turn:turn:work".into(),
                        source_high_watermark: 30,
                        projection_updated_at_unix_micros: 30_000,
                    }];
                    assert!(core.apply_thread_semantic_live_update(
                    crate::timeline::semantic::SemanticTimelineLiveUpdate::ThreadTimelineBlocksChanged(notification)
                ));
                }
                assert!(
                    core.thread_semantic_snapshot("a")
                        .thread("a")
                        .unwrap()
                        .work_range("turn")
                        .is_none()
                );
                assert!(
                    core.begin_thread_semantic_request(
                        SemanticTimelineRequestAction::TurnWorkItemsGet {
                            key: key.clone(),
                            params: TurnWorkItemsGetParams {
                                thread_id: "a".into(),
                                turn_id: "turn".into(),
                                work_item_ids: vec!["blocked-required".into()],
                            },
                        }
                    )
                    .is_err(),
                    "admission behind current authority barrier must refuse whole action"
                );
            };
            let reauthorize = || {
                let mut work = run_work();
                work.turn_id = "turn".into();
                assert!(core.apply_thread_semantic_live_update(
                    crate::timeline::semantic::SemanticTimelineLiveUpdate::TurnWorkStateChanged(
                        pioneer_protocol::TurnWorkStateChangedNotification {
                            workspace_id: "ws".into(),
                            thread_id: "a".into(),
                            turn_id: "turn".into(),
                            source_high_watermark: 31,
                            projection_updated_at_unix_micros: 31_000,
                            work,
                            reason: TimelineChangeReason::LiveEvent,
                        }
                    )
                ));
            };
            let fresh_request = std::cell::RefCell::new(None);
            let fresh_loading = std::cell::RefCell::new(None);
            let late_old = reauthorized_before_completion.then(|| ThreadSemanticRequest {
                id: request.id.clone(),
                generation: request.generation,
                request_generation: request.request_generation,
                explicit_intent: request.explicit_intent,
                recovery: request.recovery,
                action: request.action.clone(),
                reconciliation: None,
                work_items: WorkItemsContinuation::default(),
            });
            let admit_fresh = || {
                let next = core
                    .begin_thread_semantic_request(
                        SemanticTimelineRequestAction::TurnWorkItemsGet {
                            key: key.clone(),
                            params: TurnWorkItemsGetParams {
                                thread_id: "a".into(),
                                turn_id: "turn".into(),
                                work_item_ids: vec!["item-0".into(), "fresh-required".into()],
                            },
                        },
                    )
                    .unwrap()
                    .unwrap();
                assert_ne!(next.request_generation, generation);
                *fresh_request.borrow_mut() = Some(next);
                let loading = core.thread_snapshot("a").unwrap();
                assert!(matches!(
                    &loading
                        .semantic
                        .thread("a")
                        .unwrap()
                        .work_range("turn")
                        .unwrap()
                        .request_status,
                    crate::timeline::semantic::TimelineRequestStatus::Loading { .. }
                ));
                *fresh_loading.borrow_mut() = Some(loading);
            };
            let transport = Transport {
                replies: std::cell::RefCell::new(vec![Ok(page(vec![incoming.clone()])), second]),
                calls: std::cell::Cell::new(0),
                before_last_reply: in_flight_reauthorization.then(|| {
                    Box::new(|| {
                        remove_range();
                        reauthorize();
                        admit_fresh();
                    }) as Box<dyn Fn() + '_>
                }),
            };
            if tombstoned {
                assert!(
                    core.begin_thread_semantic_request(
                        SemanticTimelineRequestAction::TurnWorkItemsGet {
                            key: key.clone(),
                            params: TurnWorkItemsGetParams {
                                thread_id: "a".into(),
                                turn_id: "turn".into(),
                                work_item_ids: vec!["trailing-old".into()],
                            },
                        }
                    )
                    .unwrap()
                    .is_none()
                );
            }
            let request = core
                .execute_thread_semantic_request(&transport, request)
                .unwrap();
            assert_eq!(transport.calls.get(), 1, "one RPC per quantum");
            let before_apply = core.thread_semantic_snapshot("a");
            assert_eq!(
                before_apply
                    .thread("a")
                    .unwrap()
                    .work_range("turn")
                    .unwrap()
                    .items_by_id
                    .get("item-0"),
                if absent { None } else { Some(&original) },
                "the first successful chunk must not partially apply"
            );
            if absent && outcome != "absent" && !tombstoned {
                // Another response can make the range Ready without caching X.
                // The relevant removal below then returns semantic changed=false.
                core.apply_turn_work_items_get_response(page(vec![]));
                let removed = if outcome == "absent-evidence-full" {
                    ids.clone()
                } else {
                    vec!["item-0".into()]
                };
                assert!(
                    !core.apply_thread_semantic_live_update(
                        crate::timeline::semantic::SemanticTimelineLiveUpdate::TurnWorkItemsChanged(
                            TurnWorkItemsChangedNotification {
                                workspace_id: "ws".into(),
                                thread_id: "a".into(),
                                turn_id: "turn".into(),
                                changed_work_item_ids: vec![],
                                removed_work_item_ids: removed,
                                before_cursor: None,
                                after_cursor: None,
                                reason: TimelineChangeReason::LiveEvent,
                            }
                        )
                    ),
                    "None-to-None removal still guards the active collection"
                );
            }
            if tombstoned && !in_flight_reauthorization {
                remove_range();
            }
            if work_page_reauthorization {
                reauthorize();
                admit_fresh();
            }
            let mut replacement_ready = None;
            let mut live = original.clone();
            live.metadata = Some(serde_json::json!("live"));
            match outcome {
                "cancel" | "absent-tombstone-cancel" => core.cancel_thread_requests(),
                "replace" | "absent-replace" => {
                    core.remove_thread_store("a");
                    core.upsert_thread(thread("a", "ws"));
                }
                "absent-tombstone-replace" => {
                    core.remove_thread_store("a");
                    core.upsert_thread(thread("a", "ws"));
                    let replacement = core
                        .begin_thread_semantic_request(
                            SemanticTimelineRequestAction::TurnWorkItemsGet {
                                key: key.clone(),
                                params: TurnWorkItemsGetParams {
                                    thread_id: "a".into(),
                                    turn_id: "turn".into(),
                                    work_item_ids: vec!["item-0".into()],
                                },
                            },
                        )
                        .unwrap()
                        .unwrap();
                    let fresh = Transport {
                        replies: std::cell::RefCell::new(vec![Ok(page(vec![incoming.clone()]))]),
                        calls: std::cell::Cell::new(0),
                        before_last_reply: None,
                    };
                    assert!(
                        core.execute_thread_semantic_request(&fresh, replacement)
                            .is_none()
                    );
                    replacement_ready = core.thread_snapshot("a");
                }
                "live-edit" => {
                    core.apply_turn_work_items_get_response(page(vec![live.clone()]));
                }
                "live-remove" => {
                    core.apply_thread_semantic_live_update(
                        crate::timeline::semantic::SemanticTimelineLiveUpdate::TurnWorkItemsChanged(
                            TurnWorkItemsChangedNotification {
                                workspace_id: "ws".into(),
                                thread_id: "a".into(),
                                turn_id: "turn".into(),
                                changed_work_item_ids: vec![],
                                removed_work_item_ids: vec!["item-0".into()],
                                before_cursor: None,
                                after_cursor: None,
                                reason: TimelineChangeReason::LiveEvent,
                            },
                        ),
                    );
                }
                _ => {}
            }
            if outcome == "absent-tombstone-dispatch" {
                assert!(core.send_timeline_request(request).is_err());
            } else {
                assert!(
                    core.execute_thread_semantic_request(&transport, request)
                        .is_none()
                );
            }
            assert_eq!(
                transport.calls.get(),
                if matches!(
                    outcome,
                    "cancel"
                        | "replace"
                        | "absent-replace"
                        | "absent-tombstone-replace"
                        | "absent-tombstone-dispatch"
                        | "absent-tombstone-cancel"
                        | "absent-tombstone"
                        | "absent-tombstone-rpc"
                        | "absent-tombstone-reauthorized"
                        | "absent-tombstone-work-page-reauthorized"
                ) {
                    1
                } else {
                    2
                }
            );
            let semantic = core.thread_semantic_snapshot("a");
            let range = semantic.thread("a").unwrap().work_range("turn");
            let actual = range.and_then(|range| range.items_by_id.get("item-0"));
            match outcome {
                "success" | "absent" | "absent-tombstone-replace" => {
                    assert_eq!(actual, Some(&incoming))
                }
                "live-edit" => assert_eq!(actual, Some(&live)),
                "replace"
                | "live-remove"
                | "absent-remove"
                | "absent-new-generation"
                | "absent-replace"
                | "absent-evidence-full"
                | "absent-tombstone"
                | "absent-tombstone-rpc"
                | "absent-tombstone-dispatch"
                | "absent-tombstone-cancel"
                | "absent-tombstone-reauthorized"
                | "absent-tombstone-inflight-reauthorized"
                | "absent-tombstone-work-page-reauthorized" => assert!(actual.is_none()),
                _ => assert_eq!(actual, Some(&original)),
            }
            if matches!(outcome, "rpc" | "capacity" | "absent-evidence-full") {
                assert!(matches!(
                    range.unwrap().request_status,
                    crate::timeline::semantic::TimelineRequestStatus::Failed { .. }
                ));
            }
            if tombstoned {
                if let Some(ready) = replacement_ready {
                    assert!(
                        Arc::ptr_eq(&ready, &core.thread_snapshot("a").unwrap()),
                        "late old collection must not publish over replacement"
                    );
                } else if reauthorized_before_completion {
                    assert!(
                        Arc::ptr_eq(
                            fresh_loading.borrow().as_ref().unwrap(),
                            &core.thread_snapshot("a").unwrap()
                        ),
                        "old completion cannot change the newly authorized summary/Loading publication"
                    );
                } else {
                    assert!(
                        range.is_none(),
                        "neither success nor error cleanup may recreate the tombstoned range"
                    );
                }
                let registry = core.thread_registry.lock().unwrap();
                assert!(!registry.timeline.accepts(&key, generation));
                assert!(
                    registry
                        .timeline
                        .request_removals(&key, generation)
                        .is_none()
                );
                assert!(!registry.timeline.pending.contains_key(&key));
            }
            if absent
                && !tombstoned
                && !matches!(outcome, "absent-evidence-full" | "absent-replace")
            {
                assert_eq!(
                    range.unwrap().items_by_id.get("item-99"),
                    Some(&peer),
                    "the other required IDs complete as the same successful collection"
                );
            }
            if matches!(
                outcome,
                "absent-new-generation" | "absent-replace" | "absent-tombstone-reauthorized"
            ) {
                if outcome == "absent-tombstone-reauthorized" {
                    reauthorize();
                }
                let next = core
                    .begin_thread_semantic_request(
                        SemanticTimelineRequestAction::TurnWorkItemsGet {
                            key: SemanticTimelineRequestKey::TurnWorkItems {
                                thread_id: "a".into(),
                                turn_id: "turn".into(),
                            },
                            params: TurnWorkItemsGetParams {
                                thread_id: "a".into(),
                                turn_id: "turn".into(),
                                work_item_ids: vec!["item-0".into()],
                            },
                        },
                    )
                    .unwrap()
                    .unwrap();
                let mut fresh_page = page(vec![incoming.clone()]);
                fresh_page.source_high_watermark = 32;
                fresh_page.projection_updated_at_unix_micros = 32_000;
                let fresh = Transport {
                    replies: std::cell::RefCell::new(vec![Ok(fresh_page)]),
                    calls: std::cell::Cell::new(0),
                    before_last_reply: None,
                };
                assert!(core.execute_thread_semantic_request(&fresh, next).is_none());
                assert_eq!(
                    core.thread_semantic_snapshot("a")
                        .thread("a")
                        .unwrap()
                        .work_range("turn")
                        .unwrap()
                        .items_by_id
                        .get("item-0"),
                    Some(&incoming),
                    "old generation removal evidence must not poison a new operation/store"
                );
            }
            if reauthorized_before_completion {
                let next = fresh_request.borrow_mut().take().unwrap();
                let mut fresh_item = incoming.clone();
                fresh_item.metadata = Some(serde_json::json!("fresh generation"));
                let mut fresh_peer = peer.clone();
                fresh_peer.work_item_id = "fresh-required".into();
                let mut response = page(vec![fresh_item.clone(), fresh_peer.clone()]);
                response.source_high_watermark = 32;
                response.projection_updated_at_unix_micros = 32_000;
                let fresh = Transport {
                    replies: std::cell::RefCell::new(vec![Ok(response)]),
                    calls: std::cell::Cell::new(0),
                    before_last_reply: None,
                };
                assert!(core.execute_thread_semantic_request(&fresh, next).is_none());
                let ready = core.thread_snapshot("a").unwrap();
                let range = ready
                    .semantic
                    .thread("a")
                    .unwrap()
                    .work_range("turn")
                    .unwrap();
                assert_eq!(range.items_by_id.get("item-0"), Some(&fresh_item));
                assert_eq!(range.items_by_id.get("fresh-required"), Some(&fresh_peer));
                assert!(!range.items_by_id.contains_key("item-99"));
                assert!(matches!(
                    &range.request_status,
                    crate::timeline::semantic::TimelineRequestStatus::Ready
                ));
                let old = late_old.unwrap();
                core.fail_timeline_dispatch(&old);
                assert!(
                    core.execute_thread_semantic_request(&transport, old)
                        .is_none()
                );
                assert_eq!(
                    transport.calls.get(),
                    if work_page_reauthorization { 1 } else { 2 },
                    "retired token cannot issue another RPC"
                );
                assert!(
                    Arc::ptr_eq(&ready, &core.thread_snapshot("a").unwrap()),
                    "late old cleanup must not alter fresh data/status/publication"
                );
            }
            assert!(core.thread_semantic_in_flight("a").is_empty());
        }
    }

    #[test]
    fn cached_walk_missing_baseline_respects_removal_without_poisoning_new_generations() {
        use crate::timeline::semantic::TimelineRequestStatus;
        for outcome in [
            "new",
            "during-rpc",
            "overlap",
            "wrong-scope",
            "evidence-full",
        ] {
            let (core, _receiver) = reconciliation_core();
            let block = reconciliation_result(140); // absent from the frozen cache
            let action = SemanticTimelineRequestAction::ThreadTimelinePage {
                key: SemanticTimelineRequestKey::ThreadNewest {
                    thread_id: "a".into(),
                },
                params: ThreadTimelinePageParams {
                    thread_id: "a".into(),
                    anchor: TimelinePageAnchor::Newest,
                    limit: Some(12),
                },
            };
            let request = core
                .begin_thread_semantic_request(action.clone())
                .unwrap()
                .unwrap();
            let remove = |workspace: &str, full: bool| {
                let mut ids = vec![block.block_id.clone()];
                if full {
                    ids.extend((0..4_096).map(|id| format!("absent-{id}")));
                }
                let GatewayNotification::ThreadTimelineBlocksChanged(mut notification) =
                    reconciliation_notification(&[])
                else {
                    unreachable!()
                };
                notification.workspace_id = workspace.into();
                notification.removed_block_ids = ids;
                core.apply_thread_semantic_live_update(crate::timeline::semantic::SemanticTimelineLiveUpdate::ThreadTimelineBlocksChanged(notification))
            };
            let transport = ReconciliationTransport {
                calls: Default::default(),
                reply: |ix: usize, _: &ThreadTimelinePageParams| {
                    // Materialize the old snapshot, then deliver removal before its
                    // RPC response reaches the actual continuation completion path.
                    let frozen = reconciliation_page(vec![block.clone()], false, false);
                    if ix == 0 && matches!(outcome, "during-rpc" | "wrong-scope" | "evidence-full")
                    {
                        assert!(
                            !remove(
                                if outcome == "wrong-scope" {
                                    "other"
                                } else {
                                    "ws"
                                },
                                outcome == "evidence-full"
                            ),
                            "absent removal need not change semantic cache"
                        );
                    }
                    Ok(frozen)
                },
            };
            let next = core.execute_thread_semantic_request(&transport, request);
            assert_eq!(transport.calls.borrow().len(), 1);
            if outcome == "evidence-full" {
                assert!(next.is_none());
            } else {
                let next = next.unwrap();
                if outcome == "overlap" {
                    assert_eq!(
                        core.thread_semantic_snapshot("a")
                            .thread("a")
                            .unwrap()
                            .top_level
                            .block(&block.block_id),
                        Some(&block)
                    );
                    assert!(remove("ws", false));
                }
                let next = core
                    .execute_thread_semantic_request(&transport, next)
                    .unwrap();
                assert_eq!(transport.calls.borrow().len(), 2);
                assert!(
                    core.execute_thread_semantic_request(&transport, next)
                        .is_none()
                );
                assert_eq!(transport.calls.borrow().len(), 3);
            }
            let snapshot = core.thread_semantic_snapshot("a");
            let cache = &snapshot.thread("a").unwrap().top_level;
            if matches!(outcome, "new" | "wrong-scope") {
                assert_eq!(
                    cache.block(&block.block_id),
                    Some(&block),
                    "new absent-baseline data still loads without an accepted removal"
                );
            } else {
                assert!(
                    cache.block(&block.block_id).is_none(),
                    "stale page/overlap must not undo live removal"
                );
            }
            if outcome == "evidence-full" {
                assert!(matches!(
                    cache.request_status,
                    TimelineRequestStatus::Failed { .. }
                ));
            } else {
                assert_eq!(cache.request_status, TimelineRequestStatus::Ready);
            }
            assert!(core.thread_semantic_in_flight("a").is_empty());
            if !matches!(outcome, "new" | "wrong-scope") {
                let fresh = core.begin_thread_semantic_request(action).unwrap().unwrap();
                let positive = ReconciliationTransport {
                    calls: Default::default(),
                    reply: |_: usize, _: &ThreadTimelinePageParams| {
                        Ok(reconciliation_page(vec![block.clone()], false, false))
                    },
                };
                let fresh = core
                    .execute_thread_semantic_request(&positive, fresh)
                    .unwrap();
                let fresh = core
                    .execute_thread_semantic_request(&positive, fresh)
                    .unwrap();
                assert!(
                    core.execute_thread_semantic_request(&positive, fresh)
                        .is_none()
                );
                assert_eq!(
                    core.thread_semantic_snapshot("a")
                        .thread("a")
                        .unwrap()
                        .top_level
                        .block(&block.block_id),
                    Some(&block),
                    "finished generation evidence must not act as global tombstones"
                );
            }
        }
    }

    #[test]
    fn oversized_cached_comparison_baseline_fails_without_dispatch_or_stuck_loading() {
        let (core, receiver) = reconciliation_core();
        let mut block = reconciliation_history().remove(140);
        if let TimelineBlockKind::UserMessage { text, .. } = &mut block.kind {
            *text = "x".repeat(SEMANTIC_CONTINUATION_BYTE_LIMIT);
        } else {
            panic!("expected a message baseline");
        }
        core.apply_thread_timeline_page(
            reconciliation_page(vec![block], true, true),
            crate::timeline::semantic::TopLevelPageMergeMode::Merge,
        );
        core.apply_thread_notification(reconciliation_notification(&["result-140"]));
        let request = reconciliation_request(&receiver);
        let transport = ReconciliationTransport {
            calls: Default::default(),
            reply: |_: usize,
                    _: &ThreadTimelinePageParams|
             -> Result<ThreadTimelinePageResponse, String> {
                panic!("an oversized continuation must not issue an RPC")
            },
        };
        assert!(
            core.execute_thread_semantic_request(&transport, request)
                .is_none()
        );
        assert_eq!(transport.calls.borrow().len(), 0);
        assert!(core.thread_semantic_in_flight("a").is_empty());
        assert!(matches!(
            core.thread_semantic_snapshot("a")
                .thread("a")
                .unwrap()
                .top_level
                .request_status,
            crate::timeline::semantic::TimelineRequestStatus::Failed { .. }
        ));
        assert!(
            core.next_timeline_request_delay().is_none(),
            "capacity errors must not reset recovery backoff"
        );
    }

    #[test]
    fn cached_walk_returns_same_owner_and_rejects_between_quantum_store_replacement() {
        let (core, receiver) = reconciliation_core();
        core.apply_thread_notification(reconciliation_notification(&["result-140"]));
        let request = reconciliation_request(&receiver);
        let owner = (request.generation, request.request_generation);
        let history = reconciliation_history();
        let transport = ReconciliationTransport {
            calls: Default::default(),
            reply: |_: usize, params: &ThreadTimelinePageParams| {
                Ok(reconciliation_server_page(&history, params))
            },
        };
        let request = core
            .execute_thread_semantic_request(&transport, request)
            .unwrap();
        assert_eq!(transport.calls.borrow().len(), 1);
        assert_eq!((request.generation, request.request_generation), owner);
        assert_eq!(
            request.reconciliation.as_ref().unwrap().remaining,
            CACHED_TIMELINE_REQUEST_BUDGET - 1
        );
        core.remove_thread_store("a");
        core.upsert_thread(thread("a", "ws"));
        assert!(
            core.execute_thread_semantic_request(&transport, request)
                .is_none()
        );
        assert_eq!(
            transport.calls.borrow().len(),
            1,
            "retired continuation must not send another page"
        );
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
            let request = core.begin_thread_semantic_request(action).unwrap().unwrap();
            let before = core.thread_snapshot("a").unwrap();
            complete_semantic_operation(&core, &WrongTurnTransport, request);
            let snapshot = core.thread_snapshot("a").unwrap();
            let semantic = snapshot.semantic.thread("a").unwrap();
            assert!(!semantic.work_ranges_by_turn.contains_key("other"));
            assert!(matches!(
                semantic.work_ranges_by_turn["expected"].request_status,
                crate::timeline::semantic::TimelineRequestStatus::Failed { .. }
            ));
            assert!(!semantic.work_ranges_by_turn["expected"].has_loaded_page);
            assert!(snapshot.revision() > before.revision());
            assert!(core.thread_semantic_in_flight("a").is_empty());
            assert_eq!(core.next_timeline_request_delay(), None);
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
        let request = core.begin_thread_semantic_request(action).unwrap().unwrap();
        core.cancel_thread_requests();
        // This transport removes a if called; a cancelled queued request must never call it.
        complete_semantic_operation(&core, &CancelTransport(&core), request);
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
            work_owner: Default::default(),
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
            cli_runtime_kind: None,
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
        let generation = core.thread_registry.lock().unwrap().stores["b"].generation;
        let request = core
            .execute_thread_resume_quantum(
                &ResumeTransport,
                ThreadResumeRequest {
                    id: "b".into(),
                    generation,
                    continuation: None,
                },
            )
            .unwrap();
        // The snapshot-only quantum retains the owner without publishing a page.
        assert!(
            core.thread_registry.lock().unwrap().stores["b"]
                .coordinator
                .resume
                .in_progress
        );
        assert!(
            core.execute_thread_resume_quantum(&ResumeTransport, request)
                .is_none()
        );
        assert!(Arc::ptr_eq(&a, &core.thread_snapshot("a").unwrap()));
        let b = core.thread_snapshot("b").unwrap();
        assert!(b.coordinator.conversation.in_flight_turn_id().is_none());
        assert!(!b.coordinator.resume.in_progress);
    }

    #[test]
    fn resume_enqueue_cleanup_cannot_consume_replacement_ready_intent() {
        let core = core();
        core.upsert_thread(thread("a", "ws"));
        core.apply_thread_conversation_event(
            "ws",
            ConversationEvent::TurnStarted {
                thread_id: "a".into(),
                turn: running_turn("old"),
            },
            None,
        );
        let old_generation = core.thread_operation_token("a").unwrap().generation;
        let (sender, receiver) = crate::threads::history_scheduler::channel();
        *core.thread_request_sender.lock().unwrap() = Some(sender);
        core.enqueue_thread_resume("a".into()).unwrap();
        let replacement = core.clone();
        RESUME_CAPTURE_HOOK.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(move || {
                replacement.remove_thread_store("a");
                replacement.upsert_thread(thread("a", "ws"));
                replacement.apply_thread_conversation_event(
                    "ws",
                    ConversationEvent::TurnStarted {
                        thread_id: "a".into(),
                        turn: running_turn("new"),
                    },
                    None,
                );
                replacement.enqueue_thread_resume("a".into()).unwrap();
                assert_eq!(
                    replacement
                        .thread_coordinator_snapshot("a")
                        .unwrap()
                        .resume
                        .next_attempt_at,
                    None,
                    "accepted ready intent has no fallback deadline"
                );
                assert_eq!(
                    replacement.next_thread_resume_delay(),
                    Some(Duration::ZERO),
                    "accepted ready intent requires immediate discovery"
                );
            }))
        });
        core.drive_due_thread_resumes();
        let new_generation = core.thread_operation_token("a").unwrap().generation;
        assert_ne!(old_generation, new_generation);
        let ThreadControllerRequest::ResumeJob(old) = receiver.try_recv().unwrap().value else {
            panic!()
        };
        assert_eq!(old.generation, old_generation);
        struct ResumeRpc(std::cell::RefCell<Vec<String>>);
        impl crate::rpc::JsonRpcRequestTransport for ResumeRpc {
            fn send_json_rpc_request(
                &self,
                _: String,
                payload: String,
                response: crate::rpc::JsonRpcResponseSender,
            ) -> Result<(), String> {
                let request: serde_json::Value = serde_json::from_str(&payload).unwrap();
                assert_eq!(request["params"]["turn_id"], "new");
                let method = request["method"].as_str().unwrap();
                self.0.borrow_mut().push(method.into());
                let result = match method {
                    "turn/get" => {
                        let mut turn = running_turn("new");
                        turn.status = TurnStatus::Completed;
                        serde_json::to_value(TurnGetResponse {
                            workspace_id: "ws".into(),
                            thread_id: "a".into(),
                            turn,
                        })
                        .unwrap()
                    }
                    "turn/items/page" => serde_json::to_value(TurnItemsResponse {
                        workspace_id: "ws".into(),
                        thread_id: "a".into(),
                        turn_id: "new".into(),
                        events: vec![],
                        last_sequence: 0,
                        has_more: false,
                        next_cursor: None,
                    })
                    .unwrap(),
                    _ => panic!("unexpected resume RPC"),
                };
                response.send(Ok(result)).unwrap();
                Ok(())
            }
        }
        let transport = ResumeRpc(Default::default());
        assert!(
            core.execute_thread_resume_quantum(&transport, old)
                .is_none()
        );
        assert!(
            transport.0.borrow().is_empty(),
            "old store job cannot execute"
        );
        core.drive_due_thread_resumes();
        let ThreadControllerRequest::ResumeJob(new) = receiver.try_recv().unwrap().value else {
            panic!("replacement ready intent must remain discoverable")
        };
        assert_eq!(new.generation, new_generation);
        let next = core.execute_thread_resume_quantum(&transport, new).unwrap();
        assert_eq!(*transport.0.borrow(), ["turn/get"]);
        assert!(
            core.execute_thread_resume_quantum(&transport, next)
                .is_none()
        );
        assert_eq!(*transport.0.borrow(), ["turn/get", "turn/items/page"]);
        let snapshot = core.thread_coordinator_snapshot("a").unwrap();
        assert!(snapshot.conversation.in_flight_turn_id().is_none());
        assert!(!snapshot.resume.in_progress);
    }

    #[test]
    fn independent_due_worker_preserves_intentional_ancestor_through_rpc() {
        use pioneer_observability::{
            live::{self, JournalPayload},
            work::{self, ContextOrigin, ExecutorRole, Trigger, WorkContext, WorkKind},
        };
        let _lock = crate::OBSERVABILITY_TEST_LOCK.lock().unwrap();
        let enabled = pioneer_observability::telemetry_enabled();
        pioneer_observability::set_telemetry_enabled(false);
        pioneer_observability::set_telemetry_enabled(true);
        let core = core();
        connect_subscription_test(&core);
        struct Cleanup {
            core: Arc<ClientCore>,
            enabled: bool,
        }
        impl Drop for Cleanup {
            fn drop(&mut self) {
                self.core.shutdown();
                pioneer_observability::set_telemetry_enabled(self.enabled);
            }
        }
        let _cleanup = Cleanup {
            core: core.clone(),
            enabled,
        };
        core.upsert_thread(thread("a", "ws"));
        core.apply_thread_conversation_event(
            "ws",
            ConversationEvent::TurnStarted {
                thread_id: "a".into(),
                turn: running_turn("due"),
            },
            None,
        );
        struct ObserveRpc(std::sync::mpsc::Sender<WorkContext>);
        impl crate::rpc::JsonRpcRequestTransport for ObserveRpc {
            fn send_json_rpc_request(
                &self,
                _: String,
                payload: String,
                response: crate::rpc::JsonRpcResponseSender,
            ) -> Result<(), String> {
                let request: serde_json::Value = serde_json::from_str(&payload).unwrap();
                assert_eq!(request["method"], "turn/get");
                self.0
                    .send(work::current().expect("real RPC scope"))
                    .unwrap();
                response
                    .send(Err(crate::rpc::JsonRpcResponseError::InvalidResponse))
                    .unwrap();
                Ok(())
            }
        }
        let (rpc_tx, rpc_rx) = std::sync::mpsc::channel();
        *core
            .transport_runtime()
            .ws_command_sender()
            .test_requests
            .lock()
            .unwrap() = Some(Arc::new(ObserveRpc(rpc_tx)));
        let (waiting_tx, waiting_rx) = std::sync::mpsc::channel();
        let watchdog = Duration::from_secs(5);
        core.start_thread_request_controller(Some(waiting_tx));
        assert_eq!(waiting_rx.recv_timeout(watchdog).unwrap(), None);
        // Public ready admission + coalesced wake, no manually parented payload.
        core.schedule_thread_resume("a").unwrap();
        let rpc = rpc_rx.recv_timeout(watchdog).unwrap();
        let snapshot = live::snapshot().unwrap();
        let context = |token| {
            snapshot
                .active
                .iter()
                .map(|work| work.context)
                .chain(
                    snapshot
                        .journal
                        .iter()
                        .filter_map(|record| match &record.payload {
                            JournalPayload::Work(work) => Some(work.context),
                            _ => None,
                        }),
                )
                .find(|context| context.token == token)
                .expect("actual carried ancestor")
        };
        assert_eq!(rpc.kind, WorkKind::ClientRpc);
        assert_eq!(rpc.origin, ContextOrigin::Inherited);
        let job = context(rpc.parent.unwrap());
        assert_eq!(job.kind, WorkKind::ClientCommand);
        assert_eq!(job.origin, ContextOrigin::Inherited);
        let cycle = context(job.parent.unwrap());
        assert_eq!(cycle.origin, ContextOrigin::IntentionallyIndependent);
        assert_eq!(cycle.trigger, Trigger::Reconcile);
        assert_eq!(cycle.executor, ExecutorRole::ClientController);
        assert_eq!(cycle.parent, None);
        assert_eq!(cycle.consent_generation, rpc.consent_generation);
        core.shutdown();
        // Wait for actual worker exit before restoring consent; no sleep ordering.
        loop {
            match waiting_rx.recv_timeout(watchdog) {
                Ok(_) => {}
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                Err(error) => panic!("controller did not stop: {error}"),
            }
        }
    }

    #[test]
    fn resume_items_pages_return_the_same_owner_and_retirement_cleans_in_progress() {
        struct PagingResume(std::cell::RefCell<Vec<(String, Option<i64>)>>);
        impl crate::rpc::JsonRpcRequestTransport for PagingResume {
            fn send_json_rpc_request(
                &self,
                _: String,
                payload: String,
                response: crate::rpc::JsonRpcResponseSender,
            ) -> Result<(), String> {
                let request: serde_json::Value = serde_json::from_str(&payload).unwrap();
                let method = request["method"].as_str().unwrap();
                let after = request["params"]["after_sequence"].as_i64();
                self.0.borrow_mut().push((method.into(), after));
                let value = if method == "turn/get" {
                    let mut turn = running_turn("turn-b");
                    turn.status = TurnStatus::Completed;
                    serde_json::to_value(TurnGetResponse {
                        thread_id: "b".into(),
                        workspace_id: "ws".into(),
                        turn,
                    })
                    .unwrap()
                } else {
                    assert_eq!(method, "turn/items/page");
                    serde_json::to_value(TurnItemsResponse {
                        thread_id: "b".into(),
                        workspace_id: "ws".into(),
                        turn_id: "turn-b".into(),
                        events: vec![],
                        last_sequence: 1,
                        has_more: after.is_none(),
                        next_cursor: after.is_none().then_some(1),
                    })
                    .unwrap()
                };
                response.send(Ok(value)).unwrap();
                Ok(())
            }
        }
        for retirement in ["complete", "replace", "terminal"] {
            let core = core();
            core.upsert_thread(thread("b", "ws"));
            core.apply_thread_conversation_event(
                "ws",
                ConversationEvent::TurnStarted {
                    thread_id: "b".into(),
                    turn: running_turn("turn-b"),
                },
                None,
            );
            let generation = core.thread_operation_token("b").unwrap().generation;
            let transport = PagingResume(Default::default());
            let request = core
                .execute_thread_resume_quantum(
                    &transport,
                    ThreadResumeRequest {
                        id: "b".into(),
                        generation,
                        continuation: None,
                    },
                )
                .unwrap();
            assert_eq!(transport.0.borrow().len(), 1, "snapshot is its own quantum");
            let request = core
                .execute_thread_resume_quantum(&transport, request)
                .unwrap();
            assert_eq!(transport.0.borrow().len(), 2, "one item page per quantum");
            assert_eq!(request.generation, generation);
            assert_eq!(request.continuation.as_ref().unwrap().after, Some(1));
            match retirement {
                "replace" => {
                    core.remove_thread_store("b");
                    core.upsert_thread(thread("b", "ws"));
                }
                "terminal" => {
                    let mut turn = running_turn("turn-b");
                    turn.status = TurnStatus::Completed;
                    core.apply_thread_conversation_event(
                        "ws",
                        ConversationEvent::TurnCompleted {
                            thread_id: "b".into(),
                            turn,
                        },
                        None,
                    );
                    // Completion first enters Completing, which is still in flight.
                    // Finalize it before exercising cleanup with no current turn.
                    assert!(core.tick_thread_conversations());
                    assert_eq!(
                        core.thread_coordinator_snapshot("b")
                            .unwrap()
                            .conversation
                            .in_flight_turn_id(),
                        None
                    );
                }
                _ => {}
            }
            // A same-store terminal transition must reach the resume cleanup
            // quantum; pruning it on the absence of a turn would leak Loading.
            let (sender, receiver) = crate::threads::history_scheduler::channel();
            *core.thread_request_sender.lock().unwrap() = Some(sender.clone());
            sender
                .send(ThreadControllerRequest::ResumeJob(request))
                .unwrap();
            core.retire_thread_controller_jobs();
            if retirement == "replace" {
                assert!(receiver.try_recv().is_err());
            } else {
                let ThreadControllerRequest::ResumeJob(request) =
                    receiver.try_recv().unwrap().value
                else {
                    panic!()
                };
                assert!(
                    core.execute_thread_resume_quantum(&transport, request)
                        .is_none()
                );
            }
            assert_eq!(
                transport.0.borrow().len(),
                if retirement == "complete" { 3 } else { 2 }
            );
            let store = core.thread_coordinator_snapshot("b").unwrap();
            assert!(!store.resume.in_progress);
            if retirement == "complete" {
                assert_eq!(transport.0.borrow()[2].1, Some(1));
            }
        }
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

    fn draft_subscription_core() -> (
        Arc<ClientCore>,
        crate::threads::history_scheduler::ThreadControllerReceiver,
    ) {
        let core = crate::catalog_test_support::settings_client();
        connect_subscription_test(&core);
        {
            let mut session = core.gateway_session.lock().unwrap();
            session.observe_transport(&crate::transport::ws::GatewayWsEvent::Connected {
                connection_id: 7,
                endpoint_id: "endpoint".into(),
                endpoint_name: "Synthetic".into(),
                gateway_base_url: GatewayBaseUrl::parse_presentation("https://synthetic.invalid")
                    .unwrap(),
            });
            core.publish_gateway_session(&session);
        }
        let (sender, receiver) = crate::threads::history_scheduler::channel();

        *core.thread_request_sender.lock().unwrap() = Some(sender);
        (core, receiver)
    }

    #[test]
    fn prepared_draft_keeps_identity_and_text_through_creation() {
        use crate::composer::store::ComposerIntent;
        let (core, receiver) = draft_subscription_core();
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
        drop(core.thread_mutation(&id, "ws").unwrap());
        let timeline_scope = ClientScope::Timeline {
            thread_id: id.clone(),
        };
        let _visible = core.subscribe(timeline_scope.clone(), NonZeroUsize::new(8).unwrap());
        core.thread_demand_changed(&timeline_scope, ClientDemand::Suspended);
        core.thread_demand_changed(&timeline_scope, ClientDemand::Visible);
        assert!(
            receiver.try_recv().is_err(),
            "reserved identity cannot open a thread"
        );
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
        assert!(
            matches!(receiver.try_recv().map(|item| item.value), Ok(ThreadControllerRequest::Subscribe(request)) if request.id == id)
        );
        assert!(
            receiver.try_recv().is_err(),
            "one subscription after creation"
        );
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
        for dispatch_before_failure in [false, true] {
            let (core, receiver) = draft_subscription_core();
            core.activate_thread(None, Some("ws"));
            let id = core.prepare_thread_draft().unwrap();
            drop(core.thread_mutation(&id, "ws").unwrap());
            // A request queued before creation claimed this identity must also retire.
            core.thread_start_mutation().pending_thread_id = None;
            let _visible = core.subscribe(
                ClientScope::Timeline {
                    thread_id: id.clone(),
                },
                NonZeroUsize::new(8).unwrap(),
            );
            let ThreadControllerRequest::Subscribe(queued) = receiver.try_recv().unwrap().value
            else {
                panic!()
            };
            core.thread_start_mutation().pending_thread_id = Some(id.clone());
            if dispatch_before_failure {
                core.execute_thread_subscription_request(queued.clone());
            } else {
                core.thread_demand_changed(
                    &ClientScope::Timeline {
                        thread_id: id.clone(),
                    },
                    ClientDemand::Suspended,
                );
            }
            assert!(
                core.create_workspace_thread_draft(
                    &FailedTransport,
                    "ws",
                    ThreadVisibility::Private
                )
                .is_err()
            );
            assert!(!core.thread_start_snapshot().in_progress);
            assert!(!core.thread_start_requested());
            assert_eq!(core.prepare_thread_draft().as_deref(), Some(id.as_str()));
            core.execute_thread_subscription_request(queued);
            core.schedule_thread_subscription(&id, "ws");
            assert!(receiver.try_recv().is_err());
            {
                let registry = core.thread_registry.lock().unwrap();
                let store = &registry.stores[&id];
                assert!(!store.coordinator.history_loading);
                assert!(store.subscription_request.is_none());
                assert!(store.subscription_retry_at.is_none());
            }
            core.thread_demand_changed(
                &ClientScope::Timeline {
                    thread_id: id.clone(),
                },
                ClientDemand::Visible,
            );
            let transport = DraftTransport(Default::default());
            assert_eq!(
                core.create_workspace_thread_draft(&transport, "ws", ThreadVisibility::Private)
                    .unwrap(),
                id
            );
            assert!(
                matches!(receiver.try_recv().map(|item| item.value), Ok(ThreadControllerRequest::Subscribe(request)) if request.id == id)
            );
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
            .unwrap()
            .unwrap();
        core.begin_authorization_epoch(Some(("synthetic-endpoint".into(), 1)));
        complete_semantic_operation(&core, &PageTransport, request);
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
            .unwrap()
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
                cli_runtime_kind: None,
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
                cli_runtime_kind: None,
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

    fn connect_subscription_test(core: &ClientCore) {
        let event = crate::transport::ws::GatewayWsEvent::Connected {
            connection_id: 7,
            endpoint_id: "endpoint".into(),
            endpoint_name: "Synthetic".into(),
            gateway_base_url: GatewayBaseUrl::parse_presentation("https://synthetic.invalid")
                .unwrap(),
        };
        core.transport_runtime()
            .ws_command_sender()
            .set_http_session_access_for_test(Some(crate::transport::http::GatewayHttpAccess {
                gateway_base_url: GatewayBaseUrl::parse_presentation("https://synthetic.invalid")
                    .unwrap(),
                gateway_id: GatewayId::new("GAAAAAAAAAAAAAAAAAAAA").unwrap(),
                session_id: AuthSessionId::new("SAAAAAAAAAAAAAAAAAAAA").unwrap(),
                generation: 7,
                access_expires_at_unix: u64::MAX,
                access_token: AuthSecretString::new("synthetic"),
            }));
        core.transport_runtime().inject_test_event(event.clone());
        core.transport_runtime().drain_ws_events();
        let mut session = core.gateway_session.lock().unwrap();
        session.observe_transport(&event);
        core.publish_gateway_session(&session);
    }

    struct SubscriptionAck(Thread);
    impl crate::rpc::JsonRpcRequestTransport for SubscriptionAck {
        fn send_json_rpc_request(
            &self,
            _: String,
            payload: String,
            response: crate::rpc::JsonRpcResponseSender,
        ) -> Result<(), String> {
            let request: pioneer_protocol::JsonRpcRequest = serde_json::from_str(&payload).unwrap();
            assert_eq!(
                request.method,
                pioneer_protocol::constants::methods::THREAD_START
            );
            response
                .send(Ok(serde_json::to_value(ThreadStartResponse {
                    thread: self.0.clone(),
                    sandbox: SandboxPolicy::from_mode(SandboxMode::FullAccess),
                })
                .unwrap()))
                .map_err(|error| error.to_string())
        }
    }

    fn acknowledge_subscription(core: &ClientCore, id: &str, workspace: &str) {
        let (request, ready) = core
            .admit_thread_subscription(id, workspace, None, false, None)
            .unwrap();
        assert!(!ready);
        let snapshot = core
            .thread_snapshot(id)
            .unwrap()
            .coordinator()
            .thread()
            .unwrap()
            .clone();
        core.execute_thread_subscription_request_using(&request, &SubscriptionAck(snapshot));
        assert!(
            request
                .flight
                .lock()
                .unwrap()
                .result
                .as_ref()
                .unwrap()
                .is_ok()
        );
    }

    // Real admission/completion/reconciliation owner with a borrowed network port.
    // The fixture supplies replies and interleavings; it does not deduplicate work.
    fn refresh_owned_subscription_using(
        core: &ClientCore,
        transport: &impl crate::rpc::JsonRpcRequestTransport,
        token: ThreadOperationToken,
        workspace: &str,
    ) -> anyhow::Result<()> {
        let (request, _) = core.admit_thread_subscription(
            &token.id,
            workspace,
            Some(token.generation),
            true,
            None,
        )?;
        core.execute_thread_subscription_request_using(&request, transport);
        let result = request
            .flight
            .lock()
            .unwrap()
            .result
            .clone()
            .expect("synchronous fixture reply");
        result.map_err(|error| error.error())
    }

    struct SubscriptionRecorder {
        starts: std::sync::mpsc::Sender<crate::rpc::JsonRpcResponseSender>,
        calls: std::sync::atomic::AtomicUsize,
    }
    impl crate::rpc::JsonRpcRequestTransport for SubscriptionRecorder {
        fn send_json_rpc_request(
            &self,
            _: String,
            payload: String,
            response: crate::rpc::JsonRpcResponseSender,
        ) -> Result<(), String> {
            let request: pioneer_protocol::JsonRpcRequest = serde_json::from_str(&payload).unwrap();
            match request.method.as_str() {
                "thread/start" | "thread/open" => {
                    self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    self.starts
                        .send(response)
                        .map_err(|error| error.to_string())
                }
                method => panic!("unexpected subscription fixture RPC: {method}"),
            }
        }
    }
    fn shared_subscription_core() -> (
        Arc<ClientCore>,
        Arc<SubscriptionRecorder>,
        std::sync::mpsc::Receiver<crate::rpc::JsonRpcResponseSender>,
        crate::threads::history_scheduler::ThreadControllerReceiver,
    ) {
        let core = crate::catalog_test_support::settings_client();
        connect_subscription_test(&core);
        core.upsert_thread(thread("a", "ws"));
        let (starts, responses) = std::sync::mpsc::channel();
        let transport = Arc::new(SubscriptionRecorder {
            starts,
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        *core
            .transport_runtime()
            .ws_command_sender()
            .test_requests
            .lock()
            .unwrap() = Some(transport.clone());
        let (worker, requests) = crate::threads::history_scheduler::channel();

        *core.thread_request_sender.lock().unwrap() = Some(worker);
        (core, transport, responses, requests)
    }
    fn subscription_reply(response: &crate::rpc::JsonRpcResponseSender) {
        response
            .send(Ok(serde_json::to_value(ThreadStartResponse {
                thread: thread("a", "ws"),
                sandbox: SandboxPolicy::from_mode(SandboxMode::FullAccess),
            })
            .unwrap()))
            .unwrap();
    }
    fn spawn_subscription_waiter(
        core: &Arc<ClientCore>,
        budget: crate::rpc::RequestBudget,
    ) -> std::thread::JoinHandle<
        anyhow::Result<crate::transport::ws::runtime::ConnectionRequestTransport>,
    > {
        let core = core.clone();
        std::thread::spawn(move || core.ensure_thread_subscription("a", "ws", &budget))
    }

    fn spawn_admitted_subscription_waiter(
        core: &Arc<ClientCore>,
        request: ThreadSubscriptionRequest,
        budget: crate::rpc::RequestBudget,
    ) -> std::thread::JoinHandle<
        anyhow::Result<crate::transport::ws::runtime::ConnectionRequestTransport>,
    > {
        let core = core.clone();
        std::thread::spawn(move || core.wait_thread_subscription(&request, false, &budget))
    }

    #[test]
    fn newer_open_intent_wins_over_late_response() {
        let (core, _transport, responses, _requests) = shared_subscription_core();
        core.upsert_thread(thread("b", "ws"));
        let a = {
            let core = core.clone();
            std::thread::spawn(move || core.open_thread_by_id("a"))
        };
        let old = responses.recv_timeout(Duration::from_secs(2)).unwrap();
        let b = {
            let core = core.clone();
            std::thread::spawn(move || core.open_thread_by_id("b"))
        };
        let latest = responses.recv_timeout(Duration::from_secs(2)).unwrap();
        latest
            .send(Ok(serde_json::to_value(ThreadStartResponse {
                thread: thread("b", "ws"),
                sandbox: SandboxPolicy::from_mode(SandboxMode::FullAccess),
            })
            .unwrap()))
            .unwrap();
        b.join().unwrap().unwrap();
        subscription_reply(&old);
        assert!(a.join().unwrap().is_err());
        assert_eq!(core.navigation_snapshot().active_thread_id(), Some("b"));
    }

    #[test]
    fn open_leading_send_joins_real_open_receipt() {
        let (core, transport, responses, _requests) = shared_subscription_core();
        let opened = {
            let core = core.clone();
            std::thread::spawn(move || core.open_thread_by_id("a"))
        };
        let response = responses.recv_timeout(Duration::from_secs(2)).unwrap();
        let (admitted_tx, admitted_rx) = std::sync::mpsc::channel();
        let sending = {
            let core = core.clone();
            std::thread::spawn(move || {
                SUBSCRIPTION_PROMOTION_HOOK.with(|slot| {
                    *slot.borrow_mut() = Some(Box::new(move || admitted_tx.send(()).unwrap()))
                });
                let budget =
                    crate::rpc::RequestBudget::new(Instant::now() + Duration::from_secs(2));
                core.ensure_thread_subscription("a", "ws", &budget)
            })
        };
        admitted_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        subscription_reply(&response);
        assert!(sending.join().unwrap().unwrap().is_current());
        opened.join().unwrap().unwrap();
        assert_eq!(transport.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        let budget = crate::rpc::RequestBudget::new(Instant::now() + Duration::from_secs(2));
        core.ensure_thread_subscription("a", "ws", &budget).unwrap();
        assert_eq!(transport.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn scheduled_open_and_send_share_subscription_and_open_keeps_hydration() {
        for scheduled in [false, true] {
            let (core, transport, responses, requests) = shared_subscription_core();
            let queued = if scheduled {
                core.schedule_thread_subscription("a", "ws");
                let queued = requests.try_recv().unwrap().value;
                assert!(matches!(queued, ThreadControllerRequest::Subscribe(_)));
                Some(queued)
            } else {
                None
            };
            // The queue deliberately is not driven: foreground must start its own
            // prerequisite without waiting for the worker's queued copy.
            let waiter = spawn_subscription_waiter(
                &core,
                crate::rpc::RequestBudget::new(Instant::now() + Duration::from_secs(2)),
            );
            let response = responses.recv_timeout(Duration::from_secs(2)).unwrap();
            let opened = {
                let core = core.clone();
                std::thread::spawn(move || core.open_thread_by_id("a"))
            };
            subscription_reply(&response);
            let binding = waiter.join().unwrap().unwrap();
            opened.join().unwrap().unwrap();
            if let Some(ThreadControllerRequest::Subscribe(queued)) = queued {
                core.execute_thread_subscription_request(queued);
            }
            assert_eq!(transport.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
            assert!(
                requests
                    .try_iter()
                    .any(|item| matches!(item.value, ThreadControllerRequest::Semantic(_))),
                "Open hydrates after Send-leading acknowledgement"
            );
            let budget = crate::rpc::RequestBudget::new(Instant::now() + Duration::from_secs(2));
            assert_eq!(
                core.ensure_thread_subscription("a", "ws", &budget)
                    .unwrap()
                    .scope(),
                binding.scope()
            );
            let mut access = core
                .transport_runtime()
                .ws_command_sender()
                .current_gateway_http_access()
                .unwrap();
            access.access_token = AuthSecretString::new("renewed-token-same-authority");
            core.transport_runtime()
                .ws_command_sender()
                .set_http_session_access_for_test(Some(access));
            assert_eq!(
                core.ensure_thread_subscription("a", "ws", &budget)
                    .unwrap()
                    .scope(),
                binding.scope()
            );
            assert_eq!(
                transport.calls.load(std::sync::atomic::Ordering::SeqCst),
                1,
                "warm and same-socket renewal do not resubscribe"
            );
            core.shutdown();
        }
    }

    #[test]
    fn late_shared_subscription_executor_preserves_completed_receipt() {
        let (core, transport, responses, requests) = shared_subscription_core();
        let (completed_tx, completed_rx) = std::sync::mpsc::channel();
        let completing_core = core.clone();
        SUBSCRIPTION_PROMOTION_HOOK.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(move || {
                // The first actual executor has passed its current check. Let
                // a second foreground caller ACK that same flight before resume.
                let request = completing_core.thread_registry.lock().unwrap().stores["a"]
                    .subscription_request
                    .clone()
                    .unwrap();
                let watchdog = Duration::from_secs(2);
                let second = spawn_subscription_waiter(
                    &completing_core,
                    crate::rpc::RequestBudget::new(Instant::now() + watchdog),
                );
                let response = responses.recv_timeout(watchdog).unwrap();
                assert_eq!(
                    completing_core.thread_registry.lock().unwrap().stores["a"]
                        .subscription_request
                        .as_ref(),
                    Some(&request)
                );
                subscription_reply(&response);
                let binding = second.join().unwrap().unwrap();
                assert!(binding.is_current());
                {
                    let registry = completing_core.thread_registry.lock().unwrap();
                    let store = &registry.stores["a"];
                    assert!(store.subscription_request.is_none());
                    let receipt = store.subscription_receipt.as_ref().unwrap();
                    assert_eq!(Some(&receipt.scope), request.scope().as_ref());
                    assert!(Arc::ptr_eq(&receipt.live, &request.live));
                    assert!(*receipt.live.lock().unwrap());
                    assert!(
                        request
                            .flight
                            .lock()
                            .unwrap()
                            .result
                            .as_ref()
                            .unwrap()
                            .is_ok()
                    );
                }
                let publication = completing_core.thread_snapshot("a").unwrap();
                completed_tx.send((binding, request, publication)).unwrap();
            }));
        });
        let budget = crate::rpc::RequestBudget::new(Instant::now() + Duration::from_secs(2));
        let first = core.ensure_thread_subscription("a", "ws", &budget).unwrap();
        let (second, request, publication) = completed_rx.try_recv().unwrap();
        assert!(first.is_current());
        assert!(second.is_current());
        assert_eq!(first.scope(), second.scope());
        assert_eq!(
            first.scope().as_ref(),
            Some(&request.scope().unwrap().socket)
        );
        assert!(Arc::ptr_eq(
            &publication,
            &core.thread_snapshot("a").unwrap()
        ));
        {
            let registry = core.thread_registry.lock().unwrap();
            let store = &registry.stores["a"];
            let receipt = store.subscription_receipt.as_ref().unwrap();
            assert_eq!(Some(&receipt.scope), request.scope().as_ref());
            assert!(Arc::ptr_eq(&receipt.live, &request.live));
            assert!(*receipt.live.lock().unwrap());
            assert!(store.subscription_request.is_none());
            assert!(store.subscription_retry_at.is_none());
            assert!(!store.coordinator.history_loading);
            assert!(!store.subscription_failed);
            assert!(
                request
                    .flight
                    .lock()
                    .unwrap()
                    .result
                    .as_ref()
                    .unwrap()
                    .is_ok()
            );
        }
        let reused = core.ensure_thread_subscription("a", "ws", &budget).unwrap();
        assert!(reused.is_current());
        assert_eq!(reused.scope(), first.scope());
        assert!(Arc::ptr_eq(
            &publication,
            &core.thread_snapshot("a").unwrap()
        ));
        assert_eq!(transport.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(
            requests.try_recv().is_err(),
            "foreground ACK does not schedule hydration"
        );
        core.shutdown();
    }

    #[test]
    fn subscription_waiter_cancel_and_timeout_leave_other_consumer_alive() {
        for cancel in [false, true] {
            let (core, transport, responses, _requests) = shared_subscription_core();
            let first_budget = crate::rpc::RequestBudget::new(
                Instant::now()
                    + if cancel {
                        Duration::from_secs(2)
                    } else {
                        Duration::from_millis(20)
                    },
            );
            let (request, _) = core
                .admit_thread_subscription("a", "ws", None, false, None)
                .unwrap();
            core.execute_thread_subscription_request(request.clone());
            let first =
                spawn_admitted_subscription_waiter(&core, request.clone(), first_budget.clone());
            let response = responses.recv_timeout(Duration::from_secs(2)).unwrap();
            let other = spawn_admitted_subscription_waiter(
                &core,
                request,
                crate::rpc::RequestBudget::new(Instant::now() + Duration::from_secs(2)),
            );
            if cancel {
                first_budget.cancel();
            }
            assert!(first.join().unwrap().is_err());
            assert!(
                response.is_active(),
                "individual departure must not retire shared subscription RPC"
            );
            subscription_reply(&response);
            assert!(other.join().unwrap().unwrap().is_current());
            assert_eq!(transport.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
            core.shutdown();
        }
    }

    #[test]
    fn subscription_failure_retirement_and_shutdown_complete_all_waiters() {
        for outcome in ["failure", "deadline", "retire", "shutdown"] {
            let (core, _transport, responses, _requests) = shared_subscription_core();
            let (request, _) = core
                .admit_thread_subscription("a", "ws", None, false, None)
                .unwrap();
            core.execute_thread_subscription_request(request.clone());
            let first = spawn_admitted_subscription_waiter(
                &core,
                request.clone(),
                crate::rpc::RequestBudget::new(Instant::now() + Duration::from_secs(2)),
            );
            let response = responses.recv_timeout(Duration::from_secs(2)).unwrap();
            let second = spawn_admitted_subscription_waiter(
                &core,
                request,
                crate::rpc::RequestBudget::new(Instant::now() + Duration::from_secs(2)),
            );
            match outcome {
                "failure" => {
                    response
                        .send(Err(crate::rpc::JsonRpcResponseError::transport(
                            "subscription failed",
                        )))
                        .unwrap();
                }
                "deadline" => {
                    core.thread_registry.lock().unwrap().stores["a"]
                        .subscription_request
                        .as_ref()
                        .unwrap()
                        .flight
                        .lock()
                        .unwrap()
                        .deadline = Instant::now();
                    core.drive_thread_subscriptions();
                }
                "retire" => core.cancel_thread_requests(),
                "shutdown" => {
                    core.shutdown();
                }
                _ => unreachable!(),
            }
            assert!(first.join().unwrap().is_err());
            assert!(second.join().unwrap().is_err());
            assert!(!response.is_active());
            core.shutdown();
        }
    }

    #[test]
    fn late_subscription_completion_cannot_confirm_replaced_store_or_socket() {
        for replace_socket in [false, true] {
            let (core, transport, responses, _requests) = shared_subscription_core();
            let first = spawn_subscription_waiter(
                &core,
                crate::rpc::RequestBudget::new(Instant::now() + Duration::from_secs(2)),
            );
            let old = responses.recv_timeout(Duration::from_secs(2)).unwrap();
            if replace_socket {
                // Same logical ID and unchanged core revision: transport receipt
                // alone must distinguish sockets before core sees disconnect.
                let event = crate::transport::ws::GatewayWsEvent::Connected {
                    connection_id: 7,
                    endpoint_id: "endpoint".into(),
                    endpoint_name: "Synthetic".into(),
                    gateway_base_url: GatewayBaseUrl::parse_presentation(
                        "https://synthetic.invalid",
                    )
                    .unwrap(),
                };
                core.transport_runtime().inject_test_event(event);
                core.transport_runtime().drain_ws_events();
            } else {
                core.cancel_thread_requests();
            }
            let second = spawn_subscription_waiter(
                &core,
                crate::rpc::RequestBudget::new(Instant::now() + Duration::from_secs(2)),
            );
            let current = responses.recv_timeout(Duration::from_secs(2)).unwrap();
            let _ = old.send(Ok(serde_json::to_value(ThreadStartResponse {
                thread: thread("a", "ws"),
                sandbox: SandboxPolicy::from_mode(SandboxMode::FullAccess),
            })
            .unwrap()));
            assert!(first.join().unwrap().is_err());
            assert!(
                core.thread_registry.lock().unwrap().stores["a"]
                    .subscription_receipt
                    .is_none()
            );
            subscription_reply(&current);
            let receipt = second.join().unwrap().unwrap();
            assert!(receipt.is_current());
            assert_eq!(transport.calls.load(std::sync::atomic::Ordering::SeqCst), 2);
            core.cancel_thread_requests();
            assert!(
                !receipt.is_current(),
                "store retirement fences dispatch even when physical socket stays alive"
            );
            core.shutdown();
        }
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
    fn hidden_request_completion_at_cache_limit_keeps_publications_consistent() {
        let core = core();
        core.upsert_thread(thread("a", "ws"));
        core.activate_thread(Some("a"), Some("ws"));
        let request = core
            .begin_thread_semantic_request(SemanticTimelineRequestAction::ThreadTimelinePage {
                key: SemanticTimelineRequestKey::ThreadNewest {
                    thread_id: "a".into(),
                },
                params: ThreadTimelinePageParams {
                    thread_id: "a".into(),
                    anchor: TimelinePageAnchor::Newest,
                    limit: Some(12),
                },
            })
            .unwrap()
            .unwrap();
        core.upsert_thread(thread("b", "ws"));
        core.activate_thread(Some("b"), Some("ws"));
        for i in 0..INACTIVE_THREAD_LIMIT {
            core.upsert_thread(thread(&format!("idle-{i}"), "ws"));
        }
        let before = core.snapshot(&scope("a")).unwrap().revisions();
        complete_semantic_operation(
            &core,
            &ReconciliationTransport {
                calls: Default::default(),
                reply: |_: usize, _: &ThreadTimelinePageParams| {
                    Ok(presentation_page(&[("answer", "completed read")]))
                },
            },
            request,
        );
        let retired = core.snapshot(&scope("a")).unwrap();
        assert!(retired.snapshot().serialized_payload().is_null());
        assert_eq!(
            retired.revisions().scoped().get(),
            before.scoped().get() + 2,
            "both the completed update and subsequent retirement were accepted"
        );
        assert_eq!(
            core.thread_registry.lock().unwrap().stores.len(),
            INACTIVE_THREAD_LIMIT + 1
        );

        // Recreate the evicted source, then exercise the directory + Navigation
        // publication path that a rejected completion used to poison.
        let mut updated = thread("a", "ws");
        updated.name = Some("Updated after completion".into());
        core.upsert_thread(updated);
        core.activate_thread(Some("a"), Some("ws"));
        let published = core.snapshot(&scope("a")).unwrap();
        let source = published.typed::<ThreadDomainSnapshot>().unwrap().payload();
        assert_eq!(
            source.coordinator.thread().unwrap().name.as_deref(),
            Some("Updated after completion")
        );
        assert_eq!(source.revision(), published.revisions().scoped().get());
        let tree_scope = ClientScope::WorkspaceTree {
            workspace_id: Some("ws".into()),
        };
        let published_tree = core
            .snapshot(&tree_scope)
            .unwrap()
            .typed::<crate::workspaces::directory::ThreadTreePublication>()
            .unwrap()
            .payload();
        assert_eq!(
            published_tree.snapshot().threads_by_id["a"].name.as_deref(),
            Some("Updated after completion")
        );
        assert_eq!(
            published_tree.revision(),
            core.workspace_tree("ws").unwrap().revision()
        );
        let navigation = core
            .snapshot(&ClientScope::Navigation)
            .unwrap()
            .typed::<crate::navigation::ClientNavigationState>()
            .unwrap()
            .payload();
        assert_eq!(navigation.active_thread_id(), Some("a"));
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
        core.enqueue_thread_resume("draft".into()).unwrap();
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

pub(crate) type HistoryApply = Box<
    dyn FnOnce(
            &ClientCore,
            anyhow::Result<serde_json::Value>,
            Option<&crate::core::HistoryAuthority>,
        ) -> Option<ThreadControllerRequest>
        + Send,
>;
pub(crate) struct PreparedHistory {
    pub(crate) identity: Option<HistoryIdentity>,
    pub(crate) rpc: Option<(&'static str, serde_json::Value)>,
    pub(crate) apply: HistoryApply,
}

pub(crate) struct PreparedSemanticRequest {
    pub(crate) rpc: Option<(&'static str, serde_json::Value)>,
    request: ThreadSemanticRequest,
    key: SemanticTimelineRequestKey,
    initial_items: Option<HashMap<String, pioneer_protocol::TurnItem>>,
    expected_turn: Option<String>,
}

/// An accepted request is tied to the incarnation of its source store.
pub struct ThreadSemanticRequest {
    id: String,
    generation: u64,
    request_generation: u64,
    explicit_intent: bool,
    recovery: bool,
    action: SemanticTimelineRequestAction,
    reconciliation: Option<CachedTimelineReconciliation>,
    work_items: WorkItemsContinuation,
}

// Defensive retained-resource budget, not a measured throughput optimum. A job
// holds either a frozen comparison baseline or an aggregate of work-item chunks.
// The controller also bounds the number of admitted jobs.
const SEMANTIC_CONTINUATION_BYTE_LIMIT: usize = 8 * 1024 * 1024;

#[derive(Default)]
struct WorkItemsContinuation {
    next: usize,
    pages: Vec<pioneer_protocol::TurnWorkItemsGetResponse>,
    bytes: usize,
    baseline: HashMap<String, Option<pioneer_protocol::TurnWorkItem>>,
    capture_failed: bool,
}

impl WorkItemsContinuation {
    fn capture(store: &ThreadDomainStore, action: &SemanticTimelineRequestAction) -> Self {
        let SemanticTimelineRequestAction::TurnWorkItemsGet { params, .. } = action else {
            return Self::default();
        };
        let range = store
            .semantic
            .thread(&params.thread_id)
            .and_then(|thread| thread.work_range(&params.turn_id));
        let baseline = params
            .work_item_ids
            .iter()
            .map(|id| (id, range.and_then(|range| range.items_by_id.get(id))))
            .collect::<HashMap<_, _>>();
        let Ok(bytes) = semantic_buffer_size(&baseline, SEMANTIC_CONTINUATION_BYTE_LIMIT) else {
            return Self {
                capture_failed: true,
                ..Self::default()
            };
        };
        Self {
            baseline: baseline
                .into_iter()
                .map(|(id, item)| (id.clone(), item.cloned()))
                .collect(),
            bytes,
            ..Self::default()
        }
    }
}

#[derive(Debug)]
struct TimelineContinuationFull;
impl std::fmt::Display for TimelineContinuationFull {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Timeline continuation capacity exceeded")
    }
}
impl std::error::Error for TimelineContinuationFull {}

fn semantic_buffer_size(value: &impl serde::Serialize, limit: usize) -> anyhow::Result<usize> {
    struct Counter {
        bytes: usize,
        limit: usize,
    }
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > self.limit.saturating_sub(self.bytes) {
                return Err(std::io::Error::other(
                    "Timeline continuation capacity exceeded",
                ));
            }
            self.bytes += bytes.len();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter { bytes: 0, limit };
    serde_json::to_writer(&mut counter, value)
        .map_err(|_| anyhow::anyhow!(TimelineContinuationFull))?;
    Ok(counter.bytes)
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
    baseline: HashMap<String, pioneer_protocol::TimelineBlock>,
    baseline_bytes: usize,
    capture_failed: bool,
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
        let baseline = &store
            .snapshot
            .as_ref()?
            .semantic
            .thread(thread_id)?
            .top_level
            .blocks_by_id;
        let baseline_bytes = semantic_buffer_size(baseline, SEMANTIC_CONTINUATION_BYTE_LIMIT);
        let mut walk = Self {
            first_cursor: cache.loaded_range.before_cursor.clone(),
            last_cursor: cache.loaded_range.after_cursor.clone(),
            last_sort_key: cache.ordered_blocks().last()?.sort_key.clone(),
            anchor: TimelinePageAnchor::Newest,
            remaining: CACHED_TIMELINE_REQUEST_BUDGET,
            previous_sort_key: None,
            visited_cursors: HashSet::new(),
            read_last_boundary: false,
            baseline: if baseline_bytes.is_ok() {
                baseline.clone()
            } else {
                HashMap::new()
            },
            baseline_bytes: baseline_bytes.as_ref().copied().unwrap_or(0),
            capture_failed: baseline_bytes.is_err(),
        };
        walk.capture_failed |= walk.check_capacity().is_err();
        if walk.capture_failed {
            // Keep only the terminal outcome when even captured metadata exceeds
            // the budget; no oversized baseline/cursor survives in the queue.
            walk.baseline.clear();
            walk.baseline_bytes = 0;
            walk.first_cursor = None;
            walk.last_cursor = None;
            walk.last_sort_key = String::new();
        }
        Some(walk)
    }

    fn check_capacity(&self) -> anyhow::Result<()> {
        anyhow::ensure!(!self.capture_failed, TimelineContinuationFull);
        semantic_buffer_size(
            &(
                &self.first_cursor,
                &self.last_cursor,
                &self.last_sort_key,
                &self.anchor,
                &self.previous_sort_key,
                &self.visited_cursors,
            ),
            SEMANTIC_CONTINUATION_BYTE_LIMIT.saturating_sub(self.baseline_bytes),
        )?;
        Ok(())
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
        self.check_capacity()?;
        Ok(true)
    }
}

impl ClientCore {
    pub fn begin_thread_semantic_request(
        &self,
        action: SemanticTimelineRequestAction,
    ) -> anyhow::Result<Option<ThreadSemanticRequest>> {
        self.accept_timeline_request(action, None, None, false, false)
    }

    fn accept_timeline_request(
        &self,
        action: SemanticTimelineRequestAction,
        reserved: Option<u64>,
        expected_store: Option<u64>,
        notification: bool,
        automatic_hydration: bool,
    ) -> anyhow::Result<Option<ThreadSemanticRequest>> {
        let explicit_intent = reserved.is_none() && !notification && !automatic_hydration;
        if self.is_stopped() {
            return Ok(None);
        }
        let id = request_thread_id(crate::timeline::semantic::semantic_timeline_request_key(
            &action,
        ))
        .to_owned();
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        let Some(store) = registry.stores.get(&id) else {
            return Ok(None);
        };
        if expected_store.is_some_and(|generation| store.generation != generation) {
            return Ok(None);
        }
        if let SemanticTimelineRequestAction::TurnWorkItemsGet { params, .. } = &action
            && !crate::timeline::semantic::accepts_turn_work_items_change(
                &store.semantic,
                &id,
                &params.turn_id,
            )
        {
            let key = crate::timeline::semantic::semantic_timeline_request_key(&action);
            if reserved.is_some_and(|generation| registry.timeline.accepts(key, generation)) {
                registry.timeline.reject_admission(key);
            }
            anyhow::bail!("Turn work collection authority retired");
        }
        if automatic_hydration {
            let SemanticTimelineRequestKey::TurnWorkInitial { turn_id, .. } =
                crate::timeline::semantic::semantic_timeline_request_key(&action)
            else {
                return Ok(None);
            };
            if store.presentation_blocked || store.demand == ClientDemand::Suspended {
                return Ok(None);
            }
            let Some(thread) = store.semantic.thread(&id) else {
                return Ok(None);
            };
            let Some(work) = thread.cached_turn_work_block(turn_id) else {
                return Ok(None);
            };
            if !crate::timeline::semantic::resolve_work_expanded(work, &thread.expansion)
                || (work.visible_work_count == 0 && !work.has_more_before && !work.has_more_after)
            {
                return Ok(None);
            }
            let range = thread.work_range(turn_id);
            if registry.timeline.in_flight.contains(
                crate::timeline::semantic::semantic_timeline_request_key(&action),
            ) || range.is_some_and(|range| {
                range.has_loaded_page
                    || matches!(
                        range.initial_request_status,
                        crate::timeline::semantic::TimelineRequestStatus::Loading { .. }
                            | crate::timeline::semantic::TimelineRequestStatus::Failed { .. }
                    )
                    || matches!(
                        range.request_status,
                        crate::timeline::semantic::TimelineRequestStatus::Loading { .. }
                    )
            }) || !registry.timeline.page_retry_allowed(
                crate::timeline::semantic::semantic_timeline_request_key(&action),
                self.timeline_now_ms(),
            ) {
                return Ok(None);
            }
        }
        // Live reduction may have rearmed an exhausted cycle. The worker owns
        // its exact Newest action and deadline; notification admission must not
        // bypass that cooldown or an existing recovery's backoff/budget.
        if notification
            && matches!(
                crate::timeline::semantic::semantic_timeline_request_key(&action),
                SemanticTimelineRequestKey::ThreadNewest { .. }
            )
            && registry.timeline.has_failed_refresh(&id)
            && !registry.timeline.has_active_refresh(&id)
        {
            return Ok(None);
        }
        let action = if let Some(generation) = reserved {
            if !registry.timeline.accepts(
                crate::timeline::semantic::semantic_timeline_request_key(&action),
                generation,
            ) {
                return Ok(None);
            }
            action
        } else {
            let key = crate::timeline::semantic::semantic_timeline_request_key(&action);
            let reset_retry = !notification
                && !registry.timeline.in_flight.contains(key)
                && (!matches!(key, SemanticTimelineRequestKey::ThreadNewest { .. })
                    || is_newest_refresh(&action)
                    || !registry.timeline.has_failed_refresh(&id));
            let key = key.clone();
            match registry.timeline.begin(action) {
                Ok(Some(action)) => {
                    if reset_retry {
                        registry.timeline.reset_page_retry(&key);
                    }
                    registry.timeline.retain_domain_request(&key);
                    action
                }
                Ok(None) => {
                    registry.timeline.retain_domain_request(&key);
                    if explicit_intent {
                        registry.timeline.mark_pending_explicit(&key);
                    }
                    return Ok(None);
                }
                Err(action) => {
                    let expected_store = registry.stores[&id].generation;
                    let expected_request = registry.timeline.request_generation(&key);
                    drop(registry);
                    self.fail_timeline_admission(&action, expected_store, expected_request);
                    anyhow::bail!("History pending capacity exceeded");
                }
            }
        };
        let request_generation = registry.timeline.request_generation(
            crate::timeline::semantic::semantic_timeline_request_key(&action),
        );
        let recovery = is_newest_refresh(&action)
            && (notification || registry.timeline.has_failed_refresh(&id));
        let preserve_refresh_status = matches!(
            &action,
            SemanticTimelineRequestAction::ThreadTimelinePage { .. }
        ) && !is_newest_refresh(&action)
            && (registry.timeline.has_active_refresh(&id)
                || registry.timeline.has_failed_refresh(&id));
        let Some(store) = registry.stores.get_mut(&id) else {
            return Ok(None);
        };
        let generation = store.generation;
        let reconciliation = CachedTimelineReconciliation::capture(store, &action);
        let work_items = WorkItemsContinuation::capture(store, &action);
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
        Ok(Some(ThreadSemanticRequest {
            id,
            generation,
            request_generation,
            explicit_intent,
            recovery,
            action,
            reconciliation,
            work_items,
        }))
    }
    pub fn schedule_thread_semantic_request(
        &self,
        action: SemanticTimelineRequestAction,
    ) -> anyhow::Result<()> {
        self.schedule_thread_semantic_request_generation(action, None)
    }

    fn schedule_thread_semantic_request_generation(
        &self,
        action: SemanticTimelineRequestAction,
        expected_store: Option<u64>,
    ) -> anyhow::Result<()> {
        self.schedule_thread_semantic_request_inner(action, expected_store, false, false)
    }

    fn schedule_thread_semantic_request_inner(
        &self,
        action: SemanticTimelineRequestAction,
        expected_store: Option<u64>,
        notification: bool,
        automatic_hydration: bool,
    ) -> anyhow::Result<()> {
        // Presentation hints can arrive before the request worker is installed.
        // Defer admission so they do not become terminal hydration failures.
        if automatic_hydration
            && self
                .thread_request_sender
                .lock()
                .expect("thread request sender poisoned")
                .is_none()
        {
            return Ok(());
        }
        if let Some(request) = self.accept_timeline_request(
            action,
            None,
            expected_store,
            notification,
            automatic_hydration,
        )? {
            self.send_timeline_request(request)?;
        }
        Ok(())
    }

    pub(crate) fn schedule_planned_timeline_request(
        &self,
        page: crate::timeline::controller::TimelinePageRequest,
    ) -> anyhow::Result<()> {
        if let Some(request) =
            self.accept_timeline_request(page.action, Some(page.generation), None, false, false)?
        {
            self.send_timeline_request(request)?;
        }
        Ok(())
    }

    fn send_timeline_request(&self, request: ThreadSemanticRequest) -> anyhow::Result<()> {
        let sender = self
            .thread_request_sender
            .lock()
            .expect("thread request sender poisoned")
            .clone();
        self.retire_thread_controller_jobs();
        let mut full = false;
        let request = if let Some(sender) = sender {
            match sender.send(ThreadControllerRequest::Semantic(request)) {
                Ok(()) => return Ok(()),
                Err(error) => {
                    full = !sender.is_closed();
                    let ThreadControllerRequest::Semantic(request) = error.0 else {
                        unreachable!()
                    };
                    request
                }
            }
        } else {
            request
        };
        if full {
            let mut registry = self
                .thread_registry
                .lock()
                .expect("thread registry poisoned");
            if !self.is_stopped()
                && registry
                    .stores
                    .get(&request.id)
                    .is_some_and(|store| store.generation == request.generation)
                && registry.timeline.defer_dispatch(
                    crate::timeline::semantic::semantic_timeline_request_key(&request.action),
                    request.request_generation,
                    self.timeline_now_ms(),
                )
            {
                set_request_status(
                    &mut registry.stores.get_mut(&request.id).unwrap().semantic,
                    crate::timeline::semantic::semantic_timeline_request_key(&request.action),
                    crate::timeline::semantic::TimelineRequestStatus::Failed {
                        message: "History worker full; recovery deferred".into(),
                    },
                );
                let drafts = registry.publish(&request.id);
                self.transition_directory(
                    &mut registry,
                    &ClientMutationAuthority { _private: () },
                    drafts,
                    vec![],
                );
                anyhow::bail!("History worker full; recovery deferred");
            }
            drop(registry);
        }
        self.fail_timeline_dispatch(&request);
        anyhow::bail!("History worker full or unavailable")
    }

    pub(crate) fn fail_timeline_admission(
        &self,
        action: &SemanticTimelineRequestAction,
        expected_store: u64,
        expected_request: u64,
    ) {
        // Zero denotes no admitted request, not an incarnation. It can recur
        // after a newer success; Err/Rejected already completes that refusal.
        if expected_request == 0 {
            return;
        }
        let key = crate::timeline::semantic::semantic_timeline_request_key(action);
        let id = request_thread_id(key);
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        if self.is_stopped()
            || !registry
                .stores
                .get(id)
                .is_some_and(|store| store.generation == expected_store)
            || registry.timeline.request_generation(key) != expected_request
            || !registry.timeline.reject_admission(key)
        {
            return;
        }
        // A never-admitted turn key has no Loading to clear. Its caller gets
        // Rejected/Err; do not turn overload into an unbounded map of empty
        // failed work ranges in the semantic cache.
        let has_status = registry.stores[id]
            .semantic
            .thread(id)
            .is_some_and(|thread| {
                use SemanticTimelineRequestKey::*;
                match key {
                    ThreadNewest { .. } | ThreadBefore { .. } | ThreadAfter { .. } => true,
                    TurnWorkInitial { turn_id, .. }
                    | TurnWorkBefore { turn_id, .. }
                    | TurnWorkAfter { turn_id, .. }
                    | TurnWorkItems { turn_id, .. } => thread.work_range(turn_id).is_some(),
                }
            });
        if !has_status {
            return;
        }
        set_request_status(
            &mut registry.stores.get_mut(id).unwrap().semantic,
            key,
            crate::timeline::semantic::TimelineRequestStatus::Failed {
                message: "History pending capacity exceeded; retry".into(),
            },
        );
        let drafts = registry.publish(id);
        self.transition_directory(
            &mut registry,
            &ClientMutationAuthority { _private: () },
            drafts,
            vec![],
        );
    }

    fn fail_timeline_dispatch(&self, request: &ThreadSemanticRequest) {
        let key = crate::timeline::semantic::semantic_timeline_request_key(&request.action);
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        if self.is_stopped()
            || !registry.timeline.accepts(key, request.request_generation)
            || !registry
                .stores
                .get(&request.id)
                .is_some_and(|store| store.generation == request.generation)
        {
            return;
        }
        if let SemanticTimelineRequestAction::TurnWorkItemsGet { params, .. } = &request.action
            && !crate::timeline::semantic::accepts_turn_work_items_change(
                &registry.stores[&request.id].semantic,
                &request.id,
                &params.turn_id,
            )
        {
            registry.timeline.reject_admission(key);
            return;
        }
        let preserve_refresh_status = matches!(
            &request.action,
            SemanticTimelineRequestAction::ThreadTimelinePage { .. }
        ) && !is_newest_refresh(&request.action)
            && (registry.timeline.has_failed_refresh(&request.id)
                || registry.timeline.has_active_refresh(&request.id));
        // A closed executor cannot perform recovery. Retain the explicit error,
        // retire its queue slot, and wait for manual Retry or reconnection.
        registry
            .timeline
            .finish_with_retry(key, false, false, self.timeline_now_ms());
        if !preserve_refresh_status {
            set_request_status(
                &mut registry.stores.get_mut(&request.id).unwrap().semantic,
                key,
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

    pub fn execute_thread_semantic_request(
        &self,
        transport: &impl crate::rpc::JsonRpcRequestTransport,
        request: ThreadSemanticRequest,
    ) -> Option<ThreadSemanticRequest> {
        let prepared = self.prepare_thread_semantic_request(request)?;
        let result = prepared.rpc.as_ref().map_or_else(
            || Ok(serde_json::Value::Null),
            |(method, params)| {
                crate::rpc::send_json_rpc_request_value(
                    transport,
                    method,
                    params.clone(),
                    crate::rpc::RPC_REQUEST_TIMEOUT,
                )
            },
        );
        self.complete_thread_semantic_request(prepared, result, None)
    }

    pub(crate) fn prepare_thread_semantic_request(
        &self,
        request: ThreadSemanticRequest,
    ) -> Option<PreparedSemanticRequest> {
        use crate::timeline::semantic::*;
        if self.is_stopped() {
            return None;
        }
        let key = semantic_timeline_request_key(&request.action).clone();
        let initial_items = {
            let registry = self
                .thread_registry
                .lock()
                .expect("thread registry poisoned");
            if !registry.stores.get(&request.id).is_some_and(|store| {
                store.generation == request.generation
                    && registry.timeline.accepts(&key, request.request_generation)
            }) {
                return None;
            }
            match &request.action {
                SemanticTimelineRequestAction::TurnWorkPage {
                    key: SemanticTimelineRequestKey::TurnWorkInitial { .. },
                    params,
                } => initial_work_item_snapshot(
                    &registry.stores[&request.id].semantic,
                    &request.id,
                    &params.turn_id,
                ),
                _ => None,
            }
        };
        let expected_turn = match &request.action {
            SemanticTimelineRequestAction::ThreadTimelinePage { .. } => None,
            SemanticTimelineRequestAction::TurnWorkPage { params, .. } => {
                Some(params.turn_id.clone())
            }
            SemanticTimelineRequestAction::TurnWorkItemsGet { params, .. } => {
                Some(params.turn_id.clone())
            }
        };
        let rpc = match request.action.clone() {
            SemanticTimelineRequestAction::ThreadTimelinePage { mut params, .. } => {
                if let Some(walk) = &request.reconciliation {
                    params.anchor = walk.anchor.clone();
                    if params.anchor != pioneer_protocol::TimelinePageAnchor::Newest {
                        params.limit = Some(CACHED_TIMELINE_PAGE_LIMIT);
                    }
                }
                Some((
                    pioneer_protocol::constants::methods::THREAD_TIMELINE_PAGE,
                    serde_json::to_value(params).expect("timeline params"),
                ))
            }
            SemanticTimelineRequestAction::TurnWorkPage { params, .. } => Some((
                pioneer_protocol::constants::methods::TURN_WORK_PAGE,
                serde_json::to_value(params).expect("work params"),
            )),
            SemanticTimelineRequestAction::TurnWorkItemsGet { mut params, .. } => {
                let end = (request.work_items.next
                    + crate::timeline::controller::WORK_ITEM_CHUNK_LIMIT)
                    .min(params.work_item_ids.len());
                if request.work_items.next == end {
                    None
                } else {
                    params.work_item_ids =
                        params.work_item_ids[request.work_items.next..end].to_vec();
                    Some((
                        pioneer_protocol::constants::methods::TURN_WORK_ITEMS_GET,
                        serde_json::to_value(params).expect("items params"),
                    ))
                }
            }
        };
        let rpc = if request.work_items.capture_failed
            || request
                .reconciliation
                .as_ref()
                .is_some_and(|w| w.capture_failed)
        {
            None
        } else {
            rpc
        };
        Some(PreparedSemanticRequest {
            rpc,
            request,
            key,
            initial_items,
            expected_turn,
        })
    }
    pub(crate) fn complete_thread_semantic_request(
        &self,
        prepared: PreparedSemanticRequest,
        response: anyhow::Result<serde_json::Value>,
        authority: Option<&crate::core::HistoryAuthority>,
    ) -> Option<ThreadSemanticRequest> {
        use crate::timeline::semantic::*;
        let PreparedSemanticRequest {
            mut request,
            key,
            initial_items,
            expected_turn,
            ..
        } = prepared;
        enum Page {
            Thread(
                pioneer_protocol::ThreadTimelinePageResponse,
                TopLevelPageMergeMode,
            ),
            Work(pioneer_protocol::TurnWorkPageResponse, WorkPageMergeMode),
            Items(Vec<pioneer_protocol::TurnWorkItemsGetResponse>),
        }
        let result = if request.work_items.capture_failed
            || request
                .reconciliation
                .as_ref()
                .is_some_and(|w| w.capture_failed)
        {
            Err(anyhow::anyhow!(TimelineContinuationFull))
        } else {
            match &request.action {
                SemanticTimelineRequestAction::ThreadTimelinePage { key, .. } => {
                    let mode = match key {
                        SemanticTimelineRequestKey::ThreadBefore { .. } => {
                            TopLevelPageMergeMode::MergeBefore
                        }
                        SemanticTimelineRequestKey::ThreadAfter { .. } => {
                            TopLevelPageMergeMode::MergeAfter
                        }
                        _ => TopLevelPageMergeMode::Merge,
                    };
                    response.and_then(|v| Ok(Page::Thread(serde_json::from_value(v)?, mode)))
                }
                SemanticTimelineRequestAction::TurnWorkPage { key, .. } => {
                    let mode = match key {
                        SemanticTimelineRequestKey::TurnWorkBefore { .. } => {
                            WorkPageMergeMode::MergeBefore
                        }
                        _ => WorkPageMergeMode::MergeAfter,
                    };
                    response.and_then(|v| Ok(Page::Work(serde_json::from_value(v)?, mode)))
                }
                SemanticTimelineRequestAction::TurnWorkItemsGet { params, .. } => {
                    let end = (request.work_items.next
                        + crate::timeline::controller::WORK_ITEM_CHUNK_LIMIT)
                        .min(params.work_item_ids.len());
                    if request.work_items.next == end {
                        Ok(Page::Items(std::mem::take(&mut request.work_items.pages)))
                    } else {
                        response.and_then(|v| {
                            let page: pioneer_protocol::TurnWorkItemsGetResponse =
                                serde_json::from_value(v)?;
                            let bytes = semantic_buffer_size(
                                &page,
                                SEMANTIC_CONTINUATION_BYTE_LIMIT
                                    .saturating_sub(request.work_items.bytes),
                            )?;
                            request.work_items.next = end;
                            request.work_items.bytes += bytes;
                            request.work_items.pages.push(page);
                            Ok(Page::Items(std::mem::take(&mut request.work_items.pages)))
                        })
                    }
                }
            }
        };
        let authority_guard = authority.map(|a| a.lock(self));
        let result = if authority_guard.as_ref().is_some_and(|a| !a.1) {
            Err(anyhow::anyhow!("History authority retired at apply"))
        } else {
            result
        };
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        let transport_guard = authority.map(|a| a.lock_transport());
        let result = if transport_guard.as_ref().is_some_and(|g| !g.1) {
            Err(anyhow::anyhow!("History socket retired at apply"))
        } else {
            result
        };
        if !registry.timeline.accepts(&key, request.request_generation) {
            return None;
        }
        let Some(store) = registry.stores.get(&request.id) else {
            return None;
        };
        if self.is_stopped() || store.generation != request.generation {
            return None;
        }
        if let SemanticTimelineRequestAction::TurnWorkItemsGet { params, .. } = &request.action
            && !accepts_turn_work_items_change(&store.semantic, &request.id, &params.turn_id)
        {
            // Current authority retired the whole range. Drop this exact collection
            // (including trailing IDs/evidence) without creating a Failed range.
            registry.timeline.reject_admission(&key);
            return None;
        }
        let removals = registry
            .timeline
            .request_removals(&key, request.request_generation)
            .expect("accepted removal owner")
            .clone();
        let result = if removals.is_full() {
            Err(anyhow::anyhow!(TimelineContinuationFull))
        } else {
            result
        };
        let refresh_key = SemanticTimelineRequestKey::ThreadNewest {
            thread_id: request.id.clone(),
        };
        let is_refresh = is_newest_refresh(&request.action);
        let preserve_refresh_status = !is_refresh
            && (registry.timeline.has_active_refresh(&request.id)
                || registry.timeline.has_failed_refresh(&request.id));
        let preserve_failure_status = preserve_refresh_status
            && matches!(
                &request.action,
                SemanticTimelineRequestAction::ThreadTimelinePage { .. }
            );
        let store = registry
            .stores
            .get_mut(&request.id)
            .expect("validated request store");
        let valid_scope = |workspace: &str, id: &str| {
            workspace == store.coordinator.workspace_id && id == request.id
        };
        let mut retryable = result.as_ref().err().is_some_and(timeline_error_retryable);
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
                    page.blocks.retain_mut(|incoming| {
                        if removals.contains(&incoming.block_id) {
                            return false;
                        }
                        let cached = store
                            .semantic
                            .thread(&request.id)
                            .and_then(|thread| thread.top_level.block(&incoming.block_id));
                        let baseline = walk.baseline.get(&incoming.block_id);
                        if cached == baseline || cached == Some(&*incoming) {
                            return true;
                        }
                        // A live removal remains authoritative. For an existing
                        // Task, retain fresh fields but apply the reauthorized list.
                        if baseline.is_some()
                            && let Some(merged) = cached.and_then(|cached| {
                                crate::timeline::semantic::merge_fetched_codex_retry_notices(
                                    cached, incoming,
                                )
                            })
                        {
                            *incoming = merged;
                            return true;
                        }
                        false
                    });
                }
                page.blocks
                    .retain(|block| !removals.contains(&block.block_id));
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
                        retryable = false;
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
                if let Err(error) = apply_requested_work_page(
                    &mut store.semantic,
                    page,
                    mode,
                    initial_items.as_ref(),
                ) {
                    succeeded = false;
                    set_request_status(
                        &mut store.semantic,
                        &key,
                        TimelineRequestStatus::Failed {
                            message: error.to_string(),
                        },
                    );
                }
            }
            Ok(Page::Items(mut pages))
                if pages.iter().all(|page| {
                    valid_scope(&page.workspace_id, &page.thread_id)
                        && expected_turn.as_deref() == Some(page.turn_id.as_str())
                }) =>
            {
                if let SemanticTimelineRequestAction::TurnWorkItemsGet { params, .. } =
                    &request.action
                    && request.work_items.next < params.work_item_ids.len()
                {
                    request.work_items.pages = pages;
                    return Some(request);
                }
                for page in &mut pages {
                    // Chunks apply only after the entire collection succeeds.
                    // Preserve values edited or removed live since admission.
                    let range = store
                        .semantic
                        .thread(&request.id)
                        .and_then(|thread| thread.work_range(&page.turn_id));
                    page.items.retain(|incoming| {
                        if removals.contains(&incoming.work_item_id) {
                            return false;
                        }
                        let cached =
                            range.and_then(|range| range.items_by_id.get(&incoming.work_item_id));
                        let baseline = request
                            .work_items
                            .baseline
                            .get(&incoming.work_item_id)
                            .and_then(Option::as_ref);
                        cached == baseline || cached == Some(incoming)
                    });
                    page.removed_work_item_ids.retain(|id| {
                        let cached = range.and_then(|range| range.items_by_id.get(id));
                        let baseline = request.work_items.baseline.get(id).and_then(Option::as_ref);
                        cached == baseline
                    });
                }
                for page in pages {
                    apply_turn_work_items_get_response(&mut store.semantic, page);
                }
            }
            Ok(_) => {
                succeeded = false;
                retryable = false;
                if !preserve_failure_status {
                    set_request_status(
                        &mut store.semantic,
                        &key,
                        TimelineRequestStatus::Failed {
                            message: "Timeline response scope mismatch".into(),
                        },
                    );
                }
            }
            Err(error) if !preserve_failure_status => set_request_status(
                &mut store.semantic,
                &key,
                TimelineRequestStatus::Failed {
                    message: format!("{error:#}"),
                },
            ),
            Err(_) => {}
        }
        let retired = registry.retire_unauthorized_targeted_requests(&request.id);
        drop(transport_guard);
        drop(authority_guard);
        if continue_reconciliation {
            let drafts = registry.publish(&request.id);
            self.transition_directory(
                &mut registry,
                &ClientMutationAuthority { _private: () },
                drafts,
                vec![],
            );
            drop(registry);

            if retired {
                self.retire_thread_controller_jobs();
            }
            return Some(request);
        }
        let next_explicit = registry.timeline.take_pending_explicit(&key);
        let next =
            registry
                .timeline
                .finish_with_retry(&key, succeeded, retryable, self.timeline_now_ms());
        let mut rejected = None;
        let next = match next.map(|action| registry.timeline.begin(action)) {
            Some(Ok(next)) => next,
            Some(Err(action)) => {
                rejected = Some(action);
                None
            }
            None => None,
        };
        if next.is_some() && matches!(key, SemanticTimelineRequestKey::TurnWorkInitial { .. }) {
            let store = registry.stores.get_mut(&request.id).expect("request owner");
            set_request_status(
                &mut store.semantic,
                &key,
                TimelineRequestStatus::Loading {
                    request_key: format!("{key:?}"),
                },
            );
        }
        let next_generation = registry.timeline.request_generation(&key);
        if next.as_ref().is_some_and(is_newest_refresh) {
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
        let next_work_items = next
            .as_ref()
            .map(|action| WorkItemsContinuation::capture(&registry.stores[&request.id], action))
            .unwrap_or_default();
        self.transition_directory(
            &mut registry,
            &ClientMutationAuthority { _private: () },
            drafts,
            vec![],
        );
        drop(registry);
        if retired {
            self.retire_thread_controller_jobs();
        }
        if let Some(action) = rejected {
            self.fail_timeline_admission(&action, request.generation, 0);
        }
        let Some(action) = next else {
            return None;
        };
        let next_recovery = is_newest_refresh(&action) && !next_explicit;
        let release_recovery_reservation = request.recovery && !next_recovery;
        request.recovery = next_recovery;
        request.action = action;
        request.explicit_intent = next_explicit;
        request.request_generation = next_generation;
        request.reconciliation = next_reconciliation;
        request.work_items = next_work_items;
        if release_recovery_reservation {
            // This is a new logical follow-up, not another page of the
            // recovery. Ordinary work must obtain ordinary admission rather
            // than inheriting the reserved recovery slot indefinitely.
            let _ = self.send_timeline_request(request);
            return None;
        }
        Some(request)
    }
}
// A request-local comparison, not a server revision: a changed Running body
// cannot safely be replaced by a snapshot whose relation to the delta is unknown.
fn initial_work_item_snapshot(
    state: &SemanticTimelineState,
    thread_id: &str,
    turn_id: &str,
) -> Option<HashMap<String, pioneer_protocol::TurnItem>> {
    let range = state
        .thread(thread_id)
        .and_then(|thread| thread.work_range(turn_id));
    if range.is_some_and(|range| range.has_loaded_page) {
        return None;
    }
    Some(
        range
            .into_iter()
            .flat_map(|range| range.ordered_items())
            .filter(|item| item.status == pioneer_protocol::TurnWorkItemStatus::Running)
            .map(|item| (item.item_id.clone(), item.item.clone()))
            .collect(),
    )
}

fn apply_requested_work_page(
    state: &mut SemanticTimelineState,
    page: pioneer_protocol::TurnWorkPageResponse,
    mode: crate::timeline::semantic::WorkPageMergeMode,
    initial_items: Option<&HashMap<String, pioneer_protocol::TurnItem>>,
) -> anyhow::Result<()> {
    if let Some(initial_items) = initial_items
        && let Some(range) = state
            .thread(&page.thread_id)
            .and_then(|thread| thread.work_range(&page.turn_id))
    {
        let conflicts = page.items.iter().any(|incoming| {
            incoming.status == pioneer_protocol::TurnWorkItemStatus::Running
                && (mode == crate::timeline::semantic::WorkPageMergeMode::Reset
                    || crate::timeline::semantic::should_merge_turn_work_item(range, incoming))
                && range.ordered_items().any(|current| {
                    current.item_id == incoming.item_id
                        && current.status == pioneer_protocol::TurnWorkItemStatus::Running
                        && initial_items.get(&current.item_id) != Some(&current.item)
                        && incoming.item != current.item
                })
        });
        anyhow::ensure!(
            !conflicts,
            "Work history changed during initial loading; retry"
        );
    }
    crate::timeline::semantic::apply_turn_work_page(state, page, mode);
    Ok(())
}

fn set_request_status(
    state: &mut SemanticTimelineState,
    key: &SemanticTimelineRequestKey,
    status: crate::timeline::semantic::TimelineRequestStatus,
) {
    use SemanticTimelineRequestKey::*;
    // Targeted cleanup (including cancellation) cannot create a range that
    // current authority removed. Other page/status policies stay unchanged.
    if let TurnWorkItems { thread_id, turn_id } = key
        && !crate::timeline::semantic::accepts_turn_work_items_change(state, thread_id, turn_id)
    {
        return;
    }
    let thread = state.thread_mut(request_thread_id(key));
    match key {
        ThreadNewest { .. } | ThreadBefore { .. } | ThreadAfter { .. } => {
            thread.top_level.request_status = status
        }
        TurnWorkInitial { turn_id, .. } => {
            let range = thread.work_range_mut(turn_id.clone());
            range.initial_request_status = status.clone();
            range.request_status = status;
        }
        TurnWorkBefore { turn_id, .. }
        | TurnWorkAfter { turn_id, .. }
        | TurnWorkItems { turn_id, .. } => {
            thread.work_range_mut(turn_id.clone()).request_status = status
        }
    }
}

fn timeline_error_retryable(error: &anyhow::Error) -> bool {
    use crate::rpc::JsonRpcResponseError;
    if error
        .chain()
        .any(|cause| cause.is::<serde_json::Error>() || cause.is::<TimelineContinuationFull>())
    {
        return false;
    }
    match crate::rpc::json_rpc_response_error(error) {
        Some(error) if error.authorization_failure().is_some() => false,
        Some(JsonRpcResponseError::InvalidResponse) => false,
        Some(JsonRpcResponseError::Server {
            code: Some(code), ..
        }) if matches!(
            *code,
            pioneer_protocol::INVALID_REQUEST_CODE
                | pioneer_protocol::INVALID_PARAMS_CODE
                | pioneer_protocol::METHOD_NOT_FOUND_CODE
                | pioneer_protocol::PARSE_ERROR_CODE
        ) =>
        {
            false
        }
        _ => true,
    }
}

fn thread_reconcile_actions(
    state: &SemanticTimelineState,
    update: &crate::timeline::semantic::SemanticTimelineLiveUpdate,
) -> Vec<SemanticTimelineRequestAction> {
    use crate::timeline::semantic::*;
    match update {
        SemanticTimelineLiveUpdate::ThreadTimelineBlocksChanged(n)
            if state.thread(&n.thread_id).is_some_and(|thread| {
                n.changed_block_ids.iter().any(|id| {
                    thread.top_level.stale_block_ids.contains(id)
                        && !n
                            .turn_work_tombstones
                            .iter()
                            .any(|tombstone| &tombstone.block_id == id)
                })
            }) =>
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
            let Some(range) = state
                .thread(&n.thread_id)
                .and_then(|thread| thread.work_range(&n.turn_id))
            else {
                return vec![];
            };
            let mut ids = n
                .changed_work_item_ids
                .iter()
                .filter(|id| range.stale_work_item_ids.contains(*id))
                .cloned()
                .collect::<Vec<_>>();
            if ids.is_empty() {
                return vec![];
            }
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
            .begin_thread_semantic_request(action)?
            .ok_or_else(|| anyhow::anyhow!("Timeline request cancelled or already pending"))?;
        let result = fetch();
        let retryable = result.as_ref().err().is_some_and(timeline_error_retryable);
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        let key = crate::timeline::semantic::semantic_timeline_request_key(&request.action);
        anyhow::ensure!(
            registry.timeline.accepts(key, request.request_generation),
            "Timeline request cancelled"
        );
        let preserve_refresh_status = matches!(
            &request.action,
            SemanticTimelineRequestAction::ThreadTimelinePage { .. }
        ) && !is_newest_refresh(&request.action)
            && registry.timeline.has_failed_refresh(&request.id);
        let store = registry
            .stores
            .get_mut(&request.id)
            .ok_or_else(|| anyhow::anyhow!("Timeline request cancelled"))?;
        anyhow::ensure!(
            !self.is_stopped() && store.generation == request.generation,
            "Timeline request cancelled"
        );
        let refresh_status = preserve_refresh_status.then(|| {
            store
                .semantic
                .thread(&request.id)
                .unwrap()
                .top_level
                .request_status
                .clone()
        });
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
        if let Some(status) = refresh_status {
            set_request_status(&mut store.semantic, key, status);
        } else if let Err(error) = &result {
            set_request_status(
                &mut store.semantic,
                key,
                crate::timeline::semantic::TimelineRequestStatus::Failed {
                    message: format!("{error:#}"),
                },
            );
        }
        let next_explicit = registry.timeline.take_pending_explicit(key);
        let next = registry.timeline.finish_with_retry(
            key,
            result.is_ok(),
            retryable,
            self.timeline_now_ms(),
        );
        let wake_recovery = registry.timeline.next_refresh_deadline().is_some();
        let drafts = registry.publish(&request.id);
        self.transition_directory(
            &mut registry,
            &ClientMutationAuthority { _private: () },
            drafts,
            vec![],
        );
        drop(registry);
        if let Some(next) = next {
            let _ = self.schedule_thread_semantic_request_inner(
                next,
                Some(request.generation),
                !next_explicit,
                false,
            );
        }
        // The worker may have waited without a deadline while a synchronous
        // Around/Oldest owned the shared key. Recheck recovery after it finishes.
        if wake_recovery
            && let Some(sender) = self
                .thread_request_sender
                .lock()
                .expect("thread request sender poisoned")
                .as_ref()
        {
            let _ = sender.send(ThreadControllerRequest::Resume);
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
        let initial = matches!(key, SemanticTimelineRequestKey::TurnWorkInitial { .. });
        let initial_items = std::cell::RefCell::new(None);
        self.fetch_thread_page(
            SemanticTimelineRequestAction::TurnWorkPage {
                key,
                params: params.clone(),
            },
            || {
                if initial {
                    *initial_items.borrow_mut() = initial_work_item_snapshot(
                        &self.thread_semantic_snapshot(&params.thread_id),
                        &params.thread_id,
                        &params.turn_id,
                    );
                }
                crate::transport::ws::command_sender::turn_work_page(transport, params)
            },
            |state, _conversation, workspace, id, page| {
                anyhow::ensure!(
                    page.workspace_id == workspace && page.thread_id == id && page.turn_id == turn,
                    "Timeline response scope mismatch"
                );
                apply_requested_work_page(
                    state,
                    page.clone(),
                    mode,
                    initial_items.borrow().as_ref(),
                )
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

/// A receipt binds authority and store generation to one physical socket.
#[derive(Clone, Debug, PartialEq, Eq)]
struct ThreadSubscriptionScope {
    workspace: String,
    generation: u64,
    transport_revision: u64,
    auth: (u64, Option<u64>),
    authority: (u64, u64),
    socket: crate::transport::ws::runtime::ConnectionScope,
}
#[derive(Clone, Debug)]
struct ThreadSubscriptionReceipt {
    scope: ThreadSubscriptionScope,
    live: Arc<std::sync::Mutex<bool>>,
}
impl PartialEq for ThreadSubscriptionReceipt {
    fn eq(&self, other: &Self) -> bool {
        self.scope == other.scope && Arc::ptr_eq(&self.live, &other.live)
    }
}
impl Eq for ThreadSubscriptionReceipt {}
impl ThreadSubscriptionReceipt {
    fn retire(&self) {
        *self.live.lock().expect("subscription receipt poisoned") = false;
    }
}
struct ThreadSubscriptionFlight {
    queued: bool,
    completing: bool,
    deadline: std::time::Instant,
    baseline: Option<ThreadSubscriptionBaseline>,
    receiver: Option<std::sync::mpsc::Receiver<crate::rpc::JsonRpcResponseResult>>,
    response: Option<crate::rpc::JsonRpcResponseSender>,
    result: Option<Result<(), ThreadSubscriptionFailure>>,
    hydrate: bool,
    hydration_claimed: bool,
}
impl Drop for ThreadSubscriptionFlight {
    fn drop(&mut self) {
        if let Some(response) = self.response.take() {
            response.cancel();
        }
    }
}

#[derive(Clone, Debug)]
struct ThreadSubscriptionFailure {
    message: String,
    rpc: Option<crate::rpc::JsonRpcResponseError>,
}
impl ThreadSubscriptionFailure {
    fn from_error(error: &anyhow::Error) -> Self {
        Self {
            message: format!("{error:#}"),
            rpc: crate::rpc::json_rpc_response_error(error).cloned(),
        }
    }
    fn error(&self) -> anyhow::Error {
        match &self.rpc {
            Some(error) => anyhow::Error::new(error.clone()).context(self.message.clone()),
            None => anyhow::anyhow!(self.message.clone()),
        }
    }
}
#[derive(Clone)]
pub(crate) struct ThreadSubscriptionRequest {
    open_existing: bool,
    open_navigation_revision: Option<u64>,
    open_intent_generation: Option<u64>,
    pub(crate) id: String,
    pub(crate) workspace: String,
    pub(crate) generation: u64,
    transport_revision: u64,
    auth: (u64, Option<u64>),
    authority: (u64, u64),
    transport: crate::transport::ws::runtime::ConnectionRequestTransport,
    flight: Arc<std::sync::Mutex<ThreadSubscriptionFlight>>,
    live: Arc<std::sync::Mutex<bool>>,
    attempt: u32,
}
impl std::fmt::Debug for ThreadSubscriptionRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ThreadSubscriptionRequest")
            .field("id", &self.id)
            .field("workspace", &self.workspace)
            .field("generation", &self.generation)
            .field("transport_revision", &self.transport_revision)
            .field("socket", &self.transport.scope())
            .field("attempt", &self.attempt)
            .finish()
    }
}
impl PartialEq for ThreadSubscriptionRequest {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.flight, &other.flight)
    }
}
impl Eq for ThreadSubscriptionRequest {}
impl ThreadSubscriptionRequest {
    fn scope(&self) -> Option<ThreadSubscriptionScope> {
        Some(ThreadSubscriptionScope {
            workspace: self.workspace.clone(),
            generation: self.generation,
            transport_revision: self.transport_revision,
            auth: self.auth,
            authority: self.authority,
            socket: self.transport.scope()?,
        })
    }
    fn retire(&self) {
        *self.live.lock().expect("subscription receipt poisoned") = false;
        let response = {
            let mut flight = self.flight.lock().expect("subscription flight poisoned");
            if flight.result.is_none() {
                flight.result = Some(Err(ThreadSubscriptionFailure {
                    message: "Thread subscription retired".into(),
                    rpc: None,
                }));
            }
            flight.response.take()
        };
        if let Some(response) = response {
            response.cancel();
        }
    }
}

pub(crate) use super::history_scheduler::ThreadControllerSender;
pub(crate) struct ThreadResumeRequest {
    id: String,
    generation: u64,
    continuation: Option<ThreadResumeContinuation>,
}
fn retire_moved_resume(store: &mut ThreadDomainStore) {
    if store.coordinator.conversation.in_flight_turn_id().is_none() {
        super::resume::reset_thread_resume_coordinator(&mut store.coordinator.resume);
    } else {
        super::resume::finish_turn_resume_attempt(&mut store.coordinator.resume);
        store
            .coordinator
            .resume
            .next_attempt_at
            .get_or_insert(std::time::Instant::now());
    }
}
struct ThreadResumeContinuation {
    attempt: u64,
    snapshot: pioneer_protocol::TurnGetResponse,
    after: Option<i64>,
}
pub(crate) enum ThreadControllerRequest {
    Read(crate::timeline::controller::TimelineReadRequest),
    Semantic(ThreadSemanticRequest),
    Resume,
    ResumeJob(ThreadResumeRequest),
    Subscribe(ThreadSubscriptionRequest),
    Binding {
        id: String,
        workspace: String,
        generation: u64,
    },
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) enum HistoryIdentity {
    Semantic {
        id: String,
        generation: u64,
        request_generation: u64,
        key: SemanticTimelineRequestKey,
    },
    Read(crate::timeline::controller::TimelineReadRequest),
    Resume {
        id: String,
        generation: u64,
        turn_id: String,
        attempt: u64,
    },
    Store {
        id: String,
        generation: u64,
    },
}
impl HistoryIdentity {
    pub(crate) fn thread(&self) -> &str {
        match self {
            Self::Semantic { id, .. } | Self::Store { id, .. } | Self::Resume { id, .. } => id,
            Self::Read(r) => &r.thread_id,
        }
    }
    pub(crate) fn semantic(&self) -> bool {
        matches!(self, Self::Semantic { .. })
    }
    pub(crate) fn current(&self, registry: &ThreadRegistry) -> bool {
        match self {
            Self::Semantic {
                id,
                generation,
                request_generation,
                key,
            } => {
                registry
                    .stores
                    .get(id)
                    .is_some_and(|s| s.generation == *generation)
                    && registry.timeline.accepts(key, *request_generation)
            }
            Self::Read(r) => registry.timeline.accepts_read(r),
            Self::Resume {
                id,
                generation,
                turn_id,
                attempt,
            } => registry.stores.get(id).is_some_and(|store| {
                store.generation == *generation
                    && store.resume_attempt == *attempt
                    && store.coordinator.resume.in_progress
                    && store.coordinator.conversation.in_flight_turn_id() == Some(turn_id.as_str())
            }),
            Self::Store { id, generation } => registry
                .stores
                .get(id)
                .is_some_and(|s| s.generation == *generation),
        }
    }
}

impl ThreadControllerRequest {
    pub(crate) fn history_identity(&self) -> HistoryIdentity {
        match self {
            Self::Semantic(r) => HistoryIdentity::Semantic {
                id: r.id.clone(),
                generation: r.generation,
                request_generation: r.request_generation,
                key: crate::timeline::semantic::semantic_timeline_request_key(&r.action).clone(),
            },
            Self::Read(r) => HistoryIdentity::Read(r.clone()),
            Self::ResumeJob(r) => HistoryIdentity::Store {
                id: r.id.clone(),
                generation: r.generation,
            },
            Self::Binding { id, generation, .. } => HistoryIdentity::Store {
                id: id.clone(),
                generation: *generation,
            },
            Self::Subscribe(r) => HistoryIdentity::Store {
                id: r.id.clone(),
                generation: r.generation,
            },
            Self::Resume => unreachable!("wake has no reservation"),
        }
    }

    pub(crate) fn is_recovery(&self) -> bool {
        matches!(self, Self::ResumeJob(_))
            || matches!(self, Self::Semantic(request) if request.recovery)
    }
    pub(crate) fn same_job(&self, other: &Self) -> bool {
        use ThreadControllerRequest as R;
        match (self, other) {
            (R::Read(a), R::Read(b)) => a.thread_id == b.thread_id && a.generation == b.generation,
            (R::Semantic(a), R::Semantic(b)) => {
                a.id == b.id
                    && a.generation == b.generation
                    && a.request_generation == b.request_generation
                    && crate::timeline::semantic::semantic_timeline_request_key(&a.action)
                        == crate::timeline::semantic::semantic_timeline_request_key(&b.action)
            }
            (R::Subscribe(a), R::Subscribe(b)) => a == b,
            (
                R::Binding {
                    id: a,
                    generation: ag,
                    ..
                },
                R::Binding {
                    id: b,
                    generation: bg,
                    ..
                },
            ) => a == b && ag == bg,
            (R::ResumeJob(a), R::ResumeJob(b)) => {
                a.id == b.id
                    && a.generation == b.generation
                    && match (&a.continuation, &b.continuation) {
                        (None, None) => true,
                        (Some(a), Some(b)) => {
                            a.attempt == b.attempt
                                && a.snapshot.turn.id == b.snapshot.turn.id
                                && a.after == b.after
                        }
                        _ => false,
                    }
            }
            _ => false,
        }
    }
    pub(crate) fn command_fits(&self, bytes: usize) -> bool {
        use ThreadControllerRequest as R;
        match self {
            // Private request construction follows the controller's aggregate
            // 2 MiB string/ID admission budget. Do not reinterpret escaped JSON
            // size here and defer an otherwise valid recovery forever.
            R::Semantic(_) => true,
            R::Read(request) => {
                request.thread_id.len()
                    + request.through_turn_id.len()
                    + request
                        .through_block
                        .as_ref()
                        .map_or(0, |p| p.block_id.len() + p.sort_key.len())
                    <= bytes
            }
            R::Binding { id, workspace, .. } => id.len() + workspace.len() <= bytes,
            R::Subscribe(request) => request.id.len() + request.workspace.len() <= bytes,
            R::ResumeJob(request) => request.id.len() <= bytes,
            R::Resume => true,
        }
    }
}
impl ThreadRegistry {
    fn retire_unauthorized_targeted_requests(&mut self, id: &str) -> bool {
        // Read the actual reducer result. Reauthorization may admit a fresh
        // generation, never revive an aggregate that survived authority removal.
        let semantic = &self.stores[id].semantic;
        let retired = self
            .timeline
            .in_flight
            .iter()
            .filter(|key| {
                let SemanticTimelineRequestKey::TurnWorkItems { thread_id, turn_id } = key else {
                    return false;
                };
                thread_id == id
                    && !crate::timeline::semantic::accepts_turn_work_items_change(
                        semantic, thread_id, turn_id,
                    )
            })
            .cloned()
            .collect::<Vec<_>>();
        for key in &retired {
            self.timeline.reject_admission(key);
        }
        !retired.is_empty()
    }

    fn controller_request_current(&self, request: &ThreadControllerRequest) -> bool {
        use ThreadControllerRequest as R;
        match request {
            R::Semantic(request) => {
                self.stores
                    .get(&request.id)
                    .is_some_and(|store| store.generation == request.generation)
                    && self.timeline.accepts(
                        crate::timeline::semantic::semantic_timeline_request_key(&request.action),
                        request.request_generation,
                    )
            }
            R::Read(read) => self.timeline.accepts_read(read),
            R::Subscribe(request) => self.stores.get(&request.id).is_some_and(|store| {
                store.generation == request.generation
                    && store.subscription_request.as_ref() == Some(request)
            }),
            R::Binding { id, generation, .. } => self
                .stores
                .get(id)
                .is_some_and(|store| store.generation == *generation),
            R::ResumeJob(request) => self.stores.get(&request.id).is_some_and(|store| {
                store.generation == request.generation
                    && request.continuation.as_ref().is_none_or(|c| {
                        // This queued item still owns logical cleanup if its
                        // turn disappeared or moved. Let guarded prepare apply
                        // the existing reset/recovery policy before releasing it.
                        c.attempt == store.resume_attempt && store.coordinator.resume.in_progress
                    })
            }),
            R::Resume => false,
        }
    }
    pub(crate) fn controller_request_interactive(&self, request: &ThreadControllerRequest) -> bool {
        use ThreadControllerRequest as R;
        match request {
            R::Read(read) => self
                .stores
                .get(&read.thread_id)
                .is_some_and(|s| s.demand == ClientDemand::Visible),
            R::Semantic(request) => self
                .timeline
                .dispatch_priority(
                    crate::timeline::semantic::semantic_timeline_request_key(&request.action),
                    request.request_generation,
                )
                .is_some_and(|demanded| {
                    (!request.recovery || request.explicit_intent)
                        && (demanded
                            || (request.explicit_intent
                                && (self.timeline.has_current_demand(&request.id)
                                    || self.stores.get(&request.id).is_some_and(|store| {
                                        store.demand == ClientDemand::Visible
                                    }))))
                }),
            R::Subscribe(request) => self
                .stores
                .get(&request.id)
                .is_some_and(|s| s.demand == ClientDemand::Visible),
            R::Binding { id, .. } => self
                .stores
                .get(id)
                .is_some_and(|s| s.demand == ClientDemand::Visible),
            R::ResumeJob(_) | R::Resume => false,
        }
    }
}

impl ClientCore {
    pub(crate) fn retire_thread_controller_jobs(&self) {
        let sender = self
            .thread_request_sender
            .lock()
            .expect("thread request sender poisoned")
            .clone();
        if let Some(sender) = sender {
            let registry = self
                .thread_registry
                .lock()
                .expect("thread registry poisoned");
            let retired = sender.retire_history(
                |identity| identity.current(&registry),
                |request| registry.controller_request_current(request),
            );
            drop(registry);
            for response in retired {
                response.cancel();
            }
        }
    }
    pub fn schedule_thread_resume(&self, id: &str) -> anyhow::Result<()> {
        self.enqueue_thread_resume(id.to_owned())?;
        if let Some(sender) = self
            .thread_request_sender
            .lock()
            .expect("thread request sender poisoned")
            .as_ref()
        {
            sender
                .send(ThreadControllerRequest::Resume)
                .map_err(|_| anyhow::anyhow!("Resume worker unavailable"))?;
        }
        Ok(())
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
        let sender = self
            .thread_request_sender
            .lock()
            .expect("thread request sender poisoned")
            .clone();
        let Some(sender) = sender else {
            return;
        };
        let jobs = {
            let registry = self
                .thread_registry
                .lock()
                .expect("thread registry poisoned");
            registry
                .stores
                .iter()
                .filter(|(id, store)| {
                    !store.coordinator.resume.in_progress
                        && (registry.ready_resume_set.contains(*id)
                            || store
                                .coordinator
                                .resume
                                .next_attempt_at
                                .is_some_and(|at| at <= std::time::Instant::now()))
                })
                .take(super::history_scheduler::MAX_JOBS)
                .map(|(id, store)| ThreadResumeRequest {
                    id: id.clone(),
                    generation: store.generation,
                    continuation: None,
                })
                .collect::<Vec<_>>()
        };
        #[cfg(test)]
        tests::RESUME_CAPTURE_HOOK.with(|slot| {
            let hook = slot.borrow_mut().take();
            if let Some(hook) = hook {
                hook();
            }
        });
        for request in jobs {
            let id = request.id.clone();
            let generation = request.generation;
            if sender
                .send(ThreadControllerRequest::ResumeJob(request))
                .is_ok()
            {
                let mut registry = self
                    .thread_registry
                    .lock()
                    .expect("thread registry poisoned");
                // Successful enqueue of an old job cannot consume a replacement's intent.
                if registry
                    .stores
                    .get(&id)
                    .is_some_and(|store| store.generation == generation)
                {
                    registry.ready_resume_set.remove(&id);
                    registry.ready_resume.retain(|queued| queued != &id);
                }
                // A due deadline remains owned until dispatch begins. Duplicate
                // discovery coalesces; full admission never loses recovery.
            }
        }
    }
    #[cfg(test)]
    pub(crate) fn execute_thread_resume_quantum(
        &self,
        transport: &impl crate::rpc::JsonRpcRequestTransport,
        request: ThreadResumeRequest,
    ) -> Option<ThreadResumeRequest> {
        let prepared = self.prepare_thread_resume_quantum(request)?;
        let (method, params) = prepared.rpc.as_ref().expect("resume RPC");
        let result = crate::rpc::send_json_rpc_request_value(
            transport,
            method,
            params.clone(),
            crate::rpc::RPC_REQUEST_TIMEOUT,
        );
        match (prepared.apply)(self, result, None) {
            Some(ThreadControllerRequest::ResumeJob(r)) => Some(r),
            _ => None,
        }
    }
    pub(crate) fn prepare_thread_resume_quantum(
        &self,
        mut request: ThreadResumeRequest,
    ) -> Option<PreparedHistory> {
        use super::resume::*;
        let id = request.id.clone();
        let id = id.as_str();
        let generation = request.generation;
        let (turn_id, workspace, attempt) = {
            let mut registry = self
                .thread_registry
                .lock()
                .expect("thread registry poisoned");
            let store = registry.stores.get_mut(id)?;
            if self.is_stopped()
                || store.generation != generation
                || (request.continuation.is_none() && store.coordinator.resume.in_progress)
            {
                return None;
            }
            // Check the receipt before even missing-turn cleanup: an old queued
            // continuation must not reset a newer attempt after a sweep race.
            if request.continuation.as_ref().is_some_and(|continuation| {
                continuation.attempt != store.resume_attempt
                    || !store.coordinator.resume.in_progress
            }) {
                return None;
            }
            let Some(turn) = store
                .coordinator
                .conversation
                .in_flight_turn_id()
                .map(str::to_owned)
            else {
                reset_thread_resume_coordinator(&mut store.coordinator.resume);
                let drafts = registry.publish(id);
                self.transition_directory(
                    &mut registry,
                    &ClientMutationAuthority { _private: () },
                    drafts,
                    vec![],
                );
                return None;
            };
            if let Some(continuation) = &request.continuation {
                if continuation.snapshot.turn.id != turn {
                    retire_moved_resume(store);
                    let drafts = registry.publish(id);
                    self.transition_directory(
                        &mut registry,
                        &ClientMutationAuthority { _private: () },
                        drafts,
                        vec![],
                    );
                    return None;
                }
            } else {
                store.resume_attempt = store
                    .resume_attempt
                    .checked_add(1)
                    .expect("resume attempt exhausted");
                begin_turn_resume_attempt(&mut store.coordinator.resume);
            }
            (
                turn,
                store.coordinator.workspace_id.clone(),
                store.resume_attempt,
            )
        };
        let rpc = if let Some(continuation) = &request.continuation {
            (
                pioneer_protocol::constants::methods::TURN_ITEMS_PAGE,
                serde_json::to_value(turn_resume_items_page_params(
                    id.to_owned(),
                    turn_id.clone(),
                    continuation.after,
                ))
                .expect("resume params"),
            )
        } else {
            (
                pioneer_protocol::constants::methods::TURN_GET,
                serde_json::to_value(turn_resume_turn_params(id.to_owned(), turn_id.clone()))
                    .expect("resume params"),
            )
        };
        let identity = HistoryIdentity::Resume {
            id: id.to_owned(),
            generation,
            turn_id: turn_id.clone(),
            attempt,
        };
        Some(PreparedHistory {
            identity: Some(identity.clone()),
            rpc: Some(rpc),
            apply: Box::new(move |core, response, authority| {
                let snapshot = request.continuation.is_none();
                enum Reply {
                    Snapshot(pioneer_protocol::TurnGetResponse),
                    Items(pioneer_protocol::TurnItemsResponse),
                }
                let response = response.and_then(|v| {
                    if snapshot {
                        Ok(Reply::Snapshot(serde_json::from_value(v)?))
                    } else {
                        Ok(Reply::Items(serde_json::from_value(v)?))
                    }
                });
                let id_owned = request.id.clone();
                let id = id_owned.as_str();
                let mut replay_events = Vec::new();
                let result = (|| -> anyhow::Result<Option<TurnResumeSnapshotReduction>> {
                    if request.continuation.is_none() {
                        let Reply::Snapshot(snapshot) = response? else {
                            unreachable!()
                        };
                        anyhow::ensure!(
                            snapshot.workspace_id == workspace,
                            "Resume response workspace mismatch"
                        );
                        if !turn_snapshot_matches_scope(id, &turn_id, &snapshot) {
                            return Ok(Some(reduce_turn_resume_turn_snapshot(
                                id, &turn_id, snapshot,
                            )));
                        }
                        anyhow::ensure!(
                            super::history_scheduler::fits_serialized(&snapshot, 8 * 1024 * 1024),
                            "Resume snapshot capacity exceeded"
                        );
                        request.continuation = Some(ThreadResumeContinuation {
                            attempt,
                            snapshot,
                            after: None,
                        });
                        return Ok(None);
                    }
                    let continuation = request.continuation.as_mut().unwrap();
                    let Reply::Items(page) = response? else {
                        unreachable!()
                    };
                    let page = reduce_turn_resume_items_page(
                        &continuation.snapshot,
                        continuation.after,
                        page,
                    )?;
                    anyhow::ensure!(page.workspace_id == workspace, "Resume workspace mismatch");
                    replay_events = page.replay_events;
                    if let Some(next) = page.next_cursor {
                        continuation.after = Some(next);
                        return Ok(None);
                    }
                    Ok(Some(reduce_turn_resume_turn_snapshot(
                        id,
                        &turn_id,
                        request.continuation.take().unwrap().snapshot,
                    )))
                })();
                let authority_guard = authority.map(|a| a.lock(core));
                let mut registry = core
                    .thread_registry
                    .lock()
                    .expect("thread registry poisoned");
                let transport_guard = authority.map(|a| a.lock_transport());
                let result = if authority_guard.as_ref().is_some_and(|a| !a.1)
                    || transport_guard.as_ref().is_some_and(|a| !a.1)
                {
                    replay_events.clear();
                    Err(anyhow::anyhow!("Resume authority retired at apply"))
                } else {
                    result
                };
                if !identity.current(&registry) {
                    if let Some(store) = registry.stores.get_mut(id).filter(|s| {
                        s.generation == generation
                            && s.resume_attempt == attempt
                            && s.coordinator.resume.in_progress
                    }) {
                        // Only this captured attempt may be retired. A moved
                        // turn gets the existing recovery scheduling policy.
                        retire_moved_resume(store);
                        drop(transport_guard);
                        drop(authority_guard);
                        let drafts = registry.publish(id);
                        core.transition_directory(
                            &mut registry,
                            &ClientMutationAuthority { _private: () },
                            drafts,
                            vec![],
                        );
                    }
                    return None;
                }
                if matches!(result, Ok(None)) {
                    // Revalidate after the snapshot quantum as well as after item pages.
                    if !core.is_stopped()
                        && let Some(store) = registry.stores.get_mut(id).filter(|store| {
                            store.generation == generation
                                && store.resume_attempt == attempt
                                && store.coordinator.resume.in_progress
                        })
                    {
                        if store.coordinator.resume.in_progress
                            && store.coordinator.conversation.in_flight_turn_id()
                                == Some(turn_id.as_str())
                        {
                            for event in replay_events {
                                store.coordinator.conversation.apply(event);
                            }
                            drop(transport_guard);
                            drop(authority_guard);
                            let drafts = registry.publish(id);
                            core.transition_directory(
                                &mut registry,
                                &ClientMutationAuthority { _private: () },
                                drafts,
                                vec![],
                            );
                            drop(registry);
                            return Some(ThreadControllerRequest::ResumeJob(request));
                        }
                        retire_moved_resume(store);
                        drop(transport_guard);
                        drop(authority_guard);
                        let drafts = registry.publish(id);
                        core.transition_directory(
                            &mut registry,
                            &ClientMutationAuthority { _private: () },
                            drafts,
                            vec![],
                        );
                    }
                    return None;
                }
                let Some(store) = registry.stores.get_mut(id) else {
                    return None;
                };
                if core.is_stopped()
                    || store.generation != generation
                    || store.resume_attempt != attempt
                    || !store.coordinator.resume.in_progress
                {
                    return None;
                }
                if store.coordinator.conversation.in_flight_turn_id() != Some(turn_id.as_str()) {
                    retire_moved_resume(store);
                    drop(transport_guard);
                    drop(authority_guard);
                    let drafts = registry.publish(id);
                    core.transition_directory(
                        &mut registry,
                        &ClientMutationAuthority { _private: () },
                        drafts,
                        vec![],
                    );
                    return None;
                }
                for event in replay_events {
                    store.coordinator.conversation.apply(event);
                }
                finish_turn_resume_attempt(&mut store.coordinator.resume);
                match result {
                    Ok(Some(TurnResumeSnapshotReduction::Apply(reduction))) => {
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
                    Ok(Some(TurnResumeSnapshotReduction::ScopeMismatch {
                        retry_after, ..
                    })) => {
                        schedule_turn_resume_after_state(
                            &mut store.coordinator.resume,
                            retry_after,
                            std::time::Instant::now(),
                        );
                    }
                    Ok(None) => unreachable!(),
                    Err(_) => {
                        apply_turn_resume_retry(
                            Some(&mut store.coordinator.resume),
                            std::time::Instant::now(),
                        );
                    }
                }
                drop(transport_guard);
                drop(authority_guard);
                let drafts = registry.publish(id);
                core.transition_directory(
                    &mut registry,
                    &ClientMutationAuthority { _private: () },
                    drafts,
                    vec![],
                );
                None
            }),
        })
    }
}

impl ClientCore {
    /// Explicit authoritative metadata read. This does not establish a
    /// subscription, change selection, or hydrate the timeline.
    pub fn refresh_thread_metadata(
        &self,
        transport: &impl crate::rpc::JsonRpcRequestTransport,
        id: &str,
    ) -> anyhow::Result<Thread> {
        let auth = self.current_auth_ticket();
        let authority = self.subscription_authority();
        let revision = self.gateway_session().startup.transport_revision;
        let before = {
            let registry = self
                .thread_registry
                .lock()
                .expect("thread registry poisoned");
            anyhow::ensure!(
                !self.is_stopped() && registry.start.pending_thread_id.as_deref() != Some(id),
                "Thread metadata refresh cancelled"
            );
            registry.stores.get(id).map(|store| {
                (
                    store.generation,
                    store.coordinator.workspace_id.clone(),
                    store.subscription_baseline(),
                )
            })
        };
        let response = crate::transport::ws::command_sender::thread_get(
            transport,
            pioneer_protocol::ThreadGetParams {
                thread_id: id.into(),
            },
        )?;
        anyhow::ensure!(
            response.thread.id == id,
            "Thread metadata refresh response scope mismatch"
        );
        anyhow::ensure!(
            !self.is_stopped()
                && self.current_auth_ticket() == auth
                && self.subscription_authority() == authority
                && self.gateway_session().startup.transport_revision == revision,
            "Thread metadata refresh transport retired"
        );
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        anyhow::ensure!(
            !self.is_stopped() && registry.start.pending_thread_id.as_deref() != Some(id),
            "Thread metadata refresh cancelled"
        );
        let Some((generation, workspace, baseline)) = before else {
            anyhow::ensure!(
                !registry.stores.contains_key(id),
                "Thread metadata refresh cancelled"
            );
            return Ok(response.thread);
        };
        let store = registry
            .stores
            .get_mut(id)
            .filter(|store| {
                store.generation == generation
                    && store.coordinator.workspace_id == workspace
                    && response.thread.workspace_id == workspace
            })
            .ok_or_else(|| anyhow::anyhow!("Thread metadata refresh cancelled"))?;
        store.reconcile_subscription_snapshot(Some(&baseline), response.thread);
        let thread = store
            .coordinator
            .thread()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Thread metadata refresh cancelled"))?;
        let drafts = registry.publish(id);
        self.transition_directory(
            &mut registry,
            &ClientMutationAuthority { _private: () },
            drafts,
            vec![],
        );
        Ok(thread)
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
                    if let Some(store) = registry.stores.remove(&id) {
                        store.retire_subscription();
                    }
                }
                if let Some(store) = registry.stores.get_mut(&id)
                    && let Some(request) = store.subscription_request.take()
                {
                    request.retire();
                    self.notify_send_readiness();
                    store.subscription_retry_at = None;
                    store.coordinator.history_loading = false;
                    store.subscription_failed = false;
                    let drafts = registry.publish(&id);
                    self.transition_directory(
                        &mut registry,
                        &ClientMutationAuthority { _private: () },
                        drafts,
                        vec![],
                    );
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
        drop(registry);
        self.resume_created_thread_delivery(&id, workspace);
        Ok(id)
    }
}

impl ClientCore {
    pub(crate) fn open_existing_thread_subscription(&self, id: &str) -> anyhow::Result<()> {
        let (revision, intent) = {
            let mut registry = self
                .thread_registry
                .lock()
                .expect("thread registry poisoned");
            if registry
                .open_intent
                .as_ref()
                .is_none_or(|(target, _)| target != id)
            {
                registry.clock = registry
                    .clock
                    .checked_add(1)
                    .expect("thread generation exhausted");
                registry.open_intent = Some((id.to_owned(), registry.clock));
            }
            (
                registry.navigation_revision,
                registry.open_intent.as_ref().expect("open intent").1,
            )
        };
        let budget = crate::rpc::RequestBudget::new(
            std::time::Instant::now() + crate::rpc::RPC_REQUEST_TIMEOUT,
        );
        let (request, ready) =
            self.admit_thread_subscription(id, "", None, false, Some((revision, intent)))?;
        if !ready {
            self.execute_thread_subscription_request(request.clone());
        }
        self.wait_thread_subscription(&request, false, &budget)?;
        anyhow::ensure!(
            self.subscription_scope_current(&request),
            "thread_open_stale"
        );
        let identity = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        anyhow::ensure!(
            identity.authorization_epoch() == request.authority
                && identity.history_ticket_matches(request.auth.0)
                && request.transport.is_current(),
            "thread_open_stale"
        );
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        anyhow::ensure!(
            registry.navigation_revision == revision
                && registry
                    .open_intent
                    .as_ref()
                    .is_some_and(|(_, generation)| *generation == intent),
            "thread_open_stale"
        );
        let (transport_guard, transport_current) =
            request.transport.history_apply_fence(request.auth.1);
        anyhow::ensure!(transport_current && !self.is_stopped(), "thread_open_stale");
        let store = registry
            .stores
            .get(id)
            .filter(|store| store.generation == request.generation)
            .ok_or_else(|| anyhow::anyhow!("thread_open_stale"))?;
        let workspace = store.coordinator.workspace_id.clone();
        registry
            .navigation
            .apply(crate::navigation::NavigationIntent::SelectThread {
                workspace_id: Some(workspace),
                thread_id: Some(id.to_owned()),
            });
        let drafts = registry.navigation_change().into_iter().collect();
        drop(transport_guard);
        self.transition_directory(
            &mut registry,
            &ClientMutationAuthority { _private: () },
            drafts,
            vec![],
        );
        drop(registry);
        drop(identity);
        request
            .flight
            .lock()
            .expect("subscription flight poisoned")
            .hydrate = true;
        self.hydrate_thread_subscription_once(&request);
        Ok(())
    }

    fn resume_created_thread_delivery(&self, id: &str, workspace: &str) {
        let visible = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned")
            .stores
            .get(id)
            .is_some_and(|store| store.demand == ClientDemand::Visible);
        if visible {
            self.schedule_thread_subscription(id, workspace);
        }
    }

    pub(crate) fn resume_visible_thread_delivery(&self) {
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
        let sender = self
            .thread_request_sender
            .lock()
            .expect("thread request sender poisoned")
            .clone();
        let Some(sender) = sender else {
            return;
        };
        let Ok((request, ready)) = self.admit_thread_subscription(id, workspace, None, true, None)
        else {
            return;
        };
        self.retire_thread_controller_jobs();
        if ready {
            return;
        }
        let retry_waiting = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned")
            .stores
            .get(id)
            .is_some_and(|store| {
                store.subscription_request.as_ref() == Some(&request)
                    && store.subscription_retry_at.is_some()
            });
        if retry_waiting {
            let _ = sender.send(ThreadControllerRequest::Resume);
            return;
        }
        let queued = {
            let mut flight = request.flight.lock().expect("subscription flight poisoned");
            if flight.queued || flight.response.is_some() {
                false
            } else {
                flight.queued = true;
                true
            }
        };
        if queued {
            if sender
                .send(ThreadControllerRequest::Subscribe(request.clone()))
                .is_err()
            {
                self.finish_thread_subscription_request(
                    &request,
                    Err(anyhow::anyhow!("Thread subscription worker unavailable")),
                );
            }
        } else {
            let _ = sender.send(ThreadControllerRequest::Resume);
        }
    }

    /// Returns the exact socket binding used for subscription, never a fresh
    /// handle acquired after acknowledgement. The budget belongs only to this waiter.
    pub(crate) fn ensure_thread_subscription(
        &self,
        id: &str,
        workspace: &str,
        budget: &crate::rpc::RequestBudget,
    ) -> anyhow::Result<crate::transport::ws::runtime::ConnectionRequestTransport> {
        self.ensure_thread_subscription_generation(id, workspace, None, false, budget)
    }

    fn subscription_authority(&self) -> (u64, u64) {
        self.identity_authorization
            .lock()
            .expect("identity owner poisoned")
            .authorization_epoch()
    }

    fn admit_thread_subscription(
        &self,
        id: &str,
        workspace: &str,
        expected: Option<u64>,
        hydrate: bool,
        expected_navigation_revision: Option<(u64, u64)>,
    ) -> anyhow::Result<(ThreadSubscriptionRequest, bool)> {
        let open_existing = expected_navigation_revision.is_some();
        let session = self.gateway_session();
        let auth = self.current_auth_ticket();
        let authority = self.subscription_authority();
        anyhow::ensure!(
            !self.is_stopped()
                && session.startup.transport_ready
                && !session.startup.identity_pending
                && self.current_auth().is_some(),
            "Thread subscription transport unavailable"
        );
        let transport = self
            .transport_runtime()
            .ws_command_sender()
            .requests_for_connection(
                auth.1
                    .ok_or_else(|| anyhow::anyhow!("Thread subscription transport unavailable"))?,
            );
        let socket = transport
            .scope()
            .filter(|_| transport.is_current())
            .ok_or_else(|| anyhow::anyhow!("Thread subscription socket unavailable"))?;
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        anyhow::ensure!(
            !self.is_stopped()
                && !id.is_empty()
                && registry.start.pending_thread_id.as_deref() != Some(id)
                && expected_navigation_revision.is_none_or(|(revision, intent)| registry
                    .navigation_revision
                    == revision
                    && registry
                        .open_intent
                        .as_ref()
                        .is_some_and(|(_, generation)| *generation == intent)),
            "Thread subscription cancelled"
        );
        // An ID-only open reserves the existing store/flight owner before IO.
        // A cold reservation has no domain snapshot or authoritative workspace.
        let workspace = if open_existing {
            registry
                .stores
                .get(id)
                .map(|store| store.coordinator.workspace_id.clone())
                .unwrap_or_default()
        } else {
            workspace.to_owned()
        };
        if open_existing && !registry.stores.contains_key(id) {
            registry.clock = registry.clock.saturating_add(1);
            let mut store = ThreadDomainStore::new(id, "");
            store.generation = registry.clock;
            store.subscriptions = registry.subscription_counts.get(id).copied().unwrap_or(0);
            store.demand = timeline_demand(
                &registry.demands,
                id,
                registry
                    .timeline_subscription_counts
                    .get(id)
                    .copied()
                    .unwrap_or(0),
            );
            registry.stores.insert(id.to_owned(), store);
        }
        let pending_open = registry.stores.get(id).is_some_and(|store| {
            (store.coordinator.workspace_id.is_empty()
                || store.coordinator.workspace_id == workspace)
                && store
                    .subscription_request
                    .as_ref()
                    .is_some_and(|request| request.open_existing)
        });
        let navigation_revision = registry.navigation_revision;
        let open_intent_generation = registry
            .open_intent
            .as_ref()
            .map(|(_, generation)| *generation);
        anyhow::ensure!(
            !self.is_stopped()
                && registry.start.pending_thread_id.as_deref() != Some(id)
                && (open_existing || pending_open || registry.require(id, &workspace)),
            "Thread subscription cancelled"
        );
        let store = registry.stores.get_mut(id).expect("retained thread");
        anyhow::ensure!(
            expected.is_none_or(|generation| generation == store.generation),
            "Thread subscription cancelled"
        );
        let scope = ThreadSubscriptionScope {
            workspace: workspace.clone(),
            generation: store.generation,
            transport_revision: session.startup.transport_revision,
            auth,
            authority,
            socket,
        };
        let ready = store.subscription_receipt.as_ref().is_some_and(|receipt| {
            receipt.scope == scope && *receipt.live.lock().expect("subscription receipt poisoned")
        });
        if let Some(request) = store.subscription_request.as_ref().filter(|request| {
            request.scope().is_some_and(|mut current| {
                if request.open_existing {
                    current.workspace = scope.workspace.clone();
                }
                current == scope
                    && request
                        .open_intent_generation
                        .is_none_or(|generation| Some(generation) == open_intent_generation)
                    && request
                        .open_navigation_revision
                        .is_none_or(|revision| revision == navigation_revision)
            })
        }) {
            request
                .flight
                .lock()
                .expect("subscription flight poisoned")
                .hydrate |= hydrate;
            return Ok((request.clone(), ready));
        }
        if let Some(previous) = store.subscription_request.take() {
            previous.retire();
        }
        let live = if ready {
            store
                .subscription_receipt
                .as_ref()
                .expect("ready receipt")
                .live
                .clone()
        } else {
            Arc::new(std::sync::Mutex::new(true))
        };
        let transport = transport.with_subscription_scope(live.clone());
        let open_navigation_revision = expected_navigation_revision.map(|(revision, _)| revision);
        let open_intent_generation = expected_navigation_revision.map(|(_, intent)| intent);
        let store = registry.stores.get_mut(id).expect("retained thread");
        let request = ThreadSubscriptionRequest {
            open_existing,
            open_navigation_revision,
            open_intent_generation,
            id: id.into(),
            workspace: workspace.into(),
            generation: store.generation,
            transport_revision: scope.transport_revision,
            auth,
            authority,
            transport,
            live,
            flight: Arc::new(std::sync::Mutex::new(ThreadSubscriptionFlight {
                queued: false,
                completing: false,
                deadline: std::time::Instant::now() + crate::rpc::RPC_REQUEST_TIMEOUT,
                baseline: None,
                receiver: None,
                response: None,
                result: ready.then_some(Ok(())),
                hydrate,
                hydration_claimed: false,
            })),
            attempt: 0,
        };
        if !ready {
            if let Some(receipt) = store.subscription_receipt.take() {
                receipt.retire();
            }
            store.subscription_request = Some(request.clone());
            store.subscription_retry_at = None;
            store.coordinator.history_loading = true;
            store.subscription_failed = false;
            let drafts = if store.coordinator.workspace_id.is_empty() {
                vec![]
            } else {
                registry.publish(id)
            };
            self.transition_directory(
                &mut registry,
                &ClientMutationAuthority { _private: () },
                drafts,
                vec![],
            );
        }
        drop(registry);
        self.notify_send_readiness();
        Ok((request, ready))
    }

    fn ensure_thread_subscription_generation(
        &self,
        id: &str,
        workspace: &str,
        expected: Option<u64>,
        hydrate: bool,
        budget: &crate::rpc::RequestBudget,
    ) -> anyhow::Result<crate::transport::ws::runtime::ConnectionRequestTransport> {
        budget.check()?;
        let (request, ready) =
            self.admit_thread_subscription(id, workspace, expected, hydrate, None)?;
        if !ready {
            // A foreground caller starts queued work directly. Admission is
            // nonblocking, so the thread worker never waits for its own queue.
            self.execute_thread_subscription_request(request.clone());
        }
        self.wait_thread_subscription(&request, hydrate, budget)
    }

    fn wait_thread_subscription(
        &self,
        request: &ThreadSubscriptionRequest,
        hydrate: bool,
        budget: &crate::rpc::RequestBudget,
    ) -> anyhow::Result<crate::transport::ws::runtime::ConnectionRequestTransport> {
        budget.notify_on_cancel(&self.send_readiness);
        loop {
            let sequence = self.send_readiness_sequence();
            budget.check()?;
            if self.subscription_scope_current(&request) {
                self.complete_ready_thread_subscription(&request);
            } else {
                self.finish_thread_subscription_request(
                    &request,
                    Err(anyhow::anyhow!("Thread subscription retired")),
                );
            }
            let (result, deadline) = {
                let flight = request.flight.lock().expect("subscription flight poisoned");
                (flight.result.clone(), flight.deadline)
            };
            if let Some(result) = result {
                result.map_err(|error| error.error())?;
                anyhow::ensure!(
                    self.subscription_scope_current(&request),
                    "Thread subscription retired"
                );
                if hydrate {
                    self.hydrate_thread_subscription_once(request);
                }
                return Ok(request.transport.clone());
            }
            // A flight timeout is reduced before returning; it is not left alive
            // merely because its last foreground waiter has departed.
            if std::time::Instant::now() >= deadline {
                continue;
            }
            if let Err(error) = self.wait_send_readiness(sequence, budget.deadline().min(deadline))
            {
                self.complete_ready_thread_subscription(&request);
                return Err(error);
            }
        }
    }

    pub fn schedule_thread_cli_binding(&self, id: &str, workspace: &str) -> anyhow::Result<()> {
        let sender = self
            .thread_request_sender
            .lock()
            .expect("thread request sender poisoned")
            .clone();
        let sender = sender.ok_or_else(|| anyhow::anyhow!("History worker unavailable"))?;
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        anyhow::ensure!(
            !self.is_stopped() && registry.require(id, workspace),
            "Binding source retired"
        );
        let generation = registry.stores[id].generation;
        drop(registry);
        self.retire_thread_controller_jobs();
        sender
            .send(ThreadControllerRequest::Binding {
                id: id.into(),
                workspace: workspace.into(),
                generation,
            })
            .map_err(|_| anyhow::anyhow!("History worker full or unavailable"))
    }

    fn subscription_scope_current(&self, request: &ThreadSubscriptionRequest) -> bool {
        let session = self.gateway_session();
        !self.is_stopped()
            && request.transport.is_current()
            && session.startup.transport_ready
            && !session.startup.identity_pending
            && session.startup.transport_revision == request.transport_revision
            && self.current_auth_ticket() == request.auth
            && self.subscription_authority() == request.authority
            && {
                let registry = self
                    .thread_registry
                    .lock()
                    .expect("thread registry poisoned");
                registry.start.pending_thread_id.as_ref() != Some(&request.id)
                    && registry.stores.get(&request.id).is_some_and(|store| {
                        store.generation == request.generation
                            && (request.open_existing
                                || store.coordinator.workspace_id == request.workspace)
                    })
            }
    }

    fn thread_subscription_request_current(&self, request: &ThreadSubscriptionRequest) -> bool {
        self.subscription_scope_current(request)
            && self
                .thread_registry
                .lock()
                .expect("thread registry poisoned")
                .stores
                .get(&request.id)
                .is_some_and(|store| store.subscription_request.as_ref() == Some(request))
    }

    pub(crate) fn execute_thread_subscription_request(&self, request: ThreadSubscriptionRequest) {
        self.execute_thread_subscription_request_using(&request, &request.transport);
    }

    fn execute_thread_subscription_request_using(
        &self,
        request: &ThreadSubscriptionRequest,
        transport: &impl crate::rpc::JsonRpcRequestTransport,
    ) {
        if !self.thread_subscription_request_current(&request) {
            self.finish_thread_subscription_request(
                &request,
                Err(anyhow::anyhow!("Thread subscription retired")),
            );
            return;
        }
        #[cfg(test)]
        tests::SUBSCRIPTION_PROMOTION_HOOK.with(|slot| {
            let hook = slot.borrow_mut().take();
            if let Some(hook) = hook {
                hook();
            }
        });
        let baseline = {
            let mut registry = self
                .thread_registry
                .lock()
                .expect("thread registry poisoned");
            registry
                .stores
                .get_mut(&request.id)
                .filter(|store| {
                    store.generation == request.generation
                        && store.subscription_request.as_ref() == Some(request)
                })
                .map(|store| {
                    if store.subscription_retry_at.take().is_some() {
                        let mut flight =
                            request.flight.lock().expect("subscription flight poisoned");
                        // A dormant retry is intent, not an active RPC lifetime.
                        // Only its owner starts the deadline on promotion.
                        if !flight.queued && flight.response.is_none() {
                            flight.deadline =
                                std::time::Instant::now() + crate::rpc::RPC_REQUEST_TIMEOUT;
                            flight.queued = true;
                        }
                    }
                    store.subscription_baseline()
                })
        };
        let Some(baseline) = baseline else {
            self.finish_thread_subscription_request(
                request,
                Err(anyhow::anyhow!("Thread subscription retired")),
            );
            return;
        };
        let response = {
            let mut flight = request.flight.lock().expect("subscription flight poisoned");
            if flight.response.is_some() || flight.result.is_some() {
                return;
            }
            if std::time::Instant::now() >= flight.deadline {
                drop(flight);
                self.finish_thread_subscription_request(
                    &request,
                    Err(crate::rpc::JsonRpcResponseError::DeadlineExceeded.into()),
                );
                return;
            }
            let (tx, rx) = std::sync::mpsc::channel();
            let signal = self.send_readiness.clone();
            let worker = self
                .thread_request_sender
                .lock()
                .expect("thread request sender poisoned")
                .clone();
            let wake = Arc::new(move || {
                let (sequence, ready) = &*signal;
                let mut sequence = sequence.lock().expect("send readiness poisoned");
                *sequence = sequence
                    .checked_add(1)
                    .expect("send readiness sequence exhausted");
                ready.notify_all();
                drop(sequence);
                if let Some(worker) = &worker {
                    let _ = worker.send(ThreadControllerRequest::Resume);
                }
            });
            let response =
                crate::rpc::JsonRpcResponseSender::new_with_wake(tx, flight.deadline, wake);
            flight.queued = true;
            flight.baseline = Some(baseline);
            flight.receiver = Some(rx);
            flight.response = Some(response.clone());
            response
        };
        let result = (|| -> anyhow::Result<()> {
            let (method, params) = if request.open_existing {
                (
                    pioneer_protocol::constants::methods::THREAD_OPEN,
                    serde_json::to_value(pioneer_protocol::ThreadGetParams {
                        thread_id: request.id.clone(),
                    })?,
                )
            } else {
                (
                    pioneer_protocol::constants::methods::THREAD_START,
                    serde_json::to_value(super::start::thread_start_params(
                        request.id.clone(),
                        request.workspace.clone(),
                    ))?,
                )
            };
            let payload = crate::rpc::build_json_rpc_request_payload(method, params)?;
            transport
                .send_json_rpc_request(payload.request_id, payload.payload, response.clone())
                .map_err(anyhow::Error::msg)
        })();
        if let Err(error) = result {
            let _ = response.send(Err(crate::rpc::JsonRpcResponseError::transport(format!(
                "{error:#}"
            ))));
        }
        self.complete_ready_thread_subscription(&request);
    }

    fn complete_ready_thread_subscription(&self, request: &ThreadSubscriptionRequest) {
        let response = {
            let mut flight = request.flight.lock().expect("subscription flight poisoned");
            if flight.result.is_some() || flight.completing {
                return;
            }
            let response = match flight.receiver.as_ref().map(|receiver| receiver.try_recv()) {
                Some(Ok(response)) => Some(response),
                Some(Err(std::sync::mpsc::TryRecvError::Disconnected)) => {
                    Some(Err(crate::rpc::JsonRpcResponseError::transport(
                        "Thread subscription response owner dropped",
                    )))
                }
                _ if std::time::Instant::now() >= flight.deadline => {
                    Some(Err(crate::rpc::JsonRpcResponseError::DeadlineExceeded))
                }
                _ => None,
            };
            if response.is_some() {
                flight.completing = true;
                flight.receiver = None;
            }
            response
        };
        let Some(response) = response else {
            return;
        };
        let result = response.map_err(anyhow::Error::new).and_then(|value| {
            let response: pioneer_protocol::ThreadStartResponse =
                crate::rpc::deserialize_json_rpc_result(
                    if request.open_existing {
                        pioneer_protocol::constants::methods::THREAD_OPEN
                    } else {
                        pioneer_protocol::constants::methods::THREAD_START
                    },
                    value,
                )?;
            anyhow::ensure!(
                response.thread.id == request.id
                    && (request.open_existing || response.thread.workspace_id == request.workspace)
                    && !response.thread.workspace_id.is_empty(),
                "Thread subscription response scope mismatch"
            );
            anyhow::ensure!(
                self.thread_subscription_request_current(request),
                "Thread subscription retired"
            );
            let identity = self
                .identity_authorization
                .lock()
                .expect("identity owner poisoned");
            anyhow::ensure!(
                identity.authorization_epoch() == request.authority
                    && identity.history_ticket_matches(request.auth.0)
                    && request.transport.is_current(),
                "Thread subscription retired"
            );
            let mut registry = self
                .thread_registry
                .lock()
                .expect("thread registry poisoned");
            anyhow::ensure!(
                request
                    .open_navigation_revision
                    .is_none_or(|revision| registry.navigation_revision == revision),
                "thread_open_stale"
            );
            anyhow::ensure!(
                request
                    .open_intent_generation
                    .is_none_or(|generation| registry
                        .open_intent
                        .as_ref()
                        .is_some_and(|(_, current)| *current == generation)),
                "thread_open_stale"
            );
            let (_transport_guard, transport_current) =
                request.transport.history_apply_fence(request.auth.1);
            anyhow::ensure!(
                transport_current && !self.is_stopped(),
                "Thread subscription retired"
            );
            let store = registry
                .stores
                .get_mut(&request.id)
                .filter(|store| {
                    store.generation == request.generation
                        && store.subscription_request.as_ref() == Some(request)
                })
                .ok_or_else(|| anyhow::anyhow!("Thread subscription retired"))?;
            let mut flight = request.flight.lock().expect("subscription flight poisoned");
            anyhow::ensure!(flight.result.is_none(), "Thread subscription retired");
            let workspace = response.thread.workspace_id.clone();
            store.reconcile_subscription_snapshot(flight.baseline.take().as_ref(), response.thread);
            store.subscription_receipt = request.scope().map(|mut scope| {
                scope.workspace = workspace;
                ThreadSubscriptionReceipt {
                    scope,
                    live: request.live.clone(),
                }
            });
            Ok(())
        });
        self.finish_thread_subscription_request(request, result);
    }

    fn finish_thread_subscription_request(
        &self,
        request: &ThreadSubscriptionRequest,
        result: anyhow::Result<()>,
    ) {
        self.finish_thread_subscription_request_inner(request, result, false);
    }

    fn finish_thread_subscription_request_inner(
        &self,
        request: &ThreadSubscriptionRequest,
        result: anyhow::Result<()>,
        only_if_dormant: bool,
    ) {
        let current = self.thread_subscription_request_current(request);
        let retryable = result.as_ref().err().is_some_and(|error| {
            crate::rpc::json_rpc_response_error(error)
                .is_none_or(|error| error.authorization_failure().is_none())
        });
        let (response, hydrate) = {
            let mut flight = request.flight.lock().expect("subscription flight poisoned");
            if flight.result.is_some()
                || (only_if_dormant
                    && (flight.queued || flight.response.is_some() || flight.completing))
            {
                return;
            }
            // Promotion marks queued under this same lock, before installing a
            // response. Claim capacity failure here or leave that active flight alone.
            flight.result = Some(
                result
                    .as_ref()
                    .map(|_| ())
                    .map_err(ThreadSubscriptionFailure::from_error),
            );
            (flight.response.take(), flight.hydrate)
        };
        if result.is_err() {
            *request.live.lock().expect("subscription receipt poisoned") = false;
        }
        if let Some(response) = response {
            response.cancel();
        }
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        let navigation_revision = registry.navigation_revision;
        if let Some(store) = registry.stores.get_mut(&request.id).filter(|store| {
            store.generation == request.generation
                && store.subscription_request.as_ref() == Some(request)
        }) {
            store.coordinator.history_loading = false;
            store.subscription_failed = result.is_err();
            if current
                && retryable
                && !store.coordinator.workspace_id.is_empty()
                && !(request.open_existing && request.workspace.is_empty())
                && request
                    .open_navigation_revision
                    .is_none_or(|revision| navigation_revision == revision)
                && request.attempt < 2
                && store.demand == ClientDemand::Visible
            {
                let mut next = request.clone();
                next.attempt += 1;
                next.live = Arc::new(std::sync::Mutex::new(true));
                next.transport = next.transport.with_subscription_scope(next.live.clone());
                next.flight = Arc::new(std::sync::Mutex::new(ThreadSubscriptionFlight {
                    queued: false,
                    completing: false,
                    deadline: std::time::Instant::now() + crate::rpc::RPC_REQUEST_TIMEOUT,
                    baseline: None,
                    receiver: None,
                    response: None,
                    result: None,
                    hydrate,
                    hydration_claimed: false,
                }));
                store.subscription_retry_at = Some(
                    std::time::Instant::now()
                        + std::time::Duration::from_millis(250 << request.attempt),
                );
                store.subscription_request = Some(next);
            } else {
                store.subscription_request = None;
                store.subscription_retry_at = None;
            }
            let drafts = if result.is_err() && request.open_existing && request.workspace.is_empty()
            {
                vec![]
            } else {
                registry.publish(&request.id)
            };
            self.transition_directory(
                &mut registry,
                &ClientMutationAuthority { _private: () },
                drafts,
                vec![],
            );
        }
        if result.is_err()
            && registry.stores.get(&request.id).is_some_and(|store| {
                store.generation == request.generation
                    && request.open_existing
                    && request.workspace.is_empty()
                    && store.subscription_request.is_none()
            })
        {
            registry.stores.remove(&request.id);
            registry.directory.remove_thread(&request.id);
            let mut drafts = registry.retire(&request.id);
            drafts.extend(registry.retire_summary(&request.id));
            self.transition_directory(
                &mut registry,
                &ClientMutationAuthority { _private: () },
                drafts,
                vec![],
            );
        }
        drop(registry);
        self.notify_send_readiness();
        if current && result.is_ok() && hydrate {
            self.hydrate_thread_subscription_once(request);
        }
    }

    fn hydrate_thread_subscription_once(&self, request: &ThreadSubscriptionRequest) {
        let hydrate = {
            let mut flight = request.flight.lock().expect("subscription flight poisoned");
            if flight.hydrate
                && !flight.hydration_claimed
                && flight.result.as_ref().is_some_and(|result| result.is_ok())
            {
                flight.hydration_claimed = true;
                true
            } else {
                false
            }
        };
        if hydrate {
            self.refresh_thread_timeline_generation(&request.id, Some(request.generation));
        }
    }

    pub(crate) fn next_thread_subscription_delay(&self) -> Option<std::time::Duration> {
        self.thread_registry
            .lock()
            .expect("thread registry poisoned")
            .stores
            .values()
            .filter_map(|store| {
                if let Some(at) = store.subscription_retry_at {
                    return (store.demand == ClientDemand::Visible).then_some(at);
                }
                store.subscription_request.as_ref().map(|request| {
                    request
                        .flight
                        .lock()
                        .expect("subscription flight poisoned")
                        .deadline
                })
            })
            .map(|at| at.saturating_duration_since(std::time::Instant::now()))
            .min()
    }

    pub(crate) fn drive_thread_subscriptions(&self) {
        let (active, due) = {
            let mut registry = self
                .thread_registry
                .lock()
                .expect("thread registry poisoned");
            let mut active = Vec::new();
            let mut due = Vec::new();
            for store in registry.stores.values_mut() {
                if let Some(request) = &store.subscription_request {
                    if let Some(at) = store.subscription_retry_at {
                        if store.demand == ClientDemand::Visible && at <= std::time::Instant::now()
                        {
                            // execute owns promotion and starts the dormant retry's deadline.
                            due.push(request.clone());
                        }
                    } else {
                        active.push(request.clone());
                    }
                }
            }
            (active, due)
        };
        #[cfg(test)]
        tests::SUBSCRIPTION_DUE_CAPTURE_HOOK.with(|slot| {
            let hook = slot.borrow_mut().take();
            if let Some(hook) = hook {
                hook();
            }
        });
        for request in active {
            if !self.thread_subscription_request_current(&request) {
                self.finish_thread_subscription_request(
                    &request,
                    Err(anyhow::anyhow!("Thread subscription retired")),
                );
            } else {
                self.complete_ready_thread_subscription(&request);
            }
        }
        let sender = self
            .thread_request_sender
            .lock()
            .expect("thread request sender poisoned")
            .clone();
        for request in due {
            let command = ThreadControllerRequest::Subscribe(request.clone());
            if !command.command_fits(super::history_scheduler::MAX_COMMAND_BYTES) {
                // Permanent capacity refusal must advance the existing bounded
                // failure policy, rather than retain an expired full-queue retry.
                self.finish_thread_subscription_request_inner(
                    &request,
                    Err(anyhow::anyhow!(
                        "Thread subscription command capacity exceeded"
                    )),
                    true,
                );
                continue;
            }
            if let Some(sender) = &sender {
                if sender.send(command).is_ok() || !sender.is_closed() {
                    // Full leaves the dormant retry and its original budget in
                    // its existing owner. No flight was started or Loading added.
                    continue;
                }
            }
            self.finish_thread_subscription_request(
                &request,
                Err(anyhow::anyhow!("Thread subscription worker unavailable")),
            );
        }
    }

    pub(crate) fn prepare_thread_cli_binding_request(
        &self,
        id: String,
        workspace: String,
        generation: u64,
    ) -> Option<PreparedHistory> {
        let id = id.as_str();
        let workspace = workspace.as_str();
        let auth = self.current_auth_ticket();
        let revision = self.gateway_session().startup.transport_revision;
        if self
            .thread_operation_token(id)
            .is_none_or(|token| token.generation != generation)
            || self.is_stopped()
        {
            return None;
        }
        let params = pioneer_protocol::CLIRuntimeThreadBindingGetParams {
            workspace_id: workspace.into(),
            thread_id: id.into(),
        };
        let id = id.to_owned();
        let workspace = workspace.to_owned();
        Some(PreparedHistory {
            identity: None,
            rpc: Some((
                pioneer_protocol::constants::methods::CLI_RUNTIME_THREAD_BINDING_GET,
                serde_json::to_value(params).expect("binding params"),
            )),
            apply: Box::new(move |core, response, authority| {
                let id = id.as_str();
                let workspace = workspace.as_str();
                let result: anyhow::Result<pioneer_protocol::CLIRuntimeThreadBindingGetResponse> =
                    response.and_then(|v| Ok(serde_json::from_value(v)?));
                if core.current_auth_ticket() != auth
                    || core.gateway_session().startup.transport_revision != revision
                {
                    return None;
                }
                let authority_guard = authority.map(|a| a.lock(core));
                if authority_guard.as_ref().is_some_and(|a| !a.1) {
                    return None;
                }
                let mut registry = core
                    .thread_registry
                    .lock()
                    .expect("thread registry poisoned");
                let transport_guard = authority.map(|a| a.lock_transport());
                if transport_guard.as_ref().is_some_and(|a| !a.1) {
                    return None;
                }
                let Some(store) = registry
                    .stores
                    .get_mut(id)
                    .filter(|store| store.generation == generation)
                else {
                    return None;
                };
                if core.is_stopped() {
                    return None;
                }
                store.cli_binding = result
                    .ok()
                    .and_then(|r| r.binding)
                    .filter(|binding| binding.thread_id == id && binding.workspace_id == workspace);
                drop(transport_guard);
                drop(authority_guard);
                let drafts = registry.publish(id);
                core.transition_directory(
                    &mut registry,
                    &ClientMutationAuthority { _private: () },
                    drafts,
                    vec![],
                );
                drop(registry);
                core.observe_composer_runtime(id, None, false);
                None
            }),
        })
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
            use pioneer_observability::queues::QueueBytes;
            let observer = self.presentation_observer.in_current_epoch();
            observer.enqueued(QueueBytes::Known(0));
            match sender.try_send((pioneer_observability::work::capture(), observer.clone())) {
                Ok(()) => {}
                Err(std::sync::mpsc::TrySendError::Full(_)) => {
                    observer.coalesced_send(QueueBytes::Known(0))
                }
                Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
                    observer.rejected_send(QueueBytes::Known(0))
                }
            }
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
            let _ =
                self.schedule_thread_semantic_request_inner(action, Some(generation), false, true);
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
            let _ = self.schedule_thread_semantic_request(
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
        let _ = self.schedule_thread_semantic_request_generation(
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
                let _ = self.schedule_thread_semantic_request_generation(
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
                let _ = self.schedule_thread_semantic_request_generation(
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
            store.retire_subscription();
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
        // Publish the whole mutation before releasing cache ownership. In particular,
        // publish() callers may still read a store, or publish other stores into this
        // batch. Retirement must be a subsequent transition, never a duplicate scope.
        let transition = self.transition(authority, drafts, effects);
        if matches!(
            transition.outcome(),
            crate::core::ClientTransitionOutcome::Changed
                | crate::core::ClientTransitionOutcome::Noop
        ) {
            let retired = registry.evict_inactive();
            if !retired.is_empty() {
                self.transition(authority, retired, vec![]);
                // Registry -> scheduler is the selection lock order. This is
                // only a coalesced wake; exact waiter cancellation runs in the
                // controller after this domain guard has been released.
                self.wake_thread_timeline_requests();
            }
        }
        transition
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
        drop(registry);
        // The cancellation owner may still hold its mutex. Only wake history;
        // its normal sweep cancels retired waiters outside all domain locks.
        self.wake_thread_timeline_requests();
        true
    }
}
