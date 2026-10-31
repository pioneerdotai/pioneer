//! Stable-ID directory and immutable sidebar publications for each workspace.
use super::projection::{ThreadTreeSnapshot, thread_tree_snapshot_from_parts};
use crate::core::{ClientCore, ClientMutationAuthority, ClientScope};
use pioneer_protocol::{
    Thread, ThreadAgentsDocSummary, ThreadFolder, ThreadPlacement, ThreadTreeResponse,
    ThreadUnreadSummary,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

/// Keep the latest turn marker supplied by thread metadata, without retaining history.
/// Composer defaults need this marker to distinguish a conversation from an empty draft.
pub(crate) fn thread_directory_summary(thread: Thread) -> Thread {
    thread.into_directory_summary()
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub enum SidebarNodeId {
    Thread(String),
    Folder(String),
    AgentsDocument(String),
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SidebarChangeSet {
    changed: Vec<SidebarNodeId>,
    removed: Vec<SidebarNodeId>,
    reordered_folders: Vec<String>,
}
impl SidebarChangeSet {
    pub fn changed(&self) -> &[SidebarNodeId] {
        &self.changed
    }
    pub fn removed(&self) -> &[SidebarNodeId] {
        &self.removed
    }
    pub fn reordered_folders(&self) -> &[String] {
        &self.reordered_folders
    }
    fn is_empty(&self) -> bool {
        self.changed.is_empty() && self.removed.is_empty() && self.reordered_folders.is_empty()
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectoryActionPublication {
    revision: u64,
    generation: u64,
    pending: bool,
    error: Option<String>,
}
impl DirectoryActionPublication {
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn is_pending(&self) -> bool {
        self.pending
    }
    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }
}
/// Evidence from the existing directory request owner. Local unread/metadata
/// projections may change `loading`, but cannot complete a full-tree request.
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThreadTreeLoadReceipt {
    pub generation: u64,
    pub state: ThreadTreeLoadState,
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThreadTreeLoadState {
    Pending,
    Ready,
    Failed,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThreadTreePublication {
    revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    load: Option<ThreadTreeLoadReceipt>,
    snapshot: ThreadTreeSnapshot,
    changes: SidebarChangeSet,
    #[serde(default)]
    actions: std::collections::BTreeMap<String, DirectoryActionPublication>,
    loading: bool,
    error: Option<String>,
}
impl ThreadTreePublication {
    pub fn load(&self) -> Option<&ThreadTreeLoadReceipt> {
        self.load.as_ref()
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn snapshot(&self) -> &ThreadTreeSnapshot {
        &self.snapshot
    }
    pub fn actions(&self) -> &std::collections::BTreeMap<String, DirectoryActionPublication> {
        &self.actions
    }
    pub fn changes(&self) -> &SidebarChangeSet {
        &self.changes
    }
    pub fn is_loading(&self) -> bool {
        self.loading
    }
    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }
}

/// Derived authoritative state, retained only for live workspace directories.
#[derive(Default)]
pub struct SidebarProjectionStore {
    publications: HashMap<String, Arc<ThreadTreePublication>>,
    revisions: HashMap<String, u64>,
}
impl SidebarProjectionStore {
    pub fn snapshot(&self, workspace: &str) -> Option<Arc<ThreadTreePublication>> {
        self.publications.get(workspace).cloned()
    }
    fn update(
        &mut self,
        next: ThreadTreeSnapshot,
        loading: bool,
        error: Option<String>,
        load: Option<ThreadTreeLoadReceipt>,
    ) -> Option<Arc<ThreadTreePublication>> {
        let previous = self.publications.get(&next.workspace_id);
        let empty = ThreadTreeSnapshot::default();
        let before = previous.map_or(&empty, |p| &p.snapshot);
        let mut changes = SidebarChangeSet::default();
        changed_nodes(
            &before.threads_by_id,
            &next.threads_by_id,
            SidebarNodeId::Thread,
            &mut changes,
        );
        changed_nodes(
            &before.folders_by_id,
            &next.folders_by_id,
            SidebarNodeId::Folder,
            &mut changes,
        );
        changed_nodes(
            &before.agents_doc_summaries_by_folder_key,
            &next.agents_doc_summaries_by_folder_key,
            SidebarNodeId::AgentsDocument,
            &mut changes,
        );
        let unread = |snapshot: &ThreadTreeSnapshot| {
            snapshot
                .unread
                .iter()
                .map(|v| (v.thread_id.clone(), v.unread_count))
                .collect::<HashMap<_, _>>()
        };
        let before_unread = unread(before);
        let next_unread = unread(&next);
        for id in next.threads_by_id.keys() {
            if (before.placements_by_thread_id.get(id) != next.placements_by_thread_id.get(id)
                || before_unread.get(id) != next_unread.get(id)
                || before.running_task_thread_ids.contains(id)
                    != next.running_task_thread_ids.contains(id))
                && !changes.changed.contains(&SidebarNodeId::Thread(id.clone()))
            {
                changes.changed.push(SidebarNodeId::Thread(id.clone()));
            }
        }
        for id in next.folders_by_id.keys() {
            let node = SidebarNodeId::Folder(id.clone());
            if (before.unread_by_folder_id.get(id) != next.unread_by_folder_id.get(id)
                || before.running_task_folder_ids.contains(id)
                    != next.running_task_folder_ids.contains(id))
                && !changes.changed.contains(&node)
            {
                changes.changed.push(node);
            }
        }
        let folders = before
            .thread_ids_by_folder_id
            .keys()
            .chain(next.thread_ids_by_folder_id.keys())
            .chain(before.child_folder_ids_by_parent_id.keys())
            .chain(next.child_folder_ids_by_parent_id.keys())
            .cloned()
            .collect::<BTreeSet<_>>();
        for folder in folders {
            if before.thread_ids_by_folder_id.get(&folder)
                != next.thread_ids_by_folder_id.get(&folder)
                || before.child_folder_ids_by_parent_id.get(&folder)
                    != next.child_folder_ids_by_parent_id.get(&folder)
            {
                changes.reordered_folders.push(folder);
            }
        }
        if previous.is_some_and(|p| {
            changes.is_empty() && p.loading == loading && p.error == error && p.load == load
        }) {
            return None;
        }
        let mut actions = previous
            .map(|publication| publication.actions.clone())
            .unwrap_or_default();
        actions.retain(|key, action| {
            if action.pending {
                return true;
            }
            if let Some(id) = key.strip_prefix("thread:") {
                return next.threads_by_id.contains_key(id);
            }
            if let Some(id) = key.strip_prefix("folder:") {
                return next.folders_by_id.contains_key(id);
            }
            if let Some((_, folder)) = key.split_once(":agents:") {
                return folder == "root" || next.folders_by_id.contains_key(folder);
            }
            true
        });
        let publication = Arc::new(ThreadTreePublication {
            load,
            revision: self.revisions.get(&next.workspace_id).copied().unwrap_or(0) + 1,
            snapshot: next,
            actions,
            changes,
            loading,
            error,
        });
        self.revisions.insert(
            publication.snapshot.workspace_id.clone(),
            publication.revision,
        );
        self.publications.insert(
            publication.snapshot.workspace_id.clone(),
            publication.clone(),
        );
        Some(publication)
    }
}
fn changed_nodes<T: PartialEq>(
    before: &HashMap<String, T>,
    next: &HashMap<String, T>,
    node: fn(String) -> SidebarNodeId,
    changes: &mut SidebarChangeSet,
) {
    for id in before
        .keys()
        .chain(next.keys())
        .cloned()
        .collect::<BTreeSet<_>>()
    {
        match (before.get(&id), next.get(&id)) {
            (Some(_), None) => changes.removed.push(node(id)),
            (old, Some(value)) if old != Some(value) => changes.changed.push(node(id)),
            _ => {}
        }
    }
}

// A request-local baseline, never another cache. It lets a full response merge
// unchanged rows while preserving metadata, removals and moves made in flight.
#[derive(Default)]
struct DirectoryRequestBaseline {
    threads: HashMap<String, Thread>,
    folders: HashMap<String, ThreadFolder>,
    placements: HashMap<String, ThreadPlacement>,
    agents_docs: HashMap<(String, Option<String>), ThreadAgentsDocSummary>,
    read_revisions: HashMap<String, u64>,
    push_revision: Option<(String, u64)>,
    // Accepted writes can return to their baseline value (or disappear from
    // both maps). Track that fact only for the lifetime of this request.
    touched_threads: std::collections::HashSet<String>,
    touched_placements: std::collections::HashSet<String>,
}

// One already-started selection load may continue after a partial read merge.
// Kept with directory/navigation lifecycle, never with an ordinary tree hint.
pub(super) struct InitialDirectoryRestore {
    pub workspace: String,
    pub connection: Option<u64>,
    pub navigation_revision: u64,
}

fn merge_directory_rows<K, V>(
    current: &mut HashMap<K, V>,
    before: HashMap<K, V>,
    mut protected: std::collections::HashSet<K>,
    incoming: impl IntoIterator<Item = (K, V)>,
    in_workspace: impl Fn(&V) -> bool,
) where
    K: Clone + Eq + std::hash::Hash,
    V: PartialEq,
{
    protected.extend(
        before
            .keys()
            .chain(current.keys())
            .filter(|id| current.get(*id) != before.get(*id))
            .cloned(),
    );
    current.retain(|id, value| !in_workspace(value) || protected.contains(id));
    for (id, value) in incoming {
        if !protected.contains(&id) {
            current.insert(id, value);
        }
    }
}

#[derive(Default)]
pub struct ThreadDirectoryStore {
    pub(crate) push_revisions: HashMap<String, (String, u64)>,
    pub(super) initial_restore: Option<InitialDirectoryRestore>,
    pub(crate) dirty_workspaces: std::collections::BTreeSet<String>,
    pub(crate) threads: HashMap<String, Thread>,
    pub(crate) placements: HashMap<String, ThreadPlacement>,
    pub(crate) summaries: HashMap<String, crate::threads::registry::SidebarSummaryChanged>,
    folders: HashMap<String, ThreadFolder>,
    agents_docs: HashMap<(String, Option<String>), ThreadAgentsDocSummary>,
    unread: HashMap<String, u64>,
    running_task_thread_ids: std::collections::HashSet<String>,
    cursors: HashMap<String, pioneer_protocol::ThreadReadCursor>,
    read_revisions: HashMap<String, u64>,
    requests: HashMap<String, u64>,
    loads: HashMap<String, ThreadTreeLoadReceipt>,
    subscriptions: HashMap<String, usize>,
    suspended_workspaces: BTreeSet<String>,
    // An access fence clears protected data, but must not erase the need to
    // reload it. Local projections can be recreated before that reload finishes.
    authorization_reloads: BTreeSet<String>,
    lifecycle_generations: HashMap<String, u64>,
    request_baselines: HashMap<String, DirectoryRequestBaseline>,
    next_request: u64,
    projections: SidebarProjectionStore,
}
impl ThreadDirectoryStore {
    pub(crate) fn cancel_initial_restore(&mut self) {
        self.initial_restore = None;
    }

    pub(crate) fn upsert_thread_summary(&mut self, thread: Thread) {
        if self.threads.get(&thread.id) == Some(&thread) {
            return;
        }
        let id = thread.id.clone();
        let workspace = thread.workspace_id.clone();
        if let Some(previous) = self.threads.insert(id.clone(), thread)
            && previous.workspace_id != workspace
            && let Some(request) = self.request_baselines.get_mut(&previous.workspace_id)
        {
            request.touched_threads.insert(id.clone());
        }
        if let Some(request) = self.request_baselines.get_mut(&workspace) {
            request.touched_threads.insert(id);
        }
    }

    pub(crate) fn upsert_placement(&mut self, placement: ThreadPlacement) {
        if self.placements.get(&placement.thread_id) == Some(&placement) {
            return;
        }
        let id = placement.thread_id.clone();
        let workspace = placement.workspace_id.clone();
        if let Some(previous) = self.placements.insert(id.clone(), placement)
            && previous.workspace_id != workspace
            && let Some(request) = self.request_baselines.get_mut(&previous.workspace_id)
        {
            request.touched_placements.insert(id.clone());
        }
        if let Some(request) = self.request_baselines.get_mut(&workspace) {
            request.touched_placements.insert(id);
        }
    }

    // Semantic removal only. Coordinator LRU eviction retains directory rows
    // and must not mark a request-local deletion.
    pub(crate) fn remove_thread(&mut self, id: &str) {
        let thread = self.threads.remove(id);
        let placement = self.placements.remove(id);
        for workspace in thread
            .iter()
            .map(|t| &t.workspace_id)
            .chain(placement.iter().map(|p| &p.workspace_id))
        {
            if let Some(request) = self.request_baselines.get_mut(workspace) {
                request.touched_threads.insert(id.to_owned());
                request.touched_placements.insert(id.to_owned());
            }
            self.dirty_workspaces.insert(workspace.clone());
        }
        self.unread.remove(id);
        self.running_task_thread_ids.remove(id);
    }

    pub(crate) fn push_demand(&self, workspace: &str) -> Option<Vec<String>> {
        if self.suspended_workspaces.contains(workspace)
            || !self.subscriptions.contains_key(workspace)
            || !self
                .loads
                .get(workspace)
                .is_some_and(|load| load.state == ThreadTreeLoadState::Ready)
        {
            return None;
        }
        Some(
            self.threads
                .values()
                .filter(|t| t.workspace_id == workspace)
                .map(|t| t.id.clone())
                .collect(),
        )
    }
    pub(crate) fn push_epoch_is_current(&self, workspace: &str, epoch: &str) -> bool {
        !self.suspended_workspaces.contains(workspace)
            && self.subscriptions.contains_key(workspace)
            && self
                .push_revisions
                .get(workspace)
                .is_some_and(|(current, _)| current == epoch)
    }
    pub(crate) fn apply_push_indicators(
        &mut self,
        packet: &pioneer_protocol::WorkspacePushNotification,
    ) -> bool {
        if self
            .push_revisions
            .get(&packet.workspace_id)
            .is_none_or(|(epoch, _)| epoch != &packet.epoch)
            || packet.indicators.iter().any(|patch| {
                !self
                    .threads
                    .get(&patch.thread_id)
                    .is_some_and(|thread| thread.workspace_id == packet.workspace_id)
            })
        {
            return false;
        }
        for patch in &packet.indicators {
            if let Some(count) = patch.unread_count {
                self.unread.insert(patch.thread_id.clone(), count);
                *self
                    .read_revisions
                    .entry(patch.thread_id.clone())
                    .or_default() += 1;
            }
            if let Some(running) = patch.has_running_tasks {
                if running {
                    self.running_task_thread_ids.insert(patch.thread_id.clone());
                } else {
                    self.running_task_thread_ids.remove(&patch.thread_id);
                }
            }
        }
        self.push_revisions.insert(
            packet.workspace_id.clone(),
            (packet.epoch.clone(), packet.revision),
        );
        true
    }
    pub(crate) fn read_position_is_confirmed(&self, thread: &str, sort_key: &str) -> bool {
        self.cursors
            .get(thread)
            .is_some_and(|cursor| cursor.sort_key.as_str() >= sort_key)
    }

    pub(crate) fn has_unread(&self, thread: &str) -> bool {
        self.unread.get(thread).copied().unwrap_or(0) > 0
    }
    pub fn snapshot(&self, workspace: &str) -> Option<Arc<ThreadTreePublication>> {
        self.projections.snapshot(workspace)
    }
    pub(crate) fn project(
        &mut self,
        workspace: &str,
        draft: Option<&str>,
        loading: bool,
        error: Option<String>,
    ) -> Option<Arc<ThreadTreePublication>> {
        let snapshot = thread_tree_snapshot_from_parts(
            workspace.to_owned(),
            self.threads
                .values()
                .filter(|t| {
                    t.workspace_id == workspace
                        && crate::threads::tree::thread_should_appear_in_sidebar(t, draft)
                })
                .cloned()
                .collect(),
            self.running_task_thread_ids.iter().cloned().collect(),
            self.unread
                .iter()
                .map(|(id, count)| ThreadUnreadSummary {
                    thread_id: id.clone(),
                    unread_count: *count,
                })
                .collect(),
            self.folders
                .values()
                .filter(|f| f.workspace_id == workspace)
                .cloned()
                .collect(),
            self.placements
                .values()
                .filter(|p| p.workspace_id == workspace)
                .cloned()
                .collect(),
            self.agents_docs
                .values()
                .filter(|a| a.workspace_id == workspace)
                .cloned()
                .collect(),
        );
        self.projections
            .update(snapshot, loading, error, self.loads.get(workspace).cloned())
    }
    pub(crate) fn invalidate(&mut self) {
        let generation = self.next_request.saturating_add(1);
        let mut authorization_reloads = std::mem::take(&mut self.authorization_reloads);
        authorization_reloads.extend(self.projections.publications.keys().cloned());
        authorization_reloads.extend(self.subscriptions.keys().cloned());
        let revisions = std::mem::take(&mut self.projections.revisions);
        let subscriptions = std::mem::take(&mut self.subscriptions);
        let suspended_workspaces = std::mem::take(&mut self.suspended_workspaces);
        let mut lifecycle_generations = std::mem::take(&mut self.lifecycle_generations);
        lifecycle_generations
            .values_mut()
            .for_each(|generation| *generation += 1);
        *self = Self::default();
        self.subscriptions = subscriptions;
        self.authorization_reloads = authorization_reloads;
        self.suspended_workspaces = suspended_workspaces;
        self.lifecycle_generations = lifecycle_generations;
        self.next_request = generation;
        self.projections.revisions = revisions;
    }
    pub(crate) fn synchronize_revision(&mut self, workspace: &str, revision: u64) {
        self.projections
            .revisions
            .insert(workspace.to_owned(), revision);
    }
    fn begin(&mut self, workspace: &str) -> u64 {
        self.authorization_reloads.remove(workspace);
        self.next_request = self
            .next_request
            .checked_add(1)
            .expect("directory request generation exhausted");
        self.requests
            .insert(workspace.to_owned(), self.next_request);
        self.request_baselines.insert(
            workspace.to_owned(),
            DirectoryRequestBaseline {
                push_revision: self.push_revisions.get(workspace).cloned(),
                threads: self
                    .threads
                    .iter()
                    .filter(|(_, t)| t.workspace_id == workspace)
                    .map(|(id, t)| (id.clone(), t.clone()))
                    .collect(),
                folders: self
                    .folders
                    .iter()
                    .filter(|(_, f)| f.workspace_id == workspace)
                    .map(|(id, f)| (id.clone(), f.clone()))
                    .collect(),
                placements: self
                    .placements
                    .iter()
                    .filter(|(_, p)| p.workspace_id == workspace)
                    .map(|(id, p)| (id.clone(), p.clone()))
                    .collect(),
                agents_docs: self
                    .agents_docs
                    .iter()
                    .filter(|((w, _), _)| w == workspace)
                    .map(|(id, a)| (id.clone(), a.clone()))
                    .collect(),
                read_revisions: self
                    .read_revisions
                    .iter()
                    .filter(|(id, _)| {
                        self.threads
                            .get(*id)
                            .is_some_and(|t| t.workspace_id == workspace)
                    })
                    .map(|(id, revision)| (id.clone(), *revision))
                    .collect(),
                ..Default::default()
            },
        );
        self.loads.insert(
            workspace.to_owned(),
            ThreadTreeLoadReceipt {
                generation: self.next_request,
                state: ThreadTreeLoadState::Pending,
            },
        );
        self.next_request
    }
    fn finish_load(&mut self, workspace: &str, generation: u64, failed: bool) {
        if let Some(load) = self.loads.get_mut(workspace).filter(|load| {
            load.generation == generation && load.state == ThreadTreeLoadState::Pending
        }) {
            load.state = if failed {
                ThreadTreeLoadState::Failed
            } else {
                ThreadTreeLoadState::Ready
            };
        }
    }
    fn complete(&mut self, workspace: &str, generation: u64, response: ThreadTreeResponse) -> bool {
        if response.workspace_id != workspace
            || self.requests.get(workspace) != Some(&generation)
            || response
                .threads
                .iter()
                .any(|thread| thread.workspace_id != workspace)
            || response
                .folders
                .iter()
                .any(|folder| folder.workspace_id != workspace)
            || response
                .placements
                .iter()
                .any(|placement| placement.workspace_id != workspace)
            || response
                .agents_docs
                .iter()
                .any(|summary| summary.workspace_id != workspace)
        {
            return false;
        }
        self.requests.remove(workspace);
        let before = self
            .request_baselines
            .remove(workspace)
            .expect("request has baseline");
        // Request/connection/access fences remain authoritative. Compare rows
        // against the request baseline so unrelated local metadata does not
        // suppress fresh indicators, including changes to unprojected drafts.
        let mut response = response.into_directory_summary();
        // A valid old snapshot can still contain IDs moved to another workspace
        // locally. Keep those row fences without discarding independent fresh
        // indicators. A payload whose own workspace fields disagree was rejected
        // above; moved rows never enter either workspace through this response.
        response.threads.retain(|thread| {
            self.threads
                .get(&thread.id)
                .is_none_or(|known| known.workspace_id == workspace)
        });
        response.folders.retain(|folder| {
            self.folders
                .get(&folder.id)
                .is_none_or(|known| known.workspace_id == workspace)
        });
        response.placements.retain(|placement| {
            self.threads
                .get(&placement.thread_id)
                .is_none_or(|known| known.workspace_id == workspace)
        });
        let response_ids = response
            .threads
            .iter()
            .map(|t| t.id.clone())
            .collect::<std::collections::HashSet<_>>();
        merge_directory_rows(
            &mut self.threads,
            before.threads,
            before.touched_threads,
            response.threads.into_iter().map(|t| (t.id.clone(), t)),
            |t| t.workspace_id == workspace,
        );
        merge_directory_rows(
            &mut self.folders,
            before.folders,
            Default::default(),
            response.folders.into_iter().map(|f| (f.id.clone(), f)),
            |f| f.workspace_id == workspace,
        );
        merge_directory_rows(
            &mut self.placements,
            before.placements,
            before.touched_placements,
            response
                .placements
                .into_iter()
                .map(|p| (p.thread_id.clone(), p)),
            |p| p.workspace_id == workspace,
        );
        merge_directory_rows(
            &mut self.agents_docs,
            before.agents_docs,
            Default::default(),
            response
                .agents_docs
                .into_iter()
                .map(|a| ((workspace.to_owned(), a.folder_id.clone()), a)),
            |a| a.workspace_id == workspace,
        );
        self.placements.retain(|id, placement| {
            placement.workspace_id != workspace || self.threads.contains_key(id)
        });
        self.unread.retain(|id, _| self.threads.contains_key(id));
        self.running_task_thread_ids
            .retain(|id| self.threads.contains_key(id));
        let incoming_unread = response
            .unread
            .into_iter()
            .map(|u| (u.thread_id, u.unread_count))
            .collect::<HashMap<_, _>>();
        let incoming_running = response
            .running_task_thread_ids
            .into_iter()
            .collect::<std::collections::HashSet<_>>();
        let mut reconciled = true;
        for id in response_ids {
            if !self
                .threads
                .get(&id)
                .is_some_and(|t| t.workspace_id == workspace)
            {
                continue; // Never resurrect a locally deleted/moved thread.
            }
            let push_unchanged =
                self.push_revisions.get(workspace) == before.push_revision.as_ref();
            if push_unchanged && incoming_running.contains(&id) {
                self.running_task_thread_ids.insert(id.clone());
            } else if push_unchanged {
                self.running_task_thread_ids.remove(&id);
            }
            if !push_unchanged {
                continue;
            }
            if self.read_revisions.get(&id) == before.read_revisions.get(&id) {
                if let Some(count) = incoming_unread.get(&id) {
                    self.unread.insert(id.clone(), *count);
                } else {
                    self.unread.remove(&id);
                }
            } else {
                // Counts have no cursor in the wire contract. Keep the newer
                // confirmed read and reconcile through the coalescing queue.
                // Pending local read requests never enter this cursor map.
                reconciled = false;
            }
        }
        reconciled
    }
}
impl ClientCore {
    pub(crate) fn workspace_subscription_changed(&self, scope: &ClientScope, added: bool) {
        let ClientScope::WorkspaceTree {
            workspace_id: Some(workspace),
        } = scope
        else {
            return;
        };
        let (suspended, resumed) = {
            let mut registry = self
                .thread_registry
                .lock()
                .expect("thread registry poisoned");
            let resumed = added && registry.directory.suspended_workspaces.remove(workspace);
            let count = registry
                .directory
                .subscriptions
                .entry(workspace.clone())
                .or_default();
            *count = if added {
                count.saturating_add(1)
            } else {
                count.saturating_sub(1)
            };
            let suspended = *count == 0;
            if suspended {
                registry.directory.subscriptions.remove(workspace);
            }
            (suspended, resumed)
        };
        if suspended {
            self.workspace_demand_changed(scope, crate::core::ClientDemand::Suspended);
        } else if added {
            self.resume_workspace_directory_demand();
            if resumed && self.authorization_snapshot(None, None).is_some() {
                self.queue_directory_refresh(workspace);
            }
        }
        self.queue_workspace_push(workspace);
    }
    pub(crate) fn resume_workspace_directory_demand(&self) {
        if self.is_stopped() || self.authorization_snapshot(None, None).is_none() {
            return;
        }
        let workspaces = {
            let registry = self
                .thread_registry
                .lock()
                .expect("thread registry poisoned");
            registry
                .directory
                .authorization_reloads
                .difference(&registry.directory.suspended_workspaces)
                .cloned()
                .collect::<Vec<_>>()
        };
        for workspace in workspaces {
            self.queue_directory_refresh(&workspace);
        }
    }
    pub(crate) fn workspace_refresh_is_demanded(&self, workspace: &str) -> bool {
        let registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        (registry.directory.snapshot(workspace).is_some()
            || registry.directory.authorization_reloads.contains(workspace))
            && !registry.directory.suspended_workspaces.contains(workspace)
    }
    pub(crate) fn workspace_operation_generation(&self, workspace: &str) -> u64 {
        *self
            .thread_registry
            .lock()
            .expect("thread registry poisoned")
            .directory
            .lifecycle_generations
            .get(workspace)
            .unwrap_or(&0)
    }
    pub(crate) fn workspace_demand_changed(
        &self,
        scope: &ClientScope,
        demand: crate::core::ClientDemand,
    ) {
        let ClientScope::WorkspaceTree {
            workspace_id: Some(workspace),
        } = scope
        else {
            return;
        };
        if demand != crate::core::ClientDemand::Suspended {
            let resumed = self
                .thread_registry
                .lock()
                .expect("thread registry poisoned")
                .directory
                .suspended_workspaces
                .remove(workspace);
            self.resume_workspace_directory_demand();
            // Lifecycle hints are intentionally not fetched while suspended.
            // Re-enter through the existing queue to recover any missed state.
            if resumed && self.authorization_snapshot(None, None).is_some() {
                self.queue_directory_refresh(workspace);
            }
            self.queue_workspace_push(workspace);
            return;
        }
        self.cancel_workspace_requests(workspace);
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        registry
            .directory
            .suspended_workspaces
            .insert(workspace.clone());
        *registry
            .directory
            .lifecycle_generations
            .entry(workspace.clone())
            .or_default() += 1;
        registry.directory.requests.remove(workspace);
        registry.directory.request_baselines.remove(workspace);
        if registry
            .directory
            .initial_restore
            .as_ref()
            .is_some_and(|initial| initial.workspace == *workspace)
        {
            registry.directory.cancel_initial_restore();
        }
        registry.directory.push_revisions.remove(workspace);
        let Some(previous) = registry.directory.snapshot(workspace) else {
            drop(registry);
            self.queue_workspace_push(workspace);
            return;
        };
        let mut next = previous.as_ref().clone();
        next.loading = false;
        next.actions.values_mut().for_each(|action| {
            if action.pending {
                action.pending = false;
                action.revision += 1;
            }
        });
        if next == *previous {
            drop(registry);
            self.queue_workspace_push(workspace);
            return;
        }
        next.revision += 1;
        let publication = Arc::new(next);
        registry
            .directory
            .projections
            .revisions
            .insert(workspace.clone(), publication.revision);
        registry
            .directory
            .projections
            .publications
            .insert(workspace.clone(), publication.clone());
        self.publish_workspace_tree(publication);
        drop(registry);
        self.queue_workspace_push(workspace);
    }
    pub fn workspace_tree(&self, workspace: &str) -> Option<Arc<ThreadTreePublication>> {
        self.thread_registry
            .lock()
            .expect("thread registry poisoned")
            .directory
            .snapshot(workspace)
    }
    pub fn refresh_workspace_tree(
        &self,
        workspace: &str,
    ) -> anyhow::Result<Arc<ThreadTreePublication>> {
        self.refresh_workspace_tree_with_transport(
            workspace,
            &self.transport_runtime().ws_command_sender(),
        )
    }
    pub(super) fn refresh_workspace_tree_with_transport(
        &self,
        workspace: &str,
        transport: &impl crate::rpc::JsonRpcRequestTransport,
    ) -> anyhow::Result<Arc<ThreadTreePublication>> {
        anyhow::ensure!(!self.is_stopped(), "Client runtime is stopped");
        let connection = self.gateway_http_generation();
        let generation = {
            let mut registry = self
                .thread_registry
                .lock()
                .expect("thread registry poisoned");
            let generation = registry.directory.begin(workspace);
            let draft = registry.navigation.drafts.get(workspace).cloned();
            if let Some(publication) =
                registry
                    .directory
                    .project(workspace, draft.as_deref(), true, None)
            {
                self.publish_workspace_tree(publication);
            }
            generation
        };
        let result = crate::transport::ws::command_sender::thread_tree(
            transport,
            crate::threads::tree::thread_tree_params(workspace),
        );
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        if self.is_stopped()
            || connection != self.gateway_http_generation()
            || registry.directory.requests.get(workspace) != Some(&generation)
        {
            // Retire only this request, never a replacement generation. Its
            // touched keys must not outlive a fenced/error completion.
            if registry.directory.requests.get(workspace) == Some(&generation) {
                registry.directory.requests.remove(workspace);
                registry.directory.request_baselines.remove(workspace);
            }
            anyhow::bail!("Directory request is stale");
        }
        let mut reconcile_counts = false;
        let error = match result {
            Ok(response) => {
                if registry.directory.complete(workspace, generation, response) {
                    None
                } else if !registry.directory.requests.contains_key(workspace) {
                    reconcile_counts = true;
                    None
                } else {
                    registry.directory.requests.remove(workspace);
                    registry.directory.request_baselines.remove(workspace);
                    Some("Directory response scope mismatch".into())
                }
            }
            Err(error) => {
                registry.directory.requests.remove(workspace);
                registry.directory.request_baselines.remove(workspace);
                Some(format!("{error:#}"))
            }
        };
        if !reconcile_counts {
            registry
                .directory
                .finish_load(workspace, generation, error.is_some());
        }
        let draft = registry.navigation.drafts.get(workspace).cloned();
        if let Some(publication) =
            registry
                .directory
                .project(workspace, draft.as_deref(), false, error)
        {
            self.publish_workspace_tree(publication);
        }
        let publication = registry
            .directory
            .snapshot(workspace)
            .expect("directory request publishes");
        let threads = registry
            .directory
            .threads
            .values()
            .filter(|thread| {
                thread.workspace_id == workspace && registry.has_thread_store(&thread.id)
            })
            .cloned()
            .collect::<Vec<_>>();
        drop(registry);
        // Active thread coordinators consume metadata from this directory owner.
        for thread in threads {
            self.upsert_existing_directory_thread(thread);
        }
        if reconcile_counts {
            // One coalesced follow-up for actual confirmed-read changes only.
            // Metadata churn cannot create a self-sustaining refresh loop.
            self.queue_directory_refresh(workspace);
            anyhow::bail!("Directory read changed during loading; reconciling counts");
        }
        self.queue_workspace_push(workspace);
        Ok(publication)
    }
    pub(crate) fn publish_workspace_tree(&self, publication: Arc<ThreadTreePublication>) {
        self.publish(
            &ClientMutationAuthority { _private: () },
            ClientScope::WorkspaceTree {
                workspace_id: Some(publication.snapshot.workspace_id.clone()),
            },
            crate::threads::registry::revisions(publication.revision),
            publication,
            vec![],
        );
    }
}

impl ClientCore {
    pub fn apply_directory_read(
        &self,
        workspace: &str,
        thread: &str,
        cursor: &pioneer_protocol::ThreadReadCursor,
        unread: u64,
    ) {
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        self.apply_directory_read_locked(&mut registry, workspace, thread, cursor, unread);
    }

    pub(crate) fn apply_directory_read_locked(
        &self,
        registry: &mut crate::threads::registry::ThreadRegistry,
        workspace: &str,
        thread: &str,
        cursor: &pioneer_protocol::ThreadReadCursor,
        unread: u64,
    ) {
        if self.is_stopped()
            || !registry
                .directory
                .threads
                .get(thread)
                .is_some_and(|t| t.workspace_id == workspace)
        {
            return;
        }
        if registry
            .directory
            .cursors
            .get(thread)
            .is_some_and(|previous| previous.sort_key > cursor.sort_key)
        {
            return;
        }
        // Negotiated publisher owns counts; legacy reads still confirm the cursor.
        let unread = if registry.directory.push_revisions.contains_key(workspace) {
            registry.directory.unread.get(thread).copied().unwrap_or(0)
        } else {
            unread
        };
        let read_changed = registry.directory.cursors.get(thread) != Some(cursor)
            || registry.directory.unread.get(thread).copied().unwrap_or(0) != unread;
        if read_changed {
            *registry
                .directory
                .read_revisions
                .entry(thread.to_owned())
                .or_default() += 1;
        }
        registry
            .directory
            .cursors
            .insert(thread.to_owned(), cursor.clone());
        if registry.directory.unread.get(thread).copied().unwrap_or(0) == unread {
            return;
        }
        registry.directory.unread.insert(thread.to_owned(), unread);
        let draft = registry.navigation.drafts.get(workspace).cloned();
        if let Some(publication) =
            registry
                .directory
                .project(workspace, draft.as_deref(), false, None)
        {
            self.publish_workspace_tree(publication);
        }
    }
}

impl ClientCore {
    pub fn directory_thread_ids(&self, workspace: &str) -> Vec<String> {
        let registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        let draft = registry
            .navigation
            .drafts
            .get(workspace)
            .map(String::as_str);
        let mut threads = registry
            .directory
            .threads
            .values()
            .filter(|thread| {
                thread.workspace_id == workspace
                    && crate::threads::tree::thread_should_appear_in_sidebar(thread, draft)
            })
            .collect::<Vec<_>>();
        threads.sort_by(|a, b| {
            b.updated_at
                .cmp(&a.updated_at)
                .then_with(|| a.id.cmp(&b.id))
        });
        threads
            .into_iter()
            .map(|thread| thread.id.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn thread(id: &str, workspace: &str, time: i64) -> Thread {
        serde_json::from_value(serde_json::json!({"id":id,"workspace_id":workspace,"preview":"","mode":"Chat","model":"model","model_provider":"provider","created_at":time,"updated_at":time,"status":"Idle","turns":[]})).unwrap()
    }
    fn snapshot(threads: Vec<Thread>, unread: Vec<ThreadUnreadSummary>) -> ThreadTreeSnapshot {
        thread_tree_snapshot_from_parts("a".into(), threads, vec![], unread, vec![], vec![], vec![])
    }

    fn conversation_thread(id: &str, workspace: &str, time: i64, effort: Option<&str>) -> Thread {
        let mut thread = thread(id, workspace, time);
        thread.model = format!("model-{id}");
        thread.reasoning_effort = effort.map(str::to_owned);
        thread.turns = ["older", "latest"]
            .map(|suffix| pioneer_protocol::Turn {
                work_owner: Default::default(),
                id: format!("{id}-{suffix}"),
                status: pioneer_protocol::TurnStatus::Completed,
                turn_kind: Default::default(),
                origin: Default::default(),
                mode: Default::default(),
                author: None,
                reply_to_turn_id: None,
                mentions: vec![],
                message_revision: 0,
                message_deleted: false,
                error: None,
                prompt_manifest: None,
                permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
            })
            .into();
        thread
    }

    fn load_directory(core: &ClientCore, workspace: &str, threads: Vec<Thread>) {
        let mut registry = core.thread_registry.lock().unwrap();
        let generation = registry.directory.begin(workspace);
        assert!(registry.directory.complete(
            workspace,
            generation,
            ThreadTreeResponse {
                running_task_thread_ids: Vec::new(),
                workspace_id: workspace.into(),
                threads,
                unread: vec![],
                folders: vec![],
                placements: vec![],
                agents_docs: vec![],
            }
        ));
    }

    fn selected_model(
        core: &ClientCore,
        active: Option<&str>,
        workspace: &str,
    ) -> Option<crate::composer::model_selection::ComposerModelSelection> {
        let coordinators = core.thread_coordinator_snapshots();
        let selection = crate::state::selectors::resolve_composer_model_selection_from(
            active,
            Some(workspace),
            &coordinators,
        );
        if let Some(thread_id) = active {
            let expected = coordinators.get(thread_id).and_then(|coordinator| {
                crate::state::selectors::resolve_composer_model_selection_from(
                    Some(thread_id),
                    Some(&coordinator.workspace_id),
                    &coordinators,
                )
            });
            assert_eq!(core.resolved_composer_model_selection(thread_id), expected);
        }
        selection
    }

    struct TreeReply<'a, F> {
        core: &'a ClientCore,
        response: ThreadTreeResponse,
        during: F,
    }
    impl<F: Fn(&ClientCore)> crate::rpc::JsonRpcRequestTransport for TreeReply<'_, F> {
        fn send_json_rpc_request(
            &self,
            _: String,
            _: String,
            tx: crate::rpc::JsonRpcResponseSender,
        ) -> Result<(), String> {
            (self.during)(self.core);
            tx.send(Ok(serde_json::to_value(&self.response).unwrap()))
                .unwrap();
            Ok(())
        }
    }
    fn response(
        threads: Vec<Thread>,
        running: &[&str],
        unread: &[(&str, u64)],
    ) -> ThreadTreeResponse {
        ThreadTreeResponse {
            workspace_id: "a".into(),
            threads,
            running_task_thread_ids: running.iter().map(|id| (*id).into()).collect(),
            unread: unread
                .iter()
                .map(|(id, count)| ThreadUnreadSummary {
                    thread_id: (*id).into(),
                    unread_count: *count,
                })
                .collect(),
            folders: vec![],
            placements: vec![],
            agents_docs: vec![],
        }
    }

    #[test]
    fn background_and_late_directory_reads_preserve_navigation() {
        use crate::navigation::NavigationIntent;
        for late in [false, true] {
            let core = ClientCore::new();
            core.navigate(
                NavigationIntent::SelectWorkspace {
                    workspace_id: Some(if late { "a" } else { "b" }.into()),
                },
                None,
            );
            let publication = core
                .refresh_workspace_tree_with_transport(
                    "a",
                    &TreeReply {
                        core: &core,
                        response: response(vec![thread("a1", "a", 1)], &[], &[]),
                        during: |core: &ClientCore| {
                            if late {
                                core.navigate(
                                    NavigationIntent::SelectWorkspace {
                                        workspace_id: Some("b".into()),
                                    },
                                    None,
                                );
                            }
                        },
                    },
                )
                .unwrap();
            assert_eq!(core.navigation_snapshot().workspace_id(), Some("b"));
            assert!(core.navigation_snapshot().active_thread_id().is_none());
            assert!(publication.snapshot().threads_by_id.contains_key("a1"));
            assert_eq!(
                publication.load().unwrap().state,
                ThreadTreeLoadState::Ready
            );
        }
    }

    #[test]
    fn rpc_and_live_directory_summaries_preserve_titles_drafts_and_metadata() {
        let core = ClientCore::new();
        let mut full = conversation_thread("t", "a", 1, Some("high"));
        full.preview = "First message 😀\n".repeat(200);
        for name in [None, Some(""), Some("   "), Some("Named")] {
            full.name = name.map(str::to_owned);
            let summary = thread_directory_summary(full.clone());
            let rpc = response(vec![full.clone()], &[], &[]).into_directory_summary();
            assert_eq!(rpc.threads, vec![summary.clone()]);
            assert_eq!(thread_directory_summary(summary.clone()), summary);
            assert_eq!(
                crate::threads::title::thread_display_title(&summary),
                crate::threads::title::thread_display_title(&full)
            );
            let mut expected = full.clone();
            expected.preview = summary.preview.clone();
            expected.turns = vec![full.turns.last().unwrap().clone()];
            assert_eq!(summary, expected);
            if name == Some("Named") {
                assert_eq!(summary.preview.chars().count(), 160);
                assert!(
                    summary.preview.capacity() <= 4 * 160,
                    "retained summary must release the full preview buffer"
                );
            } else {
                assert_eq!(summary.preview, full.preview);
            }
            core.upsert_thread(full.clone());
            let published = core.workspace_tree("a").unwrap();
            assert_eq!(published.snapshot().threads_by_id["t"], summary);
            assert_eq!(
                core.thread_snapshot("t")
                    .unwrap()
                    .coordinator()
                    .thread()
                    .unwrap()
                    .preview,
                full.preview
            );
        }
        let draft = thread_directory_summary(thread("draft", "a", 1));
        assert!(draft.preview.is_empty() && draft.turns.is_empty());
    }

    #[test]
    fn directory_summary_preserves_first_message_sentinel_after_long_whitespace() {
        let mut full = thread("t", "a", 1);
        full.name = Some("Named".into());
        for preview in [
            format!("{}first message", "\u{2003}".repeat(170)),
            " ".repeat(400),
        ] {
            full.preview = preview;
            let summary = thread_directory_summary(full.clone());
            assert_eq!(
                summary.preview.trim().is_empty(),
                full.preview.trim().is_empty()
            );
            assert_eq!(thread_directory_summary(summary.clone()), summary);
            let rpc = response(vec![full.clone()], &[], &[]).into_directory_summary();
            assert_eq!(rpc.threads, vec![summary.clone()]);
            if !full.preview.trim().is_empty() {
                assert_eq!(summary.preview, full.preview);
            } else {
                assert_eq!(summary.preview.chars().count(), 160);
            }
        }
    }

    #[test]
    fn in_flight_notification_moves_back_to_baseline_keep_current_ancestor_indicators() {
        use pioneer_protocol::{GatewayNotification, ThreadUpdatedNotification};

        let core = ClientCore::new();
        let fixture = folder_directory();
        let mut initial = response(vec![thread("t", "a", 1), thread("other", "a", 1)], &[], &[]);
        initial.folders = fixture.folders.values().cloned().collect();
        initial.placements = fixture.placements.values().cloned().collect();
        initial.placements.push(ThreadPlacement {
            thread_id: "other".into(),
            workspace_id: "a".into(),
            folder_id: Some("right".into()),
        });
        core.refresh_workspace_tree_with_transport(
            "a",
            &TreeReply {
                core: &core,
                response: initial.clone(),
                during: |_: &ClientCore| {},
            },
        )
        .unwrap();
        let mut incoming = initial;
        incoming
            .placements
            .iter_mut()
            .find(|p| p.thread_id == "t")
            .unwrap()
            .folder_id = Some("right".into());
        incoming.running_task_thread_ids = vec!["t".into(), "other".into()];
        incoming.unread = [("t", 7), ("other", 5)]
            .into_iter()
            .map(|(id, count)| ThreadUnreadSummary {
                thread_id: id.into(),
                unread_count: count,
            })
            .collect();
        let published = core
            .refresh_workspace_tree_with_transport(
                "a",
                &TreeReply {
                    core: &core,
                    response: incoming,
                    during: |core: &ClientCore| {
                        for (index, folder) in ["child", "right", "child"].into_iter().enumerate() {
                            assert!(core.apply_thread_notification(
                                GatewayNotification::ThreadUpdated(ThreadUpdatedNotification {
                                    thread: thread("t", "a", 1),
                                    placement: Some(ThreadPlacement {
                                        thread_id: "t".into(),
                                        workspace_id: "a".into(),
                                        folder_id: Some(folder.into())
                                    }),
                                })
                            ));
                            if index == 0 {
                                let registry = core.thread_registry.lock().unwrap();
                                let request = &registry.directory.request_baselines["a"];
                                assert!(request.touched_threads.is_empty());
                                assert!(request.touched_placements.is_empty());
                            }
                        }
                    },
                },
            )
            .unwrap();
        assert_eq!(
            published.snapshot().placements_by_thread_id["t"]
                .folder_id
                .as_deref(),
            Some("child")
        );
        for (folder, count) in [("child", 7), ("left", 7), ("root", 12), ("right", 5)] {
            assert_eq!(published.snapshot().unread_by_folder_id[folder], count);
            assert!(
                published
                    .snapshot()
                    .running_task_folder_ids
                    .contains(folder)
            );
        }
        assert!(
            published
                .snapshot()
                .running_task_thread_ids
                .contains("other")
        );
        assert_eq!(published.load().unwrap().state, ThreadTreeLoadState::Ready);
        assert!(
            core.thread_registry
                .lock()
                .unwrap()
                .directory
                .request_baselines
                .is_empty()
        );
    }

    #[test]
    fn in_flight_notification_creation_then_removal_does_not_resurrect_unknown_id() {
        use pioneer_protocol::{
            GatewayNotification, ThreadClosedNotification, ThreadUpdatedNotification,
        };

        let core = ClientCore::new();
        let mut incoming = response(
            vec![thread("transient", "a", 1)],
            &["transient"],
            &[("transient", 9)],
        );
        incoming.placements.push(ThreadPlacement {
            thread_id: "transient".into(),
            workspace_id: "a".into(),
            folder_id: None,
        });
        let published = core
            .refresh_workspace_tree_with_transport(
                "a",
                &TreeReply {
                    core: &core,
                    response: incoming.clone(),
                    during: |core: &ClientCore| {
                        assert!(core.apply_thread_notification(
                            GatewayNotification::ThreadUpdated(ThreadUpdatedNotification {
                                thread: thread("transient", "a", 1),
                                placement: incoming.placements.first().cloned(),
                            })
                        ));
                        assert!(
                            core.apply_thread_notification(GatewayNotification::ThreadClosed(
                                ThreadClosedNotification {
                                    workspace_id: "a".into(),
                                    thread_id: "transient".into(),
                                }
                            ))
                        );
                    },
                },
            )
            .unwrap();
        assert!(!published.snapshot().threads_by_id.contains_key("transient"));
        let registry = core.thread_registry.lock().unwrap();
        assert!(!registry.directory.placements.contains_key("transient"));
        assert!(!registry.directory.unread.contains_key("transient"));
        assert!(
            !registry
                .directory
                .running_task_thread_ids
                .contains("transient")
        );
        assert!(registry.directory.request_baselines.is_empty());
        drop(registry);
        // Request-local protection is gone; a later authoritative response can
        // accept this ID normally, without retained deletion history.
        let fresh = core
            .refresh_workspace_tree_with_transport(
                "a",
                &TreeReply {
                    core: &core,
                    response: incoming,
                    during: |_: &ClientCore| {},
                },
            )
            .unwrap();
        assert!(fresh.snapshot().threads_by_id.contains_key("transient"));
        assert!(
            fresh
                .snapshot()
                .running_task_thread_ids
                .contains("transient")
        );
        assert_eq!(fresh.snapshot().unread[0].unread_count, 9);
        assert!(
            core.thread_registry
                .lock()
                .unwrap()
                .directory
                .request_baselines
                .is_empty()
        );
    }

    #[test]
    fn refresh_preserves_local_rows_but_delivers_independent_activity_and_unread() {
        let core = ClientCore::new();
        core.upsert_thread(thread("local", "a", 1));
        core.upsert_thread(thread("other", "a", 1));
        core.upsert_thread(thread("deleted", "a", 1));
        let reply = TreeReply {
            core: &core,
            response: response(
                vec![
                    thread("local", "a", 1),
                    thread("other", "a", 1),
                    thread("deleted", "a", 1),
                ],
                &["other"],
                &[("other", 7)],
            ),
            during: |core: &ClientCore| {
                let mut local = thread("local", "a", 2);
                local.name = Some("fresh".into());
                core.upsert_thread(local);
                let mut registry = core.thread_registry.lock().unwrap();
                registry.directory.threads.remove("deleted");
                registry.directory.project("a", None, true, None);
            },
        };
        let published = core
            .refresh_workspace_tree_with_transport("a", &reply)
            .unwrap();
        assert_eq!(
            published.snapshot().threads_by_id["local"].name.as_deref(),
            Some("fresh")
        );
        assert!(!published.snapshot().threads_by_id.contains_key("deleted"));
        assert!(
            published
                .snapshot()
                .running_task_thread_ids
                .contains("other")
        );
        assert_eq!(
            published
                .snapshot()
                .unread
                .iter()
                .find(|u| u.thread_id == "other")
                .unwrap()
                .unread_count,
            7
        );
        assert_eq!(published.load().unwrap().state, ThreadTreeLoadState::Ready);
    }

    #[test]
    fn in_flight_placement_merge_keeps_moves_and_does_not_restore_deleted_membership() {
        let mut directory = folder_directory();
        directory
            .threads
            .insert("deleted".into(), thread("deleted", "a", 1));
        let mut incoming = response(
            vec![thread("t", "a", 1), thread("deleted", "a", 1)],
            &["t"],
            &[("t", 7)],
        );
        incoming.folders = directory.folders.values().cloned().collect();
        incoming.placements = directory.placements.values().cloned().collect();
        incoming.placements.push(ThreadPlacement {
            thread_id: "deleted".into(),
            workspace_id: "a".into(),
            folder_id: Some("left".into()),
        });
        let generation = directory.begin("a");
        directory.placements.get_mut("t").unwrap().folder_id = Some("right".into());
        directory.threads.remove("deleted");
        assert!(directory.complete("a", generation, incoming));
        assert_eq!(
            directory.placements["t"].folder_id.as_deref(),
            Some("right")
        );
        assert!(!directory.placements.contains_key("deleted"));
        let publication = directory.project("a", None, false, None).unwrap();
        assert_eq!(publication.snapshot().unread_by_folder_id["right"], 7);
        assert_eq!(publication.snapshot().unread_by_folder_id["left"], 0);
        assert!(
            publication
                .snapshot()
                .running_task_folder_ids
                .contains("right")
        );
    }

    #[test]
    fn local_workspace_transfer_fences_its_rows_without_dropping_other_indicators() {
        let mut directory = folder_directory();
        directory
            .threads
            .insert("other".into(), thread("other", "a", 1));
        let mut incoming = response(
            vec![thread("t", "a", 1), thread("other", "a", 1)],
            &["other"],
            &[("t", 99), ("other", 7)],
        );
        incoming.folders = directory.folders.values().cloned().collect();
        incoming.placements = directory.placements.values().cloned().collect();
        incoming.placements.push(ThreadPlacement {
            thread_id: "other".into(),
            workspace_id: "a".into(),
            folder_id: Some("right".into()),
        });
        let generation = directory.begin("a");
        directory.threads.get_mut("t").unwrap().workspace_id = "b".into();
        directory.placements.insert(
            "t".into(),
            ThreadPlacement {
                thread_id: "t".into(),
                workspace_id: "b".into(),
                folder_id: None,
            },
        );
        directory.unread.insert("t".into(), 4);
        directory.running_task_thread_ids.insert("t".into());
        assert!(directory.complete("a", generation, incoming));
        assert_eq!(directory.threads["t"].workspace_id, "b");
        assert_eq!(directory.placements["t"].workspace_id, "b");
        assert_eq!(directory.unread["t"], 4);
        let a = directory.project("a", None, false, None).unwrap();
        assert!(!a.snapshot().threads_by_id.contains_key("t"));
        assert!(a.snapshot().running_task_thread_ids.contains("other"));
        assert_eq!(a.snapshot().unread_by_folder_id["right"], 7);
        let b = directory.project("b", None, false, None).unwrap();
        assert!(b.snapshot().running_task_thread_ids.contains("t"));
        assert_eq!(b.snapshot().unread[0].unread_count, 4);
    }

    #[test]
    fn confirmed_read_conflict_is_partial_then_late_answer_reconciles() {
        let core = ClientCore::new();
        core.upsert_thread(thread("read", "a", 1));
        core.upsert_thread(thread("other", "a", 1));
        let cursor = pioneer_protocol::ThreadReadCursor {
            sort_key: "002".into(),
            through_turn_id: "turn".into(),
        };
        let reply = TreeReply {
            core: &core,
            response: response(
                vec![thread("read", "a", 1), thread("other", "a", 1)],
                &["other"],
                &[("read", 4), ("other", 3)],
            ),
            during: |core: &ClientCore| core.apply_directory_read("a", "read", &cursor, 0),
        };
        assert!(
            core.refresh_workspace_tree_with_transport("a", &reply)
                .is_err()
        );
        let partial = core.workspace_tree("a").unwrap();
        assert_eq!(partial.load().unwrap().state, ThreadTreeLoadState::Pending);
        assert!(partial.snapshot().running_task_thread_ids.contains("other"));
        // Unread summaries are sparse: a missing row also represents zero.
        assert_eq!(
            partial
                .snapshot()
                .unread
                .iter()
                .find(|u| u.thread_id == "read")
                .map(|u| u.unread_count)
                .unwrap_or(0),
            0
        );
        assert_eq!(
            partial
                .snapshot()
                .unread
                .iter()
                .find(|u| u.thread_id == "other")
                .unwrap()
                .unread_count,
            3
        );
        let followup = TreeReply {
            core: &core,
            response: response(
                vec![thread("read", "a", 1), thread("other", "a", 1)],
                &[],
                &[("read", 1)],
            ),
            during: |_: &ClientCore| {},
        };
        let ready = core
            .refresh_workspace_tree_with_transport("a", &followup)
            .unwrap();
        assert_eq!(ready.load().unwrap().state, ThreadTreeLoadState::Ready);
        assert_eq!(
            ready
                .snapshot()
                .unread
                .iter()
                .find(|u| u.thread_id == "read")
                .unwrap()
                .unread_count,
            1
        );
        core.apply_directory_read(
            "a",
            "read",
            &pioneer_protocol::ThreadReadCursor {
                sort_key: "001".into(),
                through_turn_id: "old".into(),
            },
            9,
        );
        assert_eq!(
            core.thread_registry.lock().unwrap().directory.cursors["read"],
            cursor
        );
        // A newer count at the same confirmed cursor also fences the response.
        let same_cursor = TreeReply {
            core: &core,
            response: followup.response.clone(),
            during: |core: &ClientCore| core.apply_directory_read("a", "read", &cursor, 0),
        };
        assert!(
            core.refresh_workspace_tree_with_transport("a", &same_cursor)
                .is_err()
        );
    }

    #[test]
    fn access_and_demand_fences_reject_in_flight_indicators() {
        let core = ClientCore::new();
        core.upsert_thread(thread("t", "a", 1));
        for suspend in [false, true] {
            let reply = TreeReply {
                core: &core,
                response: response(vec![thread("t", "a", 1)], &["t"], &[("t", 9)]),
                during: |core: &ClientCore| {
                    if suspend {
                        core.workspace_demand_changed(
                            &ClientScope::WorkspaceTree {
                                workspace_id: Some("a".into()),
                            },
                            crate::core::ClientDemand::Suspended,
                        );
                    } else {
                        core.invalidate_authorization_revision(88);
                    }
                },
            };
            assert!(
                core.refresh_workspace_tree_with_transport("a", &reply)
                    .is_err()
            );
            assert!(
                core.workspace_tree("a")
                    .is_none_or(|p| !p.snapshot().running_task_thread_ids.contains("t"))
            );
        }
    }

    #[test]
    fn composer_defaults_survive_directory_loading_and_coordinator_hydration() {
        use crate::composer::model_selection::ComposerModelSelection;
        let core = ClientCore::new();
        let older = conversation_thread("older", "a", 10, Some("high"));
        let latest = conversation_thread("latest", "a", 20, None);
        load_directory(
            &core,
            "a",
            vec![older.clone(), latest.clone(), thread("draft", "a", 30)],
        );
        load_directory(
            &core,
            "b",
            vec![conversation_thread("foreign", "b", 100, Some("low"))],
        );

        for hydrated in [false, true] {
            if hydrated {
                // The directory refresh feeds its compact records to active coordinators.
                let summaries = core
                    .thread_registry
                    .lock()
                    .unwrap()
                    .directory
                    .threads
                    .clone();
                for thread in summaries.into_values() {
                    core.upsert_thread(thread);
                }
            }
            assert_eq!(
                selected_model(&core, Some("older"), "a"),
                ComposerModelSelection::from_thread(&older)
            );
            assert_eq!(
                selected_model(&core, Some("latest"), "a"),
                ComposerModelSelection::from_thread(&latest)
            );
            for active in [None, Some("draft"), Some("not-created-yet")] {
                assert_eq!(
                    selected_model(&core, active, "a"),
                    ComposerModelSelection::from_thread(&latest)
                );
            }
        }
        let registry = core.thread_registry.lock().unwrap();
        assert_eq!(registry.directory.threads["older"].turns.len(), 1);
        assert_eq!(
            registry.directory.threads["older"].turns[0].id,
            "older-latest"
        );
        assert!(registry.directory.threads["draft"].turns.is_empty());
    }

    #[test]
    fn composer_defaults_survive_open_response_without_historical_turns() {
        use crate::composer::model_selection::ComposerModelSelection;
        let core = ClientCore::new();
        let mut docs = conversation_thread("docs", "a", 10, Some("max"));
        docs.model_provider = "cli_runtime:codex".into();
        docs.model = "gpt-5.6-luna".into();
        // Production Docs/Changle log end in TaskRun markers. They are not
        // copied into the legacy Conversation projection when metadata loads.
        for turn in &mut docs.turns {
            turn.turn_kind = pioneer_protocol::TurnKind::TaskRun;
        }
        let mut latest = conversation_thread("latest", "a", 20, Some("high"));
        latest.model_provider = "cli_runtime:codex".into();
        latest.model = "gpt-6-astra".into();
        load_directory(&core, "a", vec![docs.clone(), latest.clone()]);
        let expected = ComposerModelSelection::from_thread(&docs);
        assert_eq!(selected_model(&core, Some("docs"), "a"), expected);

        // Opening a persisted thread returns only the runtime's turns, which
        // are empty on a cold gateway, although the directory knows its history.
        let mut opened = docs.clone();
        opened.turns.clear();
        core.upsert_thread(opened.clone());
        assert_eq!(selected_model(&core, Some("docs"), "a"), expected);
        assert!(
            core.thread_snapshot("docs")
                .unwrap()
                .coordinator()
                .thread()
                .unwrap()
                .turns
                .is_empty(),
            "historical evidence must not become runtime turns"
        );
        assert_eq!(
            selected_model(&core, None, "a"),
            ComposerModelSelection::from_thread(&latest)
        );

        // The opening notification and later metadata refresh have the same
        // omission. History eviction must also keep the directory's evidence.
        core.upsert_thread(opened);
        assert_eq!(selected_model(&core, Some("docs"), "a"), expected);
        for index in 0..80 {
            core.upsert_thread(thread(&format!("empty-{index}"), "a", 100 + index));
        }
        assert!(core.thread_snapshot("docs").is_none());
        assert_eq!(selected_model(&core, Some("docs"), "a"), expected);
        core.remove_thread_store("docs");
        assert_eq!(
            selected_model(&core, Some("docs"), "a"),
            ComposerModelSelection::from_thread(&latest)
        );
    }

    #[test]
    fn composer_defaults_survive_history_eviction_but_not_thread_removal() {
        use crate::composer::model_selection::ComposerModelSelection;
        let core = ClientCore::new();
        let existing = conversation_thread("existing", "a", 10, Some("high"));
        core.upsert_thread(existing.clone());
        for index in 0..80 {
            core.upsert_thread(thread(&format!("empty-{index}"), "a", 100 + index));
        }
        assert!(
            core.thread_snapshot("existing").is_none(),
            "history was evicted"
        );
        assert_eq!(
            selected_model(&core, Some("existing"), "a"),
            ComposerModelSelection::from_thread(&existing)
        );
        assert_eq!(
            selected_model(&core, None, "a"),
            ComposerModelSelection::from_thread(&existing)
        );
        core.remove_thread_store("existing");
        assert_eq!(selected_model(&core, None, "a"), None);
        assert_eq!(selected_model(&core, Some("empty-79"), "a"), None);
    }
    #[test]
    fn equal_input_is_noop_and_unread_changes_only_its_semantic_node() {
        let mut store = SidebarProjectionStore::default();
        let a = thread("a1", "a", 1);
        let b = thread("a2", "a", 2);
        let initial = snapshot(vec![a.clone(), b.clone()], vec![]);
        let first = store.update(initial.clone(), false, None, None).unwrap();
        assert!(store.update(initial, false, None, None).is_none());
        assert!(Arc::ptr_eq(&first, &store.snapshot("a").unwrap()));
        let unread = snapshot(
            vec![b, a],
            vec![ThreadUnreadSummary {
                thread_id: "a1".into(),
                unread_count: 2,
            }],
        );
        let next = store.update(unread, false, None, None).unwrap();
        assert_eq!(next.revision, first.revision + 1);
        assert_eq!(
            next.changes.changed,
            vec![SidebarNodeId::Thread("a1".into())]
        );
        assert!(next.changes.reordered_folders.is_empty());
    }
    fn folder_directory() -> ThreadDirectoryStore {
        let mut directory = ThreadDirectoryStore::default();
        directory.threads.insert("t".into(), thread("t", "a", 1));
        directory.unread.insert("t".into(), 3);
        for (id, parent) in [
            ("root", None),
            ("left", Some("root")),
            ("child", Some("left")),
            ("right", Some("root")),
        ] {
            directory.folders.insert(
                id.into(),
                ThreadFolder {
                    id: id.into(),
                    workspace_id: "a".into(),
                    parent_folder_id: parent.map(str::to_owned),
                    name: id.into(),
                    created_at: 1,
                    updated_at: 1,
                },
            );
        }
        directory.placements.insert(
            "t".into(),
            ThreadPlacement {
                thread_id: "t".into(),
                workspace_id: "a".into(),
                folder_id: Some("child".into()),
            },
        );
        directory
    }

    #[test]
    fn task_activity_publishes_thread_and_ancestors_without_unread_or_navigation_changes() {
        let mut directory = folder_directory();
        directory.unread.clear();
        directory.project("a", None, false, None).unwrap();
        directory.running_task_thread_ids.insert("t".into());
        let active = directory.project("a", None, false, None).unwrap();
        for node in [
            SidebarNodeId::Thread("t".into()),
            SidebarNodeId::Folder("root".into()),
            SidebarNodeId::Folder("left".into()),
            SidebarNodeId::Folder("child".into()),
        ] {
            assert!(active.changes.changed.contains(&node));
        }
        assert!(active.changes.reordered_folders.is_empty());
        assert_eq!(active.snapshot.running_task_folder_ids.len(), 3);
        let decoded: ThreadTreePublication =
            serde_json::from_value(serde_json::to_value(active.as_ref()).unwrap()).unwrap();
        assert_eq!(&decoded, active.as_ref());
        // Partial thread/get/local publications preserve the authoritative activity.
        directory.threads.get_mut("t").unwrap().name = Some("renamed".into());
        let renamed = directory.project("a", None, false, None).unwrap();
        assert_eq!(
            renamed.snapshot.running_task_thread_ids,
            active.snapshot.running_task_thread_ids
        );
        directory.placements.get_mut("t").unwrap().folder_id = Some("right".into());
        let moved = directory.project("a", None, false, None).unwrap();
        assert_eq!(
            moved.snapshot.running_task_folder_ids,
            std::collections::HashSet::from(["root".into(), "right".into()])
        );
        for id in ["left", "child", "right"] {
            assert!(
                moved
                    .changes
                    .changed
                    .contains(&SidebarNodeId::Folder(id.into()))
            );
        }
        directory.placements.get_mut("t").unwrap().folder_id = Some("child".into());
        directory.project("a", None, false, None).unwrap();
        directory.folders.get_mut("child").unwrap().parent_folder_id = Some("right".into());
        let moved = directory.project("a", None, false, None).unwrap();
        assert!(!moved.snapshot.running_task_folder_ids.contains("left"));
        assert!(moved.snapshot.running_task_folder_ids.contains("right"));
        directory.running_task_thread_ids.clear();
        let done = directory.project("a", None, false, None).unwrap();
        assert!(done.snapshot.running_task_folder_ids.is_empty());
        assert!(
            done.changes
                .changed
                .contains(&SidebarNodeId::Thread("t".into()))
        );
        assert!(
            done.changes
                .changed
                .contains(&SidebarNodeId::Folder("root".into()))
        );
    }

    #[test]
    fn task_activity_survives_partial_thread_get_publications() {
        let core = ClientCore::new();
        {
            let mut registry = core.thread_registry.lock().unwrap();
            registry.directory = folder_directory();
            registry
                .directory
                .running_task_thread_ids
                .insert("t".into());
            core.publish_workspace_tree(
                registry.directory.project("a", None, false, None).unwrap(),
            );
        }
        let navigation = core.navigation_snapshot();
        core.upsert_thread(thread("t", "a", 2));
        let updated = core.workspace_tree("a").unwrap();
        assert!(updated.snapshot().running_task_thread_ids.contains("t"));
        assert!(updated.snapshot().running_task_folder_ids.contains("root"));
        assert_eq!(navigation.as_ref(), core.navigation_snapshot().as_ref());
    }

    #[test]
    fn task_activity_reloads_without_timeline_and_is_scoped_to_authoritative_visible_tree() {
        let mut directory = folder_directory();
        directory
            .threads
            .insert("foreign".into(), thread("foreign", "b", 1));
        let mut hidden = thread("hidden", "a", 1);
        hidden.sidebar_visibility = pioneer_protocol::ThreadSidebarVisibility::Hidden;
        let response = |ids: Vec<&str>| ThreadTreeResponse {
            workspace_id: "a".into(),
            threads: vec![thread("t", "a", 1), hidden.clone()],
            running_task_thread_ids: ids.into_iter().map(str::to_owned).collect(),
            unread: vec![],
            folders: directory.folders.values().cloned().collect(),
            placements: directory.placements.values().cloned().collect(),
            agents_docs: vec![],
        };
        let active_response = response(vec!["t", "foreign", "hidden"]);
        let inactive_response = response(vec![]);
        let generation = directory.begin("a");
        assert!(directory.complete("a", generation, active_response.clone()));
        let active = directory.project("a", None, false, None).unwrap();
        assert_eq!(
            active.snapshot.running_task_thread_ids,
            std::collections::HashSet::from(["t".into()])
        );
        assert!(
            directory
                .project("b", None, false, None)
                .unwrap()
                .snapshot
                .running_task_folder_ids
                .is_empty()
        );
        directory.threads.get_mut("t").unwrap().sidebar_visibility =
            pioneer_protocol::ThreadSidebarVisibility::Hidden;
        assert!(
            directory
                .project("a", None, false, None)
                .unwrap()
                .snapshot
                .running_task_folder_ids
                .is_empty()
        );
        directory.threads.get_mut("t").unwrap().sidebar_visibility =
            pioneer_protocol::ThreadSidebarVisibility::Visible;
        let visible = directory.project("a", None, false, None).unwrap();
        assert!(visible.snapshot.running_task_thread_ids.contains("t"));
        assert!(visible.snapshot.running_task_folder_ids.contains("root"));
        assert!(
            directory
                .project("a", Some("t"), false, None)
                .unwrap()
                .snapshot
                .running_task_folder_ids
                .is_empty()
        );
        directory.invalidate(); // authorization fence / reconnect clears protected state
        let generation = directory.begin("a");
        assert!(directory.complete("a", generation, active_response));
        assert!(
            directory
                .project("a", None, false, None)
                .unwrap()
                .snapshot
                .running_task_folder_ids
                .contains("root")
        );
        let generation = directory.begin("a");
        assert!(directory.complete("a", generation, inactive_response));
        assert!(
            directory
                .project("a", None, false, None)
                .unwrap()
                .snapshot
                .running_task_folder_ids
                .is_empty()
        );
        directory.threads.remove("t");
        assert!(
            directory
                .project("a", None, false, None)
                .unwrap()
                .snapshot
                .running_task_thread_ids
                .is_empty()
        );
    }

    #[test]
    fn folder_unread_read_updates_publish_ancestors_without_reordering() {
        let core = ClientCore::new();
        {
            let mut registry = core.thread_registry.lock().unwrap();
            registry.directory = folder_directory();
            let publication = registry.directory.project("a", None, false, None).unwrap();
            core.publish_workspace_tree(publication);
        }
        let navigation = core.navigation_snapshot();
        let mut revision = core.workspace_tree("a").unwrap().revision();
        for (sort_key, count) in [("1", 8), ("2", 0)] {
            core.apply_directory_read(
                "a",
                "t",
                &pioneer_protocol::ThreadReadCursor {
                    sort_key: sort_key.into(),
                    through_turn_id: "turn".into(),
                },
                count,
            );
            let publication = core.workspace_tree("a").unwrap();
            assert_eq!(publication.revision(), revision + 1);
            revision = publication.revision();
            for id in ["root", "left", "child"] {
                assert_eq!(publication.snapshot().unread_by_folder_id[id], count);
                assert!(
                    publication
                        .changes()
                        .changed()
                        .contains(&SidebarNodeId::Folder(id.into()))
                );
            }
            assert_eq!(publication.snapshot().unread_by_folder_id["right"], 0);
            assert!(publication.changes().reordered_folders().is_empty());
            let decoded: ThreadTreePublication =
                serde_json::from_value(serde_json::to_value(publication.as_ref()).unwrap())
                    .unwrap();
            assert_eq!(&decoded, publication.as_ref());
        }
        assert_eq!(core.navigation_snapshot().as_ref(), navigation.as_ref());
    }

    #[test]
    fn folder_unread_moves_remove_old_ancestors_and_add_new_ones() {
        let mut directory = folder_directory();
        directory.project("a", None, false, None).unwrap();
        directory.placements.get_mut("t").unwrap().folder_id = Some("right".into());
        let moved = directory.project("a", None, false, None).unwrap();
        assert_eq!(moved.snapshot.unread_by_folder_id["left"], 0);
        assert_eq!(moved.snapshot.unread_by_folder_id["child"], 0);
        assert_eq!(moved.snapshot.unread_by_folder_id["right"], 3);
        assert_eq!(moved.snapshot.unread_by_folder_id["root"], 3);
        for id in ["left", "child", "right"] {
            assert!(
                moved
                    .changes
                    .changed
                    .contains(&SidebarNodeId::Folder(id.into()))
            );
        }
        directory.placements.get_mut("t").unwrap().folder_id = Some("child".into());
        directory.project("a", None, false, None);
        directory.folders.get_mut("child").unwrap().parent_folder_id = Some("right".into());
        let moved = directory.project("a", None, false, None).unwrap();
        assert_eq!(moved.snapshot.unread_by_folder_id["left"], 0);
        assert_eq!(moved.snapshot.unread_by_folder_id["right"], 3);
        assert_eq!(moved.snapshot.unread_by_folder_id["root"], 3);
        assert!(
            moved
                .changes
                .changed
                .contains(&SidebarNodeId::Folder("left".into()))
        );
        assert!(
            moved
                .changes
                .changed
                .contains(&SidebarNodeId::Folder("right".into()))
        );
        directory.folders.get_mut("child").unwrap().parent_folder_id = None;
        let root = directory.project("a", None, false, None).unwrap();
        assert_eq!(root.snapshot.unread_by_folder_id["root"], 0);
        assert_eq!(root.snapshot.unread_by_folder_id["right"], 0);
        assert_eq!(root.snapshot.unread_by_folder_id["child"], 3);
        directory.placements.get_mut("t").unwrap().folder_id = None;
        let root = directory.project("a", None, false, None).unwrap();
        assert!(
            root.snapshot
                .unread_by_folder_id
                .values()
                .all(|count| *count == 0)
        );
        directory.placements.get_mut("t").unwrap().folder_id = Some("child".into());
        directory.project("a", None, false, None);
        directory.folders.get_mut("child").unwrap().name = "Renamed".into();
        let renamed = directory.project("a", None, false, None).unwrap();
        assert_eq!(renamed.snapshot.unread_by_folder_id["child"], 3);
        directory.folders.remove("child");
        let deleted = directory.project("a", None, false, None).unwrap();
        assert!(!deleted.snapshot.unread_by_folder_id.contains_key("child"));
        assert!(
            deleted
                .snapshot
                .unread_by_folder_id
                .values()
                .all(|count| *count == 0)
        );
        assert!(
            deleted
                .changes
                .removed
                .contains(&SidebarNodeId::Folder("child".into()))
        );
    }

    #[test]
    fn folder_unread_filters_visibility_drafts_foreign_and_removed_threads_and_reconnects() {
        let mut directory = folder_directory();
        directory
            .threads
            .insert("foreign".into(), thread("foreign", "b", 1));
        directory.unread.insert("foreign".into(), 90);
        directory.unread.insert("inaccessible".into(), 80);
        for id in ["foreign", "inaccessible"] {
            directory.placements.insert(
                id.into(),
                ThreadPlacement {
                    thread_id: id.into(),
                    workspace_id: "a".into(),
                    folder_id: Some("child".into()),
                },
            );
        }
        let initial = directory.project("a", None, false, None).unwrap();
        assert_eq!(initial.snapshot.unread_by_folder_id["root"], 3);
        let foreign = directory.project("b", None, false, None).unwrap();
        assert!(foreign.snapshot.unread_by_folder_id.is_empty());
        assert!(Arc::ptr_eq(&initial, &directory.snapshot("a").unwrap()));
        directory.threads.get_mut("t").unwrap().sidebar_visibility =
            pioneer_protocol::ThreadSidebarVisibility::Hidden;
        let hidden = directory.project("a", None, false, None).unwrap();
        assert_eq!(hidden.snapshot.unread_by_folder_id["root"], 0);
        assert!(
            hidden
                .changes
                .changed
                .contains(&SidebarNodeId::Folder("root".into()))
        );
        directory.threads.get_mut("t").unwrap().sidebar_visibility =
            pioneer_protocol::ThreadSidebarVisibility::Visible;
        directory.project("a", None, false, None).unwrap();
        let draft = directory.project("a", Some("t"), false, None).unwrap();
        assert_eq!(draft.snapshot.unread_by_folder_id["root"], 0);
        directory.project("a", None, false, None).unwrap();
        directory.threads.remove("t");
        let deleted = directory.project("a", None, false, None).unwrap();
        assert_eq!(deleted.snapshot.unread_by_folder_id["root"], 0);
        directory.invalidate();
        assert!(directory.snapshot("a").is_none());
        assert!(directory.snapshot("b").is_none());
        let generation = directory.begin("a");
        assert!(directory.complete(
            "a",
            generation,
            ThreadTreeResponse {
                running_task_thread_ids: Vec::new(),
                workspace_id: "a".into(),
                threads: vec![],
                unread: vec![],
                folders: vec![],
                placements: vec![],
                agents_docs: vec![],
            }
        ));
        let reconnected = directory.project("a", None, false, None).unwrap();
        assert!(reconnected.snapshot.unread_by_folder_id.is_empty());
        assert!(reconnected.revision > deleted.revision);
    }

    #[test]
    fn node_actions_complete_once_and_last_subscription_cancels_old_operation() {
        let core = Arc::new(ClientCore::new());
        core.upsert_thread(thread("a1", "a", 1));
        core.upsert_thread(thread("a2", "a", 1));
        let command = |id: &str| super::super::intents::WorkspaceIntent::RenameThread {
            workspace_id: "a".into(),
            thread_id: id.into(),
            name: "renamed".into(),
        };
        let old = core.begin_directory_action(&command("a1")).unwrap();
        let current = core.begin_directory_action(&command("a1")).unwrap();
        core.begin_directory_action(&command("a2"));
        let before = core.workspace_tree("a").unwrap();
        core.complete_directory_action(old, Some("stale".into()));
        assert!(Arc::ptr_eq(&before, &core.workspace_tree("a").unwrap()));
        core.complete_directory_action(current.clone(), None);
        let complete = core.workspace_tree("a").unwrap();
        assert_eq!(complete.actions["thread:a2"], before.actions["thread:a2"]);
        assert!(!complete.actions["thread:a1"].pending);
        core.complete_directory_action(current, Some("duplicate".into()));
        assert!(Arc::ptr_eq(&complete, &core.workspace_tree("a").unwrap()));
        let late = core.begin_directory_action(&command("a1")).unwrap();
        let subscription = core.subscribe(
            ClientScope::WorkspaceTree {
                workspace_id: Some("a".into()),
            },
            std::num::NonZeroUsize::new(4).unwrap(),
        );
        assert!(core.workspace_refresh_is_demanded("a"));
        drop(subscription);
        assert!(!core.workspace_refresh_is_demanded("a"));
        let closed = core.workspace_tree("a").unwrap();
        assert!(closed.actions.values().all(|action| !action.pending));
        core.complete_directory_action(late, Some("late".into()));
        assert!(Arc::ptr_eq(&closed, &core.workspace_tree("a").unwrap()));
        assert!(core.navigation_snapshot().active_thread_id().is_none());
        let scope = ClientScope::WorkspaceTree {
            workspace_id: Some("a".into()),
        };
        let _subscription = core.subscribe(scope.clone(), std::num::NonZeroUsize::new(4).unwrap());
        assert!(core.workspace_refresh_is_demanded("a"));
        core.workspace_demand_changed(&scope, crate::core::ClientDemand::Suspended);
        assert!(!core.workspace_refresh_is_demanded("a"));
        core.workspace_demand_changed(&scope, crate::core::ClientDemand::Visible);
        assert!(core.workspace_refresh_is_demanded("a"));
    }
    #[test]
    fn in_flight_tree_response_cannot_overwrite_a_semantic_event_or_foreign_node() {
        let mut directory = ThreadDirectoryStore::default();
        directory.threads.insert("a1".into(), thread("a1", "a", 1));
        directory.project("a", None, false, None);
        let generation = directory.begin("a");
        directory.threads.get_mut("a1").unwrap().name = Some("new title".into());
        directory.project("a", None, true, None);
        let response = |threads| ThreadTreeResponse {
            running_task_thread_ids: Vec::new(),
            workspace_id: "a".into(),
            threads,
            unread: vec![],
            folders: vec![],
            placements: vec![],
            agents_docs: vec![],
        };
        assert!(directory.complete("a", generation, response(vec![thread("a1", "a", 1)])));
        assert_eq!(directory.threads["a1"].name.as_deref(), Some("new title"));
        let generation = directory.begin("a");
        assert!(!directory.complete("a", generation, response(vec![thread("foreign", "b", 2)])));
        assert!(!directory.threads.contains_key("foreign"));
    }
    #[test]
    fn load_receipt_follows_replacement_and_survives_local_projections() {
        let mut directory = ThreadDirectoryStore::default();
        let first = directory.begin("a");
        directory.project("a", None, true, None).unwrap();
        let replacement = directory.begin("a");
        let pending = directory.project("a", None, true, None).unwrap();
        assert_eq!(pending.load().unwrap().generation, replacement);
        directory.finish_load("a", first, true);
        // An unrelated metadata/unread projection must not claim that the
        // still-pending replacement completed, even when its UI loading is false.
        let local = directory.project("a", None, false, None).unwrap();
        assert_eq!(local.load().unwrap().state, ThreadTreeLoadState::Pending);
        directory.finish_load("a", replacement, false);
        let ready = directory.project("a", None, false, None).unwrap();
        assert!(ready.snapshot().threads_by_id.is_empty());
        assert_eq!(ready.load().unwrap().state, ThreadTreeLoadState::Ready);
        let next = directory.begin("a");
        directory.finish_load("a", next, true);
        let failed = directory
            .project("a", None, false, Some("request failed".into()))
            .unwrap();
        assert_eq!(failed.load().unwrap().state, ThreadTreeLoadState::Failed);
        let local = directory.project("a", None, false, None).unwrap();
        assert_eq!(local.load(), failed.load());
        directory.invalidate();
        assert!(directory.loads.is_empty());
    }

    #[test]
    fn request_scope_and_generation_reject_late_inputs() {
        let mut directory = ThreadDirectoryStore::default();
        let first = directory.begin("a");
        let second = directory.begin("a");
        let response = |workspace: &str| ThreadTreeResponse {
            running_task_thread_ids: Vec::new(),
            workspace_id: workspace.into(),
            threads: vec![],
            unread: vec![],
            folders: vec![],
            placements: vec![],
            agents_docs: vec![],
        };
        assert!(!directory.complete("a", first, response("a")));
        assert!(!directory.complete("a", second, response("b")));
        assert!(directory.complete("a", second, response("a")));
        assert!(!directory.complete("a", second, response("a")));
        directory.invalidate();
        assert!(directory.snapshot("a").is_none());
    }
    #[test]
    fn insert_remove_reorder_keeps_domain_ids_and_scope() {
        let mut store = SidebarProjectionStore::default();
        store.update(
            snapshot(vec![thread("a1", "a", 1), thread("a2", "a", 2)], vec![]),
            false,
            None,
            None,
        );
        let next = store
            .update(
                snapshot(vec![thread("a2", "a", 3), thread("a3", "a", 4)], vec![]),
                false,
                None,
                None,
            )
            .unwrap();
        assert_eq!(
            next.snapshot.thread_ids_by_folder_id["__root__"],
            vec!["a3", "a2"]
        );
        assert_eq!(
            next.changes.removed,
            vec![SidebarNodeId::Thread("a1".into())]
        );
        assert_eq!(
            serde_json::from_value::<ThreadTreePublication>(
                serde_json::to_value(next.as_ref()).unwrap()
            )
            .unwrap(),
            *next
        );
        assert!(store.snapshot("b").is_none());
    }
}

impl ClientCore {
    pub(crate) fn begin_directory_action(
        &self,
        intent: &super::intents::WorkspaceIntent,
    ) -> Option<(String, String, u64)> {
        let workspace = intent.workspace_id()?;
        let key = directory_action_key(intent)?;
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        let mut publication = registry.directory.snapshot(workspace)?.as_ref().clone();
        registry.directory.next_request += 1;
        let generation = registry.directory.next_request;
        let revision = publication
            .actions
            .get(&key)
            .map_or(1, |action| action.revision + 1);
        publication.actions.insert(
            key.clone(),
            DirectoryActionPublication {
                revision,
                generation,
                pending: true,
                error: None,
            },
        );
        publication.revision += 1;
        publication.changes = SidebarChangeSet::default();
        if let Some(node) = directory_action_node(&key) {
            publication.changes.changed.push(node);
        }
        let publication = Arc::new(publication);
        registry
            .directory
            .projections
            .revisions
            .insert(workspace.to_owned(), publication.revision);
        registry
            .directory
            .projections
            .publications
            .insert(workspace.to_owned(), publication.clone());
        self.publish_workspace_tree(publication);
        Some((workspace.to_owned(), key, generation))
    }
    pub(crate) fn complete_directory_action(
        &self,
        operation: (String, String, u64),
        error: Option<String>,
    ) {
        let (workspace, key, generation) = operation;
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        let Some(previous) = registry.directory.snapshot(&workspace) else {
            return;
        };
        if self.is_stopped()
            || !previous
                .actions
                .get(&key)
                .is_some_and(|action| action.pending && action.generation == generation)
        {
            return;
        }
        let mut publication = previous.as_ref().clone();
        let revision = publication.actions[&key].revision + 1;
        publication.actions.insert(
            key.clone(),
            DirectoryActionPublication {
                revision,
                generation,
                pending: false,
                error,
            },
        );
        publication.revision += 1;
        publication.changes = SidebarChangeSet::default();
        if let Some(node) = directory_action_node(&key) {
            publication.changes.changed.push(node);
        }
        let publication = Arc::new(publication);
        registry
            .directory
            .projections
            .revisions
            .insert(workspace.clone(), publication.revision);
        registry
            .directory
            .projections
            .publications
            .insert(workspace, publication.clone());
        self.publish_workspace_tree(publication);
    }
}
fn directory_action_key(intent: &super::intents::WorkspaceIntent) -> Option<String> {
    use super::intents::WorkspaceIntent::*;
    match intent {
        RenameThread { thread_id, .. }
        | DeleteThread { thread_id, .. }
        | MoveThread { thread_id, .. } => Some(format!("thread:{thread_id}")),
        RenameFolder { folder_id, .. }
        | DeleteFolder { folder_id, .. }
        | MoveFolder { folder_id, .. } => Some(format!("folder:{folder_id}")),
        NewThread { workspace_id } => Some(format!("workspace:{workspace_id}:new-thread")),
        CreateFolder { workspace_id, .. } => Some(format!("workspace:{workspace_id}:new-folder")),
        RemoveAgentsDocument {
            workspace_id,
            folder_id,
        } => Some(format!(
            "workspace:{workspace_id}:agents:{}",
            folder_id.as_deref().unwrap_or("root")
        )),
        _ => None,
    }
}

fn directory_action_node(key: &str) -> Option<SidebarNodeId> {
    key.strip_prefix("thread:")
        .map(|id| SidebarNodeId::Thread(id.to_owned()))
        .or_else(|| {
            key.strip_prefix("folder:")
                .map(|id| SidebarNodeId::Folder(id.to_owned()))
        })
}

#[cfg(feature = "test-support")]
impl ClientMutationAuthority {
    /// Publish a controlled read state through the existing directory projection.
    pub fn publish_workspace_tree_for_test(
        &self,
        core: &ClientCore,
        workspace: &str,
        loading: bool,
        error: Option<String>,
    ) {
        let mut registry = core.thread_registry.lock().unwrap();
        let draft = registry.navigation.draft(workspace).map(str::to_owned);
        if let Some(publication) =
            registry
                .directory
                .project(workspace, draft.as_deref(), loading, error)
        {
            core.publish_workspace_tree(publication);
        }
    }
}
