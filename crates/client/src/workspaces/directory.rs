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
pub(crate) fn thread_directory_summary(mut thread: Thread) -> Thread {
    thread.turns = thread.turns.into_iter().next_back().into_iter().collect();
    thread
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
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThreadTreePublication {
    revision: u64,
    snapshot: ThreadTreeSnapshot,
    changes: SidebarChangeSet,
    #[serde(default)]
    actions: std::collections::BTreeMap<String, DirectoryActionPublication>,
    loading: bool,
    error: Option<String>,
}
impl ThreadTreePublication {
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
                || before_unread.get(id) != next_unread.get(id))
                && !changes.changed.contains(&SidebarNodeId::Thread(id.clone()))
            {
                changes.changed.push(SidebarNodeId::Thread(id.clone()));
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
        if previous.is_some_and(|p| changes.is_empty() && p.loading == loading && p.error == error)
        {
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

#[derive(Default)]
pub struct ThreadDirectoryStore {
    pub(crate) dirty_workspaces: std::collections::BTreeSet<String>,
    pub(crate) threads: HashMap<String, Thread>,
    pub(crate) placements: HashMap<String, ThreadPlacement>,
    pub(crate) summaries: HashMap<String, crate::threads::registry::SidebarSummaryChanged>,
    folders: HashMap<String, ThreadFolder>,
    agents_docs: HashMap<(String, Option<String>), ThreadAgentsDocSummary>,
    unread: HashMap<String, u64>,
    cursors: HashMap<String, pioneer_protocol::ThreadReadCursor>,
    requests: HashMap<String, u64>,
    subscriptions: HashMap<String, usize>,
    suspended_workspaces: BTreeSet<String>,
    lifecycle_generations: HashMap<String, u64>,
    request_versions: HashMap<String, u64>,
    semantic_versions: HashMap<String, u64>,
    next_request: u64,
    projections: SidebarProjectionStore,
}
impl ThreadDirectoryStore {
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
        let publication = self.projections.update(snapshot, loading, error);
        if publication
            .as_ref()
            .is_some_and(|publication| !publication.changes.is_empty())
        {
            *self
                .semantic_versions
                .entry(workspace.to_owned())
                .or_default() += 1;
        }
        publication
    }
    pub(crate) fn invalidate(&mut self) {
        let generation = self.next_request.saturating_add(1);
        let revisions = std::mem::take(&mut self.projections.revisions);
        let subscriptions = std::mem::take(&mut self.subscriptions);
        let suspended_workspaces = std::mem::take(&mut self.suspended_workspaces);
        let mut lifecycle_generations = std::mem::take(&mut self.lifecycle_generations);
        lifecycle_generations
            .values_mut()
            .for_each(|generation| *generation += 1);
        *self = Self::default();
        self.subscriptions = subscriptions;
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
        self.next_request = self
            .next_request
            .checked_add(1)
            .expect("directory request generation exhausted");
        self.requests
            .insert(workspace.to_owned(), self.next_request);
        self.request_versions.insert(
            workspace.to_owned(),
            self.semantic_versions.get(workspace).copied().unwrap_or(0),
        );
        self.next_request
    }
    fn complete(&mut self, workspace: &str, generation: u64, response: ThreadTreeResponse) -> bool {
        if response.workspace_id != workspace
            || self.requests.get(workspace) != Some(&generation)
            || response.threads.iter().any(|thread| {
                thread.workspace_id != workspace
                    || self
                        .threads
                        .get(&thread.id)
                        .is_some_and(|known| known.workspace_id != workspace)
            })
            || response.folders.iter().any(|folder| {
                folder.workspace_id != workspace
                    || self
                        .folders
                        .get(&folder.id)
                        .is_some_and(|known| known.workspace_id != workspace)
            })
            || response.placements.iter().any(|placement| {
                placement.workspace_id != workspace
                    || self
                        .threads
                        .get(&placement.thread_id)
                        .is_some_and(|known| known.workspace_id != workspace)
            })
            || response
                .agents_docs
                .iter()
                .any(|summary| summary.workspace_id != workspace)
        {
            return false;
        }
        self.requests.remove(workspace);
        if self.request_versions.remove(workspace)
            != Some(self.semantic_versions.get(workspace).copied().unwrap_or(0))
        {
            return false;
        }
        let removed = self
            .threads
            .values()
            .filter(|t| t.workspace_id == workspace)
            .map(|t| t.id.clone())
            .collect::<Vec<_>>();
        for id in removed {
            self.threads.remove(&id);
            self.unread.remove(&id);
        }
        self.folders.retain(|_, f| f.workspace_id != workspace);
        self.placements.retain(|_, p| p.workspace_id != workspace);
        self.agents_docs.retain(|(w, _), _| w != workspace);
        for thread in response
            .threads
            .into_iter()
            .filter(|t| t.workspace_id == workspace)
        {
            self.threads
                .insert(thread.id.clone(), thread_directory_summary(thread));
        }
        for folder in response
            .folders
            .into_iter()
            .filter(|f| f.workspace_id == workspace)
        {
            self.folders.insert(folder.id.clone(), folder);
        }
        for placement in response
            .placements
            .into_iter()
            .filter(|p| p.workspace_id == workspace)
        {
            self.placements
                .insert(placement.thread_id.clone(), placement);
        }
        for summary in response
            .agents_docs
            .into_iter()
            .filter(|a| a.workspace_id == workspace)
        {
            self.agents_docs
                .insert((workspace.to_owned(), summary.folder_id.clone()), summary);
        }
        for summary in response.unread {
            if self
                .threads
                .get(&summary.thread_id)
                .is_some_and(|t| t.workspace_id == workspace)
            {
                self.unread.insert(summary.thread_id, summary.unread_count);
            }
        }
        true
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
        let suspended = {
            let mut registry = self
                .thread_registry
                .lock()
                .expect("thread registry poisoned");
            if added {
                registry.directory.suspended_workspaces.remove(workspace);
            }
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
            suspended
        };
        if suspended {
            self.workspace_demand_changed(scope, crate::core::ClientDemand::Suspended);
        }
    }
    pub(crate) fn workspace_refresh_is_demanded(&self, workspace: &str) -> bool {
        let registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        registry.directory.snapshot(workspace).is_some()
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
            self.thread_registry
                .lock()
                .expect("thread registry poisoned")
                .directory
                .suspended_workspaces
                .remove(workspace);
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
        registry.directory.request_versions.remove(workspace);
        let Some(previous) = registry.directory.snapshot(workspace) else {
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
            let version = registry
                .directory
                .semantic_versions
                .get(workspace)
                .copied()
                .unwrap_or(0);
            registry
                .directory
                .request_versions
                .insert(workspace.to_owned(), version);
            generation
        };
        let result = self
            .compatibility_runtime()
            .ws_command_sender()
            .thread_tree(crate::threads::tree::thread_tree_params(workspace));
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        anyhow::ensure!(
            !self.is_stopped()
                && connection == self.gateway_http_generation()
                && registry.directory.requests.get(workspace) == Some(&generation),
            "Directory request is stale"
        );
        let error = match result {
            Ok(response) => {
                let changed_during_request =
                    registry.directory.request_versions.get(workspace).copied()
                        != Some(
                            registry
                                .directory
                                .semantic_versions
                                .get(workspace)
                                .copied()
                                .unwrap_or(0),
                        );
                if registry.directory.complete(workspace, generation, response) {
                    None
                } else if changed_during_request {
                    drop(registry);
                    self.queue_directory_refresh(workspace);
                    anyhow::bail!("Directory semantic generation changed during loading");
                } else {
                    registry.directory.requests.remove(workspace);
                    registry.directory.request_versions.remove(workspace);
                    Some("Directory response scope mismatch".into())
                }
            }
            Err(error) => {
                registry.directory.requests.remove(workspace);
                Some(format!("{error:#}"))
            }
        };
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
            .filter(|thread| thread.workspace_id == workspace)
            .cloned()
            .collect::<Vec<_>>();
        drop(registry);
        // Active thread coordinators consume metadata from this directory owner.
        for thread in threads {
            self.upsert_thread(thread);
        }
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
        if registry.directory.cursors.get(thread) != Some(cursor) {
            *registry
                .directory
                .semantic_versions
                .entry(workspace.to_owned())
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
        thread_tree_snapshot_from_parts("a".into(), threads, unread, vec![], vec![], vec![])
    }

    fn conversation_thread(id: &str, workspace: &str, time: i64, effort: Option<&str>) -> Thread {
        let mut thread = thread(id, workspace, time);
        thread.model = format!("model-{id}");
        thread.reasoning_effort = effort.map(str::to_owned);
        thread.turns = ["older", "latest"]
            .map(|suffix| pioneer_protocol::Turn {
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
        let first = store.update(initial.clone(), false, None).unwrap();
        assert!(store.update(initial, false, None).is_none());
        assert!(Arc::ptr_eq(&first, &store.snapshot("a").unwrap()));
        let unread = snapshot(
            vec![b, a],
            vec![ThreadUnreadSummary {
                thread_id: "a1".into(),
                unread_count: 2,
            }],
        );
        let next = store.update(unread, false, None).unwrap();
        assert_eq!(next.revision, first.revision + 1);
        assert_eq!(
            next.changes.changed,
            vec![SidebarNodeId::Thread("a1".into())]
        );
        assert!(next.changes.reordered_folders.is_empty());
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
            workspace_id: "a".into(),
            threads,
            unread: vec![],
            folders: vec![],
            placements: vec![],
            agents_docs: vec![],
        };
        assert!(!directory.complete("a", generation, response(vec![thread("a1", "a", 1)])));
        assert_eq!(directory.threads["a1"].name.as_deref(), Some("new title"));
        let generation = directory.begin("a");
        assert!(!directory.complete("a", generation, response(vec![thread("foreign", "b", 2)])));
        assert!(!directory.threads.contains_key("foreign"));
    }
    #[test]
    fn request_scope_and_generation_reject_late_inputs() {
        let mut directory = ThreadDirectoryStore::default();
        let first = directory.begin("a");
        let second = directory.begin("a");
        let response = |workspace: &str| ThreadTreeResponse {
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
        );
        let next = store
            .update(
                snapshot(vec![thread("a2", "a", 3), thread("a3", "a", 4)], vec![]),
                false,
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
