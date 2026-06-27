//! Thread tree normalization, placement rules, and sidebar tree projection.

use crate::agents_doc::scope::{AgentsDocEditorScope, ThreadAgentsDocSummaryKey};
use crate::threads::coordinator::ThreadCoordinator;
use pioneer_protocol::{
    Thread, ThreadAgentsDocSummary, ThreadFolder, ThreadFolderCreateParams,
    ThreadFolderDeleteParams, ThreadFolderMoveParams, ThreadMoveParams, ThreadPlacement,
    ThreadTreeParams, ThreadTreeResponse, ThreadUpdateParams, ThreadUpdateResponse,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

pub const SIDEBAR_THREAD_NODE_PREFIX: &str = "thread:";
pub const SIDEBAR_FOLDER_NODE_PREFIX: &str = "folder:";
pub const SIDEBAR_AGENTS_DOC_ROOT_NODE_ID: &str = "agents_doc:root";
pub const SIDEBAR_AGENTS_DOC_FOLDER_NODE_PREFIX: &str = "agents_doc:folder:";
pub const SIDEBAR_THREADS_HEADER_NODE_ID: &str = "__threads_header__";

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ThreadTreeSnapshotNormalization {
    pub folders_by_id: HashMap<String, ThreadFolder>,
    pub folder_expanded: HashMap<String, bool>,
    pub placements_by_thread_id: HashMap<String, ThreadPlacement>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkspaceThreadState {
    pub active_thread_id: Option<String>,
    pub draft_thread_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SidebarTreeNodeKind {
    ThreadsHeader,
    Thread { thread_id: String },
    Folder { folder_id: String },
    AgentsDocRoot,
    AgentsDocFolder { folder_id: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SidebarTreeNodeKey<'a> {
    ThreadsHeader,
    Thread(&'a str),
    Folder(&'a str),
    AgentsDocRoot,
    AgentsDocFolder(&'a str),
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SidebarTreeDragItemRef<'a> {
    Thread { thread_id: &'a str },
    Folder { folder_id: &'a str },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SidebarTreeItem {
    pub id: String,
    pub label: String,
    pub kind: SidebarTreeNodeKind,
    pub expanded: bool,
    pub disabled: bool,
    pub children: Vec<SidebarTreeItem>,
}

impl SidebarTreeItem {
    pub fn new(id: String, label: String, kind: SidebarTreeNodeKind) -> Self {
        Self {
            id,
            label,
            kind,
            expanded: false,
            disabled: false,
            children: Vec::new(),
        }
    }

    pub fn expanded(mut self, expanded: bool) -> Self {
        self.expanded = expanded;
        self
    }

    pub fn disabled(mut self, disabled: bool) -> Self {
        self.disabled = disabled;
        self
    }

    pub fn children(mut self, children: Vec<SidebarTreeItem>) -> Self {
        self.children = children;
        self
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SidebarTreeModel {
    pub items: Vec<SidebarTreeItem>,
    pub visible_node_ids: Vec<String>,
}

pub struct SidebarTreeSourceData<'a> {
    pub workspace_id: &'a str,
    pub folders: Vec<&'a ThreadFolder>,
    pub placements: Vec<&'a ThreadPlacement>,
    pub sorted_thread_ids: Vec<String>,
    pub agents_doc_summaries: Vec<&'a ThreadAgentsDocSummary>,
    pub active_agents_doc_editor_scope: Option<&'a AgentsDocEditorScope>,
    pub expanded_folder_ids: HashSet<String>,
}

pub fn thread_tree_params(workspace_id: impl Into<String>) -> ThreadTreeParams {
    ThreadTreeParams {
        workspace_id: workspace_id.into(),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ThreadTreeActionRejection {
    MissingWorkspace,
    MissingFolder,
    MissingThread,
    ForeignWorkspace,
    EmptyName,
    Unchanged,
    InvalidDestination,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ThreadFolderRenamePlan {
    Skip(ThreadTreeActionRejection),
    Request(ThreadFolderRenameRequest),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ThreadFolderRenameRequest {
    pub create: ThreadFolderCreateParams,
    pub old_folder_id: String,
    pub child_folder_ids: Vec<String>,
    pub child_thread_ids: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ThreadFolderRenameFollowUp {
    pub folder_moves: Vec<ThreadFolderMoveParams>,
    pub thread_moves: Vec<ThreadMoveParams>,
    pub delete: ThreadFolderDeleteParams,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ThreadRenamePlan {
    Skip(ThreadTreeActionRejection),
    Request(ThreadUpdateParams),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ThreadRenameSuccessReduction {
    pub thread: Thread,
    pub thread_id: String,
    pub workspace_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ThreadMovePlan {
    Skip(ThreadTreeActionRejection),
    Request(ThreadMoveParams),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ThreadFolderMovePlan {
    Skip(ThreadTreeActionRejection),
    Request(ThreadFolderMoveParams),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ThreadFolderDeletePlan {
    Skip(ThreadTreeActionRejection),
    Request(ThreadFolderDeleteParams),
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ThreadTreeRefreshContext<'a> {
    pub active_thread_id: Option<&'a str>,
    pub existing_draft_thread_id: Option<&'a str>,
    pub existing_draft_thread_workspace_id: Option<&'a str>,
    pub has_known_threads_for_workspace: bool,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ThreadTreeThreadAction {
    pub thread_id: String,
    pub workspace_id: String,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ThreadTreeRefreshSuccessReduction {
    pub workspace_id: String,
    pub threads: Vec<Thread>,
    pub folders: Vec<ThreadFolder>,
    pub placements: Vec<ThreadPlacement>,
    pub agents_docs: Vec<ThreadAgentsDocSummary>,
    pub set_active_thread_id: Option<String>,
    pub set_preferred_workspace_id: Option<String>,
    pub ensure_thread_subscription: Option<ThreadTreeThreadAction>,
    pub ensure_thread_timeline_loaded: Option<String>,
    pub request_thread_start_if_needed: bool,
    pub drive_thread_start_queue: bool,
    pub sync_composer_model_selection: bool,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ThreadTreeRefreshFailureReduction {
    pub request_thread_start_if_needed: bool,
    pub drive_thread_start_queue: bool,
}

pub fn reduce_thread_tree_refresh_success(
    response: ThreadTreeResponse,
    context: ThreadTreeRefreshContext<'_>,
) -> ThreadTreeRefreshSuccessReduction {
    let mut set_active_thread_id = None;
    let mut set_preferred_workspace_id = None;
    let mut ensure_thread_subscription = None;
    let mut ensure_thread_timeline_loaded = None;
    let mut request_thread_start_if_needed = false;
    let mut drive_thread_start_queue = false;

    match context.active_thread_id {
        Some(active_thread_id) => {
            ensure_thread_timeline_loaded = Some(active_thread_id.to_owned());
        }
        None => {
            if let Some(draft_thread_id) = context.existing_draft_thread_id {
                set_active_thread_id = Some(draft_thread_id.to_owned());
                ensure_thread_timeline_loaded = Some(draft_thread_id.to_owned());

                if let Some(workspace_id) = context.existing_draft_thread_workspace_id {
                    set_preferred_workspace_id = Some(workspace_id.to_owned());
                    ensure_thread_subscription = Some(ThreadTreeThreadAction {
                        thread_id: draft_thread_id.to_owned(),
                        workspace_id: workspace_id.to_owned(),
                    });
                }
            }

            request_thread_start_if_needed = true;
            drive_thread_start_queue = true;
        }
    }

    ThreadTreeRefreshSuccessReduction {
        workspace_id: response.workspace_id,
        threads: response.threads,
        folders: response.folders,
        placements: response.placements,
        agents_docs: response.agents_docs,
        set_active_thread_id,
        set_preferred_workspace_id,
        ensure_thread_subscription,
        ensure_thread_timeline_loaded,
        request_thread_start_if_needed,
        drive_thread_start_queue,
        sync_composer_model_selection: true,
    }
}

pub fn reduce_thread_tree_refresh_failure(
    context: ThreadTreeRefreshContext<'_>,
) -> ThreadTreeRefreshFailureReduction {
    if context.active_thread_id.is_some() || context.has_known_threads_for_workspace {
        return ThreadTreeRefreshFailureReduction::default();
    }

    ThreadTreeRefreshFailureReduction {
        request_thread_start_if_needed: true,
        drive_thread_start_queue: true,
    }
}

pub fn normalize_thread_tree_snapshot(
    folders: Vec<ThreadFolder>,
    placements: Vec<ThreadPlacement>,
    previous_folder_expanded: &HashMap<String, bool>,
) -> ThreadTreeSnapshotNormalization {
    let folders_by_id = folders
        .into_iter()
        .map(|folder| (folder.id.clone(), folder))
        .collect::<HashMap<_, _>>();

    let folder_expanded = folders_by_id
        .keys()
        .map(|folder_id| {
            (
                folder_id.clone(),
                previous_folder_expanded
                    .get(folder_id)
                    .copied()
                    .unwrap_or(false),
            )
        })
        .collect();

    let placements_by_thread_id = placements
        .into_iter()
        .map(|placement| (placement.thread_id.clone(), placement))
        .collect();

    ThreadTreeSnapshotNormalization {
        folders_by_id,
        folder_expanded,
        placements_by_thread_id,
    }
}

pub fn queue_thread_tree_refresh(refresh_requested: &mut bool) {
    *refresh_requested = true;
}

pub fn take_thread_tree_refresh_request(refresh_requested: &mut bool) -> bool {
    if !*refresh_requested {
        return false;
    }
    *refresh_requested = false;
    true
}

pub fn toggle_thread_folder_expanded(
    expanded_folders: &mut HashMap<String, bool>,
    folder_id: &str,
) -> bool {
    let expanded = !expanded_folders.get(folder_id).copied().unwrap_or(false);
    expanded_folders.insert(folder_id.to_owned(), expanded);
    expanded
}

pub fn set_thread_folder_expanded(
    expanded_folders: &mut HashMap<String, bool>,
    folder_id: &str,
    expanded: bool,
) {
    expanded_folders.insert(folder_id.to_owned(), expanded);
}

pub fn thread_folders_for_workspace<'a>(
    folders: &'a HashMap<String, ThreadFolder>,
    workspace_id: &str,
) -> Vec<&'a ThreadFolder> {
    folders
        .values()
        .filter(|folder| folder.workspace_id == workspace_id)
        .collect()
}

pub fn thread_placements_for_workspace<'a>(
    placements: &'a HashMap<String, ThreadPlacement>,
    workspace_id: &str,
) -> Vec<&'a ThreadPlacement> {
    placements
        .values()
        .filter(|placement| placement.workspace_id == workspace_id)
        .collect()
}

pub fn sidebar_thread_node_id(thread_id: &str) -> String {
    format!("{SIDEBAR_THREAD_NODE_PREFIX}{thread_id}")
}

pub fn sidebar_folder_node_id(folder_id: &str) -> String {
    format!("{SIDEBAR_FOLDER_NODE_PREFIX}{folder_id}")
}

pub fn sidebar_agents_doc_root_node_id() -> String {
    SIDEBAR_AGENTS_DOC_ROOT_NODE_ID.to_owned()
}

pub fn sidebar_agents_doc_folder_node_id(folder_id: &str) -> String {
    format!("{SIDEBAR_AGENTS_DOC_FOLDER_NODE_PREFIX}{folder_id}")
}

pub fn sidebar_agents_doc_node_id_for_scope(scope: &AgentsDocEditorScope) -> String {
    match scope {
        AgentsDocEditorScope::Root { .. } => sidebar_agents_doc_root_node_id(),
        AgentsDocEditorScope::Folder { folder_id, .. } => {
            sidebar_agents_doc_folder_node_id(folder_id.as_str())
        }
    }
}

pub fn parse_sidebar_tree_node_id(value: &str) -> SidebarTreeNodeKey<'_> {
    if value == SIDEBAR_THREADS_HEADER_NODE_ID {
        return SidebarTreeNodeKey::ThreadsHeader;
    }

    if let Some(thread_id) = value.strip_prefix(SIDEBAR_THREAD_NODE_PREFIX) {
        return SidebarTreeNodeKey::Thread(thread_id);
    }

    if let Some(folder_id) = value.strip_prefix(SIDEBAR_FOLDER_NODE_PREFIX) {
        return SidebarTreeNodeKey::Folder(folder_id);
    }

    if value == SIDEBAR_AGENTS_DOC_ROOT_NODE_ID {
        return SidebarTreeNodeKey::AgentsDocRoot;
    }

    if let Some(folder_id) = value.strip_prefix(SIDEBAR_AGENTS_DOC_FOLDER_NODE_PREFIX) {
        return SidebarTreeNodeKey::AgentsDocFolder(folder_id);
    }

    SidebarTreeNodeKey::Unknown
}

pub fn sidebar_tree_model_from_workspace_data(data: SidebarTreeSourceData<'_>) -> SidebarTreeModel {
    let items = sidebar_tree_items_from_workspace_data(data);
    let visible_node_ids = collect_visible_sidebar_node_ids(items.as_slice());
    SidebarTreeModel {
        items,
        visible_node_ids,
    }
}

pub fn sidebar_tree_items_from_workspace_data(
    data: SidebarTreeSourceData<'_>,
) -> Vec<SidebarTreeItem> {
    let folders_by_id: HashMap<String, &ThreadFolder> = data
        .folders
        .into_iter()
        .filter(|folder| folder.workspace_id == data.workspace_id)
        .map(|folder| (folder.id.clone(), folder))
        .collect();
    let folder_id_set: HashSet<String> = folders_by_id.keys().cloned().collect();
    let placements_by_thread_id: HashMap<String, &ThreadPlacement> = data
        .placements
        .into_iter()
        .filter(|placement| placement.workspace_id == data.workspace_id)
        .map(|placement| (placement.thread_id.clone(), placement))
        .collect();
    let mut visible_agents_doc_keys: HashSet<ThreadAgentsDocSummaryKey> = data
        .agents_doc_summaries
        .into_iter()
        .filter(|summary| summary.workspace_id == data.workspace_id)
        .map(|summary| ThreadAgentsDocSummaryKey::from_folder_id(summary.folder_id.as_deref()))
        .collect();

    if let Some(scope) = data
        .active_agents_doc_editor_scope
        .filter(|scope| scope.workspace_id() == data.workspace_id)
    {
        visible_agents_doc_keys
            .insert(ThreadAgentsDocSummaryKey::from_folder_id(scope.folder_id()));
    }

    let mut folders_by_parent: HashMap<String, Vec<String>> = HashMap::new();
    for folder in folders_by_id.values() {
        let parent_key = folder
            .parent_folder_id
            .as_deref()
            .filter(|parent_id| folder_id_set.contains(*parent_id))
            .unwrap_or_default()
            .to_owned();
        folders_by_parent
            .entry(parent_key)
            .or_default()
            .push(folder.id.clone());
    }

    for folder_ids in folders_by_parent.values_mut() {
        folder_ids.sort_by(|lhs, rhs| {
            let lhs_name = folders_by_id
                .get(lhs.as_str())
                .map(|folder| folder.name.as_str())
                .unwrap_or_default();
            let rhs_name = folders_by_id
                .get(rhs.as_str())
                .map(|folder| folder.name.as_str())
                .unwrap_or_default();
            lhs_name
                .to_lowercase()
                .cmp(&rhs_name.to_lowercase())
                .then_with(|| lhs.cmp(rhs))
        });
    }

    let mut threads_by_folder: HashMap<String, Vec<String>> = HashMap::new();
    for thread_id in data.sorted_thread_ids {
        let folder_key = placements_by_thread_id
            .get(thread_id.as_str())
            .and_then(|placement| placement.folder_id.as_deref())
            .filter(|folder_id| folder_id_set.contains(*folder_id))
            .unwrap_or_default()
            .to_owned();
        threads_by_folder
            .entry(folder_key)
            .or_default()
            .push(thread_id);
    }

    let mut visited_folders = HashSet::new();
    let mut items = sidebar_tree_branch_from_workspace_data(
        "",
        &folders_by_id,
        &folders_by_parent,
        &threads_by_folder,
        &visible_agents_doc_keys,
        &data.expanded_folder_ids,
        &mut visited_folders,
    );
    if visible_agents_doc_keys.contains(&ThreadAgentsDocSummaryKey::Root) {
        items.insert(
            0,
            SidebarTreeItem::new(
                sidebar_agents_doc_root_node_id(),
                "AGENTS.md".to_owned(),
                SidebarTreeNodeKind::AgentsDocRoot,
            ),
        );
    }

    sidebar_tree_items_with_header(items)
}

pub fn sidebar_tree_items_with_header(mut items: Vec<SidebarTreeItem>) -> Vec<SidebarTreeItem> {
    if items.is_empty() {
        return items;
    }

    items.insert(
        0,
        SidebarTreeItem::new(
            SIDEBAR_THREADS_HEADER_NODE_ID.to_owned(),
            "threads-header".to_owned(),
            SidebarTreeNodeKind::ThreadsHeader,
        )
        .disabled(true),
    );
    items
}

pub fn collect_visible_sidebar_node_ids(items: &[SidebarTreeItem]) -> Vec<String> {
    fn visit(items: &[SidebarTreeItem], out: &mut Vec<String>) {
        for item in items {
            out.push(item.id.clone());
            if item.expanded {
                visit(item.children.as_slice(), out);
            }
        }
    }

    let mut out = Vec::new();
    visit(items, &mut out);
    out
}

fn sidebar_tree_branch_from_workspace_data(
    parent_key: &str,
    folders_by_id: &HashMap<String, &ThreadFolder>,
    folders_by_parent: &HashMap<String, Vec<String>>,
    threads_by_folder: &HashMap<String, Vec<String>>,
    visible_agents_doc_keys: &HashSet<ThreadAgentsDocSummaryKey>,
    expanded_folder_ids: &HashSet<String>,
    visited_folders: &mut HashSet<String>,
) -> Vec<SidebarTreeItem> {
    let mut items = Vec::new();

    if let Some(folder_ids) = folders_by_parent.get(parent_key) {
        for folder_id in folder_ids {
            if !visited_folders.insert(folder_id.clone()) {
                continue;
            }

            let mut children = sidebar_tree_branch_from_workspace_data(
                folder_id.as_str(),
                folders_by_id,
                folders_by_parent,
                threads_by_folder,
                visible_agents_doc_keys,
                expanded_folder_ids,
                visited_folders,
            );
            let folder_summary_key = ThreadAgentsDocSummaryKey::Folder(folder_id.clone());
            if visible_agents_doc_keys.contains(&folder_summary_key) {
                children.insert(
                    0,
                    SidebarTreeItem::new(
                        sidebar_agents_doc_folder_node_id(folder_id.as_str()),
                        "AGENTS.md".to_owned(),
                        SidebarTreeNodeKind::AgentsDocFolder {
                            folder_id: folder_id.clone(),
                        },
                    ),
                );
            }

            let folder_name = folders_by_id
                .get(folder_id.as_str())
                .map(|folder| folder.name.clone())
                .unwrap_or_else(|| folder_id.clone());

            items.push(
                SidebarTreeItem::new(
                    sidebar_folder_node_id(folder_id.as_str()),
                    folder_name,
                    SidebarTreeNodeKind::Folder {
                        folder_id: folder_id.clone(),
                    },
                )
                .children(children)
                .expanded(expanded_folder_ids.contains(folder_id.as_str())),
            );
        }
    }

    if let Some(thread_ids) = threads_by_folder.get(parent_key) {
        for thread_id in thread_ids {
            items.push(SidebarTreeItem::new(
                sidebar_thread_node_id(thread_id.as_str()),
                thread_id.clone(),
                SidebarTreeNodeKind::Thread {
                    thread_id: thread_id.clone(),
                },
            ));
        }
    }

    items
}

pub fn sorted_thread_ids_from_coordinators(
    coordinators: &HashMap<String, ThreadCoordinator>,
    draft_thread_id: Option<&str>,
    workspace_id: Option<&str>,
) -> Vec<String> {
    let mut thread_ids: Vec<String> = coordinators
        .iter()
        .filter(|(thread_id, coordinator)| {
            Some(thread_id.as_str()) != draft_thread_id
                && workspace_id.is_none_or(|workspace_id| coordinator.workspace_id == workspace_id)
        })
        .map(|(thread_id, _)| thread_id.clone())
        .collect();
    thread_ids.sort_by(|lhs, rhs| {
        let lhs_updated = coordinators
            .get(lhs.as_str())
            .map(ThreadCoordinator::updated_at)
            .unwrap_or_default();
        let rhs_updated = coordinators
            .get(rhs.as_str())
            .map(ThreadCoordinator::updated_at)
            .unwrap_or_default();
        rhs_updated.cmp(&lhs_updated).then_with(|| lhs.cmp(rhs))
    });
    thread_ids
}

pub fn child_folder_ids_for_folder(
    folders: &HashMap<String, ThreadFolder>,
    workspace_id: &str,
    folder_id: &str,
) -> Vec<String> {
    folders
        .values()
        .filter(|folder| {
            folder.workspace_id == workspace_id
                && folder.parent_folder_id.as_deref() == Some(folder_id)
        })
        .map(|folder| folder.id.clone())
        .collect()
}

pub fn child_thread_ids_for_folder(
    placements: &HashMap<String, ThreadPlacement>,
    workspace_id: &str,
    folder_id: &str,
) -> Vec<String> {
    placements
        .values()
        .filter(|placement| {
            placement.workspace_id == workspace_id
                && placement.folder_id.as_deref() == Some(folder_id)
        })
        .map(|placement| placement.thread_id.clone())
        .collect()
}

pub fn next_new_folder_name(
    folders: &HashMap<String, ThreadFolder>,
    workspace_id: &str,
    base_name: &str,
) -> String {
    if !folder_name_exists(folders, workspace_id, base_name) {
        return base_name.to_owned();
    }

    for index in 2..10_000 {
        let name = format!("{base_name} {index}");
        if !folder_name_exists(folders, workspace_id, name.as_str()) {
            return name;
        }
    }

    base_name.to_owned()
}

pub fn can_move_folder_to(
    folders: &HashMap<String, ThreadFolder>,
    folder_id: &str,
    target_parent_folder_id: &str,
) -> bool {
    if folder_id == target_parent_folder_id {
        return false;
    }

    let Some(folder) = folders.get(folder_id) else {
        return false;
    };
    let Some(target_parent_folder) = folders.get(target_parent_folder_id) else {
        return false;
    };
    if folder.workspace_id != target_parent_folder.workspace_id {
        return false;
    }

    let mut visited = HashSet::new();
    let mut current_id = Some(target_parent_folder_id);
    while let Some(current_folder_id) = current_id {
        if current_folder_id == folder_id {
            return false;
        }
        if !visited.insert(current_folder_id) {
            return false;
        }
        current_id = folders
            .get(current_folder_id)
            .and_then(|current_folder| current_folder.parent_folder_id.as_deref());
    }

    true
}

pub fn can_drop_sidebar_tree_item_on_folder(
    folders: &HashMap<String, ThreadFolder>,
    active_workspace_id: Option<&str>,
    item: SidebarTreeDragItemRef<'_>,
    target_folder_id: &str,
) -> bool {
    let Some(target_folder) = folders.get(target_folder_id) else {
        return false;
    };
    if active_workspace_id.is_some_and(|workspace_id| target_folder.workspace_id != workspace_id) {
        return false;
    }

    match item {
        SidebarTreeDragItemRef::Thread { .. } => true,
        SidebarTreeDragItemRef::Folder { folder_id } => {
            can_move_folder_to(folders, folder_id, target_folder_id)
        }
    }
}

pub fn plan_thread_folder_create(
    folders: &HashMap<String, ThreadFolder>,
    workspace_id: Option<&str>,
    base_name: &str,
) -> Result<ThreadFolderCreateParams, ThreadTreeActionRejection> {
    let Some(workspace_id) = workspace_id.filter(|workspace_id| !workspace_id.trim().is_empty())
    else {
        return Err(ThreadTreeActionRejection::MissingWorkspace);
    };

    Ok(ThreadFolderCreateParams {
        workspace_id: workspace_id.to_owned(),
        parent_folder_id: None,
        name: next_new_folder_name(folders, workspace_id, base_name),
    })
}

pub fn plan_thread_folder_rename(
    folders: &HashMap<String, ThreadFolder>,
    placements: &HashMap<String, ThreadPlacement>,
    active_workspace_id: Option<&str>,
    folder_id: &str,
    new_name: &str,
) -> ThreadFolderRenamePlan {
    let Some(active_workspace_id) =
        active_workspace_id.filter(|workspace_id| !workspace_id.trim().is_empty())
    else {
        return ThreadFolderRenamePlan::Skip(ThreadTreeActionRejection::MissingWorkspace);
    };
    let Some(folder) = folders.get(folder_id) else {
        return ThreadFolderRenamePlan::Skip(ThreadTreeActionRejection::MissingFolder);
    };
    if folder.workspace_id != active_workspace_id {
        return ThreadFolderRenamePlan::Skip(ThreadTreeActionRejection::ForeignWorkspace);
    }

    let new_name = new_name.trim();
    if new_name.is_empty() {
        return ThreadFolderRenamePlan::Skip(ThreadTreeActionRejection::EmptyName);
    }
    if folder.name == new_name {
        return ThreadFolderRenamePlan::Skip(ThreadTreeActionRejection::Unchanged);
    }

    ThreadFolderRenamePlan::Request(ThreadFolderRenameRequest {
        create: ThreadFolderCreateParams {
            workspace_id: folder.workspace_id.clone(),
            parent_folder_id: folder.parent_folder_id.clone(),
            name: new_name.to_owned(),
        },
        old_folder_id: folder.id.clone(),
        child_folder_ids: child_folder_ids_for_folder(
            folders,
            folder.workspace_id.as_str(),
            folder.id.as_str(),
        ),
        child_thread_ids: child_thread_ids_for_folder(
            placements,
            folder.workspace_id.as_str(),
            folder.id.as_str(),
        ),
    })
}

pub fn thread_folder_rename_follow_up_params(
    request: &ThreadFolderRenameRequest,
    new_folder_id: impl Into<String>,
) -> ThreadFolderRenameFollowUp {
    let new_folder_id = new_folder_id.into();
    let workspace_id = request.create.workspace_id.clone();
    ThreadFolderRenameFollowUp {
        folder_moves: request
            .child_folder_ids
            .iter()
            .map(|folder_id| ThreadFolderMoveParams {
                workspace_id: workspace_id.clone(),
                folder_id: folder_id.clone(),
                parent_folder_id: Some(new_folder_id.clone()),
            })
            .collect(),
        thread_moves: request
            .child_thread_ids
            .iter()
            .map(|thread_id| ThreadMoveParams {
                workspace_id: workspace_id.clone(),
                thread_id: thread_id.clone(),
                folder_id: Some(new_folder_id.clone()),
            })
            .collect(),
        delete: ThreadFolderDeleteParams {
            workspace_id,
            folder_id: request.old_folder_id.clone(),
        },
    }
}

pub fn plan_thread_rename(
    coordinators: &HashMap<String, ThreadCoordinator>,
    workspace_id: Option<&str>,
    thread_id: &str,
    new_name: &str,
) -> ThreadRenamePlan {
    let Some(workspace_id) = workspace_id.filter(|workspace_id| !workspace_id.trim().is_empty())
    else {
        return ThreadRenamePlan::Skip(ThreadTreeActionRejection::MissingWorkspace);
    };
    let Some(coordinator) = coordinators.get(thread_id) else {
        return ThreadRenamePlan::Skip(ThreadTreeActionRejection::MissingThread);
    };
    if coordinator.workspace_id != workspace_id {
        return ThreadRenamePlan::Skip(ThreadTreeActionRejection::ForeignWorkspace);
    }

    let new_name = new_name.trim();
    if new_name.is_empty() {
        return ThreadRenamePlan::Skip(ThreadTreeActionRejection::EmptyName);
    }
    if coordinator
        .thread()
        .and_then(|thread| thread.name.as_deref())
        .is_some_and(|name| name == new_name)
    {
        return ThreadRenamePlan::Skip(ThreadTreeActionRejection::Unchanged);
    }

    ThreadRenamePlan::Request(ThreadUpdateParams {
        workspace_id: workspace_id.to_owned(),
        thread_id: thread_id.to_owned(),
        name: Some(new_name.to_owned()),
    })
}

pub fn reduce_thread_rename_success(
    response: ThreadUpdateResponse,
) -> ThreadRenameSuccessReduction {
    let thread = response.thread;
    ThreadRenameSuccessReduction {
        thread_id: thread.id.clone(),
        workspace_id: thread.workspace_id.clone(),
        thread,
    }
}

pub fn plan_thread_move(
    coordinators: &HashMap<String, ThreadCoordinator>,
    folders: &HashMap<String, ThreadFolder>,
    workspace_id: Option<&str>,
    thread_id: &str,
    folder_id: Option<&str>,
) -> ThreadMovePlan {
    let Some(workspace_id) = workspace_id.filter(|workspace_id| !workspace_id.trim().is_empty())
    else {
        return ThreadMovePlan::Skip(ThreadTreeActionRejection::MissingWorkspace);
    };
    let Some(coordinator) = coordinators.get(thread_id) else {
        return ThreadMovePlan::Skip(ThreadTreeActionRejection::MissingThread);
    };
    if coordinator.workspace_id != workspace_id {
        return ThreadMovePlan::Skip(ThreadTreeActionRejection::ForeignWorkspace);
    }

    if let Some(folder_id) = folder_id {
        let Some(folder) = folders.get(folder_id) else {
            return ThreadMovePlan::Skip(ThreadTreeActionRejection::MissingFolder);
        };
        if folder.workspace_id != workspace_id {
            return ThreadMovePlan::Skip(ThreadTreeActionRejection::ForeignWorkspace);
        }
    }

    ThreadMovePlan::Request(ThreadMoveParams {
        workspace_id: workspace_id.to_owned(),
        thread_id: thread_id.to_owned(),
        folder_id: folder_id.map(str::to_owned),
    })
}

pub fn plan_thread_folder_move(
    folders: &HashMap<String, ThreadFolder>,
    active_workspace_id: Option<&str>,
    folder_id: &str,
    parent_folder_id: Option<&str>,
) -> ThreadFolderMovePlan {
    let Some(active_workspace_id) =
        active_workspace_id.filter(|workspace_id| !workspace_id.trim().is_empty())
    else {
        return ThreadFolderMovePlan::Skip(ThreadTreeActionRejection::MissingWorkspace);
    };
    let Some(folder) = folders.get(folder_id) else {
        return ThreadFolderMovePlan::Skip(ThreadTreeActionRejection::MissingFolder);
    };
    if folder.workspace_id != active_workspace_id {
        return ThreadFolderMovePlan::Skip(ThreadTreeActionRejection::ForeignWorkspace);
    }

    if let Some(parent_folder_id) = parent_folder_id {
        let Some(parent_folder) = folders.get(parent_folder_id) else {
            return ThreadFolderMovePlan::Skip(ThreadTreeActionRejection::MissingFolder);
        };
        if parent_folder.workspace_id != folder.workspace_id {
            return ThreadFolderMovePlan::Skip(ThreadTreeActionRejection::ForeignWorkspace);
        }
        if !can_move_folder_to(folders, folder.id.as_str(), parent_folder.id.as_str()) {
            return ThreadFolderMovePlan::Skip(ThreadTreeActionRejection::InvalidDestination);
        }
    }

    ThreadFolderMovePlan::Request(ThreadFolderMoveParams {
        workspace_id: folder.workspace_id.clone(),
        folder_id: folder.id.clone(),
        parent_folder_id: parent_folder_id.map(str::to_owned),
    })
}

pub fn plan_thread_folder_delete(
    folders: &HashMap<String, ThreadFolder>,
    active_workspace_id: Option<&str>,
    folder_id: &str,
) -> ThreadFolderDeletePlan {
    let Some(active_workspace_id) =
        active_workspace_id.filter(|workspace_id| !workspace_id.trim().is_empty())
    else {
        return ThreadFolderDeletePlan::Skip(ThreadTreeActionRejection::MissingWorkspace);
    };
    let Some(folder) = folders.get(folder_id) else {
        return ThreadFolderDeletePlan::Skip(ThreadTreeActionRejection::MissingFolder);
    };
    if folder.workspace_id != active_workspace_id {
        return ThreadFolderDeletePlan::Skip(ThreadTreeActionRejection::ForeignWorkspace);
    }

    ThreadFolderDeletePlan::Request(ThreadFolderDeleteParams {
        workspace_id: folder.workspace_id.clone(),
        folder_id: folder.id.clone(),
    })
}

fn folder_name_exists(
    folders: &HashMap<String, ThreadFolder>,
    workspace_id: &str,
    name: &str,
) -> bool {
    folders
        .values()
        .any(|folder| folder.workspace_id == workspace_id && folder.name == name)
}

pub fn remembered_thread_for_workspace<'a>(
    remembered_threads: &'a HashMap<String, String>,
    workspace_id: &str,
) -> Option<&'a str> {
    remembered_threads.get(workspace_id).map(String::as_str)
}

pub fn remember_thread_for_workspace(
    remembered_threads: &mut HashMap<String, String>,
    workspace_id: &str,
    thread_id: Option<String>,
) {
    match thread_id {
        Some(thread_id) => {
            remembered_threads.insert(workspace_id.to_owned(), thread_id);
        }
        None => {
            remembered_threads.remove(workspace_id);
        }
    }
}

pub fn remember_workspace_thread_state(
    workspace_id: &str,
    active_thread_id: Option<&str>,
    draft_thread_id: Option<&str>,
    pending_thread_id: Option<&str>,
    thread_workspace_matches: impl Fn(&str, &str) -> bool,
) -> WorkspaceThreadState {
    let active_thread_id = active_thread_id
        .filter(|thread_id| thread_workspace_matches(thread_id, workspace_id))
        .map(str::to_owned);
    let draft_thread_id = draft_thread_id
        .filter(|thread_id| thread_workspace_matches(thread_id, workspace_id))
        .or(pending_thread_id)
        .map(str::to_owned);

    WorkspaceThreadState {
        active_thread_id,
        draft_thread_id,
    }
}

pub fn restore_workspace_thread_state(
    workspace_id: &str,
    last_active_thread_id: Option<&str>,
    draft_thread_id: Option<&str>,
    thread_workspace_matches: impl Fn(&str, &str) -> bool,
) -> WorkspaceThreadState {
    let draft_thread_id = draft_thread_id
        .filter(|thread_id| thread_workspace_matches(thread_id, workspace_id))
        .map(str::to_owned);
    let active_thread_id = last_active_thread_id
        .filter(|thread_id| thread_workspace_matches(thread_id, workspace_id))
        .map(str::to_owned)
        .or_else(|| draft_thread_id.clone());

    WorkspaceThreadState {
        active_thread_id,
        draft_thread_id,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        Thread, ThreadAgentsDocStatus, ThreadAgentsDocSummary, ThreadMode, ThreadOriginKind,
        ThreadSidebarVisibility, ThreadStatus,
    };

    fn folder(id: &str) -> ThreadFolder {
        folder_for_workspace(id, "ws_a", None, id)
    }

    fn folder_for_workspace(
        id: &str,
        workspace_id: &str,
        parent_folder_id: Option<&str>,
        name: &str,
    ) -> ThreadFolder {
        ThreadFolder {
            id: id.to_owned(),
            workspace_id: workspace_id.to_owned(),
            parent_folder_id: parent_folder_id.map(str::to_owned),
            name: name.to_owned(),
            created_at: 1,
            updated_at: 2,
        }
    }

    fn placement(thread_id: &str, folder_id: Option<&str>) -> ThreadPlacement {
        ThreadPlacement {
            thread_id: thread_id.to_owned(),
            workspace_id: "ws_a".to_owned(),
            folder_id: folder_id.map(str::to_owned),
        }
    }

    fn placement_for_workspace(
        thread_id: &str,
        workspace_id: &str,
        folder_id: Option<&str>,
    ) -> ThreadPlacement {
        ThreadPlacement {
            thread_id: thread_id.to_owned(),
            workspace_id: workspace_id.to_owned(),
            folder_id: folder_id.map(str::to_owned),
        }
    }

    fn agents_doc_summary_for_workspace(
        workspace_id: &str,
        folder_id: Option<&str>,
        status: ThreadAgentsDocStatus,
    ) -> ThreadAgentsDocSummary {
        ThreadAgentsDocSummary {
            id: "agd_1".to_owned(),
            workspace_id: workspace_id.to_owned(),
            folder_id: folder_id.map(str::to_owned),
            status,
            content_sha256: "sha256:test".to_owned(),
            version: 1,
            char_count: 20,
            updated_at: 1_700_000_000,
        }
    }

    fn thread(thread_id: &str, workspace_id: &str, updated_at: i64) -> Thread {
        Thread {
            workspace_id: workspace_id.to_owned(),
            id: thread_id.to_owned(),
            name: None,
            preview: String::new(),
            mode: ThreadMode::Chat,
            model: "gpt-5.4".to_owned(),
            model_provider: "openai".to_owned(),
            reasoning_effort: None,
            created_at: updated_at,
            updated_at,
            status: ThreadStatus::Idle,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            turns: Vec::new(),
        }
    }

    fn coordinator(thread_id: &str, workspace_id: &str, updated_at: i64) -> ThreadCoordinator {
        ThreadCoordinator::new(thread(thread_id, workspace_id, updated_at))
    }

    #[test]
    fn thread_tree_refresh_queue_batches_until_taken() {
        let mut requested = false;

        assert!(!take_thread_tree_refresh_request(&mut requested));
        queue_thread_tree_refresh(&mut requested);
        queue_thread_tree_refresh(&mut requested);
        assert!(take_thread_tree_refresh_request(&mut requested));
        assert!(!take_thread_tree_refresh_request(&mut requested));
    }

    #[test]
    fn thread_tree_refresh_success_restores_existing_draft_and_requests_start() {
        let response_thread = thread("thread_draft", "ws_a", 10);
        let response_folder = folder("folder_a");
        let response_placement = placement("thread_draft", Some("folder_a"));
        let response_agents_doc =
            agents_doc_summary_for_workspace("ws_a", None, ThreadAgentsDocStatus::Active);

        let reduction = reduce_thread_tree_refresh_success(
            ThreadTreeResponse {
                workspace_id: "ws_a".to_owned(),
                threads: vec![response_thread.clone()],
                folders: vec![response_folder.clone()],
                placements: vec![response_placement.clone()],
                agents_docs: vec![response_agents_doc.clone()],
            },
            ThreadTreeRefreshContext {
                active_thread_id: None,
                existing_draft_thread_id: Some("thread_draft"),
                existing_draft_thread_workspace_id: Some("ws_a"),
                has_known_threads_for_workspace: false,
            },
        );

        assert_eq!(reduction.workspace_id, "ws_a");
        assert_eq!(reduction.threads, vec![response_thread]);
        assert_eq!(reduction.folders, vec![response_folder]);
        assert_eq!(reduction.placements, vec![response_placement]);
        assert_eq!(reduction.agents_docs, vec![response_agents_doc]);
        assert_eq!(
            reduction.set_active_thread_id.as_deref(),
            Some("thread_draft")
        );
        assert_eq!(
            reduction.set_preferred_workspace_id.as_deref(),
            Some("ws_a")
        );
        assert_eq!(
            reduction.ensure_thread_subscription,
            Some(ThreadTreeThreadAction {
                thread_id: "thread_draft".to_owned(),
                workspace_id: "ws_a".to_owned(),
            })
        );
        assert_eq!(
            reduction.ensure_thread_timeline_loaded.as_deref(),
            Some("thread_draft")
        );
        assert!(reduction.request_thread_start_if_needed);
        assert!(reduction.drive_thread_start_queue);
        assert!(reduction.sync_composer_model_selection);
    }

    #[test]
    fn thread_tree_refresh_success_loads_active_thread_timeline_without_starting_thread() {
        let reduction = reduce_thread_tree_refresh_success(
            ThreadTreeResponse {
                workspace_id: "ws_a".to_owned(),
                threads: Vec::new(),
                folders: Vec::new(),
                placements: Vec::new(),
                agents_docs: Vec::new(),
            },
            ThreadTreeRefreshContext {
                active_thread_id: Some("thread_active"),
                existing_draft_thread_id: Some("thread_draft"),
                existing_draft_thread_workspace_id: Some("ws_a"),
                has_known_threads_for_workspace: true,
            },
        );

        assert_eq!(reduction.set_active_thread_id, None);
        assert_eq!(reduction.set_preferred_workspace_id, None);
        assert_eq!(reduction.ensure_thread_subscription, None);
        assert_eq!(
            reduction.ensure_thread_timeline_loaded.as_deref(),
            Some("thread_active")
        );
        assert!(!reduction.request_thread_start_if_needed);
        assert!(!reduction.drive_thread_start_queue);
        assert!(reduction.sync_composer_model_selection);
    }

    #[test]
    fn thread_tree_refresh_failure_starts_thread_only_when_workspace_is_empty_and_inactive() {
        let start = reduce_thread_tree_refresh_failure(ThreadTreeRefreshContext {
            active_thread_id: None,
            existing_draft_thread_id: None,
            existing_draft_thread_workspace_id: None,
            has_known_threads_for_workspace: false,
        });
        assert!(start.request_thread_start_if_needed);
        assert!(start.drive_thread_start_queue);

        let active = reduce_thread_tree_refresh_failure(ThreadTreeRefreshContext {
            active_thread_id: Some("thread_active"),
            existing_draft_thread_id: None,
            existing_draft_thread_workspace_id: None,
            has_known_threads_for_workspace: false,
        });
        assert_eq!(active, ThreadTreeRefreshFailureReduction::default());

        let known_threads = reduce_thread_tree_refresh_failure(ThreadTreeRefreshContext {
            active_thread_id: None,
            existing_draft_thread_id: None,
            existing_draft_thread_workspace_id: None,
            has_known_threads_for_workspace: true,
        });
        assert_eq!(known_threads, ThreadTreeRefreshFailureReduction::default());
    }

    #[test]
    fn snapshot_normalization_indexes_folders_and_prunes_expansion_state() {
        let previous_expanded = HashMap::from([
            ("folder_existing".to_owned(), true),
            ("folder_removed".to_owned(), true),
        ]);

        let normalized = normalize_thread_tree_snapshot(
            vec![folder("folder_existing"), folder("folder_new")],
            vec![placement("thread_a", Some("folder_existing"))],
            &previous_expanded,
        );

        assert!(normalized.folders_by_id.contains_key("folder_existing"));
        assert!(normalized.folders_by_id.contains_key("folder_new"));
        assert_eq!(
            normalized.folder_expanded.get("folder_existing").copied(),
            Some(true)
        );
        assert_eq!(
            normalized.folder_expanded.get("folder_new").copied(),
            Some(false)
        );
        assert!(!normalized.folder_expanded.contains_key("folder_removed"));
        assert_eq!(
            normalized
                .placements_by_thread_id
                .get("thread_a")
                .and_then(|placement| placement.folder_id.as_deref()),
            Some("folder_existing")
        );
    }

    #[test]
    fn folder_expansion_mutators_toggle_missing_and_preserve_explicit_value() {
        let mut expanded_folders = HashMap::from([("folder_existing".to_owned(), true)]);

        assert!(!toggle_thread_folder_expanded(
            &mut expanded_folders,
            "folder_existing"
        ));
        assert_eq!(expanded_folders.get("folder_existing"), Some(&false));
        assert!(toggle_thread_folder_expanded(
            &mut expanded_folders,
            "folder_new"
        ));
        assert_eq!(expanded_folders.get("folder_new"), Some(&true));

        set_thread_folder_expanded(&mut expanded_folders, "folder_existing", true);
        assert_eq!(expanded_folders.get("folder_existing"), Some(&true));
    }

    #[test]
    fn workspace_filters_ignore_other_workspaces() {
        let folders = HashMap::from([
            (
                "folder_a".to_owned(),
                folder_for_workspace("folder_a", "ws_a", None, "Folder A"),
            ),
            (
                "folder_b".to_owned(),
                folder_for_workspace("folder_b", "ws_b", None, "Folder B"),
            ),
        ]);
        let placements = HashMap::from([
            (
                "thread_a".to_owned(),
                placement("thread_a", Some("folder_a")),
            ),
            (
                "thread_b".to_owned(),
                ThreadPlacement {
                    thread_id: "thread_b".to_owned(),
                    workspace_id: "ws_b".to_owned(),
                    folder_id: Some("folder_b".to_owned()),
                },
            ),
        ]);

        assert_eq!(
            thread_folders_for_workspace(&folders, "ws_a")
                .into_iter()
                .map(|folder| folder.id.as_str())
                .collect::<Vec<_>>(),
            vec!["folder_a"]
        );
        assert_eq!(
            thread_placements_for_workspace(&placements, "ws_a")
                .into_iter()
                .map(|placement| placement.thread_id.as_str())
                .collect::<Vec<_>>(),
            vec!["thread_a"]
        );
    }

    #[test]
    fn sidebar_tree_projection_filters_workspace_and_includes_agents_doc_nodes() {
        let folder_a = folder_for_workspace("fld_a", "ws_a", None, "Alpha");
        let folder_b = folder_for_workspace("fld_b", "ws_b", None, "Beta");
        let placement_a = placement_for_workspace("thr_a", "ws_a", Some("fld_a"));
        let placement_b = placement_for_workspace("thr_b", "ws_b", Some("fld_b"));
        let root_agents_doc_a =
            agents_doc_summary_for_workspace("ws_a", None, ThreadAgentsDocStatus::Active);
        let folder_agents_doc_a =
            agents_doc_summary_for_workspace("ws_a", Some("fld_a"), ThreadAgentsDocStatus::Active);
        let root_agents_doc_b =
            agents_doc_summary_for_workspace("ws_b", None, ThreadAgentsDocStatus::Active);

        let model = sidebar_tree_model_from_workspace_data(SidebarTreeSourceData {
            workspace_id: "ws_a",
            folders: vec![&folder_a, &folder_b],
            placements: vec![&placement_a, &placement_b],
            sorted_thread_ids: vec!["thr_a".to_owned(), "thr_orphan".to_owned()],
            agents_doc_summaries: vec![
                &root_agents_doc_a,
                &folder_agents_doc_a,
                &root_agents_doc_b,
            ],
            active_agents_doc_editor_scope: None,
            expanded_folder_ids: HashSet::from(["fld_a".to_owned()]),
        });

        assert_eq!(
            model.visible_node_ids,
            vec![
                SIDEBAR_THREADS_HEADER_NODE_ID.to_owned(),
                sidebar_agents_doc_root_node_id(),
                sidebar_folder_node_id("fld_a"),
                sidebar_agents_doc_folder_node_id("fld_a"),
                sidebar_thread_node_id("thr_a"),
                sidebar_thread_node_id("thr_orphan"),
            ]
        );
        assert!(
            !model
                .visible_node_ids
                .contains(&sidebar_folder_node_id("fld_b"))
        );
        assert!(
            !model
                .visible_node_ids
                .contains(&sidebar_thread_node_id("thr_b"))
        );
    }

    #[test]
    fn sorted_thread_ids_filter_workspace_and_draft_then_order_by_updated_at() {
        let coordinators = HashMap::from([
            (
                "thread_a_old".to_owned(),
                coordinator("thread_a_old", "ws_a", 10),
            ),
            (
                "thread_a_new".to_owned(),
                coordinator("thread_a_new", "ws_a", 30),
            ),
            (
                "thread_a_draft".to_owned(),
                coordinator("thread_a_draft", "ws_a", 40),
            ),
            (
                "thread_b_newer".to_owned(),
                coordinator("thread_b_newer", "ws_b", 100),
            ),
        ]);

        assert_eq!(
            sorted_thread_ids_from_coordinators(
                &coordinators,
                Some("thread_a_draft"),
                Some("ws_a"),
            ),
            vec!["thread_a_new".to_owned(), "thread_a_old".to_owned()]
        );
    }

    #[test]
    fn folder_children_are_collected_within_workspace() {
        let folders = HashMap::from([
            (
                "parent".to_owned(),
                folder_for_workspace("parent", "ws_a", None, "Parent"),
            ),
            (
                "child_a".to_owned(),
                folder_for_workspace("child_a", "ws_a", Some("parent"), "Child A"),
            ),
            (
                "child_b".to_owned(),
                folder_for_workspace("child_b", "ws_b", Some("parent"), "Child B"),
            ),
        ]);
        let placements = HashMap::from([
            ("thread_a".to_owned(), placement("thread_a", Some("parent"))),
            (
                "thread_b".to_owned(),
                ThreadPlacement {
                    thread_id: "thread_b".to_owned(),
                    workspace_id: "ws_b".to_owned(),
                    folder_id: Some("parent".to_owned()),
                },
            ),
        ]);

        assert_eq!(
            child_folder_ids_for_folder(&folders, "ws_a", "parent"),
            vec!["child_a".to_owned()]
        );
        assert_eq!(
            child_thread_ids_for_folder(&placements, "ws_a", "parent"),
            vec!["thread_a".to_owned()]
        );
    }

    #[test]
    fn next_new_folder_name_uses_first_available_workspace_local_suffix() {
        let folders = HashMap::from([
            (
                "folder_1".to_owned(),
                folder_for_workspace("folder_1", "ws_a", None, "New folder"),
            ),
            (
                "folder_2".to_owned(),
                folder_for_workspace("folder_2", "ws_a", None, "New folder 2"),
            ),
            (
                "folder_other".to_owned(),
                folder_for_workspace("folder_other", "ws_b", None, "New folder 3"),
            ),
        ]);

        assert_eq!(
            next_new_folder_name(&folders, "ws_a", "New folder"),
            "New folder 3"
        );
        assert_eq!(
            next_new_folder_name(&folders, "ws_b", "New folder"),
            "New folder"
        );
    }

    #[test]
    fn folder_move_destination_rejects_self_descendant_and_cross_workspace() {
        let folders = HashMap::from([
            (
                "root".to_owned(),
                folder_for_workspace("root", "ws_a", None, "Root"),
            ),
            (
                "child".to_owned(),
                folder_for_workspace("child", "ws_a", Some("root"), "Child"),
            ),
            (
                "grandchild".to_owned(),
                folder_for_workspace("grandchild", "ws_a", Some("child"), "Grandchild"),
            ),
            (
                "other_ws".to_owned(),
                folder_for_workspace("other_ws", "ws_b", None, "Other"),
            ),
        ]);

        assert!(can_move_folder_to(&folders, "grandchild", "root"));
        assert!(!can_move_folder_to(&folders, "child", "child"));
        assert!(!can_move_folder_to(&folders, "child", "grandchild"));
        assert!(!can_move_folder_to(&folders, "child", "other_ws"));
        assert!(!can_move_folder_to(&folders, "missing", "root"));
        assert!(!can_move_folder_to(&folders, "child", "missing"));
    }

    #[test]
    fn sidebar_folder_drop_guard_uses_folder_tree_rules() {
        let folders = HashMap::from([
            (
                "root".to_owned(),
                folder_for_workspace("root", "ws_a", None, "Root"),
            ),
            (
                "child".to_owned(),
                folder_for_workspace("child", "ws_a", Some("root"), "Child"),
            ),
            (
                "sibling".to_owned(),
                folder_for_workspace("sibling", "ws_a", None, "Sibling"),
            ),
            (
                "other_ws".to_owned(),
                folder_for_workspace("other_ws", "ws_b", None, "Other"),
            ),
        ]);

        assert!(can_drop_sidebar_tree_item_on_folder(
            &folders,
            Some("ws_a"),
            SidebarTreeDragItemRef::Thread {
                thread_id: "thread_1"
            },
            "root"
        ));
        assert!(!can_drop_sidebar_tree_item_on_folder(
            &folders,
            Some("ws_a"),
            SidebarTreeDragItemRef::Folder { folder_id: "root" },
            "root"
        ));
        assert!(!can_drop_sidebar_tree_item_on_folder(
            &folders,
            Some("ws_a"),
            SidebarTreeDragItemRef::Folder { folder_id: "root" },
            "child"
        ));
        assert!(can_drop_sidebar_tree_item_on_folder(
            &folders,
            Some("ws_a"),
            SidebarTreeDragItemRef::Folder { folder_id: "root" },
            "sibling"
        ));
        assert!(!can_drop_sidebar_tree_item_on_folder(
            &folders,
            Some("ws_a"),
            SidebarTreeDragItemRef::Thread {
                thread_id: "thread_1"
            },
            "other_ws"
        ));
    }

    #[test]
    fn folder_create_and_rename_plans_build_protocol_requests() {
        let folders = HashMap::from([
            (
                "parent".to_owned(),
                folder_for_workspace("parent", "ws_a", None, "Parent"),
            ),
            (
                "child".to_owned(),
                folder_for_workspace("child", "ws_a", Some("parent"), "Child"),
            ),
            (
                "folder_1".to_owned(),
                folder_for_workspace("folder_1", "ws_a", None, "New folder"),
            ),
        ]);
        let placements =
            HashMap::from([("thread_a".to_owned(), placement("thread_a", Some("parent")))]);

        let create =
            plan_thread_folder_create(&folders, Some("ws_a"), "New folder").expect("create plan");
        assert_eq!(create.workspace_id, "ws_a");
        assert_eq!(create.parent_folder_id, None);
        assert_eq!(create.name, "New folder 2");

        let rename = match plan_thread_folder_rename(
            &folders,
            &placements,
            Some("ws_a"),
            "parent",
            "  Renamed  ",
        ) {
            ThreadFolderRenamePlan::Request(request) => request,
            other => panic!("unexpected rename plan: {other:?}"),
        };
        assert_eq!(rename.create.workspace_id, "ws_a");
        assert_eq!(rename.create.parent_folder_id, None);
        assert_eq!(rename.create.name, "Renamed");
        assert_eq!(rename.old_folder_id, "parent");
        assert_eq!(rename.child_folder_ids, vec!["child".to_owned()]);
        assert_eq!(rename.child_thread_ids, vec!["thread_a".to_owned()]);

        let follow_up = thread_folder_rename_follow_up_params(&rename, "new_parent");
        assert_eq!(follow_up.folder_moves[0].folder_id, "child");
        assert_eq!(
            follow_up.folder_moves[0].parent_folder_id.as_deref(),
            Some("new_parent")
        );
        assert_eq!(follow_up.thread_moves[0].thread_id, "thread_a");
        assert_eq!(
            follow_up.thread_moves[0].folder_id.as_deref(),
            Some("new_parent")
        );
        assert_eq!(follow_up.delete.folder_id, "parent");

        assert_eq!(
            plan_thread_folder_rename(&folders, &placements, Some("ws_b"), "parent", "Renamed"),
            ThreadFolderRenamePlan::Skip(ThreadTreeActionRejection::ForeignWorkspace)
        );
        assert_eq!(
            plan_thread_folder_rename(&folders, &placements, Some("ws_a"), "parent", "Parent"),
            ThreadFolderRenamePlan::Skip(ThreadTreeActionRejection::Unchanged)
        );
    }

    #[test]
    fn thread_tree_action_plans_validate_workspace_and_targets() {
        let folders = HashMap::from([
            (
                "root".to_owned(),
                folder_for_workspace("root", "ws_a", None, "Root"),
            ),
            (
                "child".to_owned(),
                folder_for_workspace("child", "ws_a", Some("root"), "Child"),
            ),
            (
                "other".to_owned(),
                folder_for_workspace("other", "ws_b", None, "Other"),
            ),
        ]);
        let coordinators =
            HashMap::from([("thread_a".to_owned(), coordinator("thread_a", "ws_a", 10))]);

        let rename =
            match plan_thread_rename(&coordinators, Some("ws_a"), "thread_a", "  New title  ") {
                ThreadRenamePlan::Request(params) => params,
                other => panic!("unexpected thread rename plan: {other:?}"),
            };
        assert_eq!(rename.workspace_id, "ws_a");
        assert_eq!(rename.thread_id, "thread_a");
        assert_eq!(rename.name.as_deref(), Some("New title"));

        let thread_move = match plan_thread_move(
            &coordinators,
            &folders,
            Some("ws_a"),
            "thread_a",
            Some("root"),
        ) {
            ThreadMovePlan::Request(params) => params,
            other => panic!("unexpected thread move plan: {other:?}"),
        };
        assert_eq!(thread_move.folder_id.as_deref(), Some("root"));

        let folder_move =
            match plan_thread_folder_move(&folders, Some("ws_a"), "child", Some("root")) {
                ThreadFolderMovePlan::Request(params) => params,
                other => panic!("unexpected folder move plan: {other:?}"),
            };
        assert_eq!(folder_move.folder_id, "child");
        assert_eq!(folder_move.parent_folder_id.as_deref(), Some("root"));

        let delete = match plan_thread_folder_delete(&folders, Some("ws_a"), "child") {
            ThreadFolderDeletePlan::Request(params) => params,
            other => panic!("unexpected folder delete plan: {other:?}"),
        };
        assert_eq!(delete.folder_id, "child");

        assert_eq!(
            plan_thread_move(
                &coordinators,
                &folders,
                Some("ws_a"),
                "thread_a",
                Some("other")
            ),
            ThreadMovePlan::Skip(ThreadTreeActionRejection::ForeignWorkspace)
        );
        assert_eq!(
            plan_thread_folder_move(&folders, Some("ws_a"), "root", Some("child")),
            ThreadFolderMovePlan::Skip(ThreadTreeActionRejection::InvalidDestination)
        );
        assert_eq!(
            plan_thread_rename(&coordinators, Some("ws_a"), "missing", "Name"),
            ThreadRenamePlan::Skip(ThreadTreeActionRejection::MissingThread)
        );
    }

    #[test]
    fn thread_rename_success_reduction_extracts_snapshot_and_workspace_mapping() {
        let mut thread = thread("thread_a", "ws_a", 10);
        thread.name = Some("Renamed".to_owned());

        let reduction = reduce_thread_rename_success(ThreadUpdateResponse {
            thread: thread.clone(),
        });

        assert_eq!(reduction.thread, thread);
        assert_eq!(reduction.thread_id, "thread_a");
        assert_eq!(reduction.workspace_id, "ws_a");
    }

    #[test]
    fn remembered_thread_map_insert_remove_and_lookup_are_workspace_scoped() {
        let mut remembered = HashMap::new();

        remember_thread_for_workspace(&mut remembered, "ws_a", Some("thread_a".to_owned()));
        remember_thread_for_workspace(&mut remembered, "ws_b", Some("thread_b".to_owned()));

        assert_eq!(
            remembered_thread_for_workspace(&remembered, "ws_a"),
            Some("thread_a")
        );
        assert_eq!(
            remembered_thread_for_workspace(&remembered, "ws_b"),
            Some("thread_b")
        );

        remember_thread_for_workspace(&mut remembered, "ws_a", None);
        assert_eq!(remembered_thread_for_workspace(&remembered, "ws_a"), None);
        assert_eq!(
            remembered_thread_for_workspace(&remembered, "ws_b"),
            Some("thread_b")
        );
    }

    #[test]
    fn workspace_thread_state_remembers_only_matching_threads_and_pending_draft() {
        let threads = HashMap::from([("thread_a", "ws_a"), ("draft_b", "ws_b")]);
        let matches_workspace = |thread_id: &str, workspace_id: &str| {
            threads.get(thread_id).copied() == Some(workspace_id)
        };

        let remembered = remember_workspace_thread_state(
            "ws_a",
            Some("thread_a"),
            Some("draft_b"),
            Some("pending_a"),
            matches_workspace,
        );

        assert_eq!(
            remembered,
            WorkspaceThreadState {
                active_thread_id: Some("thread_a".to_owned()),
                draft_thread_id: Some("pending_a".to_owned()),
            }
        );
    }

    #[test]
    fn workspace_thread_state_restores_valid_last_active_or_valid_draft() {
        let threads = HashMap::from([("draft_a", "ws_a"), ("thread_b", "ws_b")]);
        let matches_workspace = |thread_id: &str, workspace_id: &str| {
            threads.get(thread_id).copied() == Some(workspace_id)
        };

        let restored = restore_workspace_thread_state(
            "ws_a",
            Some("thread_missing"),
            Some("draft_a"),
            matches_workspace,
        );

        assert_eq!(
            restored,
            WorkspaceThreadState {
                active_thread_id: Some("draft_a".to_owned()),
                draft_thread_id: Some("draft_a".to_owned()),
            }
        );

        let restored_missing = restore_workspace_thread_state(
            "ws_a",
            Some("thread_b"),
            Some("draft_missing"),
            matches_workspace,
        );

        assert_eq!(
            restored_missing,
            WorkspaceThreadState {
                active_thread_id: None,
                draft_thread_id: None,
            }
        );
    }
}
