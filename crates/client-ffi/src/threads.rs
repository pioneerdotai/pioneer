use pioneer_client::{
    composer::model_selection::ComposerModelSelection,
    rpc::JsonRpcRequestTransport,
    threads::tree::{ThreadTreeRefreshContext, reduce_thread_tree_refresh_success},
    transport::ws::command_sender as ws_commands,
};
use pioneer_protocol::{
    Thread, ThreadAgentsDocSummary, ThreadFolder, ThreadPlacement, ThreadSidebarVisibility,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

const THREAD_TREE_ROOT_FOLDER_KEY: &str = "__root__";

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ThreadTreeRefreshRequest {
    pub workspace_id: String,
    #[serde(default)]
    pub active_thread_id: Option<String>,
    #[serde(default)]
    pub existing_draft_thread_id: Option<String>,
    #[serde(default)]
    pub existing_draft_thread_workspace_id: Option<String>,
    #[serde(default)]
    pub has_known_threads_for_workspace: bool,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientThreadTreeSnapshot {
    pub workspace_id: String,
    pub threads_by_id: HashMap<String, Thread>,
    pub folders_by_id: HashMap<String, ThreadFolder>,
    pub placements_by_thread_id: HashMap<String, ThreadPlacement>,
    pub child_folder_ids_by_parent_id: HashMap<String, Vec<String>>,
    pub thread_ids_by_folder_id: HashMap<String, Vec<String>>,
    pub agents_doc_summaries_by_folder_key: HashMap<String, ThreadAgentsDocSummary>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientThreadTreeQueryData {
    pub snapshot: ClientThreadTreeSnapshot,
    pub composer_model_selection: Option<ComposerModelSelection>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ThreadTreeLevelRequest {
    pub snapshot: ClientThreadTreeSnapshot,
    #[serde(default)]
    pub folder_id: Option<String>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientThreadTreeLevel {
    pub folder_id: Option<String>,
    pub folder: Option<ThreadFolder>,
    pub parent_folder_id: Option<String>,
    pub folder_path: Vec<String>,
    pub agents_doc_summary: Option<ThreadAgentsDocSummary>,
    pub folders: Vec<ThreadFolder>,
    pub threads: Vec<Thread>,
}

pub fn refresh_thread_tree<TTransport>(
    transport: &TTransport,
    request: ThreadTreeRefreshRequest,
) -> anyhow::Result<ClientThreadTreeQueryData>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    let response = ws_commands::thread_tree(
        transport,
        pioneer_client::threads::tree::thread_tree_params(request.workspace_id),
    )?;
    let reduction = reduce_thread_tree_refresh_success(
        response,
        ThreadTreeRefreshContext {
            active_thread_id: request.active_thread_id.as_deref(),
            existing_draft_thread_id: request.existing_draft_thread_id.as_deref(),
            existing_draft_thread_workspace_id: request
                .existing_draft_thread_workspace_id
                .as_deref(),
            has_known_threads_for_workspace: request.has_known_threads_for_workspace,
        },
    );

    Ok(ClientThreadTreeQueryData {
        snapshot: client_thread_tree_snapshot_from_reduction(reduction),
        composer_model_selection: None,
    })
}

pub fn client_thread_tree_snapshot_from_reduction(
    reduction: pioneer_client::threads::tree::ThreadTreeRefreshSuccessReduction,
) -> ClientThreadTreeSnapshot {
    let workspace_id = reduction.workspace_id;
    let folders = reduction.folders;
    let placements = reduction.placements;
    let agents_docs = reduction.agents_docs;
    let mut threads = reduction
        .threads
        .into_iter()
        .filter(|thread| {
            thread.workspace_id == workspace_id
                && thread.sidebar_visibility == ThreadSidebarVisibility::Visible
        })
        .collect::<Vec<_>>();
    threads.sort_by(|lhs, rhs| {
        rhs.updated_at
            .cmp(&lhs.updated_at)
            .then_with(|| lhs.id.cmp(&rhs.id))
    });

    client_thread_tree_snapshot_from_parts(workspace_id, threads, folders, placements, agents_docs)
}

pub fn client_thread_tree_snapshot_from_parts(
    workspace_id: String,
    threads: Vec<Thread>,
    folders: Vec<ThreadFolder>,
    placements: Vec<ThreadPlacement>,
    agents_docs: Vec<ThreadAgentsDocSummary>,
) -> ClientThreadTreeSnapshot {
    let folders_by_id = folders
        .into_iter()
        .filter(|folder| folder.workspace_id == workspace_id)
        .map(|folder| (folder.id.clone(), folder))
        .collect::<HashMap<_, _>>();
    let folder_id_set = folders_by_id.keys().cloned().collect::<HashSet<_>>();

    let mut child_folder_ids_by_parent_id: HashMap<String, Vec<String>> = HashMap::new();
    for folder in folders_by_id.values() {
        let parent_key = thread_tree_folder_key(folder.parent_folder_id.as_deref(), &folder_id_set);
        child_folder_ids_by_parent_id
            .entry(parent_key)
            .or_default()
            .push(folder.id.clone());
    }
    for child_folder_ids in child_folder_ids_by_parent_id.values_mut() {
        child_folder_ids.sort_by(|lhs, rhs| {
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

    let placements_by_thread_id = placements
        .into_iter()
        .filter(|placement| placement.workspace_id == workspace_id)
        .map(|placement| (placement.thread_id.clone(), placement))
        .collect::<HashMap<_, _>>();
    let threads_by_id = threads
        .into_iter()
        .filter(|thread| thread.workspace_id == workspace_id)
        .map(|thread| (thread.id.clone(), thread))
        .collect::<HashMap<_, _>>();

    let mut sorted_thread_ids = threads_by_id.keys().cloned().collect::<Vec<_>>();
    sorted_thread_ids.sort_by(|lhs, rhs| {
        let lhs_updated = threads_by_id
            .get(lhs.as_str())
            .map(|thread| thread.updated_at)
            .unwrap_or_default();
        let rhs_updated = threads_by_id
            .get(rhs.as_str())
            .map(|thread| thread.updated_at)
            .unwrap_or_default();
        rhs_updated.cmp(&lhs_updated).then_with(|| lhs.cmp(rhs))
    });

    let mut thread_ids_by_folder_id: HashMap<String, Vec<String>> = HashMap::new();
    for thread_id in sorted_thread_ids {
        let folder_key = placements_by_thread_id
            .get(thread_id.as_str())
            .and_then(|placement| placement.folder_id.as_deref())
            .filter(|folder_id| folder_id_set.contains(*folder_id))
            .unwrap_or(THREAD_TREE_ROOT_FOLDER_KEY)
            .to_owned();
        thread_ids_by_folder_id
            .entry(folder_key)
            .or_default()
            .push(thread_id);
    }

    let agents_doc_summaries_by_folder_key = agents_docs
        .into_iter()
        .filter(|summary| summary.workspace_id == workspace_id)
        .map(|summary| {
            (
                thread_tree_agents_doc_folder_key(summary.folder_id.as_deref()),
                summary,
            )
        })
        .collect();

    ClientThreadTreeSnapshot {
        workspace_id,
        threads_by_id,
        folders_by_id,
        placements_by_thread_id,
        child_folder_ids_by_parent_id,
        thread_ids_by_folder_id,
        agents_doc_summaries_by_folder_key,
    }
}

pub fn client_thread_tree_level(request: ThreadTreeLevelRequest) -> ClientThreadTreeLevel {
    let snapshot = request.snapshot;
    let folder_id = request.folder_id.and_then(|folder_id| {
        snapshot
            .folders_by_id
            .contains_key(&folder_id)
            .then_some(folder_id)
    });
    let folder_key = thread_tree_agents_doc_folder_key(folder_id.as_deref());
    let folder = folder_id
        .as_deref()
        .and_then(|folder_id| snapshot.folders_by_id.get(folder_id))
        .cloned();
    let parent_folder_id = folder
        .as_ref()
        .and_then(|folder| folder.parent_folder_id.clone())
        .filter(|parent_id| snapshot.folders_by_id.contains_key(parent_id));
    let folder_path = client_thread_tree_folder_path(&snapshot, folder_id.as_deref());
    let folders = snapshot
        .child_folder_ids_by_parent_id
        .get(folder_key.as_str())
        .into_iter()
        .flat_map(|folder_ids| folder_ids.iter())
        .filter_map(|folder_id| snapshot.folders_by_id.get(folder_id))
        .cloned()
        .collect();
    let threads = snapshot
        .thread_ids_by_folder_id
        .get(folder_key.as_str())
        .into_iter()
        .flat_map(|thread_ids| thread_ids.iter())
        .filter_map(|thread_id| snapshot.threads_by_id.get(thread_id))
        .cloned()
        .collect();
    let agents_doc_summary = snapshot
        .agents_doc_summaries_by_folder_key
        .get(folder_key.as_str())
        .cloned();

    ClientThreadTreeLevel {
        folder_id,
        folder,
        parent_folder_id,
        folder_path,
        agents_doc_summary,
        folders,
        threads,
    }
}

fn client_thread_tree_folder_path(
    snapshot: &ClientThreadTreeSnapshot,
    folder_id: Option<&str>,
) -> Vec<String> {
    let mut path = Vec::new();
    let mut current = folder_id;
    let mut visited = HashSet::new();

    while let Some(folder_id) = current {
        if !visited.insert(folder_id.to_owned()) {
            break;
        }
        let Some(folder) = snapshot.folders_by_id.get(folder_id) else {
            break;
        };
        path.push(folder_id.to_owned());
        current = folder
            .parent_folder_id
            .as_deref()
            .filter(|parent_id| snapshot.folders_by_id.contains_key(*parent_id));
    }

    path.reverse();
    path
}

fn thread_tree_folder_key(folder_id: Option<&str>, folder_id_set: &HashSet<String>) -> String {
    folder_id
        .filter(|folder_id| folder_id_set.contains(*folder_id))
        .unwrap_or(THREAD_TREE_ROOT_FOLDER_KEY)
        .to_owned()
}

fn thread_tree_agents_doc_folder_key(folder_id: Option<&str>) -> String {
    folder_id.unwrap_or(THREAD_TREE_ROOT_FOLDER_KEY).to_owned()
}
