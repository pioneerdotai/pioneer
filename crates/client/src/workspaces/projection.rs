//! Immutable shell-neutral folder and thread list projection.
use crate::threads::tree::thread_should_appear_in_sidebar;
use pioneer_protocol::{
    Thread, ThreadAgentsDocSummary, ThreadFolder, ThreadPlacement, ThreadUnreadSummary,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
const THREAD_TREE_ROOT_FOLDER_KEY: &str = "__root__";
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ThreadTreeSnapshot {
    pub workspace_id: String,
    pub threads_by_id: HashMap<String, Thread>,
    pub unread: Vec<ThreadUnreadSummary>,
    /// Unread messages in each folder and all descendants, including zero totals.
    pub unread_by_folder_id: HashMap<String, u64>,
    pub folders_by_id: HashMap<String, ThreadFolder>,
    pub placements_by_thread_id: HashMap<String, ThreadPlacement>,
    pub child_folder_ids_by_parent_id: HashMap<String, Vec<String>>,
    pub thread_ids_by_folder_id: HashMap<String, Vec<String>>,
    pub agents_doc_summaries_by_folder_key: HashMap<String, ThreadAgentsDocSummary>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ThreadTreeLevel {
    pub folder_id: Option<String>,
    pub folder: Option<ThreadFolder>,
    pub parent_folder_id: Option<String>,
    pub folder_path: Vec<String>,
    pub agents_doc_summary: Option<ThreadAgentsDocSummary>,
    pub folders: Vec<ThreadFolder>,
    pub threads: Vec<Thread>,
}

pub fn thread_tree_snapshot_from_reduction(
    reduction: crate::threads::tree::ThreadTreeRefreshSuccessReduction,
    draft_thread_id: Option<&str>,
) -> ThreadTreeSnapshot {
    let workspace_id = reduction.workspace_id;
    let folders = reduction.folders;
    let placements = reduction.placements;
    let agents_docs = reduction.agents_docs;
    let unread = reduction.unread;
    let mut threads = reduction
        .threads
        .into_iter()
        .filter(|thread| {
            thread.workspace_id == workspace_id
                && thread_should_appear_in_sidebar(thread, draft_thread_id)
        })
        .collect::<Vec<_>>();
    threads.sort_by(|lhs, rhs| {
        rhs.updated_at
            .cmp(&lhs.updated_at)
            .then_with(|| lhs.id.cmp(&rhs.id))
    });

    thread_tree_snapshot_from_parts(
        workspace_id,
        threads,
        unread,
        folders,
        placements,
        agents_docs,
    )
}

pub fn thread_tree_snapshot_from_parts(
    workspace_id: String,
    threads: Vec<Thread>,
    mut unread: Vec<ThreadUnreadSummary>,
    folders: Vec<ThreadFolder>,
    placements: Vec<ThreadPlacement>,
    agents_docs: Vec<ThreadAgentsDocSummary>,
) -> ThreadTreeSnapshot {
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
    unread.retain(|summary| threads_by_id.contains_key(summary.thread_id.as_str()));
    unread.sort_by(|lhs, rhs| lhs.thread_id.cmp(&rhs.thread_id));

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

    let unread_by_folder_id =
        folder_unread_counts(&folders_by_id, &thread_ids_by_folder_id, &unread);

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

    ThreadTreeSnapshot {
        workspace_id,
        threads_by_id,
        unread,
        unread_by_folder_id,
        folders_by_id,
        placements_by_thread_id,
        child_folder_ids_by_parent_id,
        thread_ids_by_folder_id,
        agents_doc_summaries_by_folder_key,
    }
}

// Sum each normalized direct membership once, then contribute that direct sum
// to each ancestor once. No expansion/selection state enters this projection.
// Work is O(threads + occupied folders * depth), bounded by the folder count
// even for malformed cycles; no recursion or per-row tree walk is needed.
fn folder_unread_counts(
    folders: &HashMap<String, ThreadFolder>,
    thread_ids_by_folder_id: &HashMap<String, Vec<String>>,
    unread: &[ThreadUnreadSummary],
) -> HashMap<String, u64> {
    let unread_by_thread_id = unread
        .iter()
        .map(|summary| (summary.thread_id.as_str(), summary.unread_count))
        .collect::<HashMap<_, _>>();
    let mut totals = folders
        .keys()
        .map(|id| (id.clone(), 0_u64))
        .collect::<HashMap<_, _>>();
    for (folder_id, thread_ids) in thread_ids_by_folder_id {
        if !folders.contains_key(folder_id) {
            continue; // Root threads and missing placements have no folder ancestors.
        }
        let direct = thread_ids.iter().fold(0_u64, |sum, id| {
            sum.saturating_add(unread_by_thread_id.get(id.as_str()).copied().unwrap_or(0))
        });
        if direct == 0 {
            continue;
        }
        let mut current = Some(folder_id.as_str());
        let mut visited = HashSet::new();
        while let Some(id) = current {
            let Some(folder) = folders.get(id) else { break };
            if !visited.insert(id) {
                break;
            }
            let total = totals.get_mut(id).expect("known folder");
            *total = total.saturating_add(direct);
            current = folder.parent_folder_id.as_deref();
        }
    }
    totals
}

pub fn thread_tree_level(
    snapshot: &ThreadTreeSnapshot,
    folder_id: Option<String>,
) -> ThreadTreeLevel {
    let folder_id = folder_id.and_then(|folder_id| {
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
    let folder_path = thread_tree_folder_path(&snapshot, folder_id.as_deref());
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

    ThreadTreeLevel {
        folder_id,
        folder,
        parent_folder_id,
        folder_path,
        agents_doc_summary,
        folders,
        threads,
    }
}

fn thread_tree_folder_path(snapshot: &ThreadTreeSnapshot, folder_id: Option<&str>) -> Vec<String> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::threads::tree::ThreadTreeRefreshSuccessReduction;
    use pioneer_protocol::{ThreadMode, ThreadOriginKind, ThreadSidebarVisibility, ThreadStatus};

    fn thread(thread_id: &str, workspace_id: &str, updated_at: i64) -> Thread {
        Thread {
            workspace_id: workspace_id.to_owned(),
            id: thread_id.to_owned(),
            name: None,
            preview: String::new(),
            preview_author: None,
            mode: ThreadMode::Chat,
            model: "gpt-5.5".to_owned(),
            model_provider: "openai".to_owned(),
            reasoning_effort: None,
            created_at: updated_at,
            updated_at,
            status: ThreadStatus::Idle,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            visibility: None,
            turns: Vec::new(),
        }
    }

    #[test]
    fn thread_tree_snapshot_filters_existing_draft_thread() {
        let reduction = ThreadTreeRefreshSuccessReduction {
            workspace_id: "workspace_a".to_owned(),
            threads: vec![
                thread("thread_visible", "workspace_a", 10),
                thread("thread_draft", "workspace_a", 20),
            ],
            unread: Vec::new(),
            folders: Vec::new(),
            placements: Vec::new(),
            agents_docs: Vec::new(),
            set_active_thread_id: None,
            set_preferred_workspace_id: None,
            ensure_thread_subscription: None,
            ensure_thread_timeline_loaded: None,
            request_thread_start_if_needed: false,
            drive_thread_start_queue: false,
            sync_composer_model_selection: false,
        };

        let snapshot = thread_tree_snapshot_from_reduction(reduction, Some("thread_draft"));
        let level = thread_tree_level(&snapshot, None);

        assert!(
            level
                .threads
                .iter()
                .all(|thread| thread.id != "thread_draft")
        );
        assert_eq!(
            level
                .threads
                .iter()
                .map(|thread| thread.id.as_str())
                .collect::<Vec<_>>(),
            vec!["thread_visible"]
        );
    }

    #[test]
    fn thread_tree_snapshot_keeps_unread_only_for_visible_authoritative_threads() {
        let reduction = ThreadTreeRefreshSuccessReduction {
            workspace_id: "workspace_a".to_owned(),
            threads: vec![
                thread("thread_b", "workspace_a", 20),
                thread("thread_a", "workspace_a", 10),
                thread("thread_draft", "workspace_a", 30),
            ],
            unread: vec![
                ThreadUnreadSummary {
                    thread_id: "thread_b".to_owned(),
                    unread_count: 2,
                },
                ThreadUnreadSummary {
                    thread_id: "thread_missing".to_owned(),
                    unread_count: 7,
                },
                ThreadUnreadSummary {
                    thread_id: "thread_a".to_owned(),
                    unread_count: 1,
                },
                ThreadUnreadSummary {
                    thread_id: "thread_draft".to_owned(),
                    unread_count: 9,
                },
            ],
            folders: Vec::new(),
            placements: Vec::new(),
            agents_docs: Vec::new(),
            set_active_thread_id: None,
            set_preferred_workspace_id: None,
            ensure_thread_subscription: None,
            ensure_thread_timeline_loaded: None,
            request_thread_start_if_needed: false,
            drive_thread_start_queue: false,
            sync_composer_model_selection: false,
        };

        let snapshot = thread_tree_snapshot_from_reduction(reduction, Some("thread_draft"));

        assert_eq!(
            snapshot.unread,
            vec![
                ThreadUnreadSummary {
                    thread_id: "thread_a".to_owned(),
                    unread_count: 1,
                },
                ThreadUnreadSummary {
                    thread_id: "thread_b".to_owned(),
                    unread_count: 2,
                },
            ]
        );
    }

    fn folder(id: &str, parent: Option<&str>) -> ThreadFolder {
        ThreadFolder {
            id: id.into(),
            workspace_id: "w".into(),
            parent_folder_id: parent.map(str::to_owned),
            name: id.into(),
            created_at: 1,
            updated_at: 1,
        }
    }

    fn placed_snapshot(
        folders: Vec<ThreadFolder>,
        entries: &[(&str, Option<&str>, u64)],
    ) -> ThreadTreeSnapshot {
        thread_tree_snapshot_from_parts(
            "w".into(),
            entries
                .iter()
                .map(|(id, _, _)| thread(id, "w", 1))
                .collect(),
            entries
                .iter()
                .map(|(id, _, count)| ThreadUnreadSummary {
                    thread_id: (*id).into(),
                    unread_count: *count,
                })
                .collect(),
            folders,
            entries
                .iter()
                .map(|(id, folder, _)| ThreadPlacement {
                    thread_id: (*id).into(),
                    workspace_id: "w".into(),
                    folder_id: folder.map(str::to_owned),
                })
                .collect(),
            vec![],
        )
    }

    #[test]
    fn folder_unread_sums_direct_threads_and_all_branches_once() {
        let snapshot = placed_snapshot(
            vec![
                folder("work", None),
                folder("projects", Some("work")),
                folder("archive", Some("projects")),
                folder("sibling", Some("work")),
                folder("empty", Some("work")),
            ],
            &[
                ("a", Some("work"), 2),
                ("b", Some("projects"), 3),
                ("c", Some("archive"), 4),
                ("d", Some("sibling"), 5),
                ("zero", Some("empty"), 0),
                ("root", None, 90),
                ("missing-folder", Some("missing"), 80),
            ],
        );
        assert_eq!(
            snapshot.unread_by_folder_id,
            HashMap::from([
                ("work".into(), 14),
                ("projects".into(), 7),
                ("archive".into(), 4),
                ("sibling".into(), 5),
                ("empty".into(), 0),
            ])
        );
        assert_eq!(thread_tree_level(&snapshot, None).folders.len(), 1);
        assert_eq!(
            thread_tree_level(&snapshot, Some("projects".into()))
                .folders
                .len(),
            1
        );
        // Selecting/projecting any level does not affect authoritative totals.
        assert_eq!(snapshot.unread_by_folder_id["work"], 14);
        assert!(!snapshot.unread_by_folder_id.contains_key("__root__"));
    }

    #[test]
    fn folder_unread_normalizes_duplicates_missing_placements_and_parents() {
        let mut snapshot = placed_snapshot(
            vec![folder("orphan", Some("missing")), folder("other", None)],
            &[("a", Some("other"), 2), ("a", Some("orphan"), 3)],
        );
        assert_eq!(snapshot.unread_by_folder_id["orphan"], 3);
        assert_eq!(snapshot.unread_by_folder_id["other"], 0);
        // Rebuild after a placement is absent: the thread belongs at root.
        snapshot = thread_tree_snapshot_from_parts(
            "w".into(),
            snapshot.threads_by_id.into_values().collect(),
            snapshot.unread,
            snapshot.folders_by_id.into_values().collect(),
            vec![],
            vec![],
        );
        assert!(
            snapshot
                .unread_by_folder_id
                .values()
                .all(|count| *count == 0)
        );
        assert_eq!(thread_tree_level(&snapshot, None).folders.len(), 2);
    }

    #[test]
    fn folder_unread_bounds_cycles_and_saturates_instead_of_overflowing() {
        let snapshot = placed_snapshot(
            vec![
                folder("a", Some("b")),
                folder("b", Some("a")),
                folder("leaf", Some("a")),
                folder("self", Some("self")),
            ],
            &[
                ("one", Some("a"), 2),
                ("two", Some("b"), 3),
                ("three", Some("leaf"), 4),
                ("max", Some("self"), u64::MAX),
                ("extra", Some("self"), 1),
            ],
        );
        assert_eq!(snapshot.unread_by_folder_id["a"], 9);
        assert_eq!(snapshot.unread_by_folder_id["b"], 9);
        assert_eq!(snapshot.unread_by_folder_id["leaf"], 4);
        assert_eq!(snapshot.unread_by_folder_id["self"], u64::MAX);
        let saturated = placed_snapshot(
            vec![folder("parent", None), folder("child", Some("parent"))],
            &[
                ("max", Some("parent"), u64::MAX),
                ("extra", Some("child"), 1),
            ],
        );
        assert_eq!(saturated.unread_by_folder_id["parent"], u64::MAX);
    }

    #[test]
    fn folder_unread_handles_deep_ancestors_without_recursive_stack_growth() {
        let mut folders = vec![folder("0", None)];
        for i in 1..512 {
            folders.push(folder(&i.to_string(), Some(&(i - 1).to_string())));
        }
        let snapshot = placed_snapshot(folders, &[("deep", Some("511"), 7)]);
        assert_eq!(snapshot.unread_by_folder_id.len(), 512);
        assert!(
            snapshot
                .unread_by_folder_id
                .values()
                .all(|count| *count == 7)
        );
    }
}
