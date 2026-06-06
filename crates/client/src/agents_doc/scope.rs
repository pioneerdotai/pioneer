//! Agents.md scope helpers.

use pioneer_protocol::{ThreadAgentsDocArchiveParams, ThreadAgentsDocSummary};
use std::collections::HashMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AgentsDocEditorScope {
    Root {
        workspace_id: String,
    },
    Folder {
        workspace_id: String,
        folder_id: String,
    },
}

impl AgentsDocEditorScope {
    pub fn root(workspace_id: impl Into<String>) -> Self {
        Self::Root {
            workspace_id: workspace_id.into(),
        }
    }

    pub fn folder(workspace_id: impl Into<String>, folder_id: impl Into<String>) -> Self {
        Self::Folder {
            workspace_id: workspace_id.into(),
            folder_id: folder_id.into(),
        }
    }

    pub fn folder_id(&self) -> Option<&str> {
        match self {
            Self::Root { .. } => None,
            Self::Folder { folder_id, .. } => Some(folder_id.as_str()),
        }
    }

    pub fn workspace_id(&self) -> &str {
        match self {
            Self::Root { workspace_id } | Self::Folder { workspace_id, .. } => {
                workspace_id.as_str()
            }
        }
    }

    pub fn into_parts(self) -> (String, Option<String>) {
        match self {
            Self::Root { workspace_id } => (workspace_id, None),
            Self::Folder {
                workspace_id,
                folder_id,
            } => (workspace_id, Some(folder_id)),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ThreadAgentsDocSummaryKey {
    Root,
    Folder(String),
}

impl ThreadAgentsDocSummaryKey {
    pub fn from_folder_id(folder_id: Option<&str>) -> Self {
        match folder_id {
            Some(folder_id) => Self::Folder(folder_id.to_owned()),
            None => Self::Root,
        }
    }
}

pub fn thread_agents_doc_summaries_by_scope(
    summaries: Vec<ThreadAgentsDocSummary>,
) -> HashMap<ThreadAgentsDocSummaryKey, ThreadAgentsDocSummary> {
    summaries
        .into_iter()
        .map(|summary| {
            (
                ThreadAgentsDocSummaryKey::from_folder_id(summary.folder_id.as_deref()),
                summary,
            )
        })
        .collect()
}

pub fn thread_agents_doc_summary<'a>(
    summaries: &'a HashMap<ThreadAgentsDocSummaryKey, ThreadAgentsDocSummary>,
    folder_id: Option<&str>,
) -> Option<&'a ThreadAgentsDocSummary> {
    summaries.get(&ThreadAgentsDocSummaryKey::from_folder_id(folder_id))
}

pub fn thread_agents_doc_summary_for_workspace<'a>(
    summaries: &'a HashMap<ThreadAgentsDocSummaryKey, ThreadAgentsDocSummary>,
    folder_id: Option<&str>,
    workspace_id: &str,
) -> Option<&'a ThreadAgentsDocSummary> {
    thread_agents_doc_summary(summaries, folder_id)
        .filter(|summary| summary.workspace_id == workspace_id)
}

pub fn remove_thread_agents_doc_summary(
    summaries: &mut HashMap<ThreadAgentsDocSummaryKey, ThreadAgentsDocSummary>,
    folder_id: Option<&str>,
) -> Option<ThreadAgentsDocSummary> {
    summaries.remove(&ThreadAgentsDocSummaryKey::from_folder_id(folder_id))
}

pub fn agents_doc_archive_params_for_summary(
    summary: &ThreadAgentsDocSummary,
    folder_id: Option<&str>,
) -> ThreadAgentsDocArchiveParams {
    ThreadAgentsDocArchiveParams {
        workspace_id: summary.workspace_id.clone(),
        folder_id: folder_id.map(str::to_owned),
        expected_version: Some(summary.version),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::ThreadAgentsDocStatus;

    fn summary(
        workspace_id: &str,
        folder_id: Option<&str>,
        status: ThreadAgentsDocStatus,
    ) -> ThreadAgentsDocSummary {
        ThreadAgentsDocSummary {
            id: format!("agd_{}", folder_id.unwrap_or("root")),
            workspace_id: workspace_id.to_owned(),
            folder_id: folder_id.map(str::to_owned),
            status,
            content_sha256: "sha".to_owned(),
            version: 1,
            char_count: 8,
            updated_at: 1_700_000_000,
        }
    }

    #[test]
    fn summary_key_handles_root_and_folder_scope() {
        assert_eq!(
            ThreadAgentsDocSummaryKey::from_folder_id(None),
            ThreadAgentsDocSummaryKey::Root
        );
        assert_eq!(
            ThreadAgentsDocSummaryKey::from_folder_id(Some("fld_1")),
            ThreadAgentsDocSummaryKey::Folder("fld_1".to_owned())
        );
    }

    #[test]
    fn editor_scope_exposes_workspace_and_folder_parts() {
        let root = AgentsDocEditorScope::root("ws_a");
        assert_eq!(root.workspace_id(), "ws_a");
        assert_eq!(root.folder_id(), None);
        assert_eq!(root.clone().into_parts(), ("ws_a".to_owned(), None));

        let folder = AgentsDocEditorScope::folder("ws_a", "fld_1");
        assert_eq!(folder.workspace_id(), "ws_a");
        assert_eq!(folder.folder_id(), Some("fld_1"));
        assert_eq!(
            folder.into_parts(),
            ("ws_a".to_owned(), Some("fld_1".to_owned()))
        );
    }

    #[test]
    fn summaries_by_scope_stores_root_and_folder() {
        let summaries = thread_agents_doc_summaries_by_scope(vec![
            summary("ws_a", None, ThreadAgentsDocStatus::Active),
            summary("ws_a", Some("fld_1"), ThreadAgentsDocStatus::Draft),
        ]);

        assert_eq!(
            summaries
                .get(&ThreadAgentsDocSummaryKey::Root)
                .map(|summary| summary.status.clone()),
            Some(ThreadAgentsDocStatus::Active)
        );
        assert_eq!(
            summaries
                .get(&ThreadAgentsDocSummaryKey::Folder("fld_1".to_owned()))
                .map(|summary| summary.status.clone()),
            Some(ThreadAgentsDocStatus::Draft)
        );
    }

    #[test]
    fn summary_lookup_filters_by_workspace() {
        let summaries = thread_agents_doc_summaries_by_scope(vec![
            summary("ws_a", None, ThreadAgentsDocStatus::Active),
            summary("ws_b", Some("fld_1"), ThreadAgentsDocStatus::Draft),
        ]);

        assert_eq!(
            thread_agents_doc_summary_for_workspace(&summaries, None, "ws_a")
                .map(|summary| summary.workspace_id.as_str()),
            Some("ws_a")
        );
        assert!(
            thread_agents_doc_summary_for_workspace(&summaries, Some("fld_1"), "ws_a").is_none()
        );
    }

    #[test]
    fn remove_summary_uses_folder_scope_key() {
        let mut summaries = thread_agents_doc_summaries_by_scope(vec![
            summary("ws_a", None, ThreadAgentsDocStatus::Active),
            summary("ws_a", Some("fld_1"), ThreadAgentsDocStatus::Draft),
        ]);

        let removed = remove_thread_agents_doc_summary(&mut summaries, Some("fld_1"));

        assert_eq!(
            removed.and_then(|summary| summary.folder_id),
            Some("fld_1".to_owned())
        );
        assert!(thread_agents_doc_summary(&summaries, Some("fld_1")).is_none());
        assert!(thread_agents_doc_summary(&summaries, None).is_some());
    }

    #[test]
    fn archive_params_target_selected_scope_and_version() {
        let folder_summary = summary("ws_a", Some("fld_1"), ThreadAgentsDocStatus::Active);
        let folder_params = agents_doc_archive_params_for_summary(&folder_summary, Some("fld_1"));

        assert_eq!(folder_params.workspace_id, "ws_a");
        assert_eq!(folder_params.folder_id.as_deref(), Some("fld_1"));
        assert_eq!(folder_params.expected_version, Some(1));

        let root_summary = summary("ws_a", None, ThreadAgentsDocStatus::Active);
        let root_params = agents_doc_archive_params_for_summary(&root_summary, None);

        assert_eq!(root_params.workspace_id, "ws_a");
        assert_eq!(root_params.folder_id, None);
        assert_eq!(root_params.expected_version, Some(1));
    }
}
