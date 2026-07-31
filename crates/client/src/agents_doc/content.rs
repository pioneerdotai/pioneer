//! Agents.md content helpers.

use pioneer_protocol::{
    ThreadAgentsDocGetParams, ThreadAgentsDocGetResponse, ThreadAgentsDocPayload,
    ThreadAgentsDocResolvedPayload, ThreadAgentsDocSaveParams, ThreadAgentsDocSaveReason,
};
use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AgentsDocSaveErrorKind {
    VersionConflict,
    Other,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentsDocLoadProjection {
    pub explicit_doc: Option<ThreadAgentsDocPayload>,
    pub effective_doc: Option<ThreadAgentsDocResolvedPayload>,
    pub buffer: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentsDocConflictRefreshProjection {
    pub explicit_doc: Option<ThreadAgentsDocPayload>,
    pub effective_doc: Option<ThreadAgentsDocResolvedPayload>,
    pub remote_doc: Option<ThreadAgentsDocPayload>,
}

pub fn agents_doc_initial_buffer(
    explicit_doc: Option<&ThreadAgentsDocPayload>,
    effective_doc: Option<&ThreadAgentsDocResolvedPayload>,
) -> String {
    explicit_doc
        .map(|doc| doc.content.clone())
        .or_else(|| effective_doc.map(|payload| payload.doc.content.clone()))
        .unwrap_or_default()
}

pub fn agents_doc_load_projection(response: ThreadAgentsDocGetResponse) -> AgentsDocLoadProjection {
    let buffer = agents_doc_initial_buffer(response.explicit.as_ref(), response.effective.as_ref());
    AgentsDocLoadProjection {
        explicit_doc: response.explicit,
        effective_doc: response.effective,
        buffer,
    }
}

pub fn agents_doc_normalize_content(content: &str) -> String {
    content.replace("\r\n", "\n").replace('\r', "\n")
}

pub fn agents_doc_content_hash(content: &str) -> String {
    let normalized = agents_doc_normalize_content(content);
    let mut hasher = Sha256::new();
    hasher.update(normalized.as_bytes());
    hex::encode(hasher.finalize())
}

pub fn agents_doc_get_params(
    workspace_id: &str,
    folder_id: Option<&str>,
) -> ThreadAgentsDocGetParams {
    ThreadAgentsDocGetParams {
        workspace_id: workspace_id.to_owned(),
        thread_id: None,
        folder_id: folder_id.map(str::to_owned),
    }
}

pub fn agents_doc_save_params(
    workspace_id: &str,
    folder_id: Option<&str>,
    content: &str,
    expected_version: Option<i64>,
    save_reason: ThreadAgentsDocSaveReason,
) -> ThreadAgentsDocSaveParams {
    ThreadAgentsDocSaveParams {
        workspace_id: workspace_id.to_owned(),
        thread_id: None,
        folder_id: folder_id.map(str::to_owned),
        content: agents_doc_normalize_content(content),
        expected_version,
        save_reason,
    }
}

pub fn agents_doc_save_error_kind(message: &str) -> AgentsDocSaveErrorKind {
    if agents_doc_is_version_conflict_error_message(message) {
        AgentsDocSaveErrorKind::VersionConflict
    } else {
        AgentsDocSaveErrorKind::Other
    }
}

pub fn agents_doc_is_version_conflict_error_message(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("version conflict")
        || (message.contains("version") && message.contains("conflict"))
}

pub fn agents_doc_conflict_refresh_projection(
    response: ThreadAgentsDocGetResponse,
) -> AgentsDocConflictRefreshProjection {
    AgentsDocConflictRefreshProjection {
        remote_doc: response.explicit.clone(),
        explicit_doc: response.explicit,
        effective_doc: response.effective,
    }
}

pub fn agents_doc_saved_at_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        ThreadAgentsDocGetResponse, ThreadAgentsDocPayload, ThreadAgentsDocResolvedPayload,
        ThreadAgentsDocSaveReason, ThreadAgentsDocStatus,
    };

    fn payload(content: &str) -> ThreadAgentsDocPayload {
        ThreadAgentsDocPayload {
            id: "agd_1".to_owned(),
            workspace_id: "ws_1".to_owned(),
            folder_id: None,
            status: ThreadAgentsDocStatus::Active,
            title: "AGENTS.md".to_owned(),
            content: content.to_owned(),
            content_sha256: "sha".to_owned(),
            version: 1,
            created_at: 1_700_000_000,
            updated_at: 1_700_000_000,
        }
    }

    fn get_response(explicit: Option<ThreadAgentsDocPayload>) -> ThreadAgentsDocGetResponse {
        ThreadAgentsDocGetResponse {
            explicit,
            effective: Some(ThreadAgentsDocResolvedPayload {
                doc: payload("effective"),
                source_folder_id: None,
                source_path: Vec::new(),
                inherited: false,
                resolved_for_folder_id: None,
                resolved_at: 1_700_000_000,
            }),
        }
    }

    #[test]
    fn initial_buffer_prefers_explicit_then_effective_content() {
        assert_eq!(
            agents_doc_initial_buffer(Some(&payload("Use cargo.")), None),
            "Use cargo."
        );
        assert_eq!(
            agents_doc_initial_buffer(
                None,
                Some(&ThreadAgentsDocResolvedPayload {
                    doc: payload("Inherited instructions."),
                    source_folder_id: None,
                    source_path: Vec::new(),
                    inherited: true,
                    resolved_for_folder_id: None,
                    resolved_at: 1_700_000_000,
                })
            ),
            "Inherited instructions."
        );
        assert_eq!(agents_doc_initial_buffer(None, None), String::new());
    }

    #[test]
    fn load_projection_preserves_docs_and_initial_buffer() {
        let projection = agents_doc_load_projection(get_response(Some(payload("local"))));

        assert_eq!(projection.buffer, "local");
        assert!(projection.explicit_doc.is_some());
        assert!(projection.effective_doc.is_some());
    }

    #[test]
    fn load_projection_uses_effective_content_without_explicit_doc() {
        let projection = agents_doc_load_projection(get_response(None));

        assert_eq!(projection.buffer, "effective");
        assert!(projection.explicit_doc.is_none());
        assert!(projection.effective_doc.is_some());
    }

    #[test]
    fn content_hash_normalizes_line_endings() {
        assert_eq!(
            agents_doc_normalize_content("line 1\r\nline 2\rline 3"),
            "line 1\nline 2\nline 3"
        );
        assert_eq!(
            agents_doc_content_hash("line 1\r\nline 2"),
            agents_doc_content_hash("line 1\nline 2")
        );
    }

    #[test]
    fn save_params_normalizes_content_and_keeps_expected_version() {
        let get_params = agents_doc_get_params("ws_1", Some("fld_1"));
        assert_eq!(get_params.workspace_id, "ws_1");
        assert_eq!(get_params.folder_id.as_deref(), Some("fld_1"));

        let params = agents_doc_save_params(
            "ws_1",
            Some("fld_1"),
            "line 1\r\nline 2",
            Some(7),
            ThreadAgentsDocSaveReason::Autosave,
        );

        assert_eq!(params.workspace_id, "ws_1");
        assert_eq!(params.folder_id.as_deref(), Some("fld_1"));
        assert_eq!(params.content, "line 1\nline 2");
        assert_eq!(params.expected_version, Some(7));
        assert_eq!(params.save_reason, ThreadAgentsDocSaveReason::Autosave);
    }

    #[test]
    fn save_error_kind_detects_version_conflicts() {
        assert_eq!(
            agents_doc_save_error_kind("version conflict, expected 1, actual 2"),
            AgentsDocSaveErrorKind::VersionConflict
        );
        assert_eq!(
            agents_doc_save_error_kind("gateway unavailable"),
            AgentsDocSaveErrorKind::Other
        );
    }

    #[test]
    fn conflict_refresh_projection_keeps_remote_explicit_doc() {
        let projection =
            agents_doc_conflict_refresh_projection(get_response(Some(payload("remote"))));

        assert_eq!(
            projection
                .remote_doc
                .as_ref()
                .map(|doc| doc.content.as_str()),
            Some("remote")
        );
        assert!(projection.explicit_doc.is_some());
        assert!(projection.effective_doc.is_some());
    }
}
