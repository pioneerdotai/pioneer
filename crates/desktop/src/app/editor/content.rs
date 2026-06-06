use anyhow::Error;
use pioneer_client::agents_doc::content as agents_doc_content;
#[cfg(test)]
use pioneer_protocol::ThreadAgentsDocPayload;
use pioneer_protocol::{
    ThreadAgentsDocGetParams, ThreadAgentsDocGetResponse, ThreadAgentsDocSaveParams,
    ThreadAgentsDocSaveReason,
};

pub(super) use pioneer_client::agents_doc::content::{
    AgentsDocConflictRefreshProjection, AgentsDocLoadProjection,
};

#[cfg(test)]
pub(super) fn agents_doc_initial_buffer(explicit_doc: Option<&ThreadAgentsDocPayload>) -> String {
    agents_doc_content::agents_doc_initial_buffer(explicit_doc)
}

pub(super) fn agents_doc_load_projection(
    response: ThreadAgentsDocGetResponse,
) -> AgentsDocLoadProjection {
    agents_doc_content::agents_doc_load_projection(response)
}

pub(super) fn agents_doc_content_hash(content: &str) -> String {
    agents_doc_content::agents_doc_content_hash(content)
}

pub(super) fn agents_doc_get_params(
    workspace_id: &str,
    folder_id: Option<&str>,
) -> ThreadAgentsDocGetParams {
    agents_doc_content::agents_doc_get_params(workspace_id, folder_id)
}

pub(super) fn agents_doc_save_params(
    workspace_id: &str,
    folder_id: Option<&str>,
    content: &str,
    expected_version: Option<i64>,
    save_reason: ThreadAgentsDocSaveReason,
) -> ThreadAgentsDocSaveParams {
    agents_doc_content::agents_doc_save_params(
        workspace_id,
        folder_id,
        content,
        expected_version,
        save_reason,
    )
}

pub(super) fn agents_doc_save_error_message(error: &Error) -> String {
    let message = format!("{error:#}");
    match agents_doc_content::agents_doc_save_error_kind(message.as_str()) {
        agents_doc_content::AgentsDocSaveErrorKind::VersionConflict => {
            t!("editor.agents_doc.save_conflict").to_string()
        }
        agents_doc_content::AgentsDocSaveErrorKind::Other => message,
    }
}

pub(super) fn agents_doc_is_version_conflict_error(error: &Error) -> bool {
    agents_doc_content::agents_doc_is_version_conflict_error_message(format!("{error:#}").as_str())
}

pub(super) fn agents_doc_conflict_refresh_projection(
    response: ThreadAgentsDocGetResponse,
) -> AgentsDocConflictRefreshProjection {
    agents_doc_content::agents_doc_conflict_refresh_projection(response)
}

pub(super) fn agents_doc_saved_at_now() -> i64 {
    agents_doc_content::agents_doc_saved_at_now()
}
