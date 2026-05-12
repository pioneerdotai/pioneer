use anyhow::Error;
use pioneer_protocol::ThreadAgentsDocPayload;
use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};

pub(super) fn agents_doc_initial_buffer(explicit_doc: Option<&ThreadAgentsDocPayload>) -> String {
    explicit_doc
        .map(|doc| doc.content.clone())
        .unwrap_or_default()
}

pub(super) fn agents_doc_normalize_content(content: &str) -> String {
    content.replace("\r\n", "\n").replace('\r', "\n")
}

pub(super) fn agents_doc_content_hash(content: &str) -> String {
    let normalized = agents_doc_normalize_content(content);
    let mut hasher = Sha256::new();
    hasher.update(normalized.as_bytes());
    hex::encode(hasher.finalize())
}

pub(super) fn agents_doc_save_error_message(error: &Error) -> String {
    let message = format!("{error:#}");
    let lower = message.to_ascii_lowercase();
    if lower.contains("version") || lower.contains("conflict") {
        t!("editor.agents_doc.save_conflict").to_string()
    } else {
        message
    }
}

pub(super) fn agents_doc_is_version_conflict_error(error: &Error) -> bool {
    let message = format!("{error:#}").to_ascii_lowercase();
    message.contains("version conflict")
        || (message.contains("version") && message.contains("conflict"))
}

pub(super) fn agents_doc_saved_at_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default()
}
