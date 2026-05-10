use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{ArtifactError, ArtifactResult};

static OPERATION_COUNTER: AtomicU64 = AtomicU64::new(1);

pub fn workspace_segment(workspace_id: &str) -> ArtifactResult<String> {
    if workspace_id.is_empty() {
        return Err(ArtifactError::EmptyWorkspaceId);
    }

    if workspace_id
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
    {
        return Ok(workspace_id.to_owned());
    }

    Err(ArtifactError::InvalidWorkspaceId {
        workspace_id: workspace_id.to_owned(),
    })
}

pub fn new_operation_id(prefix: &str) -> String {
    let counter = OPERATION_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("{prefix}-{}-{nanos}-{counter}", std::process::id())
}

pub fn sha256_storage_key(sha256: &str) -> String {
    format!("sha256/{}/{}/{}", &sha256[0..2], &sha256[2..4], sha256)
}

pub fn is_lower_hex_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub fn is_safe_file_name(value: &str) -> bool {
    !value.is_empty()
        && value != "."
        && value != ".."
        && value
            .chars()
            .all(|ch| ch != '/' && ch != '\\' && ch != '\0')
}
