//! UI-neutral artifact presentation helpers.

use crate::artifacts::state::ThreadArtifactFilter;
use pioneer_protocol::{
    ArtifactBindingDirection, ArtifactBindingKind, ArtifactCreatedByKind, ArtifactKind,
    ArtifactStatus,
};
use std::hash::{Hash, Hasher};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactPresentationBlockReason {
    CapabilityDenied,
    Disconnected,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ArtifactPresentationPolicy {
    pub can_attach: bool,
    pub can_list: bool,
    pub can_open: bool,
    pub can_download: bool,
    pub can_share: bool,
    pub read_block_reason: Option<ArtifactPresentationBlockReason>,
    pub write_block_reason: Option<ArtifactPresentationBlockReason>,
}

pub fn artifact_presentation_policy(
    can_read_artifacts: bool,
    can_attach_artifacts: bool,
    connected: bool,
) -> ArtifactPresentationPolicy {
    let can_read = connected && can_read_artifacts;
    let can_attach = connected && can_attach_artifacts;
    let reason = |allowed: bool| {
        if !allowed {
            Some(ArtifactPresentationBlockReason::CapabilityDenied)
        } else if !connected {
            Some(ArtifactPresentationBlockReason::Disconnected)
        } else {
            None
        }
    };
    ArtifactPresentationPolicy {
        can_attach,
        can_list: can_read,
        can_open: can_read,
        can_download: can_read,
        can_share: can_read,
        read_block_reason: reason(can_read_artifacts),
        write_block_reason: reason(can_attach_artifacts),
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum ArtifactBindingTargetKind {
    Thread,
    Turn,
    Message,
    Task,
    Tool,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ArtifactBindingTargetPart {
    pub kind: ArtifactBindingTargetKind,
    pub id: String,
}

pub fn thread_artifact_filter_id(filter: ThreadArtifactFilter) -> usize {
    match filter {
        ThreadArtifactFilter::All => 0,
        ThreadArtifactFilter::Uploaded => 1,
        ThreadArtifactFilter::Generated => 2,
        ThreadArtifactFilter::TaskOutput => 3,
        ThreadArtifactFilter::Images => 4,
        ThreadArtifactFilter::Documents => 5,
    }
}

pub fn stable_artifact_row_id(artifact_id: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    artifact_id.hash(&mut hasher);
    hasher.finish()
}

pub fn format_artifact_size_bytes(size_bytes: u64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = 1024.0 * 1024.0;
    if size_bytes < 1024 {
        format!("{size_bytes} B")
    } else if size_bytes < 1024 * 1024 {
        format!("{:.1} KB", size_bytes as f64 / KB)
    } else {
        format!("{:.1} MB", size_bytes as f64 / MB)
    }
}

pub fn artifact_kind_code(kind: ArtifactKind) -> String {
    format!("{kind:?}").to_ascii_lowercase()
}

pub fn artifact_created_by_code(kind: ArtifactCreatedByKind) -> &'static str {
    match kind {
        ArtifactCreatedByKind::User => "user",
        ArtifactCreatedByKind::Agent => "agent",
        ArtifactCreatedByKind::Tool => "tool",
        ArtifactCreatedByKind::Task => "task",
        ArtifactCreatedByKind::System => "system",
        ArtifactCreatedByKind::Import => "import",
        ArtifactCreatedByKind::ExternalAgent => "external_agent",
    }
}

pub fn artifact_status_code(status: ArtifactStatus) -> &'static str {
    match status {
        ArtifactStatus::Ready => "ready",
        ArtifactStatus::Pending => "pending",
        ArtifactStatus::Quarantined => "quarantined",
        ArtifactStatus::Deleted => "deleted",
        ArtifactStatus::MissingExternalSource => "missing_external_source",
        ArtifactStatus::Failed => "failed",
    }
}

pub fn artifact_binding_kind_code(kind: ArtifactBindingKind) -> String {
    format!("{kind:?}").to_ascii_lowercase()
}

pub fn artifact_binding_direction_code(direction: ArtifactBindingDirection) -> String {
    format!("{direction:?}").to_ascii_lowercase()
}

pub fn artifact_binding_target_parts(
    thread_id: Option<&str>,
    turn_id: Option<&str>,
    message_id: Option<&str>,
    task_id: Option<&str>,
    tool_call_id: Option<&str>,
) -> Vec<ArtifactBindingTargetPart> {
    let mut parts = Vec::new();
    if let Some(thread_id) = thread_id {
        parts.push(ArtifactBindingTargetPart {
            kind: ArtifactBindingTargetKind::Thread,
            id: thread_id.to_owned(),
        });
    }
    if let Some(turn_id) = turn_id {
        parts.push(ArtifactBindingTargetPart {
            kind: ArtifactBindingTargetKind::Turn,
            id: turn_id.to_owned(),
        });
    }
    if let Some(message_id) = message_id {
        parts.push(ArtifactBindingTargetPart {
            kind: ArtifactBindingTargetKind::Message,
            id: message_id.to_owned(),
        });
    }
    if let Some(task_id) = task_id {
        parts.push(ArtifactBindingTargetPart {
            kind: ArtifactBindingTargetKind::Task,
            id: task_id.to_owned(),
        });
    }
    if let Some(tool_call_id) = tool_call_id {
        parts.push(ArtifactBindingTargetPart {
            kind: ArtifactBindingTargetKind::Tool,
            id: tool_call_id.to_owned(),
        });
    }
    parts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn artifact_size_formatter_matches_desktop_contract() {
        assert_eq!(format_artifact_size_bytes(12), "12 B");
        assert_eq!(format_artifact_size_bytes(1_536), "1.5 KB");
        assert_eq!(format_artifact_size_bytes(2 * 1024 * 1024), "2.0 MB");
    }

    #[test]
    fn artifact_filter_ids_are_stable() {
        assert_eq!(thread_artifact_filter_id(ThreadArtifactFilter::All), 0);
        assert_eq!(
            thread_artifact_filter_id(ThreadArtifactFilter::TaskOutput),
            3
        );
        assert_eq!(
            thread_artifact_filter_id(ThreadArtifactFilter::Documents),
            5
        );
    }

    #[test]
    fn artifact_actions_follow_exact_server_capabilities_and_connection_state() {
        let allowed = artifact_presentation_policy(true, true, true);
        assert!(allowed.can_list);
        assert!(allowed.can_open);
        assert!(allowed.can_download);
        assert!(allowed.can_share);
        assert!(allowed.can_attach);
        assert_eq!(allowed.read_block_reason, None);
        assert_eq!(allowed.write_block_reason, None);

        let read_only = artifact_presentation_policy(true, false, true);
        assert!(read_only.can_open);
        assert!(!read_only.can_attach);
        assert_eq!(
            read_only.write_block_reason,
            Some(ArtifactPresentationBlockReason::CapabilityDenied)
        );

        let disconnected = artifact_presentation_policy(true, true, false);
        assert!(!disconnected.can_open);
        assert!(!disconnected.can_attach);
        assert_eq!(
            disconnected.read_block_reason,
            Some(ArtifactPresentationBlockReason::Disconnected)
        );
        assert_eq!(
            disconnected.write_block_reason,
            Some(ArtifactPresentationBlockReason::Disconnected)
        );
    }

    #[test]
    fn binding_target_parts_keep_order_and_kind() {
        let parts = artifact_binding_target_parts(
            Some("thread_1"),
            Some("turn_1"),
            None,
            Some("task_1"),
            None,
        );

        assert_eq!(
            parts,
            vec![
                ArtifactBindingTargetPart {
                    kind: ArtifactBindingTargetKind::Thread,
                    id: "thread_1".to_owned(),
                },
                ArtifactBindingTargetPart {
                    kind: ArtifactBindingTargetKind::Turn,
                    id: "turn_1".to_owned(),
                },
                ArtifactBindingTargetPart {
                    kind: ArtifactBindingTargetKind::Task,
                    id: "task_1".to_owned(),
                },
            ]
        );
    }
}
