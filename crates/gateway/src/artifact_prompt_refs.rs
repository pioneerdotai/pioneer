use pioneer_crud::ConversationArtifactRef;

pub(crate) const HISTORY_USER_ARTIFACT_REFS_HEADER: &str =
    "Available artifacts from this user message:";
pub(crate) const HISTORY_ASSISTANT_ARTIFACT_REFS_HEADER: &str =
    "Available artifacts from this assistant message:";
pub(crate) const EPISODIC_ARTIFACT_REFS_HEADER_PREFIX: &str = "Available artifacts for ";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HistoryArtifactRefRole {
    User,
    Assistant,
}

pub(crate) fn append_history_artifact_refs(
    text: Option<&str>,
    refs: &[ConversationArtifactRef],
    role: HistoryArtifactRefRole,
) -> Option<String> {
    let header = match role {
        HistoryArtifactRefRole::User => HISTORY_USER_ARTIFACT_REFS_HEADER,
        HistoryArtifactRefRole::Assistant => HISTORY_ASSISTANT_ARTIFACT_REFS_HEADER,
    };
    render_artifact_refs_block(header, text, refs)
}

pub(crate) fn append_episodic_artifact_refs(
    text: &str,
    source_id: &str,
    refs: &[ConversationArtifactRef],
) -> String {
    let source_id = source_id.trim();
    if refs.is_empty() || source_id.is_empty() {
        return text.to_owned();
    }
    let header = format!("{EPISODIC_ARTIFACT_REFS_HEADER_PREFIX}{source_id}:");
    render_artifact_refs_block(header.as_str(), Some(text), refs).unwrap_or_else(|| text.to_owned())
}

fn render_artifact_refs_block(
    header: &str,
    text: Option<&str>,
    refs: &[ConversationArtifactRef],
) -> Option<String> {
    let text = text.unwrap_or_default().trim();
    if text.is_empty() && refs.is_empty() {
        return None;
    }

    let mut output = String::new();
    if !text.is_empty() {
        output.push_str(text);
    }
    if !refs.is_empty() {
        if !output.is_empty() {
            output.push_str("\n\n");
        }
        output.push_str(header);
        for artifact_ref in refs {
            output.push('\n');
            output.push_str(render_artifact_ref_line(artifact_ref).as_str());
        }
    }
    Some(output)
}

fn render_artifact_ref_line(artifact_ref: &ConversationArtifactRef) -> String {
    let mut parts = vec![
        format!("artifactId={}", artifact_ref.artifact_id),
        artifact_ref
            .version_id
            .as_ref()
            .map(|version_id| format!("versionId={version_id}"))
            .unwrap_or_else(|| "versionId=null".to_owned()),
        format!(
            "name=\"{}\"",
            escaped_display_name(&artifact_ref.display_name)
        ),
        format!("kind={}", enum_label(artifact_ref.kind)),
    ];
    if let Some(mime_type) = artifact_ref
        .mime_type
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        parts.push(format!("mime={mime_type}"));
    }
    if let Some(size_bytes) = artifact_ref.size_bytes {
        parts.push(format!("size={}", human_size(size_bytes)));
    }
    if let Some(role) = artifact_ref.role {
        parts.push(format!("role={}", enum_label(role)));
    }
    format!("- {}.", parts.join(", "))
}

fn escaped_display_name(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn enum_label<T>(value: T) -> String
where
    T: serde::Serialize,
{
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(ToOwned::to_owned))
        .unwrap_or_else(|| "unknown".to_owned())
}

fn human_size(size: u64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = 1024.0 * 1024.0;
    const GB: f64 = 1024.0 * 1024.0 * 1024.0;
    let size = size as f64;
    if size >= GB {
        format!("{:.1} GB", size / GB)
    } else if size >= MB {
        format!("{:.1} MB", size / MB)
    } else if size >= KB {
        format!("{:.0} KB", size / KB)
    } else {
        format!("{} B", size as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        ArtifactBindingDirection, ArtifactBindingKind, ArtifactKind, ArtifactRole,
    };

    fn artifact_ref() -> ConversationArtifactRef {
        ConversationArtifactRef {
            artifact_id: "art_car".to_owned(),
            version_id: Some("ver_car".to_owned()),
            display_name: "car.jpg".to_owned(),
            kind: ArtifactKind::Image,
            mime_type: Some("image/jpeg".to_owned()),
            size_bytes: Some(862_208),
            sha256: Some("sha".to_owned()),
            binding_kind: ArtifactBindingKind::UserInput,
            direction: ArtifactBindingDirection::Input,
            role: Some(ArtifactRole::User),
            turn_id: Some("turn_1".to_owned()),
            message_id: Some("msg_1".to_owned()),
            turn_item_id: Some("item_1".to_owned()),
            item_index: Some(0),
        }
    }

    #[test]
    fn history_artifact_refs_are_metadata_only() {
        let rendered = append_history_artifact_refs(
            Some("Что за машина?"),
            &[artifact_ref()],
            HistoryArtifactRefRole::User,
        )
        .expect("rendered");

        assert!(rendered.contains("Available artifacts from this user message:"));
        assert!(rendered.contains("artifactId=art_car"));
        assert!(rendered.contains("kind=image"));
        assert!(!rendered.contains("artifact_read"));
    }

    #[test]
    fn episodic_artifact_refs_name_exact_source_id() {
        let rendered = append_episodic_artifact_refs(
            "- [thread:turn_1/item_1/chunk_0]: Что за машина?",
            "thread:turn_1/item_1/chunk_0",
            &[artifact_ref()],
        );

        assert!(rendered.contains("Available artifacts for thread:turn_1/item_1/chunk_0:"));
    }
}
