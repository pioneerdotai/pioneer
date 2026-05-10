use std::collections::BTreeMap;

use pioneer_crud::CrudStore;
use pioneer_protocol::{ArtifactKind, ArtifactProjectionKind, ArtifactProjectionStatus};
use serde_json::json;

use crate::error::ArtifactResult;

const PLAIN_TEXT_EXTRACTOR_VERSION: &str = "plain_text_v1";
const THUMBNAIL_EXTRACTOR_VERSION: &str = "thumbnail_pending_v1";
const MAX_INLINE_TEXT_PROJECTION_BYTES: usize = 256 * 1024;

pub use pioneer_crud::ArtifactProjectionRecord;

pub async fn create_inline_projections(
    store: &CrudStore,
    workspace_id: &str,
    artifact_id: &str,
    artifact_version_id: &str,
    kind: ArtifactKind,
    mime_type: Option<&str>,
    bytes: &[u8],
) -> ArtifactResult<Vec<ArtifactProjectionRecord>> {
    let mut records = Vec::new();
    if supports_thumbnail_projection(kind, mime_type) {
        let mut metadata = BTreeMap::new();
        metadata.insert(
            "extractor_version".to_owned(),
            json!(THUMBNAIL_EXTRACTOR_VERSION),
        );
        metadata.insert(
            "reason".to_owned(),
            json!("thumbnail_generation_not_configured"),
        );
        records.push(
            insert_projection(
                store,
                workspace_id,
                artifact_id,
                artifact_version_id,
                ArtifactProjectionKind::Thumbnail,
                ArtifactProjectionStatus::Pending,
                None,
                metadata,
            )
            .await?,
        );
    }
    if !supports_inline_plain_text_projection(kind, mime_type, bytes.len()) {
        return Ok(records);
    }
    let text = match std::str::from_utf8(bytes) {
        Ok(text) => text.to_owned(),
        Err(error) => {
            let mut metadata = BTreeMap::new();
            metadata.insert(
                "extractor_version".to_owned(),
                json!(PLAIN_TEXT_EXTRACTOR_VERSION),
            );
            metadata.insert("error_class".to_owned(), json!("utf8"));
            metadata.insert("error".to_owned(), json!(error.to_string()));
            let record = insert_projection(
                store,
                workspace_id,
                artifact_id,
                artifact_version_id,
                ArtifactProjectionKind::PlainText,
                ArtifactProjectionStatus::Failed,
                None,
                metadata,
            )
            .await?;
            records.push(record);
            return Ok(records);
        }
    };
    let mut metadata = BTreeMap::new();
    metadata.insert(
        "extractor_version".to_owned(),
        json!(PLAIN_TEXT_EXTRACTOR_VERSION),
    );
    metadata.insert("source_bytes".to_owned(), json!(bytes.len()));
    let record = insert_projection(
        store,
        workspace_id,
        artifact_id,
        artifact_version_id,
        ArtifactProjectionKind::PlainText,
        ArtifactProjectionStatus::Ready,
        Some(text),
        metadata,
    )
    .await?;
    records.push(record);
    Ok(records)
}

pub async fn list_projections(
    store: &CrudStore,
    workspace_id: &str,
    artifact_id: &str,
    artifact_version_id: Option<&str>,
) -> ArtifactResult<Vec<ArtifactProjectionRecord>> {
    Ok(store
        .list_artifact_projections(workspace_id, artifact_id, artifact_version_id)
        .await?)
}

async fn insert_projection(
    store: &CrudStore,
    workspace_id: &str,
    artifact_id: &str,
    artifact_version_id: &str,
    projection_kind: ArtifactProjectionKind,
    status: ArtifactProjectionStatus,
    text_content: Option<String>,
    metadata: BTreeMap<String, serde_json::Value>,
) -> ArtifactResult<ArtifactProjectionRecord> {
    Ok(store
        .replace_artifact_projection(
            workspace_id,
            artifact_id,
            artifact_version_id,
            projection_kind,
            status,
            text_content,
            metadata,
        )
        .await?)
}

pub(crate) fn supports_inline_plain_text_projection(
    kind: ArtifactKind,
    mime_type: Option<&str>,
    size_bytes: usize,
) -> bool {
    size_bytes <= MAX_INLINE_TEXT_PROJECTION_BYTES
        && (matches!(
            kind,
            ArtifactKind::Text | ArtifactKind::Json | ArtifactKind::WorkspaceFile
        ) || mime_type.is_some_and(|mime| {
            mime.starts_with("text/")
                || matches!(
                    mime,
                    "application/json" | "application/x-ndjson" | "text/csv"
                )
        }))
}

pub(crate) fn supports_thumbnail_projection(kind: ArtifactKind, mime_type: Option<&str>) -> bool {
    matches!(
        kind,
        ArtifactKind::Image | ArtifactKind::GeneratedImage | ArtifactKind::Screenshot
    ) || mime_type.is_some_and(|mime| mime.starts_with("image/"))
}
