use std::{collections::BTreeMap, io::Cursor};

use image::{DynamicImage, ImageFormat, imageops::FilterType};
use pioneer_crud::{CrudStore, NewArtifactBlobRecord};
use pioneer_protocol::{ArtifactKind, ArtifactProjectionKind, ArtifactProjectionStatus};
use serde_json::json;

use crate::blob_store::{ArtifactBlobInput, ArtifactBlobStore};
use crate::error::ArtifactResult;

const PLAIN_TEXT_EXTRACTOR_VERSION: &str = "plain_text_v1";
const THUMBNAIL_EXTRACTOR_VERSION: &str = "thumbnail_png_v1";
const THUMBNAIL_MIME_TYPE: &str = "image/png";
const THUMBNAIL_MAX_EDGE_PX: u32 = 320;
const MAX_INLINE_TEXT_PROJECTION_BYTES: usize = 256 * 1024;
pub const MAX_THUMBNAIL_SOURCE_BYTES: u64 = 64 * 1024 * 1024;

pub use pioneer_crud::ArtifactProjectionRecord;

pub async fn create_inline_projections(
    store: &CrudStore,
    blob_store: &dyn ArtifactBlobStore,
    workspace_id: &str,
    artifact_id: &str,
    artifact_version_id: &str,
    kind: ArtifactKind,
    mime_type: Option<&str>,
    bytes: &[u8],
) -> ArtifactResult<Vec<ArtifactProjectionRecord>> {
    let mut records = Vec::new();
    if supports_thumbnail_projection(kind, mime_type) && !bytes.is_empty() {
        records.push(
            create_thumbnail_projection(
                store,
                blob_store,
                workspace_id,
                artifact_id,
                artifact_version_id,
                bytes,
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
        None,
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
    blob_id: Option<String>,
    metadata: BTreeMap<String, serde_json::Value>,
) -> ArtifactResult<ArtifactProjectionRecord> {
    Ok(store
        .replace_artifact_projection_with_blob(
            workspace_id,
            artifact_id,
            artifact_version_id,
            projection_kind,
            status,
            text_content,
            blob_id,
            metadata,
        )
        .await?)
}

async fn create_thumbnail_projection(
    store: &CrudStore,
    blob_store: &dyn ArtifactBlobStore,
    workspace_id: &str,
    artifact_id: &str,
    artifact_version_id: &str,
    bytes: &[u8],
) -> ArtifactResult<ArtifactProjectionRecord> {
    let thumbnail = match build_thumbnail_png(bytes) {
        Ok(thumbnail) => thumbnail,
        Err(error) => {
            let mut metadata = thumbnail_metadata_base(bytes.len());
            metadata.insert("error_class".to_owned(), json!("decode_or_encode"));
            metadata.insert("error".to_owned(), json!(error));
            return insert_projection(
                store,
                workspace_id,
                artifact_id,
                artifact_version_id,
                ArtifactProjectionKind::Thumbnail,
                ArtifactProjectionStatus::Failed,
                None,
                None,
                metadata,
            )
            .await;
        }
    };

    let stored_blob = blob_store
        .put_bytes(workspace_id, ArtifactBlobInput::Bytes(thumbnail.bytes))
        .await?;
    let mut blob_metadata = BTreeMap::new();
    blob_metadata.insert("source_artifact_id".to_owned(), json!(artifact_id));
    blob_metadata.insert(
        "source_artifact_version_id".to_owned(),
        json!(artifact_version_id),
    );
    blob_metadata.insert("projection_kind".to_owned(), json!("thumbnail"));
    blob_metadata.insert(
        "extractor_version".to_owned(),
        json!(THUMBNAIL_EXTRACTOR_VERSION),
    );
    let blob = store
        .find_or_create_artifact_blob(NewArtifactBlobRecord {
            workspace_id: workspace_id.to_owned(),
            sha256: stored_blob.sha256,
            size_bytes: stored_blob.size_bytes,
            mime_type: Some(THUMBNAIL_MIME_TYPE.to_owned()),
            storage_backend: stored_blob.storage_backend,
            storage_key: stored_blob.storage_key,
            metadata: blob_metadata,
        })
        .await?;

    let mut metadata = thumbnail_metadata_base(bytes.len());
    metadata.insert("source_width_px".to_owned(), json!(thumbnail.source_width));
    metadata.insert(
        "source_height_px".to_owned(),
        json!(thumbnail.source_height),
    );
    metadata.insert("width_px".to_owned(), json!(thumbnail.width));
    metadata.insert("height_px".to_owned(), json!(thumbnail.height));
    metadata.insert("mime_type".to_owned(), json!(THUMBNAIL_MIME_TYPE));

    insert_projection(
        store,
        workspace_id,
        artifact_id,
        artifact_version_id,
        ArtifactProjectionKind::Thumbnail,
        ArtifactProjectionStatus::Ready,
        None,
        Some(blob.id),
        metadata,
    )
    .await
}

struct ThumbnailProjection {
    bytes: Vec<u8>,
    source_width: u32,
    source_height: u32,
    width: u32,
    height: u32,
}

fn build_thumbnail_png(bytes: &[u8]) -> Result<ThumbnailProjection, String> {
    let image = image::load_from_memory(bytes)
        .map_err(|error| format!("failed to decode image for thumbnail: {error}"))?;
    let source_width = image.width();
    let source_height = image.height();
    let thumbnail = resize_thumbnail(image);
    let width = thumbnail.width();
    let height = thumbnail.height();
    let mut output = Cursor::new(Vec::new());
    thumbnail
        .write_to(&mut output, ImageFormat::Png)
        .map_err(|error| format!("failed to encode thumbnail PNG: {error}"))?;

    Ok(ThumbnailProjection {
        bytes: output.into_inner(),
        source_width,
        source_height,
        width,
        height,
    })
}

fn resize_thumbnail(image: DynamicImage) -> DynamicImage {
    if image.width() <= THUMBNAIL_MAX_EDGE_PX && image.height() <= THUMBNAIL_MAX_EDGE_PX {
        return image;
    }
    image.resize(
        THUMBNAIL_MAX_EDGE_PX,
        THUMBNAIL_MAX_EDGE_PX,
        FilterType::Triangle,
    )
}

fn thumbnail_metadata_base(source_bytes: usize) -> BTreeMap<String, serde_json::Value> {
    let mut metadata = BTreeMap::new();
    metadata.insert(
        "extractor_version".to_owned(),
        json!(THUMBNAIL_EXTRACTOR_VERSION),
    );
    metadata.insert("source_bytes".to_owned(), json!(source_bytes));
    metadata
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

pub fn supports_thumbnail_projection(kind: ArtifactKind, mime_type: Option<&str>) -> bool {
    matches!(
        kind,
        ArtifactKind::Image | ArtifactKind::GeneratedImage | ArtifactKind::Screenshot
    ) || mime_type.is_some_and(|mime| mime.starts_with("image/"))
}
