use pioneer_protocol::{ArtifactKind, ArtifactStatus};
use pioneer_provider::{
    AttachmentArtifactContext, AttachmentDataSource, InputContentType, MessageAttachment,
};

use crate::error::{ArtifactError, ArtifactResult};
use crate::ids::is_safe_file_name;
use crate::mime::{OCTET_STREAM, sanitize_display_name};
use crate::service::ArtifactService;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedProviderArtifact {
    pub artifact_id: String,
    pub version_id: Option<String>,
    pub content_type: InputContentType,
    pub attachment: MessageAttachment,
}

impl ArtifactService {
    /// Own exact immutable bytes through native preparation/provider reads.
    /// No readable-copy lifetime or GC lease is involved.
    pub async fn resolve_owned_provider_attachment(
        &self,
        workspace_id: &str,
        artifact_id: &str,
        version_id: &str,
        max_bytes: usize,
    ) -> ArtifactResult<ResolvedProviderArtifact> {
        if version_id.is_empty() || max_bytes == 0 {
            return Err(ArtifactError::InvalidRequest {
                message: "exact bounded artifact version required".to_owned(),
            });
        }
        let read = self
            .read_bounded_artifact(
                crate::service::ArtifactBoundedReadRequest {
                    workspace_id: workspace_id.to_owned(),
                    artifact_id: artifact_id.to_owned(),
                    version_id: Some(version_id.to_owned()),
                    projection_kind: None,
                    offset: None,
                    max_bytes: Some(max_bytes as u64),
                },
                max_bytes as u64,
            )
            .await
            .map_err(|error| match error {
                ArtifactError::NotFound { .. } => ArtifactError::NotFound {
                    message: "exact native media version is unavailable".to_owned(),
                },
                _ => ArtifactError::InvalidRequest {
                    message: "bounded native media read failed".to_owned(),
                },
            })?;
        if read.truncated
            || read.artifact.version_id.as_deref() != Some(version_id)
            || read.len != read.total_size_bytes
        {
            return Err(ArtifactError::InvalidRequest {
                message: "artifact input exceeds read limit or exact version is unavailable"
                    .to_owned(),
            });
        }
        use sha2::Digest;
        let hash = hex::encode(sha2::Sha256::digest(&read.bytes));
        if hash != read.sha256 || read.artifact.sha256.as_deref() != Some(hash.as_str()) {
            return Err(ArtifactError::InvalidRequest {
                message: "artifact input content hash mismatch".to_owned(),
            });
        }
        let attachment = MessageAttachment {
            mime_type: read
                .artifact
                .mime_type
                .clone()
                .unwrap_or_else(|| OCTET_STREAM.to_owned()),
            name: Some(read.artifact.display_name.clone()),
            size_bytes: Some(read.len),
            sha256: Some(hash),
            source: pioneer_provider::attachments::owned_bytes_source(&read.bytes),
            artifact: Some(AttachmentArtifactContext {
                workspace_id: workspace_id.to_owned(),
                artifact_id: artifact_id.to_owned(),
                artifact_version_id: Some(version_id.to_owned()),
            }),
        };
        Ok(ResolvedProviderArtifact {
            artifact_id: artifact_id.to_owned(),
            version_id: Some(version_id.to_owned()),
            content_type: provider_content_type(read.artifact.kind),
            attachment,
        })
    }

    pub async fn resolve_provider_attachment(
        &self,
        workspace_id: &str,
        artifact_id: &str,
        version_id: Option<&str>,
    ) -> ArtifactResult<ResolvedProviderArtifact> {
        let summary = self
            .get_artifact(workspace_id, artifact_id, version_id)
            .await?;
        if summary.artifact.status != ArtifactStatus::Ready {
            return Err(ArtifactError::InvalidRequest {
                message: format!(
                    "artifact `{artifact_id}` is not ready for provider input: {:?}",
                    summary.artifact.status
                ),
            });
        }

        let blob = self
            .store
            .get_artifact_version_blob(workspace_id, artifact_id, version_id)
            .await?;
        let safe_name = provider_safe_name(summary.artifact.display_name.as_str());
        let path = self
            .blob_store
            .materialize_readable_copy(workspace_id, blob.storage_key.as_str(), safe_name.as_str())
            .await?;
        let mime_type = summary
            .artifact
            .mime_type
            .clone()
            .unwrap_or_else(|| OCTET_STREAM.to_owned());
        let mut attachment =
            MessageAttachment::from_path(path.to_string_lossy().to_string(), mime_type);
        attachment.name = Some(summary.artifact.display_name.clone());
        attachment.size_bytes = summary.artifact.size_bytes;
        attachment.sha256 = summary.artifact.sha256.clone();
        attachment.artifact = Some(AttachmentArtifactContext {
            workspace_id: workspace_id.to_owned(),
            artifact_id: summary.artifact.artifact_id.clone(),
            artifact_version_id: summary.artifact.version_id.clone(),
        });

        debug_assert!(matches!(
            attachment.source,
            AttachmentDataSource::Path { .. }
        ));

        Ok(ResolvedProviderArtifact {
            artifact_id: summary.artifact.artifact_id,
            version_id: summary.artifact.version_id,
            content_type: provider_content_type(summary.artifact.kind),
            attachment,
        })
    }
}

fn provider_safe_name(display_name: &str) -> String {
    let candidate = sanitize_display_name(display_name);
    if is_safe_file_name(candidate.as_str()) {
        candidate
    } else {
        "artifact".to_owned()
    }
}

fn provider_content_type(kind: ArtifactKind) -> InputContentType {
    match kind {
        ArtifactKind::Image | ArtifactKind::GeneratedImage | ArtifactKind::Screenshot => {
            InputContentType::Image
        }
        ArtifactKind::Audio => InputContentType::Audio,
        ArtifactKind::Video => InputContentType::Video,
        _ => InputContentType::File,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_content_type_maps_visual_artifacts_to_image() {
        assert_eq!(
            provider_content_type(ArtifactKind::GeneratedImage),
            InputContentType::Image
        );
        assert_eq!(
            provider_content_type(ArtifactKind::Pdf),
            InputContentType::File
        );
    }
}
