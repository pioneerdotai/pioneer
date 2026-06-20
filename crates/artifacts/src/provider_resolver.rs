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
