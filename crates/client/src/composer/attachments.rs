//! Composer attachment state.

use pioneer_protocol::{ArtifactKind, ArtifactRef, ArtifactStatus};
use std::path::{Path, PathBuf};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum ComposerAttachmentKind {
    Image,
    File,
    Audio,
    Video,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ComposerAttachment {
    pub path: String,
    pub file_name: String,
    pub kind: ComposerAttachmentKind,
    pub upload_state: ComposerAttachmentUploadState,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum ComposerAttachmentUploadState {
    Local,
    Uploading,
    Uploaded { artifact: ArtifactRef },
    Failed { error: String },
}

pub fn composer_attachment_from_path(path: &Path) -> Option<ComposerAttachment> {
    let path_value = path.to_string_lossy().trim().to_owned();
    if path_value.is_empty() {
        return None;
    }

    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .map(str::to_owned)
        .unwrap_or_else(|| path_value.clone());

    Some(ComposerAttachment {
        kind: infer_attachment_kind(path_value.as_str()),
        path: path_value,
        file_name,
        upload_state: ComposerAttachmentUploadState::Local,
    })
}

pub fn composer_attachment_from_artifact(artifact: ArtifactRef) -> Option<ComposerAttachment> {
    if artifact.status != ArtifactStatus::Ready {
        return None;
    }

    let file_name = if artifact.display_name.trim().is_empty() {
        artifact.artifact_id.clone()
    } else {
        artifact.display_name.clone()
    };
    let version_suffix = artifact
        .version_id
        .as_ref()
        .map(|version_id| format!("#{version_id}"))
        .unwrap_or_default();

    Some(ComposerAttachment {
        path: format!("artifact://{}{}", artifact.artifact_id, version_suffix),
        file_name,
        kind: composer_attachment_kind_from_artifact_kind(artifact.kind),
        upload_state: ComposerAttachmentUploadState::Uploaded { artifact },
    })
}

pub fn composer_attachment_has_path(attachments: &[ComposerAttachment], path: &str) -> bool {
    attachments.iter().any(|attachment| attachment.path == path)
}

pub fn composer_attachment_has_artifact(
    attachments: &[ComposerAttachment],
    artifact: &ArtifactRef,
) -> bool {
    attachments.iter().any(|attachment| {
        matches!(
            &attachment.upload_state,
            ComposerAttachmentUploadState::Uploaded { artifact: existing }
                if existing.artifact_id == artifact.artifact_id
                    && existing.version_id == artifact.version_id
        )
    })
}

pub fn add_composer_attachment_from_artifact(
    attachments: &mut Vec<ComposerAttachment>,
    artifact: ArtifactRef,
) -> bool {
    if composer_attachment_has_artifact(attachments, &artifact) {
        return false;
    }
    let Some(attachment) = composer_attachment_from_artifact(artifact) else {
        return false;
    };

    attachments.push(attachment);
    true
}

pub fn remove_composer_attachment_at(
    attachments: &mut Vec<ComposerAttachment>,
    index: usize,
) -> bool {
    if index >= attachments.len() {
        return false;
    }
    attachments.remove(index);
    true
}

pub fn append_composer_attachment_paths(
    attachments: &mut Vec<ComposerAttachment>,
    paths: impl IntoIterator<Item = PathBuf>,
) -> bool {
    let mut changed = false;
    for path in paths {
        let Some(attachment) = composer_attachment_from_path(path.as_path()) else {
            continue;
        };
        if composer_attachment_has_path(attachments, attachment.path.as_str()) {
            continue;
        }
        attachments.push(attachment);
        changed = true;
    }
    changed
}

pub fn infer_attachment_kind(path: &str) -> ComposerAttachmentKind {
    let extension = Path::new(path)
        .extension()
        .and_then(|value| value.to_str())
        .map(|value| value.to_ascii_lowercase());

    match extension.as_deref() {
        Some("png") | Some("jpg") | Some("jpeg") | Some("gif") | Some("webp") | Some("bmp")
        | Some("tif") | Some("tiff") | Some("avif") | Some("heic") | Some("heif") | Some("svg") => {
            ComposerAttachmentKind::Image
        }
        Some("mp3") | Some("wav") | Some("m4a") | Some("aac") | Some("ogg") | Some("oga")
        | Some("flac") => ComposerAttachmentKind::Audio,
        Some("mp4") | Some("mov") | Some("webm") | Some("mkv") | Some("avi") | Some("mpeg")
        | Some("mpg") => ComposerAttachmentKind::Video,
        _ => ComposerAttachmentKind::File,
    }
}

pub fn composer_attachment_kind_from_artifact_kind(kind: ArtifactKind) -> ComposerAttachmentKind {
    match kind {
        ArtifactKind::Image | ArtifactKind::GeneratedImage | ArtifactKind::Screenshot => {
            ComposerAttachmentKind::Image
        }
        ArtifactKind::Audio => ComposerAttachmentKind::Audio,
        ArtifactKind::Video => ComposerAttachmentKind::Video,
        _ => ComposerAttachmentKind::File,
    }
}

pub fn uploaded_artifact_from_attachment(attachment: &ComposerAttachment) -> Option<ArtifactRef> {
    match &attachment.upload_state {
        ComposerAttachmentUploadState::Uploaded { artifact } => Some(artifact.clone()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn artifact_ref(
        id: &str,
        version_id: Option<&str>,
        display_name: &str,
        kind: ArtifactKind,
    ) -> ArtifactRef {
        ArtifactRef {
            artifact_id: id.to_owned(),
            version_id: version_id.map(str::to_owned),
            display_name: display_name.to_owned(),
            kind,
            mime_type: Some("text/plain".to_owned()),
            size_bytes: Some(5),
            sha256: Some("a".repeat(64)),
            status: ArtifactStatus::Ready,
            preview: None,
        }
    }

    #[test]
    fn infer_attachment_kind_uses_extension() {
        assert_eq!(
            infer_attachment_kind("/tmp/sample.png"),
            ComposerAttachmentKind::Image
        );
        assert_eq!(
            infer_attachment_kind("/tmp/sample.wav"),
            ComposerAttachmentKind::Audio
        );
        assert_eq!(
            infer_attachment_kind("/tmp/sample.mp4"),
            ComposerAttachmentKind::Video
        );
        assert_eq!(
            infer_attachment_kind("/tmp/sample.bin"),
            ComposerAttachmentKind::File
        );
    }

    #[test]
    fn local_attachment_from_path_trims_and_infers_file_name() {
        let attachment =
            composer_attachment_from_path(Path::new("/tmp/snap.png")).expect("attachment");

        assert_eq!(attachment.path, "/tmp/snap.png");
        assert_eq!(attachment.file_name, "snap.png");
        assert_eq!(attachment.kind, ComposerAttachmentKind::Image);
        assert_eq!(
            attachment.upload_state,
            ComposerAttachmentUploadState::Local
        );
    }

    #[test]
    fn append_paths_deduplicates_by_normalized_path() {
        let mut attachments = Vec::new();

        assert!(append_composer_attachment_paths(
            &mut attachments,
            vec![PathBuf::from("/tmp/a.txt"), PathBuf::from("/tmp/a.txt")]
        ));

        assert_eq!(attachments.len(), 1);
        assert!(!append_composer_attachment_paths(
            &mut attachments,
            vec![PathBuf::from("/tmp/a.txt")]
        ));
    }

    #[test]
    fn remove_attachment_at_reports_bounds() {
        let mut attachments = vec![
            composer_attachment_from_path(Path::new("/tmp/a.txt")).expect("attachment"),
            composer_attachment_from_path(Path::new("/tmp/b.txt")).expect("attachment"),
        ];

        assert!(remove_composer_attachment_at(&mut attachments, 0));
        assert_eq!(attachments.len(), 1);
        assert_eq!(attachments[0].file_name, "b.txt");
        assert!(!remove_composer_attachment_at(&mut attachments, 9));
    }

    #[test]
    fn artifact_attachment_uses_display_name_and_dedupes_by_artifact_version() {
        let artifact = artifact_ref("art_1", Some("v1"), "result.txt", ArtifactKind::File);
        let attachment =
            composer_attachment_from_artifact(artifact.clone()).expect("artifact attachment");

        assert_eq!(attachment.path, "artifact://art_1#v1");
        assert_eq!(attachment.file_name, "result.txt");
        assert_eq!(attachment.kind, ComposerAttachmentKind::File);
        assert!(composer_attachment_has_artifact(&[attachment], &artifact));
    }

    #[test]
    fn add_artifact_attachment_dedupes_and_rejects_non_ready_artifacts() {
        let artifact = artifact_ref("art_1", Some("v1"), "result.txt", ArtifactKind::File);
        let mut pending = artifact.clone();
        pending.status = ArtifactStatus::Pending;
        let mut attachments = Vec::new();

        assert!(add_composer_attachment_from_artifact(
            &mut attachments,
            artifact.clone(),
        ));
        assert_eq!(attachments.len(), 1);
        assert!(!add_composer_attachment_from_artifact(
            &mut attachments,
            artifact,
        ));
        assert_eq!(attachments.len(), 1);
        assert!(!add_composer_attachment_from_artifact(
            &mut attachments,
            pending,
        ));
        assert_eq!(attachments.len(), 1);
    }

    #[test]
    fn artifact_kind_maps_to_attachment_kind() {
        assert_eq!(
            composer_attachment_kind_from_artifact_kind(ArtifactKind::GeneratedImage),
            ComposerAttachmentKind::Image
        );
        assert_eq!(
            composer_attachment_kind_from_artifact_kind(ArtifactKind::Audio),
            ComposerAttachmentKind::Audio
        );
        assert_eq!(
            composer_attachment_kind_from_artifact_kind(ArtifactKind::Json),
            ComposerAttachmentKind::File
        );
    }
}
