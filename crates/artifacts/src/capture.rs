use std::collections::BTreeMap;
use std::path::PathBuf;

use pioneer_protocol::{
    ArtifactBindingDirection, ArtifactBindingKind, ArtifactCreatedByKind, ArtifactKind,
    ArtifactRole, ArtifactSummary,
};
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::error::{ArtifactError, ArtifactResult};
use crate::mime::{OCTET_STREAM, classify_kind, sanitize_display_name};
use crate::models::{ArtifactBindingTarget, IngestArtifactBytesRequest};
use crate::security::{ArtifactLocalPathPolicy, read_validated_local_file};
use crate::service::ArtifactService;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactCaptureCandidate {
    pub path: PathBuf,
    pub display_name: Option<String>,
    pub mime_type: Option<String>,
    pub kind_hint: Option<ArtifactKind>,
    pub sha256: Option<String>,
    pub size_bytes: Option<u64>,
    pub source: ArtifactCaptureSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactCaptureSource {
    DownloadTool,
    ApplyPatchAddFile,
    GeneratedImage,
    ComputerUseSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactCaptureContext {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub message_id: Option<String>,
    pub turn_item_id: Option<String>,
    pub tool_call_id: Option<String>,
    pub created_by_actor_id: Option<String>,
    pub item_index: Option<i64>,
    pub allowed_roots: Vec<PathBuf>,
    pub max_file_bytes: Option<u64>,
}

impl ArtifactCaptureSource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DownloadTool => "download_tool",
            Self::ApplyPatchAddFile => "apply_patch_add_file",
            Self::GeneratedImage => "generated_image",
            Self::ComputerUseSnapshot => "computer_use_snapshot",
        }
    }

    const fn default_kind(self) -> Option<ArtifactKind> {
        match self {
            Self::ApplyPatchAddFile => Some(ArtifactKind::WorkspaceFile),
            Self::GeneratedImage => Some(ArtifactKind::GeneratedImage),
            Self::ComputerUseSnapshot => Some(ArtifactKind::Screenshot),
            Self::DownloadTool => None,
        }
    }
}

impl ArtifactService {
    pub async fn capture_candidate(
        &self,
        context: ArtifactCaptureContext,
        candidate: ArtifactCaptureCandidate,
    ) -> ArtifactResult<ArtifactSummary> {
        let mut policy = ArtifactLocalPathPolicy::new(context.allowed_roots.clone());
        if let Some(max_file_bytes) = context.max_file_bytes {
            policy.max_file_bytes = max_file_bytes;
        }
        policy.follow_symlinks = false;

        let local_file = read_validated_local_file(candidate.path.as_path(), &policy).await?;
        validate_capture_hints(&candidate, local_file.bytes.as_slice())?;

        let mime_type = candidate
            .mime_type
            .clone()
            .or_else(|| Some(local_file.mime_type.clone()))
            .unwrap_or_else(|| OCTET_STREAM.to_owned());
        let kind = candidate
            .kind_hint
            .or_else(|| candidate.source.default_kind())
            .unwrap_or_else(|| {
                classify_kind(Some(mime_type.as_str()), Some(&local_file.canonical_path))
            });
        let display_name = candidate
            .display_name
            .as_deref()
            .map(sanitize_display_name)
            .unwrap_or_else(|| local_file.display_name.clone());

        let mut metadata = BTreeMap::new();
        metadata.insert("source_kind".to_owned(), json!("tool_capture"));
        metadata.insert(
            "capture_source".to_owned(),
            json!(candidate.source.as_str()),
        );
        metadata.insert(
            "source_path".to_owned(),
            json!(local_file.canonical_path.display().to_string()),
        );
        if let Some(original_file_name) = local_file.original_file_name {
            metadata.insert("original_file_name".to_owned(), json!(original_file_name));
        }
        if let Some(sha256) = candidate.sha256 {
            metadata.insert("candidate_sha256".to_owned(), json!(sha256));
        }
        if let Some(size_bytes) = candidate.size_bytes {
            metadata.insert("candidate_size_bytes".to_owned(), json!(size_bytes));
        }

        self.ingest_bytes(IngestArtifactBytesRequest {
            workspace_id: context.workspace_id.clone(),
            primary_thread_id: Some(context.thread_id.clone()),
            bytes: local_file.bytes,
            display_name,
            kind,
            mime_type: Some(mime_type),
            created_by_kind: ArtifactCreatedByKind::Tool,
            created_by_actor_id: context.created_by_actor_id,
            binding: Some(ArtifactBindingTarget {
                thread_id: Some(context.thread_id),
                turn_id: Some(context.turn_id),
                message_id: context.message_id,
                turn_item_id: context.turn_item_id,
                tool_call_id: context.tool_call_id,
                task_id: None,
                task_run_id: None,
                binding_kind: ArtifactBindingKind::ToolOutput,
                direction: ArtifactBindingDirection::Output,
                role: Some(ArtifactRole::Tool),
                item_index: context.item_index,
            }),
            metadata,
        })
        .await
    }
}

fn validate_capture_hints(
    candidate: &ArtifactCaptureCandidate,
    bytes: &[u8],
) -> ArtifactResult<()> {
    if let Some(expected_size) = candidate.size_bytes
        && expected_size != bytes.len() as u64
    {
        return Err(ArtifactError::LocalPathRejected {
            message: format!(
                "capture size mismatch for {}: expected {}, got {}",
                candidate.path.display(),
                expected_size,
                bytes.len()
            ),
        });
    }
    if let Some(expected_sha256) = candidate.sha256.as_deref()
        && !expected_sha256.trim().is_empty()
    {
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        let actual_sha256 = hex::encode(hasher.finalize());
        if actual_sha256 != expected_sha256 {
            return Err(ArtifactError::LocalPathRejected {
                message: format!("capture sha256 mismatch for {}", candidate.path.display()),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ArtifactService, LocalArtifactBlobStore};
    use migration::{Migrator, MigratorTrait};
    use pioneer_crud::CrudStore;
    use sea_orm::Database;
    use std::sync::Arc;

    #[tokio::test]
    async fn artifact_capture_local_file_creates_tool_output_binding() {
        let temp = tempfile::tempdir().expect("tempdir");
        let source_path = temp.path().join("report.txt");
        let bytes = b"captured artifact";
        tokio::fs::write(source_path.as_path(), bytes)
            .await
            .expect("write source");
        let harness = harness(temp.path().join("runtime")).await;

        let summary = harness
            .capture_candidate(
                capture_context(temp.path()),
                ArtifactCaptureCandidate {
                    path: source_path,
                    display_name: Some("report.txt".to_owned()),
                    mime_type: Some("text/plain".to_owned()),
                    kind_hint: None,
                    sha256: Some(sha256(bytes)),
                    size_bytes: Some(bytes.len() as u64),
                    source: ArtifactCaptureSource::DownloadTool,
                },
            )
            .await
            .expect("capture");

        assert_eq!(summary.workspace_id, "ws_capture");
        assert_eq!(summary.artifact.kind, ArtifactKind::Text);
        assert_eq!(summary.bindings.len(), 1);
        assert_eq!(
            summary.bindings[0].thread_id.as_deref(),
            Some("thr_capture")
        );
        assert_eq!(summary.bindings[0].turn_id.as_deref(), Some("turn_capture"));
        assert_eq!(
            summary.bindings[0].tool_call_id.as_deref(),
            Some("call_capture")
        );
        assert_eq!(
            summary.bindings[0].binding_kind,
            ArtifactBindingKind::ToolOutput
        );

        let page = harness
            .list_thread_artifacts(
                "ws_capture",
                "thr_capture",
                crate::ArtifactListFilter::default(),
            )
            .await
            .expect("list");
        assert_eq!(page.items.len(), 1);
        assert_eq!(
            page.items[0].artifact.artifact_id,
            summary.artifact.artifact_id
        );
    }

    #[tokio::test]
    async fn artifact_capture_rejects_oversized_file() {
        let temp = tempfile::tempdir().expect("tempdir");
        let source_path = temp.path().join("large.bin");
        tokio::fs::write(source_path.as_path(), b"abcdef")
            .await
            .expect("write source");
        let harness = harness(temp.path().join("runtime")).await;
        let mut context = capture_context(temp.path());
        context.max_file_bytes = Some(3);

        let error = harness
            .capture_candidate(
                context,
                ArtifactCaptureCandidate {
                    path: source_path,
                    display_name: Some("large.bin".to_owned()),
                    mime_type: None,
                    kind_hint: None,
                    sha256: None,
                    size_bytes: None,
                    source: ArtifactCaptureSource::DownloadTool,
                },
            )
            .await
            .expect_err("oversized capture must fail");

        assert!(matches!(error, ArtifactError::LocalPathRejected { .. }));
    }

    async fn harness(runtime_home: PathBuf) -> ArtifactService {
        let db = Database::connect("sqlite::memory:")
            .await
            .expect("connect sqlite");
        Migrator::up(&db, None).await.expect("migrate");
        ArtifactService::new(
            Arc::new(CrudStore::new(db)),
            Arc::new(LocalArtifactBlobStore::new(runtime_home)),
        )
    }

    fn capture_context(root: &std::path::Path) -> ArtifactCaptureContext {
        ArtifactCaptureContext {
            workspace_id: "ws_capture".to_owned(),
            thread_id: "thr_capture".to_owned(),
            turn_id: "turn_capture".to_owned(),
            message_id: None,
            turn_item_id: Some("call_capture".to_owned()),
            tool_call_id: Some("call_capture".to_owned()),
            created_by_actor_id: Some("download_url".to_owned()),
            item_index: Some(0),
            allowed_roots: vec![root.to_path_buf()],
            max_file_bytes: None,
        }
    }

    fn sha256(bytes: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        hex::encode(hasher.finalize())
    }
}
