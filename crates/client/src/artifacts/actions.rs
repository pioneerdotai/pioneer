//! Artifact file action state and helpers.

use crate::artifacts::download::{ArtifactDownloadRequest, ArtifactDownloadResult};
use crate::platform::{ArtifactFileOpener, ClientPath};
use anyhow::{Context as _, Result, anyhow, bail};
use pioneer_protocol::{ArtifactRef, ArtifactStatus, ArtifactSummary};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    fs,
    io::Read,
    path::{Component, Path, PathBuf},
};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
pub struct ArtifactVersionKey {
    pub artifact_id: String,
    pub version_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactLocalFile {
    pub path: PathBuf,
    pub sha256: String,
    pub size_bytes: Option<u64>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum ArtifactActionStatus {
    Queued,
    Downloading,
    Verifying,
    Opening,
    Revealing,
    Failed(String),
}

impl ArtifactActionStatus {
    pub fn is_in_progress(&self) -> bool {
        !matches!(self, Self::Failed(_))
    }
}

#[derive(Clone, Debug, Default)]
pub struct ThreadArtifactActionState {
    status_by_artifact: HashMap<ArtifactVersionKey, ArtifactActionStatus>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactCachedDownload {
    pub local_path: PathBuf,
    pub size_bytes: u64,
    pub sha256: String,
}

impl ArtifactCachedDownload {
    pub fn new(local_path: PathBuf, size_bytes: u64, sha256: String) -> Self {
        Self {
            local_path,
            size_bytes,
            sha256,
        }
    }
}

pub trait ArtifactCachedDownloadClient {
    fn download_artifact_to_cache(
        &self,
        request: ArtifactDownloadRequest,
    ) -> Result<ArtifactDownloadResult>;
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum ArtifactFileActionBlockReason {
    NotReady,
    ActionInProgress,
    NotConnected,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum ArtifactDownloadRequestPlanError {
    MissingGatewayProfile,
    MissingWorkspaceId,
}

pub fn artifact_version_key(artifact: &ArtifactRef) -> ArtifactVersionKey {
    ArtifactVersionKey {
        artifact_id: artifact.artifact_id.clone(),
        version_id: artifact.version_id.clone(),
    }
}

pub fn artifact_download_block_reason(
    summary: &ArtifactSummary,
    action_in_progress: bool,
    connected: bool,
) -> Option<ArtifactFileActionBlockReason> {
    if summary.artifact.status != ArtifactStatus::Ready {
        return Some(ArtifactFileActionBlockReason::NotReady);
    }
    if action_in_progress {
        return Some(ArtifactFileActionBlockReason::ActionInProgress);
    }
    if !connected {
        return Some(ArtifactFileActionBlockReason::NotConnected);
    }
    None
}

pub fn plan_artifact_download_request(
    gateway_profile_id: Option<String>,
    summary: &ArtifactSummary,
) -> std::result::Result<ArtifactDownloadRequest, ArtifactDownloadRequestPlanError> {
    let gateway_profile_id = gateway_profile_id
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .ok_or(ArtifactDownloadRequestPlanError::MissingGatewayProfile)?;
    if summary.workspace_id.trim().is_empty() {
        return Err(ArtifactDownloadRequestPlanError::MissingWorkspaceId);
    }

    Ok(ArtifactDownloadRequest {
        gateway_profile_id,
        workspace_id: summary.workspace_id.clone(),
        artifact_id: summary.artifact.artifact_id.clone(),
        version_id: summary.artifact.version_id.clone(),
    })
}

pub fn local_file_from_cached_download(download: &ArtifactCachedDownload) -> ArtifactLocalFile {
    ArtifactLocalFile {
        path: download.local_path.clone(),
        sha256: download.sha256.clone(),
        size_bytes: Some(download.size_bytes),
    }
}

pub fn cached_download_from_download_result(
    result: &ArtifactDownloadResult,
) -> ArtifactCachedDownload {
    ArtifactCachedDownload::new(
        result.local_path.as_path().to_owned(),
        result.size_bytes,
        result.sha256.clone(),
    )
}

pub fn local_file_from_download_result(result: &ArtifactDownloadResult) -> ArtifactLocalFile {
    local_file_from_cached_download(&cached_download_from_download_result(result))
}

pub fn ensure_artifact_local_copy_for_open<C: ArtifactCachedDownloadClient>(
    client: &C,
    request: ArtifactDownloadRequest,
    artifact: &ArtifactRef,
    existing: Option<&ArtifactLocalFile>,
) -> Result<ArtifactLocalFile> {
    if let Some(existing) = existing
        && existing_local_file_is_verified(existing, artifact)?
    {
        return Ok(existing.clone());
    }

    let result = client.download_artifact_to_cache(request)?;
    let download = cached_download_from_download_result(&result);
    verify_cached_download(&download)?;
    Ok(local_file_from_cached_download(&download))
}

pub fn copy_download_result_to_destination(
    result: &ArtifactDownloadResult,
    display_name: &str,
    destination_dir: &Path,
) -> Result<ArtifactLocalFile> {
    copy_cached_download_to_destination(
        &cached_download_from_download_result(result),
        display_name,
        destination_dir,
    )
}

impl ThreadArtifactActionState {
    pub fn status(&self, artifact: &ArtifactRef) -> Option<&ArtifactActionStatus> {
        self.status_by_artifact.get(&artifact_version_key(artifact))
    }

    pub fn set_status(&mut self, artifact: &ArtifactRef, status: ArtifactActionStatus) {
        self.status_by_artifact
            .insert(artifact_version_key(artifact), status);
    }

    pub fn clear_status(&mut self, artifact: &ArtifactRef) {
        self.status_by_artifact
            .remove(&artifact_version_key(artifact));
    }

    pub fn in_progress(&self, artifact: &ArtifactRef) -> bool {
        self.status(artifact)
            .is_some_and(ArtifactActionStatus::is_in_progress)
    }
}

pub fn copy_cached_download_to_destination(
    download: &ArtifactCachedDownload,
    display_name: &str,
    destination_dir: &Path,
) -> Result<ArtifactLocalFile> {
    if !destination_dir.is_dir() {
        bail!(
            "artifact destination `{}` is not a directory",
            destination_dir.display()
        );
    }
    verify_cached_download(download)?;

    let final_path = unique_destination_path(destination_dir, display_name)?;
    let part_path = unique_part_path(&final_path)?;
    let copy_result = (|| -> Result<()> {
        if part_path.exists() {
            fs::remove_file(part_path.as_path()).with_context(|| {
                format!(
                    "failed to remove stale artifact download part `{}`",
                    part_path.display()
                )
            })?;
        }
        fs::copy(download.local_path.as_path(), part_path.as_path()).with_context(|| {
            format!(
                "failed to copy artifact download `{}` to `{}`",
                download.local_path.display(),
                part_path.display()
            )
        })?;
        verify_file(
            part_path.as_path(),
            download.sha256.as_str(),
            Some(download.size_bytes),
        )?;
        fs::rename(part_path.as_path(), final_path.as_path()).with_context(|| {
            format!(
                "failed to finalize artifact download `{}`",
                final_path.display()
            )
        })?;
        Ok(())
    })();

    if copy_result.is_err() {
        let _ = fs::remove_file(part_path.as_path());
    }
    copy_result?;

    Ok(ArtifactLocalFile {
        path: final_path,
        sha256: download.sha256.clone(),
        size_bytes: Some(download.size_bytes),
    })
}

pub fn open_artifact_local_file<O: ArtifactFileOpener>(opener: &O, path: &Path) -> Result<()> {
    if !path.is_file() {
        bail!("artifact local file `{}` does not exist", path.display());
    }
    opener.open_file(&ClientPath::new(path.to_owned()))?;
    Ok(())
}

pub fn reveal_artifact_local_file<O: ArtifactFileOpener>(opener: &O, path: &Path) -> Result<()> {
    if !path.is_file() {
        bail!("artifact local file `{}` does not exist", path.display());
    }
    opener.reveal_file(&ClientPath::new(path.to_owned()))?;
    Ok(())
}

pub fn existing_local_file_is_verified(
    local_file: &ArtifactLocalFile,
    artifact: &ArtifactRef,
) -> Result<bool> {
    if !local_file.path.is_file() {
        return Ok(false);
    }
    let expected_sha = artifact
        .sha256
        .as_deref()
        .unwrap_or(local_file.sha256.as_str());
    if expected_sha.trim().is_empty() {
        return Ok(false);
    }
    if let Some(expected_size) = artifact.size_bytes.or(local_file.size_bytes) {
        let actual_size = fs::metadata(local_file.path.as_path())
            .with_context(|| {
                format!(
                    "failed to stat artifact local file `{}`",
                    local_file.path.display()
                )
            })?
            .len();
        if actual_size != expected_size {
            return Ok(false);
        }
    }
    Ok(sha256_file(local_file.path.as_path())? == expected_sha)
}

pub fn sanitized_artifact_file_name(display_name: &str) -> String {
    let fallback = "artifact";
    let candidate = Path::new(display_name)
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or(display_name);
    let mut sanitized = String::with_capacity(candidate.len().max(fallback.len()));
    for ch in candidate.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '_') {
            sanitized.push(ch);
        } else {
            sanitized.push('_');
        }
    }
    let trimmed = sanitized
        .trim_matches([' ', '\t', '\r', '\n'])
        .trim_matches('.');
    if trimmed.is_empty() {
        fallback.to_owned()
    } else {
        trimmed.to_owned()
    }
}

pub fn unique_destination_path(destination_dir: &Path, display_name: &str) -> Result<PathBuf> {
    reject_path_traversal(destination_dir)?;
    let safe_name = sanitized_artifact_file_name(display_name);
    let initial = destination_dir.join(safe_name.as_str());
    if !initial.exists() {
        return Ok(initial);
    }

    let safe_path = Path::new(safe_name.as_str());
    let stem = safe_path
        .file_stem()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("artifact");
    let extension = safe_path.extension().and_then(|value| value.to_str());
    for index in 1..10_000 {
        let candidate_name = if let Some(extension) = extension {
            format!("{stem} ({index}).{extension}")
        } else {
            format!("{stem} ({index})")
        };
        let candidate = destination_dir.join(candidate_name);
        if !candidate.exists() {
            return Ok(candidate);
        }
    }

    bail!("failed to choose a unique artifact destination file name")
}

pub fn verify_cached_download(download: &ArtifactCachedDownload) -> Result<()> {
    verify_file(
        download.local_path.as_path(),
        download.sha256.as_str(),
        Some(download.size_bytes),
    )
}

pub fn verify_file(path: &Path, expected_sha256: &str, expected_size: Option<u64>) -> Result<()> {
    if !path.is_file() {
        bail!("artifact file `{}` does not exist", path.display());
    }
    if let Some(expected_size) = expected_size {
        let actual_size = fs::metadata(path)
            .with_context(|| format!("failed to stat artifact file `{}`", path.display()))?
            .len();
        if actual_size != expected_size {
            bail!(
                "artifact file size mismatch for `{}`: expected {}, got {}",
                path.display(),
                expected_size,
                actual_size
            );
        }
    }
    let actual_sha256 = sha256_file(path)?;
    if actual_sha256 != expected_sha256 {
        bail!("artifact file sha256 mismatch for `{}`", path.display());
    }
    Ok(())
}

pub fn sha256_file(path: &Path) -> Result<String> {
    let mut file =
        fs::File::open(path).with_context(|| format!("failed to open `{}`", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .with_context(|| format!("failed to read `{}`", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex::encode(hasher.finalize()))
}

fn unique_part_path(final_path: &Path) -> Result<PathBuf> {
    let file_name = final_path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| anyhow!("artifact destination has no file name"))?;
    Ok(final_path.with_file_name(format!("{file_name}.part")))
}

fn reject_path_traversal(path: &Path) -> Result<()> {
    for component in path.components() {
        if matches!(component, Component::ParentDir) {
            bail!("artifact destination must not contain parent traversal");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::platform::ClientPath;
    use pioneer_protocol::{ArtifactCreatedByKind, ArtifactKind, ArtifactStatus};
    use std::cell::RefCell;
    use std::collections::BTreeMap;

    #[test]
    fn artifact_download_writes_verified_bytes_to_selected_path() {
        let temp = tempfile::tempdir().expect("tempdir");
        let bytes = b"artifact bytes";
        let cache_path = temp.path().join("cache.bin");
        fs::write(cache_path.as_path(), bytes).expect("write cache");
        let download = cached_download(cache_path, bytes);

        let local_file = copy_cached_download_to_destination(&download, "report?.txt", temp.path())
            .expect("copy download");

        assert_eq!(fs::read(local_file.path.as_path()).expect("read"), bytes);
        assert_eq!(
            local_file.path.file_name().and_then(|value| value.to_str()),
            Some("report_.txt")
        );
        assert_eq!(local_file.sha256, sha256_bytes(bytes));
        assert_eq!(local_file.size_bytes, Some(bytes.len() as u64));
    }

    #[test]
    fn failed_download_copy_does_not_leave_final_file() {
        let temp = tempfile::tempdir().expect("tempdir");
        let bytes = b"artifact bytes";
        let cache_path = temp.path().join("cache.bin");
        fs::write(cache_path.as_path(), b"corrupt bytes").expect("write corrupt cache");
        let download = cached_download(cache_path, bytes);

        let error = copy_cached_download_to_destination(&download, "report.txt", temp.path())
            .expect_err("should fail verification");

        assert!(
            error.to_string().contains("size mismatch") || error.to_string().contains("sha256")
        );
        assert!(!temp.path().join("report.txt").exists());
    }

    #[test]
    fn artifact_file_names_are_sanitized_and_uniqued() {
        let temp = tempfile::tempdir().expect("tempdir");
        fs::write(temp.path().join("report_.txt"), b"existing").expect("write existing");

        let path = unique_destination_path(temp.path(), "../report?.txt").expect("path");

        assert_eq!(
            path.file_name().and_then(|value| value.to_str()),
            Some("report_ (1).txt")
        );
        assert!(path.starts_with(temp.path()));
    }

    #[test]
    fn existing_local_file_verification_checks_path_size_and_sha() {
        let temp = tempfile::tempdir().expect("tempdir");
        let bytes = b"artifact bytes";
        let local_path = temp.path().join("artifact.txt");
        fs::write(local_path.as_path(), bytes).expect("write artifact");
        let artifact = artifact_ref(
            "art_1",
            "artifact.txt",
            Some(bytes.len() as u64),
            Some(sha256_bytes(bytes)),
        );
        let local_file = ArtifactLocalFile {
            path: local_path.clone(),
            sha256: sha256_bytes(bytes),
            size_bytes: Some(bytes.len() as u64),
        };

        assert!(
            existing_local_file_is_verified(&local_file, &artifact).expect("verified local file")
        );

        let wrong_size = artifact_ref("art_1", "artifact.txt", Some(1), Some(sha256_bytes(bytes)));
        assert!(
            !existing_local_file_is_verified(&local_file, &wrong_size)
                .expect("size mismatch returns false")
        );

        let missing = ArtifactLocalFile {
            path: temp.path().join("missing.txt"),
            sha256: sha256_bytes(bytes),
            size_bytes: Some(bytes.len() as u64),
        };
        assert!(
            !existing_local_file_is_verified(&missing, &artifact)
                .expect("missing file returns false")
        );
    }

    #[test]
    fn artifact_version_key_uses_artifact_and_version_ids() {
        let artifact = artifact_ref("art_1", "artifact.txt", None, None);

        let key = artifact_version_key(&artifact);

        assert_eq!(key.artifact_id, "art_1");
        assert_eq!(key.version_id.as_deref(), Some("ver_1"));
    }

    #[test]
    fn artifact_action_state_tracks_status_by_artifact_version() {
        let artifact = artifact_ref("art_1", "artifact.txt", None, None);
        let mut state = ThreadArtifactActionState::default();

        assert!(state.status(&artifact).is_none());
        assert!(!state.in_progress(&artifact));

        state.set_status(&artifact, ArtifactActionStatus::Downloading);
        assert_eq!(
            state.status(&artifact),
            Some(&ArtifactActionStatus::Downloading)
        );
        assert!(state.in_progress(&artifact));

        state.set_status(&artifact, ArtifactActionStatus::Failed("boom".to_owned()));
        assert!(!state.in_progress(&artifact));

        state.clear_status(&artifact);
        assert!(state.status(&artifact).is_none());
    }

    #[test]
    fn artifact_download_action_block_reason_uses_ready_busy_and_connection_state() {
        let mut summary = artifact_summary(artifact_ref(
            "art_1",
            "artifact.txt",
            None,
            Some("sha".to_owned()),
        ));

        assert_eq!(artifact_download_block_reason(&summary, false, true), None);
        assert_eq!(
            artifact_download_block_reason(&summary, true, true),
            Some(ArtifactFileActionBlockReason::ActionInProgress)
        );
        assert_eq!(
            artifact_download_block_reason(&summary, false, false),
            Some(ArtifactFileActionBlockReason::NotConnected)
        );

        summary.artifact.status = ArtifactStatus::Pending;
        assert_eq!(
            artifact_download_block_reason(&summary, false, true),
            Some(ArtifactFileActionBlockReason::NotReady)
        );
    }

    #[test]
    fn artifact_download_request_planning_validates_gateway_and_workspace() {
        let summary = artifact_summary(artifact_ref("art_1", "artifact.txt", None, None));

        let request =
            plan_artifact_download_request(Some(" remote-1 ".to_owned()), &summary).expect("plan");

        assert_eq!(request.gateway_profile_id, "remote-1");
        assert_eq!(request.workspace_id, "ws_1");
        assert_eq!(request.artifact_id, "art_1");
        assert_eq!(request.version_id.as_deref(), Some("ver_1"));

        assert_eq!(
            plan_artifact_download_request(None, &summary).expect_err("missing gateway"),
            ArtifactDownloadRequestPlanError::MissingGatewayProfile
        );

        let mut missing_workspace = summary;
        missing_workspace.workspace_id = "  ".to_owned();
        assert_eq!(
            plan_artifact_download_request(Some("remote-1".to_owned()), &missing_workspace)
                .expect_err("missing workspace"),
            ArtifactDownloadRequestPlanError::MissingWorkspaceId
        );
    }

    #[test]
    fn ensure_local_copy_for_open_downloads_and_reuses_verified_file() {
        let temp = tempfile::tempdir().expect("tempdir");
        let bytes = b"artifact bytes";
        let cache_path = temp.path().join("cache.bin");
        fs::write(cache_path.as_path(), bytes).expect("write cache");
        let client = FakeDownloadClient {
            result: ArtifactDownloadResult {
                local_path: ClientPath::new(cache_path.clone()),
                artifact: artifact_ref(
                    "art_1",
                    "artifact.txt",
                    Some(bytes.len() as u64),
                    Some(sha256_bytes(bytes)),
                ),
                size_bytes: bytes.len() as u64,
                sha256: sha256_bytes(bytes),
            },
            calls: Default::default(),
        };
        let artifact = artifact_ref(
            "art_1",
            "artifact.txt",
            Some(bytes.len() as u64),
            Some(sha256_bytes(bytes)),
        );

        let local_file = ensure_artifact_local_copy_for_open(
            &client,
            download_request("art_1"),
            &artifact,
            None,
        )
        .expect("local copy");

        assert_eq!(local_file.path, cache_path);
        assert_eq!(*client.calls.borrow(), 1);

        let reused = ensure_artifact_local_copy_for_open(
            &client,
            download_request("art_1"),
            &artifact,
            Some(&local_file),
        )
        .expect("reused local copy");

        assert_eq!(reused.path, local_file.path);
        assert_eq!(*client.calls.borrow(), 1);
    }

    #[derive(Clone)]
    struct FakeDownloadClient {
        result: ArtifactDownloadResult,
        calls: std::rc::Rc<RefCell<usize>>,
    }

    impl ArtifactCachedDownloadClient for FakeDownloadClient {
        fn download_artifact_to_cache(
            &self,
            request: ArtifactDownloadRequest,
        ) -> Result<ArtifactDownloadResult> {
            assert_eq!(request.artifact_id, self.result.artifact.artifact_id);
            *self.calls.borrow_mut() += 1;
            Ok(self.result.clone())
        }
    }

    fn cached_download(local_path: PathBuf, bytes: &[u8]) -> ArtifactCachedDownload {
        ArtifactCachedDownload::new(local_path, bytes.len() as u64, sha256_bytes(bytes))
    }

    fn artifact_ref(
        artifact_id: &str,
        display_name: &str,
        size_bytes: Option<u64>,
        sha256: Option<String>,
    ) -> ArtifactRef {
        ArtifactRef {
            artifact_id: artifact_id.to_owned(),
            version_id: Some("ver_1".to_owned()),
            display_name: display_name.to_owned(),
            kind: ArtifactKind::Text,
            mime_type: Some("text/plain".to_owned()),
            size_bytes,
            sha256,
            status: ArtifactStatus::Ready,
            preview: None,
        }
    }

    fn artifact_summary(artifact: ArtifactRef) -> ArtifactSummary {
        ArtifactSummary {
            artifact,
            workspace_id: "ws_1".to_owned(),
            primary_thread_id: Some("thread_1".to_owned()),
            created_by_kind: ArtifactCreatedByKind::User,
            created_by_actor_id: None,
            created_at: 1,
            updated_at: 1,
            bindings: Vec::new(),
            metadata: BTreeMap::new(),
        }
    }

    fn download_request(artifact_id: &str) -> ArtifactDownloadRequest {
        ArtifactDownloadRequest {
            gateway_profile_id: "remote".to_owned(),
            workspace_id: "ws_1".to_owned(),
            artifact_id: artifact_id.to_owned(),
            version_id: Some("ver_1".to_owned()),
        }
    }

    fn sha256_bytes(bytes: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        hex::encode(hasher.finalize())
    }
}
