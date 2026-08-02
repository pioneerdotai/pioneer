//! Artifact file action state and helpers.

use crate::artifacts::http_download::{ArtifactHttpDownloadRequest, ArtifactHttpDownloadResult};
use crate::local_file::{configure_std_no_follow, metadata_is_plain_file};
use crate::platform::{ArtifactFileOpener, ClientPath};
use anyhow::{Context as _, Result, bail};
use pioneer_protocol::{ArtifactRef, ArtifactStatus, ArtifactSummary};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    fs,
    io::{Read, Seek, SeekFrom, Write},
    path::{Component, Path, PathBuf},
    sync::{
        Arc, Mutex, OnceLock, Weak,
        atomic::{AtomicU64, Ordering},
    },
};

const MAX_DESTINATION_FILE_NAME_BYTES: usize = 180;
const DESTINATION_FILE_NAME_HASH_CHARS: usize = 16;
const MAX_DESTINATION_EXTENSION_BYTES: usize = 16;

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
    Downloading {
        downloaded_bytes: u64,
        total_bytes: u64,
    },
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

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum ArtifactFileActionBlockReason {
    NotReady,
    ActionInProgress,
    NotConnected,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum ArtifactHttpDownloadRequestPlanError {
    MissingGatewayProfile,
    MissingWorkspaceId,
    MissingVersionId,
    MissingSize,
    MissingSha256,
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

pub fn plan_artifact_http_download_request(
    gateway_profile_id: Option<String>,
    summary: &ArtifactSummary,
) -> std::result::Result<ArtifactHttpDownloadRequest, ArtifactHttpDownloadRequestPlanError> {
    let gateway_profile_id = gateway_profile_id
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .ok_or(ArtifactHttpDownloadRequestPlanError::MissingGatewayProfile)?;
    if summary.workspace_id.trim().is_empty() {
        return Err(ArtifactHttpDownloadRequestPlanError::MissingWorkspaceId);
    }
    let version_id = summary
        .artifact
        .version_id
        .clone()
        .filter(|value| !value.trim().is_empty())
        .ok_or(ArtifactHttpDownloadRequestPlanError::MissingVersionId)?;
    let expected_size_bytes = summary
        .artifact
        .size_bytes
        .ok_or(ArtifactHttpDownloadRequestPlanError::MissingSize)?;
    let expected_sha256 = summary
        .artifact
        .sha256
        .clone()
        .filter(|value| !value.trim().is_empty())
        .ok_or(ArtifactHttpDownloadRequestPlanError::MissingSha256)?;
    Ok(ArtifactHttpDownloadRequest {
        gateway_profile_id,
        workspace_id: summary.workspace_id.clone(),
        artifact_id: summary.artifact.artifact_id.clone(),
        version_id,
        display_name: summary.artifact.display_name.clone(),
        expected_size_bytes,
        expected_sha256,
    })
}

pub fn local_file_from_cached_download(download: &ArtifactCachedDownload) -> ArtifactLocalFile {
    ArtifactLocalFile {
        path: download.local_path.clone(),
        sha256: download.sha256.clone(),
        size_bytes: Some(download.size_bytes),
    }
}

pub fn cached_download_from_http_download_result(
    result: &ArtifactHttpDownloadResult,
) -> ArtifactCachedDownload {
    ArtifactCachedDownload::new(
        result.local_path.as_path().to_owned(),
        result.size_bytes,
        result.sha256.clone(),
    )
}

pub fn copy_http_download_result_to_destination(
    result: &ArtifactHttpDownloadResult,
    display_name: &str,
    destination_dir: &Path,
) -> Result<ArtifactLocalFile> {
    copy_cached_download_to_destination(
        &cached_download_from_http_download_result(result),
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

    pub(crate) fn remove_keys(&mut self, keys: &HashSet<ArtifactVersionKey>) {
        self.status_by_artifact.retain(|key, _| !keys.contains(key));
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
    let destination_identity = fs::canonicalize(destination_dir).with_context(|| {
        format!(
            "failed to resolve artifact destination `{}`",
            destination_dir.display()
        )
    })?;
    let destination_gate = destination_gate_for(destination_identity.as_path());
    let _destination_operation = destination_gate
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (part_path, mut part_file) = create_owned_part_file(destination_dir)?;
    let copy_result = (|| -> Result<PathBuf> {
        // Verify and copy through the same no-follow file handle. Reopening the
        // cache path after verification would leave a swap/symlink TOCTOU gap.
        let mut source = open_verified_file(
            download.local_path.as_path(),
            download.sha256.as_str(),
            Some(download.size_bytes),
        )?;
        std::io::copy(&mut source, &mut part_file).with_context(|| {
            format!(
                "failed to copy verified artifact download into `{}`",
                destination_dir.display()
            )
        })?;
        part_file.flush().with_context(|| {
            format!(
                "failed to flush artifact download in `{}`",
                destination_dir.display()
            )
        })?;
        part_file.sync_all().with_context(|| {
            format!(
                "failed to synchronize artifact download in `{}`",
                destination_dir.display()
            )
        })?;
        drop(part_file);
        verify_file(
            part_path.as_path(),
            download.sha256.as_str(),
            Some(download.size_bytes),
        )?;
        let final_path = publish_without_overwrite(
            part_path.as_path(),
            destination_dir,
            display_name,
        )?;
        let _ = fs::remove_file(part_path.as_path());
        sync_directory(destination_dir)?;
        Ok(final_path)
    })();

    if copy_result.is_err() {
        let _ = fs::remove_file(part_path.as_path());
    }
    let final_path = copy_result?;

    Ok(ArtifactLocalFile {
        path: final_path,
        sha256: download.sha256.clone(),
        size_bytes: Some(download.size_bytes),
    })
}

pub fn open_artifact_local_file<O: ArtifactFileOpener>(opener: &O, path: &Path) -> Result<()> {
    if !fs::symlink_metadata(path).is_ok_and(|metadata| metadata_is_plain_file(&metadata)) {
        bail!("artifact local file `{}` does not exist", path.display());
    }
    opener.open_file(&ClientPath::new(path.to_owned()))?;
    Ok(())
}

pub fn reveal_artifact_local_file<O: ArtifactFileOpener>(opener: &O, path: &Path) -> Result<()> {
    if !fs::symlink_metadata(path).is_ok_and(|metadata| metadata_is_plain_file(&metadata)) {
        bail!("artifact local file `{}` does not exist", path.display());
    }
    opener.reveal_file(&ClientPath::new(path.to_owned()))?;
    Ok(())
}

pub fn existing_local_file_is_verified(
    local_file: &ArtifactLocalFile,
    artifact: &ArtifactRef,
) -> Result<bool> {
    let metadata = match fs::symlink_metadata(local_file.path.as_path()) {
        Ok(metadata) if metadata_is_plain_file(&metadata) => metadata,
        Ok(_) => return Ok(false),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "failed to inspect artifact local file `{}`",
                    local_file.path.display()
                )
            });
        }
    };
    let expected_sha = artifact
        .sha256
        .as_deref()
        .unwrap_or(local_file.sha256.as_str());
    if expected_sha.trim().is_empty() {
        return Ok(false);
    }
    if let Some(expected_size) = artifact.size_bytes.or(local_file.size_bytes) {
        if metadata.len() != expected_size {
            return Ok(false);
        }
    }
    let mut file = open_readonly_no_follow(local_file.path.as_path())?;
    Ok(sha256_open_file(&mut file, local_file.path.as_path())? == expected_sha)
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
    let safe_name = if trimmed.is_empty() {
        fallback
    } else {
        trimmed
    };
    if safe_name.len() <= MAX_DESTINATION_FILE_NAME_BYTES {
        safe_name.to_owned()
    } else {
        bounded_artifact_file_name(safe_name)
    }
}

pub fn unique_destination_path(destination_dir: &Path, display_name: &str) -> Result<PathBuf> {
    reject_path_traversal(destination_dir)?;
    let safe_name = sanitized_artifact_file_name(display_name);
    let initial = destination_dir.join(safe_name.as_str());
    if !path_is_occupied(initial.as_path())? {
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
        if !path_is_occupied(candidate.as_path())? {
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
    drop(open_verified_file(path, expected_sha256, expected_size)?);
    Ok(())
}

pub fn sha256_file(path: &Path) -> Result<String> {
    let mut file = open_readonly_no_follow(path)?;
    sha256_open_file(&mut file, path)
}

fn open_verified_file(
    path: &Path,
    expected_sha256: &str,
    expected_size: Option<u64>,
) -> Result<fs::File> {
    let mut file = open_readonly_no_follow(path)?;
    if let Some(expected_size) = expected_size {
        let actual_size = file
            .metadata()
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
    let actual_sha256 = sha256_open_file(&mut file, path)?;
    if actual_sha256 != expected_sha256 {
        bail!("artifact file sha256 mismatch for `{}`", path.display());
    }
    file.seek(SeekFrom::Start(0))
        .with_context(|| format!("failed to rewind artifact file `{}`", path.display()))?;
    Ok(file)
}

fn open_readonly_no_follow(path: &Path) -> Result<fs::File> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("failed to inspect artifact file `{}`", path.display()))?;
    if !metadata_is_plain_file(&metadata) {
        bail!("artifact file `{}` is not a regular file", path.display());
    }
    let mut options = fs::OpenOptions::new();
    options.read(true);
    configure_std_no_follow(&mut options);
    let file = options
        .open(path)
        .with_context(|| format!("failed to open artifact file `{}`", path.display()))?;
    let opened_metadata = file
        .metadata()
        .with_context(|| format!("failed to stat artifact file `{}`", path.display()))?;
    if !metadata_is_plain_file(&opened_metadata) {
        bail!("artifact file `{}` is not a regular file", path.display());
    }
    Ok(file)
}

fn sha256_open_file(file: &mut fs::File, path: &Path) -> Result<String> {
    file.seek(SeekFrom::Start(0))
        .with_context(|| format!("failed to rewind artifact file `{}`", path.display()))?;
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

fn destination_gate_for(destination: &Path) -> Arc<Mutex<()>> {
    static GATES: OnceLock<Mutex<HashMap<PathBuf, Weak<Mutex<()>>>>> = OnceLock::new();
    let gates = GATES.get_or_init(|| Mutex::new(HashMap::new()));
    let mut gates = gates
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    gates.retain(|_, gate| gate.strong_count() > 0);
    if let Some(gate) = gates.get(destination).and_then(Weak::upgrade) {
        return gate;
    }
    let gate = Arc::new(Mutex::new(()));
    gates.insert(destination.to_owned(), Arc::downgrade(&gate));
    gate
}

fn create_owned_part_file(destination_dir: &Path) -> Result<(PathBuf, fs::File)> {
    static NEXT_PART_ID: AtomicU64 = AtomicU64::new(1);
    for _ in 0..10_000 {
        let part_id = NEXT_PART_ID.fetch_add(1, Ordering::Relaxed);
        let part_path = destination_dir.join(format!(
            ".pioneer-artifact-{}-{part_id}.part",
            std::process::id()
        ));
        let mut options = fs::OpenOptions::new();
        options.write(true).create_new(true);
        configure_std_no_follow(&mut options);
        match options.open(part_path.as_path()) {
            Ok(file) => return Ok((part_path, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "failed to create artifact download staging file in `{}`",
                        destination_dir.display()
                    )
                });
            }
        }
    }
    bail!("failed to allocate an artifact download staging file")
}

fn publish_without_overwrite(
    part_path: &Path,
    destination_dir: &Path,
    display_name: &str,
) -> Result<PathBuf> {
    for _ in 0..10_000 {
        let final_path = unique_destination_path(destination_dir, display_name)?;
        match fs::hard_link(part_path, final_path.as_path()) {
            Ok(()) => return Ok(final_path),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "failed to publish artifact download in `{}`",
                        destination_dir.display()
                    )
                });
            }
        }
    }
    bail!("failed to publish a uniquely named artifact download")
}

fn path_is_occupied(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error).with_context(|| {
            format!("failed to inspect artifact destination `{}`", path.display())
        }),
    }
}

fn bounded_artifact_file_name(file_name: &str) -> String {
    let digest = Sha256::digest(file_name.as_bytes());
    let digest = &hex::encode(digest)[..DESTINATION_FILE_NAME_HASH_CHARS];
    let path = Path::new(file_name);
    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty() && value.len() <= MAX_DESTINATION_EXTENSION_BYTES);
    let suffix_len = 1
        + DESTINATION_FILE_NAME_HASH_CHARS
        + extension.map_or(0, |value| value.len() + 1);
    let stem_budget = MAX_DESTINATION_FILE_NAME_BYTES.saturating_sub(suffix_len);
    let stem = path
        .file_stem()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("artifact");
    let stem = &stem[..stem.len().min(stem_budget)];

    match extension {
        Some(extension) => format!("{stem}-{digest}.{extension}"),
        None => format!("{stem}-{digest}"),
    }
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<()> {
    fs::File::open(path)
        .with_context(|| format!("failed to open artifact destination `{}`", path.display()))?
        .sync_all()
        .with_context(|| format!("failed to synchronize artifact destination `{}`", path.display()))
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<()> {
    Ok(())
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
    use pioneer_protocol::{ArtifactCreatedByKind, ArtifactKind, ArtifactStatus};
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
    fn destination_copy_never_removes_a_preexisting_part_file() {
        let temp = tempfile::tempdir().expect("tempdir");
        let bytes = b"artifact bytes";
        let cache_path = temp.path().join("cache.bin");
        let existing_part = temp.path().join("report.txt.part");
        fs::write(cache_path.as_path(), bytes).expect("write cache");
        fs::write(existing_part.as_path(), b"owned by another operation")
            .expect("write existing part");

        let local_file = copy_cached_download_to_destination(
            &cached_download(cache_path, bytes),
            "report.txt",
            temp.path(),
        )
        .expect("copy download");

        assert_eq!(fs::read(local_file.path).expect("read final"), bytes);
        assert_eq!(
            fs::read(existing_part).expect("read existing part"),
            b"owned by another operation"
        );
        assert!(
            fs::read_dir(temp.path())
                .expect("read destination")
                .filter_map(Result::ok)
                .all(|entry| !entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".pioneer-artifact-"))
        );
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
    fn artifact_file_names_are_bounded_without_losing_extension_or_identity() {
        let first = format!("{}.txt", "a".repeat(400));
        let second = format!("{}b.txt", "a".repeat(399));

        let first_safe = sanitized_artifact_file_name(first.as_str());
        let second_safe = sanitized_artifact_file_name(second.as_str());

        assert!(first_safe.len() <= MAX_DESTINATION_FILE_NAME_BYTES);
        assert!(second_safe.len() <= MAX_DESTINATION_FILE_NAME_BYTES);
        assert!(first_safe.ends_with(".txt"));
        assert!(second_safe.ends_with(".txt"));
        assert_ne!(first_safe, second_safe);
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

        state.set_status(
            &artifact,
            ArtifactActionStatus::Downloading {
                downloaded_bytes: 4,
                total_bytes: 10,
            },
        );
        assert_eq!(
            state.status(&artifact),
            Some(&ArtifactActionStatus::Downloading {
                downloaded_bytes: 4,
                total_bytes: 10,
            })
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
    fn http_download_request_planning_requires_immutable_version_metadata() {
        let bytes = b"artifact bytes";
        let digest = sha256_bytes(bytes);
        let summary = artifact_summary(artifact_ref(
            "art_1",
            "artifact.txt",
            Some(bytes.len() as u64),
            Some(digest.clone()),
        ));

        let request = plan_artifact_http_download_request(
            Some(" remote-1 ".to_owned()),
            &summary,
        )
        .expect("HTTP plan");

        assert_eq!(request.gateway_profile_id, "remote-1");
        assert_eq!(request.workspace_id, "ws_1");
        assert_eq!(request.artifact_id, "art_1");
        assert_eq!(request.version_id, "ver_1");
        assert_eq!(request.display_name, "artifact.txt");
        assert_eq!(request.expected_size_bytes, bytes.len() as u64);
        assert_eq!(request.expected_sha256, digest);

        let mut missing_version = summary.clone();
        missing_version.artifact.version_id = None;
        assert_eq!(
            plan_artifact_http_download_request(Some("remote-1".to_owned()), &missing_version)
                .expect_err("missing version"),
            ArtifactHttpDownloadRequestPlanError::MissingVersionId
        );

        let mut missing_size = summary.clone();
        missing_size.artifact.size_bytes = None;
        assert_eq!(
            plan_artifact_http_download_request(Some("remote-1".to_owned()), &missing_size)
                .expect_err("missing size"),
            ArtifactHttpDownloadRequestPlanError::MissingSize
        );

        let mut missing_sha = summary;
        missing_sha.artifact.sha256 = None;
        assert_eq!(
            plan_artifact_http_download_request(Some("remote-1".to_owned()), &missing_sha)
                .expect_err("missing SHA"),
            ArtifactHttpDownloadRequestPlanError::MissingSha256
        );
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

    fn sha256_bytes(bytes: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        hex::encode(hasher.finalize())
    }
}
