use super::*;
use anyhow::{Context as _, bail};
use std::{
    fs,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

const ARTIFACT_DOWNLOAD_CHUNK_FRAME_MAGIC: &[u8; 4] = b"ARTD";
pub(super) const ARTIFACT_DOWNLOAD_CACHE_MAX_AGE: Duration = Duration::from_secs(7 * 24 * 60 * 60);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ArtifactDownloadChunkPayload {
    pub header: ArtifactDownloadChunkHeader,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ArtifactDownloadCachePaths {
    pub final_path: PathBuf,
    pub part_path: PathBuf,
}

pub(super) fn artifact_download_chunk_key(download_id: &str, offset: u64) -> String {
    format!("{download_id}:{offset}")
}

pub(super) fn parse_artifact_download_chunk_frame(
    frame: &[u8],
) -> Result<ArtifactDownloadChunkPayload> {
    if frame.len() < 8 {
        bail!("artifact download frame is too short");
    }
    if &frame[0..4] != ARTIFACT_DOWNLOAD_CHUNK_FRAME_MAGIC {
        bail!("artifact download frame has invalid magic");
    }
    let header_len = u32::from_be_bytes([frame[4], frame[5], frame[6], frame[7]]) as usize;
    let header_start = 8usize;
    let header_end = header_start.saturating_add(header_len);
    if header_end > frame.len() {
        bail!("artifact download frame header length exceeds frame length");
    }
    let header =
        serde_json::from_slice::<ArtifactDownloadChunkHeader>(&frame[header_start..header_end])
            .context("failed to parse artifact download frame header")?;
    let bytes = frame[header_end..].to_vec();
    if header.len != u64::try_from(bytes.len()).unwrap_or(u64::MAX) {
        bail!("artifact download frame chunk length mismatch");
    }
    let actual_sha256 = sha256_bytes(bytes.as_slice());
    if header.chunk_sha256 != actual_sha256 {
        bail!("artifact download frame chunk sha256 mismatch");
    }
    Ok(ArtifactDownloadChunkPayload { header, bytes })
}

pub(super) fn process_artifact_download_binary_frame(
    frame: &[u8],
    pending_download_chunks: &mut HashMap<
        String,
        Sender<std::result::Result<ArtifactDownloadChunkPayload, String>>,
    >,
) -> bool {
    if frame.len() < 4 || &frame[0..4] != ARTIFACT_DOWNLOAD_CHUNK_FRAME_MAGIC {
        return false;
    }

    let payload = match parse_artifact_download_chunk_frame(frame) {
        Ok(payload) => payload,
        Err(error) => {
            for (_, response_tx) in pending_download_chunks.drain() {
                let _ = response_tx.send(Err(format!("{error:#}")));
            }
            return true;
        }
    };
    let key =
        artifact_download_chunk_key(payload.header.download_id.as_str(), payload.header.offset);
    if let Some(response_tx) = pending_download_chunks.remove(&key) {
        let _ = response_tx.send(Ok(payload));
    }
    true
}

#[allow(clippy::too_many_arguments)]
pub(super) fn validate_artifact_download_chunk_payload(
    payload: &ArtifactDownloadChunkPayload,
    workspace_id: &str,
    download_id: &str,
    artifact_id: &str,
    version_id: &str,
    offset: u64,
    len: u64,
    total_size_bytes: u64,
) -> Result<()> {
    let header = &payload.header;
    if header.workspace_id != workspace_id
        || header.download_id != download_id
        || header.artifact_id != artifact_id
        || header.version_id != version_id
    {
        bail!("artifact download frame identity mismatch");
    }
    if header.offset != offset || header.len != len {
        bail!("artifact download frame range mismatch");
    }
    if header.total_size_bytes != total_size_bytes {
        bail!("artifact download frame total size mismatch");
    }
    Ok(())
}

pub(super) fn build_artifact_download_cache_path(
    runtime_home: &Path,
    gateway_profile_id: &str,
    workspace_id: &str,
    artifact_id: &str,
    version_id: &str,
    display_name: &str,
) -> Result<ArtifactDownloadCachePaths> {
    let safe_gateway_id = safe_path_segment(gateway_profile_id, "gateway");
    let safe_workspace_id = safe_path_segment(workspace_id, "workspace");
    let safe_artifact_id = safe_path_segment(artifact_id, "artifact");
    let safe_version_id = safe_path_segment(version_id, "version");
    let safe_display_name = safe_path_segment(display_name, "artifact.bin");

    let directory = runtime_home
        .join("downloads")
        .join("gateways")
        .join(safe_gateway_id)
        .join("workspaces")
        .join(safe_workspace_id)
        .join("artifacts")
        .join(safe_artifact_id)
        .join(safe_version_id);
    let final_path = directory.join(safe_display_name.as_str());
    let part_path = directory.join(format!("{safe_display_name}.part"));
    ensure_child_path(runtime_home, final_path.as_path())?;
    ensure_child_path(runtime_home, part_path.as_path())?;
    Ok(ArtifactDownloadCachePaths {
        final_path,
        part_path,
    })
}

pub(super) fn prune_artifact_download_cache(runtime_home: &Path, max_age: Duration) -> Result<u64> {
    let cache_root = runtime_home.join("downloads").join("gateways");
    ensure_child_path(runtime_home, cache_root.as_path())?;
    if !cache_root.exists() {
        return Ok(0);
    }
    let cutoff = SystemTime::now()
        .checked_sub(max_age)
        .unwrap_or(SystemTime::UNIX_EPOCH);
    prune_download_cache_dir(cache_root.as_path(), cutoff)
}

pub(super) fn sha256_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn ensure_child_path(root: &Path, candidate: &Path) -> Result<()> {
    if !candidate.starts_with(root) {
        bail!("artifact download cache path escaped runtime home");
    }
    Ok(())
}

fn safe_path_segment(value: &str, fallback: &str) -> String {
    let sanitized = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '.' || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    let trimmed = sanitized
        .trim_matches([' ', '\t', '\n', '\r'])
        .trim_matches('.');
    if trimmed.is_empty() || trimmed == "." || trimmed == ".." {
        fallback.to_owned()
    } else {
        trimmed.to_owned()
    }
}

fn prune_download_cache_dir(dir: &Path, cutoff: SystemTime) -> Result<u64> {
    let mut removed = 0_u64;
    for entry in fs::read_dir(dir)
        .with_context(|| format!("failed to read cache dir `{}`", dir.display()))?
    {
        let entry = entry
            .with_context(|| format!("failed to read cache entry under `{}`", dir.display()))?;
        let path = entry.path();
        let metadata = entry
            .metadata()
            .with_context(|| format!("failed to stat cache entry `{}`", path.display()))?;
        if metadata.is_dir() {
            removed = removed.saturating_add(prune_download_cache_dir(path.as_path(), cutoff)?);
            if fs::read_dir(path.as_path())
                .map(|mut entries| entries.next().is_none())
                .unwrap_or(false)
            {
                let _ = fs::remove_dir(path.as_path());
            }
            continue;
        }
        if metadata.is_file()
            && metadata
                .modified()
                .map(|modified| modified <= cutoff)
                .unwrap_or(false)
        {
            fs::remove_file(path.as_path())
                .with_context(|| format!("failed to remove cache file `{}`", path.display()))?;
            removed = removed.saturating_add(1);
        }
    }
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn artifact_download_parse_valid_artd_frame() {
        let frame = test_frame("download_1", 0, b"hello", true);

        let payload = parse_artifact_download_chunk_frame(frame.as_slice()).expect("parse frame");

        assert_eq!(payload.header.download_id, "download_1");
        assert_eq!(payload.header.offset, 0);
        assert_eq!(payload.bytes, b"hello");
    }

    #[test]
    fn artifact_download_rejects_wrong_magic() {
        let mut frame = test_frame("download_1", 0, b"hello", true);
        frame[0..4].copy_from_slice(b"NOPE");

        assert!(parse_artifact_download_chunk_frame(frame.as_slice()).is_err());
    }

    #[test]
    fn artifact_download_rejects_wrong_chunk_sha() {
        let header = json!({
            "workspace_id": "ws_1",
            "download_id": "download_1",
            "artifact_id": "art_1",
            "version_id": "ver_1",
            "offset": 0,
            "len": 5,
            "total_size_bytes": 5,
            "chunk_sha256": "0".repeat(64),
            "final_chunk": true
        });
        let header_bytes = serde_json::to_vec(&header).unwrap();
        let mut frame = Vec::new();
        frame.extend_from_slice(ARTIFACT_DOWNLOAD_CHUNK_FRAME_MAGIC);
        frame.extend_from_slice(&(header_bytes.len() as u32).to_be_bytes());
        frame.extend_from_slice(header_bytes.as_slice());
        frame.extend_from_slice(b"hello");

        assert!(parse_artifact_download_chunk_frame(frame.as_slice()).is_err());
    }

    #[test]
    fn artifact_download_rejects_wrong_offset_or_download_id() {
        let payload = parse_artifact_download_chunk_frame(
            test_frame("download_1", 4, b"hello", false).as_slice(),
        )
        .expect("parse frame");

        assert!(
            validate_artifact_download_chunk_payload(
                &payload,
                "ws_1",
                "download_2",
                "art_1",
                "ver_1",
                4,
                5,
                9,
            )
            .is_err()
        );
        assert!(
            validate_artifact_download_chunk_payload(
                &payload,
                "ws_1",
                "download_1",
                "art_1",
                "ver_1",
                0,
                5,
                9,
            )
            .is_err()
        );
    }

    #[test]
    fn artifact_download_cache_path_stays_under_runtime_home() {
        let temp = tempfile::tempdir().expect("temp dir");
        let paths = build_artifact_download_cache_path(
            temp.path(),
            "../gateway",
            "ws/1",
            "art/1",
            "ver/1",
            "../report.txt",
        )
        .expect("cache path");

        assert!(paths.final_path.starts_with(temp.path()));
        assert!(paths.part_path.starts_with(temp.path()));
        assert_eq!(
            paths
                .final_path
                .file_name()
                .and_then(|value| value.to_str()),
            Some("_report.txt")
        );
    }

    #[test]
    fn artifact_download_cache_prune_removes_expired_files() {
        let temp = tempfile::tempdir().expect("temp dir");
        let paths = build_artifact_download_cache_path(
            temp.path(),
            "gateway",
            "ws",
            "art",
            "ver",
            "report.txt",
        )
        .expect("cache path");
        fs::create_dir_all(paths.final_path.parent().expect("parent")).expect("create cache dir");
        fs::write(paths.final_path.as_path(), b"cached").expect("write cache file");

        let removed =
            prune_artifact_download_cache(temp.path(), Duration::ZERO).expect("prune cache");

        assert_eq!(removed, 1);
        assert!(!paths.final_path.exists());
    }

    fn test_frame(download_id: &str, offset: u64, chunk: &[u8], final_chunk: bool) -> Vec<u8> {
        let header = ArtifactDownloadChunkHeader {
            workspace_id: "ws_1".to_owned(),
            download_id: download_id.to_owned(),
            artifact_id: "art_1".to_owned(),
            version_id: "ver_1".to_owned(),
            offset,
            len: chunk.len() as u64,
            total_size_bytes: offset + chunk.len() as u64,
            chunk_sha256: sha256_bytes(chunk),
            final_chunk,
        };
        let header_bytes = serde_json::to_vec(&header).unwrap();
        let mut frame = Vec::new();
        frame.extend_from_slice(ARTIFACT_DOWNLOAD_CHUNK_FRAME_MAGIC);
        frame.extend_from_slice(&(header_bytes.len() as u32).to_be_bytes());
        frame.extend_from_slice(header_bytes.as_slice());
        frame.extend_from_slice(chunk);
        frame
    }
}
