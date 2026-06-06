//! Artifact download state and helpers.

use crate::{
    platform::ClientPath,
    transport::ws::download::{
        ArtifactDownloadChunkPayload, validate_artifact_download_chunk_payload,
    },
    transport::ws::frames::sha256_bytes,
};
use anyhow::{Context as _, Result, anyhow, bail};
use pioneer_protocol::{
    ArtifactDownloadAbortParams, ArtifactDownloadAbortResponse, ArtifactDownloadChunkParams,
    ArtifactDownloadChunkResponse, ArtifactDownloadFinishParams, ArtifactDownloadFinishResponse,
    ArtifactDownloadStartParams, ArtifactDownloadStartResponse, ArtifactRef,
};
use std::{
    fs,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

pub const ARTIFACT_DOWNLOAD_CHUNK_TIMEOUT: Duration = Duration::from_secs(30);
pub const ARTIFACT_DOWNLOAD_CACHE_MAX_AGE: Duration = Duration::from_secs(7 * 24 * 60 * 60);

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ArtifactDownloadRequest {
    pub gateway_profile_id: String,
    pub workspace_id: String,
    pub artifact_id: String,
    pub version_id: Option<String>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ArtifactDownloadResult {
    pub local_path: ClientPath,
    pub artifact: ArtifactRef,
    pub size_bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactDownloadCachePaths {
    pub final_path: ClientPath,
    pub part_path: ClientPath,
}

pub trait ArtifactDownloadChunkWaiter {
    fn recv_timeout(&self, timeout: Duration) -> Result<ArtifactDownloadChunkPayload>;
}

pub trait ArtifactDownloadTransport {
    fn artifact_download_start(
        &self,
        params: ArtifactDownloadStartParams,
    ) -> Result<ArtifactDownloadStartResponse>;

    fn register_artifact_download_chunk(
        &self,
        download_id: &str,
        offset: u64,
    ) -> Result<Box<dyn ArtifactDownloadChunkWaiter>>;

    fn artifact_download_chunk(
        &self,
        params: ArtifactDownloadChunkParams,
    ) -> Result<ArtifactDownloadChunkResponse>;

    fn artifact_download_finish(
        &self,
        params: ArtifactDownloadFinishParams,
    ) -> Result<ArtifactDownloadFinishResponse>;

    fn artifact_download_abort(
        &self,
        params: ArtifactDownloadAbortParams,
    ) -> Result<ArtifactDownloadAbortResponse>;
}

pub trait ArtifactDownloadCache {
    type Sink: ArtifactDownloadSink;

    fn prune(&self) -> Result<()>;

    fn create_sink(
        &self,
        request: &ArtifactDownloadRequest,
        start: &ArtifactDownloadStartResponse,
        version_id: &str,
    ) -> Result<Self::Sink>;
}

pub trait ArtifactDownloadSink {
    fn prepare(&mut self) -> Result<()>;

    fn write_chunk(&mut self, offset: u64, bytes: &[u8]) -> Result<()>;

    fn finalize(&mut self) -> Result<ClientPath>;

    fn cleanup_partial(&mut self);
}

#[derive(Clone, Debug)]
pub struct ArtifactDownloadFileCache {
    runtime_home: PathBuf,
}

impl ArtifactDownloadFileCache {
    pub fn new(runtime_home: impl Into<PathBuf>) -> Self {
        Self {
            runtime_home: runtime_home.into(),
        }
    }

    pub fn runtime_home(&self) -> &Path {
        self.runtime_home.as_path()
    }
}

impl ArtifactDownloadCache for ArtifactDownloadFileCache {
    type Sink = ArtifactDownloadFileSink;

    fn prune(&self) -> Result<()> {
        let _ = prune_artifact_download_cache(self.runtime_home(), ARTIFACT_DOWNLOAD_CACHE_MAX_AGE);
        Ok(())
    }

    fn create_sink(
        &self,
        request: &ArtifactDownloadRequest,
        start: &ArtifactDownloadStartResponse,
        version_id: &str,
    ) -> Result<Self::Sink> {
        let cache_paths = build_artifact_download_cache_path(
            self.runtime_home(),
            request.gateway_profile_id.as_str(),
            request.workspace_id.as_str(),
            request.artifact_id.as_str(),
            version_id,
            start.file_name.as_str(),
        )?;
        Ok(ArtifactDownloadFileSink {
            cache_paths,
            expected_size_bytes: start.size_bytes,
            expected_sha256: start.sha256.clone(),
            file: None,
        })
    }
}

pub struct ArtifactDownloadFileSink {
    cache_paths: ArtifactDownloadCachePaths,
    expected_size_bytes: u64,
    expected_sha256: String,
    file: Option<fs::File>,
}

impl ArtifactDownloadSink for ArtifactDownloadFileSink {
    fn prepare(&mut self) -> Result<()> {
        if let Some(parent) = self.cache_paths.part_path.as_path().parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "failed to create artifact download cache {}",
                    parent.display()
                )
            })?;
        }
        let _ = fs::remove_file(self.cache_paths.part_path.as_path());
        let file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(true)
            .open(self.cache_paths.part_path.as_path())
            .with_context(|| {
                format!(
                    "failed to create artifact download part file {}",
                    self.cache_paths.part_path.as_path().display()
                )
            })?;
        self.file = Some(file);
        Ok(())
    }

    fn write_chunk(&mut self, offset: u64, bytes: &[u8]) -> Result<()> {
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| anyhow!("artifact download sink is not prepared"))?;
        write_chunk_at(file, offset, bytes)
    }

    fn finalize(&mut self) -> Result<ClientPath> {
        if let Some(file) = self.file.take() {
            file.sync_data()
                .context("failed to sync artifact download")?;
        }

        verify_artifact_download_file(
            self.cache_paths.part_path.as_path(),
            self.expected_size_bytes,
            self.expected_sha256.as_str(),
        )?;

        let _ = fs::remove_file(self.cache_paths.final_path.as_path());
        fs::rename(
            self.cache_paths.part_path.as_path(),
            self.cache_paths.final_path.as_path(),
        )
        .with_context(|| {
            format!(
                "failed to finalize artifact download {}",
                self.cache_paths.final_path.as_path().display()
            )
        })?;
        Ok(self.cache_paths.final_path.clone())
    }

    fn cleanup_partial(&mut self) {
        self.file.take();
        let _ = fs::remove_file(self.cache_paths.part_path.as_path());
    }
}

pub fn build_artifact_download_cache_path(
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
        final_path: ClientPath::new(final_path),
        part_path: ClientPath::new(part_path),
    })
}

pub fn prune_artifact_download_cache(runtime_home: &Path, max_age: Duration) -> Result<u64> {
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

pub fn sha256_file(path: &Path) -> Result<String> {
    let bytes = fs::read(path).with_context(|| format!("failed to read `{}`", path.display()))?;
    Ok(sha256_bytes(bytes.as_slice()))
}

pub fn verify_artifact_download_file(
    path: &Path,
    expected_size_bytes: u64,
    expected_sha256: &str,
) -> Result<()> {
    let actual_size = fs::metadata(path)
        .with_context(|| {
            format!(
                "failed to stat artifact download part file {}",
                path.display()
            )
        })?
        .len();
    if actual_size != expected_size_bytes {
        return Err(anyhow!(
            "artifact download size mismatch: expected {}, got {}",
            expected_size_bytes,
            actual_size
        ));
    }
    let actual_sha256 = sha256_file(path)?;
    if actual_sha256 != expected_sha256 {
        return Err(anyhow!("artifact download sha256 mismatch"));
    }
    Ok(())
}

pub fn download_artifact_to_cache<TTransport, TCache>(
    transport: &TTransport,
    cache: &TCache,
    request: ArtifactDownloadRequest,
) -> Result<ArtifactDownloadResult>
where
    TTransport: ArtifactDownloadTransport,
    TCache: ArtifactDownloadCache,
{
    validate_artifact_download_request(&request)?;
    let _ = cache.prune();

    let start = transport.artifact_download_start(ArtifactDownloadStartParams {
        workspace_id: request.workspace_id.clone(),
        artifact_id: request.artifact_id.clone(),
        version_id: request.version_id.clone(),
        preferred_chunk_size_bytes: None,
    })?;
    let version_id = start
        .artifact
        .version_id
        .clone()
        .or(request.version_id.clone())
        .unwrap_or_else(|| "latest".to_owned());
    let mut sink = cache.create_sink(&request, &start, version_id.as_str())?;
    let result = download_artifact_chunks_and_finish(transport, &request, &start, &mut sink);
    if result.is_err() {
        let _ = transport.artifact_download_abort(ArtifactDownloadAbortParams {
            workspace_id: request.workspace_id,
            download_id: start.download_id,
        });
        sink.cleanup_partial();
    }
    result
}

fn validate_artifact_download_request(request: &ArtifactDownloadRequest) -> Result<()> {
    if request.gateway_profile_id.trim().is_empty() {
        return Err(anyhow!(
            "gateway_profile_id is required for artifact download"
        ));
    }
    if request.workspace_id.trim().is_empty() {
        return Err(anyhow!("workspace_id is required for artifact download"));
    }
    if request.artifact_id.trim().is_empty() {
        return Err(anyhow!("artifact_id is required for artifact download"));
    }
    Ok(())
}

fn download_artifact_chunks_and_finish<TTransport, TSink>(
    transport: &TTransport,
    request: &ArtifactDownloadRequest,
    start: &ArtifactDownloadStartResponse,
    sink: &mut TSink,
) -> Result<ArtifactDownloadResult>
where
    TTransport: ArtifactDownloadTransport,
    TSink: ArtifactDownloadSink,
{
    sink.prepare()?;

    let mut offset = 0_u64;
    let chunk_size = start
        .recommended_chunk_size_bytes
        .min(start.max_chunk_size_bytes)
        .max(1);
    let version_id = start
        .artifact
        .version_id
        .as_deref()
        .or(request.version_id.as_deref())
        .unwrap_or("latest");
    while offset < start.size_bytes {
        let len = (start.size_bytes - offset).min(chunk_size);
        let waiter =
            transport.register_artifact_download_chunk(start.download_id.as_str(), offset)?;
        let response = transport.artifact_download_chunk(ArtifactDownloadChunkParams {
            workspace_id: request.workspace_id.clone(),
            download_id: start.download_id.clone(),
            offset,
            len,
        })?;
        if !response.queued || response.offset != offset || response.len != len {
            return Err(anyhow!("artifact/download/chunk returned an invalid range"));
        }
        let payload = waiter.recv_timeout(ARTIFACT_DOWNLOAD_CHUNK_TIMEOUT)?;
        validate_artifact_download_chunk_payload(
            &payload,
            request.workspace_id.as_str(),
            start.download_id.as_str(),
            request.artifact_id.as_str(),
            version_id,
            offset,
            len,
            start.size_bytes,
        )?;
        sink.write_chunk(offset, payload.bytes.as_slice())?;
        offset = offset.saturating_add(len);
    }

    let local_path = sink.finalize()?;
    transport.artifact_download_finish(ArtifactDownloadFinishParams {
        workspace_id: request.workspace_id.clone(),
        download_id: start.download_id.clone(),
    })?;
    Ok(ArtifactDownloadResult {
        local_path,
        artifact: start.artifact.clone(),
        size_bytes: start.size_bytes,
        sha256: start.sha256.clone(),
    })
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

fn write_chunk_at(file: &mut fs::File, offset: u64, bytes: &[u8]) -> Result<()> {
    use std::io::{Seek, SeekFrom, Write};

    file.seek(SeekFrom::Start(offset))
        .context("failed to seek artifact download part file")?;
    file.write_all(bytes)
        .context("failed to write artifact download chunk")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::ws::frames::sha256_bytes;
    use pioneer_protocol::{ArtifactDownloadChunkHeader, ArtifactKind, ArtifactStatus};
    use std::sync::{Arc, Mutex};

    #[test]
    fn artifacts_download_to_cache_writes_chunks_and_finishes() {
        let transport = FakeDownloadTransport::new(b"hello world".to_vec());
        let cache = FakeDownloadCache::default();

        let result = download_artifact_to_cache(
            &transport,
            &cache,
            ArtifactDownloadRequest {
                gateway_profile_id: "gateway_1".to_owned(),
                workspace_id: "ws_1".to_owned(),
                artifact_id: "artifact_1".to_owned(),
                version_id: Some("version_1".to_owned()),
            },
        )
        .expect("download artifact");

        assert_eq!(
            result.local_path.as_path().to_string_lossy(),
            "/cache/artifact"
        );
        assert_eq!(cache.bytes(), b"hello world");
        assert_eq!(transport.chunk_requests(), vec![(0, 5), (5, 5), (10, 1)]);
        assert_eq!(transport.finished(), vec!["download_1".to_owned()]);
        assert!(transport.aborted().is_empty());
        assert!(cache.cleaned_partials().is_empty());
    }

    #[test]
    fn artifacts_download_to_cache_aborts_and_cleans_partial_on_wait_failure() {
        let transport = FakeDownloadTransport::new(b"hello".to_vec());
        transport.fail_waiter("download interrupted");
        let cache = FakeDownloadCache::default();

        let error = download_artifact_to_cache(
            &transport,
            &cache,
            ArtifactDownloadRequest {
                gateway_profile_id: "gateway_1".to_owned(),
                workspace_id: "ws_1".to_owned(),
                artifact_id: "artifact_1".to_owned(),
                version_id: Some("version_1".to_owned()),
            },
        )
        .expect_err("wait failure should abort");

        assert!(error.to_string().contains("download interrupted"));
        assert_eq!(transport.aborted(), vec!["download_1".to_owned()]);
        assert!(transport.finished().is_empty());
        assert_eq!(cache.cleaned_partials(), vec![true]);
    }

    #[test]
    fn artifacts_file_cache_download_writes_verified_file() {
        let temp = tempfile::tempdir().expect("temp dir");
        let transport = FakeDownloadTransport::new(b"hello world".to_vec());
        let cache = ArtifactDownloadFileCache::new(temp.path());

        let result = download_artifact_to_cache(
            &transport,
            &cache,
            ArtifactDownloadRequest {
                gateway_profile_id: "gateway_1".to_owned(),
                workspace_id: "ws_1".to_owned(),
                artifact_id: "artifact_1".to_owned(),
                version_id: Some("version_1".to_owned()),
            },
        )
        .expect("download artifact");

        assert_eq!(
            fs::read(result.local_path.as_path()).expect("read downloaded artifact"),
            b"hello world"
        );
        verify_artifact_download_file(
            result.local_path.as_path(),
            b"hello world".len() as u64,
            sha256_bytes(b"hello world").as_str(),
        )
        .expect("verified artifact file");
        assert!(transport.aborted().is_empty());
    }

    #[test]
    fn artifacts_file_cache_removes_partial_on_download_failure() {
        let temp = tempfile::tempdir().expect("temp dir");
        let transport = FakeDownloadTransport::new(b"hello".to_vec());
        transport.fail_waiter("download interrupted");
        let cache = ArtifactDownloadFileCache::new(temp.path());
        let paths = build_artifact_download_cache_path(
            temp.path(),
            "gateway_1",
            "ws_1",
            "artifact_1",
            "version_1",
            "artifact.txt",
        )
        .expect("cache path");

        let error = download_artifact_to_cache(
            &transport,
            &cache,
            ArtifactDownloadRequest {
                gateway_profile_id: "gateway_1".to_owned(),
                workspace_id: "ws_1".to_owned(),
                artifact_id: "artifact_1".to_owned(),
                version_id: Some("version_1".to_owned()),
            },
        )
        .expect_err("wait failure should abort");

        assert!(error.to_string().contains("download interrupted"));
        assert_eq!(transport.aborted(), vec!["download_1".to_owned()]);
        assert!(!paths.part_path.as_path().exists());
        assert!(!paths.final_path.as_path().exists());
    }

    #[test]
    fn artifacts_download_cache_path_stays_under_runtime_home() {
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

        assert!(paths.final_path.as_path().starts_with(temp.path()));
        assert!(paths.part_path.as_path().starts_with(temp.path()));
        assert_eq!(
            paths
                .final_path
                .as_path()
                .file_name()
                .and_then(|value| value.to_str()),
            Some("_report.txt")
        );
    }

    #[test]
    fn artifacts_download_cache_prune_removes_expired_files() {
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
        fs::create_dir_all(paths.final_path.as_path().parent().expect("parent"))
            .expect("create cache dir");
        fs::write(paths.final_path.as_path(), b"cached").expect("write cache file");

        let removed =
            prune_artifact_download_cache(temp.path(), Duration::ZERO).expect("prune cache");

        assert_eq!(removed, 1);
        assert!(!paths.final_path.as_path().exists());
    }

    #[test]
    fn artifacts_download_verify_file_rejects_size_or_sha_mismatch() {
        let temp = tempfile::tempdir().expect("temp dir");
        let path = temp.path().join("artifact.txt");
        fs::write(path.as_path(), b"hello").expect("write artifact");
        let sha = sha256_bytes(b"hello");

        verify_artifact_download_file(path.as_path(), 5, sha.as_str()).expect("valid artifact");
        assert!(verify_artifact_download_file(path.as_path(), 6, sha.as_str()).is_err());
        assert!(verify_artifact_download_file(path.as_path(), 5, "0").is_err());
    }

    #[derive(Clone)]
    struct FakeDownloadTransport {
        state: Arc<Mutex<FakeDownloadTransportState>>,
    }

    struct FakeDownloadTransportState {
        bytes: Vec<u8>,
        chunk_requests: Vec<(u64, u64)>,
        finished: Vec<String>,
        aborted: Vec<String>,
        waiter_error: Option<String>,
    }

    impl FakeDownloadTransport {
        fn new(bytes: Vec<u8>) -> Self {
            Self {
                state: Arc::new(Mutex::new(FakeDownloadTransportState {
                    bytes,
                    chunk_requests: Vec::new(),
                    finished: Vec::new(),
                    aborted: Vec::new(),
                    waiter_error: None,
                })),
            }
        }

        fn fail_waiter(&self, error: &str) {
            self.state.lock().expect("lock state").waiter_error = Some(error.to_owned());
        }

        fn chunk_requests(&self) -> Vec<(u64, u64)> {
            self.state
                .lock()
                .expect("lock state")
                .chunk_requests
                .clone()
        }

        fn finished(&self) -> Vec<String> {
            self.state.lock().expect("lock state").finished.clone()
        }

        fn aborted(&self) -> Vec<String> {
            self.state.lock().expect("lock state").aborted.clone()
        }
    }

    impl ArtifactDownloadTransport for FakeDownloadTransport {
        fn artifact_download_start(
            &self,
            _params: ArtifactDownloadStartParams,
        ) -> Result<ArtifactDownloadStartResponse> {
            let bytes = self.state.lock().expect("lock state").bytes.clone();
            Ok(ArtifactDownloadStartResponse {
                download_id: "download_1".to_owned(),
                artifact: artifact_ref(bytes.len() as u64, sha256_bytes(bytes.as_slice())),
                file_name: "artifact.txt".to_owned(),
                size_bytes: bytes.len() as u64,
                sha256: sha256_bytes(bytes.as_slice()),
                recommended_chunk_size_bytes: 5,
                max_chunk_size_bytes: 5,
                expires_at_unix: 0,
            })
        }

        fn register_artifact_download_chunk(
            &self,
            _download_id: &str,
            offset: u64,
        ) -> Result<Box<dyn ArtifactDownloadChunkWaiter>> {
            let state = self.state.lock().expect("lock state");
            if let Some(error) = state.waiter_error.clone() {
                return Ok(Box::new(FakeDownloadChunkWaiter {
                    payload: Err(error),
                }));
            }
            let len = (state.bytes.len() as u64 - offset).min(5);
            let chunk = state.bytes[offset as usize..(offset + len) as usize].to_vec();
            Ok(Box::new(FakeDownloadChunkWaiter {
                payload: Ok(download_payload(offset, state.bytes.len() as u64, chunk)),
            }))
        }

        fn artifact_download_chunk(
            &self,
            params: ArtifactDownloadChunkParams,
        ) -> Result<ArtifactDownloadChunkResponse> {
            self.state
                .lock()
                .expect("lock state")
                .chunk_requests
                .push((params.offset, params.len));
            Ok(ArtifactDownloadChunkResponse {
                download_id: params.download_id,
                offset: params.offset,
                len: params.len,
                queued: true,
            })
        }

        fn artifact_download_finish(
            &self,
            params: ArtifactDownloadFinishParams,
        ) -> Result<ArtifactDownloadFinishResponse> {
            self.state
                .lock()
                .expect("lock state")
                .finished
                .push(params.download_id.clone());
            Ok(ArtifactDownloadFinishResponse {
                download_id: params.download_id,
                status: "finished".to_owned(),
            })
        }

        fn artifact_download_abort(
            &self,
            params: ArtifactDownloadAbortParams,
        ) -> Result<ArtifactDownloadAbortResponse> {
            self.state
                .lock()
                .expect("lock state")
                .aborted
                .push(params.download_id.clone());
            Ok(ArtifactDownloadAbortResponse {
                download_id: params.download_id,
                status: "aborted".to_owned(),
            })
        }
    }

    struct FakeDownloadChunkWaiter {
        payload: std::result::Result<ArtifactDownloadChunkPayload, String>,
    }

    impl ArtifactDownloadChunkWaiter for FakeDownloadChunkWaiter {
        fn recv_timeout(&self, _timeout: Duration) -> Result<ArtifactDownloadChunkPayload> {
            self.payload.clone().map_err(anyhow::Error::msg)
        }
    }

    #[derive(Default)]
    struct FakeDownloadCache {
        state: Arc<Mutex<FakeDownloadCacheState>>,
    }

    #[derive(Default)]
    struct FakeDownloadCacheState {
        bytes: Vec<u8>,
        cleaned_partials: Vec<bool>,
    }

    impl FakeDownloadCache {
        fn bytes(&self) -> Vec<u8> {
            self.state.lock().expect("lock state").bytes.clone()
        }

        fn cleaned_partials(&self) -> Vec<bool> {
            self.state
                .lock()
                .expect("lock state")
                .cleaned_partials
                .clone()
        }
    }

    impl ArtifactDownloadCache for FakeDownloadCache {
        type Sink = FakeDownloadSink;

        fn prune(&self) -> Result<()> {
            Ok(())
        }

        fn create_sink(
            &self,
            _request: &ArtifactDownloadRequest,
            _start: &ArtifactDownloadStartResponse,
            _version_id: &str,
        ) -> Result<Self::Sink> {
            Ok(FakeDownloadSink {
                state: self.state.clone(),
            })
        }
    }

    struct FakeDownloadSink {
        state: Arc<Mutex<FakeDownloadCacheState>>,
    }

    impl ArtifactDownloadSink for FakeDownloadSink {
        fn prepare(&mut self) -> Result<()> {
            self.state.lock().expect("lock state").bytes.clear();
            Ok(())
        }

        fn write_chunk(&mut self, offset: u64, bytes: &[u8]) -> Result<()> {
            let mut state = self.state.lock().expect("lock state");
            if state.bytes.len() != offset as usize {
                return Err(anyhow!("unexpected offset"));
            }
            state.bytes.extend_from_slice(bytes);
            Ok(())
        }

        fn finalize(&mut self) -> Result<ClientPath> {
            Ok(ClientPath::new("/cache/artifact"))
        }

        fn cleanup_partial(&mut self) {
            self.state
                .lock()
                .expect("lock state")
                .cleaned_partials
                .push(true);
        }
    }

    fn download_payload(
        offset: u64,
        total_size_bytes: u64,
        bytes: Vec<u8>,
    ) -> ArtifactDownloadChunkPayload {
        ArtifactDownloadChunkPayload {
            header: ArtifactDownloadChunkHeader {
                workspace_id: "ws_1".to_owned(),
                download_id: "download_1".to_owned(),
                artifact_id: "artifact_1".to_owned(),
                version_id: "version_1".to_owned(),
                offset,
                len: bytes.len() as u64,
                total_size_bytes,
                chunk_sha256: sha256_bytes(bytes.as_slice()),
                final_chunk: offset + bytes.len() as u64 == total_size_bytes,
            },
            bytes,
        }
    }

    fn artifact_ref(size_bytes: u64, sha256: String) -> ArtifactRef {
        ArtifactRef {
            artifact_id: "artifact_1".to_owned(),
            version_id: Some("version_1".to_owned()),
            display_name: "artifact.txt".to_owned(),
            kind: ArtifactKind::File,
            mime_type: Some("text/plain".to_owned()),
            size_bytes: Some(size_bytes),
            sha256: Some(sha256),
            status: ArtifactStatus::Ready,
            preview: None,
        }
    }
}
