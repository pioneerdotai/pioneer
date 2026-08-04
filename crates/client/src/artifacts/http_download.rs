//! Native explicit artifact downloads over the authenticated Gateway HTTP edge.
//!
//! The resumable state is an internal cache implementation detail. It contains
//! only immutable artifact identity, integrity metadata and the representation
//! ETag; session credentials never cross this boundary.
//!
//! State machine and ownership:
//!
//! - a verified immutable final file is reused without network or overwrite;
//! - an invalid final file is moved out of the publish path before downloading;
//! - a partial file is resumed only when its persisted identity, size and ETag
//!   match a fresh authenticated HEAD response;
//! - changed/malformed integrity state restarts from an empty owned partial;
//! - cancellation and transport failure keep a matching partial for retry;
//! - length/SHA failure removes the partial, and only a verified partial is
//!   atomically renamed to the final path.

use std::{
    collections::HashMap,
    fmt,
    io::Read as _,
    path::{Path, PathBuf},
    sync::{Arc, Mutex as StdMutex, OnceLock, Weak},
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncReadExt as _, AsyncWriteExt as _, BufWriter},
    sync::Mutex,
};
use tokio_util::sync::CancellationToken;

use crate::{
    artifacts::download::{
        ArtifactHttpDownloadCachePaths, build_artifact_http_download_cache_path,
    },
    local_file::{
        configure_std_no_follow, configure_tokio_no_follow, ensure_owned_directory,
        metadata_is_plain_file,
    },
    platform::ClientPath,
    transport::http::{
        GatewayHttpError, GatewayHttpRequest, GatewayHttpResponse, GatewayHttpSession,
    },
};

const PARTIAL_METADATA_VERSION: u8 = 1;
const MAX_ID_BYTES: usize = 512;
const MAX_DISPLAY_NAME_BYTES: usize = 1_024;
const MAX_PARTIAL_METADATA_BYTES: u64 = 16 * 1024;

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactHttpDownloadRequest {
    pub gateway_profile_id: String,
    pub workspace_id: String,
    pub artifact_id: String,
    pub version_id: String,
    pub display_name: String,
    pub expected_size_bytes: u64,
    pub expected_sha256: String,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactHttpDownloadResult {
    pub local_path: ClientPath,
    pub artifact_id: String,
    pub version_id: String,
    pub size_bytes: u64,
    pub sha256: String,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactHttpDownloadError {
    InvalidRequest,
    Authentication,
    RevokedOrUnavailable,
    Transport,
    InvalidResponse,
    Integrity,
    DiskFull,
    DiskWrite,
    Cancelled,
}

impl ArtifactHttpDownloadError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidRequest => "invalid_request",
            Self::Authentication => "authentication_failed",
            Self::RevokedOrUnavailable => "artifact_revoked_or_unavailable",
            Self::Transport => "transport_failed",
            Self::InvalidResponse => "invalid_response",
            Self::Integrity => "integrity_failed",
            Self::DiskFull => "disk_full",
            Self::DiskWrite => "disk_write_failed",
            Self::Cancelled => "cancelled",
        }
    }
}

impl fmt::Display for ArtifactHttpDownloadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code())
    }
}

impl std::error::Error for ArtifactHttpDownloadError {}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactHttpDownloadProgress {
    pub downloaded_bytes: u64,
    pub total_bytes: u64,
    pub resumed_from_bytes: u64,
}

pub trait ArtifactHttpDownloadProgressSink: Send + Sync {
    fn on_progress(&self, progress: ArtifactHttpDownloadProgress);
}

impl<F> ArtifactHttpDownloadProgressSink for F
where
    F: Fn(ArtifactHttpDownloadProgress) + Send + Sync,
{
    fn on_progress(&self, progress: ArtifactHttpDownloadProgress) {
        self(progress);
    }
}

#[async_trait]
trait ArtifactDownloadHttp: Send + Sync {
    async fn execute(
        &self,
        request: GatewayHttpRequest,
        cancellation: CancellationToken,
    ) -> Result<GatewayHttpResponse, GatewayHttpError>;
}

#[async_trait]
impl ArtifactDownloadHttp for GatewayHttpSession {
    async fn execute(
        &self,
        request: GatewayHttpRequest,
        cancellation: CancellationToken,
    ) -> Result<GatewayHttpResponse, GatewayHttpError> {
        GatewayHttpSession::execute(self, request, cancellation).await
    }
}

#[derive(Clone)]
pub struct ArtifactHttpDownloadService {
    http: Arc<dyn ArtifactDownloadHttp>,
    runtime_home: PathBuf,
}

impl fmt::Debug for ArtifactHttpDownloadService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ArtifactHttpDownloadService")
            .field("runtime_home", &"[owned-cache]")
            .finish_non_exhaustive()
    }
}

impl ArtifactHttpDownloadService {
    pub fn new(http: GatewayHttpSession, runtime_home: impl Into<PathBuf>) -> Self {
        Self {
            http: Arc::new(http),
            runtime_home: runtime_home.into(),
        }
    }

    pub async fn download(
        &self,
        request: ArtifactHttpDownloadRequest,
        cancellation: CancellationToken,
        progress: Option<&dyn ArtifactHttpDownloadProgressSink>,
    ) -> Result<ArtifactHttpDownloadResult, ArtifactHttpDownloadError> {
        let request = ValidatedRequest::new(request)?;
        let paths = OwnedDownloadPaths::new(self.runtime_home.as_path(), &request)?;
        let operation_gate = download_gate_for(paths.cache.final_path.as_path());
        let _operation = operation_gate.lock().await;
        ensure_not_cancelled(&cancellation)?;
        ensure_owned_directory(self.runtime_home.as_path(), paths.parent())
            .await
            .map_err(map_disk_error)?;

        if verified_file(paths.cache.final_path.as_path(), &request).await? {
            cleanup_partial(&paths).await;
            return Ok(request.result(paths.cache.final_path));
        }
        quarantine_invalid_final(&paths, &request).await?;

        let storage_path = request.storage_path();
        let representation = match self
            .head_representation(storage_path.as_str(), &request, cancellation.clone())
            .await
        {
            Ok(representation) => representation,
            Err(error) => {
                if invalidates_partial(error) {
                    cleanup_partial(&paths).await;
                }
                return Err(error);
            }
        };
        let mut resume_from = load_resume_offset(&paths, &request, &representation).await?;
        if resume_from == request.expected_size_bytes && resume_from > 0 {
            return finalize_complete_partial(paths, request).await;
        }

        let mut response = match self
            .get_representation(
                storage_path.as_str(),
                resume_from,
                &representation.etag,
                cancellation.clone(),
            )
            .await
        {
            Ok(response) => response,
            Err(error) => {
                if invalidates_partial(error) {
                    cleanup_partial(&paths).await;
                }
                return Err(error);
            }
        };
        let response_mode =
            validate_get_response(&response, &request, &representation, resume_from);
        let response_mode = match response_mode {
            Ok(mode) => mode,
            Err(error) => {
                cleanup_partial(&paths).await;
                return Err(error);
            }
        };
        if response_mode == DownloadResponseMode::RestartedFull {
            cleanup_partial(&paths).await;
            resume_from = 0;
        }

        let file = open_partial_file(&paths, resume_from).await?;
        let mut writer = BufWriter::new(file);
        let mut downloaded = resume_from;
        let resumed_from_bytes = resume_from;
        notify_progress(progress, downloaded, &request, resumed_from_bytes);

        while let Some(chunk) = response.body.next_chunk().await {
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(error) => return Err(map_http_error(error)),
            };
            ensure_not_cancelled(&cancellation)?;
            let chunk_len = u64::try_from(chunk.len())
                .map_err(|_| ArtifactHttpDownloadError::InvalidResponse)?;
            let next = downloaded
                .checked_add(chunk_len)
                .ok_or(ArtifactHttpDownloadError::InvalidResponse)?;
            if next > request.expected_size_bytes {
                drop(writer);
                cleanup_partial(&paths).await;
                return Err(ArtifactHttpDownloadError::InvalidResponse);
            }
            if let Err(error) = writer.write_all(chunk.as_slice()).await {
                drop(writer);
                restore_partial_length(&paths, downloaded).await;
                return Err(map_disk_error(error));
            }
            if let Err(error) = writer.flush().await {
                drop(writer);
                restore_partial_length(&paths, downloaded).await;
                return Err(map_disk_error(error));
            }
            downloaded = next;
            let metadata = PartialDownloadMetadata::new(&request, &representation.etag, downloaded);
            if let Err(error) = persist_partial_metadata(&paths, &metadata).await {
                drop(writer);
                restore_partial_length(&paths, downloaded.saturating_sub(chunk_len)).await;
                return Err(error);
            }
            notify_progress(progress, downloaded, &request, resumed_from_bytes);
        }

        ensure_not_cancelled(&cancellation)?;
        if downloaded != request.expected_size_bytes {
            drop(writer);
            cleanup_partial(&paths).await;
            return Err(ArtifactHttpDownloadError::InvalidResponse);
        }
        writer.flush().await.map_err(map_disk_error)?;
        writer.get_ref().sync_all().await.map_err(map_disk_error)?;
        drop(writer);
        finalize_complete_partial(paths, request).await
    }

    async fn head_representation(
        &self,
        storage_path: &str,
        request: &ValidatedRequest,
        cancellation: CancellationToken,
    ) -> Result<CurrentRepresentation, ArtifactHttpDownloadError> {
        let response = self
            .http
            .execute(
                GatewayHttpRequest::head(storage_path.to_owned()).map_err(map_http_error)?,
                cancellation,
            )
            .await
            .map_err(map_http_error)?;
        if response.head.status != 200
            || response.head.content_length != Some(request.expected_size_bytes)
            || response.head.etag.as_deref() != Some(request.expected_etag.as_str())
        {
            return Err(integrity_failure("head_metadata_mismatch"));
        }
        Ok(CurrentRepresentation {
            etag: request.expected_etag.clone(),
        })
    }

    async fn get_representation(
        &self,
        storage_path: &str,
        resume_from: u64,
        etag: &str,
        cancellation: CancellationToken,
    ) -> Result<GatewayHttpResponse, ArtifactHttpDownloadError> {
        let mut request =
            GatewayHttpRequest::get(storage_path.to_owned()).map_err(map_http_error)?;
        if resume_from > 0 {
            request = request
                .with_range(format!("bytes={resume_from}-"))
                .and_then(|request| request.with_if_range(etag.to_owned()))
                .map_err(map_http_error)?;
        }
        self.http
            .execute(request, cancellation)
            .await
            .map_err(map_http_error)
    }

    #[cfg(test)]
    fn with_http(http: Arc<dyn ArtifactDownloadHttp>, runtime_home: impl Into<PathBuf>) -> Self {
        Self {
            http,
            runtime_home: runtime_home.into(),
        }
    }
}

fn download_gate_for(target: &Path) -> Arc<Mutex<()>> {
    static GATES: OnceLock<StdMutex<HashMap<PathBuf, Weak<Mutex<()>>>>> = OnceLock::new();
    let gates = GATES.get_or_init(|| StdMutex::new(HashMap::new()));
    let mut gates = gates
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    gates.retain(|_, gate| gate.strong_count() > 0);
    if let Some(gate) = gates.get(target).and_then(Weak::upgrade) {
        return gate;
    }
    let gate = Arc::new(Mutex::new(()));
    gates.insert(target.to_owned(), Arc::downgrade(&gate));
    gate
}

#[derive(Clone)]
struct ValidatedRequest {
    gateway_profile_id: String,
    workspace_id: String,
    artifact_id: String,
    version_id: String,
    display_name: String,
    expected_size_bytes: u64,
    expected_sha256: String,
    expected_etag: String,
}

impl ValidatedRequest {
    fn new(request: ArtifactHttpDownloadRequest) -> Result<Self, ArtifactHttpDownloadError> {
        for value in [request.gateway_profile_id.as_str()] {
            if value.trim().is_empty() || value.len() > MAX_ID_BYTES || value.contains('\0') {
                return Err(ArtifactHttpDownloadError::InvalidRequest);
            }
        }
        for value in [
            request.workspace_id.as_str(),
            request.artifact_id.as_str(),
            request.version_id.as_str(),
        ] {
            if !valid_storage_path_segment(value) {
                return Err(ArtifactHttpDownloadError::InvalidRequest);
            }
        }
        if request.display_name.trim().is_empty()
            || request.display_name.len() > MAX_DISPLAY_NAME_BYTES
            || request.display_name.contains('\0')
        {
            return Err(ArtifactHttpDownloadError::InvalidRequest);
        }
        let expected_sha256 = request.expected_sha256.to_ascii_lowercase();
        if expected_sha256.len() != 64
            || !expected_sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(ArtifactHttpDownloadError::InvalidRequest);
        }
        Ok(Self {
            gateway_profile_id: request.gateway_profile_id,
            workspace_id: request.workspace_id,
            artifact_id: request.artifact_id,
            version_id: request.version_id,
            display_name: request.display_name,
            expected_size_bytes: request.expected_size_bytes,
            expected_etag: format!("\"sha256-{expected_sha256}\""),
            expected_sha256,
        })
    }

    fn storage_path(&self) -> String {
        format!(
            "storage/workspaces/{}/artifacts/{}/versions/{}/content",
            self.workspace_id, self.artifact_id, self.version_id,
        )
    }

    fn result(&self, local_path: ClientPath) -> ArtifactHttpDownloadResult {
        ArtifactHttpDownloadResult {
            local_path,
            artifact_id: self.artifact_id.clone(),
            version_id: self.version_id.clone(),
            size_bytes: self.expected_size_bytes,
            sha256: self.expected_sha256.clone(),
        }
    }
}

struct OwnedDownloadPaths {
    cache: ArtifactHttpDownloadCachePaths,
    metadata: PathBuf,
    metadata_temp: PathBuf,
}

impl OwnedDownloadPaths {
    fn new(
        runtime_home: &Path,
        request: &ValidatedRequest,
    ) -> Result<Self, ArtifactHttpDownloadError> {
        let cache = build_artifact_http_download_cache_path(
            runtime_home,
            request.gateway_profile_id.as_str(),
            request.workspace_id.as_str(),
            request.artifact_id.as_str(),
            request.version_id.as_str(),
            request.display_name.as_str(),
        )
        .map_err(|_| ArtifactHttpDownloadError::InvalidRequest)?;
        let metadata = append_file_suffix(cache.part_path.as_path(), ".json")?;
        let metadata_temp = append_file_suffix(cache.part_path.as_path(), ".json.tmp")?;
        Ok(Self {
            cache,
            metadata,
            metadata_temp,
        })
    }

    fn parent(&self) -> &Path {
        self.cache
            .part_path
            .as_path()
            .parent()
            .expect("owned download path always has a parent")
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct PartialDownloadMetadata {
    schema_version: u8,
    workspace_id: String,
    artifact_id: String,
    version_id: String,
    expected_size_bytes: u64,
    expected_sha256: String,
    etag: String,
    downloaded_bytes: u64,
}

impl PartialDownloadMetadata {
    fn new(request: &ValidatedRequest, etag: &str, downloaded_bytes: u64) -> Self {
        Self {
            schema_version: PARTIAL_METADATA_VERSION,
            workspace_id: request.workspace_id.clone(),
            artifact_id: request.artifact_id.clone(),
            version_id: request.version_id.clone(),
            expected_size_bytes: request.expected_size_bytes,
            expected_sha256: request.expected_sha256.clone(),
            etag: etag.to_owned(),
            downloaded_bytes,
        }
    }

    fn matches(
        &self,
        request: &ValidatedRequest,
        representation: &CurrentRepresentation,
        file_length: u64,
    ) -> bool {
        self.schema_version == PARTIAL_METADATA_VERSION
            && self.workspace_id == request.workspace_id
            && self.artifact_id == request.artifact_id
            && self.version_id == request.version_id
            && self.expected_size_bytes == request.expected_size_bytes
            && self.expected_sha256 == request.expected_sha256
            && self.etag == representation.etag
            && self.downloaded_bytes == file_length
            && file_length <= request.expected_size_bytes
    }
}

struct CurrentRepresentation {
    etag: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DownloadResponseMode {
    FreshFull,
    ResumedPartial,
    RestartedFull,
}

fn validate_get_response(
    response: &GatewayHttpResponse,
    request: &ValidatedRequest,
    representation: &CurrentRepresentation,
    resume_from: u64,
) -> Result<DownloadResponseMode, ArtifactHttpDownloadError> {
    if response.head.etag.as_deref() != Some(representation.etag.as_str()) {
        return Err(integrity_failure("get_etag_mismatch"));
    }
    if resume_from == 0 {
        return if response.head.status == 200
            && response.head.content_length == Some(request.expected_size_bytes)
            && response.head.content_range.is_none()
        {
            Ok(DownloadResponseMode::FreshFull)
        } else {
            Err(ArtifactHttpDownloadError::InvalidResponse)
        };
    }
    if response.head.status == 200
        && response.head.content_length == Some(request.expected_size_bytes)
        && response.head.content_range.is_none()
    {
        return Ok(DownloadResponseMode::RestartedFull);
    }
    let remaining = request.expected_size_bytes.saturating_sub(resume_from);
    let expected_content_range = format!(
        "bytes {}-{}/{}",
        resume_from,
        request.expected_size_bytes.saturating_sub(1),
        request.expected_size_bytes
    );
    if response.head.status == 206
        && response.head.content_length == Some(remaining)
        && response.head.content_range.as_deref() == Some(expected_content_range.as_str())
    {
        Ok(DownloadResponseMode::ResumedPartial)
    } else {
        Err(ArtifactHttpDownloadError::InvalidResponse)
    }
}

async fn load_resume_offset(
    paths: &OwnedDownloadPaths,
    request: &ValidatedRequest,
    representation: &CurrentRepresentation,
) -> Result<u64, ArtifactHttpDownloadError> {
    let part_length = match fs::symlink_metadata(paths.cache.part_path.as_path()).await {
        Ok(metadata) if metadata_is_plain_file(&metadata) => Some(metadata.len()),
        Ok(_) => {
            cleanup_partial(paths).await;
            return Ok(0);
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(map_disk_error(error)),
    };
    let metadata = match read_owned_metadata(paths.metadata.as_path()).await {
        Ok(Some(bytes)) => serde_json::from_slice::<PartialDownloadMetadata>(bytes.as_slice()).ok(),
        Ok(None) => None,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(map_disk_error(error)),
    };
    match (part_length, metadata) {
        (Some(file_length), Some(metadata))
            if metadata.matches(request, representation, file_length) =>
        {
            Ok(file_length)
        }
        (None, None) => Ok(0),
        _ => {
            cleanup_partial(paths).await;
            Ok(0)
        }
    }
}

async fn open_partial_file(
    paths: &OwnedDownloadPaths,
    resume_from: u64,
) -> Result<File, ArtifactHttpDownloadError> {
    let mut options = OpenOptions::new();
    options.write(true);
    if resume_from == 0 {
        options.create_new(true);
    } else {
        let metadata = fs::symlink_metadata(paths.cache.part_path.as_path())
            .await
            .map_err(map_disk_error)?;
        if !metadata_is_plain_file(&metadata) {
            return Err(ArtifactHttpDownloadError::DiskWrite);
        }
        options.append(true);
    }
    configure_tokio_no_follow(&mut options);
    let file = options
        .open(paths.cache.part_path.as_path())
        .await
        .map_err(map_disk_error)?;
    let opened_metadata = file.metadata().await.map_err(map_disk_error)?;
    if !metadata_is_plain_file(&opened_metadata)
        || (resume_from > 0 && opened_metadata.len() != resume_from)
    {
        return Err(ArtifactHttpDownloadError::DiskWrite);
    }
    Ok(file)
}

async fn persist_partial_metadata(
    paths: &OwnedDownloadPaths,
    metadata: &PartialDownloadMetadata,
) -> Result<(), ArtifactHttpDownloadError> {
    let bytes = serde_json::to_vec(metadata).map_err(|_| ArtifactHttpDownloadError::DiskWrite)?;
    match fs::symlink_metadata(paths.metadata_temp.as_path()).await {
        Ok(metadata) if metadata.is_file() || metadata.file_type().is_symlink() => {
            fs::remove_file(paths.metadata_temp.as_path())
                .await
                .map_err(map_disk_error)?;
        }
        Ok(_) => return Err(ArtifactHttpDownloadError::DiskWrite),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(map_disk_error(error)),
    }
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    configure_tokio_no_follow(&mut options);
    let mut file = options
        .open(paths.metadata_temp.as_path())
        .await
        .map_err(map_disk_error)?;
    if !metadata_is_plain_file(&file.metadata().await.map_err(map_disk_error)?) {
        return Err(ArtifactHttpDownloadError::DiskWrite);
    }
    file.write_all(bytes.as_slice())
        .await
        .map_err(map_disk_error)?;
    file.sync_all().await.map_err(map_disk_error)?;
    drop(file);
    if fs::rename(paths.metadata_temp.as_path(), paths.metadata.as_path())
        .await
        .is_err()
    {
        let _ = fs::remove_file(paths.metadata.as_path()).await;
        fs::rename(paths.metadata_temp.as_path(), paths.metadata.as_path())
            .await
            .map_err(map_disk_error)?;
    }
    sync_parent_directory(paths.parent()).await?;
    Ok(())
}

async fn restore_partial_length(paths: &OwnedDownloadPaths, length: u64) {
    let Ok(metadata) = fs::symlink_metadata(paths.cache.part_path.as_path()).await else {
        return;
    };
    if !metadata_is_plain_file(&metadata) {
        return;
    }
    let mut options = OpenOptions::new();
    options.write(true);
    configure_tokio_no_follow(&mut options);
    if let Ok(file) = options.open(paths.cache.part_path.as_path()).await {
        if file
            .metadata()
            .await
            .is_ok_and(|metadata| metadata_is_plain_file(&metadata))
        {
            let _ = file.set_len(length).await;
            let _ = file.sync_all().await;
        }
    }
}

async fn cleanup_partial(paths: &OwnedDownloadPaths) {
    for path in [
        paths.cache.part_path.as_path(),
        paths.metadata.as_path(),
        paths.metadata_temp.as_path(),
    ] {
        let _ = fs::remove_file(path).await;
    }
}

async fn quarantine_invalid_final(
    paths: &OwnedDownloadPaths,
    request: &ValidatedRequest,
) -> Result<(), ArtifactHttpDownloadError> {
    match fs::symlink_metadata(paths.cache.final_path.as_path()).await {
        Ok(metadata) if metadata_is_plain_file(&metadata) => {
            if verified_file(paths.cache.final_path.as_path(), request).await? {
                return Ok(());
            }
        }
        Ok(metadata) if metadata.file_type().is_symlink() => {}
        Ok(_) => return Err(ArtifactHttpDownloadError::DiskWrite),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(map_disk_error(error)),
    }
    fs::remove_file(paths.cache.final_path.as_path())
        .await
        .map_err(map_disk_error)?;
    sync_parent_directory(paths.parent()).await
}

async fn finalize_complete_partial(
    paths: OwnedDownloadPaths,
    request: ValidatedRequest,
) -> Result<ArtifactHttpDownloadResult, ArtifactHttpDownloadError> {
    let mut options = OpenOptions::new();
    options.write(true);
    configure_tokio_no_follow(&mut options);
    let file = options
        .open(paths.cache.part_path.as_path())
        .await
        .map_err(map_disk_error)?;
    if !metadata_is_plain_file(&file.metadata().await.map_err(map_disk_error)?) {
        return Err(ArtifactHttpDownloadError::DiskWrite);
    }
    file.sync_all().await.map_err(map_disk_error)?;
    if !verified_file(paths.cache.part_path.as_path(), &request).await? {
        cleanup_partial(&paths).await;
        return Err(integrity_failure("downloaded_content_mismatch"));
    }
    if path_is_regular_file(paths.cache.final_path.as_path()).await? {
        if verified_file(paths.cache.final_path.as_path(), &request).await? {
            cleanup_partial(&paths).await;
            return Ok(request.result(paths.cache.final_path));
        }
        quarantine_invalid_final(&paths, &request).await?;
    }
    match fs::hard_link(
        paths.cache.part_path.as_path(),
        paths.cache.final_path.as_path(),
    )
    .await
    {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            if verified_file(paths.cache.final_path.as_path(), &request).await? {
                cleanup_partial(&paths).await;
                return Ok(request.result(paths.cache.final_path));
            }
            return Err(ArtifactHttpDownloadError::DiskWrite);
        }
        Err(error) => return Err(map_disk_error(error)),
    }
    // Verification and publication are separate pathname operations. Recheck
    // the published inode before returning it so a local pathname swap cannot
    // turn a verified partial into an unverified cache hit.
    if !verified_file(paths.cache.final_path.as_path(), &request).await? {
        let _ = fs::remove_file(paths.cache.final_path.as_path()).await;
        cleanup_partial(&paths).await;
        sync_parent_directory(paths.parent()).await?;
        return Err(integrity_failure("published_content_mismatch"));
    }
    fs::remove_file(paths.cache.part_path.as_path())
        .await
        .map_err(map_disk_error)?;
    let _ = fs::remove_file(paths.metadata.as_path()).await;
    let _ = fs::remove_file(paths.metadata_temp.as_path()).await;
    sync_parent_directory(paths.parent()).await?;
    Ok(request.result(paths.cache.final_path))
}

#[cfg(unix)]
async fn sync_parent_directory(parent: &Path) -> Result<(), ArtifactHttpDownloadError> {
    File::open(parent)
        .await
        .map_err(map_disk_error)?
        .sync_all()
        .await
        .map_err(map_disk_error)
}

#[cfg(not(unix))]
async fn sync_parent_directory(_parent: &Path) -> Result<(), ArtifactHttpDownloadError> {
    Ok(())
}

async fn verified_file(
    path: &Path,
    request: &ValidatedRequest,
) -> Result<bool, ArtifactHttpDownloadError> {
    if !path_is_regular_file(path).await? {
        return Ok(false);
    }
    let path = path.to_owned();
    let expected_size = request.expected_size_bytes;
    let expected_sha = request.expected_sha256.clone();
    tokio::task::spawn_blocking(move || {
        verify_file_sync(path.as_path(), expected_size, &expected_sha)
    })
    .await
    .map_err(|_| ArtifactHttpDownloadError::DiskWrite)?
    .map_err(map_disk_error)
}

fn verify_file_sync(path: &Path, expected_size: u64, expected_sha: &str) -> std::io::Result<bool> {
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    configure_std_no_follow(&mut options);
    let mut file = options.open(path)?;
    let metadata = file.metadata()?;
    if !metadata_is_plain_file(&metadata) || metadata.len() != expected_size {
        return Ok(false);
    }
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex::encode(hasher.finalize()) == expected_sha)
}

async fn path_is_regular_file(path: &Path) -> Result<bool, ArtifactHttpDownloadError> {
    match fs::symlink_metadata(path).await {
        Ok(metadata) => Ok(metadata_is_plain_file(&metadata)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(map_disk_error(error)),
    }
}

async fn read_owned_metadata(path: &Path) -> std::io::Result<Option<Vec<u8>>> {
    match fs::symlink_metadata(path).await {
        Ok(metadata)
            if metadata_is_plain_file(&metadata)
                && metadata.len() <= MAX_PARTIAL_METADATA_BYTES => {}
        Ok(_) => return Ok(None),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    }
    let mut options = OpenOptions::new();
    options.read(true);
    configure_tokio_no_follow(&mut options);
    let file = options.open(path).await?;
    let opened_metadata = file.metadata().await?;
    if !metadata_is_plain_file(&opened_metadata)
        || opened_metadata.len() > MAX_PARTIAL_METADATA_BYTES
    {
        return Ok(None);
    }
    let mut bytes = Vec::with_capacity(opened_metadata.len() as usize);
    file.take(MAX_PARTIAL_METADATA_BYTES + 1)
        .read_to_end(&mut bytes)
        .await?;
    if bytes.len() as u64 > MAX_PARTIAL_METADATA_BYTES {
        return Ok(None);
    }
    Ok(Some(bytes))
}

fn notify_progress(
    sink: Option<&dyn ArtifactHttpDownloadProgressSink>,
    downloaded_bytes: u64,
    request: &ValidatedRequest,
    resumed_from_bytes: u64,
) {
    if let Some(sink) = sink {
        sink.on_progress(ArtifactHttpDownloadProgress {
            downloaded_bytes: downloaded_bytes.min(request.expected_size_bytes),
            total_bytes: request.expected_size_bytes,
            resumed_from_bytes,
        });
    }
}

fn ensure_not_cancelled(cancellation: &CancellationToken) -> Result<(), ArtifactHttpDownloadError> {
    if cancellation.is_cancelled() {
        Err(ArtifactHttpDownloadError::Cancelled)
    } else {
        Ok(())
    }
}

fn invalidates_partial(error: ArtifactHttpDownloadError) -> bool {
    matches!(
        error,
        ArtifactHttpDownloadError::InvalidRequest
            | ArtifactHttpDownloadError::InvalidResponse
            | ArtifactHttpDownloadError::Integrity
    )
}

fn integrity_failure(reason_code: &'static str) -> ArtifactHttpDownloadError {
    tracing::warn!(
        event = "artifact_download_integrity_failure",
        outcome = "rejected",
        reason_code,
    );
    ArtifactHttpDownloadError::Integrity
}

fn map_http_error(error: GatewayHttpError) -> ArtifactHttpDownloadError {
    match error {
        GatewayHttpError::AuthenticationTerminal(_)
        | GatewayHttpError::AuthenticationUnavailable
        | GatewayHttpError::Unauthorized => ArtifactHttpDownloadError::Authentication,
        GatewayHttpError::Forbidden | GatewayHttpError::NotFound => {
            ArtifactHttpDownloadError::RevokedOrUnavailable
        }
        GatewayHttpError::Cancelled => ArtifactHttpDownloadError::Cancelled,
        GatewayHttpError::Transport
        | GatewayHttpError::ServiceUnavailable
        | GatewayHttpError::TooManyRequests
        | GatewayHttpError::Server => ArtifactHttpDownloadError::Transport,
        GatewayHttpError::InvalidEndpoint
        | GatewayHttpError::InvalidStoragePath
        | GatewayHttpError::InvalidHeader
        | GatewayHttpError::GatewayPinMismatch
        | GatewayHttpError::SessionMismatch => ArtifactHttpDownloadError::InvalidRequest,
        GatewayHttpError::InvalidResponse
        | GatewayHttpError::Conflict
        | GatewayHttpError::RangeNotSatisfiable => ArtifactHttpDownloadError::InvalidResponse,
    }
}

fn map_disk_error(error: std::io::Error) -> ArtifactHttpDownloadError {
    if matches!(error.raw_os_error(), Some(28 | 112)) {
        ArtifactHttpDownloadError::DiskFull
    } else {
        ArtifactHttpDownloadError::DiskWrite
    }
}

pub(crate) fn valid_storage_path_segment(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_ID_BYTES
        && !matches!(value, "." | "..")
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~'))
}

fn append_file_suffix(path: &Path, suffix: &str) -> Result<PathBuf, ArtifactHttpDownloadError> {
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or(ArtifactHttpDownloadError::InvalidRequest)?;
    Ok(path.with_file_name(format!("{file_name}{suffix}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex as StdMutex},
    };

    use crate::transport::http::{GatewayHttpBody, GatewayHttpMethod, GatewayHttpResponseHead};

    #[derive(Clone)]
    struct FakeHttp {
        responses: Arc<StdMutex<VecDeque<FakeResponse>>>,
        requests: Arc<StdMutex<Vec<RecordedRequest>>>,
    }

    struct FakeResponse {
        head: GatewayHttpResponseHead,
        chunks: Vec<Result<Vec<u8>, GatewayHttpError>>,
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct RecordedRequest {
        method: GatewayHttpMethod,
        path: String,
        range: Option<String>,
        if_range: Option<String>,
    }

    impl FakeHttp {
        fn new(responses: Vec<FakeResponse>) -> Self {
            Self {
                responses: Arc::new(StdMutex::new(responses.into())),
                requests: Arc::new(StdMutex::new(Vec::new())),
            }
        }

        fn requests(&self) -> Vec<RecordedRequest> {
            self.requests.lock().expect("request lock").clone()
        }
    }

    #[async_trait]
    impl ArtifactDownloadHttp for FakeHttp {
        async fn execute(
            &self,
            request: GatewayHttpRequest,
            cancellation: CancellationToken,
        ) -> Result<GatewayHttpResponse, GatewayHttpError> {
            self.requests
                .lock()
                .expect("request lock")
                .push(RecordedRequest {
                    method: request.method(),
                    path: request.storage_path().to_owned(),
                    range: request.range().map(ToOwned::to_owned),
                    if_range: request.if_range().map(ToOwned::to_owned),
                });
            let response = self
                .responses
                .lock()
                .expect("response lock")
                .pop_front()
                .expect("fake response");
            Ok(GatewayHttpResponse {
                head: response.head,
                body: GatewayHttpBody::from_test_chunks(response.chunks, cancellation),
            })
        }
    }

    #[tokio::test]
    async fn success_streams_verifies_and_atomically_publishes() {
        let temp = tempfile::tempdir().expect("temp");
        let bytes = b"hello artifact";
        let request = request(bytes);
        let http = Arc::new(FakeHttp::new(vec![
            head(bytes),
            full(bytes, vec![b"hello ", b"artifact"]),
        ]));
        let service = ArtifactHttpDownloadService::with_http(http.clone(), temp.path());
        let progress = StdMutex::new(Vec::new());

        let result = service
            .download(
                request.clone(),
                CancellationToken::new(),
                Some(&|value| progress.lock().expect("progress lock").push(value)),
            )
            .await
            .expect("download");

        assert_eq!(
            fs::read(result.local_path.as_path()).await.expect("read"),
            bytes
        );
        assert!(result.local_path.as_path().starts_with(temp.path()));
        assert_ne!(
            result
                .local_path
                .as_path()
                .file_name()
                .and_then(|name| name.to_str()),
            Some("../report?.txt")
        );
        let paths = owned_paths(temp.path(), &request);
        assert!(!paths.cache.part_path.as_path().exists());
        assert!(!paths.metadata.exists());
        assert_eq!(http.requests().len(), 2);
        assert_eq!(http.requests()[0].method, GatewayHttpMethod::Head);
        assert_eq!(http.requests()[1].method, GatewayHttpMethod::Get);
        assert_eq!(
            http.requests()[1].path,
            "storage/workspaces/workspace-one/artifacts/artifact-one/versions/version-one/content"
        );
        let updates = progress.lock().expect("progress lock");
        assert_eq!(
            updates.last().expect("progress").downloaded_bytes,
            bytes.len() as u64
        );
    }

    #[tokio::test]
    async fn resume_requires_matching_metadata_and_uses_if_range() {
        let temp = tempfile::tempdir().expect("temp");
        let bytes = b"hello artifact";
        let request = request(bytes);
        seed_partial(temp.path(), &request, &bytes[..6], etag(bytes)).await;
        let http = Arc::new(FakeHttp::new(vec![
            head(bytes),
            partial(bytes, 6, vec![bytes[6..].to_vec()]),
        ]));
        let service = ArtifactHttpDownloadService::with_http(http.clone(), temp.path());

        let result = service
            .download(request, CancellationToken::new(), None)
            .await
            .expect("resume");

        assert_eq!(
            fs::read(result.local_path.as_path()).await.expect("read"),
            bytes
        );
        let requests = http.requests();
        assert_eq!(requests[1].range.as_deref(), Some("bytes=6-"));
        assert_eq!(requests[1].if_range.as_deref(), Some(etag(bytes).as_str()));
    }

    #[tokio::test]
    async fn changed_etag_discards_partial_and_restarts_full() {
        let temp = tempfile::tempdir().expect("temp");
        let bytes = b"current bytes";
        let request = request(bytes);
        seed_partial(
            temp.path(),
            &request,
            b"stale",
            "\"sha256-stale\"".to_owned(),
        )
        .await;
        let http = Arc::new(FakeHttp::new(vec![head(bytes), full(bytes, vec![bytes])]));
        let service = ArtifactHttpDownloadService::with_http(http.clone(), temp.path());

        service
            .download(request, CancellationToken::new(), None)
            .await
            .expect("restart");

        assert_eq!(http.requests()[1].range, None);
    }

    #[tokio::test]
    async fn short_and_long_bodies_never_publish_final_file() {
        for (name, response) in [
            ("short", full(b"hello", vec![b"hell".as_slice()])),
            ("long", full(b"hello", vec![b"hello!".as_slice()])),
        ] {
            let temp = tempfile::tempdir().expect("temp");
            let request = request(b"hello");
            let service = ArtifactHttpDownloadService::with_http(
                Arc::new(FakeHttp::new(vec![head(b"hello"), response])),
                temp.path(),
            );
            let error = service
                .download(request.clone(), CancellationToken::new(), None)
                .await
                .expect_err(name);
            assert_eq!(error, ArtifactHttpDownloadError::InvalidResponse);
            assert!(
                !owned_paths(temp.path(), &request)
                    .cache
                    .final_path
                    .as_path()
                    .exists()
            );
        }
    }

    #[tokio::test]
    async fn sha_mismatch_removes_partial_and_never_publishes() {
        let temp = tempfile::tempdir().expect("temp");
        let mut request = request(b"hello");
        request.expected_sha256 = sha256(b"other");
        let expected_etag = format!("\"sha256-{}\"", request.expected_sha256);
        let service = ArtifactHttpDownloadService::with_http(
            Arc::new(FakeHttp::new(vec![
                response_head(200, Some(5), Some(expected_etag.clone()), None, vec![]),
                response_head(
                    200,
                    Some(5),
                    Some(expected_etag),
                    None,
                    vec![Ok(b"hello".to_vec())],
                ),
            ])),
            temp.path(),
        );

        let error = service
            .download(request.clone(), CancellationToken::new(), None)
            .await
            .expect_err("sha mismatch");

        assert_eq!(error, ArtifactHttpDownloadError::Integrity);
        let paths = owned_paths(temp.path(), &request);
        assert!(!paths.cache.final_path.as_path().exists());
        assert!(!paths.cache.part_path.as_path().exists());
        assert!(!paths.metadata.exists());
    }

    #[tokio::test]
    async fn cancellation_preserves_matching_recoverable_partial() {
        let temp = tempfile::tempdir().expect("temp");
        let bytes = b"hello artifact";
        let request = request(bytes);
        let cancellation = CancellationToken::new();
        let cancel_after_first = cancellation.clone();
        let service = ArtifactHttpDownloadService::with_http(
            Arc::new(FakeHttp::new(vec![
                head(bytes),
                full(bytes, vec![b"hello ", b"artifact"]),
            ])),
            temp.path(),
        );

        let error = service
            .download(
                request.clone(),
                cancellation,
                Some(&move |progress: ArtifactHttpDownloadProgress| {
                    if progress.downloaded_bytes == 6 {
                        cancel_after_first.cancel();
                    }
                }),
            )
            .await
            .expect_err("cancelled");

        assert_eq!(error, ArtifactHttpDownloadError::Cancelled);
        let paths = owned_paths(temp.path(), &request);
        assert_eq!(
            fs::metadata(paths.cache.part_path.as_path())
                .await
                .expect("part")
                .len(),
            6
        );
        assert!(paths.metadata.exists());
        assert!(!paths.cache.final_path.as_path().exists());
    }

    #[tokio::test]
    async fn disk_failure_is_typed_and_contains_no_path() {
        let temp = tempfile::tempdir().expect("temp");
        let blocked_root = temp.path().join("not-a-directory");
        fs::write(blocked_root.as_path(), b"file")
            .await
            .expect("seed file");
        let service = ArtifactHttpDownloadService::with_http(
            Arc::new(FakeHttp::new(Vec::new())),
            blocked_root,
        );

        let error = service
            .download(request(b"hello"), CancellationToken::new(), None)
            .await
            .expect_err("disk failure");

        assert_eq!(error, ArtifactHttpDownloadError::DiskWrite);
        assert_eq!(error.to_string(), "disk_write_failed");
    }

    #[tokio::test]
    async fn verified_existing_final_is_reused_without_http_or_overwrite() {
        let temp = tempfile::tempdir().expect("temp");
        let bytes = b"already complete";
        let request = request(bytes);
        let paths = owned_paths(temp.path(), &request);
        fs::create_dir_all(paths.parent()).await.expect("parent");
        fs::write(paths.cache.final_path.as_path(), bytes)
            .await
            .expect("final");
        let http = Arc::new(FakeHttp::new(Vec::new()));
        let service = ArtifactHttpDownloadService::with_http(http.clone(), temp.path());

        let result = service
            .download(request, CancellationToken::new(), None)
            .await
            .expect("reuse");

        assert_eq!(
            fs::read(result.local_path.as_path()).await.expect("read"),
            bytes
        );
        assert!(http.requests().is_empty());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn final_cache_symlink_is_replaced_without_following_its_target() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().expect("temp");
        let bytes = b"downloaded bytes";
        let request = request(bytes);
        let paths = owned_paths(temp.path(), &request);
        fs::create_dir_all(paths.parent()).await.expect("parent");
        let outside = temp.path().join("outside.txt");
        fs::write(outside.as_path(), b"do not overwrite")
            .await
            .expect("outside file");
        symlink(outside.as_path(), paths.cache.final_path.as_path()).expect("cache symlink");
        let service = ArtifactHttpDownloadService::with_http(
            Arc::new(FakeHttp::new(vec![head(bytes), full(bytes, vec![bytes])])),
            temp.path(),
        );

        let result = service
            .download(request, CancellationToken::new(), None)
            .await
            .expect("download");

        assert_eq!(fs::read(result.local_path.as_path()).await.unwrap(), bytes);
        assert_eq!(
            fs::read(outside.as_path()).await.unwrap(),
            b"do not overwrite"
        );
        assert!(
            !fs::symlink_metadata(result.local_path.as_path())
                .await
                .unwrap()
                .file_type()
                .is_symlink()
        );
    }

    #[test]
    fn error_mapping_preserves_auth_revoke_disk_full_and_secret_free_codes() {
        assert_eq!(
            map_http_error(GatewayHttpError::Unauthorized),
            ArtifactHttpDownloadError::Authentication
        );
        assert_eq!(
            map_http_error(GatewayHttpError::Forbidden),
            ArtifactHttpDownloadError::RevokedOrUnavailable
        );
        assert_eq!(
            map_http_error(GatewayHttpError::TooManyRequests),
            ArtifactHttpDownloadError::Transport
        );
        assert_eq!(
            map_http_error(GatewayHttpError::Server),
            ArtifactHttpDownloadError::Transport
        );
        assert_eq!(
            map_http_error(GatewayHttpError::Conflict),
            ArtifactHttpDownloadError::InvalidResponse
        );
        assert_eq!(
            map_disk_error(std::io::Error::from_raw_os_error(28)),
            ArtifactHttpDownloadError::DiskFull
        );
        for error in [
            ArtifactHttpDownloadError::Authentication,
            ArtifactHttpDownloadError::RevokedOrUnavailable,
            ArtifactHttpDownloadError::DiskFull,
        ] {
            let diagnostic = format!("{error:?} {error}");
            assert!(!diagnostic.contains("Bearer"));
            assert!(!diagnostic.contains("Authorization"));
        }
    }

    #[test]
    fn operation_gates_are_shared_only_by_the_exact_owned_target() {
        let root = Path::new("/owned/runtime/downloads");
        let first = download_gate_for(&root.join("artifact-a"));
        let same = download_gate_for(&root.join("artifact-a"));
        let other = download_gate_for(&root.join("artifact-b"));

        assert!(Arc::ptr_eq(&first, &same));
        assert!(!Arc::ptr_eq(&first, &other));
    }

    #[tokio::test]
    async fn partial_metadata_contains_integrity_state_but_no_credential_fields() {
        let temp = tempfile::tempdir().expect("temp");
        let request = request(b"hello");
        seed_partial(temp.path(), &request, b"he", etag(b"hello")).await;
        let metadata = fs::read_to_string(owned_paths(temp.path(), &request).metadata)
            .await
            .expect("metadata");

        assert!(metadata.contains("expected_sha256"));
        assert!(metadata.contains("etag"));
        assert!(!metadata.to_ascii_lowercase().contains("authorization"));
        assert!(!metadata.to_ascii_lowercase().contains("access_token"));
        assert!(!metadata.contains("Bearer"));
    }

    fn request(bytes: &[u8]) -> ArtifactHttpDownloadRequest {
        ArtifactHttpDownloadRequest {
            gateway_profile_id: "gateway/one".to_owned(),
            workspace_id: "workspace-one".to_owned(),
            artifact_id: "artifact-one".to_owned(),
            version_id: "version-one".to_owned(),
            display_name: "../report?.txt".to_owned(),
            expected_size_bytes: bytes.len() as u64,
            expected_sha256: sha256(bytes),
        }
    }

    fn head(bytes: &[u8]) -> FakeResponse {
        response_head(
            200,
            Some(bytes.len() as u64),
            Some(etag(bytes)),
            None,
            vec![],
        )
    }

    fn full(bytes: &[u8], chunks: Vec<&[u8]>) -> FakeResponse {
        response_head(
            200,
            Some(bytes.len() as u64),
            Some(etag(bytes)),
            None,
            chunks.into_iter().map(|chunk| Ok(chunk.to_vec())).collect(),
        )
    }

    fn partial(bytes: &[u8], offset: u64, chunks: Vec<Vec<u8>>) -> FakeResponse {
        response_head(
            206,
            Some(bytes.len() as u64 - offset),
            Some(etag(bytes)),
            Some(format!(
                "bytes {offset}-{}/{}",
                bytes.len() - 1,
                bytes.len()
            )),
            chunks.into_iter().map(Ok).collect(),
        )
    }

    fn response_head(
        status: u16,
        content_length: Option<u64>,
        etag: Option<String>,
        content_range: Option<String>,
        chunks: Vec<Result<Vec<u8>, GatewayHttpError>>,
    ) -> FakeResponse {
        FakeResponse {
            head: GatewayHttpResponseHead {
                status,
                request_id: Some("request-test".to_owned()),
                etag,
                content_length,
                content_range,
                content_type: Some("text/plain".to_owned()),
                content_disposition: Some("inline; filename=report.txt".to_owned()),
            },
            chunks,
        }
    }

    fn sha256(bytes: &[u8]) -> String {
        hex::encode(Sha256::digest(bytes))
    }

    fn etag(bytes: &[u8]) -> String {
        format!("\"sha256-{}\"", sha256(bytes))
    }

    fn owned_paths(
        runtime_home: &Path,
        request: &ArtifactHttpDownloadRequest,
    ) -> OwnedDownloadPaths {
        OwnedDownloadPaths::new(
            runtime_home,
            &ValidatedRequest::new(request.clone()).expect("validated request"),
        )
        .expect("owned paths")
    }

    async fn seed_partial(
        runtime_home: &Path,
        request: &ArtifactHttpDownloadRequest,
        bytes: &[u8],
        partial_etag: String,
    ) {
        let validated = ValidatedRequest::new(request.clone()).expect("validated request");
        let paths = OwnedDownloadPaths::new(runtime_home, &validated).expect("paths");
        fs::create_dir_all(paths.parent()).await.expect("parent");
        fs::write(paths.cache.part_path.as_path(), bytes)
            .await
            .expect("partial");
        persist_partial_metadata(
            &paths,
            &PartialDownloadMetadata::new(&validated, partial_etag.as_str(), bytes.len() as u64),
        )
        .await
        .expect("metadata");
    }
}
