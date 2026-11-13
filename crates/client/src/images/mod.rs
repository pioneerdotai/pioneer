//! Shared authenticated image delivery and immutable cache.
//!
//! Cache entries are immutable, revision-addressed and contain no credentials.
//! Authorization remains owned by `GatewayHttpSession`; shell boundaries only
//! receive an owned local image path and validated representation metadata.

use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock, Weak};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use pioneer_protocol::{AuthSessionId, GatewayId, IMAGE_MAX_BYTES};
pub use pioneer_protocol::{ImageMediaType, ImageReference};
use sha2::{Digest, Sha256};
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::local_file::{
    configure_tokio_no_follow, ensure_owned_directory, metadata_is_plain_file,
    remove_owned_directory,
};
use crate::platform::ClientPath;
use crate::transport::http::{
    GatewayHttpError, GatewayHttpRequest, GatewayHttpResponse, GatewayHttpSession,
};

const IMAGE_CACHE_MAX_AGE: Duration = Duration::from_secs(30 * 24 * 60 * 60);
const IMAGE_CACHE_MAX_FILES: usize = 2_048;

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ImageCacheSource {
    Downloaded,
    Revalidated,
    OfflineCache,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ImageCacheError {
    InvalidRequest,
    Authentication,
    HiddenOrMissing,
    Offline,
    InvalidResponse,
    Corrupt,
    Disk,
    Cancelled,
}

impl ImageCacheError {
    // Existing avatar callers expose these codes; preserve them at the shared boundary.
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidRequest => "avatar_invalid_request",
            Self::Authentication => "avatar_authentication_required",
            Self::HiddenOrMissing => "avatar_hidden_or_missing",
            Self::Offline => "avatar_offline",
            Self::InvalidResponse => "avatar_invalid_response",
            Self::Corrupt => "avatar_cache_corrupt",
            Self::Disk => "avatar_cache_disk_failed",
            Self::Cancelled => "avatar_cancelled",
        }
    }
}

impl fmt::Display for ImageCacheError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code())
    }
}

impl std::error::Error for ImageCacheError {}

#[async_trait]
trait ImageHttp: Send + Sync {
    async fn execute(
        &self,
        request: GatewayHttpRequest,
        cancellation: CancellationToken,
    ) -> Result<GatewayHttpResponse, GatewayHttpError>;
}

#[async_trait]
impl ImageHttp for GatewayHttpSession {
    async fn execute(
        &self,
        request: GatewayHttpRequest,
        cancellation: CancellationToken,
    ) -> Result<GatewayHttpResponse, GatewayHttpError> {
        GatewayHttpSession::execute(self, request, cancellation).await
    }
}

#[derive(Clone)]
pub struct ImageCacheService {
    http: Arc<dyn ImageHttp>,
    runtime_home: PathBuf,
    gateway_id: GatewayId,
    session_id: AuthSessionId,
}

impl fmt::Debug for ImageCacheService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ImageCacheService")
            .field("gateway_id", &self.gateway_id)
            .field("runtime_home", &"[owned-private-cache]")
            .finish_non_exhaustive()
    }
}

impl ImageCacheService {
    pub fn new(
        http: GatewayHttpSession,
        runtime_home: impl Into<PathBuf>,
        gateway_id: GatewayId,
        session_id: AuthSessionId,
    ) -> Self {
        Self {
            http: Arc::new(http),
            runtime_home: runtime_home.into(),
            gateway_id,
            session_id,
        }
    }

    pub async fn resolve_image(
        &self,
        request: ImageCacheRequest,
        cancellation: CancellationToken,
    ) -> Result<ImageCacheResult, ImageCacheError> {
        let representation = self.resolve_validated(&request, cancellation).await?;
        Ok(ImageCacheResult {
            local_path: representation.local_path,
            revision: request.revision,
            media_type: representation.media_type,
            source: representation.source,
        })
    }

    async fn resolve_validated(
        &self,
        request: &ImageCacheRequest,
        cancellation: CancellationToken,
    ) -> Result<ResolvedImageRepresentation, ImageCacheError> {
        let paths = ImageCachePaths::new(
            self.runtime_home.as_path(),
            &self.gateway_id,
            &self.session_id,
            request,
        )?;
        let operation_gate = image_cache_gate_for(paths.gateway_root.as_path());
        let _operation = operation_gate.lock().await;
        ensure_not_cancelled(&cancellation)?;
        ensure_owned_directory(self.runtime_home.as_path(), paths.parent())
            .await
            .map_err(|_| ImageCacheError::Disk)?;
        let cached_media_type = verify_cached(paths.final_path.as_path(), request).await?;
        if cached_media_type.is_none() {
            remove_owned_file(paths.final_path.as_path()).await;
        }

        let mut native_request = GatewayHttpRequest::get(request.storage_path())
            .map_err(|_| ImageCacheError::InvalidRequest)?;
        if cached_media_type.is_some() {
            native_request = native_request
                .with_if_none_match(request.etag())
                .map_err(|_| ImageCacheError::InvalidRequest)?;
        }
        let response = self
            .http
            .execute(native_request, cancellation.clone())
            .await;
        ensure_not_cancelled(&cancellation)?;
        let mut response = match response {
            Ok(response) => response,
            Err(error) => {
                let mapped = map_http_error(error);
                if invalidates_cache(mapped) {
                    remove_owned_file(paths.final_path.as_path()).await;
                }
                if let Some(media_type) = cached_media_type
                    && mapped == ImageCacheError::Offline
                {
                    return Ok(ResolvedImageRepresentation {
                        local_path: paths.final_path,
                        media_type,
                        source: ImageCacheSource::OfflineCache,
                    });
                }
                return Err(mapped);
            }
        };

        if response.head.status == 304 {
            if cached_media_type.is_none()
                || response.head.etag.as_deref() != Some(request.etag().as_str())
            {
                remove_owned_file(paths.final_path.as_path()).await;
                return Err(ImageCacheError::InvalidResponse);
            }
            return Ok(ResolvedImageRepresentation {
                local_path: paths.final_path,
                media_type: cached_media_type
                    .expect("304 requires a verified cached representation"),
                source: ImageCacheSource::Revalidated,
            });
        }
        let media_type = validate_response_head(&response, request)?;
        let expected_length = response
            .head
            .content_length
            .ok_or(ImageCacheError::InvalidResponse)?;
        let mut bytes = Vec::with_capacity(expected_length as usize);
        while let Some(chunk) = response.body.next_chunk().await {
            ensure_not_cancelled(&cancellation)?;
            let chunk = chunk.map_err(map_http_error)?;
            if bytes.len().saturating_add(chunk.len()) > IMAGE_MAX_BYTES {
                return Err(ImageCacheError::InvalidResponse);
            }
            bytes.extend_from_slice(chunk.as_slice());
        }
        if bytes.len() as u64 != expected_length || sha256_hex(bytes.as_slice()) != request.revision
        {
            return Err(ImageCacheError::Corrupt);
        }
        if detect_image_media_type(bytes.as_slice()) != Some(media_type) {
            return Err(ImageCacheError::Corrupt);
        }
        ensure_not_cancelled(&cancellation)?;
        persist_atomically(&paths, bytes.as_slice()).await?;
        if cancellation.is_cancelled() {
            remove_owned_file(paths.final_path.as_path()).await;
            return Err(ImageCacheError::Cancelled);
        }
        let _ = prune_image_cache(self.runtime_home.as_path()).await;
        ensure_not_cancelled(&cancellation)?;
        Ok(ResolvedImageRepresentation {
            local_path: paths.final_path,
            media_type,
            source: ImageCacheSource::Downloaded,
        })
    }

    pub async fn invalidate_gateway(&self) -> Result<(), ImageCacheError> {
        let gateway_root =
            image_cache_root(self.runtime_home.as_path()).join(self.gateway_id.as_str());
        let operation_gate = image_cache_gate_for(gateway_root.as_path());
        let _operation = operation_gate.lock().await;
        let runtime_home = self.runtime_home.clone();
        let gateway_id = self.gateway_id.clone();
        pioneer_observability::work::spawn_blocking(move || {
            remove_owned_directory(
                runtime_home.as_path(),
                image_cache_root(runtime_home.as_path())
                    .join(gateway_id.as_str())
                    .as_path(),
            )
        })
        .await
        .map_err(|_| ImageCacheError::Disk)?
        .map_err(|_| ImageCacheError::Disk)
    }

    #[cfg(test)]
    fn with_http(
        http: Arc<dyn ImageHttp>,
        runtime_home: impl Into<PathBuf>,
        gateway_id: GatewayId,
        session_id: AuthSessionId,
    ) -> Self {
        Self {
            http,
            runtime_home: runtime_home.into(),
            gateway_id,
            session_id,
        }
    }
}

pub fn invalidate_image_cache(runtime_home: &Path) -> Result<(), ImageCacheError> {
    remove_owned_directory(
        runtime_home,
        runtime_home.join("cache").join("avatars").as_path(),
    )
    .map_err(|_| ImageCacheError::Disk)
}

/// An immutable image in the authenticated native storage namespace.
#[derive(Clone, Debug)]
pub struct ImageCacheRequest {
    resource_id: String,
    path: String,
    revision: String,
}
impl ImageCacheRequest {
    pub fn new(image: ImageReference) -> Result<Self, ImageCacheError> {
        let path = pioneer_protocol::canonical_storage_path(&image.path)
            .map_err(|_| ImageCacheError::InvalidRequest)?;
        GatewayHttpRequest::get(path.clone()).map_err(|_| ImageCacheError::InvalidRequest)?;
        if !valid_image_revision(&image.revision) {
            return Err(ImageCacheError::InvalidRequest);
        }
        Ok(Self {
            resource_id: path.clone(),
            path,
            revision: image.revision,
        })
    }
    pub fn reference(&self) -> ImageReference {
        ImageReference {
            path: self.path.clone(),
            revision: self.revision.clone(),
        }
    }
    pub fn resource_id(&self) -> &str {
        &self.resource_id
    }
    pub fn revision(&self) -> &str {
        &self.revision
    }
    pub fn key(&self) -> String {
        image_identity_key(&self.resource_id, &self.revision)
    }
    pub(crate) fn with_resource_id(mut self, resource_id: String) -> Self {
        self.resource_id = resource_id;
        self
    }
    fn storage_path(&self) -> String {
        self.path.clone()
    }
    fn etag(&self) -> String {
        format!("\"{}\"", self.revision)
    }
    fn cache_parent(&self, session_root: PathBuf) -> PathBuf {
        // Preserve existing verified cache locations for the established storage routes.
        let parts: Vec<_> = self.path.split('/').collect();
        match parts.as_slice() {
            ["storage", "members", principal, "avatar", _] if safe_cache_segment(principal) => {
                session_root.join("members").join(principal)
            }
            ["storage", "system", "agent", "avatar", _] => {
                session_root.join("system").join("agent")
            }
            [
                "storage",
                "workspaces",
                workspace,
                "plugins",
                plugin,
                "icons",
                _,
            ] if safe_cache_segment(workspace) && safe_cache_segment(plugin) => {
                session_root.join("plugins").join(workspace).join(plugin)
            }
            _ => session_root
                .join("resources")
                .join(sha256_hex(self.path.as_bytes())),
        }
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ImageCacheResult {
    pub local_path: ClientPath,
    pub revision: String,
    pub media_type: ImageMediaType,
    pub source: ImageCacheSource,
}

fn safe_cache_segment(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn valid_image_revision(revision: &str) -> bool {
    revision.len() == 64
        && revision
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

struct ResolvedImageRepresentation {
    local_path: ClientPath,
    media_type: ImageMediaType,
    source: ImageCacheSource,
}

struct ImageCachePaths {
    gateway_root: PathBuf,
    final_path: ClientPath,
    part_path: PathBuf,
}

impl ImageCachePaths {
    fn new(
        runtime_home: &Path,
        gateway_id: &GatewayId,
        session_id: &AuthSessionId,
        request: &ImageCacheRequest,
    ) -> Result<Self, ImageCacheError> {
        static NEXT_PART_ID: AtomicU64 = AtomicU64::new(1);
        let gateway_root = image_cache_root(runtime_home).join(gateway_id.as_str());
        let session_root = gateway_root.join("sessions").join(session_id.as_str());
        let parent = request.cache_parent(session_root);
        let final_path = parent.join(format!("{}.avatar", request.revision));
        let part_id = NEXT_PART_ID.fetch_add(1, Ordering::Relaxed);
        let part_path = parent.join(format!(
            ".{}.{}.{}.avatar.part",
            request.revision,
            std::process::id(),
            part_id,
        ));
        if !final_path.starts_with(runtime_home) || !part_path.starts_with(runtime_home) {
            return Err(ImageCacheError::InvalidRequest);
        }
        Ok(Self {
            gateway_root,
            final_path: ClientPath::new(final_path),
            part_path,
        })
    }

    fn parent(&self) -> &Path {
        self.final_path
            .as_path()
            .parent()
            .expect("owned image cache path has a parent")
    }
}

fn image_cache_root(runtime_home: &Path) -> PathBuf {
    // Retain the established on-disk layout so verified entries remain usable after this refactor.
    runtime_home.join("cache").join("avatars").join("gateways")
}

fn image_cache_gate_for(gateway_root: &Path) -> Arc<Mutex<()>> {
    static GATES: OnceLock<StdMutex<HashMap<PathBuf, Weak<Mutex<()>>>>> = OnceLock::new();
    let gates = GATES.get_or_init(|| StdMutex::new(HashMap::new()));
    let mut gates = gates
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    gates.retain(|_, gate| gate.strong_count() > 0);
    if let Some(gate) = gates.get(gateway_root).and_then(Weak::upgrade) {
        return gate;
    }
    let gate = Arc::new(Mutex::new(()));
    gates.insert(gateway_root.to_owned(), Arc::downgrade(&gate));
    gate
}

fn validate_response_head(
    response: &GatewayHttpResponse,
    request: &ImageCacheRequest,
) -> Result<ImageMediaType, ImageCacheError> {
    if response.head.status != 200
        || response.head.etag.as_deref() != Some(request.etag().as_str())
        || response.head.content_range.is_some()
    {
        return Err(ImageCacheError::InvalidResponse);
    }
    let length = response
        .head
        .content_length
        .ok_or(ImageCacheError::InvalidResponse)?;
    if length == 0 || length > IMAGE_MAX_BYTES as u64 {
        return Err(ImageCacheError::InvalidResponse);
    }
    match response.head.content_type.as_deref() {
        Some("image/png") => Ok(ImageMediaType::Png),
        Some("image/jpeg") => Ok(ImageMediaType::Jpeg),
        Some("image/webp") => Ok(ImageMediaType::Webp),
        _ => Err(ImageCacheError::InvalidResponse),
    }
}

async fn persist_atomically(paths: &ImageCachePaths, bytes: &[u8]) -> Result<(), ImageCacheError> {
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    configure_tokio_no_follow(&mut options);
    let mut file = options
        .open(paths.part_path.as_path())
        .await
        .map_err(|_| ImageCacheError::Disk)?;
    if !metadata_is_plain_file(&file.metadata().await.map_err(|_| ImageCacheError::Disk)?) {
        return Err(ImageCacheError::Disk);
    }
    if file.write_all(bytes).await.is_err() || file.sync_all().await.is_err() {
        drop(file);
        remove_owned_file(paths.part_path.as_path()).await;
        return Err(ImageCacheError::Disk);
    }
    drop(file);
    if read_regular_file(paths.final_path.as_path())
        .await?
        .is_some_and(|existing| existing == bytes)
    {
        remove_owned_file(paths.part_path.as_path()).await;
        return sync_cache_directory(paths.parent()).await;
    }
    // `std::fs::rename` (and therefore Tokio's wrapper) uses replacement
    // semantics on the supported Unix and Windows targets. Do not unlink the
    // published cache entry first: that would introduce a visible missing-file
    // window and break the atomic cache contract on Windows.
    if fs::rename(paths.part_path.as_path(), paths.final_path.as_path())
        .await
        .is_err()
    {
        remove_owned_file(paths.part_path.as_path()).await;
        return Err(ImageCacheError::Disk);
    }
    sync_cache_directory(paths.parent()).await
}

#[cfg(unix)]
async fn sync_cache_directory(path: &Path) -> Result<(), ImageCacheError> {
    fs::File::open(path)
        .await
        .map_err(|_| ImageCacheError::Disk)?
        .sync_all()
        .await
        .map_err(|_| ImageCacheError::Disk)
}

#[cfg(not(unix))]
async fn sync_cache_directory(_path: &Path) -> Result<(), ImageCacheError> {
    Ok(())
}

async fn verify_cached(
    path: &Path,
    request: &ImageCacheRequest,
) -> Result<Option<ImageMediaType>, ImageCacheError> {
    let metadata = match fs::symlink_metadata(path).await {
        Ok(metadata) if metadata_is_plain_file(&metadata) => metadata,
        Ok(_) => return Ok(None),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(ImageCacheError::Disk),
    };
    let cutoff = SystemTime::now()
        .checked_sub(IMAGE_CACHE_MAX_AGE)
        .unwrap_or(SystemTime::UNIX_EPOCH);
    if metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH) <= cutoff {
        return Ok(None);
    }
    let Some(bytes) = read_regular_file(path).await? else {
        return Ok(None);
    };
    let valid = !bytes.is_empty()
        && bytes.len() <= IMAGE_MAX_BYTES
        && sha256_hex(bytes.as_slice()) == request.revision;
    Ok(valid
        .then(|| detect_image_media_type(bytes.as_slice()))
        .flatten())
}

async fn read_regular_file(path: &Path) -> Result<Option<Vec<u8>>, ImageCacheError> {
    let metadata = match fs::symlink_metadata(path).await {
        Ok(metadata) if metadata_is_plain_file(&metadata) => metadata,
        Ok(_) => return Ok(None),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(ImageCacheError::Disk),
    };
    if metadata.len() > IMAGE_MAX_BYTES as u64 {
        return Ok(None);
    }
    let mut options = OpenOptions::new();
    options.read(true);
    configure_tokio_no_follow(&mut options);
    let file = match options.open(path).await {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(ImageCacheError::Disk),
    };
    let opened_metadata = file.metadata().await.map_err(|_| ImageCacheError::Disk)?;
    if !metadata_is_plain_file(&opened_metadata) || opened_metadata.len() > IMAGE_MAX_BYTES as u64 {
        return Ok(None);
    }
    let mut bytes = Vec::with_capacity(opened_metadata.len() as usize);
    file.take(IMAGE_MAX_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .await
        .map_err(|_| ImageCacheError::Disk)?;
    if bytes.len() > IMAGE_MAX_BYTES {
        return Ok(None);
    }
    Ok(Some(bytes))
}

async fn prune_image_cache(runtime_home: &Path) -> Result<u64, ImageCacheError> {
    let root = image_cache_root(runtime_home);
    let mut files = Vec::new();
    collect_cache_files(root.as_path(), &mut files).await?;
    files.sort_by_key(|(_, modified)| *modified);
    let cutoff = SystemTime::now()
        .checked_sub(IMAGE_CACHE_MAX_AGE)
        .unwrap_or(SystemTime::UNIX_EPOCH);
    let over_limit = files.len().saturating_sub(IMAGE_CACHE_MAX_FILES);
    let mut removed = 0_u64;
    for (index, (path, modified)) in files.into_iter().enumerate() {
        if index < over_limit || modified <= cutoff {
            remove_owned_file(path.as_path()).await;
            removed = removed.saturating_add(1);
        }
    }
    Ok(removed)
}

fn collect_cache_files<'a>(
    root: &'a Path,
    output: &'a mut Vec<(PathBuf, SystemTime)>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ImageCacheError>> + Send + 'a>> {
    Box::pin(async move {
        let mut entries = match fs::read_dir(root).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(_) => return Err(ImageCacheError::Disk),
        };
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|_| ImageCacheError::Disk)?
        {
            let file_type = entry.file_type().await.map_err(|_| ImageCacheError::Disk)?;
            if file_type.is_dir() {
                collect_cache_files(entry.path().as_path(), output).await?;
            } else if file_type.is_file() {
                let metadata = entry.metadata().await.map_err(|_| ImageCacheError::Disk)?;
                output.push((
                    entry.path(),
                    metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
                ));
            }
        }
        Ok(())
    })
}

async fn remove_owned_file(path: &Path) {
    let _ = fs::remove_file(path).await;
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn detect_image_media_type(bytes: &[u8]) -> Option<ImageMediaType> {
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        Some(ImageMediaType::Png)
    } else if bytes.starts_with(&[0xff, 0xd8, 0xff]) {
        Some(ImageMediaType::Jpeg)
    } else if bytes.len() >= 12 && bytes.starts_with(b"RIFF") && &bytes[8..12] == b"WEBP" {
        Some(ImageMediaType::Webp)
    } else {
        None
    }
}

fn ensure_not_cancelled(cancellation: &CancellationToken) -> Result<(), ImageCacheError> {
    if cancellation.is_cancelled() {
        Err(ImageCacheError::Cancelled)
    } else {
        Ok(())
    }
}

fn invalidates_cache(error: ImageCacheError) -> bool {
    matches!(
        error,
        ImageCacheError::Authentication
            | ImageCacheError::HiddenOrMissing
            | ImageCacheError::InvalidResponse
            | ImageCacheError::Corrupt
    )
}

fn map_http_error(error: GatewayHttpError) -> ImageCacheError {
    match error {
        GatewayHttpError::AuthenticationTerminal(_)
        | GatewayHttpError::Unauthorized
        | GatewayHttpError::GatewayPinMismatch
        | GatewayHttpError::SessionMismatch => ImageCacheError::Authentication,
        GatewayHttpError::Forbidden | GatewayHttpError::NotFound => {
            ImageCacheError::HiddenOrMissing
        }
        GatewayHttpError::Cancelled => ImageCacheError::Cancelled,
        GatewayHttpError::AuthenticationUnavailable
        | GatewayHttpError::Transport
        | GatewayHttpError::ServiceUnavailable
        | GatewayHttpError::Server
        | GatewayHttpError::TooManyRequests => ImageCacheError::Offline,
        GatewayHttpError::InvalidEndpoint
        | GatewayHttpError::InvalidStoragePath
        | GatewayHttpError::InvalidHeader
        | GatewayHttpError::InvalidResponse
        | GatewayHttpError::Conflict
        | GatewayHttpError::RangeNotSatisfiable => ImageCacheError::InvalidResponse,
    }
}

#[cfg(test)]
mod tests {
    use crate::avatars::{AvatarCacheRequest, PrincipalId};
    use std::collections::VecDeque;
    use tokio::sync::Mutex as TokioMutex;

    use super::*;
    use crate::transport::http::{GatewayHttpBody, GatewayHttpResponseHead};

    struct FakeHttp {
        responses: TokioMutex<VecDeque<Result<GatewayHttpResponse, GatewayHttpError>>>,
        requests: TokioMutex<Vec<(String, Option<String>)>>,
    }

    #[async_trait]
    impl ImageHttp for FakeHttp {
        async fn execute(
            &self,
            request: GatewayHttpRequest,
            _cancellation: CancellationToken,
        ) -> Result<GatewayHttpResponse, GatewayHttpError> {
            self.requests.lock().await.push((
                request.storage_path().to_owned(),
                request.if_none_match().map(str::to_owned),
            ));
            self.responses
                .lock()
                .await
                .pop_front()
                .expect("fake avatar response")
        }
    }

    fn gateway_id() -> GatewayId {
        GatewayId::new("G00000000000000000001").unwrap()
    }

    fn session_id() -> AuthSessionId {
        AuthSessionId::new("S00000000000000000001").unwrap()
    }

    fn request(bytes: &[u8]) -> AvatarCacheRequest {
        AvatarCacheRequest {
            principal_id: PrincipalId::new("P00000000000000000001").unwrap(),
            avatar_revision: sha256_hex(bytes),
        }
    }

    fn png(payload: &[u8]) -> Vec<u8> {
        let mut bytes = b"\x89PNG\r\n\x1a\n".to_vec();
        bytes.extend_from_slice(payload);
        bytes
    }

    fn response(status: u16, request: &AvatarCacheRequest, bytes: Vec<u8>) -> GatewayHttpResponse {
        GatewayHttpResponse {
            head: GatewayHttpResponseHead {
                status,
                request_id: None,
                etag: Some(format!("\"{}\"", request.avatar_revision)),
                content_length: (status == 200).then_some(bytes.len() as u64),
                content_range: None,
                content_type: (status == 200).then_some("image/png".to_owned()),
                content_disposition: None,
            },
            body: GatewayHttpBody::from_test_chunks(
                if bytes.is_empty() {
                    vec![]
                } else {
                    vec![Ok(bytes)]
                },
                CancellationToken::new(),
            ),
        }
    }

    #[tokio::test]
    async fn cancellation_after_http_completion_cannot_commit_or_publish_bytes() {
        // The port deliberately returns a completed response after cancellation.
        struct CancellingHttp(TokioMutex<Option<GatewayHttpResponse>>);
        #[async_trait]
        impl ImageHttp for CancellingHttp {
            async fn execute(
                &self,
                _: GatewayHttpRequest,
                cancellation: CancellationToken,
            ) -> Result<GatewayHttpResponse, GatewayHttpError> {
                cancellation.cancel();
                Ok(self.0.lock().await.take().unwrap())
            }
        }
        let temp = tempfile::tempdir().unwrap();
        let bytes = png(b"synthetic");
        let request = request(&bytes);
        let http = Arc::new(CancellingHttp(TokioMutex::new(Some(response(
            200, &request, bytes,
        )))));
        let service = ImageCacheService::with_http(http, temp.path(), gateway_id(), session_id());
        assert_eq!(
            service
                .resolve(request, CancellationToken::new())
                .await
                .unwrap_err(),
            ImageCacheError::Cancelled
        );
        let mut files = Vec::new();
        collect_cache_files(&image_cache_root(temp.path()), &mut files)
            .await
            .unwrap();
        assert!(files.is_empty());
    }
    #[tokio::test]
    async fn fetch_commits_validated_bytes_then_revalidates_with_304() {
        let temp = tempfile::tempdir().unwrap();
        let bytes = png(b"avatar-one");
        let request = request(bytes.as_slice());
        let http = Arc::new(FakeHttp {
            responses: TokioMutex::new(VecDeque::from([
                Ok(response(200, &request, bytes.clone())),
                Ok(response(304, &request, Vec::new())),
            ])),
            requests: TokioMutex::new(Vec::new()),
        });
        let service =
            ImageCacheService::with_http(http.clone(), temp.path(), gateway_id(), session_id());

        let downloaded = service
            .resolve(request.clone(), CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(downloaded.source, ImageCacheSource::Downloaded);
        assert_eq!(
            fs::read(downloaded.local_path.as_path()).await.unwrap(),
            bytes
        );

        let revalidated = service
            .resolve(request.clone(), CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(revalidated.source, ImageCacheSource::Revalidated);
        let requests = http.requests.lock().await;
        assert_eq!(requests[0].1, None);
        assert_eq!(
            requests[1].1,
            Some(format!("\"{}\"", request.avatar_revision))
        );
        assert!(requests[0].0.starts_with("storage/members/"));
    }

    #[tokio::test]
    async fn immutable_member_revisions_coexist_and_corruption_never_publishes() {
        let temp = tempfile::tempdir().unwrap();
        let first_bytes = png(b"avatar-one");
        let second_bytes = png(b"avatar-two");
        let first = request(first_bytes.as_slice());
        let second = request(second_bytes.as_slice());
        let http = Arc::new(FakeHttp {
            responses: TokioMutex::new(VecDeque::from([
                Ok(response(200, &first, first_bytes)),
                Ok(response(200, &second, second_bytes.clone())),
                Ok(response(200, &second, b"corrupt".to_vec())),
            ])),
            requests: TokioMutex::new(Vec::new()),
        });
        let service = ImageCacheService::with_http(http, temp.path(), gateway_id(), session_id());
        let first_result = service
            .resolve(first, CancellationToken::new())
            .await
            .unwrap();
        let second_result = service
            .resolve(second.clone(), CancellationToken::new())
            .await
            .unwrap();
        assert!(first_result.local_path.as_path().exists());
        assert_eq!(
            fs::read(second_result.local_path.as_path()).await.unwrap(),
            second_bytes
        );

        fs::write(second_result.local_path.as_path(), b"broken")
            .await
            .unwrap();
        assert_eq!(
            service.resolve(second, CancellationToken::new()).await,
            Err(ImageCacheError::Corrupt)
        );
    }

    #[tokio::test]
    async fn offline_uses_only_verified_cache_and_hidden_response_invalidates_it() {
        let temp = tempfile::tempdir().unwrap();
        let bytes = png(b"avatar-one");
        let request = request(bytes.as_slice());
        let http = Arc::new(FakeHttp {
            responses: TokioMutex::new(VecDeque::from([
                Ok(response(200, &request, bytes)),
                Err(GatewayHttpError::Transport),
                Err(GatewayHttpError::NotFound),
            ])),
            requests: TokioMutex::new(Vec::new()),
        });
        let service = ImageCacheService::with_http(http, temp.path(), gateway_id(), session_id());
        let first = service
            .resolve(request.clone(), CancellationToken::new())
            .await
            .unwrap();
        let offline = service
            .resolve(request.clone(), CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(offline.source, ImageCacheSource::OfflineCache);
        assert_eq!(
            service.resolve(request, CancellationToken::new()).await,
            Err(ImageCacheError::HiddenOrMissing)
        );
        assert!(!first.local_path.as_path().exists());
    }

    #[tokio::test]
    async fn offline_never_uses_an_expired_cache_entry() {
        let temp = tempfile::tempdir().unwrap();
        let bytes = png(b"avatar-one");
        let request = request(bytes.as_slice());
        let http = Arc::new(FakeHttp {
            responses: TokioMutex::new(VecDeque::from([
                Ok(response(200, &request, bytes)),
                Err(GatewayHttpError::Transport),
            ])),
            requests: TokioMutex::new(Vec::new()),
        });
        let service = ImageCacheService::with_http(http, temp.path(), gateway_id(), session_id());
        let cached = service
            .resolve(request.clone(), CancellationToken::new())
            .await
            .unwrap();
        std::fs::File::options()
            .write(true)
            .open(cached.local_path.as_path())
            .unwrap()
            .set_modified(SystemTime::UNIX_EPOCH)
            .unwrap();

        assert_eq!(
            service.resolve(request, CancellationToken::new()).await,
            Err(ImageCacheError::Offline)
        );
        assert!(!cached.local_path.as_path().exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cache_reader_never_follows_symlinks() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let outside = temp.path().join("outside-avatar");
        let linked = temp.path().join("cached-avatar");
        fs::write(outside.as_path(), png(b"outside")).await.unwrap();
        symlink(outside.as_path(), linked.as_path()).unwrap();

        assert_eq!(read_regular_file(linked.as_path()).await.unwrap(), None);
        assert_eq!(fs::read(outside.as_path()).await.unwrap(), png(b"outside"));
    }

    #[test]
    fn cache_paths_are_private_to_the_authenticated_session() {
        let temp = tempfile::tempdir().unwrap();
        let avatar = request(png(b"same-avatar").as_slice());
        let first = ImageCachePaths::new(
            temp.path(),
            &gateway_id(),
            &session_id(),
            &ImageCacheRequest::member(avatar.clone()).unwrap(),
        )
        .unwrap();
        let second_session = AuthSessionId::new("S00000000000000000002").unwrap();
        let second = ImageCachePaths::new(
            temp.path(),
            &gateway_id(),
            &second_session,
            &ImageCacheRequest::member(avatar).unwrap(),
        )
        .unwrap();

        assert_ne!(first.final_path, second.final_path);
    }

    #[test]
    fn agent_avatar_uses_the_authenticated_system_route_and_an_isolated_cache_entry() {
        let temp = tempfile::tempdir().unwrap();
        let agent_request =
            ImageCacheRequest::agent(pioneer_protocol::PIONEER_AGENT_AVATAR_REVISION.to_owned())
                .unwrap();
        assert_eq!(
            agent_request.storage_path(),
            format!(
                "storage/system/agent/avatar/{}",
                pioneer_protocol::PIONEER_AGENT_AVATAR_REVISION
            )
        );

        let paths = ImageCachePaths::new(temp.path(), &gateway_id(), &session_id(), &agent_request)
            .unwrap();
        assert!(paths.parent().ends_with("system/agent"));
    }
}

mod controller;
pub(crate) use controller::ImageStore;
pub use controller::{ImagePublication, image_identity_key};
