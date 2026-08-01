//! Native authenticated member-avatar cache.
//!
//! Cache entries are immutable, revision-addressed and contain no credentials.
//! Authorization remains owned by `GatewayHttpSession`; shell boundaries only
//! receive an owned local image path and validated representation metadata.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use pioneer_protocol::{
    GatewayId, PrincipalId, ProfileAvatarMediaType, PROFILE_AVATAR_MAX_DECODED_BYTES,
};
use sha2::{Digest, Sha256};
use tokio::fs::{self, File};
use tokio::io::AsyncWriteExt as _;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::platform::ClientPath;
use crate::transport::http::{
    GatewayHttpError, GatewayHttpRequest, GatewayHttpResponse, GatewayHttpSession,
};

const AVATAR_CACHE_MAX_AGE: Duration = Duration::from_secs(30 * 24 * 60 * 60);
const AVATAR_CACHE_MAX_FILES: usize = 2_048;

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AvatarCacheRequest {
    pub principal_id: PrincipalId,
    pub avatar_revision: String,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AvatarCacheSource {
    Downloaded,
    Revalidated,
    OfflineCache,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct AvatarCacheResult {
    pub local_path: ClientPath,
    pub principal_id: PrincipalId,
    pub avatar_revision: String,
    pub media_type: ProfileAvatarMediaType,
    pub source: AvatarCacheSource,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AvatarCacheError {
    InvalidRequest,
    Authentication,
    HiddenOrMissing,
    Offline,
    InvalidResponse,
    Corrupt,
    Disk,
    Cancelled,
}

impl AvatarCacheError {
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

impl fmt::Display for AvatarCacheError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code())
    }
}

impl std::error::Error for AvatarCacheError {}

#[async_trait]
trait AvatarHttp: Send + Sync {
    async fn execute(
        &self,
        request: GatewayHttpRequest,
        cancellation: CancellationToken,
    ) -> Result<GatewayHttpResponse, GatewayHttpError>;
}

#[async_trait]
impl AvatarHttp for GatewayHttpSession {
    async fn execute(
        &self,
        request: GatewayHttpRequest,
        cancellation: CancellationToken,
    ) -> Result<GatewayHttpResponse, GatewayHttpError> {
        GatewayHttpSession::execute(self, request, cancellation).await
    }
}

#[derive(Clone)]
pub struct AvatarCacheService {
    http: Arc<dyn AvatarHttp>,
    runtime_home: PathBuf,
    gateway_id: GatewayId,
    operation_gate: Arc<Mutex<()>>,
}

impl fmt::Debug for AvatarCacheService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AvatarCacheService")
            .field("gateway_id", &self.gateway_id)
            .field("runtime_home", &"[owned-private-cache]")
            .finish_non_exhaustive()
    }
}

impl AvatarCacheService {
    pub fn new(
        http: GatewayHttpSession,
        runtime_home: impl Into<PathBuf>,
        gateway_id: GatewayId,
    ) -> Self {
        Self {
            http: Arc::new(http),
            runtime_home: runtime_home.into(),
            gateway_id,
            operation_gate: Arc::new(Mutex::new(())),
        }
    }

    pub async fn resolve(
        &self,
        request: AvatarCacheRequest,
        cancellation: CancellationToken,
    ) -> Result<AvatarCacheResult, AvatarCacheError> {
        let _operation = self.operation_gate.lock().await;
        let request = ValidatedAvatarRequest::new(request)?;
        let paths = AvatarCachePaths::new(
            self.runtime_home.as_path(),
            &self.gateway_id,
            &request,
        )?;
        ensure_not_cancelled(&cancellation)?;
        fs::create_dir_all(paths.parent())
            .await
            .map_err(|_| AvatarCacheError::Disk)?;
        let cached_media_type = verify_cached(paths.final_path.as_path(), &request).await?;
        if cached_media_type.is_none() {
            remove_owned_file(paths.final_path.as_path()).await;
        }

        let mut native_request = GatewayHttpRequest::get(request.storage_path())
            .map_err(|_| AvatarCacheError::InvalidRequest)?;
        if cached_media_type.is_some() {
            native_request = native_request
                .with_if_none_match(request.etag())
                .map_err(|_| AvatarCacheError::InvalidRequest)?;
        }
        let response = self.http.execute(native_request, cancellation.clone()).await;
        let mut response = match response {
            Ok(response) => response,
            Err(error) => {
                let mapped = map_http_error(error);
                if invalidates_cache(mapped) {
                    remove_owned_file(paths.final_path.as_path()).await;
                }
                if let Some(media_type) = cached_media_type
                    && mapped == AvatarCacheError::Offline
                {
                    return Ok(request.result(
                        paths.final_path,
                        media_type,
                        AvatarCacheSource::OfflineCache,
                    ));
                }
                return Err(mapped);
            }
        };

        if response.head.status == 304 {
            if cached_media_type.is_none()
                || response.head.etag.as_deref() != Some(request.etag().as_str())
            {
                remove_owned_file(paths.final_path.as_path()).await;
                return Err(AvatarCacheError::InvalidResponse);
            }
            return Ok(request.result(
                paths.final_path,
                cached_media_type.expect("304 requires a verified cached representation"),
                AvatarCacheSource::Revalidated,
            ));
        }
        let media_type = validate_response_head(&response, &request)?;
        let expected_length = response
            .head
            .content_length
            .ok_or(AvatarCacheError::InvalidResponse)?;
        let mut bytes = Vec::with_capacity(expected_length as usize);
        while let Some(chunk) = response.body.next_chunk().await {
            ensure_not_cancelled(&cancellation)?;
            let chunk = chunk.map_err(map_http_error)?;
            if bytes.len().saturating_add(chunk.len()) > PROFILE_AVATAR_MAX_DECODED_BYTES {
                return Err(AvatarCacheError::InvalidResponse);
            }
            bytes.extend_from_slice(chunk.as_slice());
        }
        if bytes.len() as u64 != expected_length || sha256_hex(bytes.as_slice()) != request.revision {
            return Err(AvatarCacheError::Corrupt);
        }
        if detect_avatar_media_type(bytes.as_slice()) != Some(media_type) {
            return Err(AvatarCacheError::Corrupt);
        }
        persist_atomically(&paths, bytes.as_slice()).await?;
        prune_other_revisions(paths.parent(), paths.final_path.as_path()).await?;
        let _ = prune_avatar_cache(self.runtime_home.as_path()).await;
        Ok(request.result(paths.final_path, media_type, AvatarCacheSource::Downloaded))
    }

    pub async fn invalidate_gateway(&self) -> Result<(), AvatarCacheError> {
        let gateway_root = avatar_cache_root(self.runtime_home.as_path())
            .join(self.gateway_id.as_str());
        if gateway_root.exists() {
            fs::remove_dir_all(gateway_root)
                .await
                .map_err(|_| AvatarCacheError::Disk)?;
        }
        Ok(())
    }

    #[cfg(test)]
    fn with_http(
        http: Arc<dyn AvatarHttp>,
        runtime_home: impl Into<PathBuf>,
        gateway_id: GatewayId,
    ) -> Self {
        Self {
            http,
            runtime_home: runtime_home.into(),
            gateway_id,
            operation_gate: Arc::new(Mutex::new(())),
        }
    }
}

struct ValidatedAvatarRequest {
    principal_id: PrincipalId,
    revision: String,
}

impl ValidatedAvatarRequest {
    fn new(request: AvatarCacheRequest) -> Result<Self, AvatarCacheError> {
        if request.avatar_revision.len() != 64
            || !request
                .avatar_revision
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(AvatarCacheError::InvalidRequest);
        }
        Ok(Self {
            principal_id: request.principal_id,
            revision: request.avatar_revision,
        })
    }

    fn storage_path(&self) -> String {
        format!(
            "storage/members/{}/avatar/{}",
            self.principal_id, self.revision
        )
    }

    fn etag(&self) -> String {
        format!("\"{}\"", self.revision)
    }

    fn result(
        &self,
        local_path: ClientPath,
        media_type: ProfileAvatarMediaType,
        source: AvatarCacheSource,
    ) -> AvatarCacheResult {
        AvatarCacheResult {
            local_path,
            principal_id: self.principal_id.clone(),
            avatar_revision: self.revision.clone(),
            media_type,
            source,
        }
    }
}

struct AvatarCachePaths {
    final_path: ClientPath,
    part_path: PathBuf,
}

impl AvatarCachePaths {
    fn new(
        runtime_home: &Path,
        gateway_id: &GatewayId,
        request: &ValidatedAvatarRequest,
    ) -> Result<Self, AvatarCacheError> {
        let parent = avatar_cache_root(runtime_home)
            .join(gateway_id.as_str())
            .join("members")
            .join(request.principal_id.as_str());
        let final_path = parent.join(format!("{}.avatar", request.revision));
        let part_path = parent.join(format!("{}.avatar.part", request.revision));
        if !final_path.starts_with(runtime_home) || !part_path.starts_with(runtime_home) {
            return Err(AvatarCacheError::InvalidRequest);
        }
        Ok(Self {
            final_path: ClientPath::new(final_path),
            part_path,
        })
    }

    fn parent(&self) -> &Path {
        self.final_path
            .as_path()
            .parent()
            .expect("owned avatar cache path has a parent")
    }
}

fn avatar_cache_root(runtime_home: &Path) -> PathBuf {
    runtime_home.join("cache").join("avatars").join("gateways")
}

fn validate_response_head(
    response: &GatewayHttpResponse,
    request: &ValidatedAvatarRequest,
) -> Result<ProfileAvatarMediaType, AvatarCacheError> {
    if response.head.status != 200
        || response.head.etag.as_deref() != Some(request.etag().as_str())
        || response.head.content_range.is_some()
    {
        return Err(AvatarCacheError::InvalidResponse);
    }
    let length = response
        .head
        .content_length
        .ok_or(AvatarCacheError::InvalidResponse)?;
    if length == 0 || length > PROFILE_AVATAR_MAX_DECODED_BYTES as u64 {
        return Err(AvatarCacheError::InvalidResponse);
    }
    match response.head.content_type.as_deref() {
        Some("image/png") => Ok(ProfileAvatarMediaType::Png),
        Some("image/jpeg") => Ok(ProfileAvatarMediaType::Jpeg),
        Some("image/webp") => Ok(ProfileAvatarMediaType::Webp),
        _ => Err(AvatarCacheError::InvalidResponse),
    }
}

async fn persist_atomically(paths: &AvatarCachePaths, bytes: &[u8]) -> Result<(), AvatarCacheError> {
    remove_owned_file(paths.part_path.as_path()).await;
    let mut file = File::create(paths.part_path.as_path())
        .await
        .map_err(|_| AvatarCacheError::Disk)?;
    file.write_all(bytes)
        .await
        .map_err(|_| AvatarCacheError::Disk)?;
    file.sync_all().await.map_err(|_| AvatarCacheError::Disk)?;
    drop(file);
    remove_owned_file(paths.final_path.as_path()).await;
    fs::rename(paths.part_path.as_path(), paths.final_path.as_path())
        .await
        .map_err(|_| AvatarCacheError::Disk)
}

async fn verify_cached(
    path: &Path,
    request: &ValidatedAvatarRequest,
) -> Result<Option<ProfileAvatarMediaType>, AvatarCacheError> {
    let bytes = match fs::read(path).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(AvatarCacheError::Disk),
    };
    let valid = !bytes.is_empty()
        && bytes.len() <= PROFILE_AVATAR_MAX_DECODED_BYTES
        && sha256_hex(bytes.as_slice()) == request.revision;
    Ok(valid.then(|| detect_avatar_media_type(bytes.as_slice())).flatten())
}

async fn prune_other_revisions(parent: &Path, keep: &Path) -> Result<(), AvatarCacheError> {
    let mut entries = fs::read_dir(parent).await.map_err(|_| AvatarCacheError::Disk)?;
    while let Some(entry) = entries.next_entry().await.map_err(|_| AvatarCacheError::Disk)? {
        let path = entry.path();
        if path != keep && entry.file_type().await.map_err(|_| AvatarCacheError::Disk)?.is_file() {
            remove_owned_file(path.as_path()).await;
        }
    }
    Ok(())
}

async fn prune_avatar_cache(runtime_home: &Path) -> Result<u64, AvatarCacheError> {
    let root = avatar_cache_root(runtime_home);
    let mut files = Vec::new();
    collect_cache_files(root.as_path(), &mut files).await?;
    files.sort_by_key(|(_, modified)| *modified);
    let cutoff = SystemTime::now()
        .checked_sub(AVATAR_CACHE_MAX_AGE)
        .unwrap_or(SystemTime::UNIX_EPOCH);
    let over_limit = files.len().saturating_sub(AVATAR_CACHE_MAX_FILES);
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
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), AvatarCacheError>> + Send + 'a>> {
    Box::pin(async move {
        let mut entries = match fs::read_dir(root).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(_) => return Err(AvatarCacheError::Disk),
        };
        while let Some(entry) = entries.next_entry().await.map_err(|_| AvatarCacheError::Disk)? {
            let metadata = entry.metadata().await.map_err(|_| AvatarCacheError::Disk)?;
            if metadata.is_dir() {
                collect_cache_files(entry.path().as_path(), output).await?;
            } else if metadata.is_file() {
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

fn detect_avatar_media_type(bytes: &[u8]) -> Option<ProfileAvatarMediaType> {
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        Some(ProfileAvatarMediaType::Png)
    } else if bytes.starts_with(&[0xff, 0xd8, 0xff]) {
        Some(ProfileAvatarMediaType::Jpeg)
    } else if bytes.len() >= 12 && bytes.starts_with(b"RIFF") && &bytes[8..12] == b"WEBP" {
        Some(ProfileAvatarMediaType::Webp)
    } else {
        None
    }
}

fn ensure_not_cancelled(cancellation: &CancellationToken) -> Result<(), AvatarCacheError> {
    if cancellation.is_cancelled() {
        Err(AvatarCacheError::Cancelled)
    } else {
        Ok(())
    }
}

fn invalidates_cache(error: AvatarCacheError) -> bool {
    matches!(
        error,
        AvatarCacheError::Authentication
            | AvatarCacheError::HiddenOrMissing
            | AvatarCacheError::InvalidResponse
            | AvatarCacheError::Corrupt
    )
}

fn map_http_error(error: GatewayHttpError) -> AvatarCacheError {
    match error {
        GatewayHttpError::AuthenticationTerminal(_)
        | GatewayHttpError::Unauthorized
        | GatewayHttpError::GatewayPinMismatch
        | GatewayHttpError::SessionMismatch => AvatarCacheError::Authentication,
        GatewayHttpError::Forbidden | GatewayHttpError::NotFound => {
            AvatarCacheError::HiddenOrMissing
        }
        GatewayHttpError::Cancelled => AvatarCacheError::Cancelled,
        GatewayHttpError::AuthenticationUnavailable
        | GatewayHttpError::Transport
        | GatewayHttpError::ServiceUnavailable
        | GatewayHttpError::Server
        | GatewayHttpError::TooManyRequests => AvatarCacheError::Offline,
        GatewayHttpError::InvalidEndpoint
        | GatewayHttpError::InvalidStoragePath
        | GatewayHttpError::InvalidHeader
        | GatewayHttpError::InvalidResponse
        | GatewayHttpError::RangeNotSatisfiable => AvatarCacheError::InvalidResponse,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use tokio::sync::Mutex as TokioMutex;

    use super::*;
    use crate::transport::http::{GatewayHttpBody, GatewayHttpResponseHead};

    struct FakeHttp {
        responses: TokioMutex<VecDeque<Result<GatewayHttpResponse, GatewayHttpError>>>,
        requests: TokioMutex<Vec<(String, Option<String>)>>,
    }

    #[async_trait]
    impl AvatarHttp for FakeHttp {
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

    fn response(
        status: u16,
        request: &AvatarCacheRequest,
        bytes: Vec<u8>,
    ) -> GatewayHttpResponse {
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
                if bytes.is_empty() { vec![] } else { vec![Ok(bytes)] },
                CancellationToken::new(),
            ),
        }
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
        let service = AvatarCacheService::with_http(http.clone(), temp.path(), gateway_id());

        let downloaded = service
            .resolve(request.clone(), CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(downloaded.source, AvatarCacheSource::Downloaded);
        assert_eq!(fs::read(downloaded.local_path.as_path()).await.unwrap(), bytes);

        let revalidated = service
            .resolve(request.clone(), CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(revalidated.source, AvatarCacheSource::Revalidated);
        let requests = http.requests.lock().await;
        assert_eq!(requests[0].1, None);
        assert_eq!(requests[1].1, Some(format!("\"{}\"", request.avatar_revision)));
        assert!(requests[0].0.starts_with("storage/members/"));
    }

    #[tokio::test]
    async fn revision_change_replaces_member_entry_and_corruption_never_publishes() {
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
        let service = AvatarCacheService::with_http(http, temp.path(), gateway_id());
        let first_result = service
            .resolve(first, CancellationToken::new())
            .await
            .unwrap();
        let second_result = service
            .resolve(second.clone(), CancellationToken::new())
            .await
            .unwrap();
        assert!(!first_result.local_path.as_path().exists());
        assert_eq!(fs::read(second_result.local_path.as_path()).await.unwrap(), second_bytes);

        fs::write(second_result.local_path.as_path(), b"broken").await.unwrap();
        assert_eq!(
            service.resolve(second, CancellationToken::new()).await,
            Err(AvatarCacheError::Corrupt)
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
        let service = AvatarCacheService::with_http(http, temp.path(), gateway_id());
        let first = service
            .resolve(request.clone(), CancellationToken::new())
            .await
            .unwrap();
        let offline = service
            .resolve(request.clone(), CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(offline.source, AvatarCacheSource::OfflineCache);
        assert_eq!(
            service.resolve(request, CancellationToken::new()).await,
            Err(AvatarCacheError::HiddenOrMissing)
        );
        assert!(!first.local_path.as_path().exists());
    }
}
