//! Artifact preview state, authenticated HTTP projection reads, and cache helpers.

use crate::artifacts::actions::{ArtifactVersionKey, artifact_version_key};
use crate::artifacts::http_download::valid_storage_path_segment;
use crate::transport::http::{
    GatewayHttpError, GatewayHttpRequest, GatewayHttpResponse, GatewayHttpSession,
};
use anyhow::{Context as _, Result, bail};
use async_trait::async_trait;
use pioneer_protocol::{
    ArtifactPreviewRef, ArtifactProjectionKind, ArtifactProjectionStatus, ArtifactRef,
};
use sha2::{Digest as _, Sha256};
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::SystemTime,
};
use tokio_util::sync::CancellationToken;

pub const ARTIFACT_PREVIEW_MAX_BYTES: u64 = 512 * 1024;
pub const ARTIFACT_PREVIEW_CACHE_MAX_BYTES: u64 = 512 * 1024 * 1024;
pub const ARTIFACT_PREVIEW_SQUARE_EDGE_PX: u32 = 128;
pub const ARTIFACT_PREVIEW_DETAIL_WIDTH_PX: u32 = 640;
pub const ARTIFACT_PREVIEW_DETAIL_HEIGHT_PX: u32 = 320;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactPreviewImagePaths {
    pub square_path: PathBuf,
    pub detail_path: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactPreviewReadData {
    pub bytes: Vec<u8>,
    pub sha256: String,
}

#[async_trait]
trait ArtifactPreviewHttp: Send + Sync {
    async fn execute(
        &self,
        request: GatewayHttpRequest,
        cancellation: CancellationToken,
    ) -> std::result::Result<GatewayHttpResponse, GatewayHttpError>;
}

#[async_trait]
impl ArtifactPreviewHttp for GatewayHttpSession {
    async fn execute(
        &self,
        request: GatewayHttpRequest,
        cancellation: CancellationToken,
    ) -> std::result::Result<GatewayHttpResponse, GatewayHttpError> {
        GatewayHttpSession::execute(self, request, cancellation).await
    }
}

#[derive(Clone)]
pub struct ArtifactHttpPreviewService {
    http: Arc<dyn ArtifactPreviewHttp>,
}

impl ArtifactHttpPreviewService {
    pub fn new(http: GatewayHttpSession) -> Self {
        Self {
            http: Arc::new(http),
        }
    }

    pub async fn fetch_thumbnail(
        &self,
        workspace_id: &str,
        artifact: &ArtifactRef,
        cancellation: CancellationToken,
    ) -> Result<ArtifactPreviewReadData> {
        let preview = thumbnail_preview(artifact)
            .ok_or_else(|| anyhow::anyhow!("artifact has no ready thumbnail projection"))?;
        let version_id = artifact
            .version_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| anyhow::anyhow!("artifact thumbnail requires an exact version"))?;
        for (field, value) in [
            ("workspace_id", workspace_id),
            ("artifact_id", artifact.artifact_id.as_str()),
            ("version_id", version_id),
        ] {
            if !valid_storage_path_segment(value) {
                bail!("invalid {field} for artifact thumbnail");
            }
        }
        let expected_size = preview
            .size_bytes
            .filter(|size| *size > 0 && *size <= ARTIFACT_PREVIEW_MAX_BYTES)
            .ok_or_else(|| anyhow::anyhow!("artifact thumbnail size is invalid"))?;
        let expected_sha256 = preview
            .sha256
            .as_deref()
            .map(str::to_ascii_lowercase)
            .filter(|value| value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()))
            .ok_or_else(|| anyhow::anyhow!("artifact thumbnail sha256 is invalid"))?;
        let path = format!(
            "storage/workspaces/{}/artifacts/{}/versions/{}/projections/thumbnail",
            workspace_id, artifact.artifact_id, version_id,
        );
        let mut response = self
            .http
            .execute(GatewayHttpRequest::get(path)?, cancellation.clone())
            .await?;
        let expected_etag = format!("\"sha256-{expected_sha256}\"");
        if response.head.status != 200
            || response.head.content_length != Some(expected_size)
            || response.head.etag.as_deref() != Some(expected_etag.as_str())
            || preview
                .mime_type
                .as_deref()
                .is_some_and(|expected| response.head.content_type.as_deref() != Some(expected))
        {
            bail!("artifact thumbnail response metadata mismatch");
        }

        let mut bytes = Vec::with_capacity(usize::try_from(expected_size).unwrap_or_default());
        while let Some(chunk) = response.body.next_chunk().await {
            let chunk = chunk?;
            if cancellation.is_cancelled()
                || bytes.len().saturating_add(chunk.len())
                    > usize::try_from(expected_size).unwrap_or(usize::MAX)
            {
                bail!("artifact thumbnail response exceeded its immutable size");
            }
            bytes.extend_from_slice(chunk.as_slice());
        }
        if bytes.len() as u64 != expected_size {
            bail!("artifact thumbnail response length mismatch");
        }
        let actual_sha256 = hex::encode(Sha256::digest(bytes.as_slice()));
        if actual_sha256 != expected_sha256 {
            bail!("artifact thumbnail sha256 mismatch");
        }
        Ok(ArtifactPreviewReadData {
            bytes,
            sha256: expected_sha256,
        })
    }

    #[cfg(test)]
    fn with_http(http: Arc<dyn ArtifactPreviewHttp>) -> Self {
        Self { http }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactPreviewVariantTarget {
    pub path: PathBuf,
    pub width_px: u32,
    pub height_px: u32,
}

pub trait ArtifactPreviewImageRenderer {
    fn write_preview_variants(
        &self,
        source_bytes: &[u8],
        targets: &[ArtifactPreviewVariantTarget],
    ) -> Result<()>;
}

#[derive(Clone, Debug, Default)]
pub struct ThreadArtifactPreviewState {
    image_paths_by_artifact: HashMap<ArtifactVersionKey, ArtifactPreviewImagePaths>,
    loading_by_artifact: HashSet<ArtifactVersionKey>,
    failed_by_artifact: HashSet<ArtifactVersionKey>,
}

impl ThreadArtifactPreviewState {
    pub fn square_image_path(&self, artifact: &ArtifactRef) -> Option<&Path> {
        self.image_paths_by_artifact
            .get(&artifact_version_key(artifact))
            .map(|paths| paths.square_path.as_path())
            .filter(|path| path.is_file())
    }

    pub fn detail_image_path(&self, artifact: &ArtifactRef) -> Option<&Path> {
        self.image_paths_by_artifact
            .get(&artifact_version_key(artifact))
            .map(|paths| paths.detail_path.as_path())
            .filter(|path| path.is_file())
    }

    pub fn has_loadable_preview(&self, artifact: &ArtifactRef) -> bool {
        thumbnail_preview(artifact).is_some()
    }

    pub fn should_load_preview(&self, artifact: &ArtifactRef) -> bool {
        if !self.has_loadable_preview(artifact) {
            return false;
        }
        let key = artifact_version_key(artifact);
        !self
            .image_paths_by_artifact
            .get(&key)
            .is_some_and(|paths| paths.square_path.is_file() && paths.detail_path.is_file())
            && !self.loading_by_artifact.contains(&key)
            && !self.failed_by_artifact.contains(&key)
    }

    pub fn mark_loading_if_needed(&mut self, artifact: &ArtifactRef) -> bool {
        if !self.should_load_preview(artifact) {
            return false;
        }
        let key = artifact_version_key(artifact);
        self.failed_by_artifact.remove(&key);
        self.loading_by_artifact.insert(key);
        true
    }

    pub fn apply_loaded(&mut self, artifact: &ArtifactRef, image_paths: ArtifactPreviewImagePaths) {
        let key = artifact_version_key(artifact);
        self.loading_by_artifact.remove(&key);
        self.failed_by_artifact.remove(&key);
        self.image_paths_by_artifact.insert(key, image_paths);
    }

    pub fn apply_failed(&mut self, artifact: &ArtifactRef) {
        let key = artifact_version_key(artifact);
        self.loading_by_artifact.remove(&key);
        self.failed_by_artifact.insert(key);
    }

    pub(crate) fn remove_keys(&mut self, keys: &HashSet<ArtifactVersionKey>) {
        self.image_paths_by_artifact
            .retain(|key, _| !keys.contains(key));
        self.loading_by_artifact.retain(|key| !keys.contains(key));
        self.failed_by_artifact.retain(|key| !keys.contains(key));
    }
}

pub fn thumbnail_preview(artifact: &ArtifactRef) -> Option<&ArtifactPreviewRef> {
    let preview = artifact.preview.as_ref()?;
    if preview.projection_kind == ArtifactProjectionKind::Thumbnail
        && preview.status == ArtifactProjectionStatus::Ready
        && preview.blob_id.is_some()
    {
        Some(preview)
    } else {
        None
    }
}

pub fn write_artifact_preview_cache_files<R: ArtifactPreviewImageRenderer>(
    renderer: &R,
    runtime_home: &Path,
    workspace_id: &str,
    artifact: &ArtifactRef,
    preview_data: &ArtifactPreviewReadData,
) -> Result<ArtifactPreviewImagePaths> {
    let image_paths = artifact_preview_cache_paths(
        runtime_home,
        workspace_id,
        artifact.artifact_id.as_str(),
        artifact.version_id.as_deref(),
        preview_data.sha256.as_str(),
    )?;
    let targets = [
        ArtifactPreviewVariantTarget {
            path: image_paths.square_path.clone(),
            width_px: ARTIFACT_PREVIEW_SQUARE_EDGE_PX,
            height_px: ARTIFACT_PREVIEW_SQUARE_EDGE_PX,
        },
        ArtifactPreviewVariantTarget {
            path: image_paths.detail_path.clone(),
            width_px: ARTIFACT_PREVIEW_DETAIL_WIDTH_PX,
            height_px: ARTIFACT_PREVIEW_DETAIL_HEIGHT_PX,
        },
    ];
    for target in &targets {
        if let Some(parent) = target.path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "failed to create artifact preview cache dir `{}`",
                    parent.display()
                )
            })?;
        }
    }

    renderer.write_preview_variants(preview_data.bytes.as_slice(), &targets)?;

    prune_artifact_preview_cache(
        runtime_home,
        ARTIFACT_PREVIEW_CACHE_MAX_BYTES,
        &[
            image_paths.square_path.clone(),
            image_paths.detail_path.clone(),
        ],
    )?;
    Ok(image_paths)
}

pub fn prune_artifact_preview_cache(
    runtime_home: &Path,
    max_bytes: u64,
    protected_files: &[PathBuf],
) -> Result<u64> {
    let cache_root = artifact_preview_cache_root(runtime_home)?;
    if !cache_root.exists() {
        return Ok(0);
    }

    let mut files = Vec::new();
    collect_artifact_preview_cache_files(cache_root.as_path(), &mut files)?;
    let mut total_size = files
        .iter()
        .fold(0_u64, |total, file| total.saturating_add(file.size));
    if total_size <= max_bytes {
        return Ok(0);
    }

    files.sort_by(|left, right| {
        left.modified
            .cmp(&right.modified)
            .then_with(|| left.path.cmp(&right.path))
    });

    let mut removed_bytes = 0_u64;
    for file in files {
        if total_size <= max_bytes {
            break;
        }
        if protected_files
            .iter()
            .any(|protected_file| file.path == *protected_file)
        {
            continue;
        }

        match fs::remove_file(file.path.as_path()) {
            Ok(()) => {
                total_size = total_size.saturating_sub(file.size);
                removed_bytes = removed_bytes.saturating_add(file.size);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "failed to remove artifact preview cache file `{}`",
                        file.path.display()
                    )
                });
            }
        }
    }

    remove_empty_artifact_preview_cache_dirs(cache_root.as_path(), cache_root.as_path())?;
    Ok(removed_bytes)
}

pub fn artifact_preview_cache_root(runtime_home: &Path) -> Result<PathBuf> {
    let cache_root = runtime_home.join("previews").join("artifacts");
    if !cache_root.starts_with(runtime_home) {
        bail!("artifact preview cache path escaped runtime home");
    }
    Ok(cache_root)
}

pub fn artifact_preview_cache_paths(
    runtime_home: &Path,
    workspace_id: &str,
    artifact_id: &str,
    version_id: Option<&str>,
    sha256: &str,
) -> Result<ArtifactPreviewImagePaths> {
    Ok(ArtifactPreviewImagePaths {
        square_path: artifact_preview_cache_path(
            runtime_home,
            workspace_id,
            artifact_id,
            version_id,
            sha256,
            "square",
        )?,
        detail_path: artifact_preview_cache_path(
            runtime_home,
            workspace_id,
            artifact_id,
            version_id,
            sha256,
            "detail",
        )?,
    })
}

pub fn artifact_preview_cache_path(
    runtime_home: &Path,
    workspace_id: &str,
    artifact_id: &str,
    version_id: Option<&str>,
    sha256: &str,
    variant: &str,
) -> Result<PathBuf> {
    let safe_workspace_id = artifact_preview_safe_path_segment(workspace_id, "workspace");
    let safe_artifact_id = artifact_preview_safe_path_segment(artifact_id, "artifact");
    let safe_version_id =
        artifact_preview_safe_path_segment(version_id.unwrap_or("latest"), "version");
    let safe_sha256 = artifact_preview_safe_path_segment(sha256, "thumbnail");
    let safe_variant = artifact_preview_safe_path_segment(variant, "preview");
    let image_path = artifact_preview_cache_root(runtime_home)?
        .join(safe_workspace_id)
        .join(safe_artifact_id)
        .join(safe_version_id)
        .join(format!("{safe_sha256}.{safe_variant}.png"));
    if !image_path.starts_with(runtime_home) {
        bail!("artifact preview cache path escaped runtime home");
    }
    Ok(image_path)
}

#[derive(Debug)]
struct ArtifactPreviewCacheFile {
    path: PathBuf,
    size: u64,
    modified: SystemTime,
}

fn collect_artifact_preview_cache_files(
    dir: &Path,
    files: &mut Vec<ArtifactPreviewCacheFile>,
) -> Result<()> {
    for entry in fs::read_dir(dir).with_context(|| {
        format!(
            "failed to read artifact preview cache dir `{}`",
            dir.display()
        )
    })? {
        let entry = entry.with_context(|| {
            format!(
                "failed to read artifact preview cache entry under `{}`",
                dir.display()
            )
        })?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .with_context(|| format!("failed to read file type `{}`", path.display()))?;
        if file_type.is_dir() {
            collect_artifact_preview_cache_files(path.as_path(), files)?;
            continue;
        }
        if !file_type.is_file() {
            continue;
        }

        let metadata = entry.metadata().with_context(|| {
            format!("failed to stat artifact preview cache `{}`", path.display())
        })?;
        files.push(ArtifactPreviewCacheFile {
            path,
            size: metadata.len(),
            modified: metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
        });
    }
    Ok(())
}

fn remove_empty_artifact_preview_cache_dirs(dir: &Path, root: &Path) -> Result<()> {
    for entry in fs::read_dir(dir).with_context(|| {
        format!(
            "failed to read artifact preview cache dir `{}`",
            dir.display()
        )
    })? {
        let entry = entry.with_context(|| {
            format!(
                "failed to read artifact preview cache entry under `{}`",
                dir.display()
            )
        })?;
        let path = entry.path();
        if entry
            .file_type()
            .with_context(|| format!("failed to read file type `{}`", path.display()))?
            .is_dir()
        {
            remove_empty_artifact_preview_cache_dirs(path.as_path(), root)?;
        }
    }

    if dir != root
        && fs::read_dir(dir)
            .map(|mut entries| entries.next().is_none())
            .unwrap_or(false)
    {
        let _ = fs::remove_dir(dir);
    }
    Ok(())
}

fn artifact_preview_safe_path_segment(value: &str, fallback: &str) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::http::{GatewayHttpBody, GatewayHttpMethod, GatewayHttpResponseHead};
    use pioneer_protocol::{
        ArtifactKind, ArtifactPreviewRef, ArtifactProjectionKind, ArtifactProjectionStatus,
        ArtifactStatus,
    };
    use std::{cell::RefCell, sync::Mutex as StdMutex};

    #[test]
    fn artifact_preview_cache_prune_keeps_cache_under_size_limit() {
        let temp = tempfile::tempdir().expect("temp dir");
        let old_path = artifact_preview_cache_path(
            temp.path(),
            "ws",
            "old_art",
            Some("v1"),
            "old_sha",
            "square",
        )
        .expect("old path");
        fs::create_dir_all(old_path.parent().expect("old parent")).expect("create old parent");
        fs::write(old_path.as_path(), vec![0_u8; 40]).expect("write old preview");

        std::thread::sleep(std::time::Duration::from_millis(5));

        let new_path = artifact_preview_cache_path(
            temp.path(),
            "ws",
            "new_art",
            Some("v1"),
            "new_sha",
            "square",
        )
        .expect("new path");
        fs::create_dir_all(new_path.parent().expect("new parent")).expect("create new parent");
        fs::write(new_path.as_path(), vec![1_u8; 40]).expect("write new preview");

        let removed =
            prune_artifact_preview_cache(temp.path(), 50, &[new_path.clone()]).expect("prune");

        assert_eq!(removed, 40);
        assert!(!old_path.exists());
        assert!(new_path.exists());
    }

    #[test]
    fn artifact_preview_cache_path_stays_under_runtime_home() {
        let temp = tempfile::tempdir().expect("temp dir");

        let path = artifact_preview_cache_path(
            temp.path(),
            "../workspace",
            "artifact/1",
            Some("version/1"),
            "sha/1",
            "../square",
        )
        .expect("preview cache path");

        assert!(path.starts_with(temp.path()));
        assert_eq!(
            path.file_name().and_then(|value| value.to_str()),
            Some("sha_1._square.png")
        );
    }

    #[tokio::test]
    async fn artifact_preview_fetches_exact_authenticated_http_projection() {
        let bytes = b"preview bytes".to_vec();
        let sha256 = hex::encode(Sha256::digest(bytes.as_slice()));
        let mut artifact = preview_artifact("art-preview");
        let preview = artifact.preview.as_mut().expect("preview");
        preview.size_bytes = Some(bytes.len() as u64);
        preview.sha256 = Some(sha256.clone());
        let http = Arc::new(FakePreviewHttp::new(GatewayHttpResponse {
            head: GatewayHttpResponseHead {
                status: 200,
                request_id: Some("request-preview".to_owned()),
                etag: Some(format!("\"sha256-{sha256}\"")),
                content_length: Some(bytes.len() as u64),
                content_range: None,
                content_type: Some("image/png".to_owned()),
                content_disposition: Some("inline".to_owned()),
            },
            body: GatewayHttpBody::from_test_chunks(
                vec![Ok(bytes.clone())],
                CancellationToken::new(),
            ),
        }));
        let service = ArtifactHttpPreviewService::with_http(http.clone());

        let result = service
            .fetch_thumbnail("ws-preview", &artifact, CancellationToken::new())
            .await
            .expect("HTTP preview");

        assert_eq!(result.bytes, bytes);
        assert_eq!(result.sha256, sha256);
        assert_eq!(
            http.requests(),
            vec![(
                GatewayHttpMethod::Get,
                "storage/workspaces/ws-preview/artifacts/art-preview/versions/v1/projections/thumbnail"
                    .to_owned(),
            )]
        );
    }

    #[test]
    fn artifact_preview_cache_writer_uses_shared_paths_and_variant_sizes() {
        let temp = tempfile::tempdir().expect("temp dir");
        let artifact = preview_artifact("art_preview");
        let preview_data = ArtifactPreviewReadData {
            bytes: b"preview bytes".to_vec(),
            sha256: "thumb_sha".to_owned(),
        };
        let renderer = FakePreviewRenderer::default();

        let image_paths = write_artifact_preview_cache_files(
            &renderer,
            temp.path(),
            "ws",
            &artifact,
            &preview_data,
        )
        .expect("write preview cache");

        assert!(image_paths.square_path.exists());
        assert!(image_paths.detail_path.exists());
        assert_eq!(
            renderer.calls.borrow().as_slice(),
            &[
                (
                    image_paths.square_path.clone(),
                    ARTIFACT_PREVIEW_SQUARE_EDGE_PX,
                    ARTIFACT_PREVIEW_SQUARE_EDGE_PX,
                ),
                (
                    image_paths.detail_path.clone(),
                    ARTIFACT_PREVIEW_DETAIL_WIDTH_PX,
                    ARTIFACT_PREVIEW_DETAIL_HEIGHT_PX,
                ),
            ]
        );
    }

    #[test]
    fn artifact_preview_state_reloads_when_cached_file_was_pruned() {
        let artifact = preview_artifact("art_preview");
        let mut state = ThreadArtifactPreviewState::default();
        state.image_paths_by_artifact.insert(
            artifact_version_key(&artifact),
            ArtifactPreviewImagePaths {
                square_path: PathBuf::from("/tmp/pioneer-missing-preview-square.png"),
                detail_path: PathBuf::from("/tmp/pioneer-missing-preview-detail.png"),
            },
        );

        assert!(state.square_image_path(&artifact).is_none());
        assert!(state.detail_image_path(&artifact).is_none());
        assert!(state.should_load_preview(&artifact));
    }

    #[test]
    fn thumbnail_preview_requires_ready_thumbnail_blob() {
        let artifact = preview_artifact("art_preview");

        assert!(thumbnail_preview(&artifact).is_some());

        let mut without_blob = artifact;
        without_blob.preview.as_mut().expect("preview").blob_id = None;
        assert!(thumbnail_preview(&without_blob).is_none());
    }

    #[derive(Default)]
    struct FakePreviewRenderer {
        calls: RefCell<Vec<(PathBuf, u32, u32)>>,
    }

    struct FakePreviewHttp {
        response: StdMutex<Option<GatewayHttpResponse>>,
        requests: StdMutex<Vec<(GatewayHttpMethod, String)>>,
    }

    impl FakePreviewHttp {
        fn new(response: GatewayHttpResponse) -> Self {
            Self {
                response: StdMutex::new(Some(response)),
                requests: StdMutex::new(Vec::new()),
            }
        }

        fn requests(&self) -> Vec<(GatewayHttpMethod, String)> {
            self.requests.lock().expect("requests lock").clone()
        }
    }

    #[async_trait]
    impl ArtifactPreviewHttp for FakePreviewHttp {
        async fn execute(
            &self,
            request: GatewayHttpRequest,
            _cancellation: CancellationToken,
        ) -> std::result::Result<GatewayHttpResponse, GatewayHttpError> {
            self.requests
                .lock()
                .expect("requests lock")
                .push((request.method(), request.storage_path().to_owned()));
            self.response
                .lock()
                .expect("response lock")
                .take()
                .ok_or(GatewayHttpError::InvalidResponse)
        }
    }

    impl ArtifactPreviewImageRenderer for FakePreviewRenderer {
        fn write_preview_variants(
            &self,
            source_bytes: &[u8],
            targets: &[ArtifactPreviewVariantTarget],
        ) -> Result<()> {
            assert_eq!(source_bytes, b"preview bytes");
            for target in targets {
                fs::write(
                    target.path.as_path(),
                    format!("{}x{}", target.width_px, target.height_px),
                )
                .expect("write fake preview");
                self.calls.borrow_mut().push((
                    target.path.clone(),
                    target.width_px,
                    target.height_px,
                ));
            }
            Ok(())
        }
    }

    fn preview_artifact(artifact_id: &str) -> ArtifactRef {
        ArtifactRef {
            artifact_id: artifact_id.to_owned(),
            version_id: Some("v1".to_owned()),
            display_name: "image.png".to_owned(),
            kind: ArtifactKind::Image,
            mime_type: Some("image/png".to_owned()),
            size_bytes: Some(2048),
            sha256: Some("artifact_sha".to_owned()),
            status: ArtifactStatus::Ready,
            preview: Some(ArtifactPreviewRef {
                projection_kind: ArtifactProjectionKind::Thumbnail,
                status: ArtifactProjectionStatus::Ready,
                artifact_id: artifact_id.to_owned(),
                version_id: "v1".to_owned(),
                blob_id: Some("blob_1".to_owned()),
                mime_type: Some("image/png".to_owned()),
                size_bytes: Some(512),
                sha256: Some("thumb_sha".to_owned()),
            }),
        }
    }
}
