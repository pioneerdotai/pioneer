//! Artifact preview state and cache helpers.

use crate::artifacts::actions::{ArtifactVersionKey, artifact_version_key};
use anyhow::{Context as _, Result, bail};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use pioneer_protocol::{
    ArtifactPreviewRef, ArtifactProjectionKind, ArtifactProjectionStatus, ArtifactReadResponse,
    ArtifactRef,
};
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
    time::SystemTime,
};

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

pub fn decode_artifact_preview_read_response(
    response: &ArtifactReadResponse,
    expected_sha256: Option<&str>,
) -> Result<ArtifactPreviewReadData> {
    if response.truncated {
        bail!("artifact thumbnail preview read was truncated");
    }
    if response.len == 0 {
        bail!("artifact thumbnail preview was empty");
    }
    if expected_sha256.is_some_and(|sha256| sha256 != response.sha256) {
        bail!("artifact thumbnail preview sha256 mismatch");
    }

    let bytes = BASE64
        .decode(response.content_base64.as_bytes())
        .context("failed to decode artifact thumbnail preview")?;
    let decoded_len = u64::try_from(bytes.len()).unwrap_or_default();
    if decoded_len != response.len || decoded_len != response.total_size_bytes {
        bail!("artifact thumbnail preview length mismatch");
    }

    Ok(ArtifactPreviewReadData {
        bytes,
        sha256: response.sha256.clone(),
    })
}

pub fn write_artifact_preview_cache_files<R: ArtifactPreviewImageRenderer>(
    renderer: &R,
    runtime_home: &Path,
    workspace_id: &str,
    artifact: &ArtifactRef,
    expected_sha256: Option<&str>,
    response: &ArtifactReadResponse,
) -> Result<ArtifactPreviewImagePaths> {
    let preview_data = decode_artifact_preview_read_response(response, expected_sha256)?;
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
    use pioneer_protocol::{
        ArtifactKind, ArtifactPreviewRef, ArtifactProjectionKind, ArtifactProjectionStatus,
        ArtifactStatus,
    };
    use std::cell::RefCell;

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

    #[test]
    fn artifact_preview_read_response_decodes_and_validates_content() {
        let bytes = b"preview bytes";
        let response = ArtifactReadResponse {
            artifact: preview_artifact("art_preview"),
            offset: 0,
            len: bytes.len() as u64,
            total_size_bytes: bytes.len() as u64,
            sha256: "thumb_sha".to_owned(),
            content_base64: BASE64.encode(bytes.as_slice()),
            truncated: false,
        };

        let decoded = decode_artifact_preview_read_response(&response, Some("thumb_sha"))
            .expect("decode preview response");

        assert_eq!(decoded.bytes, bytes);
        assert_eq!(decoded.sha256, "thumb_sha");

        let mismatch = decode_artifact_preview_read_response(&response, Some("different_sha"))
            .expect_err("sha mismatch");
        assert!(mismatch.to_string().contains("sha256 mismatch"));
    }

    #[test]
    fn artifact_preview_cache_writer_uses_shared_paths_and_variant_sizes() {
        let temp = tempfile::tempdir().expect("temp dir");
        let artifact = preview_artifact("art_preview");
        let response = ArtifactReadResponse {
            artifact: artifact.clone(),
            offset: 0,
            len: 13,
            total_size_bytes: 13,
            sha256: "thumb_sha".to_owned(),
            content_base64: BASE64.encode(b"preview bytes"),
            truncated: false,
        };
        let renderer = FakePreviewRenderer::default();

        let image_paths = write_artifact_preview_cache_files(
            &renderer,
            temp.path(),
            "ws",
            &artifact,
            Some("thumb_sha"),
            &response,
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
