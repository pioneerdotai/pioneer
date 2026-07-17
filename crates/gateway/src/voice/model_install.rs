use super::model_catalog::{
    VoiceModelArchiveType, VoiceModelCatalogEntry, VoiceModelInstallLayout, voice_model_catalog,
    voice_model_catalog_entry, voice_model_install_layout,
};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use flate2::read::GzDecoder;
use futures_util::StreamExt;
use pioneer_config::AppConfig;
use pioneer_provider::providers::LocalTranscriptionEngine;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs;
use std::io::{BufReader, Read};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tar::Archive;
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;

const MAX_TAR_ENTRY_COUNT: u64 = 100_000;
const MAX_TAR_PATH_COMPONENTS: usize = 64;
const MAX_TAR_PATH_BYTES: usize = 4_096;
const MAX_TAR_EXPANSION_RATIO: u64 = 8;
const DOWNLOAD_PROGRESS_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum VoiceModelInstallPhase {
    Downloading,
    Verifying,
    Installing,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct VoiceModelInstallProgress {
    pub(crate) phase: VoiceModelInstallPhase,
    pub(crate) downloaded_bytes: u64,
    pub(crate) total_bytes: Option<u64>,
}

pub(crate) trait VoiceModelInstallProgressObserver: Send + Sync {
    fn report(&self, progress: VoiceModelInstallProgress);
}

impl<F> VoiceModelInstallProgressObserver for F
where
    F: Fn(VoiceModelInstallProgress) + Send + Sync,
{
    fn report(&self, progress: VoiceModelInstallProgress) {
        self(progress);
    }
}

#[derive(Clone)]
pub(crate) struct VoiceModelInstallControl {
    cancellation: CancellationToken,
    progress: Arc<dyn VoiceModelInstallProgressObserver>,
}

impl VoiceModelInstallControl {
    pub(crate) fn new(
        cancellation: CancellationToken,
        progress: Arc<dyn VoiceModelInstallProgressObserver>,
    ) -> Self {
        Self {
            cancellation,
            progress,
        }
    }

    fn check_cancelled(&self) -> Result<()> {
        if self.cancellation.is_cancelled() {
            bail!("voice model installation cancelled");
        }
        Ok(())
    }

    pub(crate) fn report(&self, progress: VoiceModelInstallProgress) {
        self.progress.report(progress);
    }
}

impl Default for VoiceModelInstallControl {
    fn default() -> Self {
        Self::new(CancellationToken::new(), Arc::new(|_| {}))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum VoiceModelInstallStatus {
    AlreadyInstalled,
    Installed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct VoiceModelInstallReport {
    pub(crate) status: VoiceModelInstallStatus,
    pub(crate) layout: VoiceModelInstallLayout,
}

#[async_trait]
pub(crate) trait VoiceModelArchiveDownloader: Send + Sync {
    async fn download_archive(
        &self,
        entry: &VoiceModelCatalogEntry,
        destination: &Path,
        control: &VoiceModelInstallControl,
    ) -> Result<()>;
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ReqwestVoiceModelArchiveDownloader {
    client: reqwest::Client,
}

impl ReqwestVoiceModelArchiveDownloader {
    pub(crate) fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl VoiceModelArchiveDownloader for ReqwestVoiceModelArchiveDownloader {
    async fn download_archive(
        &self,
        entry: &VoiceModelCatalogEntry,
        destination: &Path,
        control: &VoiceModelInstallControl,
    ) -> Result<()> {
        control.check_cancelled()?;
        let response = tokio::select! {
            _ = control.cancellation.cancelled() => bail!("voice model installation cancelled"),
            response = self.client.get(entry.url).send() => response,
        }
        .with_context(|| format!("failed to download voice model {}", entry.id))?
        .error_for_status()
        .with_context(|| format!("voice model download returned an error for {}", entry.id))?;
        let total_bytes = response.content_length();
        control.report(VoiceModelInstallProgress {
            phase: VoiceModelInstallPhase::Downloading,
            downloaded_bytes: 0,
            total_bytes,
        });

        let mut destination_file =
            tokio::fs::File::create(destination)
                .await
                .with_context(|| {
                    format!(
                        "failed to create voice model partial archive {}",
                        destination.display()
                    )
                })?;
        let mut stream = response.bytes_stream();
        let mut downloaded_bytes = 0_u64;
        let mut last_progress_at = Instant::now();
        loop {
            let next_chunk = tokio::select! {
                _ = control.cancellation.cancelled() => bail!("voice model installation cancelled"),
                chunk = stream.next() => chunk,
            };
            let Some(chunk) = next_chunk else {
                break;
            };
            let chunk = chunk
                .with_context(|| format!("failed to read voice model download {}", entry.id))?;
            downloaded_bytes = downloaded_bytes
                .checked_add(chunk.len() as u64)
                .context("voice model download byte count overflow")?;
            if total_bytes.is_some_and(|total| downloaded_bytes > total) {
                bail!("voice model download exceeded declared Content-Length");
            }
            destination_file
                .write_all(chunk.as_ref())
                .await
                .with_context(|| {
                    format!(
                        "failed to write voice model partial archive {}",
                        destination.display()
                    )
                })?;
            if last_progress_at.elapsed() >= DOWNLOAD_PROGRESS_INTERVAL {
                control.report(VoiceModelInstallProgress {
                    phase: VoiceModelInstallPhase::Downloading,
                    downloaded_bytes,
                    total_bytes,
                });
                last_progress_at = Instant::now();
            }
        }
        control.check_cancelled()?;
        destination_file.flush().await.with_context(|| {
            format!(
                "failed to flush voice model partial archive {}",
                destination.display()
            )
        })?;
        destination_file.sync_all().await.with_context(|| {
            format!(
                "failed to sync voice model partial archive {}",
                destination.display()
            )
        })?;
        if total_bytes.is_some_and(|total| downloaded_bytes != total) {
            bail!("voice model download did not match declared Content-Length");
        }
        control.report(VoiceModelInstallProgress {
            phase: VoiceModelInstallPhase::Downloading,
            downloaded_bytes,
            total_bytes,
        });

        Ok(())
    }
}

pub(crate) async fn ensure_voice_model_installed_with_control<D>(
    entry: &VoiceModelCatalogEntry,
    config: &AppConfig,
    runtime_home: &Path,
    downloader: &D,
    control: &VoiceModelInstallControl,
) -> Result<VoiceModelInstallReport>
where
    D: VoiceModelArchiveDownloader + ?Sized,
{
    let layout = voice_model_install_layout(entry, config, runtime_home)?;
    ensure_voice_model_installed_at_with_control(entry, layout, downloader, control).await
}

pub(crate) async fn force_fresh_voice_model_install<D>(
    entry: &VoiceModelCatalogEntry,
    config: &AppConfig,
    runtime_home: &Path,
    downloader: &D,
    control: &VoiceModelInstallControl,
) -> Result<VoiceModelInstallReport>
where
    D: VoiceModelArchiveDownloader + ?Sized,
{
    let trusted_entry = voice_model_catalog_entry(entry.id)
        .with_context(|| format!("unknown local transcription model `{}`", entry.id))?;
    if trusted_entry != *entry {
        bail!(
            "voice model retry metadata does not match trusted catalog entry {}",
            entry.id
        );
    }
    let layout = voice_model_install_layout(&trusted_entry, config, runtime_home)?;
    force_fresh_voice_model_install_at(&trusted_entry, layout, downloader, control).await
}

async fn force_fresh_voice_model_install_at<D>(
    entry: &VoiceModelCatalogEntry,
    layout: VoiceModelInstallLayout,
    downloader: &D,
    control: &VoiceModelInstallControl,
) -> Result<VoiceModelInstallReport>
where
    D: VoiceModelArchiveDownloader + ?Sized,
{
    control.check_cancelled()?;
    validate_install_layout_paths(&layout)?;
    validate_layout_matches_entry(entry, &layout)?;
    fs::create_dir_all(layout.models_root.as_path())?;
    reject_symlink(layout.models_root.as_path(), "voice model root")?;
    fs::create_dir_all(layout.downloads_dir.as_path())?;
    reject_symlink(
        layout.downloads_dir.as_path(),
        "voice model downloads directory",
    )?;
    reject_symlink(
        layout.install_dir.as_path(),
        "voice model install directory",
    )?;
    reject_symlink(
        layout.staging_dir.as_path(),
        "voice model staging directory",
    )?;
    reject_symlink(layout.archive_path.as_path(), "voice model artifact")?;
    reject_symlink(
        layout.partial_archive_path.as_path(),
        "partial voice model artifact",
    )?;

    remove_owned_staging_if_exists(entry, &layout)?;
    remove_downloaded_model_archives(&layout)?;
    remove_path_if_exists(layout.install_dir.as_path()).with_context(|| {
        format!(
            "failed to remove selected voice model install {} for fresh retry",
            layout.install_dir.display()
        )
    })?;

    ensure_voice_model_installed_at_with_control(entry, layout, downloader, control).await
}

#[cfg(test)]
pub(crate) async fn ensure_voice_model_installed_at<D>(
    entry: &VoiceModelCatalogEntry,
    layout: VoiceModelInstallLayout,
    downloader: &D,
) -> Result<VoiceModelInstallReport>
where
    D: VoiceModelArchiveDownloader + ?Sized,
{
    ensure_voice_model_installed_at_with_control(
        entry,
        layout,
        downloader,
        &VoiceModelInstallControl::default(),
    )
    .await
}

pub(crate) async fn ensure_voice_model_installed_at_with_control<D>(
    entry: &VoiceModelCatalogEntry,
    layout: VoiceModelInstallLayout,
    downloader: &D,
    control: &VoiceModelInstallControl,
) -> Result<VoiceModelInstallReport>
where
    D: VoiceModelArchiveDownloader + ?Sized,
{
    if is_voice_model_installed_and_verified(entry, &layout) {
        remove_downloaded_model_archives(&layout)?;
        return Ok(VoiceModelInstallReport {
            status: VoiceModelInstallStatus::AlreadyInstalled,
            layout,
        });
    }

    prepare_install_workspace(entry, &layout)?;
    let attempt = async {
        control.check_cancelled()?;
        let artifact_bytes = ensure_archive_downloaded(entry, &layout, downloader, control).await?;
        control.check_cancelled()?;
        control.report(VoiceModelInstallProgress {
            phase: VoiceModelInstallPhase::Installing,
            downloaded_bytes: artifact_bytes,
            total_bytes: Some(artifact_bytes),
        });
        extract_voice_model_archive(entry, &layout, control).with_context(|| {
            format!(
                "failed to install voice model {} from {}",
                entry.id,
                layout.archive_path.display()
            )
        })?;
        control.check_cancelled()?;
        write_ready_marker(entry, &layout)?;
        control.report(VoiceModelInstallProgress {
            phase: VoiceModelInstallPhase::Installing,
            downloaded_bytes: artifact_bytes,
            total_bytes: Some(artifact_bytes),
        });

        Ok(VoiceModelInstallReport {
            status: VoiceModelInstallStatus::Installed,
            layout: layout.clone(),
        })
    }
    .await;

    if attempt.is_err() && !layout.ready_marker_path.exists() {
        let _ = remove_path_if_exists(layout.install_dir.as_path());
    }
    let cleanup = remove_downloaded_model_archives(&layout);
    match (attempt, cleanup) {
        (Ok(report), Ok(())) => Ok(report),
        (Ok(_), Err(cleanup_error)) => Err(cleanup_error),
        (Err(error), Ok(())) => Err(error),
        (Err(error), Err(cleanup_error)) => Err(error.context(format!(
            "also failed to clean transient voice model artifacts: {cleanup_error:#}"
        ))),
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct VoiceModelCleanupReport {
    pub(crate) removed_install_dirs: Vec<PathBuf>,
}

pub(crate) fn remove_non_selected_voice_model_installs(
    config: &AppConfig,
    runtime_home: &Path,
    selected_model_id: &str,
    cancellation: &CancellationToken,
    protected_model_id: &RwLock<Option<String>>,
) -> Result<VoiceModelCleanupReport> {
    let selected_entry = voice_model_catalog_entry(selected_model_id).with_context(|| {
        format!("unknown selected local transcription model `{selected_model_id}`")
    })?;
    let selected_layout = voice_model_install_layout(&selected_entry, config, runtime_home)?;
    let mut report = VoiceModelCleanupReport::default();

    for entry in voice_model_catalog() {
        if cancellation.is_cancelled() {
            bail!("voice model replacement cleanup cancelled");
        }
        let layout = voice_model_install_layout(&entry, config, runtime_home)?;
        validate_catalog_cleanup_target(&layout)?;
        let protected_model_id = protected_model_id
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if entry.id == selected_model_id
            || layout.install_dir == selected_layout.install_dir
            || protected_model_id.as_deref() == Some(entry.id)
        {
            continue;
        }
        match fs::symlink_metadata(layout.install_dir.as_path()) {
            Ok(_) => {
                remove_path_if_exists(layout.install_dir.as_path()).with_context(|| {
                    format!(
                        "failed to remove superseded voice model install {}",
                        layout.install_dir.display()
                    )
                })?;
                report.removed_install_dirs.push(layout.install_dir);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }

    Ok(report)
}

fn validate_catalog_cleanup_target(layout: &VoiceModelInstallLayout) -> Result<()> {
    if layout.install_dir == layout.models_root
        || layout.install_dir.parent() != Some(layout.models_root.as_path())
        || !layout.install_dir.starts_with(layout.models_root.as_path())
    {
        bail!(
            "refusing unsafe voice model cleanup target {} outside model root {}",
            layout.install_dir.display(),
            layout.models_root.display()
        );
    }
    Ok(())
}

#[derive(Deserialize, Serialize)]
struct VoiceModelReadyMarker {
    id: String,
    version: String,
    sha256: String,
}

pub(crate) fn is_voice_model_installed_and_verified(
    entry: &VoiceModelCatalogEntry,
    layout: &VoiceModelInstallLayout,
) -> bool {
    let Ok(marker_metadata) = fs::symlink_metadata(layout.ready_marker_path.as_path()) else {
        return false;
    };
    if marker_metadata.file_type().is_symlink() || !marker_metadata.is_file() {
        return false;
    }
    let Ok(marker_bytes) = fs::read(layout.ready_marker_path.as_path()) else {
        return false;
    };
    let Ok(marker) = serde_json::from_slice::<VoiceModelReadyMarker>(marker_bytes.as_slice())
    else {
        return false;
    };

    validate_installed_model_layout(entry, layout).is_ok()
        && marker.id == entry.id
        && marker.version == entry.version
        && marker.sha256 == entry.sha256
}

async fn ensure_archive_downloaded<D>(
    entry: &VoiceModelCatalogEntry,
    layout: &VoiceModelInstallLayout,
    downloader: &D,
    control: &VoiceModelInstallControl,
) -> Result<u64>
where
    D: VoiceModelArchiveDownloader + ?Sized,
{
    remove_downloaded_model_archives(layout)?;
    control.check_cancelled()?;

    downloader
        .download_archive(entry, layout.partial_archive_path.as_path(), control)
        .await?;
    control.check_cancelled()?;

    let artifact_bytes = fs::metadata(layout.partial_archive_path.as_path())
        .with_context(|| {
            format!(
                "failed to inspect voice model partial archive {}",
                layout.partial_archive_path.display()
            )
        })?
        .len();
    control.report(VoiceModelInstallProgress {
        phase: VoiceModelInstallPhase::Verifying,
        downloaded_bytes: artifact_bytes,
        total_bytes: Some(artifact_bytes),
    });
    let partial_hash =
        sha256_file_with_cancellation(layout.partial_archive_path.as_path(), &control.cancellation)
            .with_context(|| {
                format!(
                    "failed to hash voice model partial archive {}",
                    layout.partial_archive_path.display()
                )
            })?;
    if partial_hash != entry.sha256 {
        bail!(
            "voice model {} partial archive checksum mismatch: expected {}, got {}",
            entry.id,
            entry.sha256,
            partial_hash
        );
    }
    control.check_cancelled()?;

    fs::rename(
        layout.partial_archive_path.as_path(),
        layout.archive_path.as_path(),
    )
    .with_context(|| {
        format!(
            "failed to promote voice model archive {} to {}",
            layout.partial_archive_path.display(),
            layout.archive_path.display()
        )
    })?;

    Ok(artifact_bytes)
}

fn remove_downloaded_model_archives(layout: &VoiceModelInstallLayout) -> Result<()> {
    remove_path_if_exists(layout.archive_path.as_path()).with_context(|| {
        format!(
            "failed to remove downloaded voice model archive {}",
            layout.archive_path.display()
        )
    })?;
    remove_path_if_exists(layout.partial_archive_path.as_path()).with_context(|| {
        format!(
            "failed to remove partial voice model archive {}",
            layout.partial_archive_path.display()
        )
    })?;

    Ok(())
}

fn prepare_install_workspace(
    entry: &VoiceModelCatalogEntry,
    layout: &VoiceModelInstallLayout,
) -> Result<()> {
    validate_install_layout_paths(layout)?;
    validate_layout_matches_entry(entry, layout)?;
    fs::create_dir_all(layout.models_root.as_path()).with_context(|| {
        format!(
            "failed to create voice model root directory {}",
            layout.models_root.display()
        )
    })?;
    reject_symlink(layout.models_root.as_path(), "voice model root")?;
    fs::create_dir_all(layout.downloads_dir.as_path()).with_context(|| {
        format!(
            "failed to create voice model downloads directory {}",
            layout.downloads_dir.display()
        )
    })?;
    reject_symlink(
        layout.downloads_dir.as_path(),
        "voice model downloads directory",
    )?;
    reject_symlink(layout.archive_path.as_path(), "voice model artifact")?;
    reject_symlink(
        layout.partial_archive_path.as_path(),
        "partial voice model artifact",
    )?;
    reject_symlink(
        layout.install_dir.as_path(),
        "voice model install directory",
    )?;
    reject_symlink(
        layout.staging_dir.as_path(),
        "voice model staging directory",
    )?;

    if layout.install_dir.exists() {
        bail!(
            "refusing to overwrite unverified voice model install directory {} for {}",
            layout.install_dir.display(),
            entry.id
        );
    }

    remove_owned_staging_if_exists(entry, layout)?;

    Ok(())
}

#[derive(Deserialize, Serialize)]
struct VoiceModelStagingMarker {
    id: String,
    sha256: String,
}

struct VoiceModelStagingGuard {
    path: PathBuf,
}

impl VoiceModelStagingGuard {
    const OWNERSHIP_MARKER_FILE: &'static str = ".pioneer-voice-staging";

    fn create(entry: &VoiceModelCatalogEntry, layout: &VoiceModelInstallLayout) -> Result<Self> {
        if layout.staging_dir.exists() {
            bail!(
                "voice model staging directory {} is not fresh",
                layout.staging_dir.display()
            );
        }
        fs::create_dir(layout.staging_dir.as_path()).with_context(|| {
            format!(
                "failed to create fresh voice model staging directory {}",
                layout.staging_dir.display()
            )
        })?;
        let guard = Self {
            path: layout.staging_dir.clone(),
        };
        let marker = VoiceModelStagingMarker {
            id: entry.id.to_owned(),
            sha256: entry.sha256.to_owned(),
        };
        fs::write(guard.ownership_marker_path(), serde_json::to_vec(&marker)?).with_context(
            || {
                format!(
                    "failed to write voice model staging ownership marker {}",
                    guard.ownership_marker_path().display()
                )
            },
        )?;
        Ok(guard)
    }

    fn path(&self) -> &Path {
        self.path.as_path()
    }

    fn ownership_marker_path(&self) -> PathBuf {
        self.path.join(Self::OWNERSHIP_MARKER_FILE)
    }

    fn remove_ownership_marker(&self) -> Result<()> {
        fs::remove_file(self.ownership_marker_path()).with_context(|| {
            format!(
                "failed to remove voice model staging ownership marker {}",
                self.ownership_marker_path().display()
            )
        })
    }
}

impl Drop for VoiceModelStagingGuard {
    fn drop(&mut self) {
        let _ = remove_path_if_exists(self.path.as_path());
    }
}

fn remove_owned_staging_if_exists(
    entry: &VoiceModelCatalogEntry,
    layout: &VoiceModelInstallLayout,
) -> Result<()> {
    let metadata = match fs::symlink_metadata(layout.staging_dir.as_path()) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        bail!(
            "refusing to replace unsafe voice model staging path {}",
            layout.staging_dir.display()
        );
    }

    let marker_path = layout
        .staging_dir
        .join(VoiceModelStagingGuard::OWNERSHIP_MARKER_FILE);
    let marker: VoiceModelStagingMarker = serde_json::from_slice(
        fs::read(marker_path.as_path())
            .with_context(|| {
                format!(
                    "refusing to remove unowned voice model staging directory {}",
                    layout.staging_dir.display()
                )
            })?
            .as_slice(),
    )
    .with_context(|| {
        format!(
            "invalid voice model staging ownership marker {}",
            marker_path.display()
        )
    })?;
    if marker.id != entry.id || marker.sha256 != entry.sha256 {
        bail!(
            "refusing to remove voice model staging directory {} owned by another artifact",
            layout.staging_dir.display()
        );
    }

    fs::remove_dir_all(layout.staging_dir.as_path()).with_context(|| {
        format!(
            "failed to remove owned stale voice model staging directory {}",
            layout.staging_dir.display()
        )
    })
}

fn validate_install_layout_paths(layout: &VoiceModelInstallLayout) -> Result<()> {
    let paths = [
        &layout.models_root,
        &layout.downloads_dir,
        &layout.archive_path,
        &layout.partial_archive_path,
        &layout.install_dir,
        &layout.staging_dir,
        &layout.model_data_dir,
        &layout.ready_marker_path,
    ];
    for path in paths {
        if path
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
        {
            bail!("voice model path is not normalized: {}", path.display());
        }
        if path != &layout.models_root && !path.starts_with(&layout.models_root) {
            bail!(
                "voice model path {} escapes models root {}",
                path.display(),
                layout.models_root.display()
            );
        }
    }
    if layout.downloads_dir.parent() != Some(layout.models_root.as_path())
        || layout.archive_path.parent() != Some(layout.downloads_dir.as_path())
        || layout.partial_archive_path.parent() != Some(layout.downloads_dir.as_path())
        || layout.install_dir.parent() != Some(layout.models_root.as_path())
        || layout.staging_dir.parent() != Some(layout.models_root.as_path())
        || !layout
            .model_data_dir
            .starts_with(layout.install_dir.as_path())
        || layout.ready_marker_path != layout.install_dir.join(".ready")
    {
        bail!("voice model install layout has unexpected path relationships");
    }
    Ok(())
}

fn validate_layout_matches_entry(
    entry: &VoiceModelCatalogEntry,
    layout: &VoiceModelInstallLayout,
) -> Result<()> {
    let expected_staging_name = format!("{}.staging", entry.install_dir_name);
    let expected_partial_name = format!("{}.partial", entry.archive_file_name);
    let model_data_matches = match entry.archive_type {
        VoiceModelArchiveType::SingleFile => {
            layout.model_data_dir == layout.install_dir.join(entry.model_data_dir_name)
        }
        VoiceModelArchiveType::TarGzDirectory if entry.model_data_dir_name.is_empty() => {
            layout.model_data_dir == layout.install_dir
        }
        VoiceModelArchiveType::TarGzDirectory => {
            layout.model_data_dir == layout.install_dir.join(entry.model_data_dir_name)
        }
    };
    if layout
        .install_dir
        .file_name()
        .and_then(|name| name.to_str())
        != Some(entry.install_dir_name)
        || layout
            .staging_dir
            .file_name()
            .and_then(|name| name.to_str())
            != Some(expected_staging_name.as_str())
        || layout
            .archive_path
            .file_name()
            .and_then(|name| name.to_str())
            != Some(entry.archive_file_name)
        || layout
            .partial_archive_path
            .file_name()
            .and_then(|name| name.to_str())
            != Some(expected_partial_name.as_str())
        || !model_data_matches
    {
        bail!(
            "voice model install layout does not match trusted catalog entry {}",
            entry.id
        );
    }
    Ok(())
}

fn reject_symlink(path: &Path, label: &str) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            bail!("{label} {} must not be a symlink", path.display())
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn validate_staged_model_layout(layout: &VoiceModelInstallLayout) -> Result<PathBuf> {
    reject_symlink(
        layout.staging_dir.as_path(),
        "voice model staging directory",
    )?;
    if layout.model_data_dir == layout.install_dir {
        let expected_dir_name = layout
            .install_dir
            .file_name()
            .context("model install directory must have a final component")?;
        let staged_top_level_model_dir = layout.staging_dir.join(expected_dir_name);
        reject_symlink(
            staged_top_level_model_dir.as_path(),
            "staged voice model directory",
        )?;
        if staged_top_level_model_dir.is_dir()
            && directory_contains_file(staged_top_level_model_dir.as_path())?
        {
            return Ok(staged_top_level_model_dir);
        }
        if directory_contains_direct_file(layout.staging_dir.as_path())? {
            return Ok(layout.staging_dir.clone());
        }
    } else {
        let relative_model_data = layout
            .model_data_dir
            .strip_prefix(layout.install_dir.as_path())
            .context("model data path must be beneath install directory")?;
        let staged_model_data = layout.staging_dir.join(relative_model_data);
        reject_symlink(staged_model_data.as_path(), "staged voice model data")?;
        if (staged_model_data.is_file() && fs::metadata(staged_model_data.as_path())?.len() > 0)
            || (staged_model_data.is_dir() && directory_contains_file(staged_model_data.as_path())?)
        {
            return Ok(layout.staging_dir.clone());
        }
    }

    bail!(
        "voice model staging did not contain expected runtime layout {}",
        layout.model_data_dir.display()
    )
}

fn validate_promoted_model_layout(layout: &VoiceModelInstallLayout) -> Result<()> {
    reject_symlink(
        layout.install_dir.as_path(),
        "voice model install directory",
    )?;
    reject_symlink(layout.model_data_dir.as_path(), "voice model runtime path")?;
    if (layout.model_data_dir.is_file() && fs::metadata(layout.model_data_dir.as_path())?.len() > 0)
        || (layout.model_data_dir.is_dir()
            && directory_contains_file(layout.model_data_dir.as_path())?)
    {
        return Ok(());
    }
    bail!(
        "promoted voice model is missing expected runtime content {}",
        layout.model_data_dir.display()
    )
}

fn validate_installed_model_layout(
    entry: &VoiceModelCatalogEntry,
    layout: &VoiceModelInstallLayout,
) -> Result<()> {
    validate_install_layout_paths(layout)?;
    validate_layout_matches_entry(entry, layout)?;
    validate_promoted_model_layout(layout)?;

    let runtime_root = layout.model_data_dir.as_path();
    match entry.engine {
        LocalTranscriptionEngine::Whisper => require_runtime_file(
            layout.install_dir.as_path(),
            &[entry.model_data_dir_name],
            "Whisper model",
        ),
        LocalTranscriptionEngine::Parakeet => {
            require_runtime_file(
                runtime_root,
                &["encoder-model.int8.onnx", "encoder-model.onnx"],
                "Parakeet encoder",
            )?;
            require_runtime_file(
                runtime_root,
                &["decoder_joint-model.int8.onnx", "decoder_joint-model.onnx"],
                "Parakeet decoder",
            )?;
            require_runtime_file(runtime_root, &["nemo128.onnx"], "Parakeet preprocessor")?;
            require_runtime_file(runtime_root, &["vocab.txt"], "Parakeet vocabulary")
        }
        LocalTranscriptionEngine::Moonshine => {
            require_runtime_file(
                runtime_root,
                &["encoder_model.int8.onnx", "encoder_model.onnx"],
                "Moonshine encoder",
            )?;
            require_runtime_file(
                runtime_root,
                &[
                    "decoder_model_merged.int8.onnx",
                    "decoder_model_merged.onnx",
                ],
                "Moonshine decoder",
            )?;
            require_runtime_file(runtime_root, &["tokenizer.json"], "Moonshine tokenizer")
        }
        LocalTranscriptionEngine::MoonshineStreaming => {
            require_runtime_file(
                runtime_root,
                &["streaming_config.json"],
                "Moonshine streaming config",
            )?;
            require_runtime_file(
                runtime_root,
                &["tokenizer.bin"],
                "Moonshine streaming tokenizer",
            )?;
            for component in ["frontend", "encoder", "adapter", "cross_kv", "decoder_kv"] {
                let candidates = [
                    format!("{component}.int8.ort"),
                    format!("{component}.ort"),
                    format!("{component}.int8.onnx"),
                    format!("{component}.onnx"),
                ];
                require_runtime_file_owned(
                    runtime_root,
                    candidates.as_slice(),
                    "Moonshine streaming component",
                )?;
            }
            Ok(())
        }
        LocalTranscriptionEngine::SenseVoice => {
            require_runtime_file(
                runtime_root,
                &["model.int8.onnx", "model.onnx"],
                "SenseVoice model",
            )?;
            require_runtime_file(runtime_root, &["tokens.txt"], "SenseVoice tokens")
        }
        LocalTranscriptionEngine::GigaAm => {
            require_runtime_file(
                runtime_root,
                &["model.int8.onnx", "model.onnx"],
                "GigaAM model",
            )?;
            require_runtime_file(runtime_root, &["vocab.txt"], "GigaAM vocabulary")
        }
        LocalTranscriptionEngine::Canary => {
            require_runtime_file(
                runtime_root,
                &["encoder-model.int8.onnx", "encoder-model.onnx"],
                "Canary encoder",
            )?;
            require_runtime_file(
                runtime_root,
                &["decoder-model.int8.onnx", "decoder-model.onnx"],
                "Canary decoder",
            )?;
            require_runtime_file(runtime_root, &["nemo128.onnx"], "Canary preprocessor")?;
            require_runtime_file(runtime_root, &["vocab.txt"], "Canary vocabulary")
        }
        LocalTranscriptionEngine::Cohere => {
            require_runtime_file(
                runtime_root,
                &[
                    "cohere-encoder.int8.onnx",
                    "encoder_model.int8.onnx",
                    "onnx/cohere-encoder.int8.onnx",
                    "onnx/encoder_model.int8.onnx",
                ],
                "Cohere encoder",
            )?;
            require_runtime_file(
                runtime_root,
                &[
                    "cohere-decoder.int8.onnx",
                    "decoder_model_merged.int8.onnx",
                    "onnx/cohere-decoder.int8.onnx",
                    "onnx/decoder_model_merged.int8.onnx",
                ],
                "Cohere decoder",
            )?;
            require_runtime_file(
                runtime_root,
                &[
                    "tokens.txt",
                    "vocabulary.txt",
                    "onnx/tokens.txt",
                    "onnx/vocabulary.txt",
                ],
                "Cohere vocabulary",
            )
        }
    }
}

fn require_runtime_file(root: &Path, candidates: &[&str], label: &str) -> Result<()> {
    let candidates = candidates
        .iter()
        .map(|candidate| (*candidate).to_owned())
        .collect::<Vec<_>>();
    require_runtime_file_owned(root, candidates.as_slice(), label)
}

fn require_runtime_file_owned(root: &Path, candidates: &[String], label: &str) -> Result<()> {
    for candidate in candidates {
        let path = root.join(candidate);
        if !path.starts_with(root) {
            continue;
        }
        let Ok(metadata) = fs::symlink_metadata(path.as_path()) else {
            continue;
        };
        if metadata.is_file() && !metadata.file_type().is_symlink() && metadata.len() > 0 {
            return Ok(());
        }
    }
    bail!(
        "voice model is missing required {label}; expected one of: {}",
        candidates.join(", ")
    )
}

fn extract_voice_model_archive(
    entry: &VoiceModelCatalogEntry,
    layout: &VoiceModelInstallLayout,
    control: &VoiceModelInstallControl,
) -> Result<()> {
    control.check_cancelled()?;
    match entry.archive_type {
        VoiceModelArchiveType::SingleFile => materialize_single_file(entry, layout, control),
        VoiceModelArchiveType::TarGzDirectory => extract_tar_gz_archive(entry, layout, control),
    }
}

fn materialize_single_file(
    entry: &VoiceModelCatalogEntry,
    layout: &VoiceModelInstallLayout,
    control: &VoiceModelInstallControl,
) -> Result<()> {
    control.check_cancelled()?;
    reject_symlink(
        layout.archive_path.as_path(),
        "single-file voice model artifact",
    )?;
    let artifact_metadata = fs::metadata(layout.archive_path.as_path()).with_context(|| {
        format!(
            "failed to inspect single-file voice model artifact {}",
            layout.archive_path.display()
        )
    })?;
    if !artifact_metadata.is_file() || artifact_metadata.len() == 0 {
        bail!(
            "single-file voice model artifact {} must be a non-empty regular file",
            layout.archive_path.display()
        );
    }
    if layout
        .archive_path
        .file_name()
        .and_then(|name| name.to_str())
        != Some(entry.archive_file_name)
    {
        bail!(
            "single-file voice model artifact path does not match trusted filename {}",
            entry.archive_file_name
        );
    }
    let runtime_file_name = Path::new(entry.model_data_dir_name);
    if entry.model_data_dir_name.is_empty()
        || runtime_file_name.components().count() != 1
        || !matches!(
            runtime_file_name.components().next(),
            Some(Component::Normal(_))
        )
        || entry.model_data_dir_name.contains('\\')
        || entry.model_data_dir_name.contains('/')
        || layout.model_data_dir != layout.install_dir.join(runtime_file_name)
    {
        bail!(
            "single-file voice model {} has an unsafe or inconsistent runtime filename",
            entry.id
        );
    }

    let staging = VoiceModelStagingGuard::create(entry, layout)?;
    let staged_runtime_file = staging.path().join(runtime_file_name);
    if !staged_runtime_file.starts_with(staging.path()) {
        bail!("single-file voice model runtime path escapes staging directory");
    }
    fs::copy(layout.archive_path.as_path(), staged_runtime_file.as_path()).with_context(|| {
        format!(
            "failed to materialize single-file voice model artifact {} as {}",
            layout.archive_path.display(),
            staged_runtime_file.display()
        )
    })?;
    control.check_cancelled()?;
    let staged_metadata = fs::symlink_metadata(staged_runtime_file.as_path())?;
    if !staged_metadata.is_file()
        || staged_metadata.file_type().is_symlink()
        || staged_metadata.len() == 0
    {
        bail!(
            "materialized voice model runtime file {} is invalid",
            staged_runtime_file.display()
        );
    }

    promote_extracted_model_directory(layout, staging, control)
}

fn extract_tar_gz_archive(
    entry: &VoiceModelCatalogEntry,
    layout: &VoiceModelInstallLayout,
    control: &VoiceModelInstallControl,
) -> Result<()> {
    control.check_cancelled()?;
    let staging = VoiceModelStagingGuard::create(entry, layout)?;

    let archive_file = fs::File::open(layout.archive_path.as_path()).with_context(|| {
        format!(
            "failed to open voice model archive {}",
            layout.archive_path.display()
        )
    })?;
    let decoder = GzDecoder::new(BufReader::new(archive_file));
    let mut archive = Archive::new(decoder);
    let max_extracted_bytes = entry
        .download_size_mb
        .saturating_mul(1024 * 1024)
        .saturating_mul(MAX_TAR_EXPANSION_RATIO)
        .max(64 * 1024 * 1024);
    let mut entry_count = 0_u64;
    let mut declared_extracted_bytes = 0_u64;

    for archive_entry in archive.entries().context("failed to read tar.gz entries")? {
        control.check_cancelled()?;
        entry_count = entry_count.saturating_add(1);
        if entry_count > MAX_TAR_ENTRY_COUNT {
            bail!("voice model archive contains too many entries");
        }
        let mut archive_entry = archive_entry.context("failed to read tar.gz entry")?;
        reject_unsafe_archive_entry(&archive_entry)?;
        if archive_entry.header().entry_type().is_file() {
            declared_extracted_bytes = declared_extracted_bytes
                .checked_add(
                    archive_entry
                        .header()
                        .size()
                        .context("failed to read tar.gz entry size")?,
                )
                .context("voice model archive declared size overflow")?;
            if declared_extracted_bytes > max_extracted_bytes {
                bail!(
                    "voice model archive exceeds trusted extracted size limit of {max_extracted_bytes} bytes"
                );
            }
        }
        let unpacked = archive_entry.unpack_in(staging.path()).with_context(|| {
            format!(
                "failed to unpack tar.gz entry into {}",
                layout.staging_dir.display()
            )
        })?;
        if !unpacked {
            bail!("voice model archive entry escaped the staging directory");
        }
    }

    control.check_cancelled()?;
    promote_extracted_model_directory(layout, staging, control)?;

    Ok(())
}

fn promote_extracted_model_directory(
    layout: &VoiceModelInstallLayout,
    staging: VoiceModelStagingGuard,
    control: &VoiceModelInstallControl,
) -> Result<()> {
    control.check_cancelled()?;
    let promotion_source = validate_staged_model_layout(layout)?;
    staging.remove_ownership_marker()?;
    if layout.install_dir.exists() {
        bail!(
            "refusing to overwrite voice model install directory {}",
            layout.install_dir.display()
        );
    }

    if layout.model_data_dir == layout.install_dir {
        let expected_dir_name = layout
            .install_dir
            .file_name()
            .context("model install directory must have a final component")?;
        let staged_top_level_model_dir = staging.path().join(expected_dir_name);
        debug_assert!(
            promotion_source == staged_top_level_model_dir || promotion_source == staging.path()
        );
    }

    fs::rename(promotion_source.as_path(), layout.install_dir.as_path()).with_context(|| {
        format!(
            "failed to atomically promote voice model staging path {} to {}",
            promotion_source.display(),
            layout.install_dir.display()
        )
    })?;
    drop(staging);

    control.check_cancelled().inspect_err(|_| {
        let _ = remove_path_if_exists(layout.install_dir.as_path());
    })?;
    validate_promoted_model_layout(layout).inspect_err(|_| {
        let _ = remove_path_if_exists(layout.install_dir.as_path());
    })
}

fn directory_contains_file(path: &Path) -> Result<bool> {
    for entry in fs::read_dir(path)
        .with_context(|| format!("failed to read directory {}", path.display()))?
    {
        let entry = entry.with_context(|| format!("failed to read entry in {}", path.display()))?;
        let metadata = fs::symlink_metadata(entry.path())?;
        if metadata.file_type().is_symlink() {
            bail!(
                "voice model staging contains symlink {}",
                entry.path().display()
            );
        }
        if metadata.is_file()
            && entry.file_name() != VoiceModelStagingGuard::OWNERSHIP_MARKER_FILE
            && entry.file_name() != ".ready"
            && entry.file_name() != ".ready.partial"
        {
            return Ok(true);
        }
        if metadata.is_dir() && directory_contains_file(entry.path().as_path())? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn directory_contains_direct_file(path: &Path) -> Result<bool> {
    for entry in fs::read_dir(path)
        .with_context(|| format!("failed to read directory {}", path.display()))?
    {
        let entry = entry.with_context(|| format!("failed to read entry in {}", path.display()))?;
        let metadata = fs::symlink_metadata(entry.path())?;
        if metadata.file_type().is_symlink() {
            bail!(
                "voice model staging contains symlink {}",
                entry.path().display()
            );
        }
        if metadata.is_file() && entry.file_name() != VoiceModelStagingGuard::OWNERSHIP_MARKER_FILE
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn reject_unsafe_archive_entry<R: Read>(entry: &tar::Entry<'_, R>) -> Result<()> {
    let entry_type = entry.header().entry_type();
    let entry_path = entry.path().context("failed to read tar.gz entry path")?;
    reject_unsafe_archive_member(entry_type, entry_path.as_ref())
}

fn reject_unsafe_archive_member(entry_type: tar::EntryType, entry_path: &Path) -> Result<()> {
    if entry_type.is_symlink() || entry_type.is_hard_link() {
        bail!("voice model archive contains link entries, which are not supported");
    }
    if !entry_type.is_file() && !entry_type.is_dir() {
        bail!("voice model archive contains unsupported special-file entries");
    }
    if entry_path.as_os_str().is_empty() {
        bail!("voice model archive contains an empty path");
    }
    if entry_path.components().count() > MAX_TAR_PATH_COMPONENTS
        || entry_path.as_os_str().as_encoded_bytes().len() > MAX_TAR_PATH_BYTES
    {
        bail!("voice model archive contains an excessively deep or long path");
    }

    for component in entry_path.components() {
        match component {
            Component::Normal(_) => {}
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                bail!(
                    "voice model archive contains unsafe path {}",
                    entry_path.display()
                );
            }
        }
    }

    Ok(())
}

fn write_ready_marker(
    entry: &VoiceModelCatalogEntry,
    layout: &VoiceModelInstallLayout,
) -> Result<()> {
    validate_installed_model_layout(entry, layout)?;
    let marker = VoiceModelReadyMarker {
        id: entry.id.to_owned(),
        version: entry.version.to_owned(),
        sha256: entry.sha256.to_owned(),
    };
    let temporary_marker_path = layout.install_dir.join(".ready.partial");
    reject_symlink(temporary_marker_path.as_path(), "temporary ready marker")?;
    fs::write(
        temporary_marker_path.as_path(),
        serde_json::to_vec_pretty(&marker)?,
    )
    .with_context(|| {
        format!(
            "failed to write voice model ready marker {}",
            temporary_marker_path.display()
        )
    })?;
    fs::rename(
        temporary_marker_path.as_path(),
        layout.ready_marker_path.as_path(),
    )
    .with_context(|| {
        format!(
            "failed to atomically promote voice model ready marker {}",
            layout.ready_marker_path.display()
        )
    })?;

    Ok(())
}

fn sha256_file_with_cancellation(path: &Path, cancellation: &CancellationToken) -> Result<String> {
    let mut file = fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];

    loop {
        if cancellation.is_cancelled() {
            bail!("voice model installation cancelled");
        }
        let bytes_read = file.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
    }

    Ok(hex::encode(hasher.finalize()))
}

fn remove_path_if_exists(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() => fs::remove_dir_all(path)?,
        Ok(_) => fs::remove_file(path)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Instant;
    use tar::{Builder, Header};
    use tempfile::TempDir;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn voice_model_replacement_cleanup_keeps_only_selected_known_install_and_unknown_directories() {
        let temp = tempfile::tempdir().expect("temp dir");
        let config = AppConfig::load().expect("config");
        let selected = voice_model_catalog_entry("medium").expect("medium catalog entry");
        let superseded = voice_model_catalog_entry("small").expect("small catalog entry");
        let selected_layout =
            voice_model_install_layout(&selected, &config, temp.path()).expect("selected layout");
        let superseded_layout = voice_model_install_layout(&superseded, &config, temp.path())
            .expect("superseded layout");
        fs::create_dir_all(selected_layout.install_dir.as_path()).expect("selected install");
        fs::create_dir_all(superseded_layout.install_dir.as_path()).expect("superseded install");
        let unknown = selected_layout.models_root.join("custom-user-model");
        fs::create_dir_all(unknown.as_path()).expect("unknown model dir");

        let report = remove_non_selected_voice_model_installs(
            &config,
            temp.path(),
            selected.id,
            &CancellationToken::new(),
            &RwLock::new(None),
        )
        .expect("replacement cleanup");

        assert!(selected_layout.install_dir.exists());
        assert!(!superseded_layout.install_dir.exists());
        assert!(unknown.exists());
        assert!(selected_layout.models_root.exists());
        assert!(
            report
                .removed_install_dirs
                .contains(&superseded_layout.install_dir)
        );
        assert!(!report.removed_install_dirs.contains(&unknown));
    }

    #[test]
    fn voice_model_replacement_cleanup_preserves_the_concurrently_selected_model() {
        let temp = tempfile::tempdir().expect("temp dir");
        let config = AppConfig::load().expect("config");
        let committed = voice_model_catalog_entry("medium").expect("committed catalog entry");
        let selected = voice_model_catalog_entry("small").expect("selected catalog entry");
        let superseded = voice_model_catalog_entry("turbo").expect("superseded catalog entry");
        let committed_layout =
            voice_model_install_layout(&committed, &config, temp.path()).expect("committed layout");
        let selected_layout =
            voice_model_install_layout(&selected, &config, temp.path()).expect("selected layout");
        let superseded_layout = voice_model_install_layout(&superseded, &config, temp.path())
            .expect("superseded layout");
        for layout in [&committed_layout, &selected_layout, &superseded_layout] {
            fs::create_dir_all(layout.install_dir.as_path()).expect("model install");
        }
        let protected_model_id = RwLock::new(Some(selected.id.to_owned()));

        let report = remove_non_selected_voice_model_installs(
            &config,
            temp.path(),
            committed.id,
            &CancellationToken::new(),
            &protected_model_id,
        )
        .expect("replacement cleanup");

        assert!(committed_layout.install_dir.exists());
        assert!(selected_layout.install_dir.exists());
        assert!(!superseded_layout.install_dir.exists());
        assert_eq!(
            report.removed_install_dirs,
            vec![superseded_layout.install_dir]
        );
    }

    #[test]
    fn voice_model_replacement_cleanup_cancellation_preserves_previous_install() {
        let temp = tempfile::tempdir().expect("temp dir");
        let config = AppConfig::load().expect("config");
        let selected = voice_model_catalog_entry("medium").expect("medium catalog entry");
        let previous = voice_model_catalog_entry("small").expect("small catalog entry");
        let selected_layout =
            voice_model_install_layout(&selected, &config, temp.path()).expect("selected layout");
        let previous_layout =
            voice_model_install_layout(&previous, &config, temp.path()).expect("previous layout");
        fs::create_dir_all(selected_layout.install_dir.as_path()).expect("selected install");
        fs::create_dir_all(previous_layout.install_dir.as_path()).expect("previous install");
        let cancellation = CancellationToken::new();
        cancellation.cancel();

        let error = remove_non_selected_voice_model_installs(
            &config,
            temp.path(),
            selected.id,
            &cancellation,
            &RwLock::new(None),
        )
        .expect_err("cancelled cleanup must stop");

        assert!(error.to_string().contains("cancelled"));
        assert!(selected_layout.install_dir.exists());
        assert!(previous_layout.install_dir.exists());
    }

    #[test]
    fn voice_model_cleanup_safety_rejects_root_and_nested_arbitrary_targets() {
        let temp = tempfile::tempdir().expect("temp dir");
        let config = AppConfig::load().expect("config");
        let entry = voice_model_catalog_entry("small").expect("small catalog entry");
        let mut layout = voice_model_install_layout(&entry, &config, temp.path()).expect("layout");
        layout.install_dir.clone_from(&layout.models_root);
        assert!(validate_catalog_cleanup_target(&layout).is_err());

        layout.install_dir = layout.models_root.join("custom").join("nested");
        assert!(validate_catalog_cleanup_target(&layout).is_err());

        layout.install_dir = temp.path().join("outside");
        assert!(validate_catalog_cleanup_target(&layout).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn voice_model_cleanup_safety_never_follows_catalog_path_symlink() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().expect("temp dir");
        let external = tempfile::tempdir().expect("external dir");
        let external_file = external.path().join("keep.txt");
        fs::write(external_file.as_path(), b"keep").expect("external file");
        let config = AppConfig::load().expect("config");
        let selected = voice_model_catalog_entry("medium").expect("medium catalog entry");
        let linked = voice_model_catalog_entry("small").expect("small catalog entry");
        let selected_layout =
            voice_model_install_layout(&selected, &config, temp.path()).expect("selected layout");
        let linked_layout =
            voice_model_install_layout(&linked, &config, temp.path()).expect("linked layout");
        fs::create_dir_all(selected_layout.install_dir.as_path()).expect("selected install");
        fs::create_dir_all(linked_layout.models_root.as_path()).expect("models root");
        symlink(external.path(), linked_layout.install_dir.as_path())
            .expect("catalog path symlink");

        remove_non_selected_voice_model_installs(
            &config,
            temp.path(),
            selected.id,
            &CancellationToken::new(),
            &RwLock::new(None),
        )
        .expect("safe cleanup");

        assert!(!linked_layout.install_dir.exists());
        assert!(external_file.exists());
    }

    const TEST_ENTRY: VoiceModelCatalogEntry = VoiceModelCatalogEntry {
        id: "test-voice-model",
        display_name: "Test voice model",
        version: "test-v1",
        engine: pioneer_provider::providers::LocalTranscriptionEngine::Parakeet,
        url: "memory://test-voice-model.tar.gz",
        sha256: "",
        download_size_mb: 1,
        archive_type: VoiceModelArchiveType::TarGzDirectory,
        archive_file_name: "test-voice-model.tar.gz",
        install_dir_name: "test-voice-model",
        model_data_dir_name: "model",
    };

    struct MemoryArchiveDownloader {
        bytes: Vec<u8>,
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl VoiceModelArchiveDownloader for MemoryArchiveDownloader {
        async fn download_archive(
            &self,
            _entry: &VoiceModelCatalogEntry,
            destination: &Path,
            control: &VoiceModelInstallControl,
        ) -> Result<()> {
            control.check_cancelled()?;
            self.calls.fetch_add(1, Ordering::SeqCst);
            control.report(VoiceModelInstallProgress {
                phase: VoiceModelInstallPhase::Downloading,
                downloaded_bytes: 0,
                total_bytes: Some(self.bytes.len() as u64),
            });
            tokio::fs::write(destination, self.bytes.as_slice())
                .await
                .with_context(|| {
                    format!("failed to write test archive {}", destination.display())
                })?;
            control.check_cancelled()?;
            control.report(VoiceModelInstallProgress {
                phase: VoiceModelInstallPhase::Downloading,
                downloaded_bytes: self.bytes.len() as u64,
                total_bytes: Some(self.bytes.len() as u64),
            });
            Ok(())
        }
    }

    struct CancellingArchiveDownloader {
        bytes: Vec<u8>,
    }

    #[async_trait]
    impl VoiceModelArchiveDownloader for CancellingArchiveDownloader {
        async fn download_archive(
            &self,
            _entry: &VoiceModelCatalogEntry,
            destination: &Path,
            control: &VoiceModelInstallControl,
        ) -> Result<()> {
            let split = self.bytes.len().max(1) / 2;
            tokio::fs::write(destination, &self.bytes[..split]).await?;
            control.report(VoiceModelInstallProgress {
                phase: VoiceModelInstallPhase::Downloading,
                downloaded_bytes: split as u64,
                total_bytes: Some(self.bytes.len() as u64),
            });
            control.cancellation.cancel();
            control.check_cancelled()
        }
    }

    fn test_entry_with_sha256(sha256: String) -> VoiceModelCatalogEntry {
        VoiceModelCatalogEntry {
            sha256: Box::leak(sha256.into_boxed_str()),
            ..TEST_ENTRY
        }
    }

    fn test_layout(temp_dir: &TempDir) -> VoiceModelInstallLayout {
        let models_root = temp_dir.path().join("models/voice");
        let downloads_dir = models_root.join("downloads");
        let archive_path = downloads_dir.join(TEST_ENTRY.archive_file_name);
        let partial_archive_path =
            downloads_dir.join(format!("{}.partial", TEST_ENTRY.archive_file_name));
        let install_dir = models_root.join(TEST_ENTRY.install_dir_name);
        let staging_dir = models_root.join(format!("{}.staging", TEST_ENTRY.install_dir_name));
        let model_data_dir = install_dir.join(TEST_ENTRY.model_data_dir_name);
        let ready_marker_path = install_dir.join(".ready");

        VoiceModelInstallLayout {
            models_root,
            downloads_dir,
            archive_path,
            partial_archive_path,
            install_dir,
            staging_dir,
            model_data_dir,
            ready_marker_path,
        }
    }

    fn single_file_entry_with_sha256(sha256: String) -> VoiceModelCatalogEntry {
        VoiceModelCatalogEntry {
            id: "test-whisper",
            display_name: "Test Whisper",
            version: "test-whisper-v1",
            engine: pioneer_provider::providers::LocalTranscriptionEngine::Whisper,
            url: "memory://test-whisper.bin",
            sha256: Box::leak(sha256.into_boxed_str()),
            download_size_mb: 1,
            archive_type: VoiceModelArchiveType::SingleFile,
            archive_file_name: "test-whisper.bin",
            install_dir_name: "test-whisper",
            model_data_dir_name: "test-whisper.bin",
        }
    }

    fn single_file_layout(temp_dir: &TempDir) -> VoiceModelInstallLayout {
        let models_root = temp_dir.path().join("models/voice");
        let downloads_dir = models_root.join("downloads");
        let archive_path = downloads_dir.join("test-whisper.bin");
        let partial_archive_path = downloads_dir.join("test-whisper.bin.partial");
        let install_dir = models_root.join("test-whisper");
        VoiceModelInstallLayout {
            models_root: models_root.clone(),
            downloads_dir,
            archive_path,
            partial_archive_path,
            staging_dir: models_root.join("test-whisper.staging"),
            model_data_dir: install_dir.join("test-whisper.bin"),
            ready_marker_path: install_dir.join(".ready"),
            install_dir,
        }
    }

    fn test_archive_bytes() -> Vec<u8> {
        let mut archive = Vec::new();
        {
            let encoder = GzEncoder::new(&mut archive, Compression::default());
            let mut builder = Builder::new(encoder);

            let mut dir_header = Header::new_gnu();
            dir_header.set_entry_type(tar::EntryType::Directory);
            dir_header.set_mode(0o755);
            dir_header.set_size(0);
            dir_header.set_cksum();
            builder
                .append_data(&mut dir_header, "model/", std::io::empty())
                .expect("append model dir");

            for file_name in [
                "encoder-model.int8.onnx",
                "decoder_joint-model.int8.onnx",
                "nemo128.onnx",
                "vocab.txt",
            ] {
                let file_bytes = b"fixture";
                let mut file_header = Header::new_gnu();
                file_header.set_entry_type(tar::EntryType::Regular);
                file_header.set_mode(0o644);
                file_header.set_size(file_bytes.len() as u64);
                file_header.set_cksum();
                builder
                    .append_data(
                        &mut file_header,
                        format!("model/{file_name}"),
                        std::io::Cursor::new(file_bytes),
                    )
                    .expect("append model file");
            }

            builder.finish().expect("finish tar");
            builder.into_inner().expect("finish gzip");
        }

        archive
    }

    fn handy_style_top_level_archive_bytes() -> Vec<u8> {
        let mut archive = Vec::new();
        {
            let encoder = GzEncoder::new(&mut archive, Compression::default());
            let mut builder = Builder::new(encoder);

            let mut dir_header = Header::new_gnu();
            dir_header.set_entry_type(tar::EntryType::Directory);
            dir_header.set_mode(0o755);
            dir_header.set_size(0);
            dir_header.set_cksum();
            builder
                .append_data(&mut dir_header, "test-voice-model/", std::io::empty())
                .expect("append top-level model dir");

            for file_name in [
                "encoder-model.int8.onnx",
                "decoder_joint-model.int8.onnx",
                "nemo128.onnx",
                "vocab.txt",
            ] {
                let file_bytes = b"fixture";
                let mut file_header = Header::new_gnu();
                file_header.set_entry_type(tar::EntryType::Regular);
                file_header.set_mode(0o644);
                file_header.set_size(file_bytes.len() as u64);
                file_header.set_cksum();
                builder
                    .append_data(
                        &mut file_header,
                        format!("test-voice-model/{file_name}"),
                        std::io::Cursor::new(file_bytes),
                    )
                    .expect("append model file");
            }

            builder.finish().expect("finish tar");
            builder.into_inner().expect("finish gzip");
        }

        archive
    }

    fn unexpected_layout_archive_bytes() -> Vec<u8> {
        let mut archive = Vec::new();
        {
            let encoder = GzEncoder::new(&mut archive, Compression::default());
            let mut builder = Builder::new(encoder);
            let bytes = b"not a model";
            let mut header = Header::new_gnu();
            header.set_entry_type(tar::EntryType::Regular);
            header.set_mode(0o644);
            header.set_size(bytes.len() as u64);
            header.set_cksum();
            builder
                .append_data(
                    &mut header,
                    "unexpected/readme.txt",
                    std::io::Cursor::new(bytes),
                )
                .expect("append unexpected file");
            builder.finish().expect("finish tar");
            builder.into_inner().expect("finish gzip");
        }
        archive
    }

    fn root_files_archive_bytes() -> Vec<u8> {
        let mut archive = Vec::new();
        {
            let encoder = GzEncoder::new(&mut archive, Compression::default());
            let mut builder = Builder::new(encoder);
            for file_name in [
                "encoder-model.int8.onnx",
                "decoder_joint-model.int8.onnx",
                "nemo128.onnx",
                "vocab.txt",
            ] {
                let bytes = b"fixture";
                let mut header = Header::new_gnu();
                header.set_entry_type(tar::EntryType::Regular);
                header.set_mode(0o644);
                header.set_size(bytes.len() as u64);
                header.set_cksum();
                builder
                    .append_data(&mut header, file_name, std::io::Cursor::new(bytes))
                    .expect("append root model file");
            }
            builder.finish().expect("finish tar");
            builder.into_inner().expect("finish gzip");
        }
        archive
    }

    fn symlink_archive_bytes() -> Vec<u8> {
        let mut archive = Vec::new();
        {
            let encoder = GzEncoder::new(&mut archive, Compression::default());
            let mut builder = Builder::new(encoder);
            let mut header = Header::new_gnu();
            header.set_entry_type(tar::EntryType::Symlink);
            header.set_mode(0o777);
            header.set_size(0);
            header.set_link_name("/tmp/outside").expect("link target");
            header.set_cksum();
            builder
                .append_data(&mut header, "model/link", std::io::empty())
                .expect("append symlink");
            builder.finish().expect("finish tar");
            builder.into_inner().expect("finish gzip");
        }
        archive
    }

    fn sha256_bytes(bytes: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        hex::encode(hasher.finalize())
    }

    fn write_parakeet_runtime_files(root: &Path) {
        fs::create_dir_all(root).expect("Parakeet model dir");
        for file_name in [
            "encoder-model.int8.onnx",
            "decoder_joint-model.int8.onnx",
            "nemo128.onnx",
            "vocab.txt",
        ] {
            fs::write(root.join(file_name), b"fixture").expect("Parakeet runtime file");
        }
    }

    #[tokio::test]
    async fn already_installed_model_is_not_downloaded() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let layout = test_layout(&temp_dir);
        let entry = test_entry_with_sha256("0".repeat(64));
        write_parakeet_runtime_files(layout.model_data_dir.as_path());
        fs::create_dir_all(layout.downloads_dir.as_path()).expect("downloads dir");
        fs::write(layout.archive_path.as_path(), b"cached archive").expect("archive");
        fs::write(layout.partial_archive_path.as_path(), b"cached partial").expect("partial");
        write_ready_marker(&entry, &layout).expect("ready marker");

        let calls = Arc::new(AtomicUsize::new(0));
        let downloader = MemoryArchiveDownloader {
            bytes: Vec::new(),
            calls: calls.clone(),
        };

        let report = ensure_voice_model_installed_at(&entry, layout, &downloader)
            .await
            .expect("install report");

        assert_eq!(report.status, VoiceModelInstallStatus::AlreadyInstalled);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(!report.layout.archive_path.exists());
        assert!(!report.layout.partial_archive_path.exists());
    }

    #[tokio::test]
    async fn missing_model_downloads_verifies_extracts_and_marks_ready() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let layout = test_layout(&temp_dir);
        let archive = test_archive_bytes();
        let entry = test_entry_with_sha256(sha256_bytes(archive.as_slice()));
        let calls = Arc::new(AtomicUsize::new(0));
        let downloader = MemoryArchiveDownloader {
            bytes: archive,
            calls: calls.clone(),
        };

        let report = ensure_voice_model_installed_at(&entry, layout.clone(), &downloader)
            .await
            .expect("install report");

        assert_eq!(report.status, VoiceModelInstallStatus::Installed);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(!layout.archive_path.exists());
        assert!(!layout.partial_archive_path.exists());
        assert!(
            layout
                .model_data_dir
                .join("encoder-model.int8.onnx")
                .is_file()
        );
        assert!(layout.ready_marker_path.is_file());
    }

    #[tokio::test]
    async fn missing_model_accepts_handy_style_top_level_directory_archive() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let mut layout = test_layout(&temp_dir);
        layout.model_data_dir = layout.install_dir.clone();
        let archive = handy_style_top_level_archive_bytes();
        let entry = VoiceModelCatalogEntry {
            model_data_dir_name: "",
            sha256: Box::leak(sha256_bytes(archive.as_slice()).into_boxed_str()),
            ..TEST_ENTRY
        };
        let calls = Arc::new(AtomicUsize::new(0));
        let downloader = MemoryArchiveDownloader {
            bytes: archive,
            calls: calls.clone(),
        };

        let report = ensure_voice_model_installed_at(&entry, layout.clone(), &downloader)
            .await
            .expect("install report");

        assert_eq!(report.status, VoiceModelInstallStatus::Installed);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(!layout.archive_path.exists());
        assert!(!layout.partial_archive_path.exists());
        assert!(layout.install_dir.join("encoder-model.int8.onnx").is_file());
        assert!(layout.ready_marker_path.is_file());
        assert!(!layout.staging_dir.exists());
    }

    #[tokio::test]
    async fn model_install_tar_accepts_explicit_root_file_layout() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let mut layout = test_layout(&temp_dir);
        layout.model_data_dir = layout.install_dir.clone();
        let archive = root_files_archive_bytes();
        let entry = VoiceModelCatalogEntry {
            model_data_dir_name: "",
            sha256: Box::leak(sha256_bytes(archive.as_slice()).into_boxed_str()),
            ..TEST_ENTRY
        };
        let downloader = MemoryArchiveDownloader {
            bytes: archive,
            calls: Arc::new(AtomicUsize::new(0)),
        };

        ensure_voice_model_installed_at(&entry, layout.clone(), &downloader)
            .await
            .expect("root-file tar install");

        assert!(layout.install_dir.join("encoder-model.int8.onnx").is_file());
        assert!(layout.ready_marker_path.is_file());
        assert!(!layout.staging_dir.exists());
    }

    #[tokio::test]
    async fn model_install_unsafe_archive_rejects_link_before_promotion() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let layout = test_layout(&temp_dir);
        let archive = symlink_archive_bytes();
        let entry = test_entry_with_sha256(sha256_bytes(archive.as_slice()));
        let downloader = MemoryArchiveDownloader {
            bytes: archive,
            calls: Arc::new(AtomicUsize::new(0)),
        };

        let error = ensure_voice_model_installed_at(&entry, layout.clone(), &downloader)
            .await
            .expect_err("link archive must fail");

        assert!(format!("{error:#}").contains("link entries"));
        assert!(!layout.staging_dir.exists());
        assert!(!layout.install_dir.exists());
        assert!(!layout.ready_marker_path.exists());
    }

    #[test]
    fn model_install_unsafe_archive_rejects_paths_links_and_special_files() {
        for path in [
            Path::new("../escape"),
            Path::new("/absolute"),
            Path::new("safe/../../escape"),
        ] {
            assert!(reject_unsafe_archive_member(tar::EntryType::Regular, path).is_err());
        }
        for entry_type in [
            tar::EntryType::Symlink,
            tar::EntryType::Link,
            tar::EntryType::Char,
            tar::EntryType::Block,
            tar::EntryType::Fifo,
            tar::EntryType::Continuous,
        ] {
            assert!(reject_unsafe_archive_member(entry_type, Path::new("model/item")).is_err());
        }
        assert!(
            reject_unsafe_archive_member(tar::EntryType::Regular, Path::new("model/file")).is_ok()
        );
        assert!(
            reject_unsafe_archive_member(tar::EntryType::Directory, Path::new("model")).is_ok()
        );
    }

    #[tokio::test]
    async fn bad_checksum_does_not_mark_model_ready() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let layout = test_layout(&temp_dir);
        let archive = test_archive_bytes();
        let entry = test_entry_with_sha256("f".repeat(64));
        let calls = Arc::new(AtomicUsize::new(0));
        let downloader = MemoryArchiveDownloader {
            bytes: archive,
            calls,
        };

        let error = ensure_voice_model_installed_at(&entry, layout.clone(), &downloader)
            .await
            .expect_err("checksum must fail");

        assert!(format!("{error:#}").contains("checksum mismatch"));
        assert!(!layout.ready_marker_path.exists());
        assert!(!layout.partial_archive_path.exists());
        assert!(!layout.staging_dir.exists());
        assert!(!layout.install_dir.exists());
    }

    #[tokio::test]
    async fn stale_partial_download_is_restarted_cleanly() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let layout = test_layout(&temp_dir);
        fs::create_dir_all(layout.downloads_dir.as_path()).expect("downloads");
        fs::write(layout.partial_archive_path.as_path(), b"interrupted").expect("partial");
        let archive = test_archive_bytes();
        let entry = test_entry_with_sha256(sha256_bytes(archive.as_slice()));
        let calls = Arc::new(AtomicUsize::new(0));
        let downloader = MemoryArchiveDownloader {
            bytes: archive,
            calls: calls.clone(),
        };

        let report = ensure_voice_model_installed_at(&entry, layout.clone(), &downloader)
            .await
            .expect("install report");

        assert_eq!(report.status, VoiceModelInstallStatus::Installed);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(!layout.archive_path.exists());
        assert!(!layout.partial_archive_path.exists());
        assert!(layout.ready_marker_path.is_file());
    }

    #[tokio::test]
    async fn model_install_cleanup_stale_completed_download_is_restarted_cleanly() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let layout = test_layout(&temp_dir);
        fs::create_dir_all(layout.downloads_dir.as_path()).expect("downloads");
        fs::write(layout.archive_path.as_path(), b"stale completed artifact")
            .expect("stale artifact");
        let archive = test_archive_bytes();
        let entry = test_entry_with_sha256(sha256_bytes(archive.as_slice()));
        let calls = Arc::new(AtomicUsize::new(0));
        let downloader = MemoryArchiveDownloader {
            bytes: archive,
            calls: calls.clone(),
        };

        ensure_voice_model_installed_at(&entry, layout.clone(), &downloader)
            .await
            .expect("fresh install");

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(!layout.archive_path.exists());
        assert!(!layout.partial_archive_path.exists());
        assert!(layout.ready_marker_path.is_file());
    }

    #[tokio::test]
    async fn model_install_cleanup_cancellation_removes_transient_artifacts_and_install() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let layout = test_layout(&temp_dir);
        let archive = test_archive_bytes();
        let entry = test_entry_with_sha256(sha256_bytes(archive.as_slice()));
        let cancellation = CancellationToken::new();
        let progress = Arc::new(Mutex::new(Vec::new()));
        let progress_sink = progress.clone();
        let control = VoiceModelInstallControl::new(
            cancellation,
            Arc::new(move |event| progress_sink.lock().unwrap().push(event)),
        );
        let downloader = CancellingArchiveDownloader { bytes: archive };

        let error = ensure_voice_model_installed_at_with_control(
            &entry,
            layout.clone(),
            &downloader,
            &control,
        )
        .await
        .expect_err("cancelled install must fail");

        assert!(format!("{error:#}").contains("cancelled"));
        assert!(!progress.lock().unwrap().is_empty());
        assert!(!layout.partial_archive_path.exists());
        assert!(!layout.archive_path.exists());
        assert!(!layout.staging_dir.exists());
        assert!(!layout.install_dir.exists());
        assert!(!layout.ready_marker_path.exists());
    }

    #[test]
    fn model_install_validation_fails_closed_for_directory_marker_and_runtime_mismatches() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let layout = test_layout(&temp_dir);
        let entry = test_entry_with_sha256("a".repeat(64));

        write_parakeet_runtime_files(layout.model_data_dir.as_path());
        assert!(!is_voice_model_installed_and_verified(&entry, &layout));

        fs::write(
            layout.ready_marker_path.as_path(),
            serde_json::to_vec(&VoiceModelReadyMarker {
                id: "wrong-model".to_owned(),
                version: entry.version.to_owned(),
                sha256: entry.sha256.to_owned(),
            })
            .unwrap(),
        )
        .expect("wrong marker");
        assert!(!is_voice_model_installed_and_verified(&entry, &layout));

        fs::write(
            layout.ready_marker_path.as_path(),
            serde_json::to_vec(&VoiceModelReadyMarker {
                id: entry.id.to_owned(),
                version: "wrong-version".to_owned(),
                sha256: entry.sha256.to_owned(),
            })
            .unwrap(),
        )
        .expect("wrong version marker");
        assert!(!is_voice_model_installed_and_verified(&entry, &layout));

        fs::write(
            layout.ready_marker_path.as_path(),
            serde_json::to_vec(&VoiceModelReadyMarker {
                id: entry.id.to_owned(),
                version: entry.version.to_owned(),
                sha256: "wrong-checksum".to_owned(),
            })
            .unwrap(),
        )
        .expect("wrong checksum marker");
        assert!(!is_voice_model_installed_and_verified(&entry, &layout));

        fs::write(
            layout.ready_marker_path.as_path(),
            serde_json::to_vec(&VoiceModelReadyMarker {
                id: entry.id.to_owned(),
                version: entry.version.to_owned(),
                sha256: entry.sha256.to_owned(),
            })
            .unwrap(),
        )
        .expect("valid marker");
        fs::remove_file(layout.model_data_dir.join("vocab.txt")).expect("remove required file");
        assert!(!is_voice_model_installed_and_verified(&entry, &layout));
    }

    #[test]
    fn model_install_legacy_parakeet_accepts_current_layout_and_marker() {
        let config = AppConfig::load().expect("config");
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let entry = super::super::model_catalog::parakeet_v3_int8_catalog_entry();
        let layout = voice_model_install_layout(&entry, &config, temp_dir.path()).expect("layout");
        write_parakeet_runtime_files(layout.install_dir.as_path());
        fs::write(
            layout.ready_marker_path.as_path(),
            serde_json::to_vec_pretty(&VoiceModelReadyMarker {
                id: "parakeet-tdt-0.6b-v3".to_owned(),
                version: "parakeet-tdt-0.6b-v3-int8".to_owned(),
                sha256: "43d37191602727524a7d8c6da0eef11c4ba24320f5b4730f1a2497befc2efa77"
                    .to_owned(),
            })
            .unwrap(),
        )
        .expect("legacy marker");

        assert!(is_voice_model_installed_and_verified(&entry, &layout));
    }

    #[tokio::test]
    async fn model_install_retry_removes_only_selected_trusted_install_and_redownloads() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let layout = test_layout(&temp_dir);
        let archive = test_archive_bytes();
        let entry = test_entry_with_sha256(sha256_bytes(archive.as_slice()));
        write_parakeet_runtime_files(layout.model_data_dir.as_path());
        write_ready_marker(&entry, &layout).expect("old ready install");
        fs::write(layout.model_data_dir.join("old-only.bin"), b"old").expect("old file");
        let unrelated = layout.models_root.join("custom-user-model");
        fs::create_dir_all(unrelated.as_path()).expect("custom model");
        fs::write(unrelated.join("keep.bin"), b"keep").expect("custom file");
        fs::create_dir_all(layout.downloads_dir.as_path()).expect("downloads");
        fs::write(layout.archive_path.as_path(), b"stale").expect("stale artifact");
        fs::write(layout.partial_archive_path.as_path(), b"stale").expect("stale partial");
        let calls = Arc::new(AtomicUsize::new(0));
        let downloader = MemoryArchiveDownloader {
            bytes: archive,
            calls: calls.clone(),
        };

        let report = force_fresh_voice_model_install_at(
            &entry,
            layout.clone(),
            &downloader,
            &VoiceModelInstallControl::default(),
        )
        .await
        .expect("fresh retry");

        assert_eq!(report.status, VoiceModelInstallStatus::Installed);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(is_voice_model_installed_and_verified(&entry, &layout));
        assert!(!layout.model_data_dir.join("old-only.bin").exists());
        assert!(unrelated.join("keep.bin").is_file());
        assert!(!layout.archive_path.exists());
        assert!(!layout.partial_archive_path.exists());
    }

    #[tokio::test]
    async fn model_install_retry_rejects_non_catalog_install_path_without_deleting_it() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let mut layout = test_layout(&temp_dir);
        let unrelated = layout.models_root.join("custom-user-model");
        fs::create_dir_all(unrelated.as_path()).expect("custom model");
        fs::write(unrelated.join("keep.bin"), b"keep").expect("custom file");
        layout.install_dir = unrelated.clone();
        layout.model_data_dir = unrelated.join("model");
        layout.ready_marker_path = unrelated.join(".ready");
        let archive = test_archive_bytes();
        let entry = test_entry_with_sha256(sha256_bytes(archive.as_slice()));
        let downloader = MemoryArchiveDownloader {
            bytes: archive,
            calls: Arc::new(AtomicUsize::new(0)),
        };

        let error = force_fresh_voice_model_install_at(
            &entry,
            layout,
            &downloader,
            &VoiceModelInstallControl::default(),
        )
        .await
        .expect_err("untrusted install path must fail");

        assert!(format!("{error:#}").contains("trusted catalog entry"));
        assert!(unrelated.join("keep.bin").is_file());
    }

    #[tokio::test]
    async fn model_install_retry_rejects_modified_catalog_metadata_before_deletion() {
        let config = AppConfig::load().expect("config");
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let trusted = super::super::model_catalog::parakeet_v3_int8_catalog_entry();
        let layout =
            voice_model_install_layout(&trusted, &config, temp_dir.path()).expect("layout");
        write_parakeet_runtime_files(layout.install_dir.as_path());
        fs::write(layout.ready_marker_path.as_path(), b"preserve").expect("existing marker");
        let modified = VoiceModelCatalogEntry {
            url: "https://untrusted.invalid/model.tar.gz",
            ..trusted
        };
        let downloader = MemoryArchiveDownloader {
            bytes: test_archive_bytes(),
            calls: Arc::new(AtomicUsize::new(0)),
        };

        let error = force_fresh_voice_model_install(
            &modified,
            &config,
            temp_dir.path(),
            &downloader,
            &VoiceModelInstallControl::default(),
        )
        .await
        .expect_err("modified metadata must fail");

        assert!(format!("{error:#}").contains("trusted catalog entry"));
        assert!(layout.install_dir.is_dir());
        assert_eq!(fs::read(layout.ready_marker_path).unwrap(), b"preserve");
    }

    #[tokio::test]
    async fn model_install_retry_pre_cancel_preserves_selected_install() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let layout = test_layout(&temp_dir);
        let archive = test_archive_bytes();
        let entry = test_entry_with_sha256(sha256_bytes(archive.as_slice()));
        write_parakeet_runtime_files(layout.model_data_dir.as_path());
        write_ready_marker(&entry, &layout).expect("ready install");
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let control = VoiceModelInstallControl::new(cancellation, Arc::new(|_| {}));
        let downloader = MemoryArchiveDownloader {
            bytes: archive,
            calls: Arc::new(AtomicUsize::new(0)),
        };

        let error =
            force_fresh_voice_model_install_at(&entry, layout.clone(), &downloader, &control)
                .await
                .expect_err("pre-cancelled retry must fail");

        assert!(format!("{error:#}").contains("cancelled"));
        assert!(is_voice_model_installed_and_verified(&entry, &layout));
    }

    #[tokio::test]
    async fn model_download_progress_is_monotonic_bounded_and_throttled() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let destination = temp_dir.path().join("model.partial");
        let body = b"first-second-third".to_vec();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let address = listener.local_addr().expect("server address");
        let server_body = body.clone();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept request");
            let mut request = [0_u8; 1024];
            let _ = stream.read(&mut request).await.expect("read request");
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                server_body.len()
            );
            stream.write_all(header.as_bytes()).await.expect("headers");
            let first = server_body.len() / 3;
            let second = first * 2;
            stream
                .write_all(&server_body[..first])
                .await
                .expect("chunk 1");
            tokio::time::sleep(Duration::from_millis(275)).await;
            stream
                .write_all(&server_body[first..second])
                .await
                .expect("chunk 2");
            tokio::time::sleep(Duration::from_millis(275)).await;
            stream
                .write_all(&server_body[second..])
                .await
                .expect("chunk 3");
        });
        let url: &'static str = Box::leak(format!("http://{address}/model").into_boxed_str());
        let entry = VoiceModelCatalogEntry { url, ..TEST_ENTRY };
        let events = Arc::new(Mutex::new(Vec::new()));
        let event_sink = events.clone();
        let control = VoiceModelInstallControl::new(
            CancellationToken::new(),
            Arc::new(move |event| event_sink.lock().unwrap().push((Instant::now(), event))),
        );

        ReqwestVoiceModelArchiveDownloader::new()
            .download_archive(&entry, destination.as_path(), &control)
            .await
            .expect("streamed download");
        server.await.expect("test server");

        assert_eq!(fs::read(destination).unwrap(), body);
        let events = events.lock().unwrap();
        assert!(events.len() >= 3);
        assert_eq!(events.first().unwrap().1.downloaded_bytes, 0);
        assert_eq!(events.last().unwrap().1.downloaded_bytes, body.len() as u64);
        for pair in events.windows(2) {
            assert!(pair[0].1.downloaded_bytes <= pair[1].1.downloaded_bytes);
        }
        for (_, event) in events.iter() {
            assert_eq!(event.phase, VoiceModelInstallPhase::Downloading);
            assert!(event.downloaded_bytes <= body.len() as u64);
            assert_eq!(event.total_bytes, Some(body.len() as u64));
        }
        for pair in events[..events.len() - 1].windows(2) {
            assert!(pair[1].0.duration_since(pair[0].0) >= DOWNLOAD_PROGRESS_INTERVAL);
        }
    }

    #[tokio::test]
    async fn model_install_staging_invalid_layout_is_cleaned_without_ready_marker() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let layout = test_layout(&temp_dir);
        let archive = unexpected_layout_archive_bytes();
        let entry = test_entry_with_sha256(sha256_bytes(archive.as_slice()));
        let downloader = MemoryArchiveDownloader {
            bytes: archive,
            calls: Arc::new(AtomicUsize::new(0)),
        };

        let error = ensure_voice_model_installed_at(&entry, layout.clone(), &downloader)
            .await
            .expect_err("unexpected layout must fail");

        assert!(format!("{error:#}").contains("expected runtime layout"));
        assert!(!layout.staging_dir.exists());
        assert!(!layout.install_dir.exists());
        assert!(!layout.ready_marker_path.exists());
    }

    #[tokio::test]
    async fn model_install_staging_refuses_to_remove_unowned_directory() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let layout = test_layout(&temp_dir);
        fs::create_dir_all(layout.staging_dir.as_path()).expect("unowned staging");
        let unrelated = layout.staging_dir.join("keep.txt");
        fs::write(unrelated.as_path(), b"keep").expect("unrelated staging content");
        let archive = test_archive_bytes();
        let entry = test_entry_with_sha256(sha256_bytes(archive.as_slice()));
        let downloader = MemoryArchiveDownloader {
            bytes: archive,
            calls: Arc::new(AtomicUsize::new(0)),
        };

        let error = ensure_voice_model_installed_at(&entry, layout.clone(), &downloader)
            .await
            .expect_err("unowned staging must be refused");

        assert!(format!("{error:#}").contains("unowned"));
        assert!(unrelated.is_file());
        assert!(!layout.ready_marker_path.exists());
    }

    #[tokio::test]
    async fn model_install_staging_refuses_to_overwrite_unrelated_install() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let layout = test_layout(&temp_dir);
        fs::create_dir_all(layout.install_dir.as_path()).expect("unrelated install");
        let unrelated = layout.install_dir.join("keep.txt");
        fs::write(unrelated.as_path(), b"keep").expect("unrelated install content");
        let archive = test_archive_bytes();
        let entry = test_entry_with_sha256(sha256_bytes(archive.as_slice()));
        let downloader = MemoryArchiveDownloader {
            bytes: archive,
            calls: Arc::new(AtomicUsize::new(0)),
        };

        let error = ensure_voice_model_installed_at(&entry, layout.clone(), &downloader)
            .await
            .expect_err("unrelated install must be refused");

        assert!(format!("{error:#}").contains("refusing to overwrite"));
        assert!(unrelated.is_file());
        assert!(!layout.ready_marker_path.exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn model_install_staging_refuses_symlink_destination() {
        use std::os::unix::fs::symlink;

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let layout = test_layout(&temp_dir);
        fs::create_dir_all(layout.models_root.as_path()).expect("models root");
        let outside = temp_dir.path().join("outside");
        fs::create_dir_all(outside.as_path()).expect("outside");
        symlink(outside.as_path(), layout.install_dir.as_path()).expect("install symlink");
        let archive = test_archive_bytes();
        let entry = test_entry_with_sha256(sha256_bytes(archive.as_slice()));
        let downloader = MemoryArchiveDownloader {
            bytes: archive,
            calls: Arc::new(AtomicUsize::new(0)),
        };

        let error = ensure_voice_model_installed_at(&entry, layout.clone(), &downloader)
            .await
            .expect_err("symlink install must be refused");

        assert!(format!("{error:#}").contains("must not be a symlink"));
        assert!(layout.install_dir.is_symlink());
        assert!(!outside.join(".ready").exists());
    }

    #[tokio::test]
    async fn model_install_single_file_materializes_runtime_file_and_ready_marker() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let layout = single_file_layout(&temp_dir);
        let bytes = b"whisper model".to_vec();
        let entry = single_file_entry_with_sha256(sha256_bytes(bytes.as_slice()));
        let downloader = MemoryArchiveDownloader {
            bytes: bytes.clone(),
            calls: Arc::new(AtomicUsize::new(0)),
        };

        let report = ensure_voice_model_installed_at(&entry, layout.clone(), &downloader)
            .await
            .expect("single-file install");

        assert_eq!(report.status, VoiceModelInstallStatus::Installed);
        assert_eq!(fs::read(layout.model_data_dir.as_path()).unwrap(), bytes);
        assert!(layout.ready_marker_path.is_file());
        assert!(!layout.archive_path.exists());
        assert!(!layout.partial_archive_path.exists());
        assert!(!layout.staging_dir.exists());
        assert!(is_voice_model_installed_and_verified(&entry, &layout));
    }

    #[tokio::test]
    async fn model_install_single_file_rejects_empty_artifact_without_install() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let layout = single_file_layout(&temp_dir);
        let entry = single_file_entry_with_sha256(sha256_bytes(&[]));
        let downloader = MemoryArchiveDownloader {
            bytes: Vec::new(),
            calls: Arc::new(AtomicUsize::new(0)),
        };

        let error = ensure_voice_model_installed_at(&entry, layout.clone(), &downloader)
            .await
            .expect_err("empty artifact must fail");

        assert!(format!("{error:#}").contains("non-empty regular file"));
        assert!(!layout.install_dir.exists());
        assert!(!layout.staging_dir.exists());
        assert!(!layout.ready_marker_path.exists());
    }

    #[test]
    fn model_install_single_file_rejects_directory_and_wrong_layout() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let mut layout = single_file_layout(&temp_dir);
        fs::create_dir_all(layout.archive_path.as_path()).expect("artifact directory");
        let entry = single_file_entry_with_sha256("0".repeat(64));
        assert!(
            materialize_single_file(&entry, &layout, &VoiceModelInstallControl::default()).is_err()
        );
        assert!(!layout.install_dir.exists());

        fs::remove_dir_all(layout.archive_path.as_path()).expect("remove artifact directory");
        fs::write(layout.archive_path.as_path(), b"model").expect("artifact file");
        layout.model_data_dir = layout.install_dir.join("wrong.bin");
        assert!(
            materialize_single_file(&entry, &layout, &VoiceModelInstallControl::default()).is_err()
        );
        assert!(!layout.install_dir.exists());
        assert!(!layout.staging_dir.exists());
    }

    #[cfg(unix)]
    #[test]
    fn model_install_single_file_rejects_symlink_artifact() {
        use std::os::unix::fs::symlink;

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let layout = single_file_layout(&temp_dir);
        fs::create_dir_all(layout.downloads_dir.as_path()).expect("downloads");
        let source = temp_dir.path().join("outside.bin");
        fs::write(source.as_path(), b"model").expect("source file");
        symlink(source.as_path(), layout.archive_path.as_path()).expect("artifact symlink");
        let entry = single_file_entry_with_sha256("0".repeat(64));

        let error = materialize_single_file(&entry, &layout, &VoiceModelInstallControl::default())
            .expect_err("symlink must fail");

        assert!(format!("{error:#}").contains("must not be a symlink"));
        assert!(!layout.install_dir.exists());
        assert!(!layout.staging_dir.exists());
    }
}
