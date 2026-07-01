use super::model_catalog::{
    VoiceModelArchiveType, VoiceModelCatalogEntry, VoiceModelInstallLayout,
    voice_model_install_layout,
};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use flate2::read::GzDecoder;
use futures_util::StreamExt;
use pioneer_config::AppConfig;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::fs;
use std::io::{BufReader, Read};
use std::path::{Component, Path};
use tar::Archive;
use tokio::io::AsyncWriteExt;

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
    ) -> Result<()> {
        let response = self
            .client
            .get(entry.url)
            .send()
            .await
            .with_context(|| format!("failed to download voice model {}", entry.id))?
            .error_for_status()
            .with_context(|| format!("voice model download returned an error for {}", entry.id))?;

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
        while let Some(chunk) = stream.next().await {
            let chunk = chunk
                .with_context(|| format!("failed to read voice model download {}", entry.id))?;
            destination_file
                .write_all(chunk.as_ref())
                .await
                .with_context(|| {
                    format!(
                        "failed to write voice model partial archive {}",
                        destination.display()
                    )
                })?;
        }
        destination_file.flush().await.with_context(|| {
            format!(
                "failed to flush voice model partial archive {}",
                destination.display()
            )
        })?;

        Ok(())
    }
}

pub(crate) async fn ensure_voice_model_installed<D>(
    entry: &VoiceModelCatalogEntry,
    config: &AppConfig,
    runtime_home: &Path,
    downloader: &D,
) -> Result<VoiceModelInstallReport>
where
    D: VoiceModelArchiveDownloader + ?Sized,
{
    let layout = voice_model_install_layout(entry, config, runtime_home)?;
    ensure_voice_model_installed_at(entry, layout, downloader).await
}

pub(crate) async fn ensure_voice_model_installed_at<D>(
    entry: &VoiceModelCatalogEntry,
    layout: VoiceModelInstallLayout,
    downloader: &D,
) -> Result<VoiceModelInstallReport>
where
    D: VoiceModelArchiveDownloader + ?Sized,
{
    if is_voice_model_installed_and_verified(entry, &layout) {
        return Ok(VoiceModelInstallReport {
            status: VoiceModelInstallStatus::AlreadyInstalled,
            layout,
        });
    }

    prepare_install_workspace(&layout)?;
    ensure_archive_downloaded(entry, &layout, downloader).await?;

    let archive_hash = sha256_file(layout.archive_path.as_path()).with_context(|| {
        format!(
            "failed to hash voice model archive {}",
            layout.archive_path.display()
        )
    })?;
    if archive_hash != entry.sha256 {
        let _ = fs::remove_file(layout.archive_path.as_path());
        bail!(
            "voice model {} archive checksum mismatch: expected {}, got {}",
            entry.id,
            entry.sha256,
            archive_hash
        );
    }

    extract_voice_model_archive(entry, &layout).with_context(|| {
        format!(
            "failed to extract voice model {} from {}",
            entry.id,
            layout.archive_path.display()
        )
    })?;
    write_ready_marker(entry, &layout)?;

    Ok(VoiceModelInstallReport {
        status: VoiceModelInstallStatus::Installed,
        layout,
    })
}

#[derive(Deserialize)]
struct VoiceModelReadyMarker {
    id: String,
    version: String,
    sha256: String,
}

pub(crate) fn is_voice_model_installed_and_verified(
    entry: &VoiceModelCatalogEntry,
    layout: &VoiceModelInstallLayout,
) -> bool {
    let Ok(marker_bytes) = fs::read(layout.ready_marker_path.as_path()) else {
        return false;
    };
    let Ok(marker) = serde_json::from_slice::<VoiceModelReadyMarker>(marker_bytes.as_slice())
    else {
        return false;
    };

    layout.model_data_dir.is_dir()
        && marker.id == entry.id
        && marker.version == entry.version
        && marker.sha256 == entry.sha256
}

async fn ensure_archive_downloaded<D>(
    entry: &VoiceModelCatalogEntry,
    layout: &VoiceModelInstallLayout,
    downloader: &D,
) -> Result<()>
where
    D: VoiceModelArchiveDownloader + ?Sized,
{
    if layout.archive_path.is_file() {
        let archive_hash = sha256_file(layout.archive_path.as_path()).with_context(|| {
            format!(
                "failed to hash existing voice model archive {}",
                layout.archive_path.display()
            )
        })?;
        if archive_hash == entry.sha256 {
            return Ok(());
        }

        fs::remove_file(layout.archive_path.as_path()).with_context(|| {
            format!(
                "failed to remove invalid voice model archive {}",
                layout.archive_path.display()
            )
        })?;
    }

    if layout.partial_archive_path.exists() {
        fs::remove_file(layout.partial_archive_path.as_path()).with_context(|| {
            format!(
                "failed to remove stale voice model partial archive {}",
                layout.partial_archive_path.display()
            )
        })?;
    }

    downloader
        .download_archive(entry, layout.partial_archive_path.as_path())
        .await?;

    let partial_hash = sha256_file(layout.partial_archive_path.as_path()).with_context(|| {
        format!(
            "failed to hash voice model partial archive {}",
            layout.partial_archive_path.display()
        )
    })?;
    if partial_hash != entry.sha256 {
        let _ = fs::remove_file(layout.partial_archive_path.as_path());
        bail!(
            "voice model {} partial archive checksum mismatch: expected {}, got {}",
            entry.id,
            entry.sha256,
            partial_hash
        );
    }

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

    Ok(())
}

fn prepare_install_workspace(layout: &VoiceModelInstallLayout) -> Result<()> {
    fs::create_dir_all(layout.downloads_dir.as_path()).with_context(|| {
        format!(
            "failed to create voice model downloads directory {}",
            layout.downloads_dir.display()
        )
    })?;
    fs::create_dir_all(layout.models_root.as_path()).with_context(|| {
        format!(
            "failed to create voice model root directory {}",
            layout.models_root.display()
        )
    })?;
    remove_path_if_exists(layout.staging_dir.as_path()).with_context(|| {
        format!(
            "failed to remove stale voice model staging directory {}",
            layout.staging_dir.display()
        )
    })?;
    remove_path_if_exists(layout.install_dir.as_path()).with_context(|| {
        format!(
            "failed to remove incomplete voice model install directory {}",
            layout.install_dir.display()
        )
    })?;

    Ok(())
}

fn extract_voice_model_archive(
    entry: &VoiceModelCatalogEntry,
    layout: &VoiceModelInstallLayout,
) -> Result<()> {
    match entry.archive_type {
        VoiceModelArchiveType::TarGz => extract_tar_gz_archive(layout),
    }
}

fn extract_tar_gz_archive(layout: &VoiceModelInstallLayout) -> Result<()> {
    fs::create_dir_all(layout.staging_dir.as_path()).with_context(|| {
        format!(
            "failed to create voice model staging directory {}",
            layout.staging_dir.display()
        )
    })?;

    let archive_file = fs::File::open(layout.archive_path.as_path()).with_context(|| {
        format!(
            "failed to open voice model archive {}",
            layout.archive_path.display()
        )
    })?;
    let decoder = GzDecoder::new(BufReader::new(archive_file));
    let mut archive = Archive::new(decoder);

    for entry in archive.entries().context("failed to read tar.gz entries")? {
        let mut archive_entry = entry.context("failed to read tar.gz entry")?;
        reject_unsafe_archive_entry(&archive_entry)?;
        archive_entry
            .unpack_in(layout.staging_dir.as_path())
            .with_context(|| {
                format!(
                    "failed to unpack tar.gz entry into {}",
                    layout.staging_dir.display()
                )
            })?;
    }

    promote_extracted_model_directory(layout)?;

    Ok(())
}

fn promote_extracted_model_directory(layout: &VoiceModelInstallLayout) -> Result<()> {
    if layout.model_data_dir == layout.install_dir {
        let expected_dir_name = layout
            .install_dir
            .file_name()
            .context("model install directory must have a final component")?;
        let staged_top_level_model_dir = layout.staging_dir.join(expected_dir_name);
        if staged_top_level_model_dir.is_dir() {
            fs::rename(
                staged_top_level_model_dir.as_path(),
                layout.install_dir.as_path(),
            )
            .with_context(|| {
                format!(
                    "failed to promote extracted voice model directory {} to {}",
                    staged_top_level_model_dir.display(),
                    layout.install_dir.display()
                )
            })?;
            remove_path_if_exists(layout.staging_dir.as_path()).with_context(|| {
                format!(
                    "failed to remove voice model staging directory {}",
                    layout.staging_dir.display()
                )
            })?;
            return Ok(());
        }

        if directory_contains_file(layout.staging_dir.as_path())? {
            fs::rename(layout.staging_dir.as_path(), layout.install_dir.as_path()).with_context(
                || {
                    format!(
                        "failed to promote voice model staging directory {} to {}",
                        layout.staging_dir.display(),
                        layout.install_dir.display()
                    )
                },
            )?;
            return Ok(());
        }
    } else if layout.staging_dir.join("model").is_dir()
        || layout
            .staging_dir
            .join(
                layout
                    .model_data_dir
                    .file_name()
                    .context("model data directory must have a final component")?,
            )
            .is_dir()
    {
        fs::rename(layout.staging_dir.as_path(), layout.install_dir.as_path()).with_context(
            || {
                format!(
                    "failed to promote voice model staging directory {} to {}",
                    layout.staging_dir.display(),
                    layout.install_dir.display()
                )
            },
        )?;
        return Ok(());
    }

    bail!(
        "voice model archive did not contain expected model directory {}",
        layout.model_data_dir.display()
    );
}

fn directory_contains_file(path: &Path) -> Result<bool> {
    for entry in fs::read_dir(path)
        .with_context(|| format!("failed to read directory {}", path.display()))?
    {
        let entry = entry.with_context(|| format!("failed to read entry in {}", path.display()))?;
        if entry.path().is_file() {
            return Ok(true);
        }
    }
    Ok(false)
}

fn reject_unsafe_archive_entry<R: Read>(entry: &tar::Entry<'_, R>) -> Result<()> {
    let entry_type = entry.header().entry_type();
    if entry_type.is_symlink() || entry_type.is_hard_link() {
        bail!("voice model archive contains link entries, which are not supported");
    }

    let entry_path = entry.path().context("failed to read tar.gz entry path")?;
    if entry_path.as_ref().as_os_str().is_empty() {
        bail!("voice model archive contains an empty path");
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
    let marker = serde_json::json!({
        "id": entry.id,
        "version": entry.version,
        "sha256": entry.sha256,
    });
    fs::write(
        layout.ready_marker_path.as_path(),
        serde_json::to_vec_pretty(&marker)?,
    )
    .with_context(|| {
        format!(
            "failed to write voice model ready marker {}",
            layout.ready_marker_path.display()
        )
    })?;

    Ok(())
}

fn sha256_file(path: &Path) -> Result<String> {
    let mut file = fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];

    loop {
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
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tar::{Builder, Header};
    use tempfile::TempDir;

    const TEST_ENTRY: VoiceModelCatalogEntry = VoiceModelCatalogEntry {
        id: "test-voice-model",
        display_name: "Test voice model",
        version: "test-v1",
        url: "memory://test-voice-model.tar.gz",
        sha256: "",
        archive_type: VoiceModelArchiveType::TarGz,
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
        ) -> Result<()> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            tokio::fs::write(destination, self.bytes.as_slice())
                .await
                .with_context(|| format!("failed to write test archive {}", destination.display()))
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

            let file_bytes = b"weights";
            let mut file_header = Header::new_gnu();
            file_header.set_entry_type(tar::EntryType::Regular);
            file_header.set_mode(0o644);
            file_header.set_size(file_bytes.len() as u64);
            file_header.set_cksum();
            builder
                .append_data(
                    &mut file_header,
                    "model/weights.bin",
                    std::io::Cursor::new(file_bytes),
                )
                .expect("append model file");

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

            let file_bytes = b"weights";
            let mut file_header = Header::new_gnu();
            file_header.set_entry_type(tar::EntryType::Regular);
            file_header.set_mode(0o644);
            file_header.set_size(file_bytes.len() as u64);
            file_header.set_cksum();
            builder
                .append_data(
                    &mut file_header,
                    "test-voice-model/weights.bin",
                    std::io::Cursor::new(file_bytes),
                )
                .expect("append model file");

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

    #[tokio::test]
    async fn already_installed_model_is_not_downloaded() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let layout = test_layout(&temp_dir);
        let entry = test_entry_with_sha256("0".repeat(64));
        fs::create_dir_all(layout.model_data_dir.as_path()).expect("model dir");
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
        assert!(layout.archive_path.is_file());
        assert!(!layout.partial_archive_path.exists());
        assert!(layout.model_data_dir.join("weights.bin").is_file());
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
        assert!(layout.model_data_dir.join("weights.bin").is_file());
        assert!(layout.ready_marker_path.is_file());
        assert!(!layout.staging_dir.exists());
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
        assert!(!layout.partial_archive_path.exists());
        assert!(layout.ready_marker_path.is_file());
    }
}
