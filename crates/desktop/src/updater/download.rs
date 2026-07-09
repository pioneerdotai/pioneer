use super::{
    platform::DesktopUpdateCandidate, release::release_asset_download_url,
    state::DesktopUpdateConfig,
};
use reqwest::blocking::Client;
use std::{
    error::Error,
    fmt, fs,
    io::{self, Write as _},
    path::{Component, Path, PathBuf},
};

pub(crate) const DESKTOP_UPDATES_DIR: &str = "desktop-updates";
pub(crate) const DESKTOP_DOWNLOADS_DIR: &str = "downloads";
pub(crate) const PARTIAL_DOWNLOAD_EXTENSION: &str = "part";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StagedDownload {
    pub(crate) tag: String,
    pub(crate) version: String,
    pub(crate) asset_name: String,
    pub(crate) url: String,
    pub(crate) path: PathBuf,
    pub(crate) sha256: String,
    pub(crate) os: String,
    pub(crate) arch: String,
    pub(crate) kind: String,
    pub(crate) size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DesktopUpdateDownloadPaths {
    pub(crate) root_dir: PathBuf,
    pub(crate) downloads_dir: PathBuf,
    pub(crate) version_dir: PathBuf,
    pub(crate) asset_path: PathBuf,
    pub(crate) partial_path: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DesktopDownloadErrorCode {
    InvalidPathComponent,
    CreateDirectory,
    RemovePartial,
    RemoveExistingAsset,
    DownloadRequest,
    DownloadStatus,
    CreatePartial,
    WritePartial,
    RenamePartial,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DesktopDownloadError {
    code: DesktopDownloadErrorCode,
    message: String,
}

impl DesktopDownloadError {
    fn new(code: DesktopDownloadErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    #[cfg(test)]
    pub(crate) fn code(&self) -> DesktopDownloadErrorCode {
        self.code
    }
}

impl fmt::Display for DesktopDownloadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for DesktopDownloadError {}

pub(crate) fn download_update_asset_to_cache_with_runtime_home(
    client: &Client,
    config: &DesktopUpdateConfig,
    candidate: &DesktopUpdateCandidate,
    runtime_home: &Path,
) -> Result<StagedDownload, DesktopDownloadError> {
    let paths = prepare_download_paths(runtime_home, candidate)?;
    let url = release_asset_download_url(
        config,
        candidate.tag.as_str(),
        candidate.asset_name.as_str(),
    );
    let response = client.get(url.as_str()).send().map_err(|error| {
        DesktopDownloadError::new(
            DesktopDownloadErrorCode::DownloadRequest,
            format!("failed to download desktop update asset from `{url}`: {error}"),
        )
    })?;
    let response = response.error_for_status().map_err(|error| {
        DesktopDownloadError::new(
            DesktopDownloadErrorCode::DownloadStatus,
            format!("desktop update asset request failed for `{url}`: {error}"),
        )
    })?;

    if let Err(error) = write_response_to_partial(response, paths.partial_path.as_path()) {
        let _ = fs::remove_file(paths.partial_path.as_path());
        return Err(error);
    }

    if paths.asset_path.is_file() {
        fs::remove_file(paths.asset_path.as_path()).map_err(|error| {
            DesktopDownloadError::new(
                DesktopDownloadErrorCode::RemoveExistingAsset,
                format!(
                    "failed to remove existing desktop update asset `{}`: {error}",
                    paths.asset_path.display()
                ),
            )
        })?;
    }

    fs::rename(paths.partial_path.as_path(), paths.asset_path.as_path()).map_err(|error| {
        DesktopDownloadError::new(
            DesktopDownloadErrorCode::RenamePartial,
            format!(
                "failed to finalize desktop update asset `{}`: {error}",
                paths.asset_path.display()
            ),
        )
    })?;

    Ok(StagedDownload {
        tag: candidate.tag.clone(),
        version: candidate.version.clone(),
        asset_name: candidate.asset_name.clone(),
        url,
        path: paths.asset_path,
        sha256: candidate.sha256.clone(),
        os: candidate.os.clone(),
        arch: candidate.arch.clone(),
        kind: candidate.kind.clone(),
        size_bytes: candidate.size_bytes,
    })
}

pub(crate) fn prepare_download_paths(
    runtime_home: &Path,
    candidate: &DesktopUpdateCandidate,
) -> Result<DesktopUpdateDownloadPaths, DesktopDownloadError> {
    let paths = download_paths_for_runtime_home(runtime_home, candidate)?;
    fs::create_dir_all(paths.version_dir.as_path()).map_err(|error| {
        DesktopDownloadError::new(
            DesktopDownloadErrorCode::CreateDirectory,
            format!(
                "failed to create desktop update download directory `{}`: {error}",
                paths.version_dir.display()
            ),
        )
    })?;

    match fs::remove_file(paths.partial_path.as_path()) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(DesktopDownloadError::new(
                DesktopDownloadErrorCode::RemovePartial,
                format!(
                    "failed to remove stale partial desktop update `{}`: {error}",
                    paths.partial_path.display()
                ),
            ));
        }
    }

    Ok(paths)
}

pub(crate) fn download_paths_for_runtime_home(
    runtime_home: &Path,
    candidate: &DesktopUpdateCandidate,
) -> Result<DesktopUpdateDownloadPaths, DesktopDownloadError> {
    let version_dir_name_raw = format!("v{}", candidate.version);
    let version_dir_name = safe_path_component(version_dir_name_raw.as_str())?;
    let asset_name = safe_path_component(candidate.asset_name.as_str())?;
    let root_dir = runtime_home.join(DESKTOP_UPDATES_DIR);
    let downloads_dir = root_dir.join(DESKTOP_DOWNLOADS_DIR);
    let version_dir = downloads_dir.join(version_dir_name);
    let asset_path = version_dir.join(asset_name);
    let partial_path = version_dir.join(format!("{asset_name}.{PARTIAL_DOWNLOAD_EXTENSION}"));

    Ok(DesktopUpdateDownloadPaths {
        root_dir,
        downloads_dir,
        version_dir,
        asset_path,
        partial_path,
    })
}

fn write_response_to_partial(
    mut response: reqwest::blocking::Response,
    partial_path: &Path,
) -> Result<(), DesktopDownloadError> {
    let mut partial_file = fs::File::create(partial_path).map_err(|error| {
        DesktopDownloadError::new(
            DesktopDownloadErrorCode::CreatePartial,
            format!(
                "failed to create partial desktop update `{}`: {error}",
                partial_path.display()
            ),
        )
    })?;

    io::copy(&mut response, &mut partial_file).map_err(|error| {
        DesktopDownloadError::new(
            DesktopDownloadErrorCode::WritePartial,
            format!(
                "failed to write partial desktop update `{}`: {error}",
                partial_path.display()
            ),
        )
    })?;
    partial_file.flush().map_err(|error| {
        DesktopDownloadError::new(
            DesktopDownloadErrorCode::WritePartial,
            format!(
                "failed to flush partial desktop update `{}`: {error}",
                partial_path.display()
            ),
        )
    })?;

    Ok(())
}

fn safe_path_component(value: &str) -> Result<&str, DesktopDownloadError> {
    let trimmed = value.trim();
    if trimmed.is_empty()
        || trimmed != value
        || trimmed.contains('/')
        || trimmed.contains('\\')
        || trimmed.contains(':')
        || Path::new(trimmed)
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(DesktopDownloadError::new(
            DesktopDownloadErrorCode::InvalidPathComponent,
            format!("invalid desktop update path component: `{value}`"),
        ));
    }

    Ok(trimmed)
}

#[cfg(test)]
mod tests {
    use super::{
        DesktopDownloadErrorCode, download_paths_for_runtime_home, prepare_download_paths,
    };
    use crate::updater::platform::DesktopUpdateCandidate;
    use std::fs;

    #[test]
    fn builds_deterministic_download_paths() {
        let temp_dir = tempfile::tempdir().unwrap();
        let candidate = candidate();

        let paths = download_paths_for_runtime_home(temp_dir.path(), &candidate).unwrap();

        assert_eq!(
            paths.asset_path,
            temp_dir
                .path()
                .join("desktop-updates")
                .join("downloads")
                .join("v0.26.0")
                .join("Pioneer-aarch64.app.zip")
        );
        assert_eq!(
            paths.partial_path,
            temp_dir
                .path()
                .join("desktop-updates")
                .join("downloads")
                .join("v0.26.0")
                .join("Pioneer-aarch64.app.zip.part")
        );
    }

    #[test]
    fn prepare_download_paths_removes_stale_partial_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let candidate = candidate();
        let paths = download_paths_for_runtime_home(temp_dir.path(), &candidate).unwrap();
        fs::create_dir_all(paths.version_dir.as_path()).unwrap();
        fs::write(paths.partial_path.as_path(), b"partial").unwrap();

        let prepared = prepare_download_paths(temp_dir.path(), &candidate).unwrap();

        assert_eq!(prepared.partial_path, paths.partial_path);
        assert!(!prepared.partial_path.exists());
        assert!(prepared.version_dir.is_dir());
    }

    #[test]
    fn rejects_asset_names_that_escape_cache_root() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut candidate = candidate();
        candidate.asset_name = "../Pioneer.app.zip".to_owned();

        let error = download_paths_for_runtime_home(temp_dir.path(), &candidate).unwrap_err();

        assert_eq!(error.code(), DesktopDownloadErrorCode::InvalidPathComponent);
    }

    fn candidate() -> DesktopUpdateCandidate {
        DesktopUpdateCandidate {
            tag: "v0.26.0".to_owned(),
            version: "0.26.0".to_owned(),
            os: "macos".to_owned(),
            arch: "aarch64".to_owned(),
            kind: "macos_app_zip".to_owned(),
            asset_name: "Pioneer-aarch64.app.zip".to_owned(),
            sha256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            size_bytes: 123,
        }
    }
}
