use super::{
    download::{DESKTOP_UPDATES_DIR, StagedDownload},
    manifest::{DESKTOP_UPDATE_PRODUCT, DESKTOP_UPDATE_SCHEMA_VERSION},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    error::Error,
    fmt::{self, Write as _},
    fs,
    io::{Read as _, Write as _},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

pub(crate) const ENV_DESKTOP_UPDATE_DISABLED: &str = "PIONEER_DESKTOP_UPDATE_DISABLED";
pub(crate) const ENV_DESKTOP_UPDATE_FORCE_CHECK: &str = "PIONEER_DESKTOP_UPDATE_FORCE_CHECK";
pub(crate) const ENV_DESKTOP_UPDATE_CHANNEL: &str = "PIONEER_DESKTOP_UPDATE_CHANNEL";
pub(crate) const ENV_RELEASE_REPO: &str = "PIONEER_RELEASE_REPO";
pub(crate) const ENV_RELEASE_API_BASE: &str = "PIONEER_RELEASE_API_BASE";
pub(crate) const ENV_RELEASE_DOWNLOAD_BASE: &str = "PIONEER_RELEASE_DOWNLOAD_BASE";

pub(crate) const DEFAULT_DESKTOP_UPDATE_CHANNEL: &str = "stable";
pub(crate) const DEFAULT_RELEASE_REPO: &str = "pioneerdotai/pioneer";
pub(crate) const DESKTOP_UPDATE_STATE_FILE: &str = "state.json";
pub(crate) const SHA256_MISMATCH_ERROR_CODE: &str = "sha256_mismatch";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DesktopUpdateConfig {
    pub(crate) disabled: bool,
    pub(crate) force_check: bool,
    pub(crate) channel: String,
    pub(crate) release_repo: String,
    pub(crate) release_api_base: String,
    pub(crate) release_download_base: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DesktopUpdateStateFile {
    pub(crate) schema_version: u32,
    pub(crate) product: String,
    #[serde(flatten)]
    pub(crate) status: DesktopUpdatePersistedStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub(crate) enum DesktopUpdatePersistedStatus {
    Ready {
        version: String,
        tag: String,
        asset_path: PathBuf,
        asset_name: String,
        sha256: String,
        os: String,
        arch: String,
        kind: String,
        size_bytes: u64,
        checked_at_unix: u64,
    },
    FailedSilent {
        error_code: String,
        checked_at_unix: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DesktopUpdateStateErrorCode {
    ReadAsset,
    Sha256Mismatch,
    RemoveMismatchedAsset,
    CreateStateDirectory,
    SerializeState,
    WriteState,
    RenameState,
    ReadState,
    ParseState,
    UnsupportedState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DesktopUpdateStateError {
    code: DesktopUpdateStateErrorCode,
    message: String,
}

impl DesktopUpdateStateError {
    fn new(code: DesktopUpdateStateErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    #[cfg(test)]
    pub(crate) fn code(&self) -> DesktopUpdateStateErrorCode {
        self.code
    }
}

impl fmt::Display for DesktopUpdateStateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for DesktopUpdateStateError {}

impl DesktopUpdateConfig {
    pub(crate) fn from_env() -> Self {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    pub(crate) fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Self {
        let release_repo =
            non_empty_or_default(lookup(ENV_RELEASE_REPO).as_deref(), DEFAULT_RELEASE_REPO);
        let release_api_base = non_empty_or_default(
            lookup(ENV_RELEASE_API_BASE).as_deref(),
            &format!("https://api.github.com/repos/{release_repo}/releases"),
        );
        let release_download_base = non_empty_or_default(
            lookup(ENV_RELEASE_DOWNLOAD_BASE).as_deref(),
            &format!("https://github.com/{release_repo}/releases/download"),
        );

        Self {
            disabled: env_flag_enabled(lookup(ENV_DESKTOP_UPDATE_DISABLED).as_deref()),
            force_check: env_flag_enabled(lookup(ENV_DESKTOP_UPDATE_FORCE_CHECK).as_deref()),
            channel: normalize_channel(lookup(ENV_DESKTOP_UPDATE_CHANNEL).as_deref()),
            release_repo,
            release_api_base,
            release_download_base,
        }
    }
}

pub(crate) fn verify_staged_download_and_record_ready_state(
    runtime_home: &Path,
    staged: &StagedDownload,
) -> Result<DesktopUpdateStateFile, DesktopUpdateStateError> {
    verify_staged_download_and_record_ready_state_at(runtime_home, staged, current_unix_timestamp())
}

pub(crate) fn verify_staged_download_and_record_ready_state_at(
    runtime_home: &Path,
    staged: &StagedDownload,
    checked_at_unix: u64,
) -> Result<DesktopUpdateStateFile, DesktopUpdateStateError> {
    let actual_sha256 = sha256_file(staged.path.as_path())?;
    if actual_sha256 != staged.sha256 {
        match fs::remove_file(staged.path.as_path()) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(DesktopUpdateStateError::new(
                    DesktopUpdateStateErrorCode::RemoveMismatchedAsset,
                    format!(
                        "failed to remove SHA256-mismatched desktop update `{}`: {error}",
                        staged.path.display()
                    ),
                ));
            }
        }

        record_silent_failure_state_at(runtime_home, SHA256_MISMATCH_ERROR_CODE, checked_at_unix)?;
        return Err(DesktopUpdateStateError::new(
            DesktopUpdateStateErrorCode::Sha256Mismatch,
            format!(
                "desktop update SHA256 mismatch for `{}`: expected {}, got {}",
                staged.path.display(),
                staged.sha256,
                actual_sha256
            ),
        ));
    }

    let state = DesktopUpdateStateFile {
        schema_version: DESKTOP_UPDATE_SCHEMA_VERSION,
        product: DESKTOP_UPDATE_PRODUCT.to_owned(),
        status: DesktopUpdatePersistedStatus::Ready {
            version: staged.version.clone(),
            tag: staged.tag.clone(),
            asset_path: staged.path.clone(),
            asset_name: staged.asset_name.clone(),
            sha256: staged.sha256.clone(),
            os: staged.os.clone(),
            arch: staged.arch.clone(),
            kind: staged.kind.clone(),
            size_bytes: staged.size_bytes,
            checked_at_unix,
        },
    };
    write_update_state(runtime_home, &state)?;
    Ok(state)
}

pub(crate) fn record_silent_failure_state_at(
    runtime_home: &Path,
    error_code: &str,
    checked_at_unix: u64,
) -> Result<DesktopUpdateStateFile, DesktopUpdateStateError> {
    let state = DesktopUpdateStateFile {
        schema_version: DESKTOP_UPDATE_SCHEMA_VERSION,
        product: DESKTOP_UPDATE_PRODUCT.to_owned(),
        status: DesktopUpdatePersistedStatus::FailedSilent {
            error_code: error_code.to_owned(),
            checked_at_unix,
        },
    };
    write_update_state(runtime_home, &state)?;
    Ok(state)
}

pub(crate) fn read_update_state(
    runtime_home: &Path,
) -> Result<Option<DesktopUpdateStateFile>, DesktopUpdateStateError> {
    let path = update_state_path(runtime_home);
    if !path.is_file() {
        return Ok(None);
    }

    let bytes = fs::read(path.as_path()).map_err(|error| {
        DesktopUpdateStateError::new(
            DesktopUpdateStateErrorCode::ReadState,
            format!(
                "failed to read desktop update state `{}`: {error}",
                path.display()
            ),
        )
    })?;
    let state: DesktopUpdateStateFile =
        serde_json::from_slice(bytes.as_slice()).map_err(|error| {
            DesktopUpdateStateError::new(
                DesktopUpdateStateErrorCode::ParseState,
                format!(
                    "failed to parse desktop update state `{}`: {error}",
                    path.display()
                ),
            )
        })?;

    if state.schema_version != DESKTOP_UPDATE_SCHEMA_VERSION
        || state.product != DESKTOP_UPDATE_PRODUCT
    {
        return Err(DesktopUpdateStateError::new(
            DesktopUpdateStateErrorCode::UnsupportedState,
            format!(
                "unsupported desktop update state schema/product in `{}`",
                path.display()
            ),
        ));
    }

    Ok(Some(state))
}

pub(crate) fn update_state_path(runtime_home: &Path) -> PathBuf {
    runtime_home
        .join(DESKTOP_UPDATES_DIR)
        .join(DESKTOP_UPDATE_STATE_FILE)
}

pub(crate) fn sha256_file(path: &Path) -> Result<String, DesktopUpdateStateError> {
    let mut file = fs::File::open(path).map_err(|error| {
        DesktopUpdateStateError::new(
            DesktopUpdateStateErrorCode::ReadAsset,
            format!(
                "failed to open desktop update asset `{}`: {error}",
                path.display()
            ),
        )
    })?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 16 * 1024];

    loop {
        let read = file.read(&mut buffer).map_err(|error| {
            DesktopUpdateStateError::new(
                DesktopUpdateStateErrorCode::ReadAsset,
                format!(
                    "failed to read desktop update asset `{}`: {error}",
                    path.display()
                ),
            )
        })?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }

    let digest = hasher.finalize();
    let mut output = String::with_capacity(64);
    for byte in digest {
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    Ok(output)
}

fn write_update_state(
    runtime_home: &Path,
    state: &DesktopUpdateStateFile,
) -> Result<(), DesktopUpdateStateError> {
    let path = update_state_path(runtime_home);
    let parent = path.parent().expect("desktop update state path has parent");
    fs::create_dir_all(parent).map_err(|error| {
        DesktopUpdateStateError::new(
            DesktopUpdateStateErrorCode::CreateStateDirectory,
            format!(
                "failed to create desktop update state directory `{}`: {error}",
                parent.display()
            ),
        )
    })?;

    let bytes = serde_json::to_vec_pretty(state).map_err(|error| {
        DesktopUpdateStateError::new(
            DesktopUpdateStateErrorCode::SerializeState,
            format!("failed to serialize desktop update state: {error}"),
        )
    })?;
    write_state_file_atomic(path.as_path(), bytes.as_slice())
}

fn write_state_file_atomic(path: &Path, bytes: &[u8]) -> Result<(), DesktopUpdateStateError> {
    let file_name = path
        .file_name()
        .and_then(|file_name| file_name.to_str())
        .unwrap_or(DESKTOP_UPDATE_STATE_FILE);
    let tmp_path = path.with_file_name(format!(".{file_name}.tmp-{}", std::process::id()));
    let mut tmp_file = fs::File::create(tmp_path.as_path()).map_err(|error| {
        DesktopUpdateStateError::new(
            DesktopUpdateStateErrorCode::WriteState,
            format!(
                "failed to create temporary desktop update state `{}`: {error}",
                tmp_path.display()
            ),
        )
    })?;
    tmp_file.write_all(bytes).map_err(|error| {
        DesktopUpdateStateError::new(
            DesktopUpdateStateErrorCode::WriteState,
            format!(
                "failed to write temporary desktop update state `{}`: {error}",
                tmp_path.display()
            ),
        )
    })?;
    tmp_file.flush().map_err(|error| {
        DesktopUpdateStateError::new(
            DesktopUpdateStateErrorCode::WriteState,
            format!(
                "failed to flush temporary desktop update state `{}`: {error}",
                tmp_path.display()
            ),
        )
    })?;
    drop(tmp_file);

    #[cfg(windows)]
    if path.exists() {
        fs::remove_file(path).map_err(|error| {
            DesktopUpdateStateError::new(
                DesktopUpdateStateErrorCode::RenameState,
                format!(
                    "failed to replace desktop update state `{}`: {error}",
                    path.display()
                ),
            )
        })?;
    }

    fs::rename(tmp_path.as_path(), path).map_err(|error| {
        let _ = fs::remove_file(tmp_path.as_path());
        DesktopUpdateStateError::new(
            DesktopUpdateStateErrorCode::RenameState,
            format!(
                "failed to finalize desktop update state `{}`: {error}",
                path.display()
            ),
        )
    })
}

fn current_unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

fn env_flag_enabled(value: Option<&str>) -> bool {
    matches!(
        value.map(|value| value.trim().to_ascii_lowercase()),
        Some(value) if matches!(value.as_str(), "1" | "true" | "yes" | "on")
    )
}

fn normalize_channel(value: Option<&str>) -> String {
    non_empty_or_default(value, DEFAULT_DESKTOP_UPDATE_CHANNEL).to_ascii_lowercase()
}

fn non_empty_or_default(value: Option<&str>, default: &str) -> String {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(default)
        .to_owned()
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_DESKTOP_UPDATE_CHANNEL, DEFAULT_RELEASE_REPO, DesktopUpdateConfig,
        DesktopUpdatePersistedStatus, DesktopUpdateStateErrorCode, ENV_DESKTOP_UPDATE_CHANNEL,
        ENV_DESKTOP_UPDATE_DISABLED, ENV_DESKTOP_UPDATE_FORCE_CHECK, ENV_RELEASE_API_BASE,
        ENV_RELEASE_DOWNLOAD_BASE, ENV_RELEASE_REPO, SHA256_MISMATCH_ERROR_CODE, env_flag_enabled,
        read_update_state, record_silent_failure_state_at, sha256_file, update_state_path,
        verify_staged_download_and_record_ready_state_at,
    };
    use crate::updater::download::StagedDownload;
    use std::collections::HashMap;
    use std::fs;

    #[test]
    fn env_flag_accepts_common_truthy_values() {
        for value in ["1", "true", "TRUE", "yes", "on", " On "] {
            assert!(env_flag_enabled(Some(value)));
        }
    }

    #[test]
    fn env_flag_rejects_empty_and_false_values() {
        for value in [
            None,
            Some(""),
            Some("0"),
            Some("false"),
            Some("off"),
            Some("no"),
        ] {
            assert!(!env_flag_enabled(value));
        }
    }

    #[test]
    fn config_uses_cli_compatible_release_defaults() {
        let config = DesktopUpdateConfig::from_lookup(|_| None);

        assert!(!config.disabled);
        assert!(!config.force_check);
        assert_eq!(config.channel, DEFAULT_DESKTOP_UPDATE_CHANNEL);
        assert_eq!(config.release_repo, DEFAULT_RELEASE_REPO);
        assert_eq!(
            config.release_api_base,
            "https://api.github.com/repos/pioneerdotai/pioneer/releases"
        );
        assert_eq!(
            config.release_download_base,
            "https://github.com/pioneerdotai/pioneer/releases/download"
        );
    }

    #[test]
    fn config_uses_env_overrides() {
        let values = HashMap::from([
            (ENV_DESKTOP_UPDATE_DISABLED, "yes"),
            (ENV_DESKTOP_UPDATE_FORCE_CHECK, "1"),
            (ENV_DESKTOP_UPDATE_CHANNEL, "Beta"),
            (ENV_RELEASE_REPO, "example/pioneer"),
            (ENV_RELEASE_API_BASE, "https://releases.example/api"),
            (
                ENV_RELEASE_DOWNLOAD_BASE,
                "https://releases.example/download",
            ),
        ]);

        let config =
            DesktopUpdateConfig::from_lookup(|key| values.get(key).map(|value| value.to_string()));

        assert!(config.disabled);
        assert!(config.force_check);
        assert_eq!(config.channel, "beta");
        assert_eq!(config.release_repo, "example/pioneer");
        assert_eq!(config.release_api_base, "https://releases.example/api");
        assert_eq!(
            config.release_download_base,
            "https://releases.example/download"
        );
    }

    #[test]
    fn missing_update_state_returns_none() {
        let temp_dir = tempfile::tempdir().unwrap();

        let state = read_update_state(temp_dir.path()).unwrap();

        assert!(state.is_none());
    }

    #[test]
    fn verified_download_records_ready_state() {
        let temp_dir = tempfile::tempdir().unwrap();
        let asset_path = temp_dir.path().join("Pioneer-aarch64.app.zip");
        fs::write(asset_path.as_path(), b"verified asset").unwrap();
        let expected_sha256 = sha256_file(asset_path.as_path()).unwrap();
        let staged = staged_download(asset_path, expected_sha256);

        let state = verify_staged_download_and_record_ready_state_at(
            temp_dir.path(),
            &staged,
            1_789_000_000,
        )
        .unwrap();

        assert_eq!(
            state.status,
            DesktopUpdatePersistedStatus::Ready {
                version: "0.26.0".to_owned(),
                tag: "v0.26.0".to_owned(),
                asset_path: staged.path.clone(),
                asset_name: "Pioneer-aarch64.app.zip".to_owned(),
                sha256: staged.sha256.clone(),
                os: "macos".to_owned(),
                arch: "aarch64".to_owned(),
                kind: "macos_app_zip".to_owned(),
                size_bytes: 14,
                checked_at_unix: 1_789_000_000,
            }
        );
        assert_eq!(read_update_state(temp_dir.path()).unwrap(), Some(state));
        assert!(update_state_path(temp_dir.path()).is_file());
    }

    #[test]
    fn sha256_mismatch_deletes_asset_and_records_silent_failure() {
        let temp_dir = tempfile::tempdir().unwrap();
        let asset_path = temp_dir.path().join("Pioneer-aarch64.app.zip");
        fs::write(asset_path.as_path(), b"bad asset").unwrap();
        let staged = staged_download(
            asset_path.clone(),
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
        );

        let error = verify_staged_download_and_record_ready_state_at(
            temp_dir.path(),
            &staged,
            1_789_000_001,
        )
        .unwrap_err();

        assert_eq!(error.code(), DesktopUpdateStateErrorCode::Sha256Mismatch);
        assert!(!asset_path.exists());
        let state = read_update_state(temp_dir.path()).unwrap().unwrap();
        assert_eq!(
            state.status,
            DesktopUpdatePersistedStatus::FailedSilent {
                error_code: SHA256_MISMATCH_ERROR_CODE.to_owned(),
                checked_at_unix: 1_789_000_001,
            }
        );
    }

    #[test]
    fn records_silent_failure_state() {
        let temp_dir = tempfile::tempdir().unwrap();

        let state =
            record_silent_failure_state_at(temp_dir.path(), "release_fetch_failed", 1_789_000_002)
                .unwrap();

        assert_eq!(
            state.status,
            DesktopUpdatePersistedStatus::FailedSilent {
                error_code: "release_fetch_failed".to_owned(),
                checked_at_unix: 1_789_000_002,
            }
        );
        assert_eq!(read_update_state(temp_dir.path()).unwrap(), Some(state));
    }

    fn staged_download(asset_path: std::path::PathBuf, sha256: String) -> StagedDownload {
        StagedDownload {
            tag: "v0.26.0".to_owned(),
            version: "0.26.0".to_owned(),
            asset_name: "Pioneer-aarch64.app.zip".to_owned(),
            url: "https://example.test/v0.26.0/Pioneer-aarch64.app.zip".to_owned(),
            path: asset_path,
            sha256,
            os: "macos".to_owned(),
            arch: "aarch64".to_owned(),
            kind: "macos_app_zip".to_owned(),
            size_bytes: 14,
        }
    }
}
