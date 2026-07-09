use super::*;
use crate::updater::{
    desktop_current_version, desktop_update_config_from_env,
    download::download_update_asset_to_cache_with_runtime_home,
    platform::select_update_candidate_for_current_platform,
    release::{DESKTOP_UPDATER_USER_AGENT, fetch_desktop_update_manifest_with_client},
    state::{
        DesktopUpdateConfig, DesktopUpdatePersistedStatus, read_update_state,
        record_silent_failure_state_at, sha256_file, verify_staged_download_and_record_ready_state,
    },
};
use reqwest::blocking::Client;
use semver::Version;
use std::{
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

const UPDATE_ERROR_RUNTIME_HOME: &str = "runtime_home";
const UPDATE_ERROR_HTTP_CLIENT: &str = "http_client";
const UPDATE_ERROR_RELEASE_FETCH: &str = "release_fetch";
const UPDATE_ERROR_PLATFORM_SELECTION: &str = "platform_selection";
const UPDATE_ERROR_DOWNLOAD: &str = "download";
const UPDATE_ERROR_VERIFY: &str = "verify";
const UPDATE_ERROR_READY_STATE_HASH: &str = "ready_state_hash";
const FAILED_CHECK_COOLDOWN_SECS: u64 = 5 * 60;

impl PioneerDesktop {
    pub(crate) fn start_desktop_update_check(&mut self, cx: &mut Context<Self>) {
        if self.desktop_update.is_style_preview() {
            return;
        }

        let config = desktop_update_config_from_env();
        if config.disabled {
            return;
        }

        if self.desktop_update != DesktopUpdateUiState::Checking {
            self.desktop_update = DesktopUpdateUiState::Checking;
            cx.notify();
        }

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();

            async move {
                let check_result = cx
                    .background_spawn(async move { run_desktop_update_check(config) })
                    .await;

                let download = match check_result {
                    DesktopUpdateCheckResult::Done(next_state) => {
                        update_desktop_update_state(&this, &mut cx, next_state);
                        return;
                    }
                    DesktopUpdateCheckResult::Download(download) => download,
                };
                let downloading_state = DesktopUpdateUiState::Downloading {
                    style_preview: false,
                };
                let _ = this.update(&mut cx, |view, cx| {
                    if view.desktop_update != downloading_state {
                        view.desktop_update = downloading_state;
                        cx.notify();
                    }
                });
                let next_state = cx
                    .background_spawn(async move { run_desktop_update_download(download) })
                    .await;
                update_desktop_update_state(&this, &mut cx, next_state);
            }
        })
        .detach();
    }
}

struct DesktopUpdateDownload {
    runtime_home: PathBuf,
    checked_at_unix: u64,
    client: Client,
    config: DesktopUpdateConfig,
    candidate: crate::updater::platform::DesktopUpdateCandidate,
}

enum DesktopUpdateCheckResult {
    Done(DesktopUpdateUiState),
    Download(DesktopUpdateDownload),
}

fn update_desktop_update_state(
    this: &WeakEntity<PioneerDesktop>,
    cx: &mut AsyncApp,
    next_state: DesktopUpdateUiState,
) {
    let _ = this.update(cx, |view, cx| {
        if view.desktop_update != next_state {
            view.desktop_update = next_state;
            cx.notify();
        }
    });
}

fn run_desktop_update_check(config: DesktopUpdateConfig) -> DesktopUpdateCheckResult {
    let checked_at_unix = current_unix_timestamp();
    let runtime_home = match crate::state::runtime_home_dir() {
        Ok(runtime_home) => runtime_home,
        Err(_) => {
            return DesktopUpdateCheckResult::Done(failed_silent(
                checked_at_unix,
                UPDATE_ERROR_RUNTIME_HOME,
            ));
        }
    };

    if let Some(state) = ready_ui_state_from_persisted(runtime_home.as_path(), checked_at_unix) {
        return DesktopUpdateCheckResult::Done(state);
    }
    if !config.force_check {
        if let Some(state) =
            recent_failure_ui_state_from_persisted(runtime_home.as_path(), checked_at_unix)
        {
            return DesktopUpdateCheckResult::Done(state);
        }
    }

    let client = match Client::builder()
        .user_agent(DESKTOP_UPDATER_USER_AGENT)
        .build()
    {
        Ok(client) => client,
        Err(_) => {
            return DesktopUpdateCheckResult::Done(record_failure(
                runtime_home.as_path(),
                checked_at_unix,
                UPDATE_ERROR_HTTP_CLIENT,
            ));
        }
    };

    let fetched = match fetch_desktop_update_manifest_with_client(&client, &config) {
        Ok(fetched) => fetched,
        Err(_) => {
            return DesktopUpdateCheckResult::Done(record_failure(
                runtime_home.as_path(),
                checked_at_unix,
                UPDATE_ERROR_RELEASE_FETCH,
            ));
        }
    };

    let candidate = match select_update_candidate_for_current_platform(
        &fetched.manifest,
        desktop_current_version(),
    ) {
        Ok(Some(candidate)) => candidate,
        Ok(None) => return DesktopUpdateCheckResult::Done(DesktopUpdateUiState::Idle),
        Err(_) => {
            return DesktopUpdateCheckResult::Done(record_failure(
                runtime_home.as_path(),
                checked_at_unix,
                UPDATE_ERROR_PLATFORM_SELECTION,
            ));
        }
    };

    DesktopUpdateCheckResult::Download(DesktopUpdateDownload {
        runtime_home,
        checked_at_unix,
        client,
        config,
        candidate,
    })
}

fn run_desktop_update_download(download: DesktopUpdateDownload) -> DesktopUpdateUiState {
    let staged = match download_update_asset_to_cache_with_runtime_home(
        &download.client,
        &download.config,
        &download.candidate,
        download.runtime_home.as_path(),
    ) {
        Ok(staged) => staged,
        Err(_) => {
            return record_failure(
                download.runtime_home.as_path(),
                download.checked_at_unix,
                UPDATE_ERROR_DOWNLOAD,
            );
        }
    };

    match verify_staged_download_and_record_ready_state(download.runtime_home.as_path(), &staged) {
        Ok(state) => ready_ui_state_from_state(state, desktop_current_version().to_owned())
            .unwrap_or(DesktopUpdateUiState::Idle),
        Err(_) => record_failure(
            download.runtime_home.as_path(),
            download.checked_at_unix,
            UPDATE_ERROR_VERIFY,
        ),
    }
}

fn ready_ui_state_from_persisted(
    runtime_home: &Path,
    checked_at_unix: u64,
) -> Option<DesktopUpdateUiState> {
    let state = read_update_state(runtime_home).ok().flatten()?;
    let ready_state = ready_ui_state_from_state(state, desktop_current_version().to_owned())?;

    let DesktopUpdateUiState::Ready {
        asset_path, sha256, ..
    } = &ready_state
    else {
        return None;
    };

    match sha256_file(asset_path.as_path()) {
        Ok(actual_sha256) if actual_sha256 == *sha256 => Some(ready_state),
        _ => {
            let _ = record_silent_failure_state_at(
                runtime_home,
                UPDATE_ERROR_READY_STATE_HASH,
                checked_at_unix,
            );
            None
        }
    }
}

fn recent_failure_ui_state_from_persisted(
    runtime_home: &Path,
    checked_at_unix: u64,
) -> Option<DesktopUpdateUiState> {
    let state = read_update_state(runtime_home).ok().flatten()?;
    match state.status {
        DesktopUpdatePersistedStatus::FailedSilent {
            error_code,
            checked_at_unix: failed_at_unix,
        } if checked_at_unix.saturating_sub(failed_at_unix) < FAILED_CHECK_COOLDOWN_SECS => {
            Some(DesktopUpdateUiState::FailedSilent {
                checked_at_unix: failed_at_unix,
                error_code,
            })
        }
        _ => None,
    }
}

fn ready_ui_state_from_state(
    state: crate::updater::state::DesktopUpdateStateFile,
    current_version: String,
) -> Option<DesktopUpdateUiState> {
    match state.status {
        DesktopUpdatePersistedStatus::Ready {
            version,
            tag,
            asset_path,
            asset_name,
            sha256,
            os,
            arch,
            kind,
            size_bytes,
            ..
        } if version_is_newer(version.as_str(), current_version.as_str()) => {
            Some(DesktopUpdateUiState::Ready {
                version,
                current_version,
                tag,
                asset_path,
                asset_name,
                sha256,
                os,
                arch,
                kind,
                size_bytes,
                style_preview: false,
            })
        }
        _ => None,
    }
}

fn record_failure(
    runtime_home: &Path,
    checked_at_unix: u64,
    error_code: &str,
) -> DesktopUpdateUiState {
    let _ = record_silent_failure_state_at(runtime_home, error_code, checked_at_unix);
    failed_silent(checked_at_unix, error_code)
}

fn failed_silent(checked_at_unix: u64, error_code: &str) -> DesktopUpdateUiState {
    DesktopUpdateUiState::FailedSilent {
        checked_at_unix,
        error_code: error_code.to_owned(),
    }
}

fn version_is_newer(version: &str, current_version: &str) -> bool {
    let Ok(version) = Version::parse(version) else {
        return false;
    };
    let Ok(current_version) = Version::parse(current_version) else {
        return false;
    };

    version > current_version
}

fn current_unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::{
        FAILED_CHECK_COOLDOWN_SECS, ready_ui_state_from_persisted, ready_ui_state_from_state,
        recent_failure_ui_state_from_persisted, version_is_newer,
    };
    use crate::{
        app::root::DesktopUpdateUiState,
        updater::{
            download::StagedDownload,
            state::{
                DesktopUpdatePersistedStatus, DesktopUpdateStateFile, read_update_state,
                verify_staged_download_and_record_ready_state_at,
            },
        },
    };
    use std::{fs, path::PathBuf};

    #[test]
    fn maps_newer_ready_state_to_ui_ready() {
        let state = ready_state("0.26.0");

        let ui_state = ready_ui_state_from_state(state, "0.25.0".to_owned()).unwrap();

        assert!(matches!(
            ui_state,
            DesktopUpdateUiState::Ready {
                version,
                current_version,
                asset_name,
                ..
            } if version == "0.26.0"
                && current_version == "0.25.0"
                && asset_name == "Pioneer-aarch64.app.zip"
        ));
    }

    #[test]
    fn equal_or_older_ready_state_stays_quiet() {
        assert!(ready_ui_state_from_state(ready_state("0.25.0"), "0.25.0".to_owned()).is_none());
        assert!(ready_ui_state_from_state(ready_state("0.24.9"), "0.25.0".to_owned()).is_none());
    }

    #[test]
    fn semver_compare_requires_newer_version() {
        assert!(version_is_newer("0.26.0", "0.25.0"));
        assert!(!version_is_newer("0.25.0", "0.25.0"));
        assert!(!version_is_newer("0.24.9", "0.25.0"));
        assert!(!version_is_newer("not-semver", "0.25.0"));
    }

    #[test]
    fn persisted_ready_state_is_reused_when_asset_hash_matches() {
        let temp_dir = tempfile::tempdir().unwrap();
        let asset_path = temp_dir.path().join("Pioneer-aarch64.app.zip");
        fs::write(asset_path.as_path(), b"verified app zip").unwrap();
        let staged = staged_download(asset_path);
        verify_staged_download_and_record_ready_state_at(temp_dir.path(), &staged, 1_789_200_000)
            .unwrap();

        let ui_state = ready_ui_state_from_persisted(temp_dir.path(), 1_789_200_100).unwrap();

        assert!(matches!(
            ui_state,
            DesktopUpdateUiState::Ready {
                version,
                asset_name,
                ..
            } if version == "99.99.99" && asset_name == "Pioneer-aarch64.app.zip"
        ));
    }

    #[test]
    fn persisted_ready_state_with_hash_mismatch_stays_quiet() {
        let temp_dir = tempfile::tempdir().unwrap();
        let asset_path = temp_dir.path().join("Pioneer-aarch64.app.zip");
        fs::write(asset_path.as_path(), b"verified app zip").unwrap();
        let staged = staged_download(asset_path.clone());
        verify_staged_download_and_record_ready_state_at(temp_dir.path(), &staged, 1_789_200_000)
            .unwrap();
        fs::write(asset_path.as_path(), b"tampered app zip").unwrap();

        let ui_state = ready_ui_state_from_persisted(temp_dir.path(), 1_789_200_100);

        assert!(ui_state.is_none());
        assert!(matches!(
            read_update_state(temp_dir.path()).unwrap().unwrap().status,
            DesktopUpdatePersistedStatus::FailedSilent { ref error_code, .. }
                if error_code == "ready_state_hash"
        ));
    }

    #[test]
    fn recent_failed_silent_state_skips_retry_loop() {
        let temp_dir = tempfile::tempdir().unwrap();
        write_failed_state(temp_dir.path(), "release_fetch", 1_789_200_000);

        let ui_state =
            recent_failure_ui_state_from_persisted(temp_dir.path(), 1_789_200_000 + 60).unwrap();

        assert!(matches!(
            ui_state,
            DesktopUpdateUiState::FailedSilent {
                checked_at_unix: 1_789_200_000,
                error_code,
            } if error_code == "release_fetch"
        ));
    }

    #[test]
    fn old_failed_silent_state_allows_retry_after_cooldown() {
        let temp_dir = tempfile::tempdir().unwrap();
        write_failed_state(temp_dir.path(), "release_fetch", 1_789_200_000);

        let ui_state = recent_failure_ui_state_from_persisted(
            temp_dir.path(),
            1_789_200_000 + FAILED_CHECK_COOLDOWN_SECS + 1,
        );

        assert!(ui_state.is_none());
    }

    fn ready_state(version: &str) -> DesktopUpdateStateFile {
        DesktopUpdateStateFile {
            schema_version: 1,
            product: "pioneer-desktop".to_owned(),
            status: DesktopUpdatePersistedStatus::Ready {
                version: version.to_owned(),
                tag: format!("v{version}"),
                asset_path: PathBuf::from("/tmp/Pioneer-aarch64.app.zip"),
                asset_name: "Pioneer-aarch64.app.zip".to_owned(),
                sha256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_owned(),
                os: "macos".to_owned(),
                arch: "aarch64".to_owned(),
                kind: "macos_app_zip".to_owned(),
                size_bytes: 123,
                checked_at_unix: 1_789_200_000,
            },
        }
    }

    fn write_failed_state(runtime_home: &std::path::Path, error_code: &str, checked_at_unix: u64) {
        let path = crate::updater::state::update_state_path(runtime_home);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(
            path.as_path(),
            serde_json::to_vec_pretty(&DesktopUpdateStateFile {
                schema_version: 1,
                product: "pioneer-desktop".to_owned(),
                status: DesktopUpdatePersistedStatus::FailedSilent {
                    error_code: error_code.to_owned(),
                    checked_at_unix,
                },
            })
            .unwrap(),
        )
        .unwrap();
    }

    fn staged_download(asset_path: PathBuf) -> StagedDownload {
        StagedDownload {
            tag: "v99.99.99".to_owned(),
            version: "99.99.99".to_owned(),
            asset_name: "Pioneer-aarch64.app.zip".to_owned(),
            url: "https://example.test/v99.99.99/Pioneer-aarch64.app.zip".to_owned(),
            path: asset_path,
            sha256: "385299ed9c24b075ce582ff4757206c198ac0077382a0dbd48f6f942c4b0840e".to_owned(),
            os: "macos".to_owned(),
            arch: "aarch64".to_owned(),
            kind: "macos_app_zip".to_owned(),
            size_bytes: b"verified app zip".len() as u64,
        }
    }
}
