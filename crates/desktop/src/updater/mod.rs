pub(crate) mod download;
pub(crate) mod manifest;
pub(crate) mod plan;
pub(crate) mod platform;
pub(crate) mod release;
pub(crate) mod state;

pub(crate) use state::DesktopUpdateConfig;

pub(crate) fn desktop_current_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

pub(crate) fn desktop_update_config_from_env() -> DesktopUpdateConfig {
    DesktopUpdateConfig::from_env()
}

#[cfg(test)]
mod tests {
    use super::{
        desktop_current_version,
        download::{StagedDownload, prepare_download_paths},
        manifest::{
            DESKTOP_UPDATE_PRODUCT, DESKTOP_UPDATE_SCHEMA_VERSION, parse_desktop_update_manifest,
        },
        platform::{DesktopPlatform, DesktopPlatformErrorCode, select_update_candidate},
        state::{
            DesktopUpdatePersistedStatus, DesktopUpdateStateErrorCode, SHA256_MISMATCH_ERROR_CODE,
            read_update_state, sha256_file, verify_staged_download_and_record_ready_state_at,
        },
    };
    use serde_json::{Value, json};
    use std::fs;

    #[test]
    fn desktop_current_version_is_set() {
        assert!(!desktop_current_version().is_empty());
    }

    #[test]
    fn equal_version_manifest_does_not_create_ready_state() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manifest = parse_manifest(equal_version_manifest());
        let platform = linux_x86_64_platform();

        let candidate = select_update_candidate(&manifest, "0.25.0", &platform).unwrap();

        assert!(candidate.is_none());
        assert!(read_update_state(temp_dir.path()).unwrap().is_none());
    }

    #[test]
    fn newer_supported_manifest_records_ready_state_after_fake_download() {
        let temp_dir = tempfile::tempdir().unwrap();
        let bytes = b"pioneer desktop update";
        let manifest = parse_manifest(newer_supported_manifest(sha256_hex(bytes)));
        let platform = linux_x86_64_platform();
        let candidate = select_update_candidate(&manifest, "0.25.0", &platform)
            .unwrap()
            .unwrap();
        let staged = fake_download(temp_dir.path(), &candidate, bytes);

        let state = verify_staged_download_and_record_ready_state_at(
            temp_dir.path(),
            &staged,
            1_789_100_000,
        )
        .unwrap();

        assert!(matches!(
            state.status,
            DesktopUpdatePersistedStatus::Ready { ref version, ref asset_name, .. }
                if version == "0.26.0" && asset_name == "pioneer-linux-x86_64.AppImage"
        ));
        assert_eq!(read_update_state(temp_dir.path()).unwrap(), Some(state));
    }

    #[test]
    fn newer_manifest_missing_platform_asset_does_not_create_ready_state() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manifest = parse_manifest(missing_platform_asset_manifest());
        let platform = linux_x86_64_platform();

        let error = select_update_candidate(&manifest, "0.25.0", &platform).unwrap_err();

        assert_eq!(error.code(), DesktopPlatformErrorCode::MissingAsset);
        assert!(read_update_state(temp_dir.path()).unwrap().is_none());
    }

    #[test]
    fn bad_fake_download_records_silent_failure_not_ready_state() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manifest = parse_manifest(newer_supported_manifest(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ));
        let platform = linux_x86_64_platform();
        let candidate = select_update_candidate(&manifest, "0.25.0", &platform)
            .unwrap()
            .unwrap();
        let staged = fake_download(temp_dir.path(), &candidate, b"unexpected bytes");

        let error = verify_staged_download_and_record_ready_state_at(
            temp_dir.path(),
            &staged,
            1_789_100_001,
        )
        .unwrap_err();

        assert_eq!(error.code(), DesktopUpdateStateErrorCode::Sha256Mismatch);
        assert!(!staged.path.exists());
        let state = read_update_state(temp_dir.path()).unwrap().unwrap();
        assert_eq!(
            state.status,
            DesktopUpdatePersistedStatus::FailedSilent {
                error_code: SHA256_MISMATCH_ERROR_CODE.to_owned(),
                checked_at_unix: 1_789_100_001,
            }
        );
    }

    #[test]
    fn malformed_schema_manifest_is_rejected_before_selection() {
        let mut value = newer_supported_manifest(sha256_hex(b"asset"));
        value["schema_version"] = json!(999);
        let bytes = serde_json::to_vec(&value).unwrap();

        let error = parse_desktop_update_manifest(bytes.as_slice()).unwrap_err();

        assert_eq!(
            error.code(),
            super::manifest::DesktopManifestErrorCode::WrongSchemaVersion
        );
    }

    fn parse_manifest(value: Value) -> super::manifest::DesktopUpdateManifest {
        let bytes = serde_json::to_vec(&value).unwrap();
        parse_desktop_update_manifest(bytes.as_slice()).unwrap()
    }

    fn equal_version_manifest() -> Value {
        manifest_fixture("0.25.0", sha256_hex(b"same version asset"), true)
    }

    fn newer_supported_manifest(sha256: impl Into<String>) -> Value {
        manifest_fixture("0.26.0", sha256.into(), true)
    }

    fn missing_platform_asset_manifest() -> Value {
        manifest_fixture("0.26.0", sha256_hex(b"mac only"), false)
    }

    fn manifest_fixture(version: &str, linux_sha256: String, include_linux: bool) -> Value {
        let mut assets = vec![json!({
            "os": "macos",
            "arch": "aarch64",
            "kind": "macos_app_zip",
            "name": "Pioneer-aarch64.app.zip",
            "sha256": sha256_hex(b"mac asset"),
            "size_bytes": 9
        })];
        if include_linux {
            assets.push(json!({
                "os": "linux",
                "arch": "x86_64",
                "kind": "appimage",
                "name": "pioneer-linux-x86_64.AppImage",
                "sha256": linux_sha256,
                "size_bytes": 22
            }));
        }

        json!({
            "schema_version": DESKTOP_UPDATE_SCHEMA_VERSION,
            "product": DESKTOP_UPDATE_PRODUCT,
            "version": version,
            "tag": format!("v{version}"),
            "channel": "stable",
            "published_at": "2026-07-08T00:00:00Z",
            "assets": assets
        })
    }

    fn fake_download(
        runtime_home: &std::path::Path,
        candidate: &super::platform::DesktopUpdateCandidate,
        bytes: &[u8],
    ) -> StagedDownload {
        let paths = prepare_download_paths(runtime_home, candidate).unwrap();
        fs::write(paths.asset_path.as_path(), bytes).unwrap();
        let size_bytes = bytes.len() as u64;

        StagedDownload {
            tag: candidate.tag.clone(),
            version: candidate.version.clone(),
            asset_name: candidate.asset_name.clone(),
            url: "fake://desktop-update".to_owned(),
            path: paths.asset_path,
            sha256: candidate.sha256.clone(),
            os: candidate.os.clone(),
            arch: candidate.arch.clone(),
            kind: candidate.kind.clone(),
            size_bytes,
        }
    }

    fn linux_x86_64_platform() -> DesktopPlatform {
        DesktopPlatform::from_raw("linux", "x86_64").unwrap()
    }

    fn sha256_hex(bytes: &[u8]) -> String {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("fixture.bin");
        fs::write(path.as_path(), bytes).unwrap();
        sha256_file(path.as_path()).unwrap()
    }
}
