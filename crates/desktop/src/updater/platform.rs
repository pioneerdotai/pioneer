use super::manifest::{DesktopUpdateManifest, DesktopUpdateManifestAsset};
use semver::Version;
use std::{error::Error, fmt};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DesktopUpdateCandidate {
    pub(crate) tag: String,
    pub(crate) version: String,
    pub(crate) os: String,
    pub(crate) arch: String,
    pub(crate) kind: String,
    pub(crate) asset_name: String,
    pub(crate) sha256: String,
    pub(crate) size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DesktopPlatform {
    os: String,
    arch: String,
    kind: String,
}

impl DesktopPlatform {
    pub(crate) fn current() -> Result<Self, DesktopPlatformError> {
        Self::from_raw(current_os(), current_arch())
    }

    pub(crate) fn from_raw(os: &str, arch: &str) -> Result<Self, DesktopPlatformError> {
        let os = normalize_os(os)?;
        let arch = normalize_arch(arch)?;
        let Some(kind) = asset_kind_for_os(os.as_str()) else {
            return Err(DesktopPlatformError::new(
                DesktopPlatformErrorCode::UnsupportedPlatform,
                format!("unsupported desktop update OS: {os}"),
            ));
        };

        Ok(Self {
            os,
            arch,
            kind: kind.to_owned(),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DesktopPlatformErrorCode {
    InvalidCurrentVersion,
    InvalidManifestVersion,
    UnsupportedPlatform,
    MissingAsset,
    DuplicateMatchingAsset,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DesktopPlatformError {
    code: DesktopPlatformErrorCode,
    message: String,
}

impl DesktopPlatformError {
    fn new(code: DesktopPlatformErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub(crate) fn code(&self) -> DesktopPlatformErrorCode {
        self.code
    }
}

impl fmt::Display for DesktopPlatformError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for DesktopPlatformError {}

pub(crate) fn select_update_candidate_for_current_platform(
    manifest: &DesktopUpdateManifest,
    current_version: &str,
) -> Result<Option<DesktopUpdateCandidate>, DesktopPlatformError> {
    let platform = DesktopPlatform::current()?;
    select_update_candidate(manifest, current_version, &platform)
}

pub(crate) fn select_update_candidate(
    manifest: &DesktopUpdateManifest,
    current_version: &str,
    platform: &DesktopPlatform,
) -> Result<Option<DesktopUpdateCandidate>, DesktopPlatformError> {
    let current_version = Version::parse(current_version).map_err(|error| {
        DesktopPlatformError::new(
            DesktopPlatformErrorCode::InvalidCurrentVersion,
            format!("failed to parse current desktop version `{current_version}`: {error}"),
        )
    })?;
    let manifest_version = Version::parse(manifest.version.as_str()).map_err(|error| {
        DesktopPlatformError::new(
            DesktopPlatformErrorCode::InvalidManifestVersion,
            format!(
                "failed to parse desktop update manifest version `{}`: {error}",
                manifest.version
            ),
        )
    })?;

    if manifest_version <= current_version {
        return Ok(None);
    }

    let matches = manifest
        .assets
        .iter()
        .filter(|asset| asset_matches_platform(asset, platform))
        .collect::<Vec<_>>();

    match matches.as_slice() {
        [] => Err(DesktopPlatformError::new(
            DesktopPlatformErrorCode::MissingAsset,
            format!(
                "desktop update manifest does not include asset for {}/{}/{}",
                platform.os, platform.arch, platform.kind
            ),
        )),
        [asset] => Ok(Some(candidate_from_asset(manifest, asset))),
        _ => Err(DesktopPlatformError::new(
            DesktopPlatformErrorCode::DuplicateMatchingAsset,
            format!(
                "desktop update manifest has duplicate assets for {}/{}/{}",
                platform.os, platform.arch, platform.kind
            ),
        )),
    }
}

pub(crate) fn current_os() -> &'static str {
    #[cfg(target_os = "macos")]
    {
        "macos"
    }

    #[cfg(target_os = "linux")]
    {
        "linux"
    }

    #[cfg(target_os = "windows")]
    {
        "windows"
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    {
        std::env::consts::OS
    }
}

pub(crate) fn current_arch() -> &'static str {
    #[cfg(target_arch = "aarch64")]
    {
        "aarch64"
    }

    #[cfg(target_arch = "x86_64")]
    {
        "x86_64"
    }

    #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
    {
        std::env::consts::ARCH
    }
}

fn candidate_from_asset(
    manifest: &DesktopUpdateManifest,
    asset: &DesktopUpdateManifestAsset,
) -> DesktopUpdateCandidate {
    DesktopUpdateCandidate {
        tag: manifest.tag.clone(),
        version: manifest.version.clone(),
        os: asset.os.clone(),
        arch: asset.arch.clone(),
        kind: asset.kind.clone(),
        asset_name: asset.name.clone(),
        sha256: asset.sha256.clone(),
        size_bytes: asset.size_bytes,
    }
}

fn asset_matches_platform(asset: &DesktopUpdateManifestAsset, platform: &DesktopPlatform) -> bool {
    asset.os == platform.os && asset.arch == platform.arch && asset.kind == platform.kind
}

fn normalize_os(os: &str) -> Result<String, DesktopPlatformError> {
    match os.trim().to_ascii_lowercase().as_str() {
        "macos" => Ok("macos".to_owned()),
        "linux" => Ok("linux".to_owned()),
        "windows" => Ok("windows".to_owned()),
        other => Err(DesktopPlatformError::new(
            DesktopPlatformErrorCode::UnsupportedPlatform,
            format!("unsupported desktop update OS: {other}"),
        )),
    }
}

fn normalize_arch(arch: &str) -> Result<String, DesktopPlatformError> {
    match arch.trim().to_ascii_lowercase().as_str() {
        "x86_64" | "amd64" => Ok("x86_64".to_owned()),
        "aarch64" | "arm64" => Ok("aarch64".to_owned()),
        other => Err(DesktopPlatformError::new(
            DesktopPlatformErrorCode::UnsupportedPlatform,
            format!("unsupported desktop update arch: {other}"),
        )),
    }
}

fn asset_kind_for_os(os: &str) -> Option<&'static str> {
    match os {
        "macos" => Some("macos_app_zip"),
        "linux" => Some("appimage"),
        "windows" => Some("wix_bundle_exe"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{DesktopPlatform, DesktopPlatformErrorCode, select_update_candidate};
    use crate::updater::manifest::{DesktopUpdateManifest, DesktopUpdateManifestAsset};

    #[test]
    fn returns_no_update_for_equal_or_older_version() {
        let platform = DesktopPlatform::from_raw("linux", "amd64").unwrap();
        assert!(
            select_update_candidate(&manifest_with_version("0.25.0"), "0.25.0", &platform)
                .unwrap()
                .is_none()
        );
        assert!(
            select_update_candidate(&manifest_with_version("0.24.9"), "0.25.0", &platform)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn returns_newer_update_candidate_for_platform() {
        let platform = DesktopPlatform::from_raw("linux", "amd64").unwrap();

        let candidate =
            select_update_candidate(&manifest_with_version("0.26.0"), "0.25.0", &platform)
                .unwrap()
                .unwrap();

        assert_eq!(candidate.tag, "v0.26.0");
        assert_eq!(candidate.version, "0.26.0");
        assert_eq!(candidate.os, "linux");
        assert_eq!(candidate.arch, "x86_64");
        assert_eq!(candidate.kind, "appimage");
        assert_eq!(candidate.asset_name, "pioneer-linux-x86_64.AppImage");
        assert_eq!(
            candidate.sha256,
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        );
        assert_eq!(candidate.size_bytes, 456);
    }

    #[test]
    fn rejects_unsupported_platform() {
        let error = DesktopPlatform::from_raw("freebsd", "x86_64").unwrap_err();

        assert_eq!(error.code(), DesktopPlatformErrorCode::UnsupportedPlatform);
    }

    #[test]
    fn returns_typed_error_for_missing_asset() {
        let platform = DesktopPlatform::from_raw("windows", "x86_64").unwrap();
        let error = select_update_candidate(&manifest_with_version("0.26.0"), "0.25.0", &platform)
            .unwrap_err();

        assert_eq!(error.code(), DesktopPlatformErrorCode::MissingAsset);
    }

    #[test]
    fn returns_typed_error_for_duplicate_matching_asset() {
        let platform = DesktopPlatform::from_raw("linux", "amd64").unwrap();
        let mut manifest = manifest_with_version("0.26.0");
        manifest.assets.push(manifest.assets[1].clone());

        let error = select_update_candidate(&manifest, "0.25.0", &platform).unwrap_err();

        assert_eq!(
            error.code(),
            DesktopPlatformErrorCode::DuplicateMatchingAsset
        );
    }

    fn manifest_with_version(version: &str) -> DesktopUpdateManifest {
        DesktopUpdateManifest {
            schema_version: 1,
            product: "pioneer-desktop".to_owned(),
            version: version.to_owned(),
            tag: format!("v{version}"),
            channel: "stable".to_owned(),
            published_at: "2026-07-08T00:00:00Z".to_owned(),
            assets: vec![
                DesktopUpdateManifestAsset {
                    os: "macos".to_owned(),
                    arch: "aarch64".to_owned(),
                    kind: "macos_app_zip".to_owned(),
                    name: "Pioneer-aarch64.app.zip".to_owned(),
                    sha256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        .to_owned(),
                    size_bytes: 123,
                },
                DesktopUpdateManifestAsset {
                    os: "linux".to_owned(),
                    arch: "x86_64".to_owned(),
                    kind: "appimage".to_owned(),
                    name: "pioneer-linux-x86_64.AppImage".to_owned(),
                    sha256: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                        .to_owned(),
                    size_bytes: 456,
                },
            ],
        }
    }
}
