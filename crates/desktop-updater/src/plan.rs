use semver::Version;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    error::Error,
    fmt::{self, Write as _},
    fs,
    io::Read as _,
    path::{Component, Path, PathBuf},
};

pub const PLAN_SCHEMA_VERSION: u32 = 1;
pub const PLAN_PRODUCT: &str = "pioneer-desktop";
const SHA256_HEX_LEN: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DesktopUpdatePlan {
    pub schema_version: u32,
    pub product: String,
    pub target_version: String,
    pub current_version: String,
    pub tag: String,
    pub os: String,
    pub arch: String,
    pub asset_kind: String,
    pub asset_path: PathBuf,
    pub asset_name: String,
    pub asset_sha256: String,
    pub current_pid: u32,
    pub current_exe_path: PathBuf,
    pub install_root_path: PathBuf,
    pub appimage_path: Option<PathBuf>,
    pub restart_after_apply: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedPlan {
    pub plan: DesktopUpdatePlan,
    pub actual_asset_sha256: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanValidationErrorCode {
    ReadPlan,
    ParsePlan,
    WrongSchema,
    WrongProduct,
    EmptyField,
    InvalidVersion,
    NonUpgradeVersion,
    WrongOs,
    WrongArch,
    WrongAssetKind,
    UnsafePath,
    InvalidSha256,
    MissingAsset,
    ReadAsset,
    Sha256Mismatch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanValidationError {
    code: PlanValidationErrorCode,
    message: String,
}

impl PlanValidationError {
    fn new(code: PlanValidationErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn code(&self) -> PlanValidationErrorCode {
        self.code
    }
}

impl PlanValidationErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ReadPlan => "read_plan",
            Self::ParsePlan => "parse_plan",
            Self::WrongSchema => "wrong_schema",
            Self::WrongProduct => "wrong_product",
            Self::EmptyField => "empty_field",
            Self::InvalidVersion => "invalid_version",
            Self::NonUpgradeVersion => "non_upgrade_version",
            Self::WrongOs => "wrong_os",
            Self::WrongArch => "wrong_arch",
            Self::WrongAssetKind => "wrong_asset_kind",
            Self::UnsafePath => "unsafe_path",
            Self::InvalidSha256 => "invalid_sha256",
            Self::MissingAsset => "missing_asset",
            Self::ReadAsset => "read_asset",
            Self::Sha256Mismatch => "sha256_mismatch",
        }
    }
}

impl fmt::Display for PlanValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for PlanValidationError {}

pub fn read_and_validate_plan(plan_path: &Path) -> Result<ValidatedPlan, PlanValidationError> {
    let bytes = fs::read(plan_path).map_err(|error| {
        PlanValidationError::new(
            PlanValidationErrorCode::ReadPlan,
            format!("failed to read desktop update plan: {error}"),
        )
    })?;
    let plan = serde_json::from_slice::<DesktopUpdatePlan>(bytes.as_slice()).map_err(|error| {
        PlanValidationError::new(
            PlanValidationErrorCode::ParsePlan,
            format!("failed to parse desktop update plan: {error}"),
        )
    })?;

    validate_plan(plan)
}

pub fn validate_plan(plan: DesktopUpdatePlan) -> Result<ValidatedPlan, PlanValidationError> {
    validate_plan_shape(&plan)?;

    if !plan.asset_path.is_file() {
        return Err(PlanValidationError::new(
            PlanValidationErrorCode::MissingAsset,
            format!(
                "desktop update asset `{}` is missing",
                plan.asset_name.as_str()
            ),
        ));
    }

    let actual_asset_sha256 = sha256_file(plan.asset_path.as_path())?;
    if actual_asset_sha256 != plan.asset_sha256 {
        return Err(PlanValidationError::new(
            PlanValidationErrorCode::Sha256Mismatch,
            format!(
                "desktop update asset SHA256 mismatch for `{}`",
                plan.asset_name.as_str()
            ),
        ));
    }

    Ok(ValidatedPlan {
        plan,
        actual_asset_sha256,
    })
}

pub fn validate_plan_shape(plan: &DesktopUpdatePlan) -> Result<(), PlanValidationError> {
    if plan.schema_version != PLAN_SCHEMA_VERSION {
        return Err(PlanValidationError::new(
            PlanValidationErrorCode::WrongSchema,
            format!(
                "unsupported desktop update plan schema {}",
                plan.schema_version
            ),
        ));
    }
    if plan.product != PLAN_PRODUCT {
        return Err(PlanValidationError::new(
            PlanValidationErrorCode::WrongProduct,
            "unsupported desktop update plan product",
        ));
    }

    if required_text_fields(plan)
        .into_iter()
        .any(|(_, value)| value.trim().is_empty())
        || plan.asset_path.as_os_str().is_empty()
        || plan.current_exe_path.as_os_str().is_empty()
        || plan.install_root_path.as_os_str().is_empty()
        || plan
            .appimage_path
            .as_ref()
            .is_some_and(|path| path.as_os_str().is_empty())
        || plan.current_pid == 0
    {
        return Err(PlanValidationError::new(
            PlanValidationErrorCode::EmptyField,
            "desktop update plan contains an empty required field",
        ));
    }

    if !is_strict_sha256_hex(plan.asset_sha256.as_str()) {
        return Err(PlanValidationError::new(
            PlanValidationErrorCode::InvalidSha256,
            "desktop update plan contains an invalid SHA256",
        ));
    }

    validate_plan_versions(plan)?;
    validate_plan_platform(plan)?;
    validate_plan_paths(plan)?;

    Ok(())
}

pub fn sha256_file(path: &Path) -> Result<String, PlanValidationError> {
    let mut file = fs::File::open(path).map_err(|error| {
        PlanValidationError::new(
            PlanValidationErrorCode::ReadAsset,
            format!("failed to open desktop update asset: {error}"),
        )
    })?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 16 * 1024];

    loop {
        let read = file.read(&mut buffer).map_err(|error| {
            PlanValidationError::new(
                PlanValidationErrorCode::ReadAsset,
                format!("failed to read desktop update asset: {error}"),
            )
        })?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }

    let digest = hasher.finalize();
    let mut output = String::with_capacity(SHA256_HEX_LEN);
    for byte in digest {
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    Ok(output)
}

fn required_text_fields(plan: &DesktopUpdatePlan) -> [(&'static str, &str); 8] {
    [
        ("target_version", plan.target_version.as_str()),
        ("current_version", plan.current_version.as_str()),
        ("tag", plan.tag.as_str()),
        ("os", plan.os.as_str()),
        ("arch", plan.arch.as_str()),
        ("asset_kind", plan.asset_kind.as_str()),
        ("asset_name", plan.asset_name.as_str()),
        ("asset_sha256", plan.asset_sha256.as_str()),
    ]
}

fn is_strict_sha256_hex(value: &str) -> bool {
    value.len() == SHA256_HEX_LEN
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn validate_plan_versions(plan: &DesktopUpdatePlan) -> Result<(), PlanValidationError> {
    let target_version = Version::parse(plan.target_version.as_str()).map_err(|error| {
        PlanValidationError::new(
            PlanValidationErrorCode::InvalidVersion,
            format!("invalid desktop update target version: {error}"),
        )
    })?;
    let current_version = Version::parse(plan.current_version.as_str()).map_err(|error| {
        PlanValidationError::new(
            PlanValidationErrorCode::InvalidVersion,
            format!("invalid current desktop version: {error}"),
        )
    })?;

    if target_version <= current_version {
        return Err(PlanValidationError::new(
            PlanValidationErrorCode::NonUpgradeVersion,
            "desktop update target version is not newer than current version",
        ));
    }

    Ok(())
}

fn validate_plan_platform(plan: &DesktopUpdatePlan) -> Result<(), PlanValidationError> {
    let current_os = current_plan_os();
    if plan.os != current_os {
        return Err(PlanValidationError::new(
            PlanValidationErrorCode::WrongOs,
            format!(
                "desktop update plan targets `{}` but this helper runs on `{current_os}`",
                plan.os
            ),
        ));
    }

    let current_arch = current_plan_arch().ok_or_else(|| {
        PlanValidationError::new(
            PlanValidationErrorCode::WrongArch,
            format!(
                "desktop update helper does not support architecture `{}`",
                std::env::consts::ARCH
            ),
        )
    })?;
    if plan.arch != current_arch {
        return Err(PlanValidationError::new(
            PlanValidationErrorCode::WrongArch,
            format!(
                "desktop update plan targets `{}` but this helper runs on `{current_arch}`",
                plan.arch
            ),
        ));
    }

    let expected_asset_kind = expected_asset_kind_for_os(current_os).ok_or_else(|| {
        PlanValidationError::new(
            PlanValidationErrorCode::WrongOs,
            format!("desktop update helper does not support OS `{current_os}`"),
        )
    })?;
    if plan.asset_kind != expected_asset_kind {
        return Err(PlanValidationError::new(
            PlanValidationErrorCode::WrongAssetKind,
            format!(
                "desktop update plan asset kind `{}` does not match `{expected_asset_kind}`",
                plan.asset_kind
            ),
        ));
    }

    Ok(())
}

fn validate_plan_paths(plan: &DesktopUpdatePlan) -> Result<(), PlanValidationError> {
    validate_safe_absolute_path("asset_path", plan.asset_path.as_path())?;
    validate_safe_absolute_path("current_exe_path", plan.current_exe_path.as_path())?;
    validate_safe_absolute_path("install_root_path", plan.install_root_path.as_path())?;
    if let Some(appimage_path) = &plan.appimage_path {
        validate_safe_absolute_path("appimage_path", appimage_path.as_path())?;
    }

    if !is_safe_asset_name(plan.asset_name.as_str()) {
        return Err(PlanValidationError::new(
            PlanValidationErrorCode::UnsafePath,
            "desktop update plan contains an unsafe asset name",
        ));
    }

    let asset_file_name = plan
        .asset_path
        .file_name()
        .and_then(|file_name| file_name.to_str())
        .ok_or_else(|| {
            PlanValidationError::new(
                PlanValidationErrorCode::UnsafePath,
                "desktop update asset path has no safe file name",
            )
        })?;
    if asset_file_name != plan.asset_name {
        return Err(PlanValidationError::new(
            PlanValidationErrorCode::UnsafePath,
            "desktop update asset path and asset name do not match",
        ));
    }

    if asset_file_name.ends_with(".part") {
        return Err(PlanValidationError::new(
            PlanValidationErrorCode::UnsafePath,
            "desktop update helper refuses partial download paths",
        ));
    }

    Ok(())
}

fn validate_safe_absolute_path(field: &str, path: &Path) -> Result<(), PlanValidationError> {
    if !path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        return Err(PlanValidationError::new(
            PlanValidationErrorCode::UnsafePath,
            format!("desktop update plan contains an unsafe {field}"),
        ));
    }

    Ok(())
}

fn is_safe_asset_name(value: &str) -> bool {
    let trimmed = value.trim();
    !trimmed.is_empty()
        && trimmed == value
        && !trimmed.contains('/')
        && !trimmed.contains('\\')
        && !trimmed.contains(':')
        && Path::new(trimmed)
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

pub(crate) fn current_plan_os() -> &'static str {
    std::env::consts::OS
}

pub(crate) fn current_plan_arch() -> Option<&'static str> {
    match std::env::consts::ARCH {
        "x86_64" | "amd64" => Some("x86_64"),
        "aarch64" | "arm64" => Some("aarch64"),
        _ => None,
    }
}

pub(crate) fn expected_asset_kind_for_os(os: &str) -> Option<&'static str> {
    match os {
        "macos" => Some("macos_app_zip"),
        "linux" => Some("appimage"),
        "windows" => Some("wix_bundle_exe"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DesktopUpdatePlan, PLAN_PRODUCT, PLAN_SCHEMA_VERSION, PlanValidationErrorCode,
        current_plan_arch, current_plan_os, expected_asset_kind_for_os, read_and_validate_plan,
        sha256_file, validate_plan,
    };
    use std::{fs, path::PathBuf};

    #[test]
    fn plan_validates_shape_and_asset_sha() {
        let temp_dir = tempfile::tempdir().unwrap();
        let asset_path = temp_dir.path().join("Pioneer-aarch64.app.zip");
        fs::write(asset_path.as_path(), b"verified desktop asset").unwrap();

        let validated = validate_plan(valid_plan(
            asset_path.clone(),
            sha256_file(&asset_path).unwrap(),
        ))
        .unwrap();

        assert_eq!(validated.plan.target_version, "0.26.0");
        assert_eq!(validated.plan.os, current_plan_os());
        assert_eq!(
            validated.plan.arch,
            current_plan_arch().expect("test host architecture is supported")
        );
        assert_eq!(validated.actual_asset_sha256, validated.plan.asset_sha256);
    }

    #[test]
    fn plan_rejects_wrong_schema() {
        let temp_dir = tempfile::tempdir().unwrap();
        let asset_path = write_asset(temp_dir.path(), b"asset");
        let mut plan = valid_plan(asset_path.clone(), sha256_file(&asset_path).unwrap());
        plan.schema_version = 2;

        let error = validate_plan(plan).unwrap_err();

        assert_eq!(error.code(), PlanValidationErrorCode::WrongSchema);
    }

    #[test]
    fn plan_rejects_wrong_product() {
        let temp_dir = tempfile::tempdir().unwrap();
        let asset_path = write_asset(temp_dir.path(), b"asset");
        let mut plan = valid_plan(asset_path.clone(), sha256_file(&asset_path).unwrap());
        plan.product = "other".to_owned();

        let error = validate_plan(plan).unwrap_err();

        assert_eq!(error.code(), PlanValidationErrorCode::WrongProduct);
    }

    #[test]
    fn plan_rejects_missing_asset() {
        let temp_dir = tempfile::tempdir().unwrap();
        let plan = valid_plan(
            temp_dir.path().join("missing.bin"),
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
        );

        let error = validate_plan(plan).unwrap_err();

        assert_eq!(error.code(), PlanValidationErrorCode::MissingAsset);
    }

    #[test]
    fn plan_rejects_invalid_versions() {
        let temp_dir = tempfile::tempdir().unwrap();
        let asset_path = write_asset(temp_dir.path(), b"asset");
        let mut plan = valid_plan(asset_path.clone(), sha256_file(&asset_path).unwrap());
        plan.target_version = "not-a-version".to_owned();

        let error = validate_plan(plan).unwrap_err();

        assert_eq!(error.code(), PlanValidationErrorCode::InvalidVersion);
    }

    #[test]
    fn plan_rejects_non_upgrade_version() {
        let temp_dir = tempfile::tempdir().unwrap();
        let asset_path = write_asset(temp_dir.path(), b"asset");
        let mut plan = valid_plan(asset_path.clone(), sha256_file(&asset_path).unwrap());
        plan.target_version = plan.current_version.clone();

        let error = validate_plan(plan).unwrap_err();

        assert_eq!(error.code(), PlanValidationErrorCode::NonUpgradeVersion);
    }

    #[test]
    fn plan_rejects_wrong_os() {
        let temp_dir = tempfile::tempdir().unwrap();
        let asset_path = write_asset(temp_dir.path(), b"asset");
        let mut plan = valid_plan(asset_path.clone(), sha256_file(&asset_path).unwrap());
        plan.os = other_os();

        let error = validate_plan(plan).unwrap_err();

        assert_eq!(error.code(), PlanValidationErrorCode::WrongOs);
    }

    #[test]
    fn plan_rejects_wrong_arch() {
        let temp_dir = tempfile::tempdir().unwrap();
        let asset_path = write_asset(temp_dir.path(), b"asset");
        let mut plan = valid_plan(asset_path.clone(), sha256_file(&asset_path).unwrap());
        plan.arch = other_arch();

        let error = validate_plan(plan).unwrap_err();

        assert_eq!(error.code(), PlanValidationErrorCode::WrongArch);
    }

    #[test]
    fn plan_rejects_wrong_asset_kind() {
        let temp_dir = tempfile::tempdir().unwrap();
        let asset_path = write_asset(temp_dir.path(), b"asset");
        let mut plan = valid_plan(asset_path.clone(), sha256_file(&asset_path).unwrap());
        plan.asset_kind = "unexpected_kind".to_owned();

        let error = validate_plan(plan).unwrap_err();

        assert_eq!(error.code(), PlanValidationErrorCode::WrongAssetKind);
    }

    #[test]
    fn plan_rejects_relative_asset_path() {
        let temp_dir = tempfile::tempdir().unwrap();
        let asset_path = write_asset(temp_dir.path(), b"asset");
        let mut plan = valid_plan(asset_path.clone(), sha256_file(&asset_path).unwrap());
        plan.asset_path = PathBuf::from("asset.bin");

        let error = validate_plan(plan).unwrap_err();

        assert_eq!(error.code(), PlanValidationErrorCode::UnsafePath);
    }

    #[test]
    fn plan_rejects_partial_asset_path() {
        let temp_dir = tempfile::tempdir().unwrap();
        let asset_path = temp_dir.path().join("asset.bin.part");
        fs::write(asset_path.as_path(), b"partial").unwrap();
        let plan = valid_plan(asset_path.clone(), sha256_file(&asset_path).unwrap());

        let error = validate_plan(plan).unwrap_err();

        assert_eq!(error.code(), PlanValidationErrorCode::UnsafePath);
    }

    #[test]
    fn plan_rejects_asset_name_that_escapes_cache_root() {
        let temp_dir = tempfile::tempdir().unwrap();
        let asset_path = write_asset(temp_dir.path(), b"asset");
        let mut plan = valid_plan(asset_path.clone(), sha256_file(&asset_path).unwrap());
        plan.asset_name = "../asset.bin".to_owned();

        let error = validate_plan(plan).unwrap_err();

        assert_eq!(error.code(), PlanValidationErrorCode::UnsafePath);
    }

    #[test]
    fn plan_rejects_invalid_sha() {
        let temp_dir = tempfile::tempdir().unwrap();
        let asset_path = write_asset(temp_dir.path(), b"asset");
        let plan = valid_plan(asset_path, "not-a-sha256".to_owned());

        let error = validate_plan(plan).unwrap_err();

        assert_eq!(error.code(), PlanValidationErrorCode::InvalidSha256);
    }

    #[test]
    fn plan_rejects_sha_mismatch() {
        let temp_dir = tempfile::tempdir().unwrap();
        let asset_path = write_asset(temp_dir.path(), b"actual");
        let plan = valid_plan(
            asset_path,
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
        );

        let error = validate_plan(plan).unwrap_err();

        assert_eq!(error.code(), PlanValidationErrorCode::Sha256Mismatch);
    }

    #[test]
    fn plan_reads_json_from_disk() {
        let temp_dir = tempfile::tempdir().unwrap();
        let asset_path = write_asset(temp_dir.path(), b"asset");
        let plan = valid_plan(asset_path.clone(), sha256_file(&asset_path).unwrap());
        let plan_path = temp_dir.path().join("plan.json");
        fs::write(
            plan_path.as_path(),
            serde_json::to_vec_pretty(&plan).unwrap(),
        )
        .unwrap();

        let validated = read_and_validate_plan(plan_path.as_path()).unwrap();

        assert_eq!(validated.plan.asset_name, "asset.bin");
    }

    fn write_asset(dir: &std::path::Path, bytes: &[u8]) -> PathBuf {
        let asset_path = dir.join("asset.bin");
        fs::write(asset_path.as_path(), bytes).unwrap();
        asset_path
    }

    fn valid_plan(asset_path: PathBuf, asset_sha256: String) -> DesktopUpdatePlan {
        let os = current_plan_os();
        let arch = current_plan_arch().expect("test host architecture is supported");
        let asset_name = asset_path
            .file_name()
            .expect("test asset has file name")
            .to_string_lossy()
            .into_owned();

        DesktopUpdatePlan {
            schema_version: PLAN_SCHEMA_VERSION,
            product: PLAN_PRODUCT.to_owned(),
            target_version: "0.26.0".to_owned(),
            current_version: "0.25.0".to_owned(),
            tag: "v0.26.0".to_owned(),
            os: os.to_owned(),
            arch: arch.to_owned(),
            asset_kind: expected_asset_kind_for_os(os)
                .expect("test host OS is supported")
                .to_owned(),
            asset_path,
            asset_name,
            asset_sha256,
            current_pid: 12345,
            current_exe_path: absolute_fixture_path("current/pioneer-app"),
            install_root_path: absolute_fixture_path("install-root"),
            appimage_path: None,
            restart_after_apply: true,
        }
    }

    fn absolute_fixture_path(relative: &str) -> PathBuf {
        std::env::temp_dir()
            .join("pioneer-app-updater-test")
            .join(relative)
    }

    fn other_os() -> String {
        match current_plan_os() {
            "macos" => "linux",
            _ => "macos",
        }
        .to_owned()
    }

    fn other_arch() -> String {
        match current_plan_arch().expect("test host architecture is supported") {
            "x86_64" => "aarch64",
            _ => "x86_64",
        }
        .to_owned()
    }
}
