use super::{
    download::DESKTOP_UPDATES_DIR,
    manifest::{DESKTOP_UPDATE_PRODUCT, DESKTOP_UPDATE_SCHEMA_VERSION},
};
use anyhow::{Context as _, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use std::{
    fs,
    io::Write as _,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

pub(crate) const DESKTOP_UPDATE_STAGING_DIR: &str = "staging";
pub(crate) const DESKTOP_UPDATE_PLAN_FILE: &str = "plan.json";
pub(crate) const DESKTOP_UPDATE_HELPER_NAME: &str = "pioneer-app-updater";
#[cfg(windows)]
pub(crate) const DESKTOP_UPDATE_HELPER_EXE_NAME: &str = "pioneer-app-updater.exe";
#[cfg(not(windows))]
pub(crate) const DESKTOP_UPDATE_HELPER_EXE_NAME: &str = DESKTOP_UPDATE_HELPER_NAME;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DesktopUpdatePlanInput {
    pub(crate) target_version: String,
    pub(crate) current_version: String,
    pub(crate) tag: String,
    pub(crate) os: String,
    pub(crate) arch: String,
    pub(crate) asset_kind: String,
    pub(crate) asset_path: PathBuf,
    pub(crate) asset_name: String,
    pub(crate) asset_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DesktopUpdateApplyPlan {
    pub(crate) schema_version: u32,
    pub(crate) product: String,
    pub(crate) target_version: String,
    pub(crate) current_version: String,
    pub(crate) tag: String,
    pub(crate) os: String,
    pub(crate) arch: String,
    pub(crate) asset_kind: String,
    pub(crate) asset_path: PathBuf,
    pub(crate) asset_name: String,
    pub(crate) asset_sha256: String,
    pub(crate) current_pid: u32,
    pub(crate) current_exe_path: PathBuf,
    pub(crate) install_root_path: PathBuf,
    pub(crate) appimage_path: Option<PathBuf>,
    pub(crate) restart_after_apply: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreparedDesktopUpdateApply {
    pub(crate) plan_path: PathBuf,
    pub(crate) helper_path: PathBuf,
    pub(crate) plan: DesktopUpdateApplyPlan,
}

pub(crate) fn prepare_desktop_update_apply(
    runtime_home: &Path,
    input: DesktopUpdatePlanInput,
) -> Result<PreparedDesktopUpdateApply> {
    let current_exe_path =
        std::env::current_exe().context("failed to resolve current desktop executable path")?;
    let appimage_path = current_appimage_path();
    let install_root_path = resolve_install_root_path(&current_exe_path, appimage_path.as_deref())?;
    let bundled_helper_path = resolve_helper_sidecar_path(&current_exe_path)?;
    let staging_dir = runtime_home
        .join(DESKTOP_UPDATES_DIR)
        .join(DESKTOP_UPDATE_STAGING_DIR)
        .join(update_plan_staging_id(input.target_version.as_str()));
    let plan_path = staging_dir.join(DESKTOP_UPDATE_PLAN_FILE);

    let plan = DesktopUpdateApplyPlan {
        schema_version: DESKTOP_UPDATE_SCHEMA_VERSION,
        product: DESKTOP_UPDATE_PRODUCT.to_owned(),
        target_version: input.target_version,
        current_version: input.current_version,
        tag: input.tag,
        os: input.os,
        arch: input.arch,
        asset_kind: input.asset_kind,
        asset_path: input.asset_path,
        asset_name: input.asset_name,
        asset_sha256: input.asset_sha256,
        current_pid: std::process::id(),
        current_exe_path,
        install_root_path,
        appimage_path,
        restart_after_apply: true,
    };

    assert_valid_plan_for_desktop(&plan)?;
    write_plan_file_atomic(plan_path.as_path(), &plan)?;
    let helper_path =
        prepare_helper_sidecar_launch_path(bundled_helper_path.as_path(), staging_dir.as_path())?;

    Ok(PreparedDesktopUpdateApply {
        plan_path,
        helper_path,
        plan,
    })
}

pub(crate) fn resolve_helper_sidecar_path(current_exe_path: &Path) -> Result<PathBuf> {
    let helper_dir = current_exe_path.parent().ok_or_else(|| {
        anyhow!(
            "current desktop executable path `{}` has no parent directory",
            current_exe_path.display()
        )
    })?;

    Ok(helper_dir.join(DESKTOP_UPDATE_HELPER_EXE_NAME))
}

fn prepare_helper_sidecar_launch_path(
    bundled_helper_path: &Path,
    staging_dir: &Path,
) -> Result<PathBuf> {
    #[cfg(windows)]
    {
        fs::create_dir_all(staging_dir).with_context(|| {
            format!(
                "failed to create desktop update staging directory `{}`",
                staging_dir.display()
            )
        })?;
        let launch_path = staging_dir.join(DESKTOP_UPDATE_HELPER_EXE_NAME);
        fs::copy(bundled_helper_path, launch_path.as_path()).with_context(|| {
            format!(
                "failed to stage desktop updater helper from `{}` to `{}`",
                bundled_helper_path.display(),
                launch_path.display()
            )
        })?;
        Ok(launch_path)
    }

    #[cfg(not(windows))]
    {
        let _ = staging_dir;
        Ok(bundled_helper_path.to_path_buf())
    }
}

pub(crate) fn resolve_install_root_path(
    current_exe_path: &Path,
    appimage_path: Option<&Path>,
) -> Result<PathBuf> {
    resolve_install_root_path_for_os(current_exe_path, appimage_path, std::env::consts::OS)
}

pub(crate) fn resolve_install_root_path_for_os(
    current_exe_path: &Path,
    appimage_path: Option<&Path>,
    os: &str,
) -> Result<PathBuf> {
    match os {
        "macos" => macos_app_root_from_exe(current_exe_path),
        "linux" => Ok(appimage_path
            .filter(|path| !path.as_os_str().is_empty())
            .map(Path::to_path_buf)
            .unwrap_or_else(|| current_exe_path.to_path_buf())),
        "windows" => current_exe_path
            .parent()
            .map(Path::to_path_buf)
            .ok_or_else(|| {
                anyhow!(
                    "current desktop executable path `{}` has no parent directory",
                    current_exe_path.display()
                )
            }),
        _ => Ok(current_exe_path.to_path_buf()),
    }
}

fn current_appimage_path() -> Option<PathBuf> {
    if std::env::consts::OS != "linux" {
        return None;
    }

    std::env::var_os("APPIMAGE")
        .filter(|value| !value.as_os_str().is_empty())
        .map(PathBuf::from)
}

fn macos_app_root_from_exe(current_exe_path: &Path) -> Result<PathBuf> {
    current_exe_path
        .ancestors()
        .find(|ancestor| {
            ancestor
                .file_name()
                .and_then(|file_name| file_name.to_str())
                .is_some_and(|file_name| file_name.ends_with(".app"))
        })
        .map(Path::to_path_buf)
        .ok_or_else(|| {
            anyhow!(
                "current desktop executable path `{}` is not inside a macOS .app bundle",
                current_exe_path.display()
            )
        })
}

fn write_plan_file_atomic(path: &Path, plan: &DesktopUpdateApplyPlan) -> Result<()> {
    let parent = path.parent().ok_or_else(|| {
        anyhow!(
            "desktop update plan path `{}` has no parent",
            path.display()
        )
    })?;
    fs::create_dir_all(parent).with_context(|| {
        format!(
            "failed to create desktop update staging directory `{}`",
            parent.display()
        )
    })?;

    let bytes =
        serde_json::to_vec_pretty(plan).context("failed to serialize desktop update plan")?;
    let file_name = path
        .file_name()
        .and_then(|file_name| file_name.to_str())
        .unwrap_or(DESKTOP_UPDATE_PLAN_FILE);
    let tmp_path = path.with_file_name(format!(".{file_name}.tmp-{}", std::process::id()));

    let mut tmp_file = fs::File::create(tmp_path.as_path()).with_context(|| {
        format!(
            "failed to create temporary desktop update plan `{}`",
            tmp_path.display()
        )
    })?;
    tmp_file.write_all(bytes.as_slice()).with_context(|| {
        format!(
            "failed to write temporary desktop update plan `{}`",
            tmp_path.display()
        )
    })?;
    tmp_file.flush().with_context(|| {
        format!(
            "failed to flush temporary desktop update plan `{}`",
            tmp_path.display()
        )
    })?;
    drop(tmp_file);

    #[cfg(windows)]
    if path.exists() {
        fs::remove_file(path).with_context(|| {
            format!(
                "failed to replace existing desktop update plan `{}`",
                path.display()
            )
        })?;
    }

    fs::rename(tmp_path.as_path(), path).with_context(|| {
        let _ = fs::remove_file(tmp_path.as_path());
        format!(
            "failed to finalize desktop update plan `{}`",
            path.display()
        )
    })
}

fn update_plan_staging_id(target_version: &str) -> String {
    let timestamp_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default();

    format!(
        "v{}-{}-{timestamp_ms}",
        sanitize_staging_component(target_version),
        std::process::id()
    )
}

fn sanitize_staging_component(value: &str) -> String {
    let sanitized = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '_') {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();

    if sanitized.is_empty() {
        "unknown".to_owned()
    } else {
        sanitized
    }
}

pub(crate) fn assert_valid_plan_for_desktop(plan: &DesktopUpdateApplyPlan) -> Result<()> {
    if plan.schema_version != DESKTOP_UPDATE_SCHEMA_VERSION {
        bail!(
            "unsupported desktop update plan schema {}",
            plan.schema_version
        );
    }
    if plan.product != DESKTOP_UPDATE_PRODUCT {
        bail!("unsupported desktop update plan product `{}`", plan.product);
    }
    if plan.target_version.trim().is_empty()
        || plan.current_version.trim().is_empty()
        || plan.tag.trim().is_empty()
        || plan.os.trim().is_empty()
        || plan.arch.trim().is_empty()
        || plan.asset_kind.trim().is_empty()
        || plan.asset_name.trim().is_empty()
        || plan.asset_sha256.trim().is_empty()
        || plan.asset_path.as_os_str().is_empty()
        || plan.current_exe_path.as_os_str().is_empty()
        || plan.install_root_path.as_os_str().is_empty()
    {
        bail!("desktop update plan contains an empty required field");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        DESKTOP_UPDATE_HELPER_EXE_NAME, DesktopUpdateApplyPlan, assert_valid_plan_for_desktop,
        prepare_helper_sidecar_launch_path, resolve_helper_sidecar_path,
        resolve_install_root_path_for_os, sanitize_staging_component,
    };
    use std::path::{Path, PathBuf};

    #[test]
    fn helper_path_resolves_next_to_current_executable() {
        let helper = resolve_helper_sidecar_path(Path::new(
            "/Applications/Pioneer.app/Contents/MacOS/pioneer-app",
        ))
        .unwrap();

        assert_eq!(
            helper,
            PathBuf::from("/Applications/Pioneer.app/Contents/MacOS")
                .join(DESKTOP_UPDATE_HELPER_EXE_NAME)
        );
    }

    #[test]
    #[cfg(not(windows))]
    fn helper_launch_path_uses_bundled_helper_on_unix() {
        let bundled = Path::new("/Applications/Pioneer.app/Contents/MacOS/pioneer-app-updater");
        let launch_path =
            prepare_helper_sidecar_launch_path(bundled, Path::new("/tmp/staging")).unwrap();

        assert_eq!(launch_path, bundled);
    }

    #[test]
    fn macos_install_root_is_app_bundle() {
        let root = resolve_install_root_path_for_os(
            Path::new("/Applications/Pioneer.app/Contents/MacOS/pioneer-app"),
            None,
            "macos",
        )
        .unwrap();

        assert_eq!(root, PathBuf::from("/Applications/Pioneer.app"));
    }

    #[test]
    fn linux_install_root_prefers_appimage_path() {
        let root = resolve_install_root_path_for_os(
            Path::new("/tmp/.mount_pioneer/usr/bin/pioneer-app"),
            Some(Path::new("/home/alex/Pioneer.AppImage")),
            "linux",
        )
        .unwrap();

        assert_eq!(root, PathBuf::from("/home/alex/Pioneer.AppImage"));
    }

    #[test]
    fn windows_install_root_is_executable_parent() {
        let root = resolve_install_root_path_for_os(
            Path::new("/Program Files/Pioneer/pioneer-app.exe"),
            None,
            "windows",
        )
        .unwrap();

        assert_eq!(root, PathBuf::from("/Program Files/Pioneer"));
    }

    #[test]
    fn staging_component_is_path_safe() {
        assert_eq!(
            sanitize_staging_component("../0.26.0 beta"),
            ".._0.26.0_beta"
        );
        assert_eq!(sanitize_staging_component(""), "unknown");
    }

    #[test]
    fn valid_plan_matches_desktop_contract() {
        let plan = DesktopUpdateApplyPlan {
            schema_version: 1,
            product: "pioneer-desktop".to_owned(),
            target_version: "0.26.0".to_owned(),
            current_version: "0.25.0".to_owned(),
            tag: "v0.26.0".to_owned(),
            os: "macos".to_owned(),
            arch: "aarch64".to_owned(),
            asset_kind: "macos_app_zip".to_owned(),
            asset_path: PathBuf::from("/tmp/Pioneer-aarch64.app.zip"),
            asset_name: "Pioneer-aarch64.app.zip".to_owned(),
            asset_sha256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .to_owned(),
            current_pid: 12345,
            current_exe_path: PathBuf::from("/Applications/Pioneer.app/Contents/MacOS/pioneer-app"),
            install_root_path: PathBuf::from("/Applications/Pioneer.app"),
            appimage_path: None,
            restart_after_apply: true,
        };

        assert_valid_plan_for_desktop(&plan).unwrap();
    }
}
