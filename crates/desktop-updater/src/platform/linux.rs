use crate::{plan::DesktopUpdatePlan, platform::PlatformApplyOutcome};
use anyhow::{Context as _, Result, anyhow, bail};
use std::{
    fs::{self, OpenOptions},
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

const LINUX_ASSET_KIND: &str = "appimage";

pub fn apply(plan: &DesktopUpdatePlan, _plan_path: &Path) -> Result<PlatformApplyOutcome> {
    require_linux_plan(plan)?;

    let target = replacement_target(plan)?;
    verify_target_writable(target.as_path())?;
    let rollback_path = rollback_path_for_target(target.as_path());
    fs::copy(target.as_path(), rollback_path.as_path()).with_context(|| {
        format!(
            "failed to copy current AppImage `{}` to rollback path `{}`",
            target.display(),
            rollback_path.display()
        )
    })?;

    let tmp_path = replacement_tmp_path(target.as_path())?;
    if let Err(error) = copy_asset_to_tmp(plan.asset_path.as_path(), tmp_path.as_path()) {
        let _ = fs::remove_file(tmp_path.as_path());
        return Err(error);
    }

    fs::rename(tmp_path.as_path(), target.as_path()).with_context(|| {
        let _ = fs::remove_file(tmp_path.as_path());
        format!(
            "failed to move verified AppImage into place at `{}`",
            target.display()
        )
    })?;
    set_executable(target.as_path())?;

    Command::new(target.as_path())
        .spawn()
        .with_context(|| format!("failed to launch updated AppImage `{}`", target.display()))?;

    Ok(PlatformApplyOutcome::default())
}

fn require_linux_plan(plan: &DesktopUpdatePlan) -> Result<()> {
    if plan.asset_kind != LINUX_ASSET_KIND {
        bail!(
            "Linux desktop update requires asset kind `{LINUX_ASSET_KIND}`, got `{}`",
            plan.asset_kind
        );
    }
    Ok(())
}

fn replacement_target(plan: &DesktopUpdatePlan) -> Result<PathBuf> {
    if let Some(appimage_path) = plan
        .appimage_path
        .as_ref()
        .filter(|path| !path.as_os_str().is_empty())
    {
        return Ok(appimage_path.clone());
    }

    if is_safe_appimage_target(plan.install_root_path.as_path()) {
        return Ok(plan.install_root_path.clone());
    }

    bail!("Linux desktop update requires a real AppImage path from APPIMAGE")
}

fn is_safe_appimage_target(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.ends_with(".AppImage"))
        && !path.to_string_lossy().contains("/.mount_")
}

fn verify_target_writable(target: &Path) -> Result<()> {
    if !target.is_file() {
        bail!("current AppImage target is missing");
    }

    let metadata = fs::metadata(target)
        .with_context(|| format!("failed to inspect AppImage target `{}`", target.display()))?;
    if metadata.permissions().readonly() {
        bail!("current AppImage target is read-only");
    }

    OpenOptions::new()
        .write(true)
        .open(target)
        .with_context(|| {
            format!(
                "current AppImage target `{}` is not writable",
                target.display()
            )
        })?;

    Ok(())
}

fn copy_asset_to_tmp(asset_path: &Path, tmp_path: &Path) -> Result<()> {
    fs::copy(asset_path, tmp_path).with_context(|| {
        format!(
            "failed to copy verified AppImage `{}` to temporary replacement `{}`",
            asset_path.display(),
            tmp_path.display()
        )
    })?;
    set_executable(tmp_path)
}

fn set_executable(path: &Path) -> Result<()> {
    let mut permissions = fs::metadata(path)
        .with_context(|| format!("failed to inspect permissions for `{}`", path.display()))?
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).with_context(|| {
        format!(
            "failed to set executable permissions on `{}`",
            path.display()
        )
    })
}

fn rollback_path_for_target(target: &Path) -> PathBuf {
    let preferred = target.with_extension("AppImage.previous");
    if !preferred.exists() {
        return preferred;
    }

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default();
    target.with_extension(format!(
        "AppImage.previous-{}-{timestamp}",
        std::process::id()
    ))
}

fn replacement_tmp_path(target: &Path) -> Result<PathBuf> {
    let file_name = target
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow!("AppImage target has no file name"))?;
    Ok(target.with_file_name(format!(".{file_name}.new-{}", std::process::id())))
}

#[cfg(test)]
mod tests {
    use super::{is_safe_appimage_target, replacement_target, verify_target_writable};
    use crate::plan::{DesktopUpdatePlan, PLAN_PRODUCT, PLAN_SCHEMA_VERSION};
    use std::{fs, path::PathBuf};

    #[test]
    fn linux_prefers_appimage_env_path() {
        let plan = linux_plan(Some(PathBuf::from("/home/alex/Pioneer.AppImage")));

        assert_eq!(
            replacement_target(&plan).unwrap(),
            PathBuf::from("/home/alex/Pioneer.AppImage")
        );
    }

    #[test]
    fn linux_rejects_mount_path_fallback() {
        assert!(!is_safe_appimage_target(std::path::Path::new(
            "/tmp/.mount_pioneer/usr/bin/pioneer-app"
        )));
    }

    #[test]
    fn linux_detects_missing_target() {
        let error =
            verify_target_writable(std::path::Path::new("/tmp/definitely-missing.AppImage"))
                .unwrap_err();

        assert!(error.to_string().contains("missing"));
    }

    fn linux_plan(appimage_path: Option<PathBuf>) -> DesktopUpdatePlan {
        DesktopUpdatePlan {
            schema_version: PLAN_SCHEMA_VERSION,
            product: PLAN_PRODUCT.to_owned(),
            target_version: "0.26.0".to_owned(),
            current_version: "0.25.0".to_owned(),
            tag: "v0.26.0".to_owned(),
            os: "linux".to_owned(),
            arch: "x86_64".to_owned(),
            asset_kind: "appimage".to_owned(),
            asset_path: PathBuf::from("/tmp/pioneer-linux-x86_64.AppImage"),
            asset_name: "pioneer-linux-x86_64.AppImage".to_owned(),
            asset_sha256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .to_owned(),
            current_pid: 12345,
            current_exe_path: PathBuf::from("/tmp/.mount_pioneer/usr/bin/pioneer-app"),
            install_root_path: PathBuf::from("/tmp/.mount_pioneer/usr/bin/pioneer-app"),
            appimage_path,
            restart_after_apply: true,
        }
    }
}
