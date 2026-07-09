use crate::plan::DesktopUpdatePlan;
use anyhow::{Context as _, Result, anyhow, bail};
use std::{
    fs, io,
    path::{Path, PathBuf},
};

const DESKTOP_UPDATES_DIR: &str = "desktop-updates";
const DESKTOP_UPDATE_STATE_FILE: &str = "state.json";
const DESKTOP_DOWNLOADS_DIR: &str = "downloads";
const DESKTOP_STAGING_DIR: &str = "staging";

pub fn cleanup_successful_apply(plan_path: &Path, plan: &DesktopUpdatePlan) -> Result<()> {
    let update_root = update_root_from_plan_path(plan_path)?;
    let downloads_dir = update_root.join(DESKTOP_DOWNLOADS_DIR);
    if !plan.asset_path.starts_with(downloads_dir.as_path()) {
        bail!(
            "desktop update asset `{}` is not inside download cache `{}`",
            plan.asset_path.display(),
            downloads_dir.display()
        );
    }

    remove_file_if_exists(update_root.join(DESKTOP_UPDATE_STATE_FILE).as_path())?;
    empty_dir(downloads_dir.as_path())?;
    Ok(())
}

fn update_root_from_plan_path(plan_path: &Path) -> Result<PathBuf> {
    let plan_dir = plan_path
        .parent()
        .ok_or_else(|| anyhow!("desktop update plan path has no parent"))?;
    let staging_dir = plan_dir
        .parent()
        .ok_or_else(|| anyhow!("desktop update plan directory has no parent"))?;
    if staging_dir.file_name().and_then(|name| name.to_str()) != Some(DESKTOP_STAGING_DIR) {
        bail!(
            "desktop update plan is not under `{DESKTOP_STAGING_DIR}`: `{}`",
            plan_path.display()
        );
    }

    let update_root = staging_dir
        .parent()
        .ok_or_else(|| anyhow!("desktop update staging directory has no parent"))?;
    if update_root.file_name().and_then(|name| name.to_str()) != Some(DESKTOP_UPDATES_DIR) {
        bail!(
            "desktop update staging directory is not under `{DESKTOP_UPDATES_DIR}`: `{}`",
            staging_dir.display()
        );
    }

    Ok(update_root.to_path_buf())
}

fn remove_file_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("failed to remove `{}`", path.display())),
    }
}

fn empty_dir(path: &Path) -> Result<()> {
    match fs::remove_dir_all(path) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error).with_context(|| format!("failed to remove `{}`", path.display()));
        }
    }

    fs::create_dir_all(path)
        .with_context(|| format!("failed to recreate empty directory `{}`", path.display()))
}

#[cfg(test)]
mod tests {
    use super::cleanup_successful_apply;
    use crate::plan::{DesktopUpdatePlan, PLAN_PRODUCT, PLAN_SCHEMA_VERSION};
    use std::{fs, path::PathBuf};

    #[test]
    fn successful_apply_cleanup_removes_state_and_empties_downloads() {
        let temp_dir = tempfile::tempdir().unwrap();
        let update_root = temp_dir.path().join("desktop-updates");
        let downloads_dir = update_root.join("downloads");
        let asset_path = downloads_dir
            .join("v0.26.0")
            .join("Pioneer-aarch64.app.zip");
        let plan_path = update_root
            .join("staging")
            .join("v0.26.0-123")
            .join("plan.json");
        fs::create_dir_all(asset_path.parent().unwrap()).unwrap();
        fs::create_dir_all(plan_path.parent().unwrap()).unwrap();
        fs::write(update_root.join("state.json"), b"ready").unwrap();
        fs::write(asset_path.as_path(), b"asset").unwrap();

        cleanup_successful_apply(plan_path.as_path(), &plan(asset_path)).unwrap();

        assert!(!update_root.join("state.json").exists());
        assert!(downloads_dir.is_dir());
        assert_eq!(fs::read_dir(downloads_dir).unwrap().count(), 0);
    }

    #[test]
    fn successful_apply_cleanup_refuses_asset_outside_downloads() {
        let temp_dir = tempfile::tempdir().unwrap();
        let update_root = temp_dir.path().join("desktop-updates");
        let plan_path = update_root
            .join("staging")
            .join("v0.26.0-123")
            .join("plan.json");
        fs::create_dir_all(plan_path.parent().unwrap()).unwrap();

        let error = cleanup_successful_apply(
            plan_path.as_path(),
            &plan(temp_dir.path().join("asset.zip")),
        )
        .unwrap_err();

        assert!(error.to_string().contains("not inside download cache"));
    }

    fn plan(asset_path: PathBuf) -> DesktopUpdatePlan {
        DesktopUpdatePlan {
            schema_version: PLAN_SCHEMA_VERSION,
            product: PLAN_PRODUCT.to_owned(),
            target_version: "0.26.0".to_owned(),
            current_version: "0.25.0".to_owned(),
            tag: "v0.26.0".to_owned(),
            os: std::env::consts::OS.to_owned(),
            arch: "aarch64".to_owned(),
            asset_kind: "macos_app_zip".to_owned(),
            asset_path,
            asset_name: "Pioneer-aarch64.app.zip".to_owned(),
            asset_sha256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .to_owned(),
            current_pid: 12345,
            current_exe_path: PathBuf::from("/tmp/pioneer-app"),
            install_root_path: PathBuf::from("/tmp/Pioneer.app"),
            appimage_path: None,
            restart_after_apply: true,
        }
    }
}
