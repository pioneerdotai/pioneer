use crate::{plan::DesktopUpdatePlan, platform::PlatformApplyOutcome};
use anyhow::{Context as _, Result, bail};
use serde_json::json;
use std::{
    path::{Path, PathBuf},
    process::{Command, ExitStatus},
};

const WINDOWS_ASSET_KIND: &str = "wix_bundle_exe";
const DESKTOP_EXE_NAME: &str = "pioneer-app.exe";

pub fn apply(plan: &DesktopUpdatePlan, _plan_path: &Path) -> Result<PlatformApplyOutcome> {
    require_windows_plan(plan)?;

    let status = Command::new(plan.asset_path.as_path())
        .status()
        .with_context(|| {
            format!(
                "failed to launch Windows installer bundle `{}`",
                plan.asset_path.display()
            )
        })?;
    if !status.success() {
        bail!(
            "Windows installer exited with status {}",
            format_exit_status(status)
        );
    }

    let relaunch_path = installed_desktop_exe_path(plan);
    Command::new(relaunch_path.as_path())
        .spawn()
        .with_context(|| {
            format!(
                "failed to relaunch installed Pioneer desktop `{}`",
                relaunch_path.display()
            )
        })?;

    Ok(PlatformApplyOutcome {
        result_details: Some(json!({
            "installer_exit_code": status.code(),
        })),
    })
}

fn require_windows_plan(plan: &DesktopUpdatePlan) -> Result<()> {
    if plan.asset_kind != WINDOWS_ASSET_KIND {
        bail!(
            "Windows desktop update requires asset kind `{WINDOWS_ASSET_KIND}`, got `{}`",
            plan.asset_kind
        );
    }
    Ok(())
}

fn installed_desktop_exe_path(plan: &DesktopUpdatePlan) -> PathBuf {
    if plan
        .current_exe_path
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.eq_ignore_ascii_case(DESKTOP_EXE_NAME))
    {
        return plan.current_exe_path.clone();
    }

    plan.install_root_path.join(DESKTOP_EXE_NAME)
}

fn format_exit_status(status: ExitStatus) -> String {
    status
        .code()
        .map(|code| code.to_string())
        .unwrap_or_else(|| "unknown".to_owned())
}

#[cfg(test)]
mod tests {
    use super::{DESKTOP_EXE_NAME, installed_desktop_exe_path};
    use crate::plan::{DesktopUpdatePlan, PLAN_PRODUCT, PLAN_SCHEMA_VERSION};
    use std::path::PathBuf;

    #[test]
    fn windows_relaunches_current_desktop_exe_when_available() {
        let plan = windows_plan(PathBuf::from(r"C:\Program Files\Pioneer\pioneer-app.exe"));

        assert_eq!(installed_desktop_exe_path(&plan), plan.current_exe_path);
    }

    #[test]
    fn windows_falls_back_to_install_root_exe() {
        let mut plan = windows_plan(PathBuf::from(r"C:\Temp\helper.exe"));
        plan.install_root_path = PathBuf::from(r"C:\Program Files\Pioneer");

        assert_eq!(
            installed_desktop_exe_path(&plan),
            PathBuf::from(r"C:\Program Files\Pioneer").join(DESKTOP_EXE_NAME)
        );
    }

    fn windows_plan(current_exe_path: PathBuf) -> DesktopUpdatePlan {
        DesktopUpdatePlan {
            schema_version: PLAN_SCHEMA_VERSION,
            product: PLAN_PRODUCT.to_owned(),
            target_version: "0.26.0".to_owned(),
            current_version: "0.25.0".to_owned(),
            tag: "v0.26.0".to_owned(),
            os: "windows".to_owned(),
            arch: "x86_64".to_owned(),
            asset_kind: "wix_bundle_exe".to_owned(),
            asset_path: PathBuf::from(r"C:\Temp\Pioneer-x86_64.exe"),
            asset_name: "Pioneer-x86_64.exe".to_owned(),
            asset_sha256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .to_owned(),
            current_pid: 12345,
            current_exe_path,
            install_root_path: PathBuf::from(r"C:\Program Files\Pioneer"),
            appimage_path: None,
            restart_after_apply: true,
        }
    }
}
