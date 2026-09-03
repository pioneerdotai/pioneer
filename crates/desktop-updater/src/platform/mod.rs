use crate::plan::DesktopUpdatePlan;
use anyhow::{Context as _, Result, anyhow};
use serde_json::Value;
use std::{
    ffi::OsString,
    path::{Path, PathBuf},
    process::Command,
};

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(windows)]
mod windows;

#[derive(Debug, Clone)]
pub struct PlatformRelaunch {
    program: PathBuf,
    args: Vec<OsString>,
}

impl PlatformRelaunch {
    pub fn new(program: impl Into<PathBuf>, args: impl IntoIterator<Item = OsString>) -> Self {
        Self {
            program: program.into(),
            args: args.into_iter().collect(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct PlatformApplyOutcome {
    pub result_details: Option<Value>,
    pub relaunch: Option<PlatformRelaunch>,
}

pub fn relaunch(outcome: &PlatformApplyOutcome) -> Result<()> {
    let relaunch = outcome
        .relaunch
        .as_ref()
        .ok_or_else(|| anyhow!("desktop update platform did not provide a relaunch command"))?;
    Command::new(relaunch.program.as_path())
        .args(relaunch.args.iter())
        .spawn()
        .with_context(|| {
            format!(
                "failed to launch updated desktop application `{}`",
                relaunch.program.display()
            )
        })?;
    Ok(())
}

pub fn apply_validated_plan(
    plan: &DesktopUpdatePlan,
    plan_path: &Path,
) -> Result<PlatformApplyOutcome> {
    #[cfg(target_os = "macos")]
    {
        return macos::apply(plan, plan_path);
    }

    #[cfg(target_os = "linux")]
    {
        return linux::apply(plan, plan_path);
    }

    #[cfg(windows)]
    {
        return windows::apply(plan, plan_path);
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux", windows)))]
    {
        let _ = (plan, plan_path);
        anyhow::bail!("desktop auto-update apply is unsupported on this platform")
    }
}
