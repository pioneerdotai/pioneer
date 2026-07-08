use crate::plan::DesktopUpdatePlan;
use anyhow::Result;
use serde_json::Value;
use std::path::Path;

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(windows)]
mod windows;

#[derive(Debug, Clone, Default)]
pub struct PlatformApplyOutcome {
    pub result_details: Option<Value>,
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
