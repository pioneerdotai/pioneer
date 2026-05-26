mod screenshots;
mod xa11y;

#[cfg(test)]
mod mock;

use super::model::{
    ActionExecution, AppHandle, AppMeta, AppTarget, CapturedFrame, DesktopPreflightOptions,
    DesktopPreflightReport, DesktopTree, InputAction, OsAction, ResolvedActionTarget,
    ResolvedInputActionTargets, SemanticAction, SnapshotBudget, SnapshotTarget,
};
use super::tree::AccessibilityTreeBudget;
use crate::error::ToolError;
use std::time::Duration;

pub(crate) use xa11y::Xa11yComputerUseBackend;

#[cfg(test)]
pub(crate) use mock::MockComputerUseBackend;

#[allow(dead_code)]
pub(crate) trait ComputerUseDesktopBackend: Send + Sync {
    fn preflight(
        &self,
        options: DesktopPreflightOptions,
    ) -> Result<DesktopPreflightReport, ToolError>;
    fn list_apps(&self) -> Result<Vec<AppMeta>, ToolError>;
    fn frontmost_app(&self) -> Result<Option<AppMeta>, ToolError>;
    fn find_app(&self, target: &AppTarget, timeout: Duration) -> Result<AppHandle, ToolError>;
    fn launch_app(&self, target: &AppTarget, launch_command: Option<&str>)
    -> Result<(), ToolError>;
    fn activate_app(&self, app: &AppHandle) -> Result<(), ToolError>;
    fn app_tree(
        &self,
        app: &AppHandle,
        budget: AccessibilityTreeBudget,
    ) -> Result<DesktopTree, ToolError>;
    fn screenshot(
        &self,
        target: &SnapshotTarget,
        snapshot_budget: &SnapshotBudget,
    ) -> Result<CapturedFrame, ToolError>;
    fn perform_semantic_action(
        &self,
        app: &AppHandle,
        action: &SemanticAction,
        target: Option<&ResolvedActionTarget>,
    ) -> Result<ActionExecution, ToolError>;
    fn perform_input_action(
        &self,
        action: &InputAction,
        targets: &ResolvedInputActionTargets,
    ) -> Result<ActionExecution, ToolError>;
    fn perform_os_action(&self, action: &OsAction) -> Result<ActionExecution, ToolError>;
    fn list_displays(&self) -> Result<Vec<super::model::DisplayMeta>, ToolError>;
    fn capture_display(
        &self,
        display_id: u32,
        snapshot_budget: &SnapshotBudget,
    ) -> Result<CapturedFrame, ToolError>;
}
