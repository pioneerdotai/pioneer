use super::backend::{ComputerUseDesktopBackend, Xa11yComputerUseBackend};
use super::model::ComputerUseArgs;
use super::state::{ComputerUseSessionManager, argument_contract_error, parse_computer_use_args};
use crate::ComputerUseToolsConfig;
use crate::context::{ToolInvocation, ToolOutput};
use crate::error::ToolError;
use crate::events::ToolEventTrace;
use crate::registry::ToolHandler;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct ComputerUseHandler {
    pub(super) config: ComputerUseToolsConfig,
    pub(super) backend: Arc<dyn ComputerUseDesktopBackend>,
    pub(super) manager: Arc<Mutex<ComputerUseSessionManager>>,
}

impl ComputerUseHandler {
    pub fn new(config: ComputerUseToolsConfig) -> Self {
        let config = config.normalized();
        let manager = Arc::new(Mutex::new(ComputerUseSessionManager::from_artifacts_root(
            config
                .runtime_home_dir
                .join(config.artifacts_subdir.as_str())
                .as_path(),
        )));
        Self {
            config,
            backend: Arc::new(Xa11yComputerUseBackend),
            manager,
        }
    }
}

#[async_trait]
impl ToolHandler for ComputerUseHandler {
    async fn handle(
        &self,
        invocation: ToolInvocation,
        trace: ToolEventTrace,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let args = parse_computer_use_args(invocation.payload)?;
        let action = args.action.trim().to_ascii_lowercase();
        validate_computer_use_action_contract(&args, action.as_str())?;

        let output = match action.as_str() {
            "preflight" => self.handle_preflight(invocation.attempt_id, &trace).await?,
            "list_displays" => {
                self.handle_list_displays(invocation.attempt_id, &trace)
                    .await?
            }
            "list_apps" => self.handle_list_apps(invocation.attempt_id, &trace).await?,
            "start" => {
                self.handle_start(args, invocation.attempt_id, &trace)
                    .await?
            }
            "snapshot" => {
                self.handle_snapshot(args, invocation.attempt_id, &trace)
                    .await?
            }
            "act" => self.handle_act(args, invocation.attempt_id, &trace).await?,
            "verify" => {
                self.handle_verify(args, invocation.attempt_id, &trace)
                    .await?
            }
            "status" => self.handle_status(args).await?,
            "stop" => {
                self.handle_stop(args, invocation.attempt_id, &trace)
                    .await?
            }
            _ => {
                return Err(argument_contract_error(
                    "$.action",
                    format!(
                        "unsupported action `{}`; supported: preflight, list_apps, list_displays, start, snapshot, act, verify, status, stop",
                        action
                    ),
                ));
            }
        };

        Ok(Box::new(output))
    }
}

fn validate_computer_use_action_contract(
    args: &ComputerUseArgs,
    action: &str,
) -> Result<(), ToolError> {
    if action.is_empty() {
        return Err(argument_contract_error(
            "$.action",
            "action must be non-empty",
        ));
    }

    match action {
        "start" => {
            require_non_empty(
                args.goal.as_deref(),
                "$.goal",
                "start requires non-empty goal",
            )?;
            reject_if_present(action, "$.session_id", args.session_id.as_ref())?;
            reject_if_present(action, "$.screenshot_path", args.screenshot_path.as_ref())?;
            reject_if_present(action, "$.act", args.act.as_ref())?;
            reject_if_present(action, "$.recovery_attempt", args.recovery_attempt.as_ref())?;
            reject_if_present(action, "$.failure_class", args.failure_class.as_ref())?;
            reject_if_present(action, "$.outcome", args.outcome.as_ref())?;
            reject_if_present(action, "$.reason", args.reason.as_ref())?;
            reject_if_present(action, "$.expect", args.expect.as_ref())?;
        }
        "snapshot" => {
            require_present(
                args.session_id.as_ref(),
                "$.session_id",
                "snapshot requires session_id",
            )?;
            reject_common_session_action_extras(args, action, &["$.screenshot_path"])?;
        }
        "act" => {
            require_present(
                args.session_id.as_ref(),
                "$.session_id",
                "act requires session_id",
            )?;
            require_present(args.act.as_ref(), "$.act", "act requires act object")?;
            reject_if_present(action, "$.goal", args.goal.as_ref())?;
            reject_if_present(action, "$.target", args.target.as_ref())?;
            reject_if_present(action, "$.display_id", args.display_id.as_ref())?;
            reject_if_present(
                action,
                "$.launch_if_missing",
                args.launch_if_missing.as_ref(),
            )?;
            reject_if_present(action, "$.launch_command", args.launch_command.as_ref())?;
            reject_if_present(
                action,
                "$.activation_timeout_ms",
                args.activation_timeout_ms.as_ref(),
            )?;
            reject_if_present(action, "$.tree_max_depth", args.tree_max_depth.as_ref())?;
            reject_if_present(action, "$.screenshot_path", args.screenshot_path.as_ref())?;
            reject_if_present(action, "$.max_steps", args.max_steps.as_ref())?;
            reject_if_present(action, "$.timeout_ms", args.timeout_ms.as_ref())?;
            reject_if_present(action, "$.planner_provider", args.planner_provider.as_ref())?;
            reject_if_present(action, "$.planner_model", args.planner_model.as_ref())?;
            reject_if_present(
                action,
                "$.snapshot_max_bytes",
                args.snapshot_max_bytes.as_ref(),
            )?;
            reject_if_present(
                action,
                "$.snapshot_max_side_px",
                args.snapshot_max_side_px.as_ref(),
            )?;
            reject_if_present(action, "$.outcome", args.outcome.as_ref())?;
            reject_if_present(action, "$.reason", args.reason.as_ref())?;
            reject_if_present(action, "$.expect", args.expect.as_ref())?;
        }
        "verify" => {
            require_present(
                args.session_id.as_ref(),
                "$.session_id",
                "verify requires session_id",
            )?;
            require_present(
                args.expect.as_ref(),
                "$.expect",
                "verify requires expect object",
            )?;
            reject_common_session_action_extras(args, action, &["$.expect"])?;
        }
        "status" => {
            require_present(
                args.session_id.as_ref(),
                "$.session_id",
                "status requires session_id",
            )?;
            reject_common_session_action_extras(args, action, &[])?;
        }
        "stop" => {
            require_present(
                args.session_id.as_ref(),
                "$.session_id",
                "stop requires session_id",
            )?;
            reject_if_present(action, "$.goal", args.goal.as_ref())?;
            reject_if_present(action, "$.target", args.target.as_ref())?;
            reject_if_present(action, "$.display_id", args.display_id.as_ref())?;
            reject_if_present(
                action,
                "$.launch_if_missing",
                args.launch_if_missing.as_ref(),
            )?;
            reject_if_present(action, "$.launch_command", args.launch_command.as_ref())?;
            reject_if_present(
                action,
                "$.activation_timeout_ms",
                args.activation_timeout_ms.as_ref(),
            )?;
            reject_if_present(action, "$.tree_max_depth", args.tree_max_depth.as_ref())?;
            reject_if_present(action, "$.screenshot_path", args.screenshot_path.as_ref())?;
            reject_if_present(action, "$.act", args.act.as_ref())?;
            reject_if_present(action, "$.max_steps", args.max_steps.as_ref())?;
            reject_if_present(action, "$.timeout_ms", args.timeout_ms.as_ref())?;
            reject_if_present(action, "$.planner_provider", args.planner_provider.as_ref())?;
            reject_if_present(action, "$.planner_model", args.planner_model.as_ref())?;
            reject_if_present(
                action,
                "$.snapshot_max_bytes",
                args.snapshot_max_bytes.as_ref(),
            )?;
            reject_if_present(
                action,
                "$.snapshot_max_side_px",
                args.snapshot_max_side_px.as_ref(),
            )?;
            reject_if_present(action, "$.recovery_attempt", args.recovery_attempt.as_ref())?;
            reject_if_present(action, "$.expect", args.expect.as_ref())?;
        }
        "preflight" | "list_apps" | "list_displays" => {
            reject_if_present(action, "$.session_id", args.session_id.as_ref())?;
            reject_if_present(action, "$.goal", args.goal.as_ref())?;
            reject_if_present(action, "$.target", args.target.as_ref())?;
            reject_if_present(action, "$.display_id", args.display_id.as_ref())?;
            reject_if_present(
                action,
                "$.launch_if_missing",
                args.launch_if_missing.as_ref(),
            )?;
            reject_if_present(action, "$.launch_command", args.launch_command.as_ref())?;
            reject_if_present(
                action,
                "$.activation_timeout_ms",
                args.activation_timeout_ms.as_ref(),
            )?;
            reject_if_present(action, "$.tree_max_depth", args.tree_max_depth.as_ref())?;
            reject_if_present(action, "$.screenshot_path", args.screenshot_path.as_ref())?;
            reject_if_present(action, "$.act", args.act.as_ref())?;
            reject_if_present(action, "$.max_steps", args.max_steps.as_ref())?;
            reject_if_present(action, "$.timeout_ms", args.timeout_ms.as_ref())?;
            reject_if_present(action, "$.planner_provider", args.planner_provider.as_ref())?;
            reject_if_present(action, "$.planner_model", args.planner_model.as_ref())?;
            reject_if_present(
                action,
                "$.snapshot_max_bytes",
                args.snapshot_max_bytes.as_ref(),
            )?;
            reject_if_present(
                action,
                "$.snapshot_max_side_px",
                args.snapshot_max_side_px.as_ref(),
            )?;
            reject_if_present(action, "$.recovery_attempt", args.recovery_attempt.as_ref())?;
            reject_if_present(action, "$.failure_class", args.failure_class.as_ref())?;
            reject_if_present(action, "$.outcome", args.outcome.as_ref())?;
            reject_if_present(action, "$.reason", args.reason.as_ref())?;
            reject_if_present(action, "$.expect", args.expect.as_ref())?;
        }
        _ => {}
    }
    Ok(())
}

fn reject_common_session_action_extras(
    args: &ComputerUseArgs,
    action: &str,
    allowed_extras: &[&str],
) -> Result<(), ToolError> {
    reject_if_present(action, "$.goal", args.goal.as_ref())?;
    reject_if_present(action, "$.target", args.target.as_ref())?;
    reject_if_present(action, "$.display_id", args.display_id.as_ref())?;
    reject_if_present(
        action,
        "$.launch_if_missing",
        args.launch_if_missing.as_ref(),
    )?;
    reject_if_present(action, "$.launch_command", args.launch_command.as_ref())?;
    reject_if_present(
        action,
        "$.activation_timeout_ms",
        args.activation_timeout_ms.as_ref(),
    )?;
    reject_if_present(action, "$.tree_max_depth", args.tree_max_depth.as_ref())?;
    if !allowed_extras.contains(&"$.screenshot_path") {
        reject_if_present(action, "$.screenshot_path", args.screenshot_path.as_ref())?;
    }
    reject_if_present(action, "$.act", args.act.as_ref())?;
    reject_if_present(action, "$.max_steps", args.max_steps.as_ref())?;
    reject_if_present(action, "$.timeout_ms", args.timeout_ms.as_ref())?;
    reject_if_present(action, "$.planner_provider", args.planner_provider.as_ref())?;
    reject_if_present(action, "$.planner_model", args.planner_model.as_ref())?;
    reject_if_present(
        action,
        "$.snapshot_max_bytes",
        args.snapshot_max_bytes.as_ref(),
    )?;
    reject_if_present(
        action,
        "$.snapshot_max_side_px",
        args.snapshot_max_side_px.as_ref(),
    )?;
    reject_if_present(action, "$.recovery_attempt", args.recovery_attempt.as_ref())?;
    reject_if_present(action, "$.failure_class", args.failure_class.as_ref())?;
    reject_if_present(action, "$.outcome", args.outcome.as_ref())?;
    reject_if_present(action, "$.reason", args.reason.as_ref())?;
    if !allowed_extras.contains(&"$.expect") {
        reject_if_present(action, "$.expect", args.expect.as_ref())?;
    }
    Ok(())
}

fn require_non_empty(value: Option<&str>, path: &str, message: &str) -> Result<(), ToolError> {
    if value.is_some_and(|value| !value.trim().is_empty()) {
        return Ok(());
    }
    Err(argument_contract_error(path, message))
}

fn require_present<T>(value: Option<&T>, path: &str, message: &str) -> Result<(), ToolError> {
    if value.is_some() {
        return Ok(());
    }
    Err(argument_contract_error(path, message))
}

fn reject_if_present<T>(action: &str, path: &str, value: Option<&T>) -> Result<(), ToolError> {
    if value.is_none() {
        return Ok(());
    }
    Err(argument_contract_error(
        path,
        format!("field is not accepted for action `{action}`"),
    ))
}

#[cfg(test)]
impl ComputerUseHandler {
    pub(super) fn with_backend(
        config: ComputerUseToolsConfig,
        backend: Arc<dyn ComputerUseDesktopBackend>,
    ) -> Self {
        let config = config.normalized();
        let manager = Arc::new(Mutex::new(ComputerUseSessionManager::from_artifacts_root(
            config
                .runtime_home_dir
                .join(config.artifacts_subdir.as_str())
                .as_path(),
        )));
        Self {
            config,
            backend,
            manager,
        }
    }
}
