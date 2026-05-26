use super::super::handler::ComputerUseHandler;
use super::super::model::*;
use super::super::permissions::computer_use_preflight_payload;
use super::super::platform;
use super::super::state::cleanup_artifacts_sync;
use super::super::util::{
    default_loop_guard, now_unix_ms, resolve_snapshot_budget, stop_session_as_completed,
    stop_session_as_stopped, stop_session_with_reason,
};
use crate::context::FunctionToolOutput;
use crate::error::ToolError;
use crate::events::ToolEventTrace;
use std::path::PathBuf;
use std::time::{Duration, Instant};

impl ComputerUseHandler {
    pub(crate) async fn handle_preflight(
        &self,
        attempt_id: u32,
        trace: &ToolEventTrace,
    ) -> Result<FunctionToolOutput, ToolError> {
        let report = self.backend.preflight(DesktopPreflightOptions {
            screenshot_probe_enabled: self.config.preflight_screenshot_probe_enabled,
            input_simulation_enabled: self.config.input_simulation_enabled,
        })?;
        let status = report.status.clone();
        let payload = computer_use_preflight_payload(report);
        trace.emit_stage(
            attempt_id,
            "computer_use.preflight",
            None,
            Some(serde_json::json!({
                "mode": "remote",
                "status": status,
                "capabilities": payload.get("capabilities").cloned().unwrap_or(serde_json::Value::Null),
                "blocking_issues": payload.get("blocking_issues").and_then(serde_json::Value::as_array).map_or(0, Vec::len),
                "warnings": payload.get("warnings").and_then(serde_json::Value::as_array).map_or(0, Vec::len),
            })),
        );
        Ok(FunctionToolOutput::with_payload(
            format!("computer_use preflight status: {status}"),
            true,
            payload,
        ))
    }

    pub(crate) fn artifacts_root(&self) -> PathBuf {
        self.config
            .runtime_home_dir
            .join(self.config.artifacts_subdir.as_str())
    }

    pub(crate) async fn cleanup_artifacts(&self) -> Result<(), ToolError> {
        let root = self.artifacts_root();
        let retention_hours = self.config.retention_hours;
        let max_total_bytes = self.config.max_total_bytes;

        tokio::task::spawn_blocking(move || {
            cleanup_artifacts_sync(root.as_path(), retention_hours, max_total_bytes)
        })
        .await
        .map_err(|error| ToolError::internal(format!("cleanup task join error: {error}")))?
    }

    pub(crate) fn choose_display(
        displays: Vec<DisplayMeta>,
        requested_id: Option<u32>,
    ) -> Result<DisplayMeta, ToolError> {
        if displays.is_empty() {
            return Err(ToolError::execution_failed(
                "no displays available for computer_use",
            ));
        }

        if let Some(display_id) = requested_id {
            return displays
                .into_iter()
                .find(|display| display.display_id == display_id)
                .ok_or_else(|| ToolError::NotFound(format!("display {} not found", display_id)));
        }

        if let Some(primary) = displays.iter().find(|display| display.is_primary) {
            return Ok(primary.clone());
        }

        displays
            .into_iter()
            .next()
            .ok_or_else(|| ToolError::execution_failed("no displays available for computer_use"))
    }

    pub(crate) async fn handle_list_displays(
        &self,
        attempt_id: u32,
        trace: &ToolEventTrace,
    ) -> Result<FunctionToolOutput, ToolError> {
        let displays = self.backend.list_displays()?;
        trace.emit_stage(
            attempt_id,
            "computer_use.list_displays",
            None,
            Some(serde_json::json!({"count": displays.len(), "mode": "remote"})),
        );
        Ok(FunctionToolOutput::with_payload(
            format!("{} display(s) available", displays.len()),
            true,
            serde_json::json!({
                "action": "list_displays",
                "mode": "remote",
                "displays": displays,
            }),
        ))
    }

    pub(crate) async fn handle_list_apps(
        &self,
        attempt_id: u32,
        trace: &ToolEventTrace,
    ) -> Result<FunctionToolOutput, ToolError> {
        let apps = self.backend.list_apps()?;
        trace.emit_stage(
            attempt_id,
            "computer_use.list_apps",
            None,
            Some(serde_json::json!({"count": apps.len(), "mode": "remote"})),
        );
        Ok(FunctionToolOutput::with_payload(
            format!("{} app(s) available", apps.len()),
            true,
            serde_json::json!({
                "action": "list_apps",
                "mode": "remote",
                "apps": apps,
            }),
        ))
    }

    fn resolve_start_target(&self, args: &ComputerUseArgs) -> Result<ComputerUseTarget, ToolError> {
        let display = Self::choose_display(
            self.backend.list_displays()?,
            args.target
                .as_ref()
                .and_then(|target| target.display_id)
                .or(args.display_id),
        )?;
        let Some(target) = args.target.as_ref() else {
            return Err(ToolError::invalid_arguments(
                "computer_use start requires explicit target. Use target.type=app_name with target.name for app tasks, target.type=active_app for the current frontmost app when supported, or target.type=screen only for whole-desktop tasks.",
            ));
        };

        let activation_timeout = Duration::from_millis(
            target
                .activation_timeout_ms
                .or(args.activation_timeout_ms)
                .unwrap_or(self.config.app_activation_timeout_ms)
                .clamp(0, 120_000),
        );
        let tree_max_depth = target
            .tree_max_depth
            .or(args.tree_max_depth)
            .unwrap_or(self.config.accessibility_tree_max_depth)
            .clamp(1, 50);
        let launch_if_missing = target
            .launch_if_missing
            .or(args.launch_if_missing)
            .unwrap_or(self.config.launch_if_missing_default);
        let launch_command = target
            .launch_command
            .as_deref()
            .or(args.launch_command.as_deref());

        match target.kind.trim().to_ascii_lowercase().as_str() {
            "screen" => Ok(ComputerUseTarget::Screen { display }),
            "app" | "app_name" => {
                if target.kind.trim().eq_ignore_ascii_case("app_name") && target.name.is_none() {
                    return Err(ToolError::invalid_arguments(
                        "computer_use target app_name requires target.name",
                    ));
                }
                let requested = app_target_from_args(target);
                ensure_app_target_has_identity(&requested)?;
                let app = self.find_app_with_optional_launch(
                    &requested,
                    activation_timeout,
                    launch_if_missing,
                    launch_command,
                )?;
                Ok(ComputerUseTarget::App {
                    requested,
                    app: app.into(),
                    display,
                    tree_max_depth,
                })
            }
            "identity_key" => {
                let identity_key = target.identity_key.clone().ok_or_else(|| {
                    ToolError::invalid_arguments(
                        "computer_use target identity_key requires target.identity_key",
                    )
                })?;
                let mut requested = app_target_from_args(target);
                requested.identity_key = Some(identity_key);
                let app = self.find_app_with_optional_launch(
                    &requested,
                    activation_timeout,
                    launch_if_missing,
                    launch_command,
                )?;
                Ok(ComputerUseTarget::App {
                    requested,
                    app: app.into(),
                    display,
                    tree_max_depth,
                })
            }
            "bundle_id" => {
                let bundle_id = target.bundle_id.clone().ok_or_else(|| {
                    ToolError::invalid_arguments(
                        "computer_use target bundle_id requires target.bundle_id",
                    )
                })?;
                let mut requested = app_target_from_args(target);
                requested.bundle_id = Some(bundle_id);
                let app = self.find_app_with_optional_launch(
                    &requested,
                    activation_timeout,
                    launch_if_missing,
                    launch_command,
                )?;
                Ok(ComputerUseTarget::App {
                    requested,
                    app: app.into(),
                    display,
                    tree_max_depth,
                })
            }
            "executable_path" => {
                let executable_path = target.executable_path.clone().ok_or_else(|| {
                    ToolError::invalid_arguments(
                        "computer_use target executable_path requires target.executable_path",
                    )
                })?;
                let mut requested = app_target_from_args(target);
                requested.executable_path = Some(executable_path);
                let app = self.find_app_with_optional_launch(
                    &requested,
                    activation_timeout,
                    launch_if_missing,
                    launch_command,
                )?;
                Ok(ComputerUseTarget::App {
                    requested,
                    app: app.into(),
                    display,
                    tree_max_depth,
                })
            }
            "pid" => {
                let pid = target.pid.ok_or_else(|| {
                    ToolError::invalid_arguments("computer_use target pid requires target.pid")
                })?;
                let mut requested = app_target_from_args(target);
                requested.pid = Some(pid);
                let app = self.find_app_with_optional_launch(
                    &requested,
                    activation_timeout,
                    launch_if_missing,
                    launch_command,
                )?;
                Ok(ComputerUseTarget::App {
                    requested,
                    app: app.into(),
                    display,
                    tree_max_depth,
                })
            }
            "active_app" => {
                let app = self.backend.frontmost_app()?.ok_or_else(|| {
                    ToolError::execution_failed(
                        "computer_use active_app target is unsupported because the backend did not report a frontmost app; pass target.type=app_name or target.type=screen explicitly",
                    )
                })?;
                Ok(ComputerUseTarget::ActiveApp {
                    app: app.into(),
                    display,
                    tree_max_depth,
                })
            }
            other => Err(ToolError::invalid_arguments(format!(
                "unsupported computer_use target type `{other}`; supported: screen, active_app, app_name, app, pid, identity_key, bundle_id, executable_path"
            ))),
        }
    }

    fn find_app_with_optional_launch(
        &self,
        target: &AppTarget,
        timeout: Duration,
        launch_if_missing: bool,
        launch_command: Option<&str>,
    ) -> Result<AppHandle, ToolError> {
        self.ensure_launch_command_allowed(launch_command)?;
        let target = platform::enrich_launch_target(target);
        match self.find_exact_app(&target, timeout) {
            Ok(app) => {
                self.backend.activate_app(&app)?;
                Ok(app)
            }
            Err(first_error) if launch_if_missing => {
                self.backend.launch_app(&target, launch_command)?;
                let app = self.find_exact_app(&target, timeout).map_err(|second_error| {
                    ToolError::NotFound(format!(
                        "computer_use app target not found after launch; failure_class=app_not_found; initial={}; retry={}. Verify the explicit target fields, desktop automation permissions, and screenshot permissions.",
                        compact_error_text(first_error.to_string().as_str(), 700),
                        compact_error_text(second_error.to_string().as_str(), 700)
                    ))
                })?;
                self.backend.activate_app(&app)?;
                Ok(app)
            }
            Err(error) => Err(ToolError::NotFound(format!(
                "computer_use app target not found; failure_class=app_not_found; {}. If launch is required, pass launch_if_missing=true with explicit app identity fields or launch_command for non-bundle executables.",
                compact_error_text(error.to_string().as_str(), 900)
            ))),
        }
    }

    fn find_exact_app(
        &self,
        target: &AppTarget,
        timeout: Duration,
    ) -> Result<AppHandle, ToolError> {
        ensure_app_target_has_identity(target)?;
        let apps = self.backend.list_apps().unwrap_or_default();
        let matches = apps
            .iter()
            .filter(|app| app_meta_matches_target(app, target))
            .cloned()
            .collect::<Vec<_>>();
        if matches.len() > 1 {
            return Err(ToolError::NotFound(app_resolution_diagnostic(
                "app target is ambiguous",
                target,
                apps.as_slice(),
            )));
        }
        if let Some(app) = matches.first() {
            let resolved_target = app_target_from_meta(app);
            return self
                .backend
                .find_app(&resolved_target, timeout)
                .map_err(|error| {
                    ToolError::NotFound(app_resolution_diagnostic(
                        format!("matched app could not be opened by backend: {error}").as_str(),
                        target,
                        apps.as_slice(),
                    ))
                });
        }
        if target.pid.is_some() || target.name.is_some() {
            match self.backend.find_app(target, timeout) {
                Ok(app) if app_handle_matches_target(&app, target) => return Ok(app),
                Ok(_) => {
                    return Err(ToolError::NotFound(app_resolution_diagnostic(
                        "backend returned an app that did not match explicit target fields",
                        target,
                        apps.as_slice(),
                    )));
                }
                Err(_) => {}
            }
        }
        Err(ToolError::NotFound(format!(
            "no app identity matched; {}",
            app_resolution_diagnostic(
                "target did not match current app inventory",
                target,
                apps.as_slice()
            )
        )))
    }

    fn ensure_launch_command_allowed(&self, launch_command: Option<&str>) -> Result<(), ToolError> {
        let Some(command) = launch_command
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            return Ok(());
        };
        if self.config.allowed_launch_commands.is_empty()
            || self
                .config
                .allowed_launch_commands
                .iter()
                .any(|allowed| command == allowed || command.starts_with(&format!("{allowed} ")))
        {
            return Ok(());
        }
        Err(ToolError::invalid_arguments(
            "computer_use launch_command is not allowed by gateway.tools.computer_use.allowed_launch_commands",
        ))
    }

    pub(crate) async fn handle_start(
        &self,
        args: ComputerUseArgs,
        attempt_id: u32,
        trace: &ToolEventTrace,
    ) -> Result<FunctionToolOutput, ToolError> {
        self.cleanup_artifacts().await?;

        let target = self.resolve_start_target(&args)?;
        let display = target.display().clone();
        let session_id = {
            let mut manager = self.manager.lock().await;
            manager.next_session_id = manager.next_session_id.saturating_add(1);
            manager.next_session_id
        };

        let artifacts_dir = self.artifacts_root().join(session_id.to_string());
        let snapshots_dir = artifacts_dir.join("snapshots");
        tokio::fs::create_dir_all(snapshots_dir.as_path())
            .await
            .map_err(|error| {
                ToolError::execution_failed(format!(
                    "failed to create computer_use artifacts directory `{}`: {error}",
                    snapshots_dir.display()
                ))
            })?;

        let now = now_unix_ms();
        let goal = args
            .goal
            .unwrap_or_else(|| "Perform the requested UI task".to_owned());
        let snapshot_budget = resolve_snapshot_budget(
            &self.config,
            args.planner_provider.as_deref(),
            args.planner_model.as_deref(),
            args.snapshot_max_bytes,
            args.snapshot_max_side_px,
        );
        let session = ComputerUseSession {
            session_id,
            goal: goal.clone(),
            status: ComputerUseStatus::Running,
            loop_state: ComputerUseLoopState::Started,
            updated_at_unix_ms: now,
            created_at_mono: Instant::now(),
            timeout_ms: args
                .timeout_ms
                .unwrap_or(DEFAULT_TIMEOUT_MS)
                .clamp(1_000, 3_600_000),
            max_steps: args
                .max_steps
                .unwrap_or(self.config.run_max_steps_default)
                .clamp(1, 200),
            step_count: 0,
            snapshot_count: 0,
            target: target.clone(),
            last_snapshot: None,
            last_accessibility_tree: None,
            last_node_refs: Vec::new(),
            last_action: None,
            last_verification: None,
            previous_verification_status: None,
            last_completion_evidence: None,
            last_evidence_at_step: None,
            last_progress_signals: None,
            stop_reason: None,
            stop_failure_class: None,
            last_action_signature: None,
            awaiting_post_action_snapshot: false,
            loop_guard: default_loop_guard(&self.config),
            recovery_attempts_current_step: 0,
            recovery_attempts_run: 0,
            max_recovery_attempts_per_step: self.config.max_recovery_attempts_per_step,
            max_recovery_attempts_per_run: self.config.max_recovery_attempts_per_run,
            snapshot_budget,
            artifacts_dir: artifacts_dir.display().to_string(),
        };
        {
            let mut manager = self.manager.lock().await;
            manager.sessions.insert(session_id, session);
        }

        trace.emit_stage(
            attempt_id,
            "computer_use.start",
            None,
            Some(serde_json::json!({
                "session_id": session_id,
                "target": target,
                "display_id": display.display_id,
                "mode": "remote",
            })),
        );
        trace.emit_stage(
            attempt_id,
            "computer_use.loop.started",
            None,
            Some(serde_json::json!({
                "session_id": session_id,
                "mode": "remote",
                "state": ComputerUseLoopState::Started.as_str(),
            })),
        );

        let session_snapshot = {
            let manager = self.manager.lock().await;
            manager.sessions.get(&session_id).cloned().ok_or_else(|| {
                ToolError::internal("failed to fetch session snapshot after start")
            })?
        };

        Ok(FunctionToolOutput::with_payload(
            format!(
                "Started remote computer_use session {} with target {}",
                session_id,
                serde_json::to_value(&session_snapshot.target)
                    .ok()
                    .and_then(|value| value.get("type").cloned())
                    .and_then(|value| value.as_str().map(str::to_owned))
                    .unwrap_or_else(|| "unknown".to_owned())
            ),
            true,
            serde_json::json!({
                "action": "start",
                "mode": "remote",
                "session_id": session_id,
                "status": "running",
                "loop_state": ComputerUseLoopState::Started.as_str(),
                "goal": goal,
                "target": session_snapshot.target,
                "display": display,
                "artifacts_dir": artifacts_dir,
                "snapshot_budget": session_snapshot.snapshot_budget,
                "loop_guard": session_snapshot.loop_guard,
                "recovery_limits": {
                    "max_recovery_attempts_per_step": session_snapshot.max_recovery_attempts_per_step,
                    "max_recovery_attempts_per_run": session_snapshot.max_recovery_attempts_per_run
                },
                "cycle": ["snapshot", "act", "snapshot"],
                "next_call": {
                    "tool": "computer_use",
                    "arguments": {
                        "action": "snapshot",
                        "session_id": session_id
                    }
                }
            }),
        ))
    }
    pub(crate) async fn handle_status(
        &self,
        args: ComputerUseArgs,
    ) -> Result<FunctionToolOutput, ToolError> {
        let session_id = args.session_id.ok_or_else(|| {
            ToolError::invalid_arguments("computer_use status requires session_id")
        })?;

        let payload = {
            let mut manager = self.manager.lock().await;
            let session = manager.sessions.get_mut(&session_id).ok_or_else(|| {
                ToolError::NotFound(format!("computer_use session {} not found", session_id))
            })?;

            if session.status == ComputerUseStatus::Running
                && session.created_at_mono.elapsed().as_millis() as u64 > session.timeout_ms
            {
                stop_session_with_reason(
                    session,
                    "timeout",
                    ComputerUseFailureClass::RuntimeActionError,
                );
            }

            serde_json::json!({
                "action": "status",
                "mode": "remote",
                "session_id": session.session_id,
                "status": session.status.as_str(),
                "loop_state": session.loop_state.as_str(),
                "goal": session.goal,
                "step_count": session.step_count,
                "snapshot_count": session.snapshot_count,
                "target": session.target,
                "display": session.target.display(),
                "last_snapshot": session.last_snapshot,
                "last_accessibility_tree": session.last_accessibility_tree,
                "last_node_ref_count": session.last_node_refs.len(),
                "last_action": session.last_action,
                "last_verification": session.last_verification,
                "previous_verification_status": session.previous_verification_status,
                "last_completion_evidence": session.last_completion_evidence,
                "last_evidence_at_step": session.last_evidence_at_step,
                "last_progress_signals": session.last_progress_signals,
                "stop_reason": session.stop_reason,
                "failure_class": session.stop_failure_class.map(|value| value.as_str().to_owned()),
                "loop_guard": session.loop_guard,
                "snapshot_budget": session.snapshot_budget,
                "recovery": {
                    "attempts_current_step": session.recovery_attempts_current_step,
                    "attempts_run": session.recovery_attempts_run,
                    "max_attempts_per_step": session.max_recovery_attempts_per_step,
                    "max_attempts_per_run": session.max_recovery_attempts_per_run,
                },
                "artifacts_dir": session.artifacts_dir,
                "timeout_ms": session.timeout_ms,
                "max_steps": session.max_steps,
            })
        };

        Ok(FunctionToolOutput::with_payload(
            format!("Status returned for session {}", session_id),
            true,
            payload,
        ))
    }

    pub(crate) async fn handle_stop(
        &self,
        args: ComputerUseArgs,
        attempt_id: u32,
        trace: &ToolEventTrace,
    ) -> Result<FunctionToolOutput, ToolError> {
        let session_id = args
            .session_id
            .ok_or_else(|| ToolError::invalid_arguments("computer_use stop requires session_id"))?;
        let outcome = args
            .outcome
            .unwrap_or_else(|| "stopped".to_owned())
            .trim()
            .to_ascii_lowercase();
        let reason = args
            .reason
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| format!("{outcome}_by_request"));
        let requested_failure_class = if let Some(raw) = args.failure_class.as_deref() {
            Some(ComputerUseFailureClass::parse(raw).ok_or_else(|| {
                ToolError::invalid_arguments(format!(
                    "invalid computer_use failure_class `{}`",
                    raw
                ))
            })?)
        } else {
            None
        };

        let payload = {
            let mut manager = self.manager.lock().await;
            let session = manager.sessions.get_mut(&session_id).ok_or_else(|| {
                ToolError::NotFound(format!("computer_use session {} not found", session_id))
            })?;
            match outcome.as_str() {
                "completed" => {
                    if !has_fresh_completion_evidence(session) {
                        return Err(ToolError::execution_failed(format!(
                            "completion_evidence_required: stop outcome=completed requires fresh positive completion evidence from computer_use verify or a post-action snapshot. session_id={}; step_count={}; last_evidence_at_step={:?}; next_call={{\"tool\":\"computer_use\",\"arguments\":{{\"action\":\"verify\",\"session_id\":{},\"expect\":{{\"visible_text\":\"<goal-specific visible text>\"}}}},\"reason\":\"completion_evidence_required\"}}",
                            session.session_id,
                            session.step_count,
                            session.last_evidence_at_step,
                            session.session_id
                        )));
                    }
                    stop_session_as_completed(session, reason.clone());
                }
                "failed" => {
                    stop_session_with_reason(
                        session,
                        reason.clone(),
                        requested_failure_class
                            .unwrap_or(ComputerUseFailureClass::RuntimeActionError),
                    );
                }
                "stopped" => {
                    stop_session_as_stopped(session, reason.clone());
                }
                _ => {
                    return Err(ToolError::invalid_arguments(format!(
                        "unsupported stop outcome `{}`; supported: stopped, completed, failed",
                        outcome
                    )));
                }
            }

            serde_json::json!({
                "action": "stop",
                "mode": "remote",
                "session_id": session.session_id,
                "status": session.status.as_str(),
                "loop_state": session.loop_state.as_str(),
                "stop_reason": session.stop_reason,
                "failure_class": session.stop_failure_class.map(|value| value.as_str().to_owned()),
                "completion_evidence": session.last_completion_evidence,
            })
        };

        let session_snapshot = {
            let manager = self.manager.lock().await;
            manager
                .sessions
                .get(&session_id)
                .cloned()
                .ok_or_else(|| ToolError::internal("failed to read session snapshot"))?
        };
        let event_name = if outcome == "completed" {
            "computer_use.loop.completed"
        } else if outcome == "failed" {
            "computer_use.loop.failed"
        } else {
            "computer_use.loop.stopped"
        };
        trace.emit_stage(
            attempt_id,
            event_name,
            None,
            Some(serde_json::json!({
                "session_id": session_id,
                "outcome": outcome,
                "loop_state": session_snapshot.loop_state.as_str(),
                "failure_class": session_snapshot.stop_failure_class.map(|value| value.as_str().to_owned()),
            })),
        );

        Ok(FunctionToolOutput::with_payload(
            format!("computer_use session {} {}", session_id, outcome),
            true,
            payload,
        ))
    }
}

fn app_target_from_args(target: &ComputerUseTargetArgs) -> AppTarget {
    AppTarget {
        name: target.name.clone(),
        pid: target.pid,
        identity_key: target.identity_key.clone(),
        bundle_id: target.bundle_id.clone(),
        executable_path: target.executable_path.clone(),
    }
}

fn ensure_app_target_has_identity(target: &AppTarget) -> Result<(), ToolError> {
    if target.pid.is_some()
        || has_non_empty(target.identity_key.as_deref())
        || has_non_empty(target.bundle_id.as_deref())
        || has_non_empty(target.executable_path.as_deref())
        || has_non_empty(target.name.as_deref())
    {
        return Ok(());
    }
    Err(ToolError::invalid_arguments(
        "computer_use app target requires one of pid, identity_key, bundle_id, executable_path, or name",
    ))
}

fn app_meta_matches_target(app: &AppMeta, target: &AppTarget) -> bool {
    if let Some(pid) = target.pid {
        return app.pid == Some(pid);
    }
    if let Some(identity_key) = non_empty(target.identity_key.as_deref()) {
        return normalized_identity_match(app.identity_key.as_deref(), identity_key);
    }
    if let Some(bundle_id) = non_empty(target.bundle_id.as_deref()) {
        return normalized_identity_match(app.bundle_id.as_deref(), bundle_id);
    }
    if let Some(executable_path) = non_empty(target.executable_path.as_deref()) {
        return normalized_identity_match(app.executable_path.as_deref(), executable_path);
    }
    if let Some(name) = non_empty(target.name.as_deref()) {
        return normalize_app_match_value(app.name.as_str()) == normalize_app_match_value(name)
            || app.localized_name.as_deref().is_some_and(|localized_name| {
                normalize_app_match_value(localized_name) == normalize_app_match_value(name)
            });
    }
    false
}

fn app_handle_matches_target(app: &AppHandle, target: &AppTarget) -> bool {
    if let Some(pid) = target.pid {
        return app.pid == Some(pid);
    }
    if let Some(identity_key) = non_empty(target.identity_key.as_deref()) {
        return normalized_identity_match(app.identity_key.as_deref(), identity_key);
    }
    if let Some(bundle_id) = non_empty(target.bundle_id.as_deref()) {
        return normalized_identity_match(app.bundle_id.as_deref(), bundle_id);
    }
    if let Some(executable_path) = non_empty(target.executable_path.as_deref()) {
        return normalized_identity_match(app.executable_path.as_deref(), executable_path);
    }
    if let Some(name) = non_empty(target.name.as_deref()) {
        return normalize_app_match_value(app.name.as_str()) == normalize_app_match_value(name)
            || app.localized_name.as_deref().is_some_and(|localized_name| {
                normalize_app_match_value(localized_name) == normalize_app_match_value(name)
            });
    }
    false
}

fn app_target_from_meta(app: &AppMeta) -> AppTarget {
    AppTarget {
        name: Some(app.name.clone()),
        pid: app.pid,
        identity_key: app.identity_key.clone(),
        bundle_id: app.bundle_id.clone(),
        executable_path: app.executable_path.clone(),
    }
}

fn app_resolution_diagnostic(message: &str, target: &AppTarget, apps: &[AppMeta]) -> String {
    const CANDIDATE_LIMIT: usize = 8;
    let candidates = apps
        .iter()
        .take(CANDIDATE_LIMIT)
        .map(|app| {
            serde_json::json!({
                "identity_key": app.identity_key.as_deref().map(compact_diagnostic_text),
                "name": compact_diagnostic_text(app.name.as_str()),
                "localized_name": app.localized_name.as_deref().map(compact_diagnostic_text),
                "pid": app.pid,
                "bundle_id": app.bundle_id.as_deref().map(compact_diagnostic_text),
                "executable_path": app.executable_path.as_deref().map(compact_diagnostic_text),
            })
        })
        .collect::<Vec<_>>();
    let diagnostic = serde_json::json!({
        "reason": message,
        "accepted_target_fields": ["pid", "identity_key", "bundle_id", "executable_path", "name"],
        "requested": {
            "pid": target.pid,
            "identity_key": target.identity_key.clone(),
            "bundle_id": target.bundle_id.clone(),
            "executable_path": target.executable_path.clone(),
            "name": target.name.clone(),
        },
        "candidate_count": apps.len(),
        "candidate_limit": CANDIDATE_LIMIT,
        "omitted_candidate_count": apps.len().saturating_sub(CANDIDATE_LIMIT),
        "candidates": candidates,
    });
    diagnostic.to_string()
}

fn compact_diagnostic_text(value: &str) -> String {
    compact_error_text(value, 160)
}

fn compact_error_text(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_owned();
    }
    let keep = max_chars.saturating_sub(1);
    format!("{}...", value.chars().take(keep).collect::<String>())
}

fn has_non_empty(value: Option<&str>) -> bool {
    non_empty(value).is_some()
}

fn non_empty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn normalized_identity_match(candidate: Option<&str>, requested: &str) -> bool {
    candidate.is_some_and(|candidate| {
        normalize_app_match_value(candidate) == normalize_app_match_value(requested)
    })
}

fn normalize_app_match_value(value: &str) -> String {
    value
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

fn has_fresh_completion_evidence(session: &ComputerUseSession) -> bool {
    session
        .last_completion_evidence
        .as_ref()
        .is_some_and(|evidence| {
            session.last_evidence_at_step == Some(session.step_count)
                && evidence.step_count == session.step_count
                && matches!(evidence.strength.as_str(), "strong" | "weak")
        })
}
