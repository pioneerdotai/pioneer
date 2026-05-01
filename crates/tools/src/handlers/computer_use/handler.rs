use super::backend::{ComputerUseBackend, LocalComputerUseBackend};
use super::model::*;
use super::state::{ComputerUseSessionManager, cleanup_artifacts_sync, parse_json_args};
use super::util::{
    apply_action_loop_guards, apply_recovery_signal, apply_snapshot_loop_guards, compute_hash,
    default_loop_guard, ensure_session_running, now_unix_ms, resolve_action,
    resolve_snapshot_budget, resolve_snapshot_destination, stop_session_as_completed,
    stop_session_as_stopped, stop_session_with_reason,
};
use crate::ComputerUseToolsConfig;
use crate::context::{FunctionToolOutput, ToolInvocation, ToolOutput};
use crate::error::ToolError;
use crate::events::ToolEventTrace;
use crate::registry::ToolHandler;
use async_trait::async_trait;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct ComputerUseHandler {
    config: ComputerUseToolsConfig,
    backend: Arc<dyn ComputerUseBackend>,
    manager: Arc<Mutex<ComputerUseSessionManager>>,
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
            backend: Arc::new(LocalComputerUseBackend),
            manager,
        }
    }

    fn artifacts_root(&self) -> PathBuf {
        self.config
            .runtime_home_dir
            .join(self.config.artifacts_subdir.as_str())
    }

    async fn cleanup_artifacts(&self) -> Result<(), ToolError> {
        let root = self.artifacts_root();
        let retention_hours = self.config.retention_hours;
        let max_total_bytes = self.config.max_total_bytes;

        tokio::task::spawn_blocking(move || {
            cleanup_artifacts_sync(root.as_path(), retention_hours, max_total_bytes)
        })
        .await
        .map_err(|error| ToolError::internal(format!("cleanup task join error: {error}")))?
    }

    fn choose_display(
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

    async fn handle_list_displays(
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

    async fn handle_start(
        &self,
        args: ComputerUseArgs,
        attempt_id: u32,
        trace: &ToolEventTrace,
    ) -> Result<FunctionToolOutput, ToolError> {
        self.cleanup_artifacts().await?;

        let display = Self::choose_display(self.backend.list_displays()?, args.display_id)?;
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
            display: display.clone(),
            last_snapshot: None,
            last_action: None,
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
                "Started remote-only computer_use session {} on display {}",
                session_id, display.display_id
            ),
            true,
            serde_json::json!({
                "action": "start",
                "mode": "remote",
                "session_id": session_id,
                "status": "running",
                "loop_state": ComputerUseLoopState::Started.as_str(),
                "goal": goal,
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

    async fn handle_snapshot(
        &self,
        args: ComputerUseArgs,
        attempt_id: u32,
        trace: &ToolEventTrace,
    ) -> Result<FunctionToolOutput, ToolError> {
        let session_id = args.session_id.ok_or_else(|| {
            ToolError::invalid_arguments("computer_use snapshot requires session_id")
        })?;

        let (display_id, snapshot_index, artifacts_dir, goal, snapshot_budget) = {
            let mut manager = self.manager.lock().await;
            let session = manager.sessions.get_mut(&session_id).ok_or_else(|| {
                ToolError::NotFound(format!("computer_use session {} not found", session_id))
            })?;
            ensure_session_running(session)?;
            (
                session.display.display_id,
                session.snapshot_count.saturating_add(1),
                PathBuf::from(session.artifacts_dir.as_str()),
                session.goal.clone(),
                session.snapshot_budget.clone(),
            )
        };

        let frame = self.backend.capture_display(display_id, &snapshot_budget)?;
        let destination = resolve_snapshot_destination(
            artifacts_dir.as_path(),
            args.screenshot_path.as_deref(),
            snapshot_index,
        )?;
        if let Some(parent) = destination.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|error| {
                ToolError::execution_failed(format!(
                    "failed to create snapshot directory `{}`: {error}",
                    parent.display()
                ))
            })?;
        }
        tokio::fs::write(destination.as_path(), frame.png_bytes.as_slice())
            .await
            .map_err(|error| {
                ToolError::execution_failed(format!(
                    "failed to write snapshot `{}`: {error}",
                    destination.display()
                ))
            })?;

        let snapshot = SnapshotMeta {
            index: snapshot_index,
            path: destination.display().to_string(),
            width_px: frame.width_px,
            height_px: frame.height_px,
            scale_factor: frame.scale_factor,
            size_bytes: frame.png_bytes.len(),
            resize_passes: frame.resize_passes,
            captured_at_unix_ms: now_unix_ms(),
            state_hash: compute_hash(frame.png_bytes.as_slice()),
        };

        let (status, stop_reason, stop_failure_class, loop_guard, loop_state) = {
            let mut manager = self.manager.lock().await;
            let session = manager.sessions.get_mut(&session_id).ok_or_else(|| {
                ToolError::NotFound(format!("computer_use session {} not found", session_id))
            })?;
            ensure_session_running(session)?;
            session.loop_state = ComputerUseLoopState::SnapshotCaptured;
            if let Some(loop_guard_reason) =
                apply_snapshot_loop_guards(session, snapshot.state_hash.as_str())
            {
                stop_session_with_reason(
                    session,
                    format!("loop_guard:{loop_guard_reason}"),
                    ComputerUseFailureClass::LoopGuardTriggered,
                );
            }
            session.snapshot_count = snapshot_index;
            session.updated_at_unix_ms = now_unix_ms();
            session.last_snapshot = Some(snapshot.clone());
            if session.status == ComputerUseStatus::Running {
                session.loop_state = ComputerUseLoopState::PlannerRequestBuilt;
            }
            (
                session.status,
                session.stop_reason.clone(),
                session.stop_failure_class,
                session.loop_guard.clone(),
                session.loop_state,
            )
        };

        trace.emit_stage(
            attempt_id,
            "computer_use.snapshot",
            None,
            Some(serde_json::json!({
                "session_id": session_id,
                "snapshot_index": snapshot_index,
                "size_bytes": snapshot.size_bytes,
                "resize_passes": snapshot.resize_passes,
                "safety_transform": {
                    "applied": snapshot.resize_passes > 0,
                    "reason": if snapshot.resize_passes > 0 { serde_json::Value::String("attachment_budget_exceeded".to_owned()) } else { serde_json::Value::Null }
                },
                "loop_guard": loop_guard,
                "status": status.as_str(),
                "mode": "remote",
            })),
        );
        if snapshot.resize_passes > 0 {
            trace.emit_stage(
                attempt_id,
                "computer_use.snapshot.transformed",
                None,
                Some(serde_json::json!({
                    "session_id": session_id,
                    "snapshot_index": snapshot_index,
                    "reason": "attachment_budget_exceeded",
                    "resize_passes": snapshot.resize_passes,
                    "size_bytes": snapshot.size_bytes,
                    "sha256": snapshot.state_hash,
                })),
            );
        }
        trace.emit_stage(
            attempt_id,
            "computer_use.loop.step",
            None,
            Some(serde_json::json!({
                "session_id": session_id,
                "phase": "snapshot",
                "status": status.as_str(),
                "state": loop_state.as_str(),
            })),
        );
        if status == ComputerUseStatus::Stopped {
            trace.emit_stage(
                attempt_id,
                "computer_use.loop.failed",
                stop_reason.clone(),
                Some(serde_json::json!({
                    "session_id": session_id,
                    "failure_class": stop_failure_class.map(|value| value.as_str().to_owned()),
                })),
            );
        }

        let mut payload = serde_json::json!({
            "action": "snapshot",
            "mode": "remote",
            "session_id": session_id,
            "status": status.as_str(),
            "loop_state": loop_state.as_str(),
            "stop_reason": stop_reason,
            "failure_class": stop_failure_class.map(|value| value.as_str().to_owned()),
            "snapshot": snapshot,
            "loop_guard": loop_guard,
            "snapshot_budget": snapshot_budget,
        });
        if status == ComputerUseStatus::Running {
            payload["llm_context"] = serde_json::json!({
                "goal": goal,
                "instruction": "Analyze screenshot and return exactly one next computer_use act command JSON with shape {\"action\":\"act\",\"session_id\":<id>,\"act\":{\"type\":\"click|double_click|right_click|move|scroll|type_text|hotkey|wait\",...}}.",
                "attachment": {
                    "path": destination,
                    "mime_type": "image/png",
                    "size_bytes": payload["snapshot"]["size_bytes"],
                    "sha256": payload["snapshot"]["state_hash"],
                    "safety_transform": {
                        "applied": payload["snapshot"]["resize_passes"].as_u64().unwrap_or(0) > 0,
                        "reason": if payload["snapshot"]["resize_passes"].as_u64().unwrap_or(0) > 0 { serde_json::Value::String("attachment_budget_exceeded".to_owned()) } else { serde_json::Value::Null },
                        "resize_passes": payload["snapshot"]["resize_passes"]
                    }
                }
            });
        }

        Ok(FunctionToolOutput::with_payload(
            format!(
                "Snapshot saved for session {} at {}",
                session_id,
                payload["snapshot"]["path"].as_str().unwrap_or_default()
            ),
            true,
            payload,
        ))
    }

    async fn handle_act(
        &self,
        args: ComputerUseArgs,
        attempt_id: u32,
        trace: &ToolEventTrace,
    ) -> Result<FunctionToolOutput, ToolError> {
        let session_id = args
            .session_id
            .ok_or_else(|| ToolError::invalid_arguments("computer_use act requires session_id"))?;
        let act = args
            .act
            .ok_or_else(|| {
                ToolError::invalid_arguments(
                    "computer_use act requires act object. Use {\"action\":\"act\",\"session_id\":<id>,\"act\":{\"type\":\"click\",\"x_norm\":0.5,\"y_norm\":0.5}}",
                )
            })?;
        let recovery_attempt = args.recovery_attempt;
        let expected_effect_mismatch = args.expected_effect_mismatch.unwrap_or(false);
        let provided_failure_class = if let Some(raw) = args.failure_class.as_deref() {
            Some(ComputerUseFailureClass::parse(raw).ok_or_else(|| {
                ToolError::invalid_arguments(format!(
                    "invalid computer_use failure_class `{}`",
                    raw
                ))
            })?)
        } else {
            None
        };
        let recovery_failure_class = if expected_effect_mismatch {
            Some(ComputerUseFailureClass::ExpectedEffectMismatch)
        } else {
            provided_failure_class
        };
        if recovery_attempt.is_some() && recovery_failure_class.is_none() {
            return Err(ToolError::invalid_arguments(
                "computer_use act recovery_attempt requires failure_class or expected_effect_mismatch=true",
            ));
        }

        if expected_effect_mismatch {
            trace.emit_stage(
                attempt_id,
                "computer_use.action.expected_effect_mismatch",
                None,
                Some(serde_json::json!({
                    "session_id": session_id,
                    "failure_class": ComputerUseFailureClass::ExpectedEffectMismatch.as_str(),
                    "attempt": recovery_attempt,
                })),
            );
        }

        let (
            display,
            goal,
            pre_status,
            pre_reason,
            pre_failure_class,
            pre_loop_guard,
            pre_session,
            pre_recovery_attempts_current_step,
            pre_recovery_attempts_run,
        ) = {
            let mut manager = self.manager.lock().await;
            let session = manager.sessions.get_mut(&session_id).ok_or_else(|| {
                ToolError::NotFound(format!("computer_use session {} not found", session_id))
            })?;
            ensure_session_running(session)?;
            session.loop_state = ComputerUseLoopState::LlmDecisionReceived;

            if let Some(failure_class) = recovery_failure_class {
                if failure_class.is_retryable() {
                    if let Some(recovery_limit_reason) =
                        apply_recovery_signal(session, recovery_attempt, Some(failure_class))
                    {
                        stop_session_with_reason(
                            session,
                            recovery_limit_reason,
                            ComputerUseFailureClass::RecoveryBudgetExceeded,
                        );
                    }
                } else {
                    stop_session_with_reason(
                        session,
                        format!("non_retryable_recovery:{}", failure_class.as_str()),
                        failure_class,
                    );
                }
            }

            (
                session.display.clone(),
                session.goal.clone(),
                session.status,
                session.stop_reason.clone(),
                session.stop_failure_class,
                session.loop_guard.clone(),
                session.clone(),
                session.recovery_attempts_current_step,
                session.recovery_attempts_run,
            )
        };
        match recovery_failure_class {
            Some(class) if class.is_retryable() => {
                trace.emit_stage(
                    attempt_id,
                    "computer_use.recovery.triggered",
                    None,
                    Some(serde_json::json!({
                        "session_id": session_id,
                        "failure_class": class.as_str(),
                        "attempt": recovery_attempt,
                        "attempts_current_step": pre_recovery_attempts_current_step,
                        "attempts_run": pre_recovery_attempts_run,
                    })),
                );
            }
            Some(class) => {
                trace.emit_stage(
                    attempt_id,
                    "computer_use.recovery.skipped",
                    Some("non-retryable failure class".to_owned()),
                    Some(serde_json::json!({
                        "session_id": session_id,
                        "failure_class": class.as_str(),
                        "attempt": recovery_attempt,
                    })),
                );
            }
            None => {}
        }
        if pre_status == ComputerUseStatus::Stopped {
            let is_exhausted =
                pre_failure_class == Some(ComputerUseFailureClass::RecoveryBudgetExceeded);
            let recovery_event = if is_exhausted {
                "computer_use.recovery.exhausted"
            } else {
                "computer_use.recovery.skipped"
            };
            trace.emit_stage(
                attempt_id,
                recovery_event,
                pre_reason.clone(),
                Some(serde_json::json!({
                    "session_id": session_id,
                    "failure_class": pre_failure_class.map(|value| value.as_str().to_owned()),
                })),
            );
            return Ok(FunctionToolOutput::with_payload(
                if is_exhausted {
                    format!("Recovery budget exhausted in session {}", session_id)
                } else {
                    format!(
                        "Recovery skipped due non-retryable failure in session {}",
                        session_id
                    )
                },
                true,
                serde_json::json!({
                    "action": "act",
                    "mode": "remote",
                    "session_id": session_id,
                    "status": pre_status.as_str(),
                    "loop_state": pre_session.loop_state.as_str(),
                    "stop_reason": pre_reason,
                    "failure_class": pre_failure_class.map(|value| value.as_str().to_owned()),
                    "loop_guard": pre_loop_guard,
                    "recovery": {
                        "attempt": recovery_attempt,
                        "failure_class": recovery_failure_class.map(|value| value.as_str().to_owned()),
                        "attempts_current_step": pre_session.recovery_attempts_current_step,
                        "attempts_run": pre_session.recovery_attempts_run,
                    },
                }),
            ));
        }

        let resolved = resolve_action(act, &display)?;
        let (guard_status, guard_reason, guard_failure_class, guard_loop_state, guard_loop_guard) = {
            let mut manager = self.manager.lock().await;
            let session = manager.sessions.get_mut(&session_id).ok_or_else(|| {
                ToolError::NotFound(format!("computer_use session {} not found", session_id))
            })?;
            ensure_session_running(session)?;
            if let Some(loop_guard_reason) = apply_action_loop_guards(session, &resolved) {
                stop_session_with_reason(
                    session,
                    format!("loop_guard:{loop_guard_reason}"),
                    ComputerUseFailureClass::LoopGuardTriggered,
                );
            }
            (
                session.status,
                session.stop_reason.clone(),
                session.stop_failure_class,
                session.loop_state,
                session.loop_guard.clone(),
            )
        };
        if guard_status == ComputerUseStatus::Stopped {
            trace.emit_stage(
                attempt_id,
                "computer_use.loop.failed",
                guard_reason.clone(),
                Some(serde_json::json!({
                    "session_id": session_id,
                    "failure_class": guard_failure_class.map(|value| value.as_str().to_owned()),
                })),
            );
            return Ok(FunctionToolOutput::with_payload(
                format!("Action rejected by loop guard in session {}", session_id),
                true,
                serde_json::json!({
                    "action": "act",
                    "mode": "remote",
                    "session_id": session_id,
                    "status": guard_status.as_str(),
                    "loop_state": guard_loop_state.as_str(),
                    "stop_reason": guard_reason,
                    "failure_class": guard_failure_class.map(|value| value.as_str().to_owned()),
                    "loop_guard": guard_loop_guard,
                }),
            ));
        }
        let message = match &resolved {
            ResolvedAction::Wait { wait_ms } => {
                tokio::time::sleep(std::time::Duration::from_millis(*wait_ms)).await;
                format!("Waited {}ms", wait_ms)
            }
            _ => self.backend.perform_action(&resolved)?,
        };

        let now = now_unix_ms();
        let payload_action = serde_json::to_value(&resolved).unwrap_or(serde_json::Value::Null);
        let (
            status,
            step_count,
            stop_reason,
            failure_class,
            loop_guard,
            loop_state,
            recovery_state,
        ) = {
            let mut manager = self.manager.lock().await;
            let session = manager.sessions.get_mut(&session_id).ok_or_else(|| {
                ToolError::NotFound(format!("computer_use session {} not found", session_id))
            })?;
            ensure_session_running(session)?;

            session.step_count = session.step_count.saturating_add(1);
            session.updated_at_unix_ms = now;
            session.loop_state = ComputerUseLoopState::ActionExecuted;
            session.awaiting_post_action_snapshot = true;
            session.recovery_attempts_current_step = 0;
            session.last_action = Some(ActionRecord {
                index: session.step_count,
                action_type: resolved.action_type().to_owned(),
                payload: payload_action.clone(),
                executed_at_unix_ms: now,
                message: message.clone(),
            });

            if session.step_count >= session.max_steps {
                stop_session_with_reason(
                    session,
                    "max_steps_exceeded",
                    ComputerUseFailureClass::RuntimeActionError,
                );
            } else {
                session.loop_state = ComputerUseLoopState::PostActionResultReported;
            }

            (
                session.status,
                session.step_count,
                session.stop_reason.clone(),
                session.stop_failure_class,
                session.loop_guard.clone(),
                session.loop_state,
                serde_json::json!({
                    "attempt": recovery_attempt,
                    "failure_class": recovery_failure_class.map(|value| value.as_str().to_owned()),
                    "attempts_current_step": session.recovery_attempts_current_step,
                    "attempts_run": session.recovery_attempts_run,
                    "max_attempts_per_step": session.max_recovery_attempts_per_step,
                    "max_attempts_per_run": session.max_recovery_attempts_per_run,
                }),
            )
        };

        trace.emit_stage(
            attempt_id,
            "computer_use.act",
            None,
            Some(serde_json::json!({
                "session_id": session_id,
                "step_count": step_count,
                "action_type": resolved.action_type(),
                "loop_guard": loop_guard,
                "mode": "remote",
            })),
        );
        trace.emit_stage(
            attempt_id,
            "computer_use.loop.step",
            None,
            Some(serde_json::json!({
                "session_id": session_id,
                "phase": "act",
                "status": status.as_str(),
                "state": loop_state.as_str(),
            })),
        );
        if status == ComputerUseStatus::Stopped {
            trace.emit_stage(
                attempt_id,
                "computer_use.loop.failed",
                stop_reason.clone(),
                Some(serde_json::json!({
                    "session_id": session_id,
                    "failure_class": failure_class.map(|value| value.as_str().to_owned()),
                })),
            );
        }

        Ok(FunctionToolOutput::with_payload(
            format!("Action executed in session {}: {}", session_id, message),
            true,
            serde_json::json!({
                "action": "act",
                "mode": "remote",
                "session_id": session_id,
                "status": status.as_str(),
                "loop_state": loop_state.as_str(),
                "step_count": step_count,
                "stop_reason": stop_reason,
                "failure_class": failure_class.map(|value| value.as_str().to_owned()),
                "loop_guard": loop_guard,
                "recovery": recovery_state,
                "result": {
                    "message": message,
                    "action": payload_action,
                    "executed_at_unix_ms": now,
                },
                "llm_context": {
                    "goal": goal,
                    "instruction": if status == ComputerUseStatus::Running { "Given the action result, request next snapshot or return done if the goal is completed." } else { "Session stopped. Analyze stop_reason and decide whether to start a new session." }
                }
            }),
        ))
    }

    async fn handle_status(&self, args: ComputerUseArgs) -> Result<FunctionToolOutput, ToolError> {
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
                "display": session.display,
                "last_snapshot": session.last_snapshot,
                "last_action": session.last_action,
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

    async fn handle_stop(
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

#[async_trait]
impl ToolHandler for ComputerUseHandler {
    async fn handle(
        &self,
        invocation: ToolInvocation,
        trace: ToolEventTrace,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let args = parse_json_args::<ComputerUseArgs>(invocation.payload)?;
        let action = args.action.trim().to_ascii_lowercase();

        let output = match action.as_str() {
            "list_displays" => {
                self.handle_list_displays(invocation.attempt_id, &trace)
                    .await?
            }
            "start" => {
                self.handle_start(args, invocation.attempt_id, &trace)
                    .await?
            }
            "snapshot" => {
                self.handle_snapshot(args, invocation.attempt_id, &trace)
                    .await?
            }
            "act" => self.handle_act(args, invocation.attempt_id, &trace).await?,
            "status" => self.handle_status(args).await?,
            "stop" => {
                self.handle_stop(args, invocation.attempt_id, &trace)
                    .await?
            }
            _ => {
                return Err(ToolError::invalid_arguments(format!(
                    "unsupported computer_use action `{}`; supported: list_displays, start, snapshot, act, status, stop",
                    action
                )));
            }
        };

        Ok(Box::new(output))
    }
}

#[cfg(test)]
impl ComputerUseHandler {
    pub(super) fn with_backend(
        config: ComputerUseToolsConfig,
        backend: Arc<dyn ComputerUseBackend>,
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
