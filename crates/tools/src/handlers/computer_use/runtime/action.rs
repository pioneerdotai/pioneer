use super::super::actions::parse_computer_use_action;
use super::super::handler::ComputerUseHandler;
use super::super::model::*;
use super::super::targets::{
    TargetResolutionError, TargetResolutionFailureClass, resolve_input_action_targets,
    resolve_semantic_action_target,
};
use super::super::util::{
    apply_action_loop_guards, apply_recovery_signal, ensure_session_running, now_unix_ms,
    stop_session_with_reason,
};
use crate::context::FunctionToolOutput;
use crate::error::ToolError;
use crate::events::ToolEventTrace;

impl ComputerUseHandler {
    pub(crate) async fn handle_act(
        &self,
        args: ComputerUseArgs,
        attempt_id: u32,
        trace: &ToolEventTrace,
        process_environment: &crate::process_policy::ProcessEnvironmentPlan,
    ) -> Result<FunctionToolOutput, ToolError> {
        let session_id = args
            .session_id
            .ok_or_else(|| ToolError::invalid_arguments("computer_use act requires session_id"))?;
        let act = args
            .act
            .ok_or_else(|| {
                ToolError::invalid_arguments(
            "computer_use act requires act object. Use {\"action\":\"act\",\"session_id\":<id>,\"act\":{\"type\":\"press\",\"target\":{\"node_id\":\"n42\",\"snapshot_id\":\"s1-1\"}}}",
                )
            })?;
        let recovery_attempt = args.recovery_attempt;
        let recovery_failure_class = if let Some(raw) = args.failure_class.as_deref() {
            Some(ComputerUseFailureClass::parse(raw).ok_or_else(|| {
                ToolError::invalid_arguments(format!(
                    "invalid computer_use failure_class `{}`",
                    raw
                ))
            })?)
        } else {
            None
        };
        if recovery_attempt.is_some() && recovery_failure_class.is_none() {
            return Err(ToolError::invalid_arguments(
                "computer_use act recovery_attempt requires failure_class",
            ));
        }
        let resolved = parse_computer_use_action(act)?;

        let (
            target,
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
                session.target.clone(),
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
        let mut pre_execution_failure = None;
        let resolved_semantic_target = match &resolved {
            ComputerUseAction::Semantic(action) => {
                match resolve_semantic_action_target(
                    action,
                    pre_session.last_node_refs.as_slice(),
                    pre_session.last_snapshot.as_ref(),
                ) {
                    Ok(target) => target,
                    Err(error) => {
                        pre_execution_failure =
                            Some(target_resolution_execution(resolved.action_type(), error));
                        None
                    }
                }
            }
            ComputerUseAction::Input(_) | ComputerUseAction::Os(_) => None,
        };
        let resolved_input_targets = if pre_execution_failure.is_some() {
            ResolvedInputActionTargets::default()
        } else {
            match &resolved {
                ComputerUseAction::Input(action) => {
                    if action.action_type != InputActionKind::Wait
                        && !self.config.input_simulation_enabled
                    {
                        ResolvedInputActionTargets::default()
                    } else {
                        match resolve_input_action_targets(
                            action,
                            pre_session.last_node_refs.as_slice(),
                            pre_session.last_snapshot.as_ref(),
                        ) {
                            Ok(targets) => targets,
                            Err(error) => {
                                pre_execution_failure = Some(target_resolution_execution(
                                    resolved.action_type(),
                                    error,
                                ));
                                ResolvedInputActionTargets::default()
                            }
                        }
                    }
                }
                ComputerUseAction::Semantic(_) | ComputerUseAction::Os(_) => {
                    ResolvedInputActionTargets::default()
                }
            }
        };

        let execution = if let Some(execution) = pre_execution_failure {
            execution
        } else {
            match &resolved {
                ComputerUseAction::Os(action) => self
                    .backend
                    .perform_os_action(action, process_environment)?,
                ComputerUseAction::Input(action) if action.action_type == InputActionKind::Wait => {
                    let wait_ms = action.wait_ms.unwrap_or(250).clamp(1, 60_000);
                    tokio::time::sleep(std::time::Duration::from_millis(wait_ms)).await;
                    ActionExecution {
                        status: "ok".to_owned(),
                        message: format!("Waited {}ms", wait_ms),
                        action_type: Some(action.action_type.as_str().to_owned()),
                        target: None,
                        duration_ms: Some(wait_ms),
                        failure_class: None,
                        app_before: None,
                        app_after: None,
                        details: None,
                    }
                }
                ComputerUseAction::Input(action) => {
                    if self.config.input_simulation_enabled {
                        self.backend
                            .perform_input_action(action, &resolved_input_targets)?
                    } else {
                        ActionExecution {
                            status: "failed".to_owned(),
                            message: "input simulation is disabled by computer_use config"
                                .to_owned(),
                            action_type: Some(action.action_type.as_str().to_owned()),
                            target: resolved_input_targets.target.clone(),
                            duration_ms: Some(0),
                            failure_class: Some(
                                ComputerUseFailureClass::InputSimulationUnavailable
                                    .as_str()
                                    .to_owned(),
                            ),
                            app_before: None,
                            app_after: None,
                            details: None,
                        }
                    }
                }
                ComputerUseAction::Semantic(action) => {
                    let app = app_handle_for_semantic_action(&target)?;
                    let action = semantic_action_with_config_timeout(
                        action,
                        self.config.semantic_action_timeout_ms,
                    );
                    self.backend.perform_semantic_action(
                        &app,
                        &action,
                        resolved_semantic_target.as_ref(),
                    )?
                }
            }
        };
        let target_update = app_target_update_for_os_action(
            &resolved,
            &execution,
            &target,
            self.config.accessibility_tree_max_depth,
        );
        let message = execution.message.clone();

        let now = now_unix_ms();
        let payload_action = serde_json::to_value(&resolved).unwrap_or(serde_json::Value::Null);
        let payload_target =
            serde_json::to_value(&resolved_semantic_target).unwrap_or(serde_json::Value::Null);
        let payload_input_targets =
            serde_json::to_value(&resolved_input_targets).unwrap_or(serde_json::Value::Null);
        let payload_execution = serde_json::to_value(&execution).unwrap_or(serde_json::Value::Null);
        let coordinate_observability = coordinate_observability_for_action(
            &resolved,
            &resolved_input_targets,
            pre_session.last_snapshot.as_ref(),
            execution.status.as_str(),
            execution.message.as_str(),
        );
        let execution_failure_class = execution
            .failure_class
            .as_deref()
            .and_then(ComputerUseFailureClass::parse);
        let suggested_fallbacks = if execution.status == "failed" {
            suggested_fallbacks_for_failed_action(
                &resolved,
                &execution,
                pre_session.last_node_refs.as_slice(),
                pre_session.last_snapshot.as_ref(),
            )
        } else {
            Vec::new()
        };
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
            if let Some(target_update) = target_update.clone() {
                session.target = target_update;
            }
            session.awaiting_post_action_snapshot = true;
            session.recovery_attempts_current_step = 0;
            session.last_action = Some(ActionRecord {
                index: session.step_count,
                action_type: resolved.action_type().to_owned(),
                payload: payload_action.clone(),
                executed_at_unix_ms: now,
                message: message.clone(),
            });

            if let Some(class) =
                execution_failure_class.filter(|class| class.is_fatal_action_failure())
            {
                stop_session_with_reason(
                    session,
                    format!("action_failed:{}", class.as_str()),
                    class,
                );
            } else if session.step_count >= session.max_steps {
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
                "suggested_fallbacks": suggested_fallbacks.clone(),
                "coordinate_observability": coordinate_observability.clone(),
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
        let payload_failure_class = failure_class.or(execution_failure_class);
        let next_call = if status == ComputerUseStatus::Running && execution.status == "failed" {
            serde_json::json!({
                "tool": "computer_use",
                "arguments": {
                    "action": "snapshot",
                    "session_id": session_id
                },
                "reason": "action_failed_refresh_state_before_next_decision"
            })
        } else {
            serde_json::Value::Null
        };
        let structured_trace = serde_json::json!({
            "session_id": session_id,
            "snapshot_id": pre_session.last_snapshot.as_ref().map(|snapshot| snapshot.snapshot_id.clone()),
            "action_kind": action_kind(&resolved),
            "action_type": resolved.action_type(),
            "target_before_resolution": payload_action,
            "resolved_target": {
                "semantic": payload_target,
                "input": payload_input_targets,
            },
            "app_before": payload_execution.get("app_before").cloned().unwrap_or(serde_json::Value::Null),
            "app_after": payload_execution.get("app_after").cloned().unwrap_or(serde_json::Value::Null),
            "coordinate_conversion": coordinate_observability,
            "execution_status": execution.status.clone(),
            "failure_class": payload_failure_class.map(|value| value.as_str().to_owned()),
            "suggested_fallbacks": suggested_fallbacks,
            "verification_evidence": pre_session.last_verification,
            "progress_signals": pre_session.last_progress_signals,
        });
        let trace_suggested_fallbacks = structured_trace
            .get("suggested_fallbacks")
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        let trace_target_before_resolution = structured_trace
            .get("target_before_resolution")
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        let trace_resolved_semantic_target = structured_trace
            .pointer("/resolved_target/semantic")
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        let trace_resolved_input_targets = structured_trace
            .pointer("/resolved_target/input")
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        let trace_coordinate_conversion = structured_trace
            .get("coordinate_conversion")
            .cloned()
            .unwrap_or(serde_json::Value::Null);

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
                "failure_class": payload_failure_class.map(|value| value.as_str().to_owned()),
                "loop_guard": loop_guard,
                "recovery": recovery_state,
                "next_call": next_call,
                "suggested_fallbacks": trace_suggested_fallbacks.clone(),
                "trace": structured_trace.clone(),
                "result": {
                    "status": execution.status,
                    "message": message,
                    "action": trace_target_before_resolution,
                    "target": trace_resolved_semantic_target,
                    "input_targets": trace_resolved_input_targets,
                    "coordinate_observability": trace_coordinate_conversion,
                    "execution": payload_execution,
                    "suggested_fallbacks": trace_suggested_fallbacks,
                    "trace": structured_trace,
                    "executed_at_unix_ms": now,
                },
                "llm_context": {
                    "goal": goal,
                    "instruction": action_result_llm_instruction(status, execution_failure_class)
                }
            }),
        ))
    }
}

fn action_kind(action: &ComputerUseAction) -> &'static str {
    match action {
        ComputerUseAction::Os(_) => "os",
        ComputerUseAction::Semantic(_) => "semantic",
        ComputerUseAction::Input(_) => "input",
    }
}

fn coordinate_observability_for_action(
    action: &ComputerUseAction,
    targets: &ResolvedInputActionTargets,
    snapshot: Option<&SnapshotMeta>,
    execution_status: &str,
    execution_message: &str,
) -> serde_json::Value {
    let ComputerUseAction::Input(action) = action else {
        return serde_json::Value::Null;
    };

    let mut slots = serde_json::Map::new();
    add_coordinate_slot(
        &mut slots,
        "target",
        action.target.as_ref(),
        targets.target.as_ref(),
        execution_status,
        execution_message,
    );
    add_coordinate_slot(
        &mut slots,
        "from",
        action.from.as_ref(),
        targets.from.as_ref(),
        execution_status,
        execution_message,
    );
    add_coordinate_slot(
        &mut slots,
        "to",
        action.to.as_ref(),
        targets.to.as_ref(),
        execution_status,
        execution_message,
    );
    if slots.is_empty() {
        return serde_json::Value::Null;
    }

    serde_json::json!({
        "validation_status": if execution_status == "failed" && execution_message.contains("coordinate_") { "failed" } else { "ok" },
        "slots": slots,
        "display_bounds": coordinate_display_bounds(snapshot),
    })
}

fn add_coordinate_slot(
    slots: &mut serde_json::Map<String, serde_json::Value>,
    name: &str,
    requested_target: Option<&ActionTarget>,
    resolved_target: Option<&ResolvedActionTarget>,
    execution_status: &str,
    execution_message: &str,
) {
    let requested_point = resolved_target
        .and_then(|target| target.requested_point.clone())
        .or_else(|| requested_target.and_then(|target| target.point.clone()));
    let converted_point = resolved_target.and_then(|target| target.point.clone());
    if requested_point.is_none() && converted_point.is_none() {
        return;
    }

    let slot_status = if execution_status == "failed" && execution_message.contains("coordinate_") {
        "failed"
    } else {
        "ok"
    };
    let requested_space = requested_point
        .as_ref()
        .and_then(|point| point.coordinate_space)
        .map(|space| space.as_str());
    let converted_space = converted_point
        .as_ref()
        .and_then(|point| point.coordinate_space)
        .map(|space| space.as_str());
    slots.insert(
        name.to_owned(),
        serde_json::json!({
            "requested_point": requested_point,
            "requested_space": requested_space,
            "converted_point": converted_point,
            "converted_space": converted_space,
            "validation_status": slot_status,
            "diagnostic": if slot_status == "failed" { serde_json::Value::String(execution_message.to_owned()) } else { serde_json::Value::Null },
        }),
    );
}

fn coordinate_display_bounds(snapshot: Option<&SnapshotMeta>) -> serde_json::Value {
    let Some(snapshot) = snapshot else {
        return serde_json::Value::Null;
    };
    let scale = if snapshot.scale_factor.is_finite() && snapshot.scale_factor > 0.0 {
        f64::from(snapshot.scale_factor)
    } else {
        1.0
    };
    let native_width = (f64::from(snapshot.width_px) / scale).ceil() as u32;
    let native_height = (f64::from(snapshot.height_px) / scale).ceil() as u32;
    serde_json::json!({
        "source_pixels": {
            "width_px": snapshot.width_px,
            "height_px": snapshot.height_px,
        },
        "transport_pixels": {
            "width_px": snapshot.transport_width_px,
            "height_px": snapshot.transport_height_px,
        },
        "native_input": {
            "width": native_width,
            "height": native_height,
        },
        "scale_factor": snapshot.scale_factor,
    })
}

fn semantic_action_with_config_timeout(action: &SemanticAction, timeout_ms: u64) -> SemanticAction {
    let mut action = action.clone();
    if action.action_type == SemanticActionKind::WaitFor {
        action.wait_ms = Some(action.wait_ms.unwrap_or(timeout_ms).min(timeout_ms.max(1)));
    }
    action
}

fn target_resolution_execution(action_type: &str, error: TargetResolutionError) -> ActionExecution {
    let target_failure_class = error.failure_class;
    let target_message = error.message;
    let target_diagnostics = error.diagnostics;
    let failure_class = match target_failure_class {
        TargetResolutionFailureClass::ElementNotFound => ComputerUseFailureClass::ElementNotFound,
        TargetResolutionFailureClass::ElementStale => ComputerUseFailureClass::ElementStale,
        TargetResolutionFailureClass::AmbiguousTarget
        | TargetResolutionFailureClass::InvalidTarget => {
            ComputerUseFailureClass::RuntimeActionError
        }
    };
    ActionExecution {
        status: "failed".to_owned(),
        message: format!("target resolution failed: {target_message}"),
        action_type: Some(action_type.to_owned()),
        target: None,
        duration_ms: Some(0),
        failure_class: Some(failure_class.as_str().to_owned()),
        app_before: None,
        app_after: None,
        details: Some(serde_json::json!({
            "target_resolution": {
                "failure_class": target_failure_class.as_str(),
                "message": target_message,
                "diagnostics": target_diagnostics,
            }
        })),
    }
}

fn suggested_fallbacks_for_failed_action(
    action: &ComputerUseAction,
    execution: &ActionExecution,
    node_refs: &[AccessibilityNodeRef],
    last_snapshot: Option<&SnapshotMeta>,
) -> Vec<SuggestedAction> {
    let mut suggestions = Vec::new();

    match action {
        ComputerUseAction::Semantic(semantic) => {
            add_node_based_fallbacks(
                &mut suggestions,
                semantic,
                node_refs,
                last_snapshot,
                execution.failure_class.as_deref(),
            );
            add_ambiguous_target_fallbacks(&mut suggestions, semantic, node_refs);
        }
        ComputerUseAction::Input(input) => {
            if let Some(target) = input.target.as_ref() {
                if let Some(node_id) = target.node_id.as_deref() {
                    if let Some(node) = find_node_ref_for_suggestion(node_refs, node_id) {
                        add_bounds_click_fallback(&mut suggestions, node, last_snapshot);
                    }
                }
            }
        }
        ComputerUseAction::Os(_) => {}
    }

    dedupe_and_limit_suggestions(suggestions, 5)
}

fn add_node_based_fallbacks(
    suggestions: &mut Vec<SuggestedAction>,
    action: &SemanticAction,
    node_refs: &[AccessibilityNodeRef],
    last_snapshot: Option<&SnapshotMeta>,
    failure_class: Option<&str>,
) {
    let Some(target) = action.target.as_ref() else {
        return;
    };
    let Some(node_id) = target
        .node_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return;
    };
    let Some(node) = find_node_ref_for_suggestion(node_refs, node_id) else {
        return;
    };

    let failed_action = action.action_type.as_str();
    let target = target_for_node(node, last_snapshot);
    for supported in &node.supported_act_types {
        if supported == failed_action && matches!(failure_class, Some("action_not_supported")) {
            continue;
        }
        if let Some(suggestion) =
            semantic_suggestion_from_supported_action(supported, action, &target)
        {
            suggestions.push(suggestion);
        }
    }

    if matches!(
        failure_class,
        Some("action_not_supported" | "element_stale" | "element_not_found")
    ) {
        add_bounds_click_fallback(suggestions, node, last_snapshot);
    }
}

fn add_ambiguous_target_fallbacks(
    suggestions: &mut Vec<SuggestedAction>,
    action: &SemanticAction,
    node_refs: &[AccessibilityNodeRef],
) {
    let Some(target) = action.target.as_ref() else {
        return;
    };
    if target.nth.is_some() || target.node_id.is_some() || target.selector.is_some() {
        return;
    }
    let role = target
        .role
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let name = target
        .name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    if role.is_none() && name.is_none() {
        return;
    }
    let matches = node_refs
        .iter()
        .filter(|node| {
            role.is_none_or(|role| node.role.eq_ignore_ascii_case(role))
                && name.is_none_or(|name| node.name.as_deref() == Some(name))
        })
        .take(3)
        .enumerate()
        .collect::<Vec<_>>();
    if matches.len() < 2 {
        return;
    }
    for (index, _) in matches {
        let disambiguated = ActionTarget {
            node_id: None,
            snapshot_id: None,
            selector: None,
            role: role.map(str::to_owned),
            name: name.map(str::to_owned),
            nth: Some(index + 1),
            bounds_anchor: None,
            point: None,
        };
        if let Some(suggestion) = semantic_suggestion_from_supported_action(
            action.action_type.as_str(),
            action,
            &disambiguated,
        ) {
            suggestions.push(suggestion);
        }
    }
}

fn semantic_suggestion_from_supported_action(
    supported: &str,
    original: &SemanticAction,
    target: &ActionTarget,
) -> Option<SuggestedAction> {
    let mut suggestion = SuggestedAction {
        action_type: supported.to_owned(),
        target: Some(target.clone()),
        text: None,
        numeric_value: None,
        action_name: None,
        app: None,
        path: None,
        url: None,
        menu_path: None,
        title: None,
        wait_ms: None,
    };
    match supported {
        "press" | "focus" | "blur" | "toggle" | "select" | "expand" | "collapse" | "show_menu"
        | "scroll_into_view" => Some(suggestion),
        "set_value" | "type_text" | "select_text" => {
            suggestion.text = original.text.clone();
            suggestion.text.as_ref()?;
            Some(suggestion)
        }
        "set_numeric_value" => {
            suggestion.numeric_value = original.numeric_value;
            suggestion.numeric_value?;
            Some(suggestion)
        }
        _ => None,
    }
}

fn add_bounds_click_fallback(
    suggestions: &mut Vec<SuggestedAction>,
    node: &AccessibilityNodeRef,
    last_snapshot: Option<&SnapshotMeta>,
) {
    if node.bounds.is_none() {
        return;
    }
    let Some(snapshot_id) = last_snapshot.map(|snapshot| snapshot.snapshot_id.clone()) else {
        return;
    };
    suggestions.push(SuggestedAction {
        action_type: "input_click".to_owned(),
        target: Some(ActionTarget {
            node_id: None,
            snapshot_id: None,
            selector: None,
            role: None,
            name: None,
            nth: None,
            bounds_anchor: Some(BoundsAnchorTarget {
                node_id: node.id.clone(),
                snapshot_id: Some(snapshot_id),
                anchor: Some("center".to_owned()),
            }),
            point: None,
        }),
        text: None,
        numeric_value: None,
        action_name: None,
        app: None,
        path: None,
        url: None,
        menu_path: None,
        title: None,
        wait_ms: None,
    });
}

fn target_for_node(
    node: &AccessibilityNodeRef,
    last_snapshot: Option<&SnapshotMeta>,
) -> ActionTarget {
    ActionTarget {
        node_id: Some(node.id.clone()),
        snapshot_id: last_snapshot.map(|snapshot| snapshot.snapshot_id.clone()),
        selector: None,
        role: None,
        name: None,
        nth: None,
        bounds_anchor: None,
        point: None,
    }
}

fn find_node_ref_for_suggestion<'a>(
    node_refs: &'a [AccessibilityNodeRef],
    node_id: &str,
) -> Option<&'a AccessibilityNodeRef> {
    node_refs.iter().find(|node| node.id == node_id)
}

fn dedupe_and_limit_suggestions(
    suggestions: Vec<SuggestedAction>,
    limit: usize,
) -> Vec<SuggestedAction> {
    let mut seen = std::collections::BTreeSet::new();
    let mut out = Vec::new();
    for suggestion in suggestions {
        let key = serde_json::to_string(&suggestion).unwrap_or_default();
        if !key.is_empty() && seen.insert(key) {
            out.push(suggestion);
        }
        if out.len() >= limit {
            break;
        }
    }
    out
}

fn action_result_llm_instruction(
    status: ComputerUseStatus,
    failure_class: Option<ComputerUseFailureClass>,
) -> &'static str {
    if status != ComputerUseStatus::Running {
        return match failure_class {
            Some(ComputerUseFailureClass::ActionNotSupported) => {
                "Session stopped because the requested action is not supported in this environment. Do not retry the same action; choose a different supported action or stop with a clear reason."
            }
            Some(ComputerUseFailureClass::InputSimulationUnavailable) => {
                "Session stopped because input simulation is unavailable. Do not retry input_* actions; choose a semantic accessibility action or stop with a clear reason."
            }
            Some(
                ComputerUseFailureClass::ElementNotFound | ComputerUseFailureClass::ElementStale,
            ) => {
                "Session stopped because the target was not found or became stale. Analyze stop_reason/failure_class and do not retry the same stale target blindly."
            }
            Some(_) => {
                "Session stopped. Analyze stop_reason/failure_class. Do not retry the same failed action blindly."
            }
            None => {
                "Session stopped. Analyze stop_reason/failure_class. Do not retry the same failed action blindly."
            }
        };
    }

    match failure_class {
        Some(ComputerUseFailureClass::ElementNotFound | ComputerUseFailureClass::ElementStale) => {
            "The target was not found or became stale. Request a new snapshot, re-resolve the target from the latest accessibility tree, then choose the next action."
        }
        Some(ComputerUseFailureClass::ActionNotSupported) => {
            "The requested action is not supported in this environment. Do not retry the same action; choose a different supported action or stop with a clear reason."
        }
        Some(ComputerUseFailureClass::InputSimulationUnavailable) => {
            "Input simulation is unavailable. Do not retry input_* actions; choose a semantic accessibility action or stop with a clear reason."
        }
        Some(ComputerUseFailureClass::AttachmentTransportFailure)
        | Some(ComputerUseFailureClass::ScreenshotUnavailable)
        | Some(ComputerUseFailureClass::ProviderTimeout)
        | Some(ComputerUseFailureClass::ProviderRateLimit) => {
            "The failure is recoverable. Request a fresh snapshot or retry after the transport/provider issue clears, then continue from the latest state."
        }
        Some(_) => {
            "Given the action failure_class, choose a different action only if the environment can satisfy it; otherwise stop with a clear reason."
        }
        None => {
            "Given the action result, request the next snapshot. Do not claim final success or call stop with outcome=completed until verify passes or the latest post-action snapshot includes completion_evidence."
        }
    }
}

fn app_handle_for_semantic_action(target: &ComputerUseTarget) -> Result<AppHandle, ToolError> {
    let app = match target {
        ComputerUseTarget::App { app, .. } | ComputerUseTarget::ActiveApp { app, .. } => app,
        ComputerUseTarget::Screen { .. } => {
            return Err(ToolError::invalid_arguments(
                "semantic computer_use actions require an app-targeted session; start with target.type app_name, app, pid, or active_app",
            ));
        }
    };
    Ok(AppHandle {
        identity_key: app.identity_key.clone(),
        name: app.name.clone(),
        pid: app.pid,
        role: app.role.clone(),
        window_title: app.window_title.clone(),
        bundle_id: app.bundle_id.clone(),
        localized_name: app.localized_name.clone(),
        executable_path: app.executable_path.clone(),
        frontmost: app.frontmost,
    })
}

fn app_target_update_for_os_action(
    action: &ComputerUseAction,
    execution: &ActionExecution,
    previous_target: &ComputerUseTarget,
    default_tree_max_depth: usize,
) -> Option<ComputerUseTarget> {
    let ComputerUseAction::Os(action) = action else {
        return None;
    };
    if !matches!(
        action.action_type,
        OsActionKind::OpenApp | OsActionKind::ActivateApp | OsActionKind::FocusWindow
    ) || execution.status != "ok"
    {
        return None;
    }
    let app = execution.app_after.clone()?;
    let tree_max_depth = match previous_target {
        ComputerUseTarget::App { tree_max_depth, .. }
        | ComputerUseTarget::ActiveApp { tree_max_depth, .. } => *tree_max_depth,
        ComputerUseTarget::Screen { .. } => default_tree_max_depth,
    };
    Some(ComputerUseTarget::App {
        requested: AppTarget {
            name: Some(app.name.clone()),
            pid: app.pid,
            identity_key: app.identity_key.clone(),
            bundle_id: app.bundle_id.clone(),
            executable_path: app.executable_path.clone(),
        },
        app,
        display: previous_target.display().clone(),
        tree_max_depth,
    })
}
