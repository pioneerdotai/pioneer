use super::super::handler::ComputerUseHandler;
use super::super::model::*;
use super::super::tree::{AccessibilityTreeBudget, absent_tree};
use super::super::util::{
    apply_snapshot_loop_guards, compute_hash, ensure_session_running, now_unix_ms,
    resolve_snapshot_destination, stop_session_with_reason,
};
use crate::apply_patch::file_mutation::{
    StagedFile, TargetExpectation, TargetResolver, TargetRole, ensure_parent_directories,
};
use crate::context::FunctionToolOutput;
use crate::error::ToolError;
use crate::events::ToolEventTrace;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

impl ComputerUseHandler {
    pub(crate) async fn handle_snapshot(
        &self,
        args: ComputerUseArgs,
        attempt_id: u32,
        trace: &ToolEventTrace,
    ) -> Result<FunctionToolOutput, ToolError> {
        let session_id = args.session_id.ok_or_else(|| {
            ToolError::invalid_arguments("computer_use snapshot requires session_id")
        })?;

        let (
            target,
            snapshot_target,
            snapshot_index,
            artifacts_dir,
            goal,
            snapshot_budget,
            previous_snapshot,
            previous_tree,
            previous_progress,
            last_action,
            last_verification,
            previous_verification_status,
        ) = {
            let mut manager = self.manager.lock().await;
            let session = manager.sessions.get_mut(&session_id).ok_or_else(|| {
                ToolError::NotFound(format!("computer_use session {} not found", session_id))
            })?;
            ensure_session_running(session)?;
            (
                session.target.clone(),
                session.target.snapshot_target(),
                session.snapshot_count.saturating_add(1),
                PathBuf::from(session.artifacts_dir.as_str()),
                session.goal.clone(),
                session.snapshot_budget.clone(),
                session.last_snapshot.clone(),
                session.last_accessibility_tree.clone(),
                session.last_progress_signals.clone(),
                session.last_action.clone(),
                session.last_verification.clone(),
                session.previous_verification_status.clone(),
            )
        };

        let frame = self
            .backend
            .screenshot(&snapshot_target, &snapshot_budget)?;
        let destination = resolve_snapshot_destination(
            artifacts_dir.as_path(),
            args.screenshot_path.as_deref(),
            snapshot_index,
        )?;
        let snapshots_root = artifacts_dir.join("snapshots");
        let relative_destination = destination
            .strip_prefix(snapshots_root.as_path())
            .map_err(|_| {
                ToolError::execution_failed(
                    "resolved computer_use snapshot escaped its session snapshots directory",
                )
            })?
            .to_path_buf();
        let snapshot_bytes = frame.png_bytes.clone();
        let write_root = snapshots_root.clone();
        tokio::task::spawn_blocking(move || {
            write_snapshot_secure(
                write_root.as_path(),
                relative_destination.as_path(),
                snapshot_bytes.as_slice(),
            )
        })
        .await
        .map_err(|error| ToolError::internal(format!("snapshot writer join error: {error}")))??;

        let snapshot = SnapshotMeta {
            index: snapshot_index,
            snapshot_id: format!("s{session_id}-{snapshot_index}"),
            path: destination.display().to_string(),
            width_px: frame.width_px,
            height_px: frame.height_px,
            transport_width_px: frame.transport_width_px,
            transport_height_px: frame.transport_height_px,
            scale_factor: frame.scale_factor,
            size_bytes: frame.png_bytes.len(),
            resize_passes: frame.resize_passes,
            captured_at_unix_ms: now_unix_ms(),
            state_hash: compute_hash(frame.png_bytes.as_slice()),
        };
        let (accessibility_tree, node_refs) = self.accessibility_tree_for_snapshot(&target);

        let progress_signals = compute_progress_signals(
            previous_snapshot.as_ref(),
            previous_tree.as_ref(),
            previous_progress.as_ref(),
            &accessibility_tree,
            node_refs.as_slice(),
            &target,
            last_action.as_ref(),
            last_verification.as_ref(),
            previous_verification_status,
            snapshot.state_hash.as_str(),
        );

        let (status, stop_reason, stop_failure_class, loop_guard, loop_state, completion_evidence) = {
            let mut manager = self.manager.lock().await;
            let session = manager.sessions.get_mut(&session_id).ok_or_else(|| {
                ToolError::NotFound(format!("computer_use session {} not found", session_id))
            })?;
            ensure_session_running(session)?;
            session.loop_state = ComputerUseLoopState::SnapshotCaptured;
            let was_awaiting_post_action_snapshot = session.awaiting_post_action_snapshot;
            if let Some(loop_guard_reason) = apply_snapshot_loop_guards(
                session,
                snapshot.state_hash.as_str(),
                progress_signals.no_progress,
            ) {
                stop_session_with_reason(
                    session,
                    format!("loop_guard:{loop_guard_reason}"),
                    ComputerUseFailureClass::LoopGuardTriggered,
                );
            }
            session.snapshot_count = snapshot_index;
            session.updated_at_unix_ms = now_unix_ms();
            session.last_snapshot = Some(snapshot.clone());
            session.last_accessibility_tree = Some(accessibility_tree.clone());
            session.last_node_refs = node_refs.clone();
            session.last_progress_signals = Some(progress_signals.clone());
            session.previous_verification_status = session
                .last_verification
                .as_ref()
                .map(|record| record.status.clone());
            if was_awaiting_post_action_snapshot {
                if let Some(evidence) = snapshot_completion_evidence(
                    progress_signals.clone(),
                    session.step_count,
                    snapshot_index,
                    session.updated_at_unix_ms,
                ) {
                    session.last_completion_evidence = Some(evidence);
                    session.last_evidence_at_step = Some(session.step_count);
                }
            }
            if session.status == ComputerUseStatus::Running {
                session.loop_state = ComputerUseLoopState::PlannerRequestBuilt;
            }
            (
                session.status,
                session.stop_reason.clone(),
                session.stop_failure_class,
                session.loop_guard.clone(),
                session.loop_state,
                session.last_completion_evidence.clone(),
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
            "step": snapshot_index,
            "status": status.as_str(),
            "loop_state": loop_state.as_str(),
            "stop_reason": stop_reason,
            "failure_class": stop_failure_class.map(|value| value.as_str().to_owned()),
            "snapshot": snapshot,
            "accessibility_tree": accessibility_tree,
            "progress_signals": progress_signals,
            "completion_evidence": completion_evidence,
            "loop_guard": loop_guard,
            "snapshot_budget": snapshot_budget,
        });
        payload["snapshot"]["width"] = payload["snapshot"]["width_px"].clone();
        payload["snapshot"]["height"] = payload["snapshot"]["height_px"].clone();
        payload["snapshot"]["timestamp"] = payload["snapshot"]["captured_at_unix_ms"].clone();
        payload["snapshot"]["transport_size"] = serde_json::json!({
            "width_px": payload["snapshot"]["transport_width_px"],
            "height_px": payload["snapshot"]["transport_height_px"],
        });
        payload["trace"] = serde_json::json!({
            "session_id": session_id,
            "snapshot_id": payload["snapshot"]["snapshot_id"],
            "snapshot_index": snapshot_index,
            "target": target_app_metadata(&target),
            "transport": {
                "source_width_px": payload["snapshot"]["width_px"],
                "source_height_px": payload["snapshot"]["height_px"],
                "transport_width_px": payload["snapshot"]["transport_width_px"],
                "transport_height_px": payload["snapshot"]["transport_height_px"],
                "resize_passes": payload["snapshot"]["resize_passes"],
                "size_bytes": payload["snapshot"]["size_bytes"],
            },
            "execution_status": status.as_str(),
            "failure_class": payload["failure_class"],
            "progress_signals": payload["progress_signals"],
            "completion_evidence": payload["completion_evidence"],
        });
        payload["accessibility"] = serde_json::json!({
            "target_app": target_app_metadata(&target),
            "snapshot_id": payload["snapshot"]["snapshot_id"],
            "tree_version": 1,
            "nodes": payload["accessibility_tree"]["nodes"],
            "truncated": payload["accessibility_tree"]["truncated"],
            "omitted_count": payload["accessibility_tree"]["omitted_count"],
            "status": payload["accessibility_tree"]["status"],
            "reason": payload["accessibility_tree"].get("reason").cloned().unwrap_or(serde_json::Value::Null),
        });
        if status == ComputerUseStatus::Running {
            payload["llm_context"] = serde_json::json!({
                "goal": goal,
                "instruction": "Use the accessibility tree first. When targeting by node_id, include target.snapshot_id from this latest snapshot and choose only an act.type listed in that node's supported_act_types; if the needed action is not listed, prefer another node or a higher-level OS action instead of guessing. Example: {\"action\":\"act\",\"session_id\":<id>,\"act\":{\"type\":\"press\",\"target\":{\"node_id\":\"n42\",\"snapshot_id\":\"s1-3\"}}}. Use selector/role/name targets when re-resolution is needed across snapshots. Use the PNG only to verify visual state. Explicit input_* actions are fallback only when semantic accessibility actions cannot express the operation. Do not claim final success or call stop with outcome=completed until verify passes or this latest snapshot includes completion_evidence.",
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
                },
                "coordinate_space": {
                    "source_width_px": payload["snapshot"]["width_px"],
                    "source_height_px": payload["snapshot"]["height_px"],
                    "transport_width_px": payload["snapshot"]["transport_width_px"],
                    "transport_height_px": payload["snapshot"]["transport_height_px"],
                    "scale_factor": payload["snapshot"]["scale_factor"],
                    "instruction": "Coordinates are fallback-only for explicit input_* actions. Prefer accessibility node targets. If you must use a point, include target.point.coordinate_space: source_pixels for original screenshot pixels, transport_pixels for the downscaled image sent to the LLM, logical_screen for accessibility/display logical coordinates, or native_input for backend input coordinates. The tool validates and reports requested_point -> converted native_input in action results."
                },
                "accessibility": payload["accessibility"],
                "accessibility_tree": payload["accessibility_tree"],
                "progress_signals": payload["progress_signals"],
                "trace": payload["trace"]
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

    fn accessibility_tree_for_snapshot(
        &self,
        target: &ComputerUseTarget,
    ) -> (AccessibilityTreePayload, Vec<AccessibilityNodeRef>) {
        let (app, requested_depth) = match target {
            ComputerUseTarget::App {
                app,
                tree_max_depth,
                ..
            }
            | ComputerUseTarget::ActiveApp {
                app,
                tree_max_depth,
                ..
            } => (app, *tree_max_depth),
            ComputerUseTarget::Screen { .. } => {
                return (
                    absent_tree("screen target has no app accessibility root yet"),
                    Vec::new(),
                );
            }
        };
        let budget = self.accessibility_tree_budget(requested_depth);
        let handle = AppHandle {
            identity_key: app.identity_key.clone(),
            name: app.name.clone(),
            pid: app.pid,
            role: app.role.clone(),
            window_title: app.window_title.clone(),
            bundle_id: app.bundle_id.clone(),
            localized_name: app.localized_name.clone(),
            executable_path: app.executable_path.clone(),
            frontmost: app.frontmost,
        };
        match self.backend.app_tree(&handle, budget) {
            Ok(tree) => (tree.payload, tree.node_refs),
            Err(error) => (
                absent_tree(format!("failed to capture accessibility tree: {error}")),
                Vec::new(),
            ),
        }
    }

    fn accessibility_tree_budget(&self, requested_depth: usize) -> AccessibilityTreeBudget {
        AccessibilityTreeBudget {
            max_depth: requested_depth
                .max(1)
                .min(self.config.accessibility_tree_max_depth),
            max_nodes: self.config.accessibility_tree_max_nodes,
            max_serialized_bytes: self.config.accessibility_tree_max_serialized_bytes,
            text_max_chars: self.config.accessibility_tree_text_max_chars,
        }
    }
}

fn write_snapshot_secure(
    snapshots_root: &Path,
    relative_destination: &Path,
    bytes: &[u8],
) -> Result<(), ToolError> {
    let resolver = TargetResolver::new(snapshots_root).map_err(|error| {
        ToolError::execution_failed(format!(
            "failed to anchor computer_use snapshot directory `{}`: {error}",
            snapshots_root.display()
        ))
    })?;
    let target = resolver
        .resolve(
            relative_destination.to_string_lossy().as_ref(),
            TargetRole::Destination,
            TargetExpectation::ExistingOrMissing,
        )
        .map_err(|error| {
            ToolError::execution_failed(format!(
                "refused unsafe computer_use snapshot destination `{}`: {error}",
                relative_destination.display()
            ))
        })?;
    ensure_parent_directories(&target).map_err(|failure| {
        ToolError::execution_failed(format!(
            "failed to securely create computer_use snapshot directories for `{}`: {}",
            relative_destination.display(),
            failure.source
        ))
    })?;
    let staged = StagedFile::create(&target).map_err(|error| {
        ToolError::execution_failed(format!(
            "failed to securely stage computer_use snapshot `{}`: {error}",
            relative_destination.display()
        ))
    })?;
    let mut file = staged.file().try_clone().map_err(|error| {
        ToolError::execution_failed(format!(
            "failed to prepare staged computer_use snapshot `{}`: {error}",
            relative_destination.display()
        ))
    })?;
    file.write_all(bytes).map_err(|error| {
        ToolError::execution_failed(format!(
            "failed to write staged computer_use snapshot `{}`: {error}",
            relative_destination.display()
        ))
    })?;
    file.flush().map_err(|error| {
        ToolError::execution_failed(format!(
            "failed to flush staged computer_use snapshot `{}`: {error}",
            relative_destination.display()
        ))
    })?;
    file.sync_all().map_err(|error| {
        ToolError::execution_failed(format!(
            "failed to sync staged computer_use snapshot `{}`: {error}",
            relative_destination.display()
        ))
    })?;
    drop(file);

    let published_parent = staged.publish_replace(false).map_err(|error| {
        ToolError::execution_failed(format!(
            "failed to publish computer_use snapshot `{}`: {}",
            relative_destination.display(),
            error.source
        ))
    })?;
    if published_parent.cleanup_failed {
        tracing::warn!(
            destination = %target.absolute().display(),
            "computer_use snapshot was published but its staging filename could not be removed"
        );
    }
    published_parent.sync_all().map_err(|error| {
        ToolError::execution_failed(format!(
            "computer_use snapshot `{}` was published but its parent directory could not be synced: {error}",
            relative_destination.display()
        ))
    })
}

fn snapshot_completion_evidence(
    progress_signals: ProgressSignals,
    step_count: u32,
    snapshot_index: u32,
    recorded_at_unix_ms: i64,
) -> Option<CompletionEvidence> {
    let target_exists = progress_signals.target_exists;
    let has_clear_state_change = progress_signals.has_meaningful_progress();
    if !target_exists || !has_clear_state_change {
        return None;
    }

    Some(CompletionEvidence {
        source: "post_action_snapshot".to_owned(),
        strength: "weak".to_owned(),
        summary: "post-action snapshot showed a concrete accessibility state change".to_owned(),
        evidence: serde_json::to_value(progress_signals).unwrap_or(serde_json::Value::Null),
        recorded_at_unix_ms,
        step_count,
        snapshot_index: Some(snapshot_index),
    })
}

fn compute_progress_signals(
    previous_snapshot: Option<&SnapshotMeta>,
    previous: Option<&AccessibilityTreePayload>,
    previous_progress: Option<&ProgressSignals>,
    current: &AccessibilityTreePayload,
    current_refs: &[AccessibilityNodeRef],
    target: &ComputerUseTarget,
    last_action: Option<&ActionRecord>,
    last_verification: Option<&VerifyRecord>,
    previous_verification_status: Option<String>,
    screenshot_hash: &str,
) -> ProgressSignals {
    let current_tree_hash = accessibility_tree_hash(current);
    let previous_tree_hash = previous.map(accessibility_tree_hash);
    let screenshot_hash = screenshot_hash.to_owned();
    let previous_screenshot_hash = previous_snapshot.map(|snapshot| snapshot.state_hash.clone());
    let current_target_exists = !current_refs.is_empty() || !current.nodes.is_empty();
    let previous_target_exists = previous.map(|tree| !tree.nodes.is_empty()).unwrap_or(false);
    let focused_node = state_node_signature(current, "focus");
    let previous_focused_node = previous.and_then(|tree| state_node_signature(tree, "focus"));
    let selected_node = state_node_signature(current, "selected");
    let previous_selected_node = previous.and_then(|tree| state_node_signature(tree, "selected"));
    let active_app = target_app_signature(target);
    let previous_active_app = previous_progress.and_then(|signals| signals.active_app.clone());
    let window_title = target_window_title_signature(target);
    let previous_window_title = previous_progress.and_then(|signals| signals.window_title.clone());
    let target_node_signature = action_target_node_signature(current, last_action);
    let previous_target_node_signature =
        previous.and_then(|tree| action_target_node_signature(tree, last_action));
    let verification_status = last_verification.map(|record| record.status.clone());
    let previous_verification_status = previous_verification_status
        .or_else(|| previous_progress.and_then(|signals| signals.verification_status.clone()));

    let screenshot_hash_changed = previous_screenshot_hash
        .as_ref()
        .map(|hash| hash != &screenshot_hash)
        .unwrap_or(false);
    let tree_hash_changed = previous_tree_hash
        .as_ref()
        .map(|hash| hash != &current_tree_hash)
        .unwrap_or(false);
    let focused_node_changed =
        previous.map(|_| focused_node.as_deref() != previous_focused_node.as_deref());
    let selected_node_changed =
        previous.map(|_| selected_node.as_deref() != previous_selected_node.as_deref());
    let active_app_changed =
        previous_progress.map(|_| active_app.as_deref() != previous_active_app.as_deref());
    let window_title_changed =
        previous_progress.map(|_| window_title.as_deref() != previous_window_title.as_deref());
    let target_node_changed = previous
        .map(|_| target_node_signature.as_deref() != previous_target_node_signature.as_deref());
    let verification_failed_to_passed = previous_verification_status.as_deref() == Some("failed")
        && verification_status.as_deref() == Some("passed");

    let mut changed_signals = Vec::new();
    push_changed(
        &mut changed_signals,
        "screenshot_hash",
        screenshot_hash_changed,
    );
    push_changed(&mut changed_signals, "tree_hash", tree_hash_changed);
    push_changed(
        &mut changed_signals,
        "focused_node",
        focused_node_changed.unwrap_or(false),
    );
    push_changed(
        &mut changed_signals,
        "selected_node",
        selected_node_changed.unwrap_or(false),
    );
    push_changed(
        &mut changed_signals,
        "active_app",
        active_app_changed.unwrap_or(false),
    );
    push_changed(
        &mut changed_signals,
        "window_title",
        window_title_changed.unwrap_or(false),
    );
    push_changed(
        &mut changed_signals,
        "target_node",
        target_node_changed.unwrap_or(false),
    );
    push_changed(
        &mut changed_signals,
        "verification_failed_to_passed",
        verification_failed_to_passed,
    );

    let meaningful_progress = tree_hash_changed
        || focused_node_changed.unwrap_or(false)
        || selected_node_changed.unwrap_or(false)
        || active_app_changed.unwrap_or(false)
        || window_title_changed.unwrap_or(false)
        || target_node_changed.unwrap_or(false)
        || verification_failed_to_passed;

    ProgressSignals {
        screenshot_hash,
        previous_screenshot_hash,
        screenshot_hash_changed,
        tree_hash: current_tree_hash,
        previous_tree_hash,
        tree_hash_changed,
        target_exists: current_target_exists,
        target_disappeared: previous_target_exists && !current_target_exists,
        focused_node,
        previous_focused_node,
        focused_node_changed,
        selected_node,
        previous_selected_node,
        selected_node_changed,
        active_app,
        previous_active_app,
        active_app_changed,
        window_title,
        previous_window_title,
        window_title_changed,
        target_node_signature,
        previous_target_node_signature,
        target_node_changed,
        verification_status,
        previous_verification_status,
        verification_failed_to_passed,
        no_progress: !meaningful_progress,
        changed_signals,
    }
}

fn accessibility_tree_hash(tree: &AccessibilityTreePayload) -> String {
    serde_json::to_vec(tree)
        .map(|bytes| compute_hash(bytes.as_slice()))
        .unwrap_or_else(|_| compute_hash(format!("{tree:?}").as_bytes()))
}

fn state_node_signature(tree: &AccessibilityTreePayload, state_fragment: &str) -> Option<String> {
    tree.nodes
        .iter()
        .find(|node| {
            node.states
                .iter()
                .any(|state| state.to_ascii_lowercase().contains(state_fragment))
        })
        .map(compact_node_signature)
}

fn compact_node_signature(node: &CompactAccessibilityNode) -> String {
    format!(
        "{}:{}:{:?}:{:?}:{:?}",
        node.id, node.role, node.name, node.value, node.states
    )
}

fn action_target_node_signature(
    tree: &AccessibilityTreePayload,
    last_action: Option<&ActionRecord>,
) -> Option<String> {
    let target = last_action
        .and_then(|record| record.payload.get("target"))
        .and_then(|value| serde_json::from_value::<ActionTarget>(value.clone()).ok())?;
    let node = if let Some(node_id) = target.node_id.as_deref() {
        tree.nodes.iter().find(|node| node.id == node_id)
    } else if let Some(selector) = target.selector.as_deref() {
        tree.nodes.iter().find(|node| {
            node.selector_hints
                .iter()
                .any(|hint| hint.as_str() == selector)
        })
    } else if let Some(name) = target.name.as_deref() {
        tree.nodes.iter().find(|node| {
            node.name
                .as_deref()
                .map(|actual| actual == name)
                .unwrap_or(false)
        })
    } else {
        None
    }?;
    Some(compact_node_signature(node))
}

fn target_app_signature(target: &ComputerUseTarget) -> Option<String> {
    match target {
        ComputerUseTarget::Screen { .. } => None,
        ComputerUseTarget::App { app, .. } | ComputerUseTarget::ActiveApp { app, .. } => {
            Some(format!("{}:{:?}", app.name, app.pid))
        }
    }
}

fn target_window_title_signature(target: &ComputerUseTarget) -> Option<String> {
    match target {
        ComputerUseTarget::Screen { .. } => None,
        ComputerUseTarget::App { app, .. } | ComputerUseTarget::ActiveApp { app, .. } => {
            app.window_title.clone()
        }
    }
}

fn push_changed(target: &mut Vec<String>, name: &str, changed: bool) {
    if changed {
        target.push(name.to_owned());
    }
}

fn target_app_metadata(target: &ComputerUseTarget) -> serde_json::Value {
    match target {
        ComputerUseTarget::App { app, .. } | ComputerUseTarget::ActiveApp { app, .. } => {
            serde_json::json!({
                "name": app.name.clone(),
                "pid": app.pid,
                "role": app.role.clone(),
                "window_title": app.window_title.clone(),
            })
        }
        ComputerUseTarget::Screen { .. } => serde_json::Value::Null,
    }
}
