use super::model::{
    ComputerUseActArgs, ComputerUseFailureClass, ComputerUseLoopState, ComputerUseSession,
    ComputerUseStatus, DisplayMeta, LoopGuardState, MAX_TEXT_CHARS, MouseButtonKind,
    ResolvedAction, SnapshotBudget,
};
use crate::ComputerUseToolsConfig;
use crate::error::ToolError;
use enigo::{Button, Key};
use sha2::{Digest, Sha256};
use std::path::{Component, Path, PathBuf};

pub(crate) fn ensure_session_running(session: &mut ComputerUseSession) -> Result<(), ToolError> {
    if session.status != ComputerUseStatus::Running {
        return Err(ToolError::invalid_arguments(format!(
            "computer_use session {} is not running (status={})",
            session.session_id,
            session.status.as_str()
        )));
    }

    if session.step_count >= session.max_steps {
        stop_session_with_reason(
            session,
            "max_steps_exceeded",
            ComputerUseFailureClass::RuntimeActionError,
        );
        return Err(ToolError::execution_failed(format!(
            "computer_use session {} stopped: max_steps_exceeded",
            session.session_id
        )));
    }

    let elapsed_ms = session.created_at_mono.elapsed().as_millis() as u64;
    if elapsed_ms > session.timeout_ms {
        stop_session_with_reason(
            session,
            "timeout",
            ComputerUseFailureClass::RuntimeActionError,
        );
        return Err(ToolError::execution_failed(format!(
            "computer_use session {} stopped: timeout",
            session.session_id
        )));
    }

    Ok(())
}

pub(crate) fn resolve_snapshot_budget(
    config: &ComputerUseToolsConfig,
    provider_hint: Option<&str>,
    model_hint: Option<&str>,
    max_bytes_override: Option<usize>,
    max_side_override: Option<u32>,
) -> SnapshotBudget {
    let provider = provider_hint
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_ascii_lowercase);
    let model = model_hint
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_ascii_lowercase);

    let (profile, default_max_bytes, default_max_side_px) = match provider.as_deref() {
        Some("anthropic") | Some("claude-code") => ("anthropic_remote", 5 * 1024 * 1024, 1120),
        Some("gemini") | Some("google") | Some("google-gemini") => {
            ("gemini_remote", 7 * 1024 * 1024, 1280)
        }
        Some("openai") | Some("azure_openai") | Some("azure-openai") => {
            ("openai_remote", 8 * 1024 * 1024, 1280)
        }
        Some("bedrock") | Some("aws-bedrock") => ("bedrock_remote", 8 * 1024 * 1024, 1280),
        Some("openrouter") => ("openrouter_remote", 8 * 1024 * 1024, 1280),
        Some("ollama") => ("ollama_remote", 8 * 1024 * 1024, 1280),
        _ => ("default_remote", 8 * 1024 * 1024, 1280),
    };

    let max_bytes = max_bytes_override
        .unwrap_or(default_max_bytes)
        .min(config.snapshot_transport_max_bytes)
        .max(256 * 1024);
    let max_side_px = max_side_override
        .unwrap_or(default_max_side_px)
        .min(config.snapshot_transport_max_side_px)
        .max(config.snapshot_transport_min_side_px);

    SnapshotBudget {
        provider_hint: provider,
        model_hint: model,
        profile: profile.to_owned(),
        max_bytes,
        max_side_px,
        min_side_px: config.snapshot_transport_min_side_px.min(max_side_px),
        downscale_factor: config.snapshot_downscale_factor,
    }
}

pub(crate) fn default_loop_guard(config: &ComputerUseToolsConfig) -> LoopGuardState {
    LoopGuardState {
        consecutive_same_snapshot_hash: 0,
        consecutive_same_action_signature: 0,
        consecutive_no_progress_steps: 0,
        max_same_snapshot_hash: config.max_consecutive_same_snapshot_hash,
        max_same_action_signature: config.max_consecutive_same_action_signature,
        max_no_progress_steps: config.max_consecutive_no_progress_steps,
    }
}

pub(crate) fn resolved_action_signature(action: &ResolvedAction) -> String {
    serde_json::to_string(action).unwrap_or_else(|_| action.action_type().to_owned())
}

pub(crate) fn apply_action_loop_guards(
    session: &mut ComputerUseSession,
    action: &ResolvedAction,
) -> Option<String> {
    let signature = resolved_action_signature(action);
    let next = if session
        .last_action_signature
        .as_ref()
        .map(|value| value == signature.as_str())
        .unwrap_or(false)
    {
        session
            .loop_guard
            .consecutive_same_action_signature
            .saturating_add(1)
    } else {
        1
    };
    session.loop_guard.consecutive_same_action_signature = next;
    session.last_action_signature = Some(signature);

    if session.loop_guard.consecutive_same_action_signature
        > session.loop_guard.max_same_action_signature
    {
        return Some("same_action_signature_limit".to_owned());
    }
    None
}

pub(crate) fn apply_snapshot_loop_guards(
    session: &mut ComputerUseSession,
    new_snapshot_hash: &str,
) -> Option<String> {
    let previous_hash = session
        .last_snapshot
        .as_ref()
        .map(|snapshot| snapshot.state_hash.as_str());

    if previous_hash == Some(new_snapshot_hash) {
        session.loop_guard.consecutive_same_snapshot_hash = session
            .loop_guard
            .consecutive_same_snapshot_hash
            .saturating_add(1)
    } else {
        session.loop_guard.consecutive_same_snapshot_hash = 1;
    }

    if session.awaiting_post_action_snapshot {
        if previous_hash == Some(new_snapshot_hash) {
            session.loop_guard.consecutive_no_progress_steps = session
                .loop_guard
                .consecutive_no_progress_steps
                .saturating_add(1);
        } else {
            session.loop_guard.consecutive_no_progress_steps = 0;
        }
        session.awaiting_post_action_snapshot = false;
    }

    if session.loop_guard.consecutive_same_snapshot_hash > session.loop_guard.max_same_snapshot_hash
    {
        return Some("same_snapshot_hash_limit".to_owned());
    }

    if session.loop_guard.consecutive_no_progress_steps > session.loop_guard.max_no_progress_steps {
        return Some("no_progress_limit".to_owned());
    }

    None
}

pub(crate) fn apply_recovery_signal(
    session: &mut ComputerUseSession,
    recovery_attempt: Option<u32>,
    failure_class: Option<ComputerUseFailureClass>,
) -> Option<String> {
    if failure_class
        .map(ComputerUseFailureClass::is_retryable)
        .unwrap_or(false)
    {
        session.recovery_attempts_current_step =
            session.recovery_attempts_current_step.saturating_add(1);
        session.recovery_attempts_run = session.recovery_attempts_run.saturating_add(1);
    }

    if let Some(attempt) = recovery_attempt {
        session.recovery_attempts_current_step =
            session.recovery_attempts_current_step.max(attempt);
        session.recovery_attempts_run = session.recovery_attempts_run.max(attempt);
    }

    if session.recovery_attempts_current_step > session.max_recovery_attempts_per_step {
        return Some("max_recovery_attempts_per_step_exceeded".to_owned());
    }
    if session.recovery_attempts_run > session.max_recovery_attempts_per_run {
        return Some("max_recovery_attempts_per_run_exceeded".to_owned());
    }

    None
}

pub(crate) fn stop_session_with_reason(
    session: &mut ComputerUseSession,
    reason: impl Into<String>,
    failure_class: ComputerUseFailureClass,
) {
    stop_session_with_failure_state(session, reason, failure_class, ComputerUseLoopState::Failed);
}

pub(crate) fn stop_session_with_failure_state(
    session: &mut ComputerUseSession,
    reason: impl Into<String>,
    failure_class: ComputerUseFailureClass,
    state: ComputerUseLoopState,
) {
    session.status = ComputerUseStatus::Stopped;
    session.loop_state = state;
    session.stop_reason = Some(reason.into());
    session.stop_failure_class = Some(failure_class);
    session.updated_at_unix_ms = now_unix_ms();
}

pub(crate) fn stop_session_as_completed(
    session: &mut ComputerUseSession,
    reason: impl Into<String>,
) {
    session.status = ComputerUseStatus::Stopped;
    session.loop_state = ComputerUseLoopState::Completed;
    session.stop_reason = Some(reason.into());
    session.stop_failure_class = None;
    session.updated_at_unix_ms = now_unix_ms();
}

pub(crate) fn stop_session_as_stopped(session: &mut ComputerUseSession, reason: impl Into<String>) {
    session.status = ComputerUseStatus::Stopped;
    session.loop_state = ComputerUseLoopState::Stopped;
    session.stop_reason = Some(reason.into());
    session.stop_failure_class = None;
    session.updated_at_unix_ms = now_unix_ms();
}

pub(crate) fn resolve_snapshot_destination(
    artifacts_dir: &Path,
    requested: Option<&str>,
    snapshot_index: u32,
) -> Result<PathBuf, ToolError> {
    let snapshots_dir = artifacts_dir.join("snapshots");
    if let Some(path) = requested {
        let trimmed = path.trim();
        if trimmed.is_empty() {
            return Ok(snapshots_dir.join(format!("{}.png", snapshot_index)));
        }
        let relative = PathBuf::from(trimmed);
        if relative.is_absolute() || relative.components().any(is_disallowed_path_component) {
            return Err(ToolError::invalid_arguments(
                "screenshot_path must be a relative path within session snapshots directory",
            ));
        }
        Ok(snapshots_dir.join(relative))
    } else {
        Ok(snapshots_dir.join(format!("{}.png", snapshot_index)))
    }
}

fn is_disallowed_path_component(component: Component<'_>) -> bool {
    matches!(
        component,
        Component::ParentDir | Component::RootDir | Component::Prefix(_)
    )
}

pub(crate) fn resolve_action(
    act: ComputerUseActArgs,
    display: &DisplayMeta,
) -> Result<ResolvedAction, ToolError> {
    let kind = act.kind.trim().to_lowercase();
    match kind.as_str() {
        "move" => {
            let (x, y) = resolve_absolute_coordinates(display, act.x_norm, act.y_norm)?;
            Ok(ResolvedAction::Move { x, y })
        }
        "click" => {
            let (x, y) = resolve_absolute_coordinates(display, act.x_norm, act.y_norm)?;
            Ok(ResolvedAction::Click {
                x,
                y,
                button: parse_mouse_button(act.button.as_deref())?,
                click_count: 1,
            })
        }
        "double_click" => {
            let (x, y) = resolve_absolute_coordinates(display, act.x_norm, act.y_norm)?;
            Ok(ResolvedAction::Click {
                x,
                y,
                button: parse_mouse_button(act.button.as_deref())?,
                click_count: 2,
            })
        }
        "right_click" => {
            let (x, y) = resolve_absolute_coordinates(display, act.x_norm, act.y_norm)?;
            Ok(ResolvedAction::Click {
                x,
                y,
                button: MouseButtonKind::Right,
                click_count: 1,
            })
        }
        "scroll" => {
            let delta_x = act.delta_x.unwrap_or(0);
            let delta_y = act.delta_y.unwrap_or(0);
            if delta_x == 0 && delta_y == 0 {
                return Err(ToolError::invalid_arguments(
                    "scroll requires non-zero delta_x or delta_y",
                ));
            }
            Ok(ResolvedAction::Scroll { delta_x, delta_y })
        }
        "type_text" => {
            let text = act.text.unwrap_or_default().trim().to_owned();
            if text.is_empty() {
                return Err(ToolError::invalid_arguments(
                    "type_text requires non-empty text",
                ));
            }
            if text.chars().count() > MAX_TEXT_CHARS {
                return Err(ToolError::invalid_arguments(format!(
                    "text exceeds max length of {} characters",
                    MAX_TEXT_CHARS
                )));
            }
            Ok(ResolvedAction::TypeText { text })
        }
        "hotkey" => {
            let keys = act.keys.unwrap_or_default();
            if keys.is_empty() {
                return Err(ToolError::invalid_arguments(
                    "hotkey requires at least one key",
                ));
            }
            if keys.len() > 5 {
                return Err(ToolError::invalid_arguments(
                    "hotkey supports at most 5 keys",
                ));
            }
            Ok(ResolvedAction::Hotkey { keys })
        }
        "wait" => {
            let wait_ms = act.wait_ms.unwrap_or(250).clamp(1, 60_000);
            Ok(ResolvedAction::Wait { wait_ms })
        }
        _ => Err(ToolError::invalid_arguments(format!(
            "unsupported act.type `{}`; supported: click, double_click, right_click, move, scroll, type_text, hotkey, wait",
            kind
        ))),
    }
}

fn parse_mouse_button(button: Option<&str>) -> Result<MouseButtonKind, ToolError> {
    match button.map(str::trim).filter(|value| !value.is_empty()) {
        None => Ok(MouseButtonKind::Left),
        Some(value) => match value.to_lowercase().as_str() {
            "left" => Ok(MouseButtonKind::Left),
            "right" => Ok(MouseButtonKind::Right),
            "middle" => Ok(MouseButtonKind::Middle),
            other => Err(ToolError::invalid_arguments(format!(
                "unsupported mouse button `{}`",
                other
            ))),
        },
    }
}

pub(crate) fn to_enigo_button(button: MouseButtonKind) -> Button {
    match button {
        MouseButtonKind::Left => Button::Left,
        MouseButtonKind::Right => Button::Right,
        MouseButtonKind::Middle => Button::Middle,
    }
}

pub(crate) fn resolve_absolute_coordinates(
    display: &DisplayMeta,
    x_norm: Option<f64>,
    y_norm: Option<f64>,
) -> Result<(i32, i32), ToolError> {
    let x_norm = x_norm.ok_or_else(|| ToolError::invalid_arguments("x_norm is required"))?;
    let y_norm = y_norm.ok_or_else(|| ToolError::invalid_arguments("y_norm is required"))?;

    if !x_norm.is_finite() || !y_norm.is_finite() {
        return Err(ToolError::invalid_arguments(
            "x_norm and y_norm must be finite numbers",
        ));
    }
    if !(0.0..=1.0).contains(&x_norm) || !(0.0..=1.0).contains(&y_norm) {
        return Err(ToolError::invalid_arguments(
            "x_norm and y_norm must be between 0 and 1",
        ));
    }
    if display.width_px == 0 || display.height_px == 0 {
        return Err(ToolError::execution_failed(format!(
            "display {} has invalid dimensions {}x{}",
            display.display_id, display.width_px, display.height_px
        )));
    }

    let local_x = (x_norm * f64::from(display.width_px.saturating_sub(1))).round() as i32;
    let local_y = (y_norm * f64::from(display.height_px.saturating_sub(1))).round() as i32;
    let global_x = display.origin_x.saturating_add(local_x);
    let global_y = display.origin_y.saturating_add(local_y);
    Ok((global_x, global_y))
}

pub(crate) fn parse_hotkey_key(value: &str) -> Result<Key, ToolError> {
    let normalized = normalize_hotkey_token(value);
    let key = match normalized.as_str() {
        "ctrl" => Key::Control,
        "shift" => Key::Shift,
        "alt" => Key::Alt,
        "meta" => Key::Meta,
        "enter" => Key::Return,
        "tab" => Key::Tab,
        "space" => Key::Space,
        "esc" => Key::Escape,
        "backspace" => Key::Backspace,
        "delete" => Key::Delete,
        "up" => Key::UpArrow,
        "down" => Key::DownArrow,
        "left" => Key::LeftArrow,
        "right" => Key::RightArrow,
        "home" => Key::Home,
        "end" => Key::End,
        "pageup" => Key::PageUp,
        "pagedown" => Key::PageDown,
        "f1" => Key::F1,
        "f2" => Key::F2,
        "f3" => Key::F3,
        "f4" => Key::F4,
        "f5" => Key::F5,
        "f6" => Key::F6,
        "f7" => Key::F7,
        "f8" => Key::F8,
        "f9" => Key::F9,
        "f10" => Key::F10,
        "f11" => Key::F11,
        "f12" => Key::F12,
        "f13" => Key::F13,
        "f14" => Key::F14,
        "f15" => Key::F15,
        "f16" => Key::F16,
        "f17" => Key::F17,
        "f18" => Key::F18,
        "f19" => Key::F19,
        "f20" => Key::F20,
        _ if normalized.chars().count() == 1 => {
            Key::Unicode(normalized.chars().next().expect("single char"))
        }
        _ => {
            return Err(ToolError::invalid_arguments(format!(
                "unsupported hotkey token `{}`",
                value
            )));
        }
    };
    Ok(key)
}

fn normalize_hotkey_token(value: &str) -> String {
    match value.trim().to_lowercase().as_str() {
        "control" | "ctrl" => "ctrl".to_owned(),
        "option" | "alt" => "alt".to_owned(),
        "command" | "cmd" | "meta" | "super" | "win" | "windows" => "meta".to_owned(),
        "return" | "enter" => "enter".to_owned(),
        "escape" | "esc" => "esc".to_owned(),
        "del" | "delete" => "delete".to_owned(),
        "pgup" | "page_up" => "pageup".to_owned(),
        "pgdn" | "page_down" => "pagedown".to_owned(),
        other => other.to_owned(),
    }
}

pub(crate) fn compute_hash(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

pub(crate) fn now_unix_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_budget_resolves_provider_profile() {
        let config = ComputerUseToolsConfig::default();
        let budget = resolve_snapshot_budget(
            &config,
            Some("anthropic"),
            Some("claude-sonnet"),
            None,
            None,
        );
        assert_eq!(budget.profile, "anthropic_remote");
        assert_eq!(budget.max_bytes, 5 * 1024 * 1024);
        assert_eq!(budget.max_side_px, 1120);
    }

    #[test]
    fn snapshot_budget_overrides_are_clamped_to_config_limits() {
        let config = ComputerUseToolsConfig {
            snapshot_transport_max_bytes: 2 * 1024 * 1024,
            snapshot_transport_max_side_px: 900,
            snapshot_transport_min_side_px: 300,
            ..ComputerUseToolsConfig::default()
        };
        let budget = resolve_snapshot_budget(
            &config,
            Some("openai"),
            Some("gpt-5"),
            Some(16 * 1024 * 1024),
            Some(2048),
        );
        assert_eq!(budget.max_bytes, 2 * 1024 * 1024);
        assert_eq!(budget.max_side_px, 900);
        assert_eq!(budget.min_side_px, 300);
    }
}
