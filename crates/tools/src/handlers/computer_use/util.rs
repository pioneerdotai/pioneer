use super::model::{
    ComputerUseAction, ComputerUseFailureClass, ComputerUseLoopState, ComputerUseSession,
    ComputerUseStatus, LoopGuardState, SnapshotBudget,
};
use crate::ComputerUseToolsConfig;
use crate::error::ToolError;
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
        Some("anthropic") => ("anthropic_remote", 5 * 1024 * 1024, 1120),
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

pub(crate) fn resolved_action_signature(action: &ComputerUseAction) -> String {
    serde_json::to_string(action).unwrap_or_else(|_| action.action_type().to_owned())
}

pub(crate) fn apply_action_loop_guards(
    session: &mut ComputerUseSession,
    action: &ComputerUseAction,
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
    no_progress_after_action: bool,
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
        if no_progress_after_action {
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
