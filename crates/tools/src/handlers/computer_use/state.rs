use super::model::{ComputerUseArgs, ComputerUseSession};
use crate::context::ToolPayload;
use crate::error::ToolError;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

fn active_artifact_directories() -> &'static Mutex<HashSet<PathBuf>> {
    static ACTIVE: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();
    ACTIVE.get_or_init(|| Mutex::new(HashSet::new()))
}

#[derive(Debug)]
pub(crate) struct ActiveArtifactLease {
    path: PathBuf,
}

impl ActiveArtifactLease {
    pub(crate) fn reserve_directory(path: PathBuf) -> std::io::Result<Arc<Self>> {
        let mut active = active_artifact_directories()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        fs::create_dir(path.as_path())?;
        active.insert(path.clone());
        Ok(Arc::new(Self { path }))
    }
}

impl Drop for ActiveArtifactLease {
    fn drop(&mut self) {
        active_artifact_directories()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.path);
    }
}

#[derive(Debug, Default)]
pub(crate) struct ComputerUseSessionManager {
    pub(crate) next_session_id: u64,
    pub(crate) sessions: HashMap<u64, ComputerUseSession>,
}

impl ComputerUseSessionManager {
    pub(crate) fn from_artifacts_root(root: &Path) -> Self {
        Self {
            next_session_id: discover_max_session_id(root),
            ..Self::default()
        }
    }
}

fn discover_max_session_id(root: &Path) -> u64 {
    let _ = fs::create_dir_all(root);

    let entries = match fs::read_dir(root) {
        Ok(value) => value,
        Err(_) => return 0,
    };

    entries
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let file_type = entry.file_type().ok()?;
            if !file_type.is_dir() || file_type.is_symlink() {
                return None;
            }
            entry
                .file_name()
                .to_str()
                .and_then(|value| value.parse::<u64>().ok())
        })
        .max()
        .unwrap_or(0)
}

const COMPUTER_USE_ACCEPTED_SHAPE: &str =
    r#"{"action":"start","goal":"...","target":{"type":"app_name","name":"ExampleApp"}}"#;
const COMPUTER_USE_VALID_EXAMPLE: &str = r#"{"action":"act","session_id":1,"act":{"type":"press","target":{"node_id":"n42","snapshot_id":"s1-1"}}}"#;
const TOP_LEVEL_FIELDS: &[&str] = &[
    "action",
    "session_id",
    "goal",
    "target",
    "display_id",
    "launch_if_missing",
    "launch_command",
    "activation_timeout_ms",
    "tree_max_depth",
    "screenshot_path",
    "act",
    "max_steps",
    "timeout_ms",
    "planner_provider",
    "planner_model",
    "snapshot_max_bytes",
    "snapshot_max_side_px",
    "recovery_attempt",
    "failure_class",
    "outcome",
    "reason",
    "expect",
];
const TARGET_FIELDS: &[&str] = &[
    "type",
    "name",
    "pid",
    "identity_key",
    "bundle_id",
    "executable_path",
    "display_id",
    "launch_if_missing",
    "launch_command",
    "activation_timeout_ms",
    "tree_max_depth",
];
const ACT_FIELDS: &[&str] = &[
    "type",
    "target",
    "from",
    "to",
    "button",
    "delta_x",
    "delta_y",
    "text",
    "keys",
    "numeric_value",
    "action_name",
    "condition",
    "wait_ms",
    "app",
    "path",
    "url",
    "menu_path",
    "title",
];
const ACTION_TARGET_FIELDS: &[&str] = &[
    "node_id",
    "snapshot_id",
    "selector",
    "role",
    "name",
    "nth",
    "bounds_anchor",
    "point",
];
const BOUNDS_ANCHOR_FIELDS: &[&str] = &["node_id", "snapshot_id", "anchor"];
const INPUT_POINT_FIELDS: &[&str] = &["x", "y", "coordinate_space"];
const VERIFY_EXPECT_FIELDS: &[&str] = &[
    "app",
    "window_title",
    "visible_text",
    "node",
    "snapshot_hash_changed",
];
const VERIFY_NODE_FIELDS: &[&str] = &["node_id", "selector", "role", "name"];

pub(crate) fn parse_computer_use_args(payload: ToolPayload) -> Result<ComputerUseArgs, ToolError> {
    let value = match payload {
        ToolPayload::Function { arguments } => arguments,
        ToolPayload::Custom { input } => serde_json::from_str::<JsonValue>(input.as_str())
            .map_err(|error| argument_contract_error("$", format!("invalid JSON: {error}")))?,
        other => {
            return Err(ToolError::invalid_arguments(format!(
                "unsupported payload for computer_use tool: {}",
                other.log_payload()
            )));
        }
    };
    validate_computer_use_json_contract(&value)?;
    serde_json::from_value(value).map_err(|error| {
        argument_contract_error(
            "$",
            format!("failed to parse computer_use arguments: {error}"),
        )
    })
}

fn validate_computer_use_json_contract(value: &JsonValue) -> Result<(), ToolError> {
    validate_object_fields(value, "$", TOP_LEVEL_FIELDS)?;
    validate_optional_object_fields(value.get("target"), "$.target", TARGET_FIELDS)?;
    if let Some(act) = value.get("act") {
        validate_object_fields(act, "$.act", ACT_FIELDS)?;
        validate_action_target_fields(act.get("target"), "$.act.target")?;
        validate_action_target_fields(act.get("from"), "$.act.from")?;
        validate_action_target_fields(act.get("to"), "$.act.to")?;
    }
    if let Some(expect) = value.get("expect") {
        validate_object_fields(expect, "$.expect", VERIFY_EXPECT_FIELDS)?;
        validate_optional_object_fields(expect.get("node"), "$.expect.node", VERIFY_NODE_FIELDS)?;
    }
    Ok(())
}

fn validate_action_target_fields(value: Option<&JsonValue>, path: &str) -> Result<(), ToolError> {
    let Some(value) = value else {
        return Ok(());
    };
    validate_object_fields(value, path, ACTION_TARGET_FIELDS)?;
    if let Some(bounds_anchor) = value.get("bounds_anchor") {
        validate_object_fields(
            bounds_anchor,
            &format!("{path}.bounds_anchor"),
            BOUNDS_ANCHOR_FIELDS,
        )?;
    }
    if let Some(point) = value.get("point") {
        validate_object_fields(point, &format!("{path}.point"), INPUT_POINT_FIELDS)?;
    }
    Ok(())
}

fn validate_optional_object_fields(
    value: Option<&JsonValue>,
    path: &str,
    accepted: &[&str],
) -> Result<(), ToolError> {
    if let Some(value) = value {
        validate_object_fields(value, path, accepted)?;
    }
    Ok(())
}

fn validate_object_fields(
    value: &JsonValue,
    path: &str,
    accepted: &[&str],
) -> Result<(), ToolError> {
    let object = value.as_object().ok_or_else(|| {
        argument_contract_error(
            path,
            format!("expected object, got {}", json_type_name(value)),
        )
    })?;
    for key in object.keys() {
        if !accepted.contains(&key.as_str()) {
            return Err(argument_contract_error(
                &format!("{path}.{key}"),
                format!("unknown field `{key}`"),
            ));
        }
    }
    Ok(())
}

fn json_type_name(value: &JsonValue) -> &'static str {
    match value {
        JsonValue::Null => "null",
        JsonValue::Bool(_) => "boolean",
        JsonValue::Number(_) => "number",
        JsonValue::String(_) => "string",
        JsonValue::Array(_) => "array",
        JsonValue::Object(_) => "object",
    }
}

pub(crate) fn argument_contract_error(path: &str, message: impl Into<String>) -> ToolError {
    ToolError::invalid_arguments(format!(
        "invalid computer_use arguments at {path}: {}. Accepted shape: {COMPUTER_USE_ACCEPTED_SHAPE}. Example: {COMPUTER_USE_VALID_EXAMPLE}",
        message.into()
    ))
}

pub(crate) fn cleanup_artifacts_sync(
    root: &Path,
    retention_hours: u64,
    max_total_bytes: u64,
) -> Result<(), ToolError> {
    fs::create_dir_all(root).map_err(|error| {
        ToolError::execution_failed(format!(
            "failed to create computer_use artifacts root `{}`: {error}",
            root.display()
        ))
    })?;
    // Reservation holds the same registry lock while it creates and leases a
    // directory. Keeping it for the complete scan/remove cycle closes the
    // otherwise exploitable create-before-lease cleanup race.
    let active = active_artifact_directories()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);

    let mut entries = scan_session_dirs(root, &active)?;
    if entries.is_empty() {
        return Ok(());
    }

    let cutoff = std::time::SystemTime::now()
        .checked_sub(std::time::Duration::from_secs(
            retention_hours.saturating_mul(3600),
        ))
        .unwrap_or(std::time::UNIX_EPOCH);

    for entry in &entries {
        if entry.modified < cutoff {
            let _ = fs::remove_dir_all(entry.path.as_path());
        }
    }

    entries = scan_session_dirs(root, &active)?;

    let mut total_bytes = entries
        .iter()
        .fold(0u64, |acc, entry| acc.saturating_add(entry.size_bytes));
    if total_bytes <= max_total_bytes {
        return Ok(());
    }

    entries.sort_by_key(|entry| entry.modified);

    for entry in entries {
        if total_bytes <= max_total_bytes {
            break;
        }
        if fs::remove_dir_all(entry.path.as_path()).is_ok() {
            total_bytes = total_bytes.saturating_sub(entry.size_bytes);
        }
    }

    Ok(())
}

#[derive(Debug)]
struct SessionDirStats {
    path: PathBuf,
    modified: std::time::SystemTime,
    size_bytes: u64,
}

fn scan_session_dirs(
    root: &Path,
    active: &HashSet<PathBuf>,
) -> Result<Vec<SessionDirStats>, ToolError> {
    let mut sessions = Vec::new();
    let entries = fs::read_dir(root).map_err(|error| {
        ToolError::execution_failed(format!(
            "failed to read computer_use artifacts root `{}`: {error}",
            root.display()
        ))
    })?;

    for entry in entries {
        let entry = entry.map_err(|error| {
            ToolError::execution_failed(format!(
                "failed to enumerate computer_use artifacts in `{}`: {error}",
                root.display()
            ))
        })?;
        let path = entry.path();
        let metadata = match fs::symlink_metadata(path.as_path()) {
            Ok(value) => value,
            Err(_) => continue,
        };
        if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
            continue;
        }
        if active.contains(path.as_path()) {
            continue;
        }

        let size_bytes = measure_dir_size(path.as_path())?;
        sessions.push(SessionDirStats {
            path,
            modified: metadata.modified().unwrap_or(std::time::UNIX_EPOCH),
            size_bytes,
        });
    }

    Ok(sessions)
}

fn measure_dir_size(dir: &Path) -> Result<u64, ToolError> {
    let mut total = 0u64;
    let entries = fs::read_dir(dir).map_err(|error| {
        ToolError::execution_failed(format!(
            "failed to read computer_use directory `{}`: {error}",
            dir.display()
        ))
    })?;

    for entry in entries {
        let entry = entry.map_err(|error| {
            ToolError::execution_failed(format!(
                "failed to iterate computer_use directory `{}`: {error}",
                dir.display()
            ))
        })?;
        let path = entry.path();
        let metadata = match fs::symlink_metadata(path.as_path()) {
            Ok(value) => value,
            Err(_) => continue,
        };
        let file_type = metadata.file_type();
        if file_type.is_symlink() {
            continue;
        }
        if file_type.is_dir() {
            total = total.saturating_add(measure_dir_size(path.as_path())?);
            continue;
        }
        if file_type.is_file() {
            total = total.saturating_add(metadata.len());
        }
    }

    Ok(total)
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;

    #[test]
    fn discovery_and_cleanup_do_not_follow_artifact_symlinks() {
        let root = tempfile::tempdir().expect("artifact root");
        let outside = tempfile::tempdir().expect("outside root");
        fs::write(outside.path().join("secret.bin"), vec![7u8; 1024 * 1024])
            .expect("outside payload");

        symlink(outside.path(), root.path().join(u64::MAX.to_string()))
            .expect("numeric session symlink");
        assert_eq!(discover_max_session_id(root.path()), 0);

        let session = root.path().join("1");
        fs::create_dir(&session).expect("session");
        fs::write(session.join("frame.png"), b"frame").expect("frame");
        symlink(outside.path(), session.join("escape")).expect("nested symlink");

        assert_eq!(measure_dir_size(&session).expect("measure session"), 5);
        cleanup_artifacts_sync(root.path(), u64::MAX, 5).expect("cleanup artifacts");
        assert!(outside.path().join("secret.bin").is_file());
    }
}
