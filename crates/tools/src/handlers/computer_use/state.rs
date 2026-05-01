use super::model::ComputerUseSession;
use crate::context::ToolPayload;
use crate::error::ToolError;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

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
            let metadata = entry.metadata().ok()?;
            if !metadata.is_dir() {
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

pub(crate) fn parse_json_args<T: for<'de> Deserialize<'de>>(
    payload: ToolPayload,
) -> Result<T, ToolError> {
    match payload {
        ToolPayload::Function { arguments } => serde_json::from_value(arguments).map_err(|error| {
            ToolError::invalid_arguments(format!("failed to parse function arguments: {error}"))
        }),
        ToolPayload::Custom { input } => {
            serde_json::from_str::<T>(input.as_str()).map_err(|error| {
                ToolError::invalid_arguments(format!("failed to parse custom arguments: {error}"))
            })
        }
        other => Err(ToolError::invalid_arguments(format!(
            "unsupported payload for computer_use tool: {}",
            other.log_payload()
        ))),
    }
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

    let mut entries = scan_session_dirs(root)?;
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

    entries = scan_session_dirs(root)?;

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

fn scan_session_dirs(root: &Path) -> Result<Vec<SessionDirStats>, ToolError> {
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
        let metadata = match entry.metadata() {
            Ok(value) => value,
            Err(_) => continue,
        };
        if !metadata.is_dir() {
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
        let metadata = match entry.metadata() {
            Ok(value) => value,
            Err(_) => continue,
        };
        if metadata.is_dir() {
            total = total.saturating_add(measure_dir_size(path.as_path())?);
            continue;
        }
        total = total.saturating_add(metadata.len());
    }

    Ok(total)
}
