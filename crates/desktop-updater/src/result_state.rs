use crate::plan::{PLAN_PRODUCT, PLAN_SCHEMA_VERSION};
use anyhow::{Context as _, Result, anyhow};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    fs,
    io::Write as _,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

pub const APPLY_RESULT_FILE_NAME: &str = "result.json";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApplyResultStatus {
    Success,
    Failure,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplyResultState {
    pub schema_version: u32,
    pub product: String,
    pub status: ApplyResultStatus,
    pub code: String,
    pub message: String,
    pub target_version: Option<String>,
    pub recorded_at_unix: u64,
    pub plan_path: PathBuf,
    pub details: Option<Value>,
}

pub fn write_apply_result_for_plan_path(
    plan_path: &Path,
    target_version: Option<&str>,
    status: ApplyResultStatus,
    code: &str,
    message: &str,
) -> Result<ApplyResultState> {
    write_apply_result_for_plan_path_with_details(
        plan_path,
        target_version,
        status,
        code,
        message,
        None,
    )
}

pub fn write_apply_result_for_plan_path_with_details(
    plan_path: &Path,
    target_version: Option<&str>,
    status: ApplyResultStatus,
    code: &str,
    message: &str,
    details: Option<Value>,
) -> Result<ApplyResultState> {
    let result_path = result_path_for_plan_path(plan_path)?;
    let state = ApplyResultState {
        schema_version: PLAN_SCHEMA_VERSION,
        product: PLAN_PRODUCT.to_owned(),
        status,
        code: code.to_owned(),
        message: message.to_owned(),
        target_version: target_version.map(str::to_owned),
        recorded_at_unix: current_unix_timestamp(),
        plan_path: plan_path.to_path_buf(),
        details,
    };

    write_apply_result(result_path.as_path(), &state)?;
    Ok(state)
}

pub fn result_path_for_plan_path(plan_path: &Path) -> Result<PathBuf> {
    let parent = plan_path
        .parent()
        .ok_or_else(|| anyhow!("desktop update plan path has no parent"))?;
    Ok(parent.join(APPLY_RESULT_FILE_NAME))
}

pub fn write_apply_result(path: &Path, state: &ApplyResultState) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("desktop update result path has no parent"))?;
    fs::create_dir_all(parent).with_context(|| {
        format!(
            "failed to create desktop update result directory `{}`",
            parent.display()
        )
    })?;

    let bytes = serde_json::to_vec_pretty(state)
        .context("failed to serialize desktop update result state")?;
    write_file_atomic(path, bytes.as_slice())
}

fn write_file_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let file_name = path
        .file_name()
        .and_then(|file_name| file_name.to_str())
        .unwrap_or(APPLY_RESULT_FILE_NAME);
    let tmp_path = path.with_file_name(format!(".{file_name}.tmp-{}", std::process::id()));

    let mut tmp_file = fs::File::create(tmp_path.as_path()).with_context(|| {
        format!(
            "failed to create temporary desktop update result `{}`",
            tmp_path.display()
        )
    })?;
    tmp_file.write_all(bytes).with_context(|| {
        format!(
            "failed to write temporary desktop update result `{}`",
            tmp_path.display()
        )
    })?;
    tmp_file.flush().with_context(|| {
        format!(
            "failed to flush temporary desktop update result `{}`",
            tmp_path.display()
        )
    })?;
    drop(tmp_file);

    #[cfg(windows)]
    if path.exists() {
        fs::remove_file(path).with_context(|| {
            format!(
                "failed to replace existing desktop update result `{}`",
                path.display()
            )
        })?;
    }

    fs::rename(tmp_path.as_path(), path).with_context(|| {
        let _ = fs::remove_file(tmp_path.as_path());
        format!(
            "failed to finalize desktop update result `{}`",
            path.display()
        )
    })
}

fn current_unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::{
        APPLY_RESULT_FILE_NAME, ApplyResultStatus, result_path_for_plan_path,
        write_apply_result_for_plan_path,
    };
    use std::fs;

    #[test]
    fn result_state_is_written_next_to_plan() {
        let temp_dir = tempfile::tempdir().unwrap();
        let plan_path = temp_dir.path().join("plan.json");

        let state = write_apply_result_for_plan_path(
            plan_path.as_path(),
            Some("0.26.0"),
            ApplyResultStatus::Failure,
            "process_exit_timeout",
            "timeout",
        )
        .unwrap();

        let result_path = temp_dir.path().join(APPLY_RESULT_FILE_NAME);
        assert_eq!(
            result_path_for_plan_path(plan_path.as_path()).unwrap(),
            result_path
        );
        assert!(result_path.is_file());
        assert_eq!(state.status, ApplyResultStatus::Failure);
        assert!(
            fs::read_to_string(result_path)
                .unwrap()
                .contains("process_exit_timeout")
        );
    }
}
