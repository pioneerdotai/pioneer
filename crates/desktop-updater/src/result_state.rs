use crate::{
    cleanup::update_root_from_plan_path,
    plan::{DesktopUpdatePlan, PLAN_PRODUCT, PLAN_SCHEMA_VERSION},
};
use anyhow::{Context as _, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    fs,
    io::Write as _,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

pub const APPLY_RESULT_FILE_NAME: &str = "result.json";
pub const APPLIED_RECEIPT_SCHEMA_VERSION: u32 = 1;
pub const RELAUNCH_RECEIPT_DIR_NAME: &str = "relaunch";
pub const PENDING_APPLIED_RECEIPT_FILE_NAME: &str = "pending.json";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AppliedReceiptStatus {
    Applied,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApplyTimingState {
    pub process_exited_at_unix_ms: u64,
    pub apply_started_at_unix_ms: u64,
    pub apply_completed_at_unix_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedUpdateReceipt {
    pub schema_version: u32,
    pub product: String,
    pub status: AppliedReceiptStatus,
    pub attempt_id: String,
    pub from_version: String,
    pub to_version: String,
    pub platform: String,
    pub relaunch_requested_at_unix_ms: u64,
    pub process_exited_at_unix_ms: u64,
    pub apply_started_at_unix_ms: u64,
    pub apply_completed_at_unix_ms: u64,
    pub receipt_written_at_unix_ms: u64,
    pub details: Option<Value>,
}

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

pub fn write_applied_receipt_for_plan_path(
    plan_path: &Path,
    plan: &DesktopUpdatePlan,
    timings: ApplyTimingState,
    details: Option<Value>,
) -> Result<AppliedUpdateReceipt> {
    if timings.process_exited_at_unix_ms < plan.relaunch_requested_at_unix_ms
        || timings.apply_started_at_unix_ms < timings.process_exited_at_unix_ms
        || timings.apply_completed_at_unix_ms < timings.apply_started_at_unix_ms
    {
        bail!("desktop update apply timings are not monotonic");
    }

    let receipt_written_at_unix_ms = current_unix_timestamp_ms();
    if receipt_written_at_unix_ms < timings.apply_completed_at_unix_ms {
        bail!("desktop update receipt timestamp precedes apply completion");
    }
    let receipt = AppliedUpdateReceipt {
        schema_version: APPLIED_RECEIPT_SCHEMA_VERSION,
        product: PLAN_PRODUCT.to_owned(),
        status: AppliedReceiptStatus::Applied,
        attempt_id: plan.attempt_id.clone(),
        from_version: plan.current_version.clone(),
        to_version: plan.target_version.clone(),
        platform: plan.os.clone(),
        relaunch_requested_at_unix_ms: plan.relaunch_requested_at_unix_ms,
        process_exited_at_unix_ms: timings.process_exited_at_unix_ms,
        apply_started_at_unix_ms: timings.apply_started_at_unix_ms,
        apply_completed_at_unix_ms: timings.apply_completed_at_unix_ms,
        receipt_written_at_unix_ms,
        details,
    };
    let path = pending_applied_receipt_path(plan_path)?;
    let bytes = serde_json::to_vec_pretty(&receipt)
        .context("failed to serialize desktop update applied receipt")?;
    write_file_atomic(path.as_path(), bytes.as_slice())?;
    Ok(receipt)
}

pub fn pending_applied_receipt_path(plan_path: &Path) -> Result<PathBuf> {
    Ok(update_root_from_plan_path(plan_path)?
        .join(RELAUNCH_RECEIPT_DIR_NAME)
        .join(PENDING_APPLIED_RECEIPT_FILE_NAME))
}

fn write_file_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("desktop update state path has no parent"))?;
    fs::create_dir_all(parent).with_context(|| {
        format!(
            "failed to create desktop update state directory `{}`",
            parent.display()
        )
    })?;

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

fn current_unix_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::{
        APPLY_RESULT_FILE_NAME, ApplyResultStatus, ApplyTimingState,
        PENDING_APPLIED_RECEIPT_FILE_NAME, RELAUNCH_RECEIPT_DIR_NAME, current_unix_timestamp_ms,
        pending_applied_receipt_path, result_path_for_plan_path,
        write_applied_receipt_for_plan_path, write_apply_result_for_plan_path,
    };
    use crate::plan::{
        DesktopUpdatePlan, PLAN_PRODUCT, PLAN_SCHEMA_VERSION, current_plan_arch, current_plan_os,
        expected_asset_kind_for_os,
    };
    use std::{fs, path::PathBuf};

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

    #[test]
    fn applied_receipt_is_written_outside_staging() {
        let temp_dir = tempfile::tempdir().unwrap();
        let update_root = temp_dir.path().join("desktop-updates");
        let plan_dir = update_root.join("staging").join("attempt");
        fs::create_dir_all(plan_dir.as_path()).unwrap();
        let plan_path = plan_dir.join("plan.json");
        let plan = valid_plan(update_root.join("downloads").join("asset.bin"));

        let receipt = write_applied_receipt_for_plan_path(
            plan_path.as_path(),
            &plan,
            ApplyTimingState {
                process_exited_at_unix_ms: plan.relaunch_requested_at_unix_ms + 10,
                apply_started_at_unix_ms: plan.relaunch_requested_at_unix_ms + 20,
                apply_completed_at_unix_ms: plan.relaunch_requested_at_unix_ms + 30,
            },
            None,
        )
        .unwrap();

        let expected = update_root
            .join(RELAUNCH_RECEIPT_DIR_NAME)
            .join(PENDING_APPLIED_RECEIPT_FILE_NAME);
        assert_eq!(pending_applied_receipt_path(&plan_path).unwrap(), expected);
        assert!(expected.is_file());
        assert_eq!(receipt.attempt_id, plan.attempt_id);
    }

    fn valid_plan(asset_path: PathBuf) -> DesktopUpdatePlan {
        let os = current_plan_os();
        DesktopUpdatePlan {
            schema_version: PLAN_SCHEMA_VERSION,
            product: PLAN_PRODUCT.to_owned(),
            attempt_id: "A1b2C3d4E5f6G7h8I9j0K".to_owned(),
            relaunch_requested_at_unix_ms: current_unix_timestamp_ms().saturating_sub(100),
            target_version: "0.26.0".to_owned(),
            current_version: "0.25.0".to_owned(),
            tag: "v0.26.0".to_owned(),
            os: os.to_owned(),
            arch: current_plan_arch().unwrap().to_owned(),
            asset_kind: expected_asset_kind_for_os(os).unwrap().to_owned(),
            asset_path,
            asset_name: "asset.bin".to_owned(),
            asset_sha256: "a".repeat(64),
            current_pid: 123,
            current_exe_path: temp_absolute("current/pioneer-app"),
            install_root_path: temp_absolute("install/Pioneer.app"),
            appimage_path: None,
            restart_after_apply: true,
        }
    }

    fn temp_absolute(relative: &str) -> PathBuf {
        std::env::temp_dir()
            .join("pioneer-receipt-test")
            .join(relative)
    }
}
