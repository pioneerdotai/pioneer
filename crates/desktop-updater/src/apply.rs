use crate::{
    cleanup,
    plan::{self, DesktopUpdatePlan},
    platform::{self, PlatformApplyOutcome},
    process,
    result_state::{self, ApplyResultStatus, ApplyTimingState},
};
use anyhow::Result;
use std::{
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub const PROCESS_EXIT_TIMEOUT: Duration = Duration::from_secs(60);
pub const PROCESS_POLL_INTERVAL: Duration = Duration::from_millis(250);

pub fn apply_plan(plan_path: &Path) -> Result<()> {
    apply_plan_inner(
        plan_path,
        |pid| process::wait_for_process_exit(pid, PROCESS_EXIT_TIMEOUT, PROCESS_POLL_INTERVAL),
        platform::apply_validated_plan,
        platform::relaunch,
    )
}

fn apply_plan_inner(
    plan_path: &Path,
    wait_for_exit: impl FnOnce(u32) -> Result<(), process::ProcessWaitError>,
    apply_platform: impl FnOnce(&DesktopUpdatePlan, &Path) -> Result<PlatformApplyOutcome>,
    relaunch_platform: impl FnOnce(&PlatformApplyOutcome) -> Result<()>,
) -> Result<()> {
    let validated = match plan::read_and_validate_plan(plan_path) {
        Ok(validated) => validated,
        Err(error) => {
            record_failure_best_effort(
                plan_path,
                None,
                error.code().as_str(),
                error.to_string().as_str(),
            );
            return Err(error.into());
        }
    };

    if let Err(error) = wait_for_exit(validated.plan.current_pid) {
        record_failure_best_effort(
            plan_path,
            Some(&validated.plan),
            error.code().as_str(),
            error.to_string().as_str(),
        );
        return Err(error.into());
    }
    let process_exited_at_unix_ms = current_unix_timestamp_ms();
    let apply_started_at_unix_ms = current_unix_timestamp_ms();

    let platform_outcome = match apply_platform(&validated.plan, plan_path) {
        Ok(outcome) => outcome,
        Err(error) => {
            record_failure_best_effort(
                plan_path,
                Some(&validated.plan),
                "platform_apply",
                format!("{error:#}").as_str(),
            );
            return Err(error);
        }
    };
    let apply_completed_at_unix_ms = current_unix_timestamp_ms();

    record_success_best_effort(
        plan_path,
        &validated.plan,
        ApplyTimingState {
            process_exited_at_unix_ms,
            apply_started_at_unix_ms,
            apply_completed_at_unix_ms,
        },
        platform_outcome.result_details.clone(),
    );
    cleanup_success_best_effort(plan_path, &validated.plan);
    if validated.plan.restart_after_apply
        && let Err(error) = relaunch_platform(&platform_outcome)
    {
        record_failure_best_effort(
            plan_path,
            Some(&validated.plan),
            "relaunch",
            format!("{error:#}").as_str(),
        );
        return Err(error);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{apply_plan_inner, current_unix_timestamp_ms};
    use crate::{
        plan::{
            DesktopUpdatePlan, PLAN_PRODUCT, PLAN_SCHEMA_VERSION, current_plan_arch,
            current_plan_os, expected_asset_kind_for_os, sha256_file,
        },
        platform::PlatformApplyOutcome,
        process::ProcessWaitError,
        result_state::{
            APPLY_RESULT_FILE_NAME, AppliedUpdateReceipt, ApplyResultState, ApplyResultStatus,
            pending_applied_receipt_path,
        },
    };
    use anyhow::anyhow;
    use std::{cell::Cell, fs, path::PathBuf};

    #[test]
    fn apply_records_invalid_plan_fields() {
        let temp_dir = tempfile::tempdir().unwrap();
        let asset_path = write_asset(temp_dir.path(), b"asset");
        let mut plan = valid_plan(asset_path.clone(), sha256_file(&asset_path).unwrap());
        plan.target_version.clear();
        let plan_path = write_plan(temp_dir.path(), &plan);

        let error = apply_plan_inner(
            plan_path.as_path(),
            |_| Ok(()),
            |_, _| Ok(PlatformApplyOutcome::default()),
            |_| Ok(()),
        )
        .unwrap_err();

        assert!(error.to_string().contains("empty required field"));
        let result = read_result(temp_dir.path());
        assert_eq!(result.status, ApplyResultStatus::Failure);
        assert_eq!(result.code, "empty_field");
    }

    #[test]
    fn sha_mismatch_prevents_wait_and_apply() {
        let temp_dir = tempfile::tempdir().unwrap();
        let asset_path = write_asset(temp_dir.path(), b"actual");
        let plan = valid_plan(
            asset_path,
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
        );
        let plan_path = write_plan(temp_dir.path(), &plan);
        let wait_called = Cell::new(false);
        let apply_called = Cell::new(false);

        let error = apply_plan_inner(
            plan_path.as_path(),
            |_| {
                wait_called.set(true);
                Ok(())
            },
            |_, _| {
                apply_called.set(true);
                Ok(PlatformApplyOutcome::default())
            },
            |_| Ok(()),
        )
        .unwrap_err();

        assert!(error.to_string().contains("SHA256 mismatch"));
        assert!(!wait_called.get());
        assert!(!apply_called.get());
        assert_eq!(read_result(temp_dir.path()).code, "sha256_mismatch");
    }

    #[test]
    fn process_timeout_prevents_platform_apply() {
        let temp_dir = tempfile::tempdir().unwrap();
        let asset_path = write_asset(temp_dir.path(), b"asset");
        let plan = valid_plan(asset_path.clone(), sha256_file(&asset_path).unwrap());
        let plan_path = write_plan(temp_dir.path(), &plan);
        let apply_called = Cell::new(false);

        let error = apply_plan_inner(
            plan_path.as_path(),
            |pid| Err(ProcessWaitError::timeout(pid)),
            |_, _| {
                apply_called.set(true);
                Ok(PlatformApplyOutcome::default())
            },
            |_| Ok(()),
        )
        .unwrap_err();

        assert!(error.to_string().contains("did not exit"));
        assert!(!apply_called.get());
        assert_eq!(read_result(temp_dir.path()).code, "process_exit_timeout");
    }

    #[test]
    fn platform_apply_failure_is_recorded() {
        let temp_dir = tempfile::tempdir().unwrap();
        let asset_path = write_asset(temp_dir.path(), b"asset");
        let plan = valid_plan(asset_path.clone(), sha256_file(&asset_path).unwrap());
        let plan_path = write_plan(temp_dir.path(), &plan);

        let error = apply_plan_inner(
            plan_path.as_path(),
            |_| Ok(()),
            |_, _| Err(anyhow!("platform exploded")),
            |_| Ok(()),
        )
        .unwrap_err();

        assert!(error.to_string().contains("platform exploded"));
        let result = read_result(temp_dir.path());
        assert_eq!(result.status, ApplyResultStatus::Failure);
        assert_eq!(result.code, "platform_apply");
    }

    #[test]
    fn applied_receipt_and_cleanup_complete_before_relaunch() {
        let temp_dir = tempfile::tempdir().unwrap();
        let update_root = temp_dir.path().join("desktop-updates");
        let downloads_dir = update_root.join("downloads");
        let plan_dir = update_root.join("staging").join("attempt");
        fs::create_dir_all(downloads_dir.as_path()).unwrap();
        fs::create_dir_all(plan_dir.as_path()).unwrap();
        let asset_path = write_asset(downloads_dir.as_path(), b"asset");
        let mut plan = valid_plan(asset_path.clone(), sha256_file(&asset_path).unwrap());
        plan.relaunch_requested_at_unix_ms = current_unix_timestamp_ms().saturating_sub(1_000);
        let plan_path = write_plan(plan_dir.as_path(), &plan);
        fs::write(update_root.join("state.json"), b"ready").unwrap();
        let receipt_path = pending_applied_receipt_path(plan_path.as_path()).unwrap();
        let relaunched = Cell::new(false);

        apply_plan_inner(
            plan_path.as_path(),
            |_| Ok(()),
            |_, _| Ok(PlatformApplyOutcome::default()),
            |_| {
                let receipt: AppliedUpdateReceipt =
                    serde_json::from_slice(fs::read(receipt_path.as_path()).unwrap().as_slice())
                        .unwrap();
                assert_eq!(receipt.attempt_id, plan.attempt_id);
                assert!(!update_root.join("state.json").exists());
                assert_eq!(fs::read_dir(downloads_dir.as_path()).unwrap().count(), 0);
                relaunched.set(true);
                Ok(())
            },
        )
        .unwrap();

        assert!(relaunched.get());
    }

    fn write_asset(dir: &std::path::Path, bytes: &[u8]) -> PathBuf {
        let asset_path = dir.join("asset.bin");
        fs::write(asset_path.as_path(), bytes).unwrap();
        asset_path
    }

    fn write_plan(dir: &std::path::Path, plan: &DesktopUpdatePlan) -> PathBuf {
        let plan_path = dir.join("plan.json");
        fs::write(
            plan_path.as_path(),
            serde_json::to_vec_pretty(plan).unwrap(),
        )
        .unwrap();
        plan_path
    }

    fn read_result(dir: &std::path::Path) -> ApplyResultState {
        let result_path = dir.join(APPLY_RESULT_FILE_NAME);
        serde_json::from_slice(fs::read(result_path).unwrap().as_slice()).unwrap()
    }

    fn valid_plan(asset_path: PathBuf, asset_sha256: String) -> DesktopUpdatePlan {
        let os = current_plan_os();
        let arch = current_plan_arch().expect("test host architecture is supported");
        let asset_name = asset_path
            .file_name()
            .expect("test asset has file name")
            .to_string_lossy()
            .into_owned();

        DesktopUpdatePlan {
            schema_version: PLAN_SCHEMA_VERSION,
            product: PLAN_PRODUCT.to_owned(),
            attempt_id: "A1b2C3d4E5f6G7h8I9j0K".to_owned(),
            relaunch_requested_at_unix_ms: 1_789_100_000_000,
            target_version: "0.26.0".to_owned(),
            current_version: "0.25.0".to_owned(),
            tag: "v0.26.0".to_owned(),
            os: os.to_owned(),
            arch: arch.to_owned(),
            asset_kind: expected_asset_kind_for_os(os)
                .expect("test host OS is supported")
                .to_owned(),
            asset_path,
            asset_name,
            asset_sha256,
            current_pid: 12345,
            current_exe_path: absolute_fixture_path("current/pioneer-app"),
            install_root_path: absolute_fixture_path("install-root"),
            appimage_path: None,
            restart_after_apply: true,
        }
    }

    fn absolute_fixture_path(relative: &str) -> PathBuf {
        std::env::temp_dir()
            .join("pioneer-app-updater-test")
            .join(relative)
    }
}

fn record_success_best_effort(
    plan_path: &Path,
    plan: &DesktopUpdatePlan,
    timings: ApplyTimingState,
    details: Option<serde_json::Value>,
) {
    if let Err(error) =
        result_state::write_applied_receipt_for_plan_path(plan_path, plan, timings, details)
    {
        eprintln!("failed to write desktop update applied receipt: {error:#}");
    }
}

fn cleanup_success_best_effort(plan_path: &Path, plan: &DesktopUpdatePlan) {
    if let Err(error) = cleanup::cleanup_successful_apply(plan_path, plan) {
        eprintln!("failed to clean successful desktop update cache: {error:#}");
    }
}

fn record_failure_best_effort(
    plan_path: &Path,
    plan: Option<&DesktopUpdatePlan>,
    code: &str,
    message: &str,
) {
    let target_version = plan.map(|plan| plan.target_version.as_str());
    if let Err(error) = result_state::write_apply_result_for_plan_path(
        plan_path,
        target_version,
        ApplyResultStatus::Failure,
        code,
        message,
    ) {
        eprintln!("failed to write desktop update result state: {error:#}");
    }
}

fn current_unix_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or_default()
}
