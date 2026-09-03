use super::{download::DESKTOP_UPDATES_DIR, manifest::DESKTOP_UPDATE_PRODUCT};
use anyhow::{Context as _, Result, bail};
use semver::Version;
use serde::Deserialize;
use serde_json::Value;
use std::{
    fs,
    io::ErrorKind,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const APPLIED_RECEIPT_SCHEMA_VERSION: u32 = 1;
const UPDATE_ATTEMPT_ID_LEN: usize = 21;
const RELAUNCH_RECEIPT_DIR_NAME: &str = "relaunch";
const PENDING_APPLIED_RECEIPT_FILE_NAME: &str = "pending.json";
const APPLIED_RECEIPT_MAX_AGE: Duration = Duration::from_secs(30 * 60);
const APPLIED_RECEIPT_MAX_CLOCK_SKEW: Duration = Duration::from_secs(5 * 60);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum AppliedReceiptStatus {
    Applied,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct AppliedUpdateReceipt {
    schema_version: u32,
    product: String,
    status: AppliedReceiptStatus,
    attempt_id: String,
    from_version: String,
    to_version: String,
    platform: String,
    relaunch_requested_at_unix_ms: u64,
    process_exited_at_unix_ms: u64,
    apply_started_at_unix_ms: u64,
    apply_completed_at_unix_ms: u64,
    receipt_written_at_unix_ms: u64,
    #[allow(dead_code)]
    details: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DesktopPostUpdateReceipt {
    pub(crate) attempt_id: String,
    pub(crate) from_version: String,
    pub(crate) to_version: String,
    pub(crate) platform: String,
    pub(crate) process_exit_wait: Duration,
    pub(crate) apply_duration: Duration,
    pub(crate) relaunch_duration: Duration,
    pub(crate) total_duration: Duration,
    pub(crate) claimed_at: SystemTime,
}

pub(crate) fn claim_post_update_receipt(
    runtime_home: &Path,
    current_version: &str,
) -> Result<Option<DesktopPostUpdateReceipt>> {
    claim_post_update_receipt_at(runtime_home, current_version, current_unix_timestamp_ms())
}

fn claim_post_update_receipt_at(
    runtime_home: &Path,
    current_version: &str,
    now_unix_ms: u64,
) -> Result<Option<DesktopPostUpdateReceipt>> {
    let receipt_dir = runtime_home
        .join(DESKTOP_UPDATES_DIR)
        .join(RELAUNCH_RECEIPT_DIR_NAME);
    let pending_path = receipt_dir.join(PENDING_APPLIED_RECEIPT_FILE_NAME);
    if !pending_path.is_file() {
        return Ok(None);
    }

    let claimed_path = claimed_receipt_path(receipt_dir.as_path(), now_unix_ms);
    match fs::rename(pending_path.as_path(), claimed_path.as_path()) {
        Ok(()) => {}
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "failed to atomically claim desktop update receipt `{}`",
                    pending_path.display()
                )
            });
        }
    }

    let claimed =
        read_and_validate_claimed_receipt(claimed_path.as_path(), current_version, now_unix_ms);
    let _ = fs::remove_file(claimed_path.as_path());
    claimed.map(Some)
}

fn read_and_validate_claimed_receipt(
    path: &Path,
    current_version: &str,
    now_unix_ms: u64,
) -> Result<DesktopPostUpdateReceipt> {
    let bytes = fs::read(path)
        .with_context(|| format!("failed to read desktop update receipt `{}`", path.display()))?;
    let receipt: AppliedUpdateReceipt =
        serde_json::from_slice(bytes.as_slice()).with_context(|| {
            format!(
                "failed to parse desktop update receipt `{}`",
                path.display()
            )
        })?;

    if receipt.schema_version != APPLIED_RECEIPT_SCHEMA_VERSION
        || receipt.product != DESKTOP_UPDATE_PRODUCT
        || receipt.status != AppliedReceiptStatus::Applied
    {
        bail!("unsupported desktop update receipt schema, product, or status");
    }
    if receipt.attempt_id.len() != UPDATE_ATTEMPT_ID_LEN
        || !receipt
            .attempt_id
            .chars()
            .all(|character| character.is_ascii_alphanumeric())
    {
        bail!("desktop update receipt has an invalid attempt identity");
    }
    if receipt.platform != std::env::consts::OS {
        bail!("desktop update receipt platform does not match this application");
    }

    let from_version = Version::parse(receipt.from_version.trim())
        .context("desktop update receipt has an invalid source version")?;
    let to_version = Version::parse(receipt.to_version.trim())
        .context("desktop update receipt has an invalid target version")?;
    let running_version = Version::parse(current_version.trim())
        .context("running desktop application has an invalid version")?;
    if to_version != running_version || from_version >= to_version {
        bail!("desktop update receipt does not describe this application upgrade");
    }

    let ordered = receipt.relaunch_requested_at_unix_ms <= receipt.process_exited_at_unix_ms
        && receipt.process_exited_at_unix_ms <= receipt.apply_started_at_unix_ms
        && receipt.apply_started_at_unix_ms <= receipt.apply_completed_at_unix_ms
        && receipt.apply_completed_at_unix_ms <= receipt.receipt_written_at_unix_ms;
    if !ordered {
        bail!("desktop update receipt timings are not monotonic");
    }
    let max_future_unix_ms =
        now_unix_ms.saturating_add(duration_millis(APPLIED_RECEIPT_MAX_CLOCK_SKEW));
    if receipt.receipt_written_at_unix_ms > max_future_unix_ms
        || now_unix_ms.saturating_sub(receipt.receipt_written_at_unix_ms)
            > duration_millis(APPLIED_RECEIPT_MAX_AGE)
    {
        bail!("desktop update receipt is stale or from the future");
    }

    Ok(DesktopPostUpdateReceipt {
        attempt_id: receipt.attempt_id,
        from_version: receipt.from_version,
        to_version: receipt.to_version,
        platform: receipt.platform,
        process_exit_wait: millis_duration(
            receipt
                .process_exited_at_unix_ms
                .saturating_sub(receipt.relaunch_requested_at_unix_ms),
        ),
        apply_duration: millis_duration(
            receipt
                .apply_completed_at_unix_ms
                .saturating_sub(receipt.apply_started_at_unix_ms),
        ),
        relaunch_duration: millis_duration(
            now_unix_ms.saturating_sub(receipt.receipt_written_at_unix_ms),
        ),
        total_duration: millis_duration(
            now_unix_ms.saturating_sub(receipt.relaunch_requested_at_unix_ms),
        ),
        claimed_at: UNIX_EPOCH
            .checked_add(millis_duration(now_unix_ms))
            .context("desktop update receipt claim timestamp is out of range")?,
    })
}

fn claimed_receipt_path(receipt_dir: &Path, now_unix_ms: u64) -> PathBuf {
    receipt_dir.join(format!("claimed-{}-{now_unix_ms}.json", std::process::id()))
}

fn current_unix_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration_millis(duration))
        .unwrap_or_default()
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn millis_duration(millis: u64) -> Duration {
    Duration::from_millis(millis)
}

#[cfg(test)]
mod tests {
    use super::{
        APPLIED_RECEIPT_MAX_AGE, PENDING_APPLIED_RECEIPT_FILE_NAME, RELAUNCH_RECEIPT_DIR_NAME,
        claim_post_update_receipt_at, duration_millis,
    };
    use serde_json::json;
    use std::fs;

    const NOW_MS: u64 = 1_789_100_100_000;

    #[test]
    fn valid_receipt_is_claimed_exactly_once() {
        let temp_dir = tempfile::tempdir().unwrap();
        write_receipt(temp_dir.path(), "0.25.0", "0.26.0", NOW_MS - 200);

        let claimed = claim_post_update_receipt_at(temp_dir.path(), "0.26.0", NOW_MS)
            .unwrap()
            .unwrap();

        assert_eq!(claimed.from_version, "0.25.0");
        assert_eq!(claimed.to_version, "0.26.0");
        assert_eq!(claimed.process_exit_wait.as_millis(), 100);
        assert_eq!(claimed.apply_duration.as_millis(), 300);
        assert_eq!(claimed.relaunch_duration.as_millis(), 200);
        assert!(
            claim_post_update_receipt_at(temp_dir.path(), "0.26.0", NOW_MS)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn target_version_mismatch_is_not_post_update() {
        let temp_dir = tempfile::tempdir().unwrap();
        write_receipt(temp_dir.path(), "0.25.0", "0.26.0", NOW_MS - 1_000);

        assert!(claim_post_update_receipt_at(temp_dir.path(), "0.27.0", NOW_MS).is_err());
        assert!(
            claim_post_update_receipt_at(temp_dir.path(), "0.27.0", NOW_MS)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn stale_receipt_is_consumed_without_classifying_startup() {
        let temp_dir = tempfile::tempdir().unwrap();
        let written_at = NOW_MS - duration_millis(APPLIED_RECEIPT_MAX_AGE) - 1;
        write_receipt(temp_dir.path(), "0.25.0", "0.26.0", written_at);

        assert!(claim_post_update_receipt_at(temp_dir.path(), "0.26.0", NOW_MS).is_err());
        assert!(
            claim_post_update_receipt_at(temp_dir.path(), "0.26.0", NOW_MS)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn missing_receipt_is_an_ordinary_startup() {
        let temp_dir = tempfile::tempdir().unwrap();
        assert!(
            claim_post_update_receipt_at(temp_dir.path(), "0.26.0", NOW_MS)
                .unwrap()
                .is_none()
        );
    }

    fn write_receipt(
        runtime_home: &std::path::Path,
        from_version: &str,
        to_version: &str,
        receipt_written_at_unix_ms: u64,
    ) {
        let receipt_dir = runtime_home
            .join("desktop-updates")
            .join(RELAUNCH_RECEIPT_DIR_NAME);
        fs::create_dir_all(receipt_dir.as_path()).unwrap();
        let requested = receipt_written_at_unix_ms - 800;
        let payload = json!({
            "schema_version": 1,
            "product": "pioneer-desktop",
            "status": "applied",
            "attempt_id": "A1b2C3d4E5f6G7h8I9j0K",
            "from_version": from_version,
            "to_version": to_version,
            "platform": std::env::consts::OS,
            "relaunch_requested_at_unix_ms": requested,
            "process_exited_at_unix_ms": requested + 100,
            "apply_started_at_unix_ms": requested + 200,
            "apply_completed_at_unix_ms": requested + 500,
            "receipt_written_at_unix_ms": receipt_written_at_unix_ms,
            "details": null
        });
        fs::write(
            receipt_dir.join(PENDING_APPLIED_RECEIPT_FILE_NAME),
            serde_json::to_vec_pretty(&payload).unwrap(),
        )
        .unwrap();
    }
}
