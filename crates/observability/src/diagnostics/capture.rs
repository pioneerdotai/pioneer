use super::{
    TargetDescriptor, TargetSnapshot,
    storage::{self, ArtifactFile},
};
use crate::{
    identity::{monotonic_ms, unix_ms},
    resources::{ChildRegistration, ChildRole, read_process},
    symbols::ModuleIdentity,
};
use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};
// xctrace's --time-limit covers recording, not stack analysis and trace saving.
// A 20 s recording of the retained Gateway took 90.5 s to exit naturally.
// Keep finalization bounded without killing it after the old 10 s allowance;
// the diagnostic client reserves this same allowance before its reply timeout.
pub(super) const XCTRACE_FINISH_ALLOWANCE_SECONDS: u64 = 120;
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CaptureMode {
    Cpu,
    Allocations,
    AllocationHistory,
    LiveAllocations,
    Vm,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CaptureRequest {
    pub mode: CaptureMode,
    pub seconds: u64,
    pub frequency_hz: u32,
    pub child_generation: Option<u64>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CaptureManifest {
    pub schema_version: u32,
    pub capture_id: String,
    pub run: crate::identity::RunIdentity,
    pub consent_generation: u64,
    pub target_pid: u32,
    pub target_start_identity: String,
    pub target_child_generation: Option<u64>,
    pub target_build_scope: String,
    pub mode: CaptureMode,
    pub tool: String,
    pub tool_version: String,
    pub configuration: Vec<String>,
    pub actual_format: String,
    pub started_unix_ms: u64,
    pub ended_unix_ms: u64,
    pub started_monotonic_ms: u64,
    pub ended_monotonic_ms: u64,
    pub clock_scope: String,
    pub module: Option<ModuleIdentity>,
    pub module_coverage: String,
    pub symbol_status: String,
    pub coverage: String,
    pub lost_samples: Option<u64>,
    pub status: String,
    pub incomplete_reasons: Vec<String>,
    pub files: Vec<ArtifactFile>,
    pub local_reference: String,
    pub correlation_scope: String,
    /// Local SQL command CPU coverage; independent from native profile status/window.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sql_cpu: Option<serde_json::Value>,
}
fn is_profile_file(mode: CaptureMode, file: &ArtifactFile) -> bool {
    file.bytes > 0
        && match mode {
            CaptureMode::Cpu | CaptureMode::Allocations => {
                file.filename.starts_with("profile.trace/") || file.filename == "perf.data"
            }
            CaptureMode::AllocationHistory | CaptureMode::LiveAllocations | CaptureMode::Vm => {
                file.filename == "tool-output.txt"
            }
        }
}
impl CaptureManifest {
    pub(super) fn profile_data_present(&self) -> bool {
        matches!(self.status.as_str(), "completed" | "partial")
            && self.files.iter().any(|f| is_profile_file(self.mode, f))
    }
    pub(super) fn profile_artifacts_present(&self, root: &Path) -> bool {
        self.profile_data_present()
            && self.files.iter().take(storage::MAX_FILES).any(|file| {
                if !is_profile_file(self.mode, file) || file.filename.len() > 512 {
                    return false;
                }
                let mut path = root.to_owned();
                for (depth, component) in Path::new(&file.filename).components().enumerate() {
                    if depth >= 12 || !matches!(component, std::path::Component::Normal(_)) {
                        return false;
                    }
                    path.push(component);
                    if !fs::symlink_metadata(&path).is_ok_and(|meta| !meta.file_type().is_symlink())
                    {
                        return false;
                    }
                }
                fs::metadata(path).is_ok_and(|meta| meta.is_file() && meta.len() == file.bytes)
            })
    }
}
/// Evidence known after a successful spawn, retained even when wait/inventory
/// fails. Bounds and tool configuration are the same concrete ToolPlan.
struct AttemptProvenance {
    tool: String,
    version: String,
    configuration: Vec<String>,
    format: String,
    module: Option<ModuleIdentity>,
    started_wall: u64,
    started_mono: u64,
    ended: Option<(u64, u64)>,
}
impl AttemptProvenance {
    fn preserve(self, manifest: &mut CaptureManifest) {
        manifest.tool = self.tool;
        manifest.tool_version = self.version;
        manifest.configuration = self.configuration;
        manifest.actual_format = self.format;
        manifest.module = self.module;
        manifest.symbol_status = symbol_status(manifest.module.as_ref()).into();
        manifest.module_coverage = if manifest.module.is_some() {
            "main executable ID recorded; loaded-module coverage tool-defined"
        } else {
            "main module ID unavailable"
        }
        .into();
        manifest.started_unix_ms = self.started_wall;
        manifest.started_monotonic_ms = self.started_mono;
        let (wall, mono) = self.ended.unwrap_or_else(|| (unix_ms(), monotonic_ms()));
        manifest.ended_unix_ms = wall;
        manifest.ended_monotonic_ms = mono;
        manifest.clock_scope = "attempt window: native tool spawn through observed stop or cleanup after error; actual sample window unknown, inspect partial native artifact".into();
        manifest.status = "partial".into();
        manifest.coverage =
            "post-start failure; samples/coverage unknown, no successful symbolication asserted"
                .into();
        manifest
            .incomplete_reasons
            .push("native_attempt_started_then_failed".into());
    }
}
fn symbol_status(module: Option<&ModuleIdentity>) -> &'static str {
    if module.is_some() {
        "post-link ID recorded; validate retained symbols before symbolication"
    } else {
        "module ID unavailable; exact symbols cannot be attested"
    }
}
struct OwnedTool(Child);
impl std::ops::Deref for OwnedTool {
    type Target = Child;
    fn deref(&self) -> &Child {
        &self.0
    }
}
impl std::ops::DerefMut for OwnedTool {
    fn deref_mut(&mut self) -> &mut Child {
        &mut self.0
    }
}
impl Drop for OwnedTool {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}
struct ToolPlan {
    program: PathBuf,
    args: Vec<String>,
    format: &'static str,
    version_args: Vec<String>,
    graceful_stop: bool,
}
fn plan(config: &CaptureRequest, pid: u32, path: &Path, stack_logging: &str) -> Result<ToolPlan> {
    ensure!(
        (1..=60).contains(&config.seconds),
        "capture duration must be 1..60 seconds"
    );
    ensure!(
        (19..=199).contains(&config.frequency_hz),
        "frequency must be 19..199 Hz"
    );
    let pid = pid.to_string();
    #[cfg(target_os = "macos")]
    {
        let plan = match config.mode {
            CaptureMode::Cpu | CaptureMode::Allocations => {
                let template = if config.mode == CaptureMode::Cpu {
                    "Time Profiler"
                } else {
                    "Allocations"
                };
                ToolPlan {
                    program: "/usr/bin/xcrun".into(),
                    args: vec![
                        "xctrace".into(),
                        "record".into(),
                        "--template".into(),
                        template.into(),
                        "--attach".into(),
                        pid,
                        "--time-limit".into(),
                        format!("{}s", config.seconds),
                        "--window".into(),
                        format!("{}s", config.seconds),
                        "--no-prompt".into(),
                        "--output".into(),
                        path.join("profile.trace").to_string_lossy().into_owned(),
                    ],
                    format: "instruments.trace",
                    version_args: vec!["xctrace".into(), "version".into()],
                    graceful_stop: true,
                }
            }
            CaptureMode::AllocationHistory | CaptureMode::LiveAllocations => {
                ensure!(
                    stack_logging == "full_history_since_launch",
                    "preparation_required: launch target with child-only MallocStackLoggingNoCompact=1; attach cannot create allocation history"
                );
                ToolPlan {
                    program: "/usr/bin/malloc_history".into(),
                    args: vec![
                        pid,
                        if config.mode == CaptureMode::AllocationHistory {
                            "-allEvents"
                        } else {
                            "-allBySize"
                        }
                        .into(),
                    ],
                    format: "apple.malloc_history.text",
                    version_args: vec![],
                    graceful_stop: false,
                }
            }
            CaptureMode::Vm => ToolPlan {
                program: "/usr/bin/vmmap".into(),
                args: vec!["-summary".into(), pid],
                format: "apple.vmmap.summary.text",
                version_args: vec![],
                graceful_stop: false,
            },
        };
        return Ok(plan);
    }
    #[cfg(target_os = "linux")]
    {
        let _ = stack_logging;
        match config.mode {
            CaptureMode::Cpu => {
                return Ok(ToolPlan {
                    program: "perf".into(),
                    args: vec![
                        "record".into(),
                        "-e".into(),
                        "cpu-clock".into(),
                        "-F".into(),
                        config.frequency_hz.to_string(),
                        "--strict-freq".into(),
                        "--call-graph".into(),
                        "dwarf,16384".into(),
                        "-m".into(),
                        "256".into(),
                        "-p".into(),
                        pid,
                        "-o".into(),
                        path.join("perf.data").to_string_lossy().into_owned(),
                    ],
                    format: "linux.perf.data",
                    version_args: vec!["--version".into()],
                    graceful_stop: true,
                });
            }
            CaptureMode::Vm => {
                return Ok(ToolPlan {
                    program: "pmap".into(),
                    args: vec!["-x".into(), pid],
                    format: "linux.pmap.text",
                    version_args: vec!["--version".into()],
                    graceful_stop: false,
                });
            }
            _ => bail!(
                "unsupported_mode: Linux system allocator has no attach allocation history/live-heap contract; VM capture is available"
            ),
        }
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = (pid, path, stack_logging);
        bail!("unsupported_mode: native profiling adapter is unavailable for this target");
    }
}
fn identity_result(start: &str, observed: std::io::Result<String>) -> Result<(), &'static str> {
    match observed {
        Ok(current) if current == start => Ok(()),
        Ok(_) => Err("target_pid_reused"),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Err("target_exited"),
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
            Err("target_identity_permission_denied")
        }
        Err(_) => Err("target_identity_unavailable"),
    }
}
fn verify_identity(pid: u32, start: &str) -> Result<()> {
    identity_result(start, crate::resources::process_start_identity(pid))
        .map_err(anyhow::Error::msg)
}
fn allowed(epoch: u64, stop: &AtomicBool, cancel: &AtomicBool) -> bool {
    !stop.load(Ordering::Acquire)
        && !cancel.load(Ordering::Acquire)
        && crate::telemetry_sample_allowed(Some(epoch))
}
fn resolve_target(target: &TargetSnapshot, config: &CaptureRequest) -> Result<(u32, String)> {
    if let Some(generation) = config.child_generation {
        let snapshot = target
            .resources
            .as_ref()
            .context("child registry not recorded")?;
        let child = snapshot
            .children
            .iter()
            .find(|c| {
                c.generation == generation
                    && c.ownership_ended_monotonic_ms.is_none()
                    && c.exited_monotonic_ms.is_none()
            })
            .context("registered child not active")?;
        ensure!(
            child.role != ChildRole::Profiler,
            "cannot profile the profiler"
        );
        Ok((
            child.pid,
            child
                .start_identity
                .clone()
                .context("child start identity unavailable")?,
        ))
    } else {
        let start = crate::resources::process_start_identity(target.run.pid)?;
        Ok((target.run.pid, start))
    }
}
fn stop_tool(child: &mut Child, graceful: bool, registration: &mut ChildRegistration) -> bool {
    #[cfg(unix)]
    if graceful {
        unsafe {
            libc::kill(child.id() as i32, libc::SIGINT);
        }
    }
    #[cfg(not(unix))]
    let _ = graceful;
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if let Some(status) = child.try_wait().ok().flatten() {
            return observed_tool_stop(registration, Some(status), true);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let status = child.wait().ok();
            return observed_tool_stop(registration, status, false);
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}
// Preserve the existing graceful/forced result separately from process exit.
fn observed_tool_stop(
    registration: &mut ChildRegistration,
    status: Option<std::process::ExitStatus>,
    graceful: bool,
) -> bool {
    if let Some(status) = status {
        registration.exited(status.success());
    }
    graceful
}
// Version subprocesses get the same bounded timeout/output ownership as captures.
fn version(
    plan: &ToolPlan,
    root: &Path,
    epoch: u64,
    stop: &AtomicBool,
    cancel: &AtomicBool,
) -> Result<String> {
    ensure!(
        allowed(epoch, stop, cancel),
        "tool_version_cancelled_or_consent_revoked"
    );
    if plan.version_args.is_empty() {
        #[cfg(target_os = "macos")]
        {
            let os_plan = ToolPlan {
                program: "/usr/bin/sw_vers".into(),
                args: vec![],
                format: "text",
                version_args: vec!["-buildVersion".into()],
                graceful_stop: false,
            };
            return version(&os_plan, root, epoch, stop, cancel)
                .map(|v| format!("OS-bundled; macOS build {v}"));
        }
        #[cfg(not(target_os = "macos"))]
        return Ok("version_unavailable".into());
    }
    let out = fs::File::create(root.join("tool-version.txt"))?;
    let mut child = OwnedTool(
        Command::new(&plan.program)
            .args(&plan.version_args)
            .env_remove("MallocStackLogging")
            .env_remove("MallocStackLoggingNoCompact")
            .env_remove("MallocStackLoggingDirectory")
            .stdin(Stdio::null())
            .stdout(Stdio::from(out.try_clone()?))
            .stderr(Stdio::from(out))
            .spawn()
            .context("tool_unavailable")?,
    );
    let mut registration = ChildRegistration::register(Some(child.id()), ChildRole::Profiler, None);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.try_wait()? {
            registration.exited(status.success());
            ensure!(status.success(), "tool_version_unavailable");
            break;
        }
        if !allowed(epoch, stop, cancel)
            || Instant::now() >= deadline
            || fs::metadata(root.join("tool-version.txt"))?.len() > 64 * 1024
        {
            stop_tool(&mut child, false, &mut registration);
            bail!("tool_version_timeout_or_cancelled");
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    let bytes = storage::read_bounded(&root.join("tool-version.txt"), 64 * 1024)?;
    Ok(String::from_utf8_lossy(&bytes).chars().take(256).collect())
}
pub(super) fn capture(
    root: &Path,
    target: &TargetSnapshot,
    descriptor: &TargetDescriptor,
    config: CaptureRequest,
    stop: &AtomicBool,
    cancel: &AtomicBool,
) -> Result<CaptureManifest> {
    let epoch = target.consent_generation;
    ensure!(
        crate::telemetry_sample_allowed(Some(epoch)),
        "consent_epoch_changed"
    );
    ensure!(!stop.load(Ordering::Acquire), "capture_shutdown");
    ensure!(
        !cancel.load(Ordering::Acquire),
        "operator_cancelled_before_attach"
    );
    let (pid, start_identity) = resolve_target(target, &config)?;
    verify_identity(pid, &start_identity)?;
    if config.child_generation.is_none() {
        ensure!(
            start_identity == descriptor.start_identity,
            "target_identity_changed"
        );
    }
    let reserve_sql_cpu = sql_cpu_eligible(&target.service, &config, pid);
    let capture_id = nanoid::nanoid!(24);
    let path = storage::capture_dir(root, &capture_id)?;
    let _capture_lease = storage::CaptureLease::new(&path);
    let mut attempt = None;
    let mut sql_cpu = SqlCpuCapture::default();
    let outcome = capture_inner(
        &path,
        &capture_id,
        target,
        config.clone(),
        pid,
        &start_identity,
        stop,
        cancel,
        &mut attempt,
        &mut sql_cpu,
    );
    // Seal even on native spawn/wait/inventory failure; deadline is owned by
    // Session and never extended by native-tool finalization.
    sql_cpu.seal();
    let sql_cpu = sql_cpu.snapshot;
    if !crate::telemetry_sample_allowed(Some(epoch)) || stop.load(Ordering::Acquire) {
        storage::remove_owned(&path);
        bail!("capture_shutdown_or_consent_revoked");
    }
    let mut manifest = match outcome {
        Ok(manifest) => manifest,
        Err(error) => recover_failed_capture(
            root,
            &path,
            &capture_id,
            target,
            &config,
            pid,
            &start_identity,
            error,
            attempt,
            reserve_sql_cpu,
        )?,
    };
    if let Some(snapshot) = sql_cpu {
        if !crate::telemetry_sample_allowed(Some(epoch)) || stop.load(Ordering::Acquire) {
            storage::remove_owned(&path);
            bail!("capture_shutdown_or_consent_revoked");
        }
        store_sql_cpu(&path, target, &mut manifest, &snapshot);
    }
    if manifest.sql_cpu.is_none() && sql_cpu_eligible(&target.service, &config, pid) {
        manifest.sql_cpu = Some(
            serde_json::json!({"status":"unavailable", "reason":"native_prerequisite_failed_or_session_unavailable"}),
        );
    }
    storage::write_new_json(&path.join("manifest.json"), &manifest)?;
    if !crate::telemetry_sample_allowed(Some(epoch)) {
        storage::remove_owned(&path);
        bail!("consent_revoked_before_capture_commit");
    }
    record_reference(&manifest);
    Ok(manifest)
}
// Native tool ownership has already been released/reaped at this boundary.
// SQL recovery preserves both the sealed snapshot and the original admitted
// directory/markers until commit, without taking another storage reservation.
fn recover_failed_capture(
    root: &Path,
    path: &Path,
    capture_id: &str,
    target: &TargetSnapshot,
    config: &CaptureRequest,
    pid: u32,
    start_identity: &str,
    error: anyhow::Error,
    mut attempt: Option<AttemptProvenance>,
    reserve_sql_cpu: bool,
) -> Result<CaptureManifest> {
    if let Some(attempt) = &mut attempt {
        attempt
            .ended
            .get_or_insert_with(|| (unix_ms(), monotonic_ms()));
    }
    let mut files = if reserve_sql_cpu {
        native_size(path, true)
            .ok()
            .and_then(|_| capture_inventory(path, true).ok())
    } else {
        storage::inventory(path).ok()
    };
    if files.is_none() {
        if reserve_sql_cpu {
            storage::clear_owned_capture_output(path)?;
            native_size(path, true)?;
            files = Some(capture_inventory(path, true)?);
        } else {
            storage::remove_owned(path);
            let recreated = storage::capture_dir(root, capture_id)?;
            files = capture_inventory(&recreated, false).ok();
        }
    }
    let mut manifest = failed_manifest(
        capture_id,
        target,
        config,
        pid,
        start_identity,
        format!("{error}").chars().take(256).collect(),
        files.unwrap_or_default(),
    );
    if let Some(attempt) = attempt {
        attempt.preserve(&mut manifest);
    }
    Ok(manifest)
}
fn capture_inventory(path: &Path, sql_cpu: bool) -> Result<Vec<ArtifactFile>> {
    if sql_cpu {
        // Leave one file/entry and eight MiB for manifest while active.json is
        // still present. Lease release can only reduce the final footprint.
        storage::inventory_with_limits(path, storage::SQL_INVENTORY_BYTES, storage::MAX_FILES - 1)
    } else {
        storage::inventory(path)
    }
}
// The diagnostic owner performs serialization/checksums, never a SQLite worker.
fn store_sql_cpu(
    path: &Path,
    target: &TargetSnapshot,
    manifest: &mut CaptureManifest,
    snapshot: &crate::query_cpu::Snapshot,
) {
    #[derive(Serialize)]
    struct Sidecar<'a> {
        schema_version: u32,
        kind: &'static str,
        run: &'a crate::identity::RunIdentity,
        consent_generation: u64,
        native_profile_status: &'a str,
        native_profile_interval: serde_json::Value,
        sql_cpu: &'a crate::query_cpu::Snapshot,
    }
    let mut created = false;
    let stored = (|| -> Result<(Vec<ArtifactFile>, u64)> {
        // The same reserve is enforced while the native child is running and
        // after its final exit (including any final flush/overshoot).
        let (limit, files) = native_storage_limits(true);
        storage::size_with_file_limit(path, limit, files)?;
        let sidecar = Sidecar {
            schema_version: 2,
            kind: "pioneer.gateway.sql_cpu",
            run: &target.run,
            consent_generation: target.consent_generation,
            native_profile_status: &manifest.status,
            native_profile_interval: serde_json::json!({
                "started_monotonic_ms": manifest.started_monotonic_ms,
                "ended_monotonic_ms": manifest.ended_monotonic_ms,
                "clock_scope": manifest.clock_scope,
            }),
            sql_cpu: snapshot,
        };
        let bytes = storage::write_new_sql_cpu_json(&path.join("sql-cpu.json"), &sidecar)?;
        created = true;
        Ok((capture_inventory(path, true)?, bytes))
    })();
    let mut summary = serde_json::json!({
        "schema_version": 2,
        "aggregation": snapshot.aggregation,
        "coverage": snapshot.coverage,
        "measured_unknown": snapshot.measured_unknown,
        "group_overflow": snapshot.group_overflow,
        "started_monotonic_ms": snapshot.started_monotonic_ms,
        "ended_monotonic_ms": snapshot.ended_monotonic_ms,
        "scope": "Observed SQLx command CPU only; aggregation completeness is independent of unknown identities, measurement gaps and native profile status. measured_unknown overlaps groups/residual; never add it to accounted.",
    });
    match stored {
        Ok((files, bytes)) => {
            manifest.files = files;
            summary["filename"] = "sql-cpu.json".into();
            summary["bytes"] = bytes.into();
            summary["status"] = "recorded".into();
        }
        Err(_) => {
            // Remove only a successfully create_new-owned sidecar, never a
            // pre-existing collision. The writer cleans its own partial writes.
            if created {
                let _ = fs::remove_file(path.join("sql-cpu.json"));
            }
            summary["status"] = "unavailable".into();
            summary["reason"] = "sql_cpu_storage_or_inventory_failed".into();
            manifest
                .incomplete_reasons
                .push("sql_cpu_sidecar_unavailable".into());
            if manifest.status == "completed" {
                manifest.status = "partial".into();
            }
        }
    }
    manifest.sql_cpu = Some(summary);
}
fn native_storage_limits(sql_cpu: bool) -> (u64, usize) {
    if sql_cpu {
        // Owner/active markers are already included in native size/count.
        // Reserve both future SQL and manifest files/entries before spawning.
        (
            storage::SQL_INVENTORY_BYTES - storage::SQL_CPU_BYTES,
            storage::MAX_FILES - 2,
        )
    } else {
        (storage::INVENTORY_BYTES, storage::MAX_FILES)
    }
}
fn native_size(path: &Path, sql_cpu: bool) -> Result<u64> {
    let (bytes, files) = native_storage_limits(sql_cpu);
    storage::size_with_file_limit(path, bytes, files)
}
#[derive(Default)]
struct SqlCpuCapture {
    session: Option<crate::query_cpu::Session>,
    snapshot: Option<crate::query_cpu::Snapshot>,
}
impl SqlCpuCapture {
    fn seal(&mut self) {
        if let Some(session) = self.session.take() {
            self.snapshot = Some(session.finish());
        }
    }
}
fn sql_cpu_eligible(service: &str, config: &CaptureRequest, pid: u32) -> bool {
    service == "gateway"
        && config.mode == CaptureMode::Cpu
        && config.child_generation.is_none()
        && pid == std::process::id()
}
fn capture_inner(
    path: &Path,
    capture_id: &str,
    target: &TargetSnapshot,
    config: CaptureRequest,
    pid: u32,
    start_identity: &str,
    stop: &AtomicBool,
    cancel: &AtomicBool,
    attempt: &mut Option<AttemptProvenance>,
    sql_cpu: &mut SqlCpuCapture,
) -> Result<CaptureManifest> {
    let epoch = target.consent_generation;
    let reserve_sql_cpu = sql_cpu_eligible(&target.service, &config, pid);
    let plan = plan(&config, pid, path, &target.stack_logging)?;
    let module = if config.child_generation.is_none() {
        Some(child_module(pid, epoch, stop, cancel)?)
    } else {
        child_module(pid, epoch, stop, cancel).ok()
    };
    let tool_version = version(&plan, path, epoch, stop, cancel)?;
    verify_identity(pid, start_identity)?;
    ensure!(
        allowed(epoch, stop, cancel),
        "capture_cancelled_before_attach"
    );
    let output = fs::File::create(path.join("tool-output.txt"))?;
    let stderr = fs::File::create(path.join("tool-stderr.txt"))?;
    if reserve_sql_cpu {
        native_size(path, true)?;
    }
    let started_unix_ms = unix_ms();
    let started_monotonic_ms = monotonic_ms();
    if sql_cpu_eligible(&target.service, &config, pid) {
        sql_cpu.session = crate::query_cpu::Session::start(config.seconds, epoch);
    }
    let mut child = OwnedTool(
        Command::new(&plan.program)
            .args(&plan.args)
            .env_remove("MallocStackLogging")
            .env_remove("MallocStackLoggingNoCompact")
            .env_remove("MallocStackLoggingDirectory")
            .stdin(Stdio::null())
            .stdout(Stdio::from(output))
            .stderr(Stdio::from(stderr))
            .spawn()
            .context("tool_unavailable_or_permission_denied")?,
    );
    *attempt = Some(AttemptProvenance {
        tool: plan
            .program
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .into_owned(),
        version: tool_version.clone(),
        configuration: plan.args.clone(),
        format: plan.format.into(),
        module: module.clone(),
        started_wall: started_unix_ms,
        started_mono: started_monotonic_ms,
        ended: None,
    });
    let mut registration = ChildRegistration::register(Some(child.id()), ChildRole::Profiler, None);
    let mut reasons = Vec::new();
    let mut status = "completed";
    let started = Instant::now();
    let finish_allowance = if cfg!(target_os = "macos")
        && matches!(config.mode, CaptureMode::Cpu | CaptureMode::Allocations)
    {
        XCTRACE_FINISH_ALLOWANCE_SECONDS
    } else {
        10
    };
    let timeout = Duration::from_secs(config.seconds + finish_allowance);
    loop {
        let exited = match child.try_wait() {
            Ok(exited) => exited,
            Err(error) => {
                sql_cpu.seal();
                return Err(error.into());
            }
        };
        if let Some(exit) = exited {
            sql_cpu.seal();
            registration.exited(exit.success());
            if !exit.success() {
                status = "failed";
                reasons.push(classify_error(path));
            }
            break;
        }
        if cancel.load(Ordering::Acquire) {
            reasons.push("operator_cancelled".into());
            status = "partial";
            sql_cpu.seal();
            stop_tool(&mut child, plan.graceful_stop, &mut registration);
            break;
        }
        if !crate::telemetry_sample_allowed(Some(epoch)) || stop.load(Ordering::Acquire) {
            sql_cpu.seal();
            stop_tool(&mut child, plan.graceful_stop, &mut registration);
            bail!("consent_revoked_or_shutdown");
        }
        if let Err(error) = verify_identity(pid, start_identity) {
            reasons.push(error.to_string());
            status = "partial";
            sql_cpu.seal();
            stop_tool(&mut child, plan.graceful_stop, &mut registration);
            break;
        }
        if native_size(path, reserve_sql_cpu).is_err() {
            reasons.push("output_overflow".into());
            status = "partial";
            sql_cpu.seal();
            stop_tool(&mut child, plan.graceful_stop, &mut registration);
            trim_overflow(path, reserve_sql_cpu)?;
            break;
        }
        #[cfg(target_os = "linux")]
        if config.mode == CaptureMode::Cpu
            && started.elapsed() >= Duration::from_secs(config.seconds)
        {
            sql_cpu.seal();
            if !stop_tool(&mut child, true, &mut registration) {
                status = "partial";
                reasons.push("profiler_forced_termination_after_grace".into());
            }
            break;
        }
        if started.elapsed() >= timeout {
            reasons.push("tool_timeout".into());
            status = "partial";
            sql_cpu.seal();
            stop_tool(&mut child, plan.graceful_stop, &mut registration);
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    sql_cpu.seal();
    let ended_unix_ms = unix_ms();
    let ended_monotonic_ms = monotonic_ms();
    if let Some(attempt) = attempt {
        attempt.ended = Some((ended_unix_ms, ended_monotonic_ms));
    }
    if let Err(error) = verify_identity(pid, start_identity) {
        status = "partial";
        let reason = error.to_string();
        if !reasons.contains(&reason) {
            reasons.push(reason);
        }
    }
    if let Some(exit) = child.try_wait()? {
        registration.exited(exit.success());
        if !exit.success() && status == "completed" {
            status = "partial";
            reasons.push("profiler_nonzero_exit_after_requested_stop".into());
        }
    }
    if let Some(before) = &module {
        let after = child_module(pid, epoch, stop, cancel);
        if after.as_ref().ok() != Some(before) {
            status = "partial";
            reasons.push("module_identity_changed_or_unavailable_after_capture".into());
        }
    }
    let files = finalize_native_files(
        path,
        config.mode,
        reserve_sql_cpu,
        &mut status,
        &mut reasons,
    )?;
    let symbol_status = symbol_status(module.as_ref());
    Ok(CaptureManifest{schema_version:1,capture_id:capture_id.into(),run:target.run.clone(),consent_generation:epoch,target_pid:pid,target_start_identity:start_identity.into(),target_child_generation:config.child_generation,target_build_scope:if config.child_generation.is_none(){"target_executable"}else{"parent_build_only; child module identity separately reported"}.into(),mode:config.mode,tool:plan.program.file_name().unwrap_or_default().to_string_lossy().into_owned(),tool_version,configuration:plan.args,actual_format:plan.format.into(),started_unix_ms,ended_unix_ms,started_monotonic_ms,ended_monotonic_ms,clock_scope:"native tool spawn through observed exit/stop, target process monotonic_ms and unix_ms; tool artifact contains actual sample times; module hashing/version preparation excluded".into(),module_coverage:if module.is_some(){"main executable only; native tool artifact contains loaded-module evidence where available"}else{"main module ID unavailable; native artifact loaded-module evidence tool-defined"}.into(),module,symbol_status:symbol_status.into(),coverage:if cfg!(target_os="macos")&&matches!(config.mode,CaptureMode::Cpu|CaptureMode::Allocations){"template-controlled sampling; all target threads; frequency and lost samples unknown until tool analysis"}else{"tool-defined; see actual format; no CPU-to-async-operation attribution"}.into(),lost_samples:None,status:status.into(),incomplete_reasons:reasons,files,local_reference:format!("captures/pioneer-capture-{capture_id}/manifest.json"),correlation_scope:"service.instance.id + build + process/start + capture interval; SQL sidecar has its own observed query window, not arbitrary async stack attribution".into(),sql_cpu:None})
}
// Actual post-exit owner boundary: includes final flush/stop overshoot, trim,
// final reserve validation and native status. Errors feed owned recovery above.
fn finalize_native_files(
    path: &Path,
    mode: CaptureMode,
    reserve_sql_cpu: bool,
    status: &mut &str,
    reasons: &mut Vec<String>,
) -> Result<Vec<ArtifactFile>> {
    if reserve_sql_cpu && native_size(path, true).is_err() {
        if *status == "completed" {
            *status = "partial";
        }
        if !reasons.iter().any(|r| r == "output_overflow") {
            reasons.push("output_overflow".into());
        }
        // Preserve the reason even if trimming cannot restore the reserve and
        // the SQL owner must clear native output in place, retaining its lease.
        trim_overflow(path, true).context("output_overflow")?;
        native_size(path, true).context("output_overflow")?;
    }
    let files = capture_inventory(path, reserve_sql_cpu)?;
    if !files.iter().any(|file| is_profile_file(mode, file)) {
        *status = "failed";
        reasons.push("profile_artifact_missing".into());
    }
    Ok(files)
}
fn child_module(
    pid: u32,
    epoch: u64,
    stop: &AtomicBool,
    cancel: &AtomicBool,
) -> Result<ModuleIdentity> {
    #[cfg(target_os = "linux")]
    {
        return crate::symbols::module_identity_guarded(
            &PathBuf::from(format!("/proc/{pid}/exe")),
            || allowed(epoch, stop, cancel),
        );
    }
    #[cfg(target_os = "macos")]
    {
        let mut buffer = [0i8; libc::PROC_PIDPATHINFO_MAXSIZE as usize];
        let n = unsafe {
            libc::proc_pidpath(pid as i32, buffer.as_mut_ptr().cast(), buffer.len() as u32)
        };
        ensure!(n > 0, "child executable path unavailable");
        let path = unsafe { std::ffi::CStr::from_ptr(buffer.as_ptr()) };
        let module = crate::symbols::module_identity_guarded(
            Path::new(std::str::from_utf8(path.to_bytes())?),
            || allowed(epoch, stop, cancel),
        )?;
        let os = read_process(pid)?;
        ensure!(
            os.main_module_id.as_ref() == Some(&module.module_id),
            "mapped executable UUID differs from file; symbols cannot be attested"
        );
        return Ok(module);
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (pid, epoch, stop, cancel);
        bail!("child module unavailable");
    }
}
fn classify_error(path: &Path) -> String {
    let mut bytes =
        storage::read_bounded(&path.join("tool-stderr.txt"), 1024 * 1024).unwrap_or_default();
    bytes.extend(
        storage::read_bounded(&path.join("tool-output.txt"), 1024 * 1024).unwrap_or_default(),
    );
    let text = String::from_utf8_lossy(&bytes).to_ascii_lowercase();
    if text.contains("permission")
        || text.contains("not permitted")
        || text.contains("task_for_pid")
        || text.contains("authorized")
        || text.contains("get-task-allow")
    {
        "permission_or_signing_denied"
    } else if text.contains("template") && text.contains("not") {
        "tool_template_unavailable"
    } else {
        "tool_nonzero_exit"
    }
    .into()
}
fn trim_overflow(path: &Path, sql_cpu: bool) -> Result<()> {
    let (limit, files) = native_storage_limits(sql_cpu);
    // Preserve bounded partial files. Tools can overshoot between checks;
    // output that still cannot be inventoried goes through owner recovery.
    for name in ["tool-output.txt", "tool-stderr.txt", "perf.data"] {
        let file = path.join(name);
        if let Ok(meta) = fs::metadata(&file) {
            if meta.len() > 64 * 1024 * 1024 {
                fs::OpenOptions::new()
                    .write(true)
                    .open(file)?
                    .set_len(64 * 1024 * 1024)?;
            }
        }
    }
    if storage::size_with_file_limit(path, limit, files).is_err() {
        let trace = path.join("profile.trace");
        if trace.is_dir() {
            fs::remove_dir_all(trace)?;
        }
    }
    Ok(())
}
fn record_reference(manifest: &CaptureManifest) {
    use opentelemetry::{
        KeyValue,
        trace::{Span, Tracer},
    };
    crate::record_in_epoch(manifest.consent_generation, || {
        let Some(owner) = crate::telemetry::state() else {
            return;
        };
        let mut span = owner.tracer.start("diagnostics.capture");
        span.set_attributes([
            KeyValue::new(
                "pioneer.consent.generation",
                manifest.consent_generation.to_string(),
            ),
            KeyValue::new("pioneer.capture.id", manifest.capture_id.clone()),
            KeyValue::new("pioneer.capture.status", manifest.status.clone()),
            KeyValue::new(
                "pioneer.capture.started_unix_ms",
                manifest.started_unix_ms as i64,
            ),
            KeyValue::new(
                "pioneer.capture.ended_unix_ms",
                manifest.ended_unix_ms as i64,
            ),
            KeyValue::new(
                "pioneer.capture.artifact_reference",
                format!("local-capture:{}", manifest.capture_id),
            ),
        ]);
        span.end();
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    fn config(mode: CaptureMode) -> CaptureRequest {
        CaptureRequest {
            mode,
            seconds: 10,
            frequency_hz: 99,
            child_generation: None,
        }
    }
    #[test]
    fn unreadable_target_identity_does_not_assert_exit_or_reuse() {
        use std::io::{Error, ErrorKind};
        assert_eq!(identity_result("a", Ok("a".into())), Ok(()));
        assert_eq!(
            identity_result("a", Ok("b".into())),
            Err("target_pid_reused")
        );
        assert_eq!(
            identity_result("a", Err(Error::from(ErrorKind::NotFound))),
            Err("target_exited")
        );
        assert_eq!(
            identity_result("a", Err(Error::from(ErrorKind::PermissionDenied))),
            Err("target_identity_permission_denied")
        );
        assert_eq!(
            identity_result("a", Err(Error::other("unknown"))),
            Err("target_identity_unavailable")
        );
    }
    #[test]
    fn post_start_failure_preserves_attempt_provenance_and_honest_symbols() {
        let target = crate::diagnostics::tests::target(7);
        for reason in ["wait_failed", "artifact_inventory_failed"] {
            let mut manifest = failed_manifest(
                "fixture",
                &target,
                &config(CaptureMode::Cpu),
                42,
                "start",
                reason.into(),
                Vec::new(),
            );
            assert_eq!(manifest.status, "failed");
            assert_eq!(manifest.tool, "not_started_or_prerequisite_failed");
            AttemptProvenance {
                tool: "fake-native-tool".into(),
                version: "version1".into(),
                configuration: vec!["--attach=42".into()],
                format: "native-format".into(),
                module: None,
                started_wall: 1000,
                started_mono: 10,
                ended: Some((2000, 1010)),
            }
            .preserve(&mut manifest);
            assert_eq!(manifest.tool, "fake-native-tool");
            assert_eq!(manifest.tool_version, "version1");
            assert_eq!(manifest.configuration, ["--attach=42"]);
            assert_eq!(
                (manifest.started_unix_ms, manifest.ended_unix_ms),
                (1000, 2000)
            );
            assert_eq!(
                (manifest.started_monotonic_ms, manifest.ended_monotonic_ms),
                (10, 1010)
            );
            assert_eq!(manifest.status, "partial");
            assert!(manifest.incomplete_reasons.iter().any(|r| r == reason));
            assert_eq!(
                manifest.symbol_status,
                "module ID unavailable; exact symbols cannot be attested"
            );
            assert!(manifest.module.is_none());
            assert!(!manifest.profile_data_present());
        }
    }
    #[test]
    fn full_sql_table_survives_reserved_native_bytes_inventory_and_manifest() {
        use sha2::{Digest, Sha256};
        let temp = tempfile::tempdir().unwrap();
        let root = storage::private_root(&temp.path().join("diagnostics")).unwrap();
        let path = storage::capture_dir(&root, "abcdefghijklmnopqrstuvwx").unwrap();
        let lease = storage::CaptureLease::new(&path);
        let target = crate::diagnostics::tests::target(7);
        let snapshot = crate::query_cpu::aggregation_fixtures::full_budget_snapshot();
        let mut manifest = failed_manifest(
            "fixture",
            &target,
            &config(CaptureMode::Cpu),
            42,
            "start",
            "native_failed".into(),
            Vec::new(),
        );
        // Include owner/active markers in the exact native byte boundary.
        let marker_bytes = storage::size(&path, storage::CAPTURE_BYTES).unwrap();
        let native = fs::File::create(path.join("perf.data")).unwrap();
        let (limit, _) = native_storage_limits(true);
        assert_eq!(limit, 81_125_376);
        assert_eq!(native_storage_limits(false).0, 127 * 1024 * 1024);
        native.set_len(limit - marker_bytes).unwrap();
        assert_eq!(native_size(&path, true).unwrap(), limit);
        fs::write(path.join("excess"), b"x").unwrap();
        assert!(native_size(&path, true).is_err());
        fs::remove_file(path.join("excess")).unwrap();
        store_sql_cpu(&path, &target, &mut manifest, &snapshot);
        assert_eq!(manifest.status, "failed"); // SQL never promotes native
        let summary = manifest.sql_cpu.as_ref().unwrap();
        assert_eq!(summary["status"], "recorded");
        assert_eq!(summary["aggregation"]["complete"], true);
        assert_eq!(summary["aggregation"]["retained_groups"], 28_320);
        assert!(summary.get("groups").is_none());
        // Real local sidecar read with its own bound, independent of generic
        // 8 MiB manifest/API JSON. The data itself, not padding, exceeds 8 MiB.
        let bytes =
            storage::read_bounded(&path.join("sql-cpu.json"), storage::SQL_CPU_BYTES).unwrap();
        assert!(bytes.len() > 8 * 1024 * 1024);
        assert_eq!(summary["bytes"], bytes.len() as u64);
        let sidecar: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(sidecar["schema_version"], 2);
        assert_eq!(
            sidecar["sql_cpu"]["groups"].as_array().unwrap().len(),
            28_320
        );
        assert_eq!(sidecar["sql_cpu"]["groups"][0]["cpu_ns"], 1_000_000);
        assert_eq!(
            sidecar["sql_cpu"]["aggregation"]["accounted"]["cpu_ns"],
            1_019_998
        );
        let file = manifest
            .files
            .iter()
            .find(|f| f.filename == "sql-cpu.json")
            .unwrap();
        assert_eq!(file.bytes, bytes.len() as u64);
        assert_eq!(file.sha256, hex::encode(Sha256::digest(&bytes)));
        storage::validate_private_file(&path.join("sql-cpu.json")).unwrap();
        storage::write_new_json(&path.join("manifest.json"), &manifest).unwrap();
        let saved: CaptureManifest = serde_json::from_slice(
            &storage::read_bounded(&path.join("manifest.json"), 8 * 1024 * 1024).unwrap(),
        )
        .unwrap();
        assert_eq!(saved.sql_cpu.as_ref().unwrap()["status"], "recorded");
        assert!(serde_json::to_vec(&saved).unwrap().len() <= 8 * 1024 * 1024);
        assert!(!saved.profile_data_present());
        assert!(storage::size(&path, storage::CAPTURE_BYTES).is_ok());
        drop(lease);
        assert!(!path.join("active.json").exists());
    }

    #[test]
    fn post_exit_overshoot_recovers_owned_native_output_then_commits_full_sql_table() {
        use sha2::{Digest, Sha256};
        let temp = tempfile::tempdir().unwrap();
        let root = storage::private_root(&temp.path().join("diagnostics")).unwrap();
        let id = "abcdefghijklmnopqrstuvwx";
        let path = storage::capture_dir(&root, id).unwrap();
        let lease = storage::CaptureLease::new(&path);
        let owner_marker = fs::read(path.join("owner.json")).unwrap();
        let active_marker = fs::read(path.join("active.json")).unwrap();
        #[cfg(unix)]
        let original_inodes = {
            use std::os::unix::fs::MetadataExt;
            ["", "owner.json", "active.json"]
                .map(|name| fs::metadata(path.join(name)).unwrap().ino())
        };
        // Fill the shared store before overshoot. These are separate active
        // reservations even though no subprocess/profiler is needed here.
        let other_captures: Vec<_> = (0..3)
            .map(|i| {
                let other = storage::capture_dir(&root, &format!("{i:024x}")).unwrap();
                let lease = storage::CaptureLease::new(&other);
                (other, lease)
            })
            .collect();
        let fifth_id = "xwvutsrqponmlkjihgfedcba";
        assert_eq!(
            storage::capture_dir(&root, fifth_id)
                .unwrap_err()
                .to_string(),
            "capture storage budget exhausted"
        );
        let foreign = root.join("captures/foreign");
        fs::create_dir(&foreign).unwrap();
        fs::write(foreign.join("keep"), b"foreign-original").unwrap();
        let target = crate::diagnostics::tests::target(7);
        let snapshot = crate::query_cpu::aggregation_fixtures::full_budget_snapshot();
        // SQL-native allowance is smaller than the existing reservation:
        // 90 MiB fits whole-capture 128 MiB but cannot fit SQL-native output.
        // Neither 45 MiB log is reduced by the existing 64 MiB per-file trim.
        for name in ["tool-output.txt", "tool-stderr.txt"] {
            fs::File::create(path.join(name))
                .unwrap()
                .set_len(45 * 1024 * 1024)
                .unwrap();
        }
        assert!(storage::size(&path, storage::CAPTURE_BYTES).is_ok());
        let admission_lock = storage::owned_lock(&root, storage::LockKind::Storage).unwrap();
        assert_eq!(
            storage::capture_dir(&root, fifth_id)
                .unwrap_err()
                .to_string(),
            "capture_storage_busy"
        );
        let mut status = "completed";
        let mut reasons = Vec::new();
        let error = finalize_native_files(&path, CaptureMode::Cpu, true, &mut status, &mut reasons)
            .unwrap_err();
        assert_eq!(status, "partial");
        assert_eq!(reasons, ["output_overflow"]);
        assert_eq!(error.to_string(), "output_overflow");
        // Native recovery must also unlink owned symlinks without traversing
        // targets, both at the root and inside a native subtree.
        fs::create_dir(path.join("native-subtree")).unwrap();
        fs::write(path.join("native-subtree/owned"), b"native-output").unwrap();
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(&foreign, path.join("native-link")).unwrap();
            std::os::unix::fs::symlink(&foreign, path.join("native-subtree/link")).unwrap();
        }
        // Exercise the production recovery while re-admission is unavailable.
        // The old remove/capture_dir path fails here with capture_storage_busy;
        // an admitted capture must use its existing reservation, without a gap.
        let mut manifest = recover_failed_capture(
            &root,
            &path,
            id,
            &target,
            &config(CaptureMode::Cpu),
            42,
            "start",
            error,
            Some(AttemptProvenance {
                tool: "fixture-native-tool".into(),
                version: "fixture".into(),
                configuration: Vec::new(),
                format: "fixture-native".into(),
                module: None,
                started_wall: 1000,
                started_mono: 10,
                ended: Some((2000, 1010)),
            }),
            true,
        )
        .unwrap();
        assert_eq!(manifest.status, "partial");
        assert!(
            manifest
                .incomplete_reasons
                .iter()
                .any(|r| r == "output_overflow")
        );
        assert!(
            manifest
                .incomplete_reasons
                .iter()
                .any(|r| r == "native_attempt_started_then_failed")
        );
        assert!(!path.join("tool-output.txt").exists());
        assert!(!path.join("native-subtree").exists());
        assert!(fs::symlink_metadata(path.join("native-link")).is_err());
        assert_eq!(fs::read(path.join("owner.json")).unwrap(), owner_marker);
        assert_eq!(fs::read(path.join("active.json")).unwrap(), active_marker);
        storage::validate_private(&path).unwrap();
        storage::validate_private_file(&path.join("owner.json")).unwrap();
        storage::validate_private_file(&path.join("active.json")).unwrap();
        assert_eq!(fs::read(foreign.join("keep")).unwrap(), b"foreign-original");
        store_sql_cpu(&path, &target, &mut manifest, &snapshot);
        storage::write_new_json(&path.join("manifest.json"), &manifest).unwrap();
        assert_eq!(manifest.status, "partial");
        assert_eq!(manifest.sql_cpu.as_ref().unwrap()["status"], "recorded");
        assert!(!manifest.profile_data_present());
        let sidecar_bytes =
            storage::read_bounded(&path.join("sql-cpu.json"), storage::SQL_CPU_BYTES).unwrap();
        let saved: serde_json::Value = serde_json::from_slice(&sidecar_bytes).unwrap();
        assert_eq!(saved["sql_cpu"]["groups"].as_array().unwrap().len(), 28_320);
        assert_eq!(saved["sql_cpu"]["aggregation"]["complete"], true);
        assert_eq!(
            saved["sql_cpu"]["groups"][0]["fingerprint"],
            "0000000000004e20"
        );
        let saved_manifest: CaptureManifest = serde_json::from_slice(
            &storage::read_bounded(&path.join("manifest.json"), 8 * 1024 * 1024).unwrap(),
        )
        .unwrap();
        let sql_file = saved_manifest
            .files
            .iter()
            .find(|f| f.filename == "sql-cpu.json")
            .unwrap();
        assert_eq!(sql_file.bytes, sidecar_bytes.len() as u64);
        assert_eq!(sql_file.sha256, hex::encode(Sha256::digest(&sidecar_bytes)));
        assert_eq!(
            saved_manifest.sql_cpu.as_ref().unwrap()["bytes"],
            sidecar_bytes.len() as u64
        );
        assert_eq!(
            saved_manifest.sql_cpu.as_ref().unwrap()["status"],
            "recorded"
        );
        assert_eq!(fs::read(path.join("owner.json")).unwrap(), owner_marker);
        assert_eq!(fs::read(path.join("active.json")).unwrap(), active_marker);
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            assert_eq!(
                original_inodes,
                ["", "owner.json", "active.json"]
                    .map(|name| fs::metadata(path.join(name)).unwrap().ino())
            );
        }
        assert!(storage::size(&path, storage::CAPTURE_BYTES).is_ok());
        // Once the unrelated admission lock is released, the store is STILL
        // full: recovery/SQL/manifest never surrendered A's live reservation.
        drop(admission_lock);
        assert_eq!(
            storage::capture_dir(&root, fifth_id)
                .unwrap_err()
                .to_string(),
            "capture storage budget exhausted"
        );
        assert!(path.join("active.json").exists());
        drop(lease); // only ordinary lease release may surrender the reserve
        let fifth = storage::capture_dir(&root, fifth_id).unwrap();
        let _fifth_lease = storage::CaptureLease::new(&fifth);
        assert!(fifth.join("active.json").is_file());
        assert!(storage::is_owned(&fifth));
        assert!(!path.exists()); // completed A may now be retired normally
        for (other, _) in &other_captures {
            assert!(other.join("active.json").is_file());
        }
        assert_eq!(fs::read(foreign.join("keep")).unwrap(), b"foreign-original");
    }

    #[test]
    fn sql_finalization_reserves_file_and_entry_slots_including_live_markers() {
        let temp = tempfile::tempdir().unwrap();
        let root = storage::private_root(&temp.path().join("diagnostics")).unwrap();
        let path = storage::capture_dir(&root, "abcdefghijklmnopqrstuvwx").unwrap();
        let lease = storage::CaptureLease::new(&path);
        assert!(path.join("owner.json").is_file());
        assert!(path.join("active.json").is_file());
        // 1020 native files + two live markers. SQL and manifest must both
        // still fit before active.json is released.
        for i in 0..1020 {
            fs::write(path.join(format!("native-{i}")), b"").unwrap();
        }
        assert!(native_size(&path, true).is_ok());
        fs::write(path.join("excess-file"), b"").unwrap();
        assert_eq!(
            native_size(&path, true).unwrap_err().to_string(),
            "artifact count overflow"
        );
        fs::remove_file(path.join("excess-file")).unwrap();
        // 1022 files +1024 directories =2046 entries before finalization.
        for i in 0..1024 {
            fs::create_dir(path.join(format!("directory-{i}"))).unwrap();
        }
        assert!(native_size(&path, true).is_ok());
        fs::create_dir(path.join("excess-directory")).unwrap();
        assert_eq!(
            native_size(&path, true).unwrap_err().to_string(),
            "artifact directory entry overflow"
        );
        fs::remove_dir(path.join("excess-directory")).unwrap();
        let target = crate::diagnostics::tests::target(7);
        let snapshot = crate::query_cpu::aggregation_fixtures::large_snapshot();
        let mut manifest = failed_manifest(
            "fixture",
            &target,
            &config(CaptureMode::Cpu),
            42,
            "start",
            "native_failed".into(),
            Vec::new(),
        );
        store_sql_cpu(&path, &target, &mut manifest, &snapshot);
        assert_eq!(manifest.sql_cpu.as_ref().unwrap()["status"], "recorded");
        storage::write_new_json(&path.join("manifest.json"), &manifest).unwrap();
        // All 1024 files /2048 entries exist while lease is still held.
        assert!(storage::size(&path, storage::CAPTURE_BYTES).is_ok());
        assert!(path.join("active.json").exists());
        drop(lease);
        assert!(!path.join("active.json").exists());
        assert!(storage::size(&path, storage::CAPTURE_BYTES).is_ok());
        let saved: CaptureManifest = serde_json::from_slice(
            &storage::read_bounded(&path.join("manifest.json"), 8 * 1024 * 1024).unwrap(),
        )
        .unwrap();
        assert_eq!(
            saved
                .files
                .iter()
                .filter(|f| f.filename == "sql-cpu.json")
                .count(),
            1
        );
        assert_eq!(saved.sql_cpu.unwrap()["status"], "recorded");
    }

    #[test]
    fn sql_storage_failure_is_explicit_and_preserves_foreign_collision() {
        let target = crate::diagnostics::tests::target(7);
        let snapshot = crate::query_cpu::aggregation_fixtures::large_snapshot();
        for (native_failed, collision) in [(false, true), (true, true), (false, false)] {
            let temp = tempfile::tempdir().unwrap();
            let mut manifest = failed_manifest(
                "fixture",
                &target,
                &config(CaptureMode::Cpu),
                42,
                "start",
                "fixture".into(),
                Vec::new(),
            );
            if !native_failed {
                manifest.status = "completed".into();
            }
            if collision {
                fs::write(temp.path().join("sql-cpu.json"), b"foreign-original").unwrap();
            } else {
                // Quota refusal must also be unavailable, even before writing.
                fs::File::create(temp.path().join("perf.data"))
                    .unwrap()
                    .set_len(native_storage_limits(true).0 + 1)
                    .unwrap();
            }
            store_sql_cpu(temp.path(), &target, &mut manifest, &snapshot);
            assert_eq!(manifest.sql_cpu.as_ref().unwrap()["status"], "unavailable");
            assert_eq!(
                manifest.sql_cpu.as_ref().unwrap()["reason"],
                "sql_cpu_storage_or_inventory_failed"
            );
            assert!(
                manifest
                    .incomplete_reasons
                    .iter()
                    .any(|r| r == "sql_cpu_sidecar_unavailable")
            );
            assert_eq!(
                manifest.status,
                if native_failed { "failed" } else { "partial" }
            );
            if collision {
                assert_eq!(
                    fs::read(temp.path().join("sql-cpu.json")).unwrap(),
                    b"foreign-original"
                );
            } else {
                assert!(!temp.path().join("sql-cpu.json").exists());
            }
            assert!(!manifest.files.iter().any(|f| f.filename == "sql-cpu.json"));
        }
    }

    #[test]
    fn profile_reference_requires_native_data_and_explicit_status() {
        let target = crate::diagnostics::tests::target(7);
        let mut m = failed_manifest(
            "fixture",
            &target,
            &config(CaptureMode::Cpu),
            42,
            "start",
            "prerequisite_failed".into(),
            vec![ArtifactFile {
                filename: "owner.json".into(),
                bytes: 64,
                sha256: "fixture".into(),
            }],
        );
        assert!(!m.profile_data_present());
        m.files.push(ArtifactFile {
            filename: "sql-cpu.json".into(),
            bytes: 128,
            sha256: "fixture".into(),
        });
        m.sql_cpu = Some(serde_json::json!({"filename":"sql-cpu.json", "status":"recorded"}));
        m.status = "partial".into();
        assert!(
            !m.profile_data_present(),
            "SQL sidecar is not a native profile"
        );
        m.status = "failed".into();
        m.files.push(ArtifactFile {
            filename: "perf.data".into(),
            bytes: 32,
            sha256: "fixture".into(),
        });
        assert!(!m.profile_data_present());
        m.status = "partial".into();
        assert!(m.profile_data_present());
        m.status = "completed".into();
        assert!(m.profile_data_present());
        m.mode = CaptureMode::Vm;
        m.files.push(ArtifactFile {
            filename: "tool-stderr.txt".into(),
            bytes: 128,
            sha256: "fixture".into(),
        });
        assert!(!m.profile_data_present()); // permission/diagnostic text is not VM data
        m.files.push(ArtifactFile {
            filename: "tool-output.txt".into(),
            bytes: 32,
            sha256: "fixture".into(),
        });
        assert!(m.profile_data_present());
    }
    #[test]
    fn limits_and_preparation_contract() {
        let mut c = config(CaptureMode::Cpu);
        c.seconds = 61;
        assert!(plan(&c, 42, Path::new("/capture"), "not_prepared").is_err());
        c.seconds = 10;
        c.frequency_hz = 1000;
        assert!(plan(&c, 42, Path::new("/capture"), "not_prepared").is_err());
        #[cfg(target_os = "macos")]
        {
            assert!(
                plan(
                    &config(CaptureMode::AllocationHistory),
                    42,
                    Path::new("/capture"),
                    "not_prepared"
                )
                .is_err()
            );
            let p = plan(
                &config(CaptureMode::LiveAllocations),
                42,
                Path::new("/capture"),
                "full_history_since_launch",
            )
            .unwrap();
            assert_eq!(p.args, ["42", "-allBySize"]);
        }
    }
    #[test]
    fn cpu_selects_target_pid_and_never_launches_it() {
        let c = config(CaptureMode::Cpu);
        #[cfg(any(target_os = "macos", target_os = "linux"))]
        {
            let p = plan(&c, 424242, Path::new("/capture"), "not_prepared").unwrap();
            assert!(p.args.iter().any(|s| s == "424242"));
            assert!(!p.args.iter().any(|s| s == "--launch"));
        }
    }
    #[test]
    fn permission_failure_is_not_success() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(
            temp.path().join("tool-output.txt"),
            b"task_for_pid failed: not permitted",
        )
        .unwrap();
        assert_eq!(classify_error(temp.path()), "permission_or_signing_denied");
    }
}

#[cfg(test)]
pub(super) fn fixture_manifest(id: &str, target: &TargetSnapshot) -> CaptureManifest {
    failed_manifest(
        id,
        target,
        &CaptureRequest {
            mode: CaptureMode::Cpu,
            seconds: 1,
            frequency_hz: 99,
            child_generation: None,
        },
        target.run.pid,
        "fixture-start",
        "tool_unavailable".into(),
        Vec::new(),
    )
}
fn failed_manifest(
    id: &str,
    target: &TargetSnapshot,
    config: &CaptureRequest,
    pid: u32,
    start: &str,
    reason: String,
    files: Vec<ArtifactFile>,
) -> CaptureManifest {
    let now = monotonic_ms();
    let wall = unix_ms();
    CaptureManifest {
        schema_version: 1,
        capture_id: id.into(),
        run: target.run.clone(),
        consent_generation: target.consent_generation,
        target_pid: pid,
        target_start_identity: start.into(),
        target_child_generation: config.child_generation,
        target_build_scope: if config.child_generation.is_none() {
            "target_executable"
        } else {
            "parent provenance; child build unknown"
        }
        .into(),
        mode: config.mode,
        tool: "not_started_or_prerequisite_failed".into(),
        tool_version: "not_recorded".into(),
        configuration: vec![
            format!("requested_duration_seconds={}", config.seconds),
            format!("requested_frequency_hz={}", config.frequency_hz),
        ],
        actual_format: "no_completed_profile; partial files individually inventoried".into(),
        started_unix_ms: wall,
        ended_unix_ms: wall,
        started_monotonic_ms: now,
        ended_monotonic_ms: now,
        clock_scope: "failure observed at target monotonic instant; sampling window not recorded"
            .into(),
        module: None,
        module_coverage: "not_recorded".into(),
        symbol_status: "not_recorded".into(),
        coverage: "not_recorded; zero samples NOT asserted".into(),
        lost_samples: None,
        status: "failed".into(),
        incomplete_reasons: vec![reason],
        files,
        sql_cpu: None,
        local_reference: format!("captures/pioneer-capture-{id}/manifest.json"),
        correlation_scope:
            "attempt linked by run/build/PID-start; no successful sampling or symbolication claimed"
                .into(),
    }
}

#[cfg(all(test, unix))]
mod fake_tool_tests {
    use super::*;
    #[test]
    fn missing_and_nonzero_fake_tools_never_report_version_success() {
        let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
        let old = crate::telemetry_enabled();
        crate::set_telemetry_enabled(false);
        crate::set_telemetry_enabled(true);
        let epoch = crate::telemetry_consent_snapshot().1;
        let temp = tempfile::tempdir().unwrap();
        let stop = AtomicBool::new(false);
        let cancel = AtomicBool::new(false);
        let mut p = ToolPlan {
            program: temp.path().join("missing-tool"),
            args: vec![],
            format: "fixture",
            version_args: vec!["--version".into()],
            graceful_stop: false,
        };
        assert!(
            version(&p, temp.path(), epoch, &stop, &cancel)
                .unwrap_err()
                .to_string()
                .contains("tool_unavailable")
        );
        p.program = "/bin/sh".into();
        p.version_args = vec!["-c".into(), "printf partial; exit 7".into()];
        assert!(version(&p, temp.path(), epoch, &stop, &cancel).is_err());
        assert_eq!(
            fs::read(temp.path().join("tool-version.txt")).unwrap(),
            b"partial"
        );
        p.version_args = vec!["-c".into(), "exec sleep 30".into()];
        cancel.store(true, Ordering::Release);
        let started = Instant::now();
        assert!(version(&p, temp.path(), epoch, &stop, &cancel).is_err());
        assert!(started.elapsed() < Duration::from_secs(6));
        crate::set_telemetry_enabled(old);
    }
    #[test]
    fn owned_tool_drop_reaps_only_its_child_and_keeps_partial_output() {
        let temp = tempfile::tempdir().unwrap();
        let out = fs::File::create(temp.path().join("partial.txt")).unwrap();
        let child = Command::new("/bin/sh")
            .args(["-c", "exec sleep 30"])
            .stdout(Stdio::from(out))
            .spawn()
            .unwrap();
        let pid = child.id();
        drop(OwnedTool(child));
        let mut status = 0;
        assert_eq!(
            unsafe { libc::waitpid(pid as i32, &mut status, libc::WNOHANG) },
            -1
        );
        assert!(temp.path().join("partial.txt").is_file());
        assert!(read_process(std::process::id()).is_ok());
    }
    #[test]
    fn oversized_output_is_retained_as_bounded_partial_not_completed() {
        let temp = tempfile::tempdir().unwrap();
        let f = fs::File::create(temp.path().join("perf.data")).unwrap();
        f.set_len(129 * 1024 * 1024).unwrap();
        assert!(storage::inventory(temp.path()).is_err());
        trim_overflow(temp.path(), false).unwrap();
        assert_eq!(
            storage::inventory(temp.path()).unwrap()[0].bytes,
            64 * 1024 * 1024
        );
    }
}

#[cfg(all(test, any(unix, windows)))]
mod stop_status_tests {
    use super::*;
    #[test]
    fn version_stop_preserves_known_status_and_grace_without_inventing_unknown_exit() {
        let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
        let old = crate::telemetry_enabled();
        crate::set_telemetry_enabled(false);
        crate::set_telemetry_enabled(true);
        for (index, (success, graceful)) in [(true, true), (false, true), (false, false)]
            .into_iter()
            .enumerate()
        {
            #[cfg(unix)]
            let status = {
                use std::os::unix::process::ExitStatusExt;
                std::process::ExitStatus::from_raw(if success { 0 } else { 7 << 8 })
            };
            #[cfg(windows)]
            let status = {
                use std::os::windows::process::ExitStatusExt;
                std::process::ExitStatus::from_raw(if success { 0 } else { 7 })
            };
            let id = 742200 + index as u64;
            let mut registration = ChildRegistration::register(
                Some(std::process::id()),
                ChildRole::Profiler,
                Some(id),
            );
            assert_eq!(
                observed_tool_stop(&mut registration, Some(status), graceful),
                graceful
            );
            let _version_result: Result<()> =
                Err(anyhow::anyhow!("tool_version_timeout_or_cancelled"));
            drop(registration);
            let snapshot = crate::resources::resource_snapshot().unwrap();
            let child = snapshot
                .children
                .iter()
                .find(|c| c.owner_generation == Some(id))
                .unwrap();
            assert_eq!(
                child.outcome,
                if success {
                    "exited_success"
                } else {
                    "exited_failure"
                }
            );
            assert!(
                child.exited_monotonic_ms.is_some() && child.ownership_ended_monotonic_ms.is_some()
            );
            assert_eq!(child.final_usage, "incomplete_no_wait_rusage");
            let token = child.work.unwrap().token;
            assert_eq!(crate::live::snapshot().unwrap().journal.iter().filter(|r| matches!(&r.payload, crate::live::JournalPayload::Work(w) if w.context.token == token && matches!(w.state, crate::live::WorkState::Terminal(_)))).count(),1);
        }
        let mut registration = ChildRegistration::register(
            Some(std::process::id()),
            ChildRole::Profiler,
            Some(742299),
        );
        assert!(!observed_tool_stop(&mut registration, None, false));
        drop(registration);
        let snapshot = crate::resources::resource_snapshot().unwrap();
        let child = snapshot
            .children
            .iter()
            .find(|c| c.owner_generation == Some(742299))
            .unwrap();
        assert_eq!(child.outcome, "owner_dropped_exit_unknown");
        assert!(child.exited_monotonic_ms.is_none());
        crate::set_telemetry_enabled(old);
    }
}
