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
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
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
    SqlCpu,
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
            CaptureMode::SqlCpu => false,
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
    ensure!(
        config.mode != CaptureMode::SqlCpu,
        "SQL CPU has no native plan"
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
            CaptureMode::SqlCpu => unreachable!("SQL CPU bypasses native adapters"),
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
// One capture-local, first-failure latch. Only classified identity results
// enter here, never arbitrary native tool errors. Successful reads cannot clear it.
#[derive(Default)]
struct NativeIdentityFailure(Mutex<Option<&'static str>>);
impl NativeIdentityFailure {
    fn record(&self, reason: &'static str) {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_or_insert(reason);
    }
    fn get(&self) -> Option<&'static str> {
        *self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}
fn verify_native_identity(
    pid: u32,
    start: &str,
    failure: Option<&NativeIdentityFailure>,
    #[cfg(test)] boundary: tests::NativeIdentityBoundary,
) -> Result<()> {
    #[cfg(test)]
    let observed = tests::NATIVE_IDENTITY
        .with(|slot| slot.borrow_mut().as_mut().and_then(|read| read(boundary)))
        .unwrap_or_else(|| crate::resources::process_start_identity(pid));
    #[cfg(not(test))]
    let observed = crate::resources::process_start_identity(pid);
    let result = identity_result(start, observed);
    if let (Some(failure), Err(reason)) = (failure, result) {
        // Publish before the caller starts bounded stop/reap/finalization.
        failure.record(reason);
    }
    result.map_err(anyhow::Error::msg)
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
    ensure!(
        (1..=60).contains(&config.seconds),
        "capture duration must be 1..60 seconds"
    );
    ensure!(
        config.mode != CaptureMode::SqlCpu
            || sql_cpu_eligible(&target.service, &config, target.run.pid),
        "unsupported_target: SQL CPU requires the owner parent Gateway without --child"
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
    let _consent_cleanup = CaptureConsentCleanup {
        path: &path,
        epoch,
        stop,
    };
    // Admission and the full SQL/manifest reserve precede all native preparation.
    if reserve_sql_cpu {
        native_size(&path, true)?;
    }
    let mut sql_cpu = SqlCpuCapture::default();
    if reserve_sql_cpu {
        sql_cpu.start(config.seconds, epoch);
    }
    let native_identity_failure = NativeIdentityFailure::default();
    let mut attempt = None;
    #[cfg(test)]
    let native_fixture = tests::NATIVE_PLAN.with(|slot| slot.borrow_mut().take());
    let outcome = if config.mode == CaptureMode::SqlCpu {
        sql_cpu.wait(epoch, pid, &start_identity, stop, cancel);
        Ok(sql_only_manifest(
            &capture_id,
            target,
            &config,
            &start_identity,
            &sql_cpu,
        ))
    } else {
        sql_cpu.run_native(
            epoch,
            pid,
            &start_identity,
            stop,
            cancel,
            &native_identity_failure,
            |native_stop| {
                #[cfg(test)]
                let native_done = native_fixture.as_ref().map(|f| f.done.clone());
                let outcome = capture_inner(
                    &path,
                    &capture_id,
                    target,
                    config.clone(),
                    pid,
                    &start_identity,
                    native_stop,
                    cancel,
                    &mut attempt,
                    reserve_sql_cpu.then_some(&native_identity_failure),
                    #[cfg(test)]
                    native_fixture.map(|f| f.plan),
                );
                #[cfg(test)]
                if let Some(done) = native_done {
                    done.store(true, Ordering::Release);
                }
                // Record the native cleanup boundary now, not after waiting for SQL.
                if let Some(attempt) = &mut attempt {
                    attempt
                        .ended
                        .get_or_insert_with(|| (unix_ms(), monotonic_ms()));
                }
                outcome
            },
        )
    };
    if !crate::telemetry_sample_allowed(Some(epoch)) || stop.load(Ordering::Acquire) {
        storage::remove_owned(&path);
        bail!("capture_shutdown_or_consent_revoked");
    }
    if let Some(reason) = sql_cpu.identity_failure {
        storage::remove_owned(&path);
        bail!("{reason}");
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
    if reserve_sql_cpu {
        if let Err(error) = verify_identity(pid, &start_identity) {
            storage::remove_owned(&path);
            return Err(error);
        }
    }
    if config.mode == CaptureMode::SqlCpu {
        manifest.files = capture_inventory(&path, true)?;
    }
    if let Some(snapshot) = &sql_cpu.snapshot {
        if !crate::telemetry_sample_allowed(Some(epoch)) || stop.load(Ordering::Acquire) {
            storage::remove_owned(&path);
            bail!("capture_shutdown_or_consent_revoked");
        }
        store_sql_cpu(&path, target, &mut manifest, snapshot);
    }
    if manifest.sql_cpu.is_none() && sql_cpu_eligible(&target.service, &config, pid) {
        manifest.sql_cpu = Some(serde_json::json!({
            "status":"unavailable", "reason":"sql_cpu_session_unavailable",
            "native_profile_status": if config.mode == CaptureMode::SqlCpu { "not_requested" } else { &manifest.status },
            "native_profile_interval": native_profile_interval(&manifest),
        }));
        if !manifest
            .incomplete_reasons
            .iter()
            .any(|r| r == "sql_cpu_session_unavailable")
        {
            manifest
                .incomplete_reasons
                .push("sql_cpu_session_unavailable".into());
        }
        if manifest.status == "completed" {
            manifest.status = if config.mode == CaptureMode::SqlCpu {
                "failed"
            } else {
                "partial"
            }
            .into();
        }
    }
    if let Some(reason) = sql_cpu.closed_reason.filter(|r| *r != "deadline") {
        if !manifest.incomplete_reasons.iter().any(|r| r == reason) {
            manifest.incomplete_reasons.push(reason.into());
        }
        if manifest.status == "completed" {
            manifest.status = "partial".into();
        }
    }
    // Identity loss cannot authorize storage of a result for a different target.
    if reserve_sql_cpu {
        if let Err(error) = verify_identity(pid, &start_identity) {
            storage::remove_owned(&path);
            return Err(error);
        }
    }
    if !crate::telemetry_sample_allowed(Some(epoch)) || stop.load(Ordering::Acquire) {
        storage::remove_owned(&path);
        bail!("consent_revoked_before_capture_commit");
    }
    #[cfg(test)]
    tests::BEFORE_COMMIT.with(|slot| {
        if let Some(hook) = slot.borrow_mut().take() {
            hook(&path);
        }
    });
    storage::write_new_json(&path.join("manifest.json"), &manifest)?;
    if reserve_sql_cpu {
        if let Err(error) = verify_identity(pid, &start_identity) {
            storage::remove_owned(&path);
            return Err(error);
        }
    }
    if !crate::telemetry_sample_allowed(Some(epoch)) || stop.load(Ordering::Acquire) {
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
            native_profile_status: if manifest.mode == CaptureMode::SqlCpu {
                "not_requested"
            } else {
                &manifest.status
            },
            native_profile_interval: native_profile_interval(manifest),
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
        "native_profile_status": if manifest.mode == CaptureMode::SqlCpu { "not_requested" } else { &manifest.status },
        "native_profile_interval": native_profile_interval(manifest),
        "started_unix_ms": snapshot.started_unix_ms,
        "elapsed_ns": snapshot.elapsed_ns,
        "requested_seconds": snapshot.requested_seconds,
        "closed_reason": snapshot.closed_reason,
        "window_complete": snapshot.closed_reason == "deadline",
        "limitations": snapshot.limitations,
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
                manifest.status = if manifest.mode == CaptureMode::SqlCpu {
                    "failed"
                } else {
                    "partial"
                }
                .into();
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
// Keep revoke/shutdown deletion authoritative on storage/recovery `?` exits as
// well. Declared before SQL ownership, so Session and joined native work finish
// before this guard can remove the admitted directory.
struct CaptureConsentCleanup<'a> {
    path: &'a Path,
    epoch: u64,
    stop: &'a AtomicBool,
}
impl Drop for CaptureConsentCleanup<'_> {
    fn drop(&mut self) {
        if self.stop.load(Ordering::Acquire) || !crate::telemetry_sample_allowed(Some(self.epoch)) {
            storage::remove_owned(self.path);
        }
    }
}

fn native_profile_interval(manifest: &CaptureManifest) -> serde_json::Value {
    if manifest.mode == CaptureMode::SqlCpu || manifest.tool == "not_started_or_prerequisite_failed"
    {
        serde_json::Value::Null
    } else {
        serde_json::json!({
            "started_monotonic_ms": manifest.started_monotonic_ms,
            "ended_monotonic_ms": manifest.ended_monotonic_ms,
            "clock_scope": manifest.clock_scope,
        })
    }
}

#[derive(Default)]
struct SqlCpuCapture {
    session: Option<crate::query_cpu::Session>,
    snapshot: Option<crate::query_cpu::Snapshot>,
    deadline: Option<Instant>,
    closed_reason: Option<&'static str>,
    identity_failure: Option<&'static str>,
}
impl SqlCpuCapture {
    fn start(&mut self, seconds: u64, epoch: u64) {
        self.session = crate::query_cpu::Session::start(seconds, epoch);
        self.deadline = self
            .session
            .as_ref()
            .map(crate::query_cpu::Session::deadline);
    }
    fn poll(&mut self, signal: Option<&'static str>) {
        // An observed loss remains terminal even if a later identity read
        // recovers, including after SQL deadline during native finalization.
        if let Some(reason) = signal.filter(|r| r.starts_with("target_")) {
            self.identity_failure.get_or_insert(reason);
        }
        if self.session.is_some()
            && (signal.is_some() || self.deadline.is_some_and(|d| Instant::now() >= d))
        {
            let mut snapshot = self.session.take().unwrap().finish();
            // The collector's original deadline determines whether SQL ran its
            // full window. A cancel processed later cannot make that partial.
            if snapshot.closed_reason != "deadline" {
                if let Some(reason) = signal {
                    snapshot.closed_reason = reason;
                }
            }
            self.closed_reason = Some(snapshot.closed_reason);
            self.snapshot = Some(snapshot);
        }
    }
    fn pause(&self) {
        let slice = Duration::from_millis(50);
        let remaining = if self.session.is_some() {
            self.deadline
                .unwrap()
                .saturating_duration_since(Instant::now())
                .min(slice)
        } else {
            slice
        };
        // No collector mutex, SQL scope or SQLite capacity is held while waiting.
        std::thread::sleep(remaining);
    }
    fn wait(&mut self, epoch: u64, pid: u32, start: &str, stop: &AtomicBool, cancel: &AtomicBool) {
        while self.session.is_some() {
            self.poll(sql_window_signal(epoch, pid, start, stop, cancel));
            if self.session.is_some() {
                self.pause();
            }
        }
    }
    fn run_native(
        &mut self,
        epoch: u64,
        pid: u32,
        start: &str,
        stop: &AtomicBool,
        cancel: &AtomicBool,
        identity_failure: &NativeIdentityFailure,
        native: impl FnOnce(&AtomicBool) -> Result<CaptureManifest> + Send,
    ) -> Result<CaptureManifest> {
        if self.session.is_none() {
            let result = native(stop);
            self.poll(identity_failure.get());
            return result;
        }
        let native_stop = AtomicBool::new(false);
        // The existing capture owner keeps the SQL Session. Native work is
        // joined before recovery/storage/lease release; it can never detach.
        std::thread::scope(|scope| {
            let worker = std::thread::Builder::new()
                .name("pioneer-native-capture".into())
                .spawn_scoped(scope, || native(&native_stop));
            match worker {
                Ok(worker) => {
                    loop {
                        let owner_signal = sql_window_signal(epoch, pid, start, stop, cancel);
                        let signal = identity_failure.get().or(owner_signal);
                        if signal.is_some() {
                            native_stop.store(true, Ordering::Release);
                        }
                        self.poll(signal);
                        if self.session.is_none() {
                            #[cfg(test)]
                            tests::BEFORE_NATIVE_FINISHED_CHECK.with(|slot| {
                                if let Some(hook) = slot.borrow_mut().take() {
                                    hook(&|| worker.is_finished());
                                }
                            });
                            if worker.is_finished() {
                                break;
                            }
                        }
                        self.pause();
                    }
                    let result = worker.join();
                    // A worker can publish between the last latch read and
                    // is_finished. Join first, then drain even on worker error.
                    self.poll(identity_failure.get());
                    result.map_err(|_| anyhow::anyhow!("native_capture_worker_panicked"))?
                }
                Err(error) => {
                    // Thread admission is a native failure too, not SQL cancel.
                    self.wait(epoch, pid, start, stop, cancel);
                    Err(error.into())
                }
            }
        })
    }
}
fn sql_window_signal(
    epoch: u64,
    pid: u32,
    start: &str,
    stop: &AtomicBool,
    cancel: &AtomicBool,
) -> Option<&'static str> {
    if !crate::telemetry_sample_allowed(Some(epoch)) {
        Some("consent_epoch_changed")
    } else if stop.load(Ordering::Acquire) {
        Some("capture_shutdown")
    } else if cancel.load(Ordering::Acquire) {
        Some("operator_cancelled")
    } else {
        identity_result(start, crate::resources::process_start_identity(pid)).err()
    }
}
fn sql_cpu_eligible(service: &str, config: &CaptureRequest, pid: u32) -> bool {
    service == "gateway"
        && matches!(config.mode, CaptureMode::Cpu | CaptureMode::SqlCpu)
        && config.child_generation.is_none()
        && pid == std::process::id()
}
fn sql_only_manifest(
    id: &str,
    target: &TargetSnapshot,
    config: &CaptureRequest,
    start: &str,
    sql: &SqlCpuCapture,
) -> CaptureManifest {
    let mut manifest = failed_manifest(
        id,
        target,
        config,
        target.run.pid,
        start,
        "sql_cpu_session_unavailable".into(),
        Vec::new(),
    );
    manifest.target_build_scope =
        "owner Gateway run/build; native module identity not requested".into();
    manifest.correlation_scope = "observed SQL window linked by exact Gateway run/build/PID-start; no native profile or arbitrary async stack attribution".into();
    manifest.tool = "pioneer.query_cpu".into();
    manifest.tool_version = "sql_cpu_schema_v2".into();
    manifest.configuration = vec![format!("requested_duration_seconds={}", config.seconds)];
    manifest.actual_format =
        "pioneer.gateway.sql_cpu JSON schema v2; native profile not requested".into();
    manifest.module = None;
    manifest.module_coverage =
        "not_applicable; native profile not requested; exact run/build retained".into();
    manifest.symbol_status = "not_applicable; native profile not requested".into();
    manifest.coverage = "observed instrumented SQL command CPU only; see sql_cpu coverage/limitations; native profile not requested".into();
    manifest.clock_scope =
        "SQL Session target monotonic window; ended_unix_ms projects started_unix_ms + elapsed_ms; native profile not requested".into();
    if let Some(snapshot) = &sql.snapshot {
        manifest.started_unix_ms = snapshot.started_unix_ms;
        manifest.ended_unix_ms = snapshot.started_unix_ms + snapshot.elapsed_ns / 1_000_000;
        manifest.started_monotonic_ms = snapshot.started_monotonic_ms;
        manifest.ended_monotonic_ms = snapshot.ended_monotonic_ms;
        manifest.status = if snapshot.closed_reason == "deadline" {
            "completed"
        } else {
            "partial"
        }
        .into();
        manifest.incomplete_reasons = sql
            .closed_reason
            .filter(|r| *r != "deadline")
            .map(str::to_owned)
            .into_iter()
            .collect();
    } else {
        manifest.clock_scope = "SQL Session unavailable; no SQL or native interval recorded".into();
    }
    manifest
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
    identity_failure: Option<&NativeIdentityFailure>,
    #[cfg(test)] native_fixture: Option<tests::NativePlan>,
) -> Result<CaptureManifest> {
    let epoch = target.consent_generation;
    let reserve_sql_cpu = sql_cpu_eligible(&target.service, &config, pid);
    #[cfg(test)]
    let fixture = native_fixture.is_some();
    #[cfg(not(test))]
    let plan = plan(&config, pid, path, &target.stack_logging)?;
    #[cfg(test)]
    let plan = match native_fixture {
        Some(plan) => plan(path).map_err(anyhow::Error::msg),
        None => plan(&config, pid, path, &target.stack_logging),
    }?;
    let module = if config.child_generation.is_none() {
        #[cfg(test)]
        let module = if fixture {
            None
        } else {
            Some(child_module(pid, epoch, stop, cancel)?)
        };
        #[cfg(not(test))]
        let module = Some(child_module(pid, epoch, stop, cancel)?);
        module
    } else {
        child_module(pid, epoch, stop, cancel).ok()
    };
    let tool_version = version(&plan, path, epoch, stop, cancel)?;
    verify_native_identity(
        pid,
        start_identity,
        identity_failure,
        #[cfg(test)]
        tests::NativeIdentityBoundary::BeforeAttach,
    )?;
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
                return Err(error.into());
            }
        };
        if let Some(exit) = exited {
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
            stop_tool(&mut child, plan.graceful_stop, &mut registration);
            break;
        }
        if !crate::telemetry_sample_allowed(Some(epoch)) || stop.load(Ordering::Acquire) {
            stop_tool(&mut child, plan.graceful_stop, &mut registration);
            bail!("consent_revoked_or_shutdown");
        }
        if let Err(error) = verify_native_identity(
            pid,
            start_identity,
            identity_failure,
            #[cfg(test)]
            tests::NativeIdentityBoundary::Recording,
        ) {
            reasons.push(error.to_string());
            status = "partial";
            stop_tool(&mut child, plan.graceful_stop, &mut registration);
            break;
        }
        if native_size(path, reserve_sql_cpu).is_err() {
            reasons.push("output_overflow".into());
            status = "partial";
            stop_tool(&mut child, plan.graceful_stop, &mut registration);
            trim_overflow(path, reserve_sql_cpu)?;
            break;
        }
        #[cfg(target_os = "linux")]
        if config.mode == CaptureMode::Cpu
            && started.elapsed() >= Duration::from_secs(config.seconds)
        {
            if !stop_tool(&mut child, true, &mut registration) {
                status = "partial";
                reasons.push("profiler_forced_termination_after_grace".into());
            }
            break;
        }
        if started.elapsed() >= timeout {
            reasons.push("tool_timeout".into());
            status = "partial";
            stop_tool(&mut child, plan.graceful_stop, &mut registration);
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let ended_unix_ms = unix_ms();
    let ended_monotonic_ms = monotonic_ms();
    if let Some(attempt) = attempt {
        attempt.ended = Some((ended_unix_ms, ended_monotonic_ms));
    }
    if let Err(error) = verify_native_identity(
        pid,
        start_identity,
        identity_failure,
        #[cfg(test)]
        tests::NativeIdentityBoundary::AfterStop,
    ) {
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
    #[derive(Clone, Copy, PartialEq, Eq)]
    pub(super) enum NativeIdentityBoundary {
        BeforeAttach,
        Recording,
        AfterStop,
    }
    type NativeIdentityRead =
        Box<dyn FnMut(NativeIdentityBoundary) -> Option<std::io::Result<String>>>;
    type NativeFinishedCheck = Box<dyn FnOnce(&dyn Fn() -> bool)>;
    pub(super) type NativePlan = Box<dyn FnOnce(&Path) -> Result<ToolPlan, &'static str> + Send>;
    pub(super) struct NativeFixture {
        pub(super) plan: NativePlan,
        pub(super) done: std::sync::Arc<AtomicBool>,
    }
    std::thread_local! {
        pub(super) static NATIVE_PLAN: std::cell::RefCell<Option<NativeFixture>> = const { std::cell::RefCell::new(None) };
        pub(super) static NATIVE_IDENTITY: std::cell::RefCell<Option<NativeIdentityRead>> = const { std::cell::RefCell::new(None) };
        pub(super) static BEFORE_NATIVE_FINISHED_CHECK: std::cell::RefCell<Option<NativeFinishedCheck>> = const { std::cell::RefCell::new(None) };
        pub(super) static BEFORE_COMMIT: std::cell::RefCell<Option<Box<dyn FnOnce(&Path)>>> = const { std::cell::RefCell::new(None) };
    }
    // These regressions use the production capture boundary, including its
    // admitted directory/lease, Session, native error recovery and SQL writer.
    struct ConsentFixture {
        old: bool,
        _telemetry: std::sync::MutexGuard<'static, ()>,
        _session: std::sync::MutexGuard<'static, ()>,
    }
    impl ConsentFixture {
        fn new() -> Self {
            let telemetry = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
            let session = crate::query_cpu::tests::SERIAL.lock().unwrap();
            let old = crate::telemetry_enabled();
            crate::set_telemetry_enabled(true);
            Self {
                old,
                _telemetry: telemetry,
                _session: session,
            }
        }
    }
    impl Drop for ConsentFixture {
        fn drop(&mut self) {
            NATIVE_PLAN.with(|slot| {
                slot.borrow_mut().take();
            });
            NATIVE_IDENTITY.with(|slot| {
                slot.borrow_mut().take();
            });
            BEFORE_NATIVE_FINISHED_CHECK.with(|slot| {
                slot.borrow_mut().take();
            });
            BEFORE_COMMIT.with(|slot| {
                slot.borrow_mut().take();
            });
            crate::set_telemetry_enabled(self.old);
        }
    }
    fn local_target() -> (TargetSnapshot, TargetDescriptor) {
        let target = crate::diagnostics::tests::target(crate::telemetry_consent_snapshot().1);
        let descriptor = TargetDescriptor {
            schema_version: 1,
            service: target.service.clone(),
            run: target.run.clone(),
            start_identity: crate::resources::process_start_identity(target.run.pid).unwrap(),
            endpoint: "127.0.0.1:1".parse().unwrap(),
            token: "fixture".into(),
        };
        (target, descriptor)
    }
    fn read_sql(root: &Path, manifest: &CaptureManifest) -> serde_json::Value {
        use sha2::{Digest, Sha256};
        let directory = root
            .join(&manifest.local_reference)
            .parent()
            .unwrap()
            .to_owned();
        let bytes =
            storage::read_bounded(&directory.join("sql-cpu.json"), storage::SQL_CPU_BYTES).unwrap();
        let file = manifest
            .files
            .iter()
            .find(|f| f.filename == "sql-cpu.json")
            .unwrap();
        assert_eq!(file.bytes, bytes.len() as u64);
        assert_eq!(file.sha256, hex::encode(Sha256::digest(&bytes)));
        assert_eq!(manifest.sql_cpu.as_ref().unwrap()["bytes"], file.bytes);
        storage::validate_private_file(&directory.join("sql-cpu.json")).unwrap();
        assert!(!directory.join("active.json").exists());
        let saved: CaptureManifest =
            serde_json::from_slice(&fs::read(directory.join("manifest.json")).unwrap()).unwrap();
        assert_eq!(saved.sql_cpu, manifest.sql_cpu);
        serde_json::from_slice(&bytes).unwrap()
    }
    fn sql_config() -> CaptureRequest {
        CaptureRequest {
            seconds: 1,
            frequency_hz: 0,
            ..config(CaptureMode::SqlCpu)
        }
    }
    fn native_fixture(plan: Result<ToolPlan, &'static str>) -> std::sync::Arc<AtomicBool> {
        let done = std::sync::Arc::new(AtomicBool::new(false));
        let make_plan: NativePlan = Box::new(move |path| {
            plan.map(|mut plan| {
                if plan.program == Path::new("/bin/sh") {
                    plan.args
                        .push(path.join("perf.data").to_string_lossy().into_owned());
                }
                plan
            })
        });
        NATIVE_PLAN.with(|slot| {
            *slot.borrow_mut() = Some(NativeFixture {
                plan: make_plan,
                done: done.clone(),
            })
        });
        done
    }
    fn wait_native(done: &AtomicBool) {
        let deadline = Instant::now() + Duration::from_secs(3);
        while !done.load(Ordering::Acquire) {
            assert!(Instant::now() < deadline, "native fixture did not finish");
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    #[test]
    fn sql_only_real_callbacks_reach_full_sidecar_without_native_prerequisites() {
        let _consent = ConsentFixture::new();
        let temp = tempfile::tempdir().unwrap();
        let root = storage::private_root(&temp.path().join("diagnostics")).unwrap();
        let (target, descriptor) = local_target();
        let stop = AtomicBool::new(false);
        let cancel = AtomicBool::new(false);
        let native = native_fixture(Err("native_must_not_be_called"));
        let manifest = std::thread::scope(|scope| {
            let callbacks = scope.spawn(|| {
                crate::query_cpu::tests::wait_for_capture();
                crate::query_cpu::tests::capture_command(17, true);
                crate::query_cpu::tests::capture_command(23, false);
                crate::query_cpu::tests::capture_control();
            });
            let manifest =
                capture(&root, &target, &descriptor, sql_config(), &stop, &cancel).unwrap();
            callbacks.join().unwrap();
            manifest
        });
        assert!(!native.load(Ordering::Acquire));
        assert_eq!(manifest.status, "completed");
        assert_eq!(manifest.mode, CaptureMode::SqlCpu);
        assert_eq!(manifest.tool, "pioneer.query_cpu");
        assert!(manifest.module.is_none());
        assert!(
            manifest
                .configuration
                .iter()
                .all(|v| !v.contains("frequency"))
        );
        assert!(!manifest.profile_data_present());
        let sql = read_sql(&root, &manifest);
        assert_eq!(sql["native_profile_status"], "not_requested");
        assert!(sql["native_profile_interval"].is_null());
        assert_eq!(sql["run"], serde_json::to_value(&target.run).unwrap());
        assert_eq!(sql["consent_generation"], target.consent_generation);
        assert_eq!(sql["sql_cpu"]["requested_seconds"], 1);
        assert_eq!(sql["sql_cpu"]["elapsed_ns"], 1_000_000_000_u64);
        let groups = sql["sql_cpu"]["groups"].as_array().unwrap();
        assert_eq!(groups.len(), 3);
        let measured = groups
            .iter()
            .find(|g| g["fingerprint"] == "0000000000000011")
            .unwrap();
        assert_eq!(measured["workload"], "timeline.page");
        assert_eq!(measured["site"], "timeline_page");
        assert!(measured["control"].is_null());
        let control = groups.iter().find(|g| g["control"] == "begin").unwrap();
        assert!(control["fingerprint"].is_null());
        assert_eq!(control["workload"], "timeline.page");
        assert_eq!(control["site"], "timeline_page");
        assert_eq!(measured["completed_calls"], 1);
        #[cfg(any(target_os = "macos", target_os = "linux", windows))]
        assert!(measured["cpu_ns"].is_u64());
        let unknown = groups
            .iter()
            .find(|g| g["fingerprint"] == "0000000000000017")
            .unwrap();
        assert!(unknown["cpu_ns"].is_null());
        assert_eq!(manifest.sql_cpu.as_ref().unwrap()["window_complete"], true);
        assert!(
            !manifest
                .files
                .iter()
                .any(|f| f.filename.starts_with("tool-") || f.filename == "perf.data")
        );
    }
    #[test]
    fn sql_only_server_rejects_unsupported_targets_and_duration_before_admission() {
        let _consent = ConsentFixture::new();
        let temp = tempfile::tempdir().unwrap();
        let root = storage::private_root(&temp.path().join("diagnostics")).unwrap();
        let (target, descriptor) = local_target();
        for case in ["desktop", "child", "foreign_pid", "zero", "too_long"] {
            let mut target = target.clone();
            let mut request = sql_config();
            match case {
                "desktop" => target.service = "desktop".into(),
                "child" => request.child_generation = Some(1),
                "foreign_pid" => target.run.pid = target.run.pid.wrapping_add(1),
                "zero" => request.seconds = 0,
                "too_long" => request.seconds = 61,
                _ => unreachable!(),
            }
            let error = capture(
                &root,
                &target,
                &descriptor,
                request,
                &AtomicBool::new(false),
                &AtomicBool::new(false),
            )
            .unwrap_err()
            .to_string();
            assert!(
                error.contains(if matches!(case, "zero" | "too_long") {
                    "duration"
                } else {
                    "unsupported_target"
                }),
                "{case}: {error}"
            );
        }
        assert!(fs::read_dir(&root).unwrap().next().is_none());
    }
    #[cfg(unix)]
    fn shell_plan(exit: i32, version: &str) -> ToolPlan {
        ToolPlan {
            program: "/bin/sh".into(),
            args: vec![
                "-c".into(),
                format!("printf fixture > \"$1\"; exit {exit}"),
                "fixture".into(),
            ],
            format: "fixture.native",
            version_args: vec!["-c".into(), version.into()],
            graceful_stop: false,
        }
    }
    #[cfg(unix)]
    #[test]
    fn native_plan_version_spawn_and_early_exit_all_leave_sql_until_original_deadline() {
        use std::os::unix::fs::PermissionsExt;
        let _consent = ConsentFixture::new();
        for case in ["plan", "version", "spawn", "exit_failure", "exit_success"] {
            let temp = tempfile::tempdir().unwrap();
            let root = storage::private_root(&temp.path().join("diagnostics")).unwrap();
            let (target, descriptor) = local_target();
            let native_plan = match case {
                "plan" => Err("fixture_plan_failed"),
                "version" => Ok(shell_plan(0, "exit 7")),
                "spawn" => {
                    // The actual bounded version succeeds, then the actual
                    // recording spawn fails because that fixture removed itself.
                    let script = temp.path().join("one-version-only");
                    fs::write(
                        &script,
                        b"#!/bin/sh\nrm -- \"$0\"\nprintf fixture-version\n",
                    )
                    .unwrap();
                    fs::set_permissions(&script, fs::Permissions::from_mode(0o700)).unwrap();
                    Ok(ToolPlan {
                        program: script,
                        args: vec![],
                        format: "fixture.native",
                        version_args: vec!["--version".into()],
                        graceful_stop: false,
                    })
                }
                "exit_failure" => Ok(shell_plan(7, "printf fixture-version")),
                "exit_success" => Ok(shell_plan(0, "printf fixture-version")),
                _ => unreachable!(),
            };
            let done = native_fixture(native_plan);
            let stop = AtomicBool::new(false);
            let cancel = AtomicBool::new(false);
            let manifest = std::thread::scope(|scope| {
                let callbacks = scope.spawn(|| {
                    let deadline = crate::query_cpu::tests::wait_for_capture();
                    wait_native(&done);
                    assert!(
                        Instant::now() < deadline,
                        "fixture failure must precede SQL callback"
                    );
                    crate::query_cpu::tests::capture_command(17, true);
                });
                let manifest = capture(
                    &root,
                    &target,
                    &descriptor,
                    CaptureRequest {
                        seconds: 2,
                        ..config(CaptureMode::Cpu)
                    },
                    &stop,
                    &cancel,
                )
                .unwrap();
                callbacks.join().unwrap();
                manifest
            });
            let sql = read_sql(&root, &manifest);
            assert_eq!(sql["sql_cpu"]["elapsed_ns"], 2_000_000_000_u64, "{case}");
            assert_eq!(
                sql["sql_cpu"]["groups"][0]["fingerprint"], "0000000000000011",
                "{case}"
            );
            assert_eq!(sql["sql_cpu"]["groups"][0]["completed_calls"], 1);
            assert_eq!(manifest.sql_cpu.as_ref().unwrap()["status"], "recorded");
            assert_eq!(
                manifest.status,
                if case == "exit_success" {
                    "completed"
                } else {
                    "failed"
                },
                "{case}"
            );
            if matches!(case, "plan" | "version" | "spawn") {
                assert!(sql["native_profile_interval"].is_null());
                assert_eq!(manifest.tool, "not_started_or_prerequisite_failed");
            } else {
                assert!(!sql["native_profile_interval"].is_null());
                assert!(
                    manifest.ended_monotonic_ms
                        < sql["sql_cpu"]["ended_monotonic_ms"].as_u64().unwrap()
                );
            }
            assert!(crate::query_cpu::Session::start(1, target.consent_generation).is_some());
        }
    }
    #[cfg(unix)]
    #[test]
    fn sql_deadline_excludes_late_callbacks_while_native_version_is_still_running() {
        let _consent = ConsentFixture::new();
        let temp = tempfile::tempdir().unwrap();
        let root = storage::private_root(&temp.path().join("diagnostics")).unwrap();
        let (target, descriptor) = local_target();
        let done = native_fixture(Ok(shell_plan(7, "exec /bin/sleep 2")));
        let manifest = std::thread::scope(|scope| {
            let callbacks = scope.spawn(|| {
                let deadline = crate::query_cpu::tests::wait_for_capture();
                crate::query_cpu::tests::capture_command(17, true);
                std::thread::sleep(
                    (deadline + Duration::from_millis(100))
                        .saturating_duration_since(Instant::now()),
                );
                assert!(!done.load(Ordering::Acquire));
                crate::query_cpu::tests::capture_command(99, true);
            });
            let manifest = capture(
                &root,
                &target,
                &descriptor,
                CaptureRequest {
                    seconds: 1,
                    ..config(CaptureMode::Cpu)
                },
                &AtomicBool::new(false),
                &AtomicBool::new(false),
            )
            .unwrap();
            callbacks.join().unwrap();
            manifest
        });
        let sql = read_sql(&root, &manifest);
        assert_eq!(sql["sql_cpu"]["elapsed_ns"], 1_000_000_000_u64);
        assert_eq!(sql["sql_cpu"]["groups"].as_array().unwrap().len(), 1);
        assert_eq!(
            sql["sql_cpu"]["groups"][0]["fingerprint"],
            "0000000000000011"
        );
        assert_eq!(manifest.status, "failed");
    }
    #[cfg(unix)]
    #[test]
    fn combined_controls_close_sql_and_join_the_owned_version_child_promptly() {
        let _consent = ConsentFixture::new();
        for signal in ["cancel", "late_cancel", "shutdown", "revoke"] {
            crate::set_telemetry_enabled(true);
            let temp = tempfile::tempdir().unwrap();
            let root = storage::private_root(&temp.path().join("diagnostics")).unwrap();
            let (target, descriptor) = local_target();
            let done = native_fixture(Ok(shell_plan(0, "printf started; exec /bin/sleep 30")));
            let stop = AtomicBool::new(false);
            let cancel = AtomicBool::new(false);
            let result = std::thread::scope(|scope| {
                let actor = scope.spawn(|| {
                    let deadline = crate::query_cpu::tests::wait_for_capture();
                    let timeout = Instant::now() + Duration::from_secs(3);
                    loop {
                        let running = fs::read_dir(root.join("captures"))
                            .unwrap()
                            .map(|e| e.unwrap().path())
                            .any(|p| {
                                fs::metadata(p.join("tool-version.txt")).is_ok_and(|m| m.len() > 0)
                            });
                        if running {
                            break;
                        }
                        assert!(Instant::now() < timeout, "version fixture did not start");
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    crate::query_cpu::tests::capture_command(17, true);
                    if signal == "late_cancel" {
                        std::thread::sleep(
                            (deadline + Duration::from_millis(100))
                                .saturating_duration_since(Instant::now()),
                        );
                    }
                    match signal {
                        "cancel" | "late_cancel" => cancel.store(true, Ordering::Release),
                        "shutdown" => stop.store(true, Ordering::Release),
                        "revoke" => crate::set_telemetry_enabled(false),
                        _ => unreachable!(),
                    }
                });
                let started = Instant::now();
                let result = capture(
                    &root,
                    &target,
                    &descriptor,
                    CaptureRequest {
                        seconds: if signal == "late_cancel" { 1 } else { 60 },
                        ..config(CaptureMode::Cpu)
                    },
                    &stop,
                    &cancel,
                );
                // Existing stop_tool(false) allows 3 s grace before kill;
                // include startup/polling slack, while refusing a 60 s SQL wait.
                assert!(started.elapsed() < Duration::from_secs(8));
                actor.join().unwrap();
                result
            });
            assert!(
                done.load(Ordering::Acquire),
                "capture returns only after native is joined/reaped"
            );
            if matches!(signal, "cancel" | "late_cancel") {
                let manifest = result.unwrap();
                assert_eq!(manifest.status, "failed"); // native version did not complete
                let sql = read_sql(&root, &manifest);
                assert_eq!(
                    sql["sql_cpu"]["closed_reason"],
                    if signal == "late_cancel" {
                        "deadline"
                    } else {
                        "operator_cancelled"
                    }
                );
                assert_eq!(
                    manifest.sql_cpu.as_ref().unwrap()["window_complete"],
                    signal == "late_cancel"
                );
                if signal == "late_cancel" {
                    assert_eq!(sql["sql_cpu"]["elapsed_ns"], 1_000_000_000_u64);
                }
                assert!(sql["sql_cpu"]["elapsed_ns"].as_u64().unwrap() < 60_000_000_000);
                assert_eq!(
                    sql["sql_cpu"]["groups"][0]["fingerprint"],
                    "0000000000000011"
                );
            } else {
                assert!(result.is_err());
                assert!(
                    fs::read_dir(root.join("captures"))
                        .unwrap()
                        .next()
                        .is_none()
                );
            }
        }
    }
    #[cfg(unix)]
    #[test]
    fn worker_only_identity_failure_is_terminal_before_deadline_and_after_final_join() {
        use std::{
            io::{Error, ErrorKind},
            sync::Arc,
        };
        let _consent = ConsentFixture::new();
        for (boundary, error_kind, reason, join_race) in [
            (
                NativeIdentityBoundary::BeforeAttach,
                Some(ErrorKind::Other),
                "target_identity_unavailable",
                false,
            ),
            (
                NativeIdentityBoundary::Recording,
                Some(ErrorKind::PermissionDenied),
                "target_identity_permission_denied",
                false,
            ),
            (
                NativeIdentityBoundary::AfterStop,
                Some(ErrorKind::NotFound),
                "target_exited",
                false,
            ),
            (
                NativeIdentityBoundary::AfterStop,
                None,
                "target_pid_reused",
                true,
            ),
        ] {
            let temp = tempfile::tempdir().unwrap();
            let root = storage::private_root(&temp.path().join("diagnostics")).unwrap();
            let (target, descriptor) = local_target();
            verify_identity(target.run.pid, &descriptor.start_identity).unwrap();
            let stop = AtomicBool::new(false);
            let cancel = AtomicBool::new(false);
            let ready = Arc::new(AtomicBool::new(false));
            let release = Arc::new(AtomicBool::new(false));
            let done = Arc::new(AtomicBool::new(false));
            let deadline = Arc::new(Mutex::new(None));
            let closed_early = Arc::new(AtomicBool::new(false));
            let closed_before_native_exit = Arc::new(AtomicBool::new(false));
            let native_ready = ready.clone();
            let native_release = release.clone();
            NATIVE_PLAN.with(|slot| {
                *slot.borrow_mut() = Some(NativeFixture {
                    done: done.clone(),
                    plan: Box::new(move |path| {
                        let mut injected = false;
                        // This override is read only by the real native verifier.
                        // Owner/common/pre/post-commit reads remain real and healthy.
                        NATIVE_IDENTITY.with(|slot| {
                            *slot.borrow_mut() = Some(Box::new(move |at| {
                                if at != boundary || injected {
                                    return None;
                                }
                                wait_native(&native_ready);
                                if join_race {
                                    wait_native(&native_release);
                                }
                                injected = true;
                                Some(match error_kind {
                                    Some(kind) => Err(Error::from(kind)),
                                    None => Ok("fixture-different-process-start".into()),
                                })
                            }))
                        });
                        let mut plan = shell_plan(0, "printf fixture-version");
                        if boundary == NativeIdentityBoundary::Recording {
                            plan.args = vec!["-c".into(), "exec /bin/sleep 30".into()];
                        } else {
                            plan.args
                                .push(path.join("perf.data").to_string_lossy().into_owned());
                        }
                        Ok(plan)
                    }),
                })
            });
            let owner_deadline = deadline.clone();
            let owner_closed_early = closed_early.clone();
            let owner_closed_before_exit = closed_before_native_exit.clone();
            BEFORE_NATIVE_FINISHED_CHECK.with(|slot| {
                *slot.borrow_mut() = Some(Box::new(move |finished| {
                    let sql_deadline = owner_deadline.lock().unwrap().unwrap();
                    owner_closed_early.store(Instant::now() < sql_deadline, Ordering::Release);
                    owner_closed_before_exit.store(!finished(), Ordering::Release);
                    if join_race {
                        assert!(Instant::now() >= sql_deadline);
                        // Owner has already polled an empty latch and closed SQL.
                        // Publish the native failure now, then make is_finished true
                        // before owner can check it. Only the post-join drain sees it.
                        release.store(true, Ordering::Release);
                        let timeout = Instant::now() + Duration::from_secs(3);
                        while !finished() {
                            assert!(
                                Instant::now() < timeout,
                                "native join-race fixture did not finish"
                            );
                            std::thread::sleep(Duration::from_millis(1));
                        }
                    }
                }))
            });
            let error = std::thread::scope(|scope| {
                let callbacks = scope.spawn(|| {
                    let end = crate::query_cpu::tests::wait_for_capture();
                    *deadline.lock().unwrap() = Some(end);
                    crate::query_cpu::tests::capture_command(17, true);
                    ready.store(true, Ordering::Release);
                });
                let result = capture(
                    &root,
                    &target,
                    &descriptor,
                    CaptureRequest {
                        seconds: if join_race { 1 } else { 5 },
                        ..config(CaptureMode::Cpu)
                    },
                    &stop,
                    &cancel,
                );
                callbacks.join().unwrap();
                result.unwrap_err()
            });
            assert_eq!(error.to_string(), reason);
            assert!(
                done.load(Ordering::Acquire),
                "native is joined before capture removal"
            );
            assert_eq!(closed_early.load(Ordering::Acquire), !join_race);
            if boundary == NativeIdentityBoundary::Recording {
                assert!(
                    closed_before_native_exit.load(Ordering::Acquire),
                    "SQL must close before the 3 s native stop grace ends"
                );
            }
            assert!(!stop.load(Ordering::Acquire));
            assert!(!cancel.load(Ordering::Acquire));
            assert!(crate::telemetry_sample_allowed(Some(
                target.consent_generation
            )));
            verify_identity(target.run.pid, &descriptor.start_identity).unwrap();
            assert!(
                fs::read_dir(root.join("captures"))
                    .unwrap()
                    .next()
                    .is_none(),
                "no SQL/native result may be retained after {reason}"
            );
        }
    }
    #[test]
    fn sql_poll_preserves_real_deadline_for_late_cancel_and_shortens_only_early_cancel() {
        let _consent = ConsentFixture::new();
        for late in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let root = storage::private_root(&temp.path().join("diagnostics")).unwrap();
            let id = "abcdefghijklmnopqrstuvwx";
            let path = storage::capture_dir(&root, id).unwrap();
            let lease = storage::CaptureLease::new(&path);
            let (target, descriptor) = local_target();
            let config = CaptureRequest {
                seconds: if late { 1 } else { 60 },
                ..sql_config()
            };
            let mut sql = SqlCpuCapture::default();
            sql.start(config.seconds, target.consent_generation);
            crate::query_cpu::tests::capture_command(17, true);
            if late {
                std::thread::sleep(
                    (sql.deadline.unwrap() + Duration::from_millis(10))
                        .saturating_duration_since(Instant::now()),
                );
            }
            // Exercise production poll with the control signal after the real
            // collector deadline, without fabricating a completed Snapshot.
            sql.poll(Some("operator_cancelled"));
            let reason = if late {
                "deadline"
            } else {
                "operator_cancelled"
            };
            assert_eq!(sql.closed_reason, Some(reason));
            let snapshot = sql.snapshot.as_ref().unwrap();
            assert_eq!(snapshot.closed_reason, reason);
            if late {
                assert_eq!(snapshot.elapsed_ns, 1_000_000_000);
            } else {
                assert!(snapshot.elapsed_ns < 60_000_000_000);
            }
            let mut manifest =
                sql_only_manifest(id, &target, &config, &descriptor.start_identity, &sql);
            store_sql_cpu(&path, &target, &mut manifest, snapshot);
            storage::write_new_json(&path.join("manifest.json"), &manifest).unwrap();
            drop(lease);
            let saved = read_sql(&root, &manifest);
            assert_eq!(manifest.status, if late { "completed" } else { "partial" });
            let summary = manifest.sql_cpu.as_ref().unwrap();
            assert_eq!(summary["status"], "recorded");
            assert_eq!(summary["window_complete"], late);
            assert_eq!(summary["closed_reason"], reason);
            assert_eq!(saved["sql_cpu"]["closed_reason"], reason);
            assert_eq!(saved["sql_cpu"]["requested_seconds"], config.seconds);
            assert_eq!(
                saved["sql_cpu"]["groups"][0]["fingerprint"],
                "0000000000000011"
            );
            assert_eq!(saved["native_profile_status"], "not_requested");
            assert!(saved["native_profile_interval"].is_null());
        }
    }
    #[test]
    fn sql_only_empty_busy_storage_failure_and_cancel_have_distinct_results() {
        let _consent = ConsentFixture::new();
        let temp = tempfile::tempdir().unwrap();
        let root = storage::private_root(&temp.path().join("diagnostics")).unwrap();
        let (target, descriptor) = local_target();
        let stop = AtomicBool::new(false);
        let cancel = AtomicBool::new(false);
        let empty = capture(&root, &target, &descriptor, sql_config(), &stop, &cancel).unwrap();
        assert_eq!(empty.status, "completed");
        let empty_sql = read_sql(&root, &empty);
        assert!(
            empty_sql["sql_cpu"]["groups"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        assert!(empty_sql["sql_cpu"]["aggregation"]["accounted"]["cpu_ns"].is_null());
        let busy_session = crate::query_cpu::Session::start(60, target.consent_generation).unwrap();
        let busy = capture(&root, &target, &descriptor, sql_config(), &stop, &cancel).unwrap();
        assert_eq!(busy.status, "failed");
        assert_eq!(
            busy.sql_cpu.as_ref().unwrap()["reason"],
            "sql_cpu_session_unavailable"
        );
        assert!(!busy.files.iter().any(|f| f.filename == "sql-cpu.json"));
        crate::query_cpu::tests::capture_command(23, false);
        assert_eq!(
            busy_session.finish().groups.len(),
            1,
            "foreign window stays with its owner"
        );
        for storage_failure in [false, true] {
            cancel.store(false, Ordering::Release);
            let manifest = std::thread::scope(|scope| {
                let actor = scope.spawn(|| {
                    crate::query_cpu::tests::wait_for_capture();
                    crate::query_cpu::tests::capture_command(17, true);
                    if storage_failure {
                        let captures = root.join("captures");
                        let active = fs::read_dir(captures)
                            .unwrap()
                            .map(|e| e.unwrap().path())
                            .find(|p| p.join("active.json").exists())
                            .unwrap();
                        fs::write(active.join("sql-cpu.json"), b"foreign-collision").unwrap();
                    }
                    if !storage_failure {
                        cancel.store(true, Ordering::Release);
                    }
                });
                let started = Instant::now();
                let manifest = capture(
                    &root,
                    &target,
                    &descriptor,
                    CaptureRequest {
                        seconds: if storage_failure { 1 } else { 60 },
                        ..sql_config()
                    },
                    &stop,
                    &cancel,
                )
                .unwrap();
                assert!(started.elapsed() < Duration::from_secs(3));
                actor.join().unwrap();
                manifest
            });
            assert_ne!(manifest.status, "completed");
            let summary = manifest.sql_cpu.as_ref().unwrap();
            assert_eq!(
                summary["closed_reason"],
                if storage_failure {
                    "deadline"
                } else {
                    "operator_cancelled"
                }
            );
            assert_eq!(summary["window_complete"], storage_failure);
            assert!(summary["elapsed_ns"].as_u64().unwrap() < 60_000_000_000);
            assert_eq!(
                summary["status"],
                if storage_failure {
                    "unavailable"
                } else {
                    "recorded"
                }
            );
            if storage_failure {
                assert_eq!(manifest.status, "failed");
                let directory = root
                    .join(&manifest.local_reference)
                    .parent()
                    .unwrap()
                    .to_owned();
                assert_eq!(
                    fs::read(directory.join("sql-cpu.json")).unwrap(),
                    b"foreign-collision"
                );
            } else {
                assert_eq!(manifest.status, "partial");
                read_sql(&root, &manifest);
            }
        }
    }
    #[test]
    fn sql_commit_error_after_revoke_or_shutdown_removes_the_admitted_sidecar() {
        let _consent = ConsentFixture::new();
        for shutdown in [false, true] {
            crate::set_telemetry_enabled(true);
            let temp = tempfile::tempdir().unwrap();
            let root = storage::private_root(&temp.path().join("diagnostics")).unwrap();
            let (target, descriptor) = local_target();
            let stop = std::sync::Arc::new(AtomicBool::new(false));
            let stop_at_commit = stop.clone();
            // The narrow hook is after the final consent check, at the real
            // create_new manifest write. No successful manifest is fabricated.
            BEFORE_COMMIT.with(|slot| {
                *slot.borrow_mut() = Some(Box::new(move |path| {
                    let sql: serde_json::Value =
                        serde_json::from_slice(&fs::read(path.join("sql-cpu.json")).unwrap())
                            .unwrap();
                    assert_eq!(
                        sql["sql_cpu"]["groups"][0]["fingerprint"],
                        "0000000000000011"
                    );
                    fs::write(path.join("manifest.json"), b"commit-collision").unwrap();
                    if shutdown {
                        stop_at_commit.store(true, Ordering::Release);
                    } else {
                        crate::set_telemetry_enabled(false);
                    }
                }))
            });
            std::thread::scope(|scope| {
                let callbacks = scope.spawn(|| {
                    crate::query_cpu::tests::wait_for_capture();
                    crate::query_cpu::tests::capture_command(17, true);
                });
                let error = capture(
                    &root,
                    &target,
                    &descriptor,
                    sql_config(),
                    &stop,
                    &AtomicBool::new(false),
                )
                .unwrap_err();
                assert_eq!(
                    error.downcast_ref::<std::io::Error>().unwrap().kind(),
                    std::io::ErrorKind::AlreadyExists
                );
                callbacks.join().unwrap();
            });
            assert!(
                fs::read_dir(root.join("captures"))
                    .unwrap()
                    .next()
                    .is_none()
            );
        }
    }
    #[test]
    fn sql_owner_stops_and_removes_capture_on_shutdown_revoke_or_epoch_change() {
        let _consent = ConsentFixture::new();
        for signal in ["shutdown", "revoke", "epoch"] {
            crate::set_telemetry_enabled(true);
            let temp = tempfile::tempdir().unwrap();
            let root = storage::private_root(&temp.path().join("diagnostics")).unwrap();
            let (target, descriptor) = local_target();
            let stop = AtomicBool::new(false);
            let cancel = AtomicBool::new(false);
            std::thread::scope(|scope| {
                let actor = scope.spawn(|| {
                    crate::query_cpu::tests::wait_for_capture();
                    crate::query_cpu::tests::capture_command(17, true);
                    if signal == "shutdown" {
                        stop.store(true, Ordering::Release);
                    } else {
                        crate::set_telemetry_enabled(false);
                        if signal == "epoch" {
                            crate::set_telemetry_enabled(true);
                        }
                    }
                });
                let started = Instant::now();
                assert!(
                    capture(
                        &root,
                        &target,
                        &descriptor,
                        CaptureRequest {
                            seconds: 60,
                            ..sql_config()
                        },
                        &stop,
                        &cancel
                    )
                    .is_err()
                );
                assert!(started.elapsed() < Duration::from_secs(3));
                actor.join().unwrap();
            });
            assert!(
                fs::read_dir(root.join("captures"))
                    .unwrap()
                    .next()
                    .is_none()
            );
            crate::set_telemetry_enabled(true);
            assert!(
                crate::query_cpu::Session::start(1, crate::telemetry_consent_snapshot().1)
                    .is_some()
            );
        }
    }

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
        m.mode = CaptureMode::SqlCpu;
        assert!(
            !m.profile_data_present(),
            "SQL-only never supplies native coverage even with native-named files"
        );
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
    fn legacy_modes_and_manifests_without_sql_summary_still_decode() {
        let target = crate::diagnostics::tests::target(7);
        for (mode, wire) in [
            (CaptureMode::Cpu, "cpu"),
            (CaptureMode::Allocations, "allocations"),
            (CaptureMode::AllocationHistory, "allocation_history"),
            (CaptureMode::LiveAllocations, "live_allocations"),
            (CaptureMode::Vm, "vm"),
        ] {
            let manifest = failed_manifest(
                "fixture",
                &target,
                &config(mode),
                42,
                "start",
                "fixture".into(),
                Vec::new(),
            );
            let mut value = serde_json::to_value(manifest).unwrap();
            value.as_object_mut().unwrap().remove("sql_cpu");
            assert_eq!(value["mode"], wire);
            let legacy: CaptureManifest = serde_json::from_value(value).unwrap();
            assert_eq!(legacy.mode, mode);
            assert!(legacy.sql_cpu.is_none());
        }
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
