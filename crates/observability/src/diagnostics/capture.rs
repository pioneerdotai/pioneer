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
    let capture_id = nanoid::nanoid!(24);
    let path = storage::capture_dir(root, &capture_id)?;
    let _capture_lease = storage::CaptureLease::new(&path);
    let mut attempt = None;
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
    );
    if !crate::telemetry_sample_allowed(Some(epoch)) || stop.load(Ordering::Acquire) {
        storage::remove_owned(&path);
        bail!("capture_shutdown_or_consent_revoked");
    }
    let manifest = match outcome {
        Ok(manifest) => manifest,
        Err(error) => {
            // capture_inner has already dropped/reaped its owned native tool.
            // Record this upper bound BEFORE recovery filesystem work.
            if let Some(attempt) = &mut attempt {
                attempt
                    .ended
                    .get_or_insert_with(|| (unix_ms(), monotonic_ms()));
            }
            let mut files = storage::inventory(&path).ok();
            if files.is_none() {
                storage::remove_owned(&path);
                let recreated = storage::capture_dir(root, &capture_id)?;
                files = storage::inventory(&recreated).ok();
            }
            let mut manifest = failed_manifest(
                &capture_id,
                target,
                &config,
                pid,
                &start_identity,
                format!("{error}").chars().take(256).collect(),
                files.unwrap_or_default(),
            );
            if let Some(attempt) = attempt {
                attempt.preserve(&mut manifest);
            }
            manifest
        }
    };
    storage::write_new_json(&path.join("manifest.json"), &manifest)?;
    if !crate::telemetry_sample_allowed(Some(epoch)) {
        storage::remove_owned(&path);
        bail!("consent_revoked_before_capture_commit");
    }
    record_reference(&manifest);
    Ok(manifest)
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
) -> Result<CaptureManifest> {
    let epoch = target.consent_generation;
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
        if let Some(exit) = child.try_wait()? {
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
        if let Err(error) = verify_identity(pid, start_identity) {
            reasons.push(error.to_string());
            status = "partial";
            stop_tool(&mut child, plan.graceful_stop, &mut registration);
            break;
        }
        if storage::size(path, storage::CAPTURE_BYTES - 1024 * 1024).is_err() {
            reasons.push("output_overflow".into());
            status = "partial";
            stop_tool(&mut child, plan.graceful_stop, &mut registration);
            trim_overflow(path)?;
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
    let files = storage::inventory(path)?;
    if !files.iter().any(|file| is_profile_file(config.mode, file)) {
        status = "failed";
        reasons.push("profile_artifact_missing".into());
    }
    let symbol_status = symbol_status(module.as_ref());
    Ok(CaptureManifest{schema_version:1,capture_id:capture_id.into(),run:target.run.clone(),consent_generation:epoch,target_pid:pid,target_start_identity:start_identity.into(),target_child_generation:config.child_generation,target_build_scope:if config.child_generation.is_none(){"target_executable"}else{"parent_build_only; child module identity separately reported"}.into(),mode:config.mode,tool:plan.program.file_name().unwrap_or_default().to_string_lossy().into_owned(),tool_version,configuration:plan.args,actual_format:plan.format.into(),started_unix_ms,ended_unix_ms,started_monotonic_ms,ended_monotonic_ms,clock_scope:"native tool spawn through observed exit/stop, target process monotonic_ms and unix_ms; tool artifact contains actual sample times; module hashing/version preparation excluded".into(),module_coverage:if module.is_some(){"main executable only; native tool artifact contains loaded-module evidence where available"}else{"main module ID unavailable; native artifact loaded-module evidence tool-defined"}.into(),module,symbol_status:symbol_status.into(),coverage:if cfg!(target_os="macos")&&matches!(config.mode,CaptureMode::Cpu|CaptureMode::Allocations){"template-controlled sampling; all target threads; frequency and lost samples unknown until tool analysis"}else{"tool-defined; see actual format; no CPU-to-async-operation attribution"}.into(),lost_samples:None,status:status.into(),incomplete_reasons:reasons,files,local_reference:format!("captures/pioneer-capture-{capture_id}/manifest.json"),correlation_scope:"service.instance.id + build + process/start + capture interval; not async work CPU attribution".into()})
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
fn trim_overflow(path: &Path) -> Result<()> {
    // Preserve bounded partial files. Tools can overshoot between checks, so a
    // directory that cannot be inventoried is removed rather than claimed bounded.
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
    if storage::size(path, storage::CAPTURE_BYTES - 1024 * 1024).is_err() {
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
        trim_overflow(temp.path()).unwrap();
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
