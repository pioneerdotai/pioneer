//! Local authenticated operator path, independent of application executors.
//! Private runtime-home descriptors select a live RUN, never the CLI's registry.
mod capture;
mod storage;
use crate::{
    identity::RunIdentity,
    resources::{ResourceSnapshot, process_start_identity},
};
use anyhow::{Context, Result, bail, ensure};
pub use capture::{CaptureManifest, CaptureMode, CaptureRequest};
use serde::{Deserialize, Serialize};
use std::{
    fs,
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};
pub use storage::{ArtifactFile, ArtifactReference};

const MAX_REQUEST: usize = 4096;
const MAX_RESPONSE: u64 = 8 * 1024 * 1024;
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TargetDescriptor {
    pub schema_version: u32,
    pub service: String,
    pub run: RunIdentity,
    pub start_identity: String,
    pub endpoint: SocketAddr,
    pub token: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TargetSnapshot {
    pub schema_version: u32,
    pub service: String,
    pub run: RunIdentity,
    pub consent_generation: u64,
    pub resources: Option<ResourceSnapshot>,
    pub stack_logging: String,
    pub module: Option<crate::symbols::ModuleIdentity>,
    pub module_status: String,
    pub live: Option<crate::live::LiveSnapshot>,
    pub runtimes: Option<crate::runtime::RuntimeSnapshots>,
    pub export_health: crate::telemetry::ExportHealth,
}
#[derive(Serialize, Deserialize)]
struct Request {
    token: String,
    run_id: String,
    epoch: Option<u64>,
    command: Command,
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
enum Command {
    Snapshot,
    Capture(CaptureRequest),
    Bundle,
    Cancel,
}
#[derive(Serialize, Deserialize)]
struct Response {
    status: String,
    value: serde_json::Value,
}

#[derive(Default)]
struct CaptureControl {
    active: AtomicBool,
    cancelled: AtomicBool,
}

pub(crate) struct DiagnosticOwner {
    stop: Arc<AtomicBool>,
    threads: Vec<JoinHandle<()>>,
    descriptor: PathBuf,
}
impl DiagnosticOwner {
    pub(crate) fn start(runtime_home: &Path, service: &str) -> Result<Self> {
        let root = storage::private_root(&runtime_home.join("diagnostics"))?;
        let endpoint_root = storage::private_root(&root.join("runs"))?;
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))?;
        listener.set_nonblocking(true)?;
        let run = crate::identity::run_identity().clone();
        let raw = process_start_identity(run.pid)
            .context("process identity unavailable for local diagnostics")?;
        let descriptor = TargetDescriptor {
            schema_version: 1,
            service: service.into(),
            run: run.clone(),
            start_identity: raw,
            endpoint: listener.local_addr()?,
            token: nanoid::nanoid!(48),
        };
        let descriptor_path = endpoint_root.join(format!("{}.json", run.run_id));
        storage::write_new_json(&descriptor_path, &descriptor)?;
        let module: Option<crate::symbols::ModuleIdentity> = None;
        let stack_logging = if cfg!(target_os = "macos")
            && std::env::var("MallocStackLoggingNoCompact").as_deref() == Ok("1")
            && std::env::var_os("MallocStackLogging").is_none()
        {
            "full_history_since_launch"
        } else {
            "not_prepared"
        }
        .to_owned();
        let stop = Arc::new(AtomicBool::new(false));
        let (tx, rx) = std::sync::mpsc::sync_channel::<(TcpStream, Request)>(1);
        let control = Arc::new(CaptureControl::default());
        let capture_control = control.clone();
        let capture_stop = stop.clone();
        let capture_descriptor = descriptor.clone();
        let capture_root = root.clone();
        let capture_module = module.clone();
        let capture_stack = stack_logging.clone();
        let worker = std::thread::Builder::new()
            .name("pioneer-capture".into())
            .spawn(move || {
                while !capture_stop.load(Ordering::Acquire) {
                    match rx.recv_timeout(Duration::from_millis(100)) {
                        Ok((mut stream, request)) => {
                            struct Active<'a>(&'a CaptureControl);
                            impl Drop for Active<'_> {
                                fn drop(&mut self) {
                                    self.0.active.store(false, Ordering::Release);
                                }
                            }
                            let _active = Active(&capture_control);
                            let outcome = (|| -> Result<serde_json::Value> {
                                let epoch = authorize(&capture_descriptor, &request)?;
                                let target = snapshot(
                                    &capture_descriptor,
                                    capture_module.clone(),
                                    &capture_stack,
                                    epoch,
                                )?;
                                match request.command {
                                    Command::Capture(config) => {
                                        Ok(serde_json::to_value(capture::capture(
                                            &capture_root,
                                            &target,
                                            &capture_descriptor,
                                            config,
                                            &capture_stop,
                                            &capture_control.cancelled,
                                        )?)?)
                                    }
                                    Command::Bundle => bundle(
                                        &capture_root,
                                        &target,
                                        &capture_stop,
                                        &capture_control.cancelled,
                                    ),
                                    Command::Snapshot | Command::Cancel => unreachable!(),
                                }
                            })();
                            let _ = respond(&mut stream, &capture_stop, outcome);
                        }
                        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                        Err(_) => break,
                    }
                }
            })
            .map_err(|error| {
                let _ = fs::remove_file(&descriptor_path);
                error
            })?;
        let server_stop = stop.clone();
        let server_descriptor = descriptor.clone();
        let server=match std::thread::Builder::new().name("pioneer-diagnostics".into()).spawn(move||{
   while !server_stop.load(Ordering::Acquire){
    match listener.accept(){
     Ok((mut stream,peer))=>{if !peer.ip().is_loopback(){continue;}let result=read_request(&mut stream, &server_stop);match result{
      Ok(request)=>{match authorize(&server_descriptor,&request){Ok(epoch)=>{
       if matches!(request.command,Command::Snapshot){let _=respond(&mut stream,&server_stop,snapshot(&server_descriptor,module.clone(),&stack_logging,epoch).and_then(|s|Ok(serde_json::to_value(s)?)));}
       else if matches!(request.command,Command::Cancel){let active=control.active.load(Ordering::Acquire);if active{control.cancelled.store(true,Ordering::Release);}let _=respond(&mut stream,&server_stop,Ok(serde_json::json!({"cancel_requested":active,"scope":"only owned SQL/native capture/bundle job; target application is not signalled"})));}
       else if control.active.compare_exchange(false,true,Ordering::AcqRel,Ordering::Acquire).is_err(){let _=respond(&mut stream,&server_stop,Err(anyhow::anyhow!("capture_busy")));}
       else {control.cancelled.store(false,Ordering::Release);match tx.try_send((stream,request)){Ok(())=>{},Err(error)=>{control.active.store(false,Ordering::Release);let (mut stream,_)=match error {std::sync::mpsc::TrySendError::Full(value)|std::sync::mpsc::TrySendError::Disconnected(value)=>value};let _=respond(&mut stream,&server_stop,Err(anyhow::anyhow!("capture_busy")));}}}
      },Err(error)=>{let _=respond(&mut stream,&server_stop,Err(error));}}}
      Err(error)=>{let _=respond(&mut stream,&server_stop,Err(error));}
     }}
     Err(error) if error.kind()==std::io::ErrorKind::WouldBlock=>std::thread::sleep(Duration::from_millis(100)),
     Err(_)=>std::thread::sleep(Duration::from_millis(100)),
    }
   }
  }){Ok(thread)=>thread,Err(error)=>{stop.store(true,Ordering::Release);let _=worker.join();let _=fs::remove_file(&descriptor_path);return Err(error.into());}};
        Ok(Self {
            stop,
            threads: vec![server, worker],
            descriptor: descriptor_path,
        })
    }
    pub(crate) fn shutdown(&mut self) {
        self.stop.store(true, Ordering::Release);
        for thread in self.threads.drain(..) {
            let _ = thread.join();
        }
        let _ = fs::remove_file(&self.descriptor);
    }
}
impl Drop for DiagnosticOwner {
    fn drop(&mut self) {
        self.shutdown();
    }
}
fn authorize(descriptor: &TargetDescriptor, request: &Request) -> Result<u64> {
    ensure!(
        request.token == descriptor.token && request.run_id == descriptor.run.run_id,
        "invalid_local_target"
    );
    let (enabled, epoch) = crate::telemetry_consent_snapshot();
    ensure!(enabled, "consent_disabled");
    ensure!(
        request.epoch.is_none_or(|g| g == epoch),
        "consent_epoch_changed"
    );
    if !matches!(request.command, Command::Snapshot) {
        ensure!(
            request.epoch == Some(epoch),
            "capture_consent_epoch_required"
        );
    }
    ensure!(
        process_start_identity(descriptor.run.pid)? == descriptor.start_identity,
        "target_identity_changed"
    );
    Ok(epoch)
}
fn snapshot(
    descriptor: &TargetDescriptor,
    module: Option<crate::symbols::ModuleIdentity>,
    stack_logging: &str,
    epoch: u64,
) -> Result<TargetSnapshot> {
    ensure!(
        crate::telemetry_sample_allowed(Some(epoch)),
        "consent_epoch_changed"
    );
    Ok(TargetSnapshot {
        schema_version: 1,
        service: descriptor.service.clone(),
        run: descriptor.run.clone(),
        consent_generation: epoch,
        resources: crate::resources::resource_snapshot(),
        stack_logging: stack_logging.into(),
        live: crate::live::snapshot(),
        runtimes: crate::runtime::snapshot(),
        export_health: crate::telemetry::export_health(),
        module_status: if module.is_some() {
            "post_link_id_available"
        } else {
            "post_link_id_unavailable"
        }
        .into(),
        module,
    })
}
const IO_SLICE: Duration = Duration::from_millis(50);
const REQUEST_BUDGET: Duration = Duration::from_millis(500);
const RESPONSE_BUDGET: Duration = Duration::from_secs(2);
fn io_remaining(deadline: Instant, now: Instant, stop: &AtomicBool) -> Result<Duration> {
    ensure!(!stop.load(Ordering::Acquire), "diagnostics_shutdown");
    let remaining = deadline
        .checked_duration_since(now)
        .filter(|d| !d.is_zero())
        .context("diagnostics_io_deadline")?;
    Ok(remaining.min(IO_SLICE))
}
fn retry_io(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::Interrupted
            | std::io::ErrorKind::TimedOut
            | std::io::ErrorKind::WouldBlock
    )
}
fn read_frame(
    stop: &AtomicBool,
    now: impl Fn() -> Instant,
    mut read: impl FnMut(Duration, &mut [u8]) -> std::io::Result<usize>,
) -> Result<Vec<u8>> {
    let deadline = now() + REQUEST_BUDGET;
    let mut bytes = Vec::with_capacity(512);
    loop {
        let timeout = io_remaining(deadline, now(), stop)?;
        let mut byte = [0];
        let result = read(timeout, &mut byte);
        io_remaining(deadline, now(), stop)?;
        match result {
            Ok(0) => bail!("invalid_or_oversized_request"),
            Ok(_) if byte[0] == b'\n' => return Ok(bytes),
            Ok(_) => {
                ensure!(bytes.len() < MAX_REQUEST, "invalid_or_oversized_request");
                bytes.push(byte[0]);
            }
            Err(error) if retry_io(&error) => {}
            Err(error) => return Err(error.into()),
        }
    }
}
fn write_frame(
    bytes: &[u8],
    stop: &AtomicBool,
    expected_epoch: Option<u64>,
    now: impl Fn() -> Instant,
    mut write: impl FnMut(Duration, &[u8]) -> std::io::Result<usize>,
) -> Result<()> {
    let deadline = now() + RESPONSE_BUDGET;
    let mut offset = 0;
    while offset < bytes.len() {
        let timeout = io_remaining(deadline, now(), stop)?;
        ensure!(
            expected_epoch.is_none_or(|epoch| crate::telemetry_sample_allowed(Some(epoch))),
            "consent_epoch_changed_during_delivery"
        );
        let result = write(timeout, &bytes[offset..]);
        io_remaining(deadline, now(), stop)?;
        ensure!(
            expected_epoch.is_none_or(|epoch| crate::telemetry_sample_allowed(Some(epoch))),
            "consent_epoch_changed_during_delivery"
        );
        match result {
            Ok(0) => bail!("diagnostics_write_zero"),
            Ok(n) => offset += n,
            Err(error) if retry_io(&error) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}
fn read_request(stream: &mut TcpStream, stop: &AtomicBool) -> Result<Request> {
    let bytes = read_frame(stop, Instant::now, |timeout, bytes| {
        stream.set_read_timeout(Some(timeout))?;
        stream.read(bytes)
    })?;
    Ok(serde_json::from_slice(&bytes)?)
}
fn respond(
    stream: &mut TcpStream,
    stop: &AtomicBool,
    result: Result<serde_json::Value>,
) -> Result<()> {
    let (bytes, epoch) = prepare_response(result)?;
    write_frame(&bytes, stop, epoch, Instant::now, |timeout, bytes| {
        stream.set_write_timeout(Some(timeout))?;
        stream.write(bytes)
    })
}
fn prepare_response(result: Result<serde_json::Value>) -> Result<(Vec<u8>, Option<u64>)> {
    let mut expected_epoch = None;
    let response = match result {
        Ok(value) => {
            let epoch = value.get("consent_generation").and_then(|v| v.as_u64());
            if epoch.is_some_and(|g| !crate::telemetry_sample_allowed(Some(g))) {
                Response {
                    status: "unavailable".into(),
                    value: serde_json::json!({"reason":"consent_epoch_changed"}),
                }
            } else {
                expected_epoch = epoch;
                Response {
                    status: "ok".into(),
                    value,
                }
            }
        }
        Err(error) => Response {
            status: "unavailable".into(),
            value: serde_json::json!({"reason":format!("{error}").chars().take(256).collect::<String>()}),
        },
    };
    let bytes = serde_json::to_vec(&response)?;
    ensure!(bytes.len() as u64 <= MAX_RESPONSE, "response_overflow");
    Ok((bytes, expected_epoch))
}
pub struct TargetScan {
    pub targets: Vec<TargetDescriptor>,
    pub skipped_descriptors: u64,
}
pub fn targets(runtime_home: &Path) -> Result<TargetScan> {
    let root = runtime_home.join("diagnostics/runs");
    if !root.exists() {
        return Ok(TargetScan {
            targets: Vec::new(),
            skipped_descriptors: 0,
        });
    }
    storage::validate_private(&root)?;
    let mut targets = Vec::new();
    let mut skipped = 0u64;
    for (index, entry) in fs::read_dir(root)?.take(1025).enumerate() {
        ensure!(index < 1024, "run_descriptor_scan_overflow");
        let entry = entry?;
        if entry.path().extension().is_none_or(|s| s != "json") {
            continue;
        }
        let descriptor = match read_descriptor(&entry.path()) {
            Ok(descriptor) => descriptor,
            Err(_) => {
                skipped = skipped.saturating_add(1);
                continue; // unknown/foreign/partial files are preserved
            }
        };
        if descriptor.schema_version != 1 || !descriptor.endpoint.ip().is_loopback() {
            continue;
        }
        match process_start_identity(descriptor.run.pid) {
            Ok(sample) if sample == descriptor.start_identity => {
                ensure!(targets.len() < 64, "live_target_count_overflow");
                targets.push(descriptor);
            }
            result => {
                let stale = match result {
                    Ok(_) => true,
                    Err(error) => error.kind() == std::io::ErrorKind::NotFound,
                };
                if stale
                    && entry.file_name().to_str()
                        == Some(format!("{}.json", descriptor.run.run_id).as_str())
                {
                    let _ = fs::remove_file(entry.path());
                }
            }
        }
    }
    Ok(TargetScan {
        targets,
        skipped_descriptors: skipped,
    })
}
fn read_descriptor(path: &Path) -> Result<TargetDescriptor> {
    storage::validate_private_file(path)?;
    let bytes = storage::read_bounded(path, 32 * 1024)?;
    Ok(serde_json::from_slice(&bytes)?)
}
fn reply_timeout(command: &Command) -> Duration {
    Duration::from_secs(match command {
        Command::Snapshot | Command::Cancel => 2,
        // SQL-only: at most 60 s collection, then existing bounded sidecar,
        // inventory/manifest and response work. No native finish allowance.
        Command::Capture(config) if config.mode == CaptureMode::SqlCpu => 120,
        Command::Capture(config)
            if cfg!(target_os = "macos")
                && matches!(config.mode, CaptureMode::Cpu | CaptureMode::Allocations) =>
        {
            60 + capture::XCTRACE_FINISH_ALLOWANCE_SECONDS + 30
        }
        _ => 120,
    })
}
fn request(
    descriptor: &TargetDescriptor,
    epoch: Option<u64>,
    command: Command,
) -> Result<serde_json::Value> {
    let mut stream = TcpStream::connect_timeout(&descriptor.endpoint, Duration::from_secs(2))?;
    stream.set_read_timeout(Some(reply_timeout(&command)))?;
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;
    let mut bytes = serde_json::to_vec(&Request {
        token: descriptor.token.clone(),
        run_id: descriptor.run.run_id.clone(),
        epoch,
        command,
    })?;
    bytes.push(b'\n');
    ensure!(bytes.len() <= MAX_REQUEST, "request too large");
    stream.write_all(&bytes)?;
    let mut response = Vec::new();
    stream.take(MAX_RESPONSE + 1).read_to_end(&mut response)?;
    ensure!(response.len() as u64 <= MAX_RESPONSE, "response too large");
    // Shutdown may close after accepting the request but before any response.
    // Only an empty transport response is teardown; nonempty JSON errors stay
    // protocol/decode failures, including a truncated JSON document.
    if response.is_empty() {
        return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof).into());
    }
    let response: Response = serde_json::from_slice(&response)?;
    ensure!(response.status == "ok", "{}", response.value);
    Ok(response.value)
}
pub fn inspect_target(descriptor: &TargetDescriptor) -> Result<TargetSnapshot> {
    Ok(serde_json::from_value(request(
        descriptor,
        None,
        Command::Snapshot,
    )?)?)
}
pub fn capture_target(
    descriptor: &TargetDescriptor,
    config: CaptureRequest,
) -> Result<CaptureManifest> {
    let target = inspect_target(descriptor)?;
    Ok(serde_json::from_value(request(
        descriptor,
        Some(target.consent_generation),
        Command::Capture(config),
    )?)?)
}
pub fn cancel_target(descriptor: &TargetDescriptor) -> Result<serde_json::Value> {
    let target = inspect_target(descriptor)?;
    request(descriptor, Some(target.consent_generation), Command::Cancel)
}
pub fn bundle_target(descriptor: &TargetDescriptor) -> Result<serde_json::Value> {
    let target = inspect_target(descriptor)?;
    request(descriptor, Some(target.consent_generation), Command::Bundle)
}
fn bundle(
    root: &Path,
    target: &TargetSnapshot,
    stop: &AtomicBool,
    cancel: &AtomicBool,
) -> Result<serde_json::Value> {
    let epoch = target.consent_generation;
    ensure!(
        !stop.load(Ordering::Acquire)
            && !cancel.load(Ordering::Acquire)
            && crate::telemetry_sample_allowed(Some(epoch)),
        "consent_epoch_changed"
    );
    let capture_id = nanoid::nanoid!(24);
    let path = storage::capture_dir(root, &capture_id)?;
    let _capture_lease = storage::CaptureLease::new(&path);
    let outcome = (|| -> Result<serde_json::Value> {
        let started = std::time::Instant::now();
        let window_end = crate::identity::monotonic_ms();
        let references = profile_references(root, target)?;
        let mut missing = Vec::new();
        if target.resources.as_ref().is_none_or(|s| s.stale) {
            missing.push("resource_snapshot_missing_or_stale");
        }
        if target.live.is_none() {
            missing.push("live_recording_not_recorded");
        }
        if !references.iter().any(|r| r.profile_data_present) {
            missing.push("no_existing_profile_in_this_run_and_epoch");
        }
        if references
            .iter()
            .any(|r| r.profile_data_present && r.status == "partial")
        {
            missing.push("existing_profile_is_partial");
        }
        if target.module.is_none() {
            missing.push("module_ID_not_recorded_in_ordinary_snapshot; captures_and_release_manifest_supply_exact_symbols");
        }
        // Local snapshot is a bounded immutable clone. No DB permit or UI context is
        // retained while serializing/hashing/writing. Axiom is not consulted.
        storage::write_new_json(&path.join("snapshot.json"), target)?;
        ensure!(
            started.elapsed() < Duration::from_secs(10),
            "bundle_serialization_timeout"
        );
        let files = storage::inventory(&path)?;
        let manifest = serde_json::json!({"schema_version":1,"kind":"pioneer.diagnostic_bundle","capture_id":capture_id,"run":target.run,"consent_generation":epoch,"snapshot_monotonic_ms":window_end,"capture_window":{"history_limit_ms":120000,"first_sequence":target.live.as_ref().and_then(|s|s.first_sequence),"last_sequence":target.live.as_ref().map(|s|s.last_sequence),"overwritten":target.live.as_ref().map(|s|s.overwritten),"lost":target.live.as_ref().map(|s|s.lost)},"status":if missing.is_empty(){"completed"}else{"partial"},"missing_reasons":missing,"existing_profiles":references,"files":files,"selected_trace_ids":selected_trace_ids(target),"correlation":"run/build/time; profile stacks exist only during their actual captures","consistency":"registry snapshot is internally coherent; independent OS/runtime/export samples retain their timestamps; no global application freeze","local_reference":format!("captures/pioneer-capture-{capture_id}/manifest.json"),"capabilities":capabilities()});
        ensure!(
            !stop.load(Ordering::Acquire)
                && !cancel.load(Ordering::Acquire)
                && crate::telemetry_sample_allowed(Some(epoch)),
            "consent_revoked_during_bundle"
        );
        storage::write_new_json(&path.join("manifest.json"), &manifest)?;
        ensure!(
            crate::telemetry_sample_allowed(Some(epoch)),
            "consent_revoked_during_bundle"
        );
        Ok(manifest)
    })();
    if outcome.is_err() {
        storage::remove_owned(&path);
    }
    outcome
}
fn profile_references(root: &Path, target: &TargetSnapshot) -> Result<Vec<ArtifactReference>> {
    let captures = root.join("captures");
    if !captures.exists() {
        return Ok(Vec::new());
    }
    let mut result = Vec::new();
    for entry in fs::read_dir(captures)?.take(257) {
        let path = entry?.path();
        if !storage::is_owned(&path) {
            continue;
        }
        let manifest = path.join("manifest.json");
        if !manifest.is_file() {
            continue;
        }
        let bytes = storage::read_bounded(&manifest, 8 * 1024 * 1024)?;
        let Ok(value) = serde_json::from_slice::<CaptureManifest>(&bytes) else {
            continue;
        };
        if value.run.run_id == target.run.run_id
            && value.consent_generation == target.consent_generation
        {
            use sha2::{Digest, Sha256};
            result.push(ArtifactReference {
                status: value.status.clone(),
                coverage: value.coverage.clone(),
                profile_data_present: value.profile_artifacts_present(&path),
                capture_id: value.capture_id,
                manifest_sha256: hex::encode(Sha256::digest(&bytes)),
                local_reference: value.local_reference,
                bytes: value
                    .files
                    .iter()
                    .fold(0u64, |sum, f| sum.saturating_add(f.bytes)),
            });
            if result.len() == 16 {
                break;
            }
        }
    }
    Ok(result)
}
fn selected_trace_ids(target: &TargetSnapshot) -> Vec<String> {
    let mut result = std::collections::BTreeSet::new();
    if let Some(live) = &target.live {
        for context in live
            .active
            .iter()
            .map(|w| &w.context)
            .chain(live.journal.iter().filter_map(|record| {
                if let crate::live::JournalPayload::Work(work) = &record.payload {
                    Some(&work.context)
                } else {
                    None
                }
            }))
        {
            if let Some(id) = context.trace_id {
                result.insert(hex::encode(id));
                if result.len() == 64 {
                    break;
                }
            }
        }
    }
    result.into_iter().collect()
}
pub fn capabilities() -> serde_json::Value {
    serde_json::json!({"schema_version":1,"sql_cpu":{"mode":"sql_cpu","target":"owner parent Gateway only; no child","duration_seconds":"1..60, default 10","frequency":"not applicable","native_profile":"not_requested","scope":"observed instrumented SQL command CPU; full local sql-cpu.json and short manifest summary"},"cpu":{"macos":"xctrace Time Profiler, developer tools + task attach permission; signed package may deny attach; template-controlled frequency, lost samples unknown","linux":"perf record cpu-clock, strict 19..199 Hz, dwarf 16KiB, perf_event permissions; kernel/tool feature validation at invocation","windows":"not implemented; resource snapshots and live bundles supported"},"memory":{"macos":{"allocation_churn":"xctrace Allocations from attach onward; no earlier history","allocation_history":"malloc_history -allEvents; explicit bounded launch-memory with NoCompact only","live_allocations":"malloc_history -allBySize; compare captures; allocation stack is not retainer","vm":"vmmap -summary native/mmap/VM; no complete GPU or Rust retain graph"},"linux":"pmap -x VM; allocation history/live heap unavailable without allocator/tool instrumentation, not inferred from RSS","windows":"native memory profile adapter unavailable"},"bounds":{"active_work":512,"queues":128,"queue_age_records_per_queue":0,"queue_oldest_source":"native owner head only; unverified channel order unavailable","request_total_deadline_ms":500,"response_total_deadline_ms":2000,"io_stop_check_slice_ms":50,"journal_records":4096,"journal_retained_bytes":2097152,"journal_age_ms":120000,"profile_retained_bytes":134217728,"store_count":16,"store_bytes":536870912,"store_age_days":7,"capture_concurrency":1,"capture_seconds_max":60},"uncertainty":"native-tool output volume polled each 100ms; temporary overshoot possible; no measured overhead claim"})
}

/// Single lifecycle consumer called by Gateway and Desktop after runtime home
/// is known. No shell dependency enters Client/FFI.
pub fn start_local_diagnostics(runtime_home: &Path) -> Result<()> {
    let state = crate::telemetry::state().context("telemetry lifecycle unavailable")?;
    let mut owner = state
        .diagnostics
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if owner.is_none() {
        let service = match state.target {
            crate::TelemetryTarget::Gateway => "gateway",
            crate::TelemetryTarget::Desktop => "desktop",
            crate::TelemetryTarget::Mobile => bail!("mobile local diagnostics unsupported"),
        };
        *owner = Some(DiagnosticOwner::start(runtime_home, service)?);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    pub(super) fn target(epoch: u64) -> TargetSnapshot {
        TargetSnapshot {
            schema_version: 1,
            service: "gateway".into(),
            run: crate::identity::run_identity().clone(),
            consent_generation: epoch,
            resources: None,
            stack_logging: "not_prepared".into(),
            live: crate::live::snapshot(),
            runtimes: None,
            export_health: crate::telemetry::export_health(),
            module_status: "not_recorded".into(),
            module: None,
        }
    }
    #[test]
    fn sql_capture_wire_dispatch_owner_gate_readers_and_reply_budget() {
        let _telemetry = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
        let _session = crate::query_cpu::tests::SERIAL.lock().unwrap();
        let old = crate::telemetry_enabled();
        crate::set_telemetry_enabled(true);
        let temp = tempfile::tempdir().unwrap();
        let mut owner = DiagnosticOwner::start(temp.path(), "gateway").unwrap();
        let descriptor = read_descriptor(&owner.descriptor).unwrap();
        let config = CaptureRequest {
            mode: CaptureMode::SqlCpu,
            seconds: 2,
            frequency_hz: 0,
            child_generation: None,
        };
        let wire = serde_json::to_value(Command::Capture(config.clone())).unwrap();
        assert_eq!(wire["command"], "capture");
        assert_eq!(wire["mode"], "sql_cpu");
        assert_eq!(
            reply_timeout(&Command::Capture(CaptureRequest {
                seconds: 60,
                ..config.clone()
            })),
            Duration::from_secs(120)
        );
        // A rejected job releases the real owner gate, allowing the next request.
        assert!(
            capture_target(
                &descriptor,
                CaptureRequest {
                    seconds: 0,
                    ..config.clone()
                }
            )
            .unwrap_err()
            .to_string()
            .contains("duration")
        );
        let manifest = std::thread::scope(|scope| {
            let client = scope.spawn(|| capture_target(&descriptor, config.clone()).unwrap());
            crate::query_cpu::tests::wait_for_capture();
            let busy = capture_target(&descriptor, config.clone()).unwrap_err();
            assert!(busy.to_string().contains("capture_busy"));
            crate::query_cpu::tests::capture_command(17, true);
            client.join().unwrap()
        });
        assert_eq!(manifest.status, "completed");
        assert_eq!(
            manifest.sql_cpu.as_ref().unwrap()["aggregation"]["accounted"]["completed_calls"],
            1
        );
        let root = temp.path().join("diagnostics");
        let snapshot = inspect_target(&descriptor).unwrap();
        let references = profile_references(&root, &snapshot).unwrap();
        assert_eq!(references.len(), 1);
        assert!(!references[0].profile_data_present);
        assert_eq!(references[0].status, "completed");
        let sidecar_path = root
            .join(&manifest.local_reference)
            .parent()
            .unwrap()
            .join("sql-cpu.json");
        let sidecar: serde_json::Value = serde_json::from_slice(
            &storage::read_bounded(&sidecar_path, storage::SQL_CPU_BYTES).unwrap(),
        )
        .unwrap();
        assert_eq!(
            sidecar["sql_cpu"]["groups"][0]["fingerprint"],
            "0000000000000011"
        );
        assert_eq!(sidecar["native_profile_status"], "not_requested");
        let bundle = bundle_target(&descriptor).unwrap();
        assert!(
            bundle["missing_reasons"]
                .as_array()
                .unwrap()
                .iter()
                .any(|r| r == "no_existing_profile_in_this_run_and_epoch")
        );
        owner.shutdown();
        crate::set_telemetry_enabled(old);
    }
    #[test]
    fn offline_bundle_distinguishes_failed_attempts_partial_profiles_and_epoch() {
        let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
        let old = crate::telemetry_enabled();
        crate::set_telemetry_enabled(false);
        crate::set_telemetry_enabled(true);
        let temp = tempfile::tempdir().unwrap();
        let root = storage::private_root(&temp.path().join("diagnostics")).unwrap();
        let target = target(crate::telemetry_consent_snapshot().1);
        for status in ["failed", "partial", "completed"] {
            let id = nanoid::nanoid!(24);
            let dir = storage::capture_dir(&root, &id).unwrap();
            let lease = storage::CaptureLease::new(&dir);
            let mut m = capture::fixture_manifest(&id, &target);
            m.status = status.into();
            if status != "failed" {
                fs::write(dir.join("perf.data"), b"native-profile-fixture").unwrap();
            }
            m.files = storage::inventory(&dir).unwrap();
            storage::write_new_json(&dir.join("manifest.json"), &m).unwrap();
            drop(lease);
            let refs = profile_references(&root, &target).unwrap();
            assert_eq!(
                refs.iter().filter(|r| r.profile_data_present).count(),
                match status {
                    "failed" => 0,
                    "partial" => 1,
                    _ => 2,
                }
            );
            let bundle = bundle(
                &root,
                &target,
                &AtomicBool::new(false),
                &AtomicBool::new(false),
            )
            .unwrap();
            assert_eq!(
                bundle["missing_reasons"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|v| v == "no_existing_profile_in_this_run_and_epoch"),
                status == "failed"
            );
            let current = refs.iter().find(|r| r.capture_id == id).unwrap();
            assert_eq!(current.status, status);
        }
        let mut other_epoch = target.clone();
        other_epoch.consent_generation += 2;
        assert!(profile_references(&root, &other_epoch).unwrap().is_empty());
        let mut other_run = target.clone();
        other_run.run.run_id = "different-run".into();
        assert!(profile_references(&root, &other_run).unwrap().is_empty());
        crate::set_telemetry_enabled(old);
    }
    #[test]
    fn data_delivery_keeps_its_epoch_after_preparation_and_between_writes() {
        let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
        let old = crate::telemetry_enabled();
        crate::set_telemetry_enabled(false);
        crate::set_telemetry_enabled(true);
        let stop = AtomicBool::new(false);
        let prepare = || {
            prepare_response(Ok(serde_json::json!({"consent_generation":crate::telemetry_consent_snapshot().1,"fixture":"data"}))).unwrap()
        };
        let (bytes, epoch) = prepare();
        crate::set_telemetry_enabled(false);
        crate::set_telemetry_enabled(true);
        let mut writes = 0;
        assert!(
            write_frame(&bytes, &stop, epoch, Instant::now, |_, data| {
                writes += 1;
                Ok(data.len())
            })
            .is_err()
        );
        assert_eq!(writes, 0);
        let (bytes, epoch) = prepare();
        assert!(
            write_frame(&bytes, &stop, epoch, Instant::now, |_, _| {
                writes += 1;
                crate::set_telemetry_enabled(false);
                crate::set_telemetry_enabled(true);
                Ok(1)
            })
            .is_err()
        );
        assert_eq!(writes, 1); // bytes already accepted cannot be revoked; no later write
        assert!(!stop.load(Ordering::Acquire));
        let (bytes, epoch) = prepare();
        let mut delivered = Vec::new();
        write_frame(&bytes, &stop, epoch, Instant::now, |_, part| {
            delivered.push(part[0]);
            Ok(1)
        })
        .unwrap();
        assert_eq!(delivered, bytes);
        crate::set_telemetry_enabled(false);
        for result in [
            Err(anyhow::anyhow!("fixture unavailable")),
            Ok(serde_json::json!({"cancel_requested":true})),
        ] {
            let (bytes, epoch) = prepare_response(result).unwrap();
            assert_eq!(epoch, None);
            write_frame(&bytes, &stop, epoch, Instant::now, |_, part| Ok(part.len())).unwrap();
        }
        let (bytes, epoch) =
            prepare_response(Ok(serde_json::json!({"consent_generation":0}))).unwrap();
        assert_eq!(epoch, None);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&bytes).unwrap()["status"],
            "unavailable"
        );
        crate::set_telemetry_enabled(old);
    }
    #[test]
    fn partial_io_obeys_total_deadline_and_stop_and_preserves_framing() {
        use std::cell::Cell;
        let base = Instant::now();
        let elapsed = Cell::new(Duration::ZERO);
        let stop = AtomicBool::new(false);
        let read = read_frame(
            &stop,
            || base + elapsed.get(),
            |timeout, out| {
                elapsed.set(elapsed.get() + timeout);
                out[0] = b'x';
                Ok(1)
            },
        );
        assert_eq!(read.unwrap_err().to_string(), "diagnostics_io_deadline");
        assert_eq!(elapsed.get(), REQUEST_BUDGET);
        elapsed.set(Duration::ZERO);
        let write = write_frame(
            &[0; 100],
            &stop,
            None,
            || base + elapsed.get(),
            |timeout, _| {
                elapsed.set(elapsed.get() + timeout);
                Ok(1)
            },
        );
        assert_eq!(write.unwrap_err().to_string(), "diagnostics_io_deadline");
        assert_eq!(elapsed.get(), RESPONSE_BUDGET);
        let mut calls = 0;
        assert!(
            read_frame(&stop, Instant::now, |_, out| {
                calls += 1;
                stop.store(true, Ordering::Release);
                out[0] = b'x';
                Ok(1)
            })
            .is_err()
        );
        assert_eq!(calls, 1);
        stop.store(false, Ordering::Release);
        let mut writes = 0;
        assert!(
            write_frame(b"response", &stop, None, Instant::now, |_, _| {
                writes += 1;
                stop.store(true, Ordering::Release);
                Ok(1)
            })
            .is_err()
        );
        assert_eq!(writes, 1);
        stop.store(false, Ordering::Release);
        let mut input = b"{}\n".iter();
        assert_eq!(
            read_frame(&stop, Instant::now, |_, out| {
                out[0] = *input.next().unwrap();
                Ok(1)
            })
            .unwrap(),
            b"{}"
        );
        let mut output = Vec::new();
        write_frame(b"{}", &stop, None, Instant::now, |_, bytes| {
            output.push(bytes[0]);
            Ok(1)
        })
        .unwrap();
        assert_eq!(output, b"{}");
    }
    #[test]
    fn partial_descriptor_does_not_hide_healthy_target_or_delete_foreign_file() {
        let temp = tempfile::tempdir().unwrap();
        let root = storage::private_root(&temp.path().join("diagnostics")).unwrap();
        let runs = storage::private_root(&root.join("runs")).unwrap();
        let healthy = TargetDescriptor {
            schema_version: 1,
            service: "gateway".into(),
            run: crate::identity::run_identity().clone(),
            start_identity: process_start_identity(std::process::id()).unwrap(),
            endpoint: "127.0.0.1:1234".parse().unwrap(),
            token: "fixture".into(),
        };
        storage::write_new_json(&runs.join("healthy.json"), &healthy).unwrap();
        storage::write_new_json(&runs.join("partial.json"), &healthy).unwrap();
        fs::write(runs.join("partial.json"), b"{").unwrap();
        let found = targets(temp.path()).unwrap();
        assert_eq!(found.targets.len(), 1);
        assert_eq!(found.skipped_descriptors, 1);
        assert_eq!(found.targets[0].run.run_id, healthy.run.run_id);
        assert_eq!(fs::read(runs.join("partial.json")).unwrap(), b"{");
    }
    #[test]
    fn offline_bundle_is_bounded_has_gaps_and_never_invents_profiles() {
        let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
        let old = crate::telemetry_enabled();
        crate::set_telemetry_enabled(false);
        crate::set_telemetry_enabled(true);
        let temp = tempfile::tempdir().unwrap();
        let root = storage::private_root(&temp.path().join("diagnostics")).unwrap();
        let t = target(crate::telemetry_consent_snapshot().1);
        let stop = AtomicBool::new(false);
        let cancel = AtomicBool::new(false);
        let manifest = bundle(&root, &t, &stop, &cancel).unwrap();
        assert_eq!(manifest["status"], "partial");
        assert!(manifest["existing_profiles"].as_array().unwrap().is_empty());
        let path = root.join(manifest["local_reference"].as_str().unwrap());
        assert!(fs::metadata(&path).unwrap().len() < 8 * 1024 * 1024);
        assert!(
            manifest["missing_reasons"]
                .as_array()
                .unwrap()
                .iter()
                .any(|v| v == "resource_snapshot_missing_or_stale")
        );
        let before = fs::read_dir(root.join("captures")).unwrap().count();
        cancel.store(true, Ordering::Release);
        assert!(bundle(&root, &t, &stop, &cancel).is_err());
        assert_eq!(fs::read_dir(root.join("captures")).unwrap().count(), before);
        crate::set_telemetry_enabled(false);
        crate::set_telemetry_enabled(true);
        cancel.store(false, Ordering::Release);
        assert!(bundle(&root, &t, &stop, &cancel).is_err());
        crate::set_telemetry_enabled(old);
    }
    #[test]
    fn bundle_write_error_preserves_foreign_files() {
        let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
        let old = crate::telemetry_enabled();
        crate::set_telemetry_enabled(false);
        crate::set_telemetry_enabled(true);
        let temp = tempfile::tempdir().unwrap();
        let root = storage::private_root(&temp.path().join("diagnostics")).unwrap();
        fs::write(root.join("captures"), b"foreign-file").unwrap();
        let t = target(crate::telemetry_consent_snapshot().1);
        assert!(bundle(&root, &t, &AtomicBool::new(false), &AtomicBool::new(false)).is_err());
        assert_eq!(fs::read(root.join("captures")).unwrap(), b"foreign-file");
        crate::set_telemetry_enabled(old);
    }
}

/// Ephemeral native stack logs for an explicitly owned preparation launch.
/// They are never normal telemetry or retained captures. One preparer per home;
/// the CLI kills/reaps its own application before this lease removes the logs.
pub struct PreparedMemoryLogs {
    path: PathBuf,
    budget_bytes: u64,
    _lock: fs::File,
}
impl PreparedMemoryLogs {
    /// Per-launch logical-byte quota, independent from retained capture limits.
    /// A budget bounds preparation; it does not guarantee the requested lifetime.
    pub fn new(runtime_home: &Path, log_budget_mib: u64) -> Result<Self> {
        ensure!(
            (1..=4096).contains(&log_budget_mib),
            "preparation log budget must be 1..4096 MiB"
        );
        let root = storage::private_root(&runtime_home.join("diagnostics"))?;
        let lock = storage::owned_lock(&root, storage::LockKind::Preparation)?;
        let id = nanoid::nanoid!(24);
        let path = storage::preparation_dir(&root, &id)?;
        Ok(Self {
            path,
            budget_bytes: log_budget_mib * 1024 * 1024,
            _lock: lock,
        })
    }
    pub fn directory(&self) -> &Path {
        &self.path
    }
    pub fn check_volume(&self) -> Result<()> {
        // Metadata-only bounded walk; collect the observed total before reporting
        // this separate ephemeral quota. Native writes may overshoot between polls.
        let bytes = storage::size(&self.path, u64::MAX)?;
        ensure!(
            bytes <= self.budget_bytes,
            "preparation log budget exceeded: measured {bytes} logical bytes, limit {} bytes",
            self.budget_bytes
        );
        Ok(())
    }
}
impl Drop for PreparedMemoryLogs {
    fn drop(&mut self) {
        storage::remove_owned(&self.path);
    }
}

#[cfg(test)]
mod preparation_tests {
    use super::*;
    #[test]
    fn preparation_is_singleflight_private_bounded_and_foreign_lock_is_preserved() {
        let temp = tempfile::tempdir().unwrap();
        let logs = PreparedMemoryLogs::new(temp.path(), 1).unwrap();
        let path = logs.directory().to_owned();
        assert_eq!(
            path.parent().unwrap(),
            temp.path().join("diagnostics/preparations")
        );
        assert!(!temp.path().join("diagnostics/captures").exists());
        assert!(PreparedMemoryLogs::new(temp.path(), 1).is_err());
        let file = fs::File::create(path.join("native-stacklog")).unwrap();
        file.set_len(128 * 1024 * 1024).unwrap();
        let error = logs.check_volume().unwrap_err().to_string();
        assert!(error.contains("preparation log budget exceeded: measured "));
        assert!(error.contains("limit 1048576 bytes"));
        drop(logs);
        assert!(!path.exists());
        let lock = temp.path().join("diagnostics/memory-preparation.lock");
        fs::remove_file(&lock).unwrap();
        storage::write_new_json(&lock, &serde_json::json!({"owner":"foreign"})).unwrap();
        assert!(PreparedMemoryLogs::new(temp.path(), 1).is_err());
        assert!(lock.is_file());
    }
}
