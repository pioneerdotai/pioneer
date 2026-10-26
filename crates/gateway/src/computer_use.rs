//! The selected Gateway's computer. No client or Desktop participates in this
//! execution path. Only the daemon-backed SDK constructors are used here.
use async_trait::async_trait;
use cua_driver_sdk::{
    CuaDriver, CuaDriverSession, EmbeddedCuaDriverHost, EmbeddedDriverError,
    EmbeddedDriverHostOptions, EmbeddedEnvironmentVariable, EmbeddedPermissionMode,
    SessionPermissionMode, TrustedSessionOptions,
};
use fs4::FileExt;
use pioneer_protocol::{CUA_DRIVER_REVISION, CUA_DRIVER_VERSION};
use pioneer_tools::{ComputerUseBackend, ToolError};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const HOST_BUNDLE_ID: &str = "ai.pioneer.gateway";
const MAX_OWNERS: usize = 32;
const QUEUE_CAPACITY: usize = 16;
const CALL_DEADLINE: Duration = Duration::from_secs(120);

/// This owner is retained by Gateway startup, independently of tool routers and
/// client connections. Its Drop also covers errors during Gateway startup.
pub(crate) struct GatewayComputerUse {
    backend: Arc<Backend>,
    worker: Option<JoinHandle<()>>,
}

impl GatewayComputerUse {
    pub(crate) fn new(runtime_home: &Path) -> Self {
        let (send, receive) = mpsc::channel(QUEUE_CAPACITY);
        let control = Arc::new(Control {
            shutdown: CancellationToken::new(),
            owners: Mutex::new(HashMap::new()),
            released: Notify::new(),
        });
        let backend = Arc::new(Backend {
            send,
            control: control.clone(),
        });
        let runtime_home = runtime_home.to_path_buf();
        let worker = tokio::spawn(async move {
            Worker::new(runtime_home, control).run(receive).await;
        });
        Self {
            backend,
            worker: Some(worker),
        }
    }

    pub(crate) fn backend(&self) -> Arc<dyn ComputerUseBackend> {
        self.backend.clone()
    }

    pub(crate) async fn shutdown(&mut self) {
        self.backend.control.shutdown.cancel();
        if let Some(worker) = self.worker.take() {
            // Never abort this worker: it must reap its daemon before releasing
            // the process-ownership lock or admitting a replacement generation.
            let _ = worker.await;
        }
    }
}

impl Drop for GatewayComputerUse {
    fn drop(&mut self) {
        self.backend.control.shutdown.cancel();
    }
}

struct Control {
    shutdown: CancellationToken,
    owners: Mutex<HashMap<String, Arc<OwnerControl>>>,
    released: Notify,
}

#[derive(Default)]
struct ObservationState {
    generation: String,
    pending: Option<String>,
    observed: bool,
}
#[derive(Default)]
struct OwnerControl {
    cancel: CancellationToken,
    observation: Mutex<ObservationState>,
}
impl OwnerControl {
    fn invalidate(&self) {
        let mut state = self.observation.lock().unwrap();
        state.observed = false;
        state.pending = None;
    }
}

struct Backend {
    send: mpsc::Sender<Request>,
    control: Arc<Control>,
}

impl fmt::Debug for Backend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GatewayComputerUseBackend")
            .finish_non_exhaustive()
    }
}

struct Request {
    owner: String,
    owner_cancel: CancellationToken,
    owner_control: Arc<OwnerControl>,
    cancellation: CancellationToken,
    name: String,
    arguments: Value,
    reply: oneshot::Sender<Result<Value, ToolError>>,
}

#[async_trait]
impl ComputerUseBackend for Backend {
    async fn execute(
        &self,
        owner: &str,
        name: &str,
        arguments: Value,
        cancellation: CancellationToken,
    ) -> Result<Value, ToolError> {
        if self.control.shutdown.is_cancelled() {
            return Err(unavailable("Gateway is stopping"));
        }
        if cancellation.is_cancelled() {
            return Err(ToolError::Cancelled(
                "CUA call cancelled before admission".into(),
            ));
        }
        // This is also checked by the tool registry; do not turn a trait call
        // into an unrestricted daemon administration or prefix-routing API.
        if !pioneer_tools::is_computer_use_tool(&format!("cua_{name}")) {
            return Err(ToolError::InvalidArguments(
                "operation is not in the Pioneer CUA allowlist".into(),
            ));
        }
        let permit = self
            .send
            .try_reserve()
            .map_err(|_| unavailable("computer-use action queue is full or stopped"))?;
        let owner_control = {
            let mut owners = self.control.owners.lock().unwrap();
            if !owners.contains_key(owner) && owners.len() >= MAX_OWNERS {
                return Err(unavailable("computer-use session admission is full"));
            }
            owners.entry(owner.to_owned()).or_default().clone()
        };
        let (reply, receive) = oneshot::channel();
        let request = Request {
            owner: owner.to_owned(),
            owner_cancel: owner_control.cancel.clone(),
            owner_control,
            cancellation: cancellation.clone(),
            name: name.to_owned(),
            arguments,
            reply,
        };
        // Admission is bounded and nonblocking. A rejected request never enters
        // IPC, so overload cannot become an unknown mutation.
        permit.send(request);
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(interrupted()),
            _ = self.control.shutdown.cancelled() => Err(interrupted()),
            result = receive => result.unwrap_or_else(|_| Err(interrupted())),
        }
        // Dropping receive causes reply.closed() in the worker. That worker
        // keeps ownership and stops the generation before processing more work.
    }

    fn observation_delivered(&self, owner: &str, context: &Value) -> bool {
        let control = self.control.owners.lock().unwrap().get(owner).cloned();
        let Some(control) = control else { return false };
        let mut state = control.observation.lock().unwrap();
        if control.cancel.is_cancelled()
            || self.control.shutdown.is_cancelled()
            || state.pending.is_none()
            || context["generation"].as_str() != Some(state.generation.as_str())
            || context["delivery_nonce"].as_str() != state.pending.as_deref()
        {
            return false;
        }
        state.pending = None;
        state.observed = true;
        true
    }

    fn release_owner(&self, owner: &str) {
        if let Some(control) = self.control.owners.lock().unwrap().remove(owner) {
            control.invalidate();
            control.cancel.cancel();
            self.control.released.notify_one();
        }
    }
}

struct OwnerSession {
    session: Arc<CuaDriverSession>,
    public_session: String,
    control: Arc<OwnerControl>,
}

struct Generation {
    host: Arc<EmbeddedCuaDriverHost>,
    driver: Arc<CuaDriver>,
    identity_checked: bool,
    id: String,
    // Keep the private directory alive until this generation is reaped.
    _directory: tempfile::TempDir,
}

struct Worker {
    runtime_home: PathBuf,
    control: Arc<Control>,
    generation: Option<Generation>,
    sessions: HashMap<String, OwnerSession>,
    ownership: Option<File>,
    poisoned: bool,
}

impl Worker {
    fn new(runtime_home: PathBuf, control: Arc<Control>) -> Self {
        Self {
            runtime_home,
            control,
            generation: None,
            sessions: HashMap::new(),
            ownership: None,
            poisoned: false,
        }
    }

    async fn run(mut self, mut receive: mpsc::Receiver<Request>) {
        loop {
            tokio::select! {
                biased;
                _ = self.control.shutdown.cancelled() => break,
                _ = self.control.released.notified() => self.prune_sessions().await,
                request = receive.recv() => {
                    let Some(mut request) = request else { break };
                    self.prune_sessions().await;
                    if request.reply.is_closed() || request.cancellation.is_cancelled() || request.owner_cancel.is_cancelled() {
                        let _ = request.reply.send(Err(interrupted()));
                        continue;
                    }
                    if let Some(generation) = &self.generation {
                        if generation.host.connection().is_none() {
                            self.retire_generation().await;
                        }
                    }
                    if self.generation.is_none() {
                        if let Err(error) = self.start_generation(&mut request).await {
                            self.retire_generation().await;
                            let _ = request.reply.send(Err(error));
                            continue;
                        }
                    }
                    // select owns the operation here, rather than in the caller.
                    // Caller disappearance can never detach a mutating SDK task
                    // and then admit another action on its still-live daemon.
                    let shutdown = self.control.shutdown.clone();
                    let result = tokio::select! {
                        biased;
                        _ = shutdown.cancelled() => None,
                        _ = request.cancellation.cancelled() => None,
                        _ = request.owner_cancel.cancelled() => None,
                        _ = request.reply.closed() => None,
                        result = tokio::time::timeout(CALL_DEADLINE, self.execute(&request.owner, &request.name, request.arguments.take(), request.owner_control.clone())) => result.ok(),
                    };
                    let result = match result {
                        Some(result) => result,
                        None => {
                            self.retire_generation().await;
                            Err(interrupted())
                        }
                    };
                    if request.reply.send(result).is_err() {
                        // The response may have been lost after a successful
                        // mutation. No new caller may reuse that generation.
                        self.retire_generation().await;
                    }
                }
            }
        }
        receive.close();
        self.retire_generation().await;
        while let Some(request) = receive.recv().await {
            let _ = request.reply.send(Err(interrupted()));
        }
    }

    async fn execute(
        &mut self,
        owner: &str,
        name: &str,
        mut arguments: Value,
        owner_control: Arc<OwnerControl>,
    ) -> Result<Value, ToolError> {
        if self.poisoned {
            return Err(unavailable(
                "the previous daemon could not be reaped; restart Gateway before further computer use",
            ));
        }
        if !arguments.is_object() {
            return Err(ToolError::InvalidArguments(
                "CUA arguments must be an object".into(),
            ));
        }
        if arguments
            .as_object()
            .unwrap()
            .keys()
            .any(|key| key == "session" || key.starts_with('_'))
        {
            return Err(ToolError::InvalidArguments(
                "session and reserved transport authority belong to Gateway".into(),
            ));
        }
        let generation = self.generation.as_mut().unwrap();
        if !generation.identity_checked {
            if let Err(error) = verify_identity(&generation.driver).await {
                self.retire_generation().await;
                return Err(error);
            }
            self.generation.as_mut().unwrap().identity_checked = true;
        }
        if !self.sessions.contains_key(owner) {
            let driver = self.generation.as_ref().unwrap().driver.clone();
            let public_session = format!("pioneer-{}", uuid::Uuid::new_v4().simple());
            let session_label = public_session.clone();
            let session = tokio::task::spawn_blocking(move || {
                driver.create_trusted_session(TrustedSessionOptions {
                    public_session,
                    mode: SessionPermissionMode::Standard,
                    ttl_seconds: 86_400,
                    idle_ttl_seconds: 3_600,
                    capability_manifest_path: None,
                    bounded_manifest_path: None,
                })
            })
            .await;
            let session = match session {
                Ok(Ok(session)) => session,
                _ => {
                    self.retire_generation().await;
                    return Err(unavailable("failed to bind a private computer-use session"));
                }
            };
            self.sessions.insert(
                owner.to_owned(),
                OwnerSession {
                    session,
                    public_session: session_label,
                    control: owner_control,
                },
            );
        }
        let entry = self.sessions.get(owner).unwrap();
        let observation = pioneer_tools::handlers::computer_use::is_passive_operation(name);
        if !observation && !entry.control.observation.lock().unwrap().observed {
            return Err(ToolError::Rejected("A fresh cua_get_window_state or cua_get_desktop_state observation is required before an action. Old references must not be replayed after interruption or daemon restart.".into()));
        }
        let session = entry.session.clone();
        // Require a fresh observation after every input. This is an evidence
        // boundary, not a second goal-verification or automatic retry engine.
        if !observation {
            // The physical computer is shared: input can invalidate another
            // turn's earlier observation as well as this turn's observation.
            for entry in self.sessions.values() {
                entry.control.invalidate();
            }
        }
        if matches!(
            name,
            "get_window_state" | "get_desktop_state" | "get_browser_state"
        ) {
            self.sessions.get(owner).unwrap().control.invalidate();
        }
        if let Err(error) = ensure_cursor_visible(&session).await {
            self.retire_generation().await;
            return Err(error);
        }
        // The trusted bound session supplies its own identity; no agent chooses
        // a session or endpoint. Preserve all operation coordinates and tokens.
        arguments.as_object_mut().unwrap().remove("session");
        let result = session
            .call_tool(name.to_owned(), arguments.to_string())
            .await;
        let result = match result {
            Ok(result) => result,
            Err(_) => {
                self.retire_generation().await;
                return Err(interrupted());
            }
        };
        let mut raw: Value = match serde_json::from_str(&result.raw_json) {
            Ok(raw) => raw,
            Err(_) => {
                self.retire_generation().await;
                return Err(interrupted());
            }
        };
        if !raw.is_object() {
            self.retire_generation().await;
            return Err(interrupted());
        }
        // Observation references stay attached to the producing generation and
        // private session in model/history content. These fields are descriptive;
        // the private delivery nonce added below is stripped by the trusted
        // handler and acknowledged only through its typed local receipt.
        raw["pioneer_cua_context"] = json!({
            "generation": self.generation.as_ref().unwrap().id,
            "session": self.sessions.get(owner).unwrap().public_session,
        });
        if matches!(
            name,
            "get_window_state" | "get_desktop_state" | "get_browser_state"
        ) && !result.is_error
        {
            let generation = self.generation.as_ref().unwrap().id.clone();
            let nonce = uuid::Uuid::new_v4().to_string();
            let mut state = self
                .sessions
                .get(owner)
                .unwrap()
                .control
                .observation
                .lock()
                .unwrap();
            state.generation = generation;
            state.pending = Some(nonce.clone());
            // No input authority until the typed outer delivery receipt commits.
            state.observed = false;
            raw["pioneer_cua_context"]["delivery_nonce"] = nonce.into();
        }
        // Do not interpret delivered input as goal completion. Upstream action
        // effect/refusal/partial/verification facts pass through unchanged.
        Ok(raw)
    }

    async fn start_generation(&mut self, request: &mut Request) -> Result<(), ToolError> {
        if self.poisoned {
            return Err(unavailable(
                "previous daemon termination failed; restart Gateway",
            ));
        }
        if self.ownership.is_none() {
            self.ownership = Some(acquire_ownership(&self.runtime_home)?);
        }
        let binary = bundled_driver_path()?;
        let directory = tempfile::Builder::new()
            .prefix("pioneer-cua-")
            .tempdir()
            .map_err(|_| unavailable("cannot create private Pioneer driver directory"))?;
        #[cfg(unix)]
        let endpoint = directory
            .path()
            .join("driver.sock")
            .to_string_lossy()
            .into_owned();
        #[cfg(windows)]
        let endpoint = format!(r"\\.\pipe\pioneer-cua-{}", uuid::Uuid::new_v4().simple());
        let data_home = self.runtime_home.join("cua");
        std::fs::create_dir_all(&data_home)
            .map_err(|_| unavailable("cannot create Pioneer driver data directory"))?;
        let driver = CuaDriver::connect(Some(endpoint.clone()))
            .map_err(|_| unavailable("cannot create owned daemon client"))?;
        let host = EmbeddedCuaDriverHost::with_options(EmbeddedDriverHostOptions {
            binary_path: binary.to_string_lossy().into_owned(),
            host_bundle_id: HOST_BUNDLE_ID.to_owned(),
            socket_path: Some(endpoint),
            startup_timeout_ms: Some(10_000),
            shutdown_timeout_ms: Some(2_000),
            permission_mode: Some(EmbeddedPermissionMode::Standard),
            capability_manifest_path: None,
            approve_capability_manifest: false,
            session_policy_path: None,
            approve_session_policy: false,
            dangerously_bypass_approvals: false,
            inherit_stderr: false,
            no_overlay: false,
            environment: vec![
                environment("CUA_DRIVER_RS_HOME", &data_home.to_string_lossy()),
                environment("CUA_DRIVER_RS_TELEMETRY_ENABLED", "false"),
                environment("CUA_TELEMETRY_ENABLED", "false"),
                environment("CUA_DRIVER_RS_UPDATE_CHECK", "false"),
            ],
        })
        .map_err(|_| unavailable("invalid trusted CUA launch configuration"))?;
        // Keep host owned even if the startup future is cancelled. Its stop()
        // cancels startup and waits for the child; callers cannot own this Arc.
        self.generation = Some(Generation {
            host: host.clone(),
            driver,
            identity_checked: false,
            id: String::new(),
            _directory: directory,
        });
        let start = host.clone().start();
        tokio::pin!(start);
        // Poll startup before cancellation, then keep polling it together with
        // stop. Dropping upstream's startup future could kill-on-drop without
        // waiting for the child and would not provide our generation barrier.
        let connection = tokio::select! {
            biased;
            result = &mut start => result.map_err(startup_failure)?,
            _ = self.control.shutdown.cancelled() => {
                let (_, stopped) = tokio::join!(&mut start, host.clone().stop());
                if stopped.is_err() { self.poisoned = true; }
                return Err(interrupted());
            }
            _ = request.cancellation.cancelled() => {
                let (_, stopped) = tokio::join!(&mut start, host.clone().stop());
                if stopped.is_err() { self.poisoned = true; }
                return Err(interrupted());
            }
            _ = request.owner_cancel.cancelled() => {
                let (_, stopped) = tokio::join!(&mut start, host.clone().stop());
                if stopped.is_err() { self.poisoned = true; }
                return Err(interrupted());
            }
            _ = request.reply.closed() => {
                let (_, stopped) = tokio::join!(&mut start, host.clone().stop());
                if stopped.is_err() { self.poisoned = true; }
                return Err(interrupted());
            }
        };
        record_daemon_pid(self.ownership.as_mut().unwrap(), Some(connection.pid))?;
        if connection.driver_version != CUA_DRIVER_VERSION {
            return Err(unavailable(
                "bundled CUA daemon version does not match the selected release",
            ));
        }
        self.generation.as_mut().unwrap().id = connection.generation;
        Ok(())
    }

    async fn prune_sessions(&mut self) {
        let ended: Vec<_> = self
            .sessions
            .iter()
            .filter(|(_, session)| session.control.cancel.is_cancelled())
            .map(|(owner, _)| owner.clone())
            .collect();
        if !ended.is_empty() {
            // Session cleanup can close a driver-owned browser/window. Other
            // turns must observe the shared computer again after that change.
            for session in self.sessions.values() {
                session.control.invalidate();
            }
        }
        for owner in ended {
            if let Some(session) = self.sessions.remove(&owner) {
                let close = async move {
                    // SDK close() is infallible and discards the daemon reply.
                    // First observe upstream's real session/cursor cleanup;
                    // any partial, lost or timed-out cleanup retires the daemon.
                    let result = session
                        .session
                        .call_tool("end_session".into(), "{}".into())
                        .await
                        .map_err(|_| ())?;
                    let raw: Value = serde_json::from_str(&result.raw_json).map_err(|_| ())?;
                    if result.is_error || raw["structuredContent"]["active"] != false {
                        return Err(());
                    }
                    tokio::task::spawn_blocking(move || session.session.close())
                        .await
                        .map_err(|_| ())?;
                    Ok(())
                };
                if !matches!(
                    tokio::time::timeout(Duration::from_secs(2), close).await,
                    Ok(Ok(()))
                ) {
                    self.retire_generation().await;
                    return;
                }
            }
        }
    }

    async fn retire_generation(&mut self) {
        if let Some(generation) = self.generation.take() {
            for session in self.sessions.values() {
                session.control.invalidate();
            }
            // stop waits for process exit (or kills and reaps). On failure, fail
            // closed permanently instead of starting a concurrent generation.
            if generation.host.stop().await.is_err() {
                self.poisoned = true;
            } else if let Some(file) = self.ownership.as_mut() {
                if record_daemon_pid(file, None).is_err() {
                    self.poisoned = true;
                }
            }
            // Drop SDK handles only after the old process is gone. Each new
            // generation uses a different endpoint; an old SDK resume cannot
            // cross into a replacement daemon.
            let sessions = std::mem::take(&mut self.sessions);
            let _ = tokio::task::spawn_blocking(move || drop(sessions)).await;
        }
    }
}

fn environment(name: &str, value: &str) -> EmbeddedEnvironmentVariable {
    EmbeddedEnvironmentVariable {
        name: name.into(),
        value: value.into(),
    }
}

fn bundled_driver_path() -> Result<PathBuf, ToolError> {
    let executable = std::env::current_exe()
        .and_then(std::fs::canonicalize)
        .map_err(|_| unavailable("cannot locate installed Gateway executable"))?;
    let parent = executable
        .parent()
        .ok_or_else(|| unavailable("invalid Gateway installation"))?;
    #[cfg(windows)]
    let binary = parent.join("cua/cua-driver.exe");
    #[cfg(not(windows))]
    let binary = if cfg!(target_os = "macos") {
        parent
            .parent()
            .ok_or_else(|| unavailable("invalid Gateway app bundle"))?
            .join("Helpers/cua-driver")
    } else {
        parent.join("cua/cua-driver")
    };
    let metadata = std::fs::symlink_metadata(&binary).map_err(|_| {
        unavailable("bundled cua-driver is missing; repair or update Gateway on this machine")
    })?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(unavailable(
            "bundled cua-driver is not a regular installed file",
        ));
    }
    verify_packaged_driver(parent, &binary)?;
    Ok(binary)
}

fn verify_packaged_driver(executable_dir: &Path, binary: &Path) -> Result<(), ToolError> {
    use sha2::{Digest, Sha256};
    #[cfg(target_os = "macos")]
    let payload = executable_dir
        .ancestors()
        .nth(3)
        .ok_or_else(|| unavailable("Gateway application payload is missing"))?
        .to_path_buf();
    #[cfg(not(target_os = "macos"))]
    let payload = executable_dir.join("cua");
    let manifest_path = payload.join("manifest.json");
    let metadata = std::fs::symlink_metadata(&manifest_path)
        .map_err(|_| unavailable("Gateway CUA provenance manifest is missing"))?;
    if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() > 16 * 1024 {
        return Err(unavailable("Gateway CUA provenance manifest is invalid"));
    }
    let manifest: Value = serde_json::from_slice(
        &std::fs::read(manifest_path)
            .map_err(|_| unavailable("cannot read Gateway CUA provenance"))?,
    )
    .map_err(|_| unavailable("Gateway CUA provenance is invalid"))?;
    let relative = binary
        .strip_prefix(&payload)
        .map_err(|_| unavailable("driver is outside the installed Gateway payload"))?;
    let relative = relative.to_string_lossy().replace('\\', "/");
    if manifest["schema_version"] != 1
        || manifest["repository"] != "https://github.com/trycua/cua"
        || manifest["revision"] != CUA_DRIVER_REVISION
        || manifest["version"] != CUA_DRIVER_VERSION
        || manifest["permission_owner"] != "gateway"
        || manifest["target"].as_str() != Some(packaged_target()?)
        || manifest["daemon"].as_str() != Some(relative.as_str())
    {
        return Err(unavailable(
            "Gateway CUA payload does not match its pinned contract",
        ));
    }
    let mut input =
        File::open(binary).map_err(|_| unavailable("cannot inspect packaged driver"))?;
    let mut digest = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let count = input
            .read(&mut buffer)
            .map_err(|_| unavailable("cannot verify packaged driver"))?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    if manifest["sha256"].as_str() != Some(hex::encode(digest.finalize()).as_str()) {
        return Err(unavailable(
            "packaged driver digest does not match its provenance manifest",
        ));
    }
    Ok(())
}

fn packaged_target() -> Result<&'static str, ToolError> {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("macos", "x86_64") => Ok("x86_64-apple-darwin"),
        ("macos", "aarch64") => Ok("aarch64-apple-darwin"),
        ("linux", "x86_64") => Ok("x86_64-unknown-linux-gnu"),
        ("linux", "aarch64") => Ok("aarch64-unknown-linux-gnu"),
        ("windows", "x86_64") => Ok("x86_64-pc-windows-msvc"),
        ("windows", "aarch64") => Ok("aarch64-pc-windows-msvc"),
        _ => Err(unavailable("no Pioneer CUA package exists for this target")),
    }
}

fn startup_failure(error: EmbeddedDriverError) -> ToolError {
    // Preserve the meaningful failure category without propagating paths,
    // native stderr, environment values or upstream payloads to audit metadata.
    match error {
        EmbeddedDriverError::StartupCancelled => interrupted(),
        EmbeddedDriverError::Spawn { .. } => unavailable(
            "the installed CUA executable could not start; repair the Gateway package and native runtime dependencies on this machine",
        ),
        EmbeddedDriverError::ExitedBeforeReady { code } => unavailable(&format!(
            "CUA exited before readiness (exit code {code:?}); verify the Gateway graphical login session and packaged native runtime dependencies"
        )),
        EmbeddedDriverError::StartupTimeout { .. } => unavailable(
            "CUA readiness timed out; verify the graphical login session on the Gateway machine",
        ),
        EmbeddedDriverError::IncompatibleDaemon { .. } => unavailable(
            "the owned daemon failed the PID/contract/version readiness handshake; repair the Gateway package",
        ),
        EmbeddedDriverError::EndpointConflict { .. } => unavailable(
            "the private CUA endpoint is already occupied or unsafe; no foreign daemon was attached",
        ),
        EmbeddedDriverError::Configuration { .. } => unavailable(
            "the trusted daemon configuration conflicts with its standard permission mode or private IPC contract",
        ),
        EmbeddedDriverError::Lifecycle { .. } => {
            unavailable("the owned CUA process lifecycle failed; restart Gateway before retrying")
        }
    }
}

async fn verify_identity(driver: &Arc<CuaDriver>) -> Result<(), ToolError> {
    #[cfg(target_os = "macos")]
    {
        let health = driver
            .call_tool(
                "health_report".into(),
                json!({"include":["bundle_identity"]}).to_string(),
            )
            .await
            .map_err(|_| unavailable("cannot observe Gateway application identity"))?;
        let raw: Value = serde_json::from_str(&health.raw_json)
            .map_err(|_| unavailable("invalid Gateway identity report"))?;
        let valid = !health.is_error
            && raw["structuredContent"]["checks"]
                .as_array()
                .is_some_and(|checks| {
                    checks.iter().any(|check| {
                        check["name"] == "bundle_identity"
                            && check["status"] == "pass"
                            && check["data"]["bundle_identifier"] == HOST_BUNDLE_ID
                            && check["data"]["identity_source"] == "parent_application"
                            && check["data"]["parent_process_id"].as_u64()
                                == Some(u64::from(std::process::id()))
                    })
                });
        if !valid {
            return Err(unavailable(
                "CUA did not observe Pioneer Gateway as its parent application. Start the installed Pioneer Gateway.app in the logged-in GUI session; grant that app Accessibility and Screen Recording, then restart Gateway.",
            ));
        }
        let permissions = driver
            .call_tool(
                "check_permissions".into(),
                json!({"prompt":false}).to_string(),
            )
            .await
            .map_err(|_| unavailable("cannot inspect Gateway OS grants"))?;
        let raw: Value = serde_json::from_str(&permissions.raw_json)
            .map_err(|_| unavailable("invalid Gateway OS permission report"))?;
        if raw["structuredContent"]["source"]["attribution"] != "host" {
            return Err(unavailable(
                "OS permission attribution does not belong to the Gateway host",
            ));
        }
        if raw["structuredContent"]["accessibility"] != true {
            return Err(unavailable(
                "Accessibility permission is absent. On the Gateway machine, allow Pioneer Gateway.app in System Settings > Privacy & Security > Accessibility, then restart Gateway.",
            ));
        }
        if raw["structuredContent"]["screen_recording"] != true {
            return Err(unavailable(
                "Screen Recording permission is absent. On the Gateway machine, allow Pioneer Gateway.app in System Settings > Privacy & Security > Screen Recording, then restart Gateway.",
            ));
        }
    }
    #[cfg(not(target_os = "macos"))]
    let _ = driver;
    Ok(())
}

fn acquire_ownership(runtime_home: &Path) -> Result<File, ToolError> {
    let root = runtime_home.join("cua");
    std::fs::create_dir_all(&root)
        .map_err(|_| unavailable("cannot prepare Pioneer driver ownership"))?;
    let metadata = std::fs::symlink_metadata(&root)
        .map_err(|_| unavailable("cannot inspect Pioneer driver ownership"))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(unavailable("Pioneer driver runtime directory is unsafe"));
    }
    let mut options = OpenOptions::new();
    options.create(true).read(true).write(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
        // SAFETY: geteuid has no arguments or memory-safety preconditions.
        if metadata.uid() != unsafe { libc::geteuid() } {
            return Err(unavailable(
                "Pioneer driver directory belongs to another user",
            ));
        }
        options
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
            .map_err(|_| unavailable("cannot secure Pioneer driver runtime directory"))?;
    }
    let file = options
        .open(root.join("owner.lock"))
        .map_err(|_| unavailable("cannot open Pioneer driver ownership lock"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let metadata = file
            .metadata()
            .map_err(|_| unavailable("cannot inspect driver lock ownership"))?;
        // SAFETY: geteuid has no arguments or memory-safety preconditions.
        if !metadata.is_file()
            || metadata.uid() != unsafe { libc::geteuid() }
            || metadata.nlink() != 1
        {
            return Err(unavailable("Pioneer driver ownership lock is unsafe"));
        }
    }
    FileExt::try_lock(&file)
        .map_err(|_| unavailable("another Gateway owns computer use in this runtime home"))?;
    // A process lock alone is insufficient across a crashed Gateway: its child
    // can still be draining the parent-liveness EOF when launchd restarts us.
    // Keep only a bounded numeric PID receipt; never attach to or kill a prior
    // process. PID reuse fails closed rather than signalling a foreign process.
    let mut prior = String::new();
    (&file)
        .take(64)
        .read_to_string(&mut prior)
        .map_err(|_| unavailable("cannot read prior driver ownership receipt"))?;
    if !prior.trim().is_empty() {
        let pid: u32 = prior.trim().parse().map_err(|_| unavailable("prior driver receipt is invalid; explicit Gateway installation recovery is required"))?;
        if pid == 0 || process_is_live(pid) {
            return Err(unavailable(
                "a prior driver generation has not exited yet; no replacement was started. If its PID was reused, repair the ownership receipt only after confirming the old driver is gone.",
            ));
        }
    }
    Ok(file)
}

fn record_daemon_pid(file: &mut File, pid: Option<u32>) -> Result<(), ToolError> {
    file.seek(SeekFrom::Start(0))
        .and_then(|_| file.set_len(0))
        .map_err(|_| unavailable("cannot update driver ownership receipt"))?;
    if let Some(pid) = pid {
        writeln!(file, "{pid}")
            .map_err(|_| unavailable("cannot record driver generation ownership"))?;
    }
    file.sync_data()
        .map_err(|_| unavailable("cannot persist driver ownership receipt"))
}

#[cfg(unix)]
fn process_is_live(pid: u32) -> bool {
    let Ok(pid) = libc::pid_t::try_from(pid) else {
        return true;
    };
    // SAFETY: signal 0 checks process existence without sending a signal.
    (unsafe { libc::kill(pid, 0) == 0 })
        || std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
}

#[cfg(windows)]
fn process_is_live(pid: u32) -> bool {
    use windows_sys::Win32::Foundation::{
        CloseHandle, ERROR_INVALID_PARAMETER, GetLastError, STILL_ACTIVE,
    };
    use windows_sys::Win32::System::Threading::{
        GetExitCodeProcess, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
    };
    // SAFETY: query-only access, a numeric PID, valid out pointer, and the
    // returned handle is closed exactly once. Failure is treated as still live.
    unsafe {
        let handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
        if handle.is_null() {
            return GetLastError() != ERROR_INVALID_PARAMETER;
        }
        let mut code = 0;
        let live = GetExitCodeProcess(handle, &mut code) == 0 || code == STILL_ACTIVE as u32;
        CloseHandle(handle);
        live
    }
}

async fn ensure_cursor_visible(session: &Arc<CuaDriverSession>) -> Result<(), ToolError> {
    async fn call(
        session: &Arc<CuaDriverSession>,
        name: &str,
        arguments: Value,
    ) -> Result<Value, ToolError> {
        let result = session
            .call_tool(name.into(), arguments.to_string())
            .await
            .map_err(|_| {
                unavailable("CUA cursor renderer is unavailable in the Gateway graphical session")
            })?;
        if result.is_error {
            return Err(unavailable(
                "CUA cursor renderer refused visibility; configure the Gateway graphical session and OS grants",
            ));
        }
        serde_json::from_str(&result.raw_json)
            .map_err(|_| unavailable("invalid CUA cursor readiness response"))
    }
    call(session, "set_agent_cursor_enabled", json!({"enabled":true})).await?;
    let state = call(session, "get_agent_cursor_state", json!({})).await?;
    if state["structuredContent"]["enabled"] != true {
        return Err(unavailable("CUA agent cursor is disabled"));
    }
    let visible = call(session, "get_session", json!({})).await?;
    if visible["structuredContent"]["cursor_visible"] == true {
        return Ok(());
    }
    // Upstream window scope, without any target, moves ONLY its rendered agent
    // overlay on all three platforms. It neither moves the real pointer nor
    // sends application hover/input. No screenshot coordinate conversion.
    let position = &state["structuredContent"]["position"];
    call(session, "move_cursor", json!({"scope":"window", "x":position["x"].as_f64().unwrap_or(32.0), "y":position["y"].as_f64().unwrap_or(32.0)})).await?;
    for _ in 0..20 {
        let visible = call(session, "get_session", json!({})).await?;
        if visible["structuredContent"]["cursor_visible"] == true {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(unavailable(
        "CUA renderer did not report a visible agent cursor. No application input was sent. Verify display/capture and GUI session availability on the Gateway machine.",
    ))
}

fn unavailable(reason: &str) -> ToolError {
    ToolError::Rejected(format!("Gateway computer use unavailable: {reason}"))
}

fn interrupted() -> ToolError {
    ToolError::ExecutionFailed("CUA completion is unknown or cancelled; its daemon generation is being retired. Do not replay this action. Obtain a fresh observation before choosing any new action.".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restarting_gateway_waits_for_the_previous_driver_process_to_exit() {
        let root = tempfile::tempdir().unwrap();
        let mut first_owner = acquire_ownership(root.path()).unwrap();
        assert!(acquire_ownership(root.path()).is_err());
        // Use this live test process solely as a liveness witness. No daemon,
        // GUI, screen, grants or action success is simulated by this fixture.
        record_daemon_pid(&mut first_owner, Some(std::process::id())).unwrap();
        drop(first_owner);
        let failure = acquire_ownership(root.path()).unwrap_err();
        assert!(
            matches!(failure, ToolError::Rejected(ref reason) if reason.contains("prior driver generation has not exited"))
        );
    }

    fn admission() -> (Backend, mpsc::Receiver<Request>) {
        let (send, receive) = mpsc::channel(1);
        (
            Backend {
                send,
                control: Arc::new(Control {
                    shutdown: CancellationToken::new(),
                    owners: Mutex::new(HashMap::new()),
                    released: Notify::new(),
                }),
            },
            receive,
        )
    }

    // Owner: Gateway backend admission. No daemon is mocked or executed; the
    // receiver only acknowledges queue admission. Repeated overload must not
    // consume session capacity that prevents a later admitted turn.
    #[tokio::test]
    async fn rejected_queue_admission_does_not_exhaust_turn_capacity() {
        let (backend, mut receive) = admission();
        let occupied = backend.send.try_reserve().unwrap();
        for owner in 0..MAX_OWNERS + 1 {
            let error = backend
                .execute(
                    &format!("rejected-{owner}"),
                    "list_apps",
                    json!({}),
                    CancellationToken::new(),
                )
                .await
                .unwrap_err();
            assert!(
                matches!(error, ToolError::Rejected(ref reason) if reason.contains("action queue is full"))
            );
        }
        drop(occupied);
        let (result, ()) = tokio::join!(
            backend.execute("admitted", "list_apps", json!({}), CancellationToken::new()),
            async {
                receive
                    .recv()
                    .await
                    .unwrap()
                    .reply
                    .send(Ok(json!({"admitted":true})))
                    .unwrap();
            }
        );
        assert_eq!(result.unwrap(), json!({"admitted":true}));
    }

    // Losing the waiting caller must become visible to the independently
    // owned worker, whose production path retires a live generation first.
    #[tokio::test]
    async fn cancelled_waiter_closes_worker_reply_without_resubmission() {
        let (backend, mut receive) = admission();
        let cancellation = CancellationToken::new();
        let (result, ()) = tokio::join!(
            backend.execute("turn", "list_apps", json!({}), cancellation.clone()),
            async {
                let mut request = receive.recv().await.unwrap();
                cancellation.cancel();
                request.reply.closed().await;
                assert!(receive.try_recv().is_err());
            }
        );
        assert!(
            matches!(result, Err(ToolError::ExecutionFailed(ref reason)) if reason.contains("Do not replay"))
        );
    }
}
