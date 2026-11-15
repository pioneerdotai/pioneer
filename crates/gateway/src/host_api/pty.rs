//! Interactive PTYs under the Gateway OS account. No UI or user impersonation.
use anyhow::{Context, Result, bail};
use portable_pty::{CommandBuilder, PtySize};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, VecDeque},
    io::{Read, Write},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

pub(super) const MAX_SESSIONS: usize = 8;
const INPUT_HISTORY_FRAMES: usize = 64;
const INPUT_HISTORY_BYTES: usize = 128 * 1024;
const MAX_OUTPUT: usize = 256 * 1024;
#[derive(Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Dimensions {
    rows: u16,
    cols: u16,
    #[serde(default)]
    pixel_width: u16,
    #[serde(default)]
    pixel_height: u16,
}
impl Dimensions {
    fn size(self) -> Result<PtySize> {
        if self.rows == 0 || self.cols == 0 || self.rows > 1000 || self.cols > 1000 {
            bail!("host.invalid_dimensions");
        }
        Ok(PtySize {
            rows: self.rows,
            cols: self.cols,
            pixel_width: self.pixel_width,
            pixel_height: self.pixel_height,
        })
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Create {
    key: String,
    #[serde(default)]
    command: Option<Vec<String>>,
    cwd: Option<String>,
    dimensions: Dimensions,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    session_id: String,
    sequence: u64,
    bytes: Vec<u8>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Resize {
    session_id: String,
    dimensions: Dimensions,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Session {
    session_id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadOutput {
    session_id: String,
    after: u64,
}
struct Output {
    start: u64,
    bytes: VecDeque<u8>,
    exited: Option<u32>,
    eof: bool,
    failed: bool,
}
impl Output {
    fn push(&mut self, bytes: &[u8]) {
        for byte in bytes {
            if self.bytes.len() == MAX_OUTPUT {
                self.bytes.pop_front();
                self.start += 1;
            }
            self.bytes.push_back(*byte);
        }
    }
}

struct Process {
    child: Option<Box<dyn portable_pty::Child + Send + Sync>>,
}
impl Drop for Process {
    fn drop(&mut self) {
        // Covers thread-creation failure before the reaper takes ownership.
        if let Some(child) = &mut self.child {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
pub(super) enum PtyInput {
    Data(Vec<u8>),
    #[cfg(windows)]
    CursorReply,
}

type SharedMaster = Arc<Mutex<Option<Box<dyn portable_pty::MasterPty + Send>>>>;
struct Pty {
    master: SharedMaster,
    writer: Option<std::sync::mpsc::SyncSender<PtyInput>>,
    #[cfg(windows)]
    input_worker: Option<std::thread::JoinHandle<()>>,
    #[cfg(windows)]
    output_worker: Option<std::thread::JoinHandle<()>>,
    #[cfg(windows)]
    exit_worker: Option<std::thread::JoinHandle<()>>,
    process: Arc<Mutex<Process>>,
    io_cancel: Arc<std::sync::atomic::AtomicBool>,
    output: Arc<Mutex<Output>>,
    input_sequence: u64,
    input_history: VecDeque<(u64, Vec<u8>)>,
    input_history_bytes: usize,
    touched: Instant,
}
impl Drop for Pty {
    fn drop(&mut self) {
        self.io_cancel
            .store(true, std::sync::atomic::Ordering::Release);
        self.writer.take();
        #[cfg(windows)]
        {
            if let Some(worker) = &self.input_worker {
                cancel_windows_io(worker);
            }
            // Console ownership terminates the attached process tree. Close
            // before waiting for the shell, while the reader and internal
            // cursor-reply path are still alive. No clone-killer result needed.
            close_windows_console(&self.master);
        }
        // Reaping and signalling share this lock. A reaped numeric PID must
        // never be used later to signal an unrelated process after PID reuse.
        let mut process = self.process.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(child) = &mut process.child {
            #[cfg(unix)]
            {
                let foreground = self
                    .master
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .as_ref()
                    .and_then(|master| master.process_group_leader())
                    .filter(|p| *p > 0);
                if let Some(group) = foreground {
                    unsafe {
                        libc::kill(-group, libc::SIGKILL);
                    }
                }
                if let Some(pid) = child.process_id().and_then(|p| i32::try_from(p).ok()) {
                    unsafe {
                        libc::kill(-pid, libc::SIGKILL);
                    }
                }
                // The I/O workers are cancelled above. On Darwin, slave close
                // can wait for echoed output to drain even after SIGKILL;
                // release the master before waiting for the child to exit.
                self.master.lock().unwrap_or_else(|e| e.into_inner()).take();
            }
            let _ = child.kill();
            let _ = child.wait();
            process.child.take();
        }
        drop(process);
        #[cfg(windows)]
        {
            for worker in [
                self.input_worker.take(),
                self.output_worker.take(),
                self.exit_worker.take(),
            ]
            .into_iter()
            .flatten()
            {
                let _ = worker.join();
            }
        }
    }
}
struct IoEndpoint<T> {
    stream: T,
    #[cfg(unix)]
    fd: libc::c_int,
}

struct StartingPty {
    slave: Option<Box<dyn portable_pty::SlavePty>>,
    input_start: Option<std::sync::mpsc::SyncSender<IoEndpoint<Box<dyn Write + Send>>>>,
    output_start: Option<std::sync::mpsc::SyncSender<IoEndpoint<Box<dyn Read + Send>>>>,
    pty: Option<Pty>,
}
impl Drop for StartingPty {
    fn drop(&mut self) {
        self.slave.take();
        self.input_start.take();
        self.output_start.take();
        self.pty.take();
    }
}

#[derive(Default)]
pub(super) struct PtyOwner {
    output_signal: Arc<tokio::sync::Notify>,
    sessions: BTreeMap<String, Pty>,
    creates: BTreeMap<String, (String, Value)>,
}
impl PtyOwner {
    pub(super) fn with_output(output: Arc<tokio::sync::Notify>) -> Self {
        Self {
            output_signal: output,
            ..Default::default()
        }
    }
    pub(super) fn until_expiry(&self) -> Option<Duration> {
        self.sessions
            .values()
            .map(|p| Duration::from_secs(300).saturating_sub(p.touched.elapsed()))
            .min()
    }
    pub(super) fn output_heads(&self) -> Value {
        json!(self.sessions.iter().map(|(id,p)| { let out = p.output.lock().unwrap_or_else(|e| e.into_inner()); json!({"session_id":id,"start":out.start,"end":out.start+out.bytes.len() as u64,"exit_status":out.exited,"failed":out.failed}) }).collect::<Vec<_>>())
    }

    pub(super) fn clear(&mut self) {
        self.sessions.clear();
        self.creates.clear();
    }
    pub(super) fn expire(&mut self) {
        self.sessions
            .retain(|_, p| p.touched.elapsed() < Duration::from_secs(300));
    }
    pub(super) fn call(&mut self, method: &str, params: Value) -> Result<Value> {
        if !matches!(
            method,
            "host.pty.create"
                | "host.pty.input"
                | "host.pty.read"
                | "host.pty.resize"
                | "host.pty.close"
                | "host.pty.keepalive"
        ) {
            bail!("host.method_not_allowed");
        }
        self.expire();
        if method == "host.pty.create" {
            let p: Create =
                serde_json::from_value(params.clone()).context("host.invalid_params")?;
            if p.key.is_empty()
                || p.key.len() > 128
                || p.command.as_ref().is_some_and(|args| {
                    args.is_empty()
                        || args.len() > 128
                        || args
                            .iter()
                            .any(|v| v.is_empty() || v.len() > 16384 || v.contains('\0'))
                })
            {
                bail!("host.invalid_params");
            }
            if let Some((id, request)) = self.creates.get(&p.key) {
                if request != &params {
                    bail!("host.idempotency_conflict");
                }
                if !self.sessions.contains_key(id) {
                    bail!("host.session_expired");
                }
                return Ok(
                    json!({"session_id":id,"input_sequence":self.sessions[id].input_sequence,"retention_seconds":300}),
                );
            }
            if self.sessions.len() >= MAX_SESSIONS || self.creates.len() >= 64 {
                bail!("host.capacity");
            }
            #[cfg(not(any(unix, windows)))]
            {
                bail!("host.pty_unsupported_platform");
            }
            #[cfg(any(unix, windows))]
            {
                let mut command = match &p.command {
                    Some(args) => CommandBuilder::from_argv(
                        args.iter()
                            .map(|arg| std::ffi::OsString::from(arg.as_str()))
                            .collect(),
                    ),
                    // portable-pty resolves the host account's default program:
                    // login shell on Unix, ComSpec on Windows, with home cwd.
                    None => CommandBuilder::new_default_prog(),
                };
                if let Some(cwd) = &p.cwd {
                    if cwd.len() > 4096 || cwd.contains('\0') {
                        bail!("host.invalid_params");
                    }
                    #[cfg(windows)]
                    if !std::path::Path::new(cwd).is_dir() {
                        // portable-pty otherwise silently falls back to home.
                        bail!("host.invalid_params");
                    }
                    command.cwd(cwd);
                }
                super::shell::configure_environment(&mut command)?;
                let (input, receiver) = std::sync::mpsc::sync_channel::<PtyInput>(16);
                let output = Arc::new(Mutex::new(Output {
                    start: 0,
                    bytes: VecDeque::new(),
                    exited: None,
                    eof: false,
                    failed: false,
                }));
                let io_cancel = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let master: SharedMaster = Arc::new(Mutex::new(None));
                let process = Arc::new(Mutex::new(Process { child: None }));
                let (input_start, input_ready) =
                    std::sync::mpsc::sync_channel::<IoEndpoint<Box<dyn Write + Send>>>(1);
                let (output_start, output_ready) =
                    std::sync::mpsc::sync_channel::<IoEndpoint<Box<dyn Read + Send>>>(1);
                #[cfg(windows)]
                let cursor_reply = input.clone();
                let mut startup = StartingPty {
                    slave: None,
                    input_start: Some(input_start),
                    output_start: Some(output_start),
                    pty: Some(Pty {
                        master: master.clone(),
                        writer: Some(input),
                        #[cfg(windows)]
                        input_worker: None,
                        #[cfg(windows)]
                        output_worker: None,
                        #[cfg(windows)]
                        exit_worker: None,
                        process: process.clone(),
                        io_cancel: io_cancel.clone(),
                        output: output.clone(),
                        input_sequence: 0,
                        input_history: VecDeque::new(),
                        input_history_bytes: 0,
                        touched: Instant::now(),
                    }),
                };
                let pty = startup.pty.as_mut().expect("owned startup");
                let write_status = output.clone();
                let write_signal = self.output_signal.clone();
                let write_cancel = io_cancel.clone();
                let input_worker = std::thread::Builder::new()
                    .name("app-pty-input".into())
                    .spawn(move || {
                        let Ok(IoEndpoint {
                            stream: mut writer,
                            #[cfg(unix)]
                            fd,
                        }) = input_ready.recv()
                        else {
                            return;
                        };
                        while let Ok(frame) = receiver.recv() {
                            let (bytes, control) = match frame {
                                PtyInput::Data(bytes) => (bytes, false),
                                #[cfg(windows)]
                                PtyInput::CursorReply => {
                                    (super::windows::CURSOR_REPLY.to_vec(), true)
                                }
                            };
                            let mut offset = 0;
                            while offset < bytes.len()
                                && (control
                                    || !write_cancel.load(std::sync::atomic::Ordering::Acquire))
                            {
                                match writer.write(&bytes[offset..]) {
                                    Ok(0) => {
                                        write_status
                                            .lock()
                                            .unwrap_or_else(|e| e.into_inner())
                                            .failed = true;
                                        write_signal.notify_one();
                                        return;
                                    }
                                    Ok(n) => offset += n,
                                    Err(error)
                                        if error.kind() == std::io::ErrorKind::WouldBlock =>
                                    {
                                        #[cfg(unix)]
                                        if wait_io(fd, libc::POLLOUT, &write_cancel) {
                                            continue;
                                        }
                                        write_status
                                            .lock()
                                            .unwrap_or_else(|e| e.into_inner())
                                            .failed = true;
                                        write_signal.notify_one();
                                        return;
                                    }
                                    Err(error)
                                        if error.kind() == std::io::ErrorKind::Interrupted => {}
                                    Err(_) => {
                                        write_status
                                            .lock()
                                            .unwrap_or_else(|e| e.into_inner())
                                            .failed = true;
                                        write_signal.notify_one();
                                        return;
                                    }
                                }
                            }
                            // On Windows retain the control path through close:
                            // ConPTY can still need its initial cursor reply.
                            #[cfg(not(windows))]
                            if write_cancel.load(std::sync::atomic::Ordering::Acquire) {
                                return;
                            }
                        }
                    })
                    .context("host.operation_failed")?;
                #[cfg(windows)]
                {
                    pty.input_worker = Some(input_worker);
                }
                #[cfg(not(windows))]
                drop(input_worker);
                let buffer = output.clone();
                let signal = self.output_signal.clone();
                let output_worker = std::thread::Builder::new()
                    .name("app-pty-output".into())
                    .spawn(move || {
                        let Ok(IoEndpoint {
                            stream: mut reader,
                            #[cfg(unix)]
                            fd,
                        }) = output_ready.recv()
                        else {
                            return;
                        };
                        #[cfg(windows)]
                        let mut cursor = super::windows::CursorHandshake::new(cursor_reply);
                        let mut chunk = [0u8; 8192];
                        // Windows must drain through ClosePseudoConsole; stopping
                        // at cancellation could deadlock its final screen flush.
                        while cfg!(windows) || !io_cancel.load(std::sync::atomic::Ordering::Acquire)
                        {
                            match reader.read(&mut chunk) {
                                Ok(0) => break,
                                Ok(n) => {
                                    #[cfg(windows)]
                                    let filtered = if cursor.pending() {
                                        match cursor.filter(&chunk[..n]) {
                                            Ok(bytes) => Some(bytes),
                                            Err(_) => {
                                                buffer
                                                    .lock()
                                                    .unwrap_or_else(|e| e.into_inner())
                                                    .failed = true;
                                                signal.notify_one();
                                                Some(Vec::new())
                                            }
                                        }
                                    } else {
                                        None
                                    };
                                    #[cfg(windows)]
                                    let bytes = filtered.as_deref().unwrap_or(&chunk[..n]);
                                    #[cfg(not(windows))]
                                    let bytes = &chunk[..n];
                                    buffer.lock().unwrap_or_else(|e| e.into_inner()).push(bytes);
                                    signal.notify_one();
                                }
                                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                                    #[cfg(unix)]
                                    if wait_io(fd, libc::POLLIN, &io_cancel) {
                                        continue;
                                    }
                                    if !io_cancel.load(std::sync::atomic::Ordering::Acquire) {
                                        buffer.lock().unwrap_or_else(|e| e.into_inner()).failed =
                                            true;
                                    }
                                    break;
                                }
                                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                                Err(error) => {
                                    if is_pty_eof(&error) {
                                        break;
                                    }
                                    buffer.lock().unwrap_or_else(|e| e.into_inner()).failed = true;
                                    break;
                                }
                            }
                        }
                        let mut out = buffer.lock().unwrap_or_else(|e| e.into_inner());
                        #[cfg(windows)]
                        out.push(&cursor.finish());
                        out.eof = true;
                        drop(out);
                        signal.notify_one();
                    })
                    .context("host.operation_failed")?;
                #[cfg(windows)]
                {
                    pty.output_worker = Some(output_worker);
                }
                #[cfg(not(windows))]
                drop(output_worker);
                #[cfg(windows)]
                super::windows::check_supported()?;
                let pair = portable_pty::native_pty_system().openpty(p.dimensions.size()?)?;
                #[cfg(unix)]
                let fd = {
                    let fd = pair
                        .master
                        .as_raw_fd()
                        .context("host.pty_unsupported_platform")?;
                    super::shell::configure_utf8_input(fd)?;
                    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
                    if flags < 0
                        || unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0
                    {
                        bail!("host.operation_failed");
                    }
                    fd
                };
                let writer = pair.master.take_writer()?;
                let reader = pair.master.try_clone_reader()?;
                *master.lock().unwrap_or_else(|e| e.into_inner()) = Some(pair.master);
                startup.slave = Some(pair.slave);
                startup
                    .input_start
                    .take()
                    .expect("input launch sender")
                    .send(IoEndpoint {
                        stream: writer,
                        #[cfg(unix)]
                        fd,
                    })
                    .map_err(|_| anyhow::anyhow!("host.operation_failed"))?;
                startup
                    .output_start
                    .take()
                    .expect("output launch sender")
                    .send(IoEndpoint {
                        stream: reader,
                        #[cfg(unix)]
                        fd,
                    })
                    .map_err(|_| anyhow::anyhow!("host.operation_failed"))?;
                // Serve ConPTY's cursor handshake before CreateProcess can
                // depend on it. On spawn error the drainer remains alive.
                let slave = startup.slave.take().expect("startup slave");
                let child = slave.spawn_command(command);
                drop(slave);
                let child = child?;
                process.lock().unwrap_or_else(|e| e.into_inner()).child = Some(child);
                #[cfg(windows)]
                let exit_handle = {
                    use std::os::windows::io::BorrowedHandle;
                    let process = process.lock().unwrap_or_else(|e| e.into_inner());
                    let handle = process
                        .child
                        .as_ref()
                        .and_then(|child| child.as_raw_handle())
                        .context("host.operation_failed")?;
                    // SAFETY: child owns this handle and the Process lock keeps
                    // it alive until the duplicate has been created.
                    unsafe { BorrowedHandle::borrow_raw(handle) }
                        .try_clone_to_owned()
                        .context("host.operation_failed")?
                };
                let exit = output.clone();
                let signal = self.output_signal.clone();
                let exit_worker = std::thread::Builder::new()
                    .name("app-pty-exit".into())
                    .spawn(move || {
                        #[cfg(windows)]
                        let exit_ready = {
                            use std::os::windows::io::AsRawHandle;
                            // SAFETY: this duplicate owns a SYNCHRONIZE-capable
                            // process handle. Wait without holding Process's lock
                            // so explicit close/revocation can terminate it.
                            (unsafe {
                                windows_sys::Win32::System::Threading::WaitForSingleObject(
                                    exit_handle.as_raw_handle(),
                                    u32::MAX,
                                )
                            }) == windows_sys::Win32::Foundation::WAIT_OBJECT_0
                        };
                        loop {
                            let mut process = process.lock().unwrap_or_else(|e| e.into_inner());
                            let Some(child) = &mut process.child else {
                                break;
                            };
                            #[cfg(windows)]
                            let result = if exit_ready {
                                child.wait().map(Some)
                            } else {
                                Err(std::io::Error::last_os_error())
                            };
                            #[cfg(not(windows))]
                            let result = child.try_wait();
                            match result {
                                Ok(None) => {}
                                result => {
                                    let mut out = exit.lock().unwrap_or_else(|e| e.into_inner());
                                    match result {
                                        Ok(Some(status)) => out.exited = Some(status.exit_code()),
                                        _ => {
                                            out.failed = true;
                                            let _ = child.kill();
                                            let _ = child.wait();
                                        }
                                    }
                                    // The owned Child records its reaped status. Its Drop cannot signal a reused PID.
                                    process.child.take();
                                    drop(out);
                                    drop(process);
                                    // ConPTY does not EOF merely because the shell
                                    // exited while its HPCON is still owned. Close
                                    // it with the output worker still draining.
                                    #[cfg(windows)]
                                    close_windows_console(&master);
                                    signal.notify_one();
                                    break;
                                }
                            }
                            drop(process);
                            #[cfg(not(windows))]
                            std::thread::sleep(Duration::from_millis(50));
                        }
                    })
                    .context("host.operation_failed")?;
                #[cfg(windows)]
                {
                    pty.exit_worker = Some(exit_worker);
                }
                #[cfg(not(windows))]
                drop(exit_worker);
                let id = pioneer_protocol::generate_id(21);
                self.sessions
                    .insert(id.clone(), startup.pty.take().expect("completed startup"));
                self.creates.insert(p.key, (id.clone(), params));
                return Ok(json!({"session_id":id,"input_sequence":0,"retention_seconds":300}));
            }
        }
        let id = params
            .get("session_id")
            .and_then(Value::as_str)
            .context("host.invalid_params")?
            .to_owned();
        if method == "host.pty.close" {
            let _: Session = serde_json::from_value(params).context("host.invalid_params")?;
            // Idempotent within this execution. IDs never address another owner.
            self.sessions.remove(&id);
            return Ok(json!({"closed":true}));
        }
        let pty = self
            .sessions
            .get_mut(&id)
            .context("host.session_unavailable")?;
        pty.touched = Instant::now();
        match method {
            "host.pty.input" => {
                let input: Input = serde_json::from_value(params).context("host.invalid_params")?;
                if input.bytes.len() > 16384 || input.bytes.is_empty() {
                    bail!("host.input_limit");
                }
                // A bounded input window can have several acknowledgments in
                // flight. Retain their identities so reconnect retries cannot
                // duplicate a write that was accepted before a later sequence.
                if input.sequence <= pty.input_sequence {
                    if pty.input_history.iter().any(|(sequence, bytes)| {
                        *sequence == input.sequence && *bytes == input.bytes
                    }) {
                        return Ok(json!({"accepted_sequence":input.sequence}));
                    }
                    bail!("host.input_sequence_conflict");
                }
                if input.sequence
                    != pty
                        .input_sequence
                        .checked_add(1)
                        .context("host.sequence_exhausted")?
                {
                    bail!("host.input_sequence_conflict");
                }
                pty.writer
                    .as_ref()
                    .context("host.input_outcome_unknown")?
                    .try_send(PtyInput::Data(input.bytes.clone()))
                    .map_err(|error| match error {
                        std::sync::mpsc::TrySendError::Full(_) => {
                            anyhow::anyhow!("host.input_backpressure")
                        }
                        std::sync::mpsc::TrySendError::Disconnected(_) => {
                            anyhow::anyhow!("host.input_outcome_unknown")
                        }
                    })?;
                pty.input_sequence = input.sequence;
                pty.input_history_bytes += input.bytes.len();
                pty.input_history.push_back((input.sequence, input.bytes));
                while pty.input_history.len() > INPUT_HISTORY_FRAMES
                    || pty.input_history_bytes > INPUT_HISTORY_BYTES
                {
                    if let Some((_, bytes)) = pty.input_history.pop_front() {
                        pty.input_history_bytes -= bytes.len();
                    }
                }
                Ok(json!({"accepted_sequence":pty.input_sequence}))
            }
            "host.pty.resize" => {
                let p: Resize = serde_json::from_value(params).context("host.invalid_params")?;
                pty.master
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .as_ref()
                    .context("host.session_unavailable")?
                    .resize(p.dimensions.size()?)?;
                Ok(json!({"resized":true}))
            }
            "host.pty.read" => {
                let p: ReadOutput =
                    serde_json::from_value(params).context("host.invalid_params")?;
                self.read_output(&p.session_id, p.after, 16384)
            }
            "host.pty.keepalive" => {
                let _: Session = serde_json::from_value(params).context("host.invalid_params")?;
                Ok(json!({"alive":true}))
            }
            _ => bail!("host.method_not_allowed"),
        }
    }

    pub(super) fn read_output(&mut self, id: &str, after: u64, limit: usize) -> Result<Value> {
        let pty = self
            .sessions
            .get_mut(id)
            .context("host.session_unavailable")?;
        if pty.touched.elapsed() >= Duration::from_secs(300) {
            bail!("host.session_expired");
        }
        pty.touched = Instant::now();
        let out = pty.output.lock().unwrap_or_else(|e| e.into_inner());
        if after < out.start {
            bail!("host.output_cursor_expired");
        }
        let offset = usize::try_from(after - out.start).context("host.invalid_cursor")?;
        if offset > out.bytes.len() {
            bail!("host.invalid_cursor");
        }
        let bytes: Vec<u8> = out
            .bytes
            .iter()
            .skip(offset)
            .take(limit.min(16384))
            .copied()
            .collect();
        // The reaper may finish before the reader drains the final PTY bytes.
        let exit = if out.eof { out.exited } else { None };
        Ok(json!({"bytes":bytes,"next":after+bytes.len() as u64,
            "end":out.start+out.bytes.len() as u64,"exit_status":exit,
            "failed":out.failed,"input_sequence":pty.input_sequence}))
    }
}

fn is_pty_eof(error: &std::io::Error) -> bool {
    #[cfg(unix)]
    if error.raw_os_error() == Some(libc::EIO) {
        return true;
    }
    #[cfg(windows)]
    if matches!(error.raw_os_error(), Some(109 | 232 | 233)) {
        // ERROR_BROKEN_PIPE / ERROR_NO_DATA / ERROR_PIPE_NOT_CONNECTED.
        return true;
    }
    false
}

#[cfg(windows)]
fn close_windows_console(master: &SharedMaster) {
    // Take under the lock, close outside it: ClosePseudoConsole may wait for
    // output to drain on Windows 10. The reader never needs this mutex.
    let console = master.lock().unwrap_or_else(|e| e.into_inner()).take();
    drop(console);
}

#[cfg(windows)]
fn cancel_windows_io(worker: &std::thread::JoinHandle<()>) {
    use std::os::windows::io::AsRawHandle;
    // SAFETY: the owned JoinHandle keeps this native thread handle valid.
    // ERROR_NOT_FOUND is harmless if the worker is idle or has just finished.
    unsafe {
        windows_sys::Win32::System::IO::CancelSynchronousIo(worker.as_raw_handle());
    }
}

#[cfg(unix)]
fn wait_io(fd: libc::c_int, events: libc::c_short, cancel: &std::sync::atomic::AtomicBool) -> bool {
    while !cancel.load(std::sync::atomic::Ordering::Acquire) {
        let mut descriptor = libc::pollfd {
            fd,
            events,
            revents: 0,
        };
        let result = unsafe { libc::poll(&mut descriptor, 1, 100) };
        if result > 0 {
            return descriptor.revents & libc::POLLNVAL == 0;
        }
        if result < 0 && std::io::Error::last_os_error().kind() != std::io::ErrorKind::Interrupted {
            return false;
        }
    }
    false
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    #[test]
    fn reconnect_preserves_input_acknowledgements_and_sessions_never_cross_owners() {
        let mut owner = PtyOwner::default();
        let mut foreign = PtyOwner::default();
        let create = json!({"key":"shell-1","command":["/bin/sh","-c",r#"IFS= read -r first; IFS= read -r second; printf 'RESULT:%s:%s\n' "$first" "$second"; exit 7"#],"dimensions":{"rows":24,"cols":80,"pixel_width":640,"pixel_height":480}});
        let mut injected = create.clone();
        injected["principal_id"] = json!("another-principal");
        assert!(
            owner
                .call("host.pty.create", injected)
                .unwrap_err()
                .to_string()
                .contains("host.invalid_params")
        );
        let result = owner.call("host.pty.create", create.clone()).unwrap();
        let id = result["session_id"].as_str().unwrap().to_owned();
        assert_eq!(
            owner.call("host.pty.create", create).unwrap()["session_id"],
            id
        );
        assert!(
            foreign
                .call("host.pty.read", json!({"session_id":id,"after":0}))
                .is_err()
        );
        let first = json!({"session_id":id,"sequence":1,"bytes":b"first\n".to_vec()});
        assert_eq!(
            owner.call("host.pty.input", first.clone()).unwrap()["accepted_sequence"],
            1
        );
        assert_eq!(
            owner.call("host.pty.input", first.clone()).unwrap()["accepted_sequence"],
            1
        );
        assert!(
            owner
                .call(
                    "host.pty.input",
                    json!({"session_id":id,"sequence":1,"bytes":b"changed\n".to_vec()})
                )
                .is_err()
        );
        owner.call("host.pty.resize",json!({"session_id":id,"dimensions":{"rows":40,"cols":120,"pixel_width":960,"pixel_height":800}})).unwrap();
        owner
            .call(
                "host.pty.input",
                json!({"session_id":id,"sequence":2,"bytes":b"second\n".to_vec()}),
            )
            .unwrap();
        // The first ACK was lost, but a later write was already accepted.
        // Replaying that earlier identity must still not enter the PTY twice.
        assert_eq!(
            owner.call("host.pty.input", first).unwrap()["accepted_sequence"],
            1
        );
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut cursor = 0;
        let mut output = Vec::new();
        loop {
            let result = owner
                .call("host.pty.read", json!({"session_id":id,"after":cursor}))
                .unwrap();
            cursor = result["next"].as_u64().unwrap();
            output.extend(
                result["bytes"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| v.as_u64().unwrap() as u8),
            );
            if result["exit_status"] == 7
                && String::from_utf8_lossy(&output).contains("RESULT:first:second")
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "PTY did not deliver the two distinct acknowledged inputs and exit status"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        owner
            .call("host.pty.close", json!({"session_id":id}))
            .unwrap();
        assert!(
            owner
                .call("host.pty.read", json!({"session_id":id,"after":cursor}))
                .is_err()
        );
        assert!(owner.call("host.pty.create",json!({"key":"shell-1","command":["/bin/sh"],"dimensions":{"rows":24,"cols":80}})).is_err());
    }

    #[test]
    fn utf8_pty_preserves_cyrillic_and_erases_a_whole_multibyte_character() {
        let mut owner = PtyOwner::default();
        let created = owner.call("host.pty.create", json!({
            "key":"utf8", "command":["/bin/sh","-c",
                r#"printf 'ENCODING:'; locale charmap; IFS= read -r line; printf 'RESULT:%s\n' "$line""#],
            "dimensions":{"rows":24,"cols":80}
        })).unwrap();
        let id = created["session_id"].as_str().unwrap();
        let mut input = "Приветя".as_bytes().to_vec();
        input.extend_from_slice(&[127, b'\n']);
        owner
            .call(
                "host.pty.input",
                json!({"session_id":id,"sequence":1,"bytes":input}),
            )
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut cursor = 0;
        let mut bytes = Vec::new();
        loop {
            let frame = owner
                .call("host.pty.read", json!({"session_id":id,"after":cursor}))
                .unwrap();
            cursor = frame["next"].as_u64().unwrap();
            bytes.extend(
                frame["bytes"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| v.as_u64().unwrap() as u8),
            );
            if !frame["exit_status"].is_null() {
                break;
            }
            assert!(Instant::now() < deadline, "UTF-8 PTY did not finish");
            std::thread::sleep(Duration::from_millis(10));
        }
        let output = String::from_utf8(bytes).expect("PTY output must remain valid UTF-8");
        assert!(
            output.contains("ENCODING:UTF-8"),
            "non-UTF-8 child locale: {output:?}"
        );
        assert!(
            output.contains("RESULT:Привет\r\n"),
            "multibyte erase corrupted the input: {output:?}"
        );
        owner
            .call("host.pty.close", json!({"session_id":id}))
            .unwrap();
    }
}

#[cfg(all(test, windows))]
#[path = "windows_pty_tests.rs"]
mod windows_pty_tests;
