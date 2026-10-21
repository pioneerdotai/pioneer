//! Interactive terminal for an explicitly selected standard terminal-auth
//! descriptor. No shell string, provider preset, or authenticate RPC is used.
use super::Launch;
use anyhow::{Context, Result, ensure};
use std::{
    io::{Read, Write},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::Mutex as AsyncMutex;
use tokio_util::sync::CancellationToken;

pub struct AuthTerminal {
    master: Mutex<Option<Box<dyn portable_pty::MasterPty + Send>>>,
    writer: Mutex<Option<Box<dyn Write + Send>>>,
    output: Arc<Mutex<Output>>,
    cancellation: CancellationToken,
    worker: AsyncMutex<Option<tokio::task::JoinHandle<()>>>,
}
struct Output {
    bytes: Vec<u8>,
    truncated: bool,
    completed: bool,
    exit_code: Option<u32>,
    error: Option<String>,
}
#[derive(Clone, Debug, serde::Serialize)]
pub struct Snapshot {
    pub output: String,
    pub truncated: bool,
    pub completed: bool,
    pub exit_code: Option<u32>,
    pub error: Option<String>,
}

impl AuthTerminal {
    #[cfg(unix)]
    pub fn spawn(launch: Launch, columns: u16, rows: u16) -> Result<Self> {
        ensure!(
            (20..=500).contains(&columns) && (5..=200).contains(&rows),
            "invalid terminal size"
        );
        let prepared = launch
            .process_config()
            .map_err(|e| anyhow::anyhow!(e.message))?
            .prepare()?;
        let pair = portable_pty::native_pty_system().openpty(portable_pty::PtySize {
            rows,
            cols: columns,
            pixel_width: 0,
            pixel_height: 0,
        })?;
        let mut command = portable_pty::CommandBuilder::new(&prepared.executable);
        command.args(&prepared.args);
        command.cwd(prepared.cwd.context("auth invocation has no cwd")?);
        for name in prepared.env_remove {
            command.env_remove(name);
        }
        for (name, value) in prepared.env.expose_iter() {
            command.env(name, value);
        }
        let reader = pair.master.try_clone_reader()?;
        let writer = pair.master.take_writer()?;
        let fd = pair
            .master
            .as_raw_fd()
            .context("PTY has no nonblocking descriptor")?;
        // SAFETY: the master owns a live fd. Cloned read/write descriptors share
        // the open-file description; both operations then remain nonblocking.
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
        ensure!(
            flags >= 0 && unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } >= 0,
            "cannot configure nonblocking auth PTY"
        );
        // Complete fallible PTY setup before creating the process.
        let child = pair.slave.spawn_command(command)?;
        drop(pair.slave);
        let output = Arc::new(Mutex::new(Output {
            bytes: Vec::new(),
            truncated: false,
            completed: false,
            exit_code: None,
            error: None,
        }));
        let cancellation = CancellationToken::new();
        let worker_output = output.clone();
        let worker_cancel = cancellation.clone();
        let worker = tokio::task::spawn_blocking(move || {
            let _completion = OutputCompletion(worker_output.clone());
            let mut process = OwnedAuthProcess {
                child,
                reaped: false,
            };
            let mut reader = reader;
            let mut buffer = [0u8; 8192];
            let deadline = std::time::Instant::now() + Duration::from_secs(15 * 60);
            loop {
                let mut finished = false;
                for _ in 0..8 {
                    match reader.read(&mut buffer) {
                        Ok(0) => {
                            finished = true;
                            break;
                        }
                        Ok(count) => {
                            let mut output = worker_output.lock().expect("auth output");
                            output.bytes.extend_from_slice(&buffer[..count]);
                            let overflow = output.bytes.len().saturating_sub(64 * 1024);
                            if overflow > 0 {
                                output.bytes.drain(..overflow);
                                output.truncated = true;
                            }
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                        // Unix PTYs commonly report EIO when the slave closes.
                        Err(error) if error.raw_os_error() == Some(libc::EIO) => {
                            finished = true;
                            break;
                        }
                        Err(error) => {
                            worker_output.lock().expect("auth output").error =
                                Some(error.to_string());
                            finished = true;
                            break;
                        }
                    }
                }
                if process.exited_without_reaping() {
                    // Kill remaining group members while the owned zombie
                    // still reserves its PID, then reap the direct child.
                    process.stop_group();
                    if let Ok(exit) = process.child.wait() {
                        process.reaped = true;
                        worker_output.lock().expect("auth output").exit_code =
                            Some(exit.exit_code());
                    }
                    break;
                }
                let reason = if worker_cancel.is_cancelled() {
                    Some("Auth terminal cancelled")
                } else if std::time::Instant::now() >= deadline {
                    Some("Auth terminal deadline exceeded")
                } else if finished {
                    Some("Auth output stream closed before invocation exit was observed")
                } else {
                    None
                };
                if let Some(reason) = reason {
                    worker_output
                        .lock()
                        .expect("auth output")
                        .error
                        .get_or_insert_with(|| reason.to_owned());
                    break;
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            if !process.reaped {
                process.stop_group();
                match process.child.wait() {
                    Ok(exit) => {
                        process.reaped = true;
                        worker_output.lock().expect("auth output").exit_code =
                            Some(exit.exit_code());
                    }
                    Err(error) => {
                        worker_output
                            .lock()
                            .expect("auth output")
                            .error
                            .get_or_insert_with(|| format!("Cannot reap auth invocation: {error}"));
                    }
                }
            }
            {
                let mut output = worker_output.lock().expect("auth output");
                if output.exit_code.is_none() && output.error.is_none() {
                    output.error =
                        Some("Auth invocation ended without an observed exit status".into());
                }
                output.completed = true;
            }
            // Dropping reader is part of PTY teardown, not a detached blocking IO.
            drop(reader);
        });
        Ok(Self {
            master: Mutex::new(Some(pair.master)),
            writer: Mutex::new(Some(writer)),
            output,
            cancellation,
            worker: AsyncMutex::new(Some(worker)),
        })
    }
    #[cfg(not(unix))]
    pub fn spawn(_launch: Launch, _columns: u16, _rows: u16) -> Result<Self> {
        anyhow::bail!("interactive auth terminal unavailable on this platform")
    }

    pub fn snapshot(&self) -> Snapshot {
        let output = self.output.lock().expect("auth output");
        Snapshot {
            output: String::from_utf8_lossy(&output.bytes).into_owned(),
            truncated: output.truncated,
            completed: output.completed,
            exit_code: output.exit_code,
            error: output.error.clone(),
        }
    }
    pub fn input(&self, data: &[u8]) -> Result<usize> {
        ensure!(
            !data.is_empty()
                && data.len() <= 4096
                && !self.cancellation.is_cancelled()
                && !self.output.lock().expect("auth output").completed,
            "auth input unavailable or exceeds budget"
        );
        let mut writer = self.writer.lock().expect("auth writer");
        let writer = writer.as_mut().context("auth terminal closed")?;
        // Report partial acceptance explicitly. Neither side replays bytes.
        match writer.write(data) {
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(0),
            result => Ok(result?),
        }
    }
    pub async fn close(&self) -> Result<()> {
        self.cancellation.cancel();
        self.writer.lock().expect("auth writer").take();
        if let Some(worker) = self.worker.lock().await.take() {
            worker.await?;
        }
        self.master.lock().expect("auth master").take();
        Ok(())
    }
}
impl Drop for AuthTerminal {
    fn drop(&mut self) {
        self.cancellation.cancel();
        self.writer.get_mut().expect("auth writer").take();
        self.master.get_mut().expect("auth master").take();
    }
}

// Declared before the process guard, so even an unwinding worker publishes
// completion only after owned process-tree cleanup has run.
struct OutputCompletion(Arc<Mutex<Output>>);
impl Drop for OutputCompletion {
    fn drop(&mut self) {
        let mut output = self.0.lock().unwrap_or_else(|poison| poison.into_inner());
        if !output.completed {
            output.error.get_or_insert_with(|| {
                "Auth worker ended without a confirmed invocation exit".into()
            });
            output.completed = true;
        }
    }
}

// portable-pty creates a new Unix session/process group. Own that entire group
// until reader completion, cancellation, deadline, or panic; always reap the
// direct child. This guard lives on the owned blocking worker, never the UI.
struct OwnedAuthProcess {
    child: Box<dyn portable_pty::Child + Send + Sync>,
    reaped: bool,
}
impl OwnedAuthProcess {
    #[cfg(unix)]
    fn exited_without_reaping(&self) -> bool {
        let Some(pid) = self.child.process_id() else {
            return false;
        };
        // SAFETY: info is an initialized siginfo_t, pid is our owned child.
        // WNOWAIT observes termination without permitting PID/group reuse.
        unsafe {
            let mut info: libc::siginfo_t = std::mem::zeroed();
            libc::waitid(
                libc::P_PID,
                pid as libc::id_t,
                &mut info,
                libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
            ) == 0
                && info.si_pid() != 0
        }
    }
    fn stop_group(&self) {
        #[cfg(unix)]
        if let Some(pid) = self
            .child
            .process_id()
            .and_then(|pid| i32::try_from(pid).ok())
        {
            // SAFETY: portable-pty creates a session/group owned by this child.
            // The caller has not reaped it, so the ID cannot be reassigned.
            unsafe {
                libc::kill(-pid, libc::SIGKILL);
            }
        }
    }
}
impl Drop for OwnedAuthProcess {
    fn drop(&mut self) {
        if !self.reaped {
            self.stop_group();
            let _ = self.child.wait();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completed_snapshot_requires_an_observed_outcome_and_disables_further_input() {
        for observed_exit in [None, Some(0), Some(1)] {
            let output = Arc::new(Mutex::new(Output {
                bytes: b"auth output".to_vec(),
                truncated: true,
                completed: observed_exit.is_some(),
                exit_code: observed_exit,
                error: None,
            }));
            // The worker completion obligation survives an early return or an
            // unwind, independently of the picker and the PTY stream's EOF.
            drop(OutputCompletion(output.clone()));
            let terminal = AuthTerminal {
                master: Mutex::new(None),
                writer: Mutex::new(Some(Box::new(std::io::sink()))),
                output,
                cancellation: CancellationToken::new(),
                worker: AsyncMutex::new(None),
            };
            let snapshot = terminal.snapshot();
            assert!(snapshot.completed && snapshot.truncated);
            assert_eq!(snapshot.output, "auth output");
            assert_eq!(snapshot.exit_code, observed_exit);
            assert_eq!(snapshot.error.is_some(), observed_exit.is_none());
            assert!(terminal.input(b"must not be replayed").is_err());
        }
    }
}
