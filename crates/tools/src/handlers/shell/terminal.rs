//! Noninteractive process handles for protocols with snapshot/kill/release
//! semantics. Reuses shell process policy, sandbox and process-tree cleanup;
//! it cannot use write_stdin's consuming cursors or automatic removal on exit.

#[cfg(not(unix))]
use super::terminate_child_process;
use super::{
    apply_process_spawn_plan_environment, apply_shell_sandbox_backend, configure_process_group,
    decode_shell_bytes, kill_process_tree,
};
use crate::{ExecCommandArgs, ToolError, build_process_spawn_plan};
use pioneer_protocol::TurnExecutionSecuritySnapshot;
use std::collections::BTreeMap;
use std::path::Path;
use std::process::{ExitStatus, Stdio};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::Command;
use tokio::sync::{Mutex as AsyncMutex, watch};
use tokio::task::JoinHandle;
use tokio::time::{Duration, timeout};
use tokio_util::sync::CancellationToken;

// Leave room for worst-case JSON escaping in a bounded external RPC frame.
const MAX_OUTPUT_BYTES: usize = 128 * 1024;
const PIPE_DRAIN_GRACE: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProcessTerminalExit {
    pub exit_code: Option<u32>,
    pub signal: Option<String>,
}

impl ProcessTerminalExit {
    fn from_status(status: ExitStatus) -> Self {
        #[cfg(unix)]
        let signal = {
            use std::os::unix::process::ExitStatusExt;
            status.signal().map(|signal| signal.to_string())
        };
        #[cfg(not(unix))]
        let signal = None;
        Self {
            exit_code: status.code().and_then(|code| code.try_into().ok()),
            signal,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProcessTerminalOutput {
    pub output: String,
    pub truncated: bool,
    pub exit: Option<ProcessTerminalExit>,
}

struct OutputBuffer {
    output: String,
    limit: usize,
    truncated: bool,
    read_error: Option<String>,
    changed: watch::Sender<u64>,
}

impl OutputBuffer {
    fn append(&mut self, text: &str) {
        self.changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));
        self.output.push_str(text);
        let mut overflow = self.output.len().saturating_sub(self.limit);
        if overflow > 0 {
            while !self.output.is_char_boundary(overflow) {
                overflow += 1;
            }
            self.output.drain(..overflow);
            self.truncated = true;
        }
    }
}

/// The session owner holds this handle until release/session close. Killing
/// waits for process-tree termination but deliberately retains its output.
pub struct ProcessTerminal {
    output: Arc<Mutex<OutputBuffer>>,
    exit: watch::Receiver<Option<Result<ProcessTerminalExit, String>>>,
    stop: CancellationToken,
    worker: AsyncMutex<Option<JoinHandle<()>>>,
}

impl ProcessTerminal {
    pub fn spawn(
        command: String,
        arguments: Vec<String>,
        cwd: &Path,
        environment: BTreeMap<String, String>,
        output_byte_limit: Option<u64>,
        security: &TurnExecutionSecuritySnapshot,
    ) -> Result<Self, ToolError> {
        if command.is_empty() || command.contains('\0') {
            return Err(ToolError::invalid_arguments(
                "terminal command is empty or invalid",
            ));
        }
        let limit = match output_byte_limit {
            Some(limit) => usize::try_from(limit)
                .ok()
                .filter(|limit| *limit > 0 && *limit <= MAX_OUTPUT_BYTES)
                .ok_or_else(|| {
                    ToolError::invalid_arguments(
                        "terminal output limit is outside the client budget",
                    )
                })?,
            None => MAX_OUTPUT_BYTES,
        };
        let argv = std::iter::once(command.clone())
            .chain(arguments.iter().cloned())
            .collect();
        let args = ExecCommandArgs {
            command: Some(argv),
            workdir: Some(cwd.to_string_lossy().into_owned()),
            timeout_ms: None,
            max_output_tokens: None,
            yield_time_ms: None,
            tty: Some(false),
        };
        let plan = build_process_spawn_plan(Some(security), cwd, &args, &environment, 60_000)?;
        // Preserve the exact requested argv: native shell's pipefail injection
        // is an API-tool convenience, not part of external terminal contracts.
        let mut process = Command::new(command);
        process.args(arguments).current_dir(&plan.cwd);
        configure_process_group(&mut process)?;
        apply_process_spawn_plan_environment(&mut process, &plan);
        apply_shell_sandbox_backend(&mut process, Some(security), &plan)?;
        process
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        process.kill_on_drop(true);
        let mut child = process.spawn().map_err(|error| {
            ToolError::execution_failed(format!("failed to start terminal: {error}"))
        })?;
        let output = Arc::new(Mutex::new(OutputBuffer {
            output: String::new(),
            limit,
            truncated: false,
            read_error: None,
            changed: watch::channel(0).0,
        }));
        let mut readers = tokio::task::JoinSet::new();
        if let Some(stdout) = child.stdout.take() {
            readers.spawn(read_output(stdout, output.clone()));
        }
        if let Some(stderr) = child.stderr.take() {
            readers.spawn(read_output(stderr, output.clone()));
        }
        let stop = CancellationToken::new();
        let stopped = stop.clone();
        let (exit_tx, exit) = watch::channel(None);
        let output_for_worker = output.clone();
        let worker = tokio::spawn(async move {
            // Retain isolated temporary directory ownership through reaping.
            let _plan = plan;
            #[cfg(unix)]
            let (status, timed_out) =
                wait_owned_terminal(&mut child, &stopped, _plan.timeout_ms).await;
            #[cfg(unix)]
            if timed_out {
                output_for_worker
                    .lock()
                    .expect("terminal output mutex poisoned")
                    .read_error =
                    Some("terminal exceeded the resolved process time budget".to_owned());
            }
            #[cfg(not(unix))]
            let status = tokio::select! {
                biased;
                _ = stopped.cancelled() => terminate_child_process(&mut child).await.map_err(|error| error.to_string()),
                result = child.wait() => result.map_err(|error| format!("terminal wait failed: {error}")),
                _ = tokio::time::sleep(Duration::from_millis(_plan.timeout_ms)) => {
                    let result = terminate_child_process(&mut child).await.map_err(|error| error.to_string());
                    output_for_worker.lock().expect("terminal output mutex poisoned").read_error = Some("terminal exceeded the resolved process time budget".to_owned());
                    result
                }
            };
            let drained = timeout(PIPE_DRAIN_GRACE, async {
                while let Some(result) = readers.join_next().await {
                    if let Err(error) = result {
                        output_for_worker
                            .lock()
                            .expect("terminal output mutex poisoned")
                            .read_error = Some(format!("terminal reader failed: {error}"));
                    }
                }
            })
            .await;
            if drained.is_err() {
                readers.abort_all();
                while readers.join_next().await.is_some() {}
                let mut output = output_for_worker
                    .lock()
                    .expect("terminal output mutex poisoned");
                output.truncated = true;
                output.read_error =
                    Some("terminal pipes did not close within the cleanup budget".to_owned());
            }
            exit_tx.send_replace(Some(status.map(ProcessTerminalExit::from_status)));
            output_for_worker
                .lock()
                .expect("terminal output mutex poisoned")
                .changed
                .send_modify(|revision| *revision = revision.wrapping_add(1));
        });
        Ok(Self {
            output,
            exit,
            stop,
            worker: AsyncMutex::new(Some(worker)),
        })
    }

    /// Coalescing display invalidation, not a consuming output cursor.
    pub fn output_changes(&self) -> watch::Receiver<u64> {
        self.output
            .lock()
            .expect("terminal output mutex poisoned")
            .changed
            .subscribe()
    }

    pub fn output(&self) -> Result<ProcessTerminalOutput, ToolError> {
        let output = self.output.lock().expect("terminal output mutex poisoned");
        if let Some(error) = &output.read_error {
            return Err(ToolError::execution_failed(error.clone()));
        }
        let exit = self
            .exit
            .borrow()
            .clone()
            .transpose()
            .map_err(ToolError::execution_failed)?;
        Ok(ProcessTerminalOutput {
            output: output.output.clone(),
            truncated: output.truncated,
            exit,
        })
    }

    pub async fn wait(
        &self,
        cancelled: &CancellationToken,
    ) -> Result<ProcessTerminalExit, ToolError> {
        let mut exit = self.exit.clone();
        loop {
            if let Some(result) = exit.borrow_and_update().clone() {
                return result.map_err(ToolError::execution_failed);
            }
            tokio::select! {
                biased;
                _ = cancelled.cancelled() => return Err(ToolError::cancelled("terminal wait cancelled")),
                changed = exit.changed() => if changed.is_err() {
                    return Err(ToolError::execution_failed("terminal owner ended without an exit status"));
                },
            }
        }
    }

    pub async fn kill(
        &self,
        cancelled: &CancellationToken,
    ) -> Result<ProcessTerminalExit, ToolError> {
        self.stop.cancel();
        self.wait(cancelled).await
    }

    pub async fn release(&self) -> Result<(), ToolError> {
        self.stop.cancel();
        if let Some(worker) = self.worker.lock().await.take() {
            worker.await.map_err(|error| {
                ToolError::execution_failed(format!("terminal cleanup failed: {error}"))
            })?;
        }
        Ok(())
    }
}

impl Drop for ProcessTerminal {
    fn drop(&mut self) {
        // The owned worker completes termination/reaping, including process
        // descendants. Aborting it here would lose that ownership fence.
        self.stop.cancel();
    }
}

#[cfg(unix)]
async fn wait_owned_terminal(
    child: &mut tokio::process::Child,
    stopped: &CancellationToken,
    timeout_ms: u64,
) -> (Result<ExitStatus, String>, bool) {
    let Some(pid) = child.id() else {
        return (Err("terminal process identity unavailable".into()), false);
    };
    let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
    let mut stopping = None;
    let mut timed_out = false;
    loop {
        // Observe without reaping. The zombie reserves the group ID until all
        // descendants have been stopped; never signal a PID after child.wait.
        let exited = unsafe {
            let mut info: libc::siginfo_t = std::mem::zeroed();
            let result = libc::waitid(
                libc::P_PID,
                pid as libc::id_t,
                &mut info,
                libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
            );
            if result == 0 {
                info.si_pid() != 0
            } else {
                let error = std::io::Error::last_os_error();
                if error.kind() == std::io::ErrorKind::Interrupted {
                    continue;
                }
                // ECHILD cannot authorize signaling an ID no longer owned.
                return (
                    Err(format!("terminal exit observation failed: {error}")),
                    timed_out,
                );
            }
        };
        let now = tokio::time::Instant::now();
        if exited || stopping.is_some_and(|deadline| now >= deadline) {
            kill_process_tree(Some(pid), true);
            return (
                child
                    .wait()
                    .await
                    .map_err(|error| format!("terminal wait failed: {error}")),
                timed_out,
            );
        }
        if stopping.is_none() && (stopped.is_cancelled() || now >= deadline) {
            timed_out = now >= deadline;
            kill_process_tree(Some(pid), false);
            stopping = Some(now + Duration::from_millis(super::PROCESS_KILL_GRACE_MS));
        }
        // Polling preserves the unreaped child; a child.wait future would reap
        // before this owner can fence descendants on natural completion.
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn read_output<R: AsyncRead + Unpin>(mut reader: R, output: Arc<Mutex<OutputBuffer>>) {
    let mut chunk = [0u8; 4096];
    let mut pending = Vec::new();
    loop {
        let read = match reader.read(&mut chunk).await {
            Ok(read) => read,
            Err(error) => {
                output
                    .lock()
                    .expect("terminal output mutex poisoned")
                    .read_error = Some(format!("terminal output read failed: {error}"));
                return;
            }
        };
        let text = decode_shell_bytes(&mut pending, &chunk[..read], read == 0);
        output
            .lock()
            .expect("terminal output mutex poisoned")
            .append(&text);
        if read == 0 {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::OutputBuffer;
    use tokio::sync::watch;

    #[test]
    fn snapshot_retains_utf8_suffix_and_truncation_without_consuming_output() {
        let mut output = OutputBuffer {
            output: String::new(),
            limit: 5,
            truncated: false,
            read_error: None,
            changed: watch::channel(0).0,
        };
        output.append("aЖ🙂");
        assert_eq!(output.output, "🙂");
        assert!(output.truncated);
        output.append("x");
        assert_eq!(output.output, "🙂x");
        output.append("y");
        assert_eq!(output.output, "xy");
    }
}
