use crate::runtime::{MaterializedStdioTransport, StderrTail};
use futures_util::{FutureExt, future::{BoxFuture, Shared}};
#[cfg(windows)]
use process_wrap::tokio::JobObject;
#[cfg(unix)]
use process_wrap::tokio::ProcessGroup;
use process_wrap::tokio::{ChildWrapper, CommandWrap, KillOnDrop};
use rmcp::RoleClient;
use rmcp::transport::async_rw::AsyncRwTransport;
use std::pin::Pin;
use std::process::Stdio;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};
use tokio::process::Command;
use tokio::process::{ChildStdin, ChildStdout};
use tokio::sync::Mutex;

pub(crate) const MCP_MAX_STDIO_FRAME_BYTES: usize = 4 * 1024 * 1024;

pub(crate) struct BoundedLineReader<R> {
    inner: R,
    current_line_bytes: usize,
    max_line_bytes: usize,
}

impl<R> BoundedLineReader<R> {
    fn new(inner: R, max_line_bytes: usize) -> Self {
        Self {
            inner,
            current_line_bytes: 0,
            max_line_bytes,
        }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for BoundedLineReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        destination: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let mut scratch = [0u8; 8 * 1024];
        let read_capacity = destination.remaining().min(scratch.len());
        if read_capacity == 0 {
            return Poll::Ready(Ok(()));
        }
        let mut scratch_buffer = ReadBuf::new(&mut scratch[..read_capacity]);
        match Pin::new(&mut self.inner).poll_read(cx, &mut scratch_buffer) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Ready(Ok(())) => {
                let bytes = scratch_buffer.filled();
                let mut line_bytes = self.current_line_bytes;
                for byte in bytes {
                    if *byte == b'\n' {
                        line_bytes = 0;
                    } else {
                        line_bytes = line_bytes.saturating_add(1);
                        if line_bytes > self.max_line_bytes {
                            return Poll::Ready(Err(std::io::Error::new(
                                std::io::ErrorKind::FileTooLarge,
                                "MCP stdio frame exceeds the transport byte budget",
                            )));
                        }
                    }
                }
                self.current_line_bytes = line_bytes;
                destination.put_slice(bytes);
                Poll::Ready(Ok(()))
            }
        }
    }
}

pub(crate) type BoundedStdioTransport =
    AsyncRwTransport<RoleClient, BoundedLineReader<ManagedChildStdout>, ChildStdin>;

pub(crate) struct ManagedChildStdout {
    child: Arc<StdioChildOwner>,
    stdout: ChildStdout,
}

impl AsyncRead for ManagedChildStdout {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        destination: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stdout).poll_read(cx, destination)
    }
}

// The session retains the real process-group wrapper after transport close.
// Drop can signal termination, but only this native helper acknowledges wait.
pub(crate) struct StdioChildOwner {
    state: Arc<Mutex<StdioChildState>>,
    // Keep the public kill/wait future across cancelled observers.
    stop: Mutex<Option<Shared<BoxFuture<'static, std::result::Result<(), Arc<std::io::Error>>>>>>,
}
struct StdioChildState {
    child: Box<dyn ChildWrapper>,
    stderr_reader: Option<tokio::task::JoinHandle<()>>,
    process_stopped: bool,
    process_error: Option<String>,
    observation: pioneer_observability::resources::ChildRegistration,
    reader_error: Option<String>,
}
impl StdioChildOwner {
    fn new(mut child: Box<dyn ChildWrapper>, tail: &StderrTail) -> Arc<Self> {
        let observation = pioneer_observability::resources::ChildRegistration::register(
            child.id(), pioneer_observability::resources::ChildRole::Mcp, None);
        let stderr_reader = child.inner_mut().stderr().take().map(|stderr| tail.spawn_reader(stderr));
        Arc::new(Self {
            state: Arc::new(Mutex::new(StdioChildState {
                child, stderr_reader, process_stopped: false,
                process_error: None, observation, reader_error: None,
            })),
            stop: Mutex::new(None),
        })
    }

    pub(crate) async fn stop_and_wait(&self) -> std::io::Result<()> {
        let completion = {
            let mut slot = self.stop.lock().await;
            slot.get_or_insert_with(|| {
                let state = self.state.clone();
                async move { Self::drain(state).await.map_err(Arc::new) }.boxed().shared()
            }).clone()
        };
        let result = completion.clone().await;
        if result.is_err() {
            // Retry a failed public stop call while retaining reader errors.
            // Cancellation never reaches this branch.
            let mut slot = self.stop.lock().await;
            if slot.as_ref().is_some_and(|current| current.ptr_eq(&completion)) {
                *slot = None;
            }
        }
        result.map_err(|error| std::io::Error::new(error.kind(), error.to_string()))
    }

    async fn drain(state: Arc<Mutex<StdioChildState>>) -> std::io::Result<()> {
        let mut state = state.lock().await;
        if !state.process_stopped {
            if let Err(error) = state.child.start_kill() {
                #[cfg(unix)]
                let already_gone = error.raw_os_error() == Some(libc::ESRCH);
                #[cfg(not(unix))]
                let already_gone = false;
                if !already_gone { return Err(error); }
            }
            let status = match state.child.wait().await {
                Ok(status) => status,
                Err(error) => {
                    state.process_error = Some("MCP process wait failed".into());
                    return Err(error);
                }
            };
            state.observation.exited(status.success());
            state.process_stopped = true;
        }
        if let Some(reader) = state.stderr_reader.as_mut() {
            reader.abort();
            let result = (&mut *reader).await;
            state.stderr_reader = None;
            if let Err(error) = result
                && !error.is_cancelled()
            {
                state.reader_error = Some("MCP stderr reader join failed".into());
            }
        }
        if let Some(error) = state.process_error.as_ref().or(state.reader_error.as_ref()) {
            return Err(std::io::Error::other(error.clone()));
        }
        Ok(())
    }
}
impl Drop for ManagedChildStdout {
    fn drop(&mut self) {
        if let Ok(mut state) = self.child.state.try_lock()
            && !state.process_stopped
        {
            let _ = state.child.start_kill();
        }
    }
}

pub(crate) fn build_stdio_transport(
    transport: &MaterializedStdioTransport,
) -> std::io::Result<(BoundedStdioTransport, StderrTail, Arc<StdioChildOwner>)> {
    let stderr_tail = StderrTail::new(16 * 1024, transport.secrets.clone());
    let mut command = Command::new(transport.command.as_str());
    command.args(transport.args.iter().map(String::as_str));
    if let Some(cwd) = transport.cwd.as_deref() {
        command.current_dir(cwd);
    }
    command.envs(
        transport
            .env
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str())),
    );
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut command = CommandWrap::from(command);
    command.wrap(KillOnDrop);
    #[cfg(unix)]
    command.wrap(ProcessGroup::leader());
    #[cfg(windows)]
    command.wrap(JobObject);

    let child = command.spawn()?;
    // Keep the wrapper and stderr reader together while taking the configured pipes.
    let owner = StdioChildOwner::new(child, &stderr_tail);
    let pipes = {
        let mut state = owner.state.try_lock().expect("new stdio owner is unobserved");
        let stdin = state.child.inner_mut().stdin().take();
        let stdout = state.child.inner_mut().stdout().take();
        (stdin, stdout)
    };
    let (Some(stdin), Some(stdout)) = pipes else {
        return Err(std::io::Error::other("MCP stdio child stdin/stdout was not piped"));
    };
    let reader = BoundedLineReader::new(
        ManagedChildStdout {
            child: owner.clone(),
            stdout,
        },
        MCP_MAX_STDIO_FRAME_BYTES,
    );
    Ok((
        AsyncRwTransport::new_client(reader, stdin),
        stderr_tail,
        owner,
    ))
}
