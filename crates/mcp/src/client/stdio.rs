use crate::runtime::{MaterializedStdioTransport, StderrTail};
use anyhow::Result;
#[cfg(windows)]
use process_wrap::tokio::JobObject;
#[cfg(unix)]
use process_wrap::tokio::ProcessGroup;
use process_wrap::tokio::{ChildWrapper, CommandWrap, KillOnDrop};
use rmcp::RoleClient;
use rmcp::transport::async_rw::AsyncRwTransport;
use std::pin::Pin;
use std::process::Stdio;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};
use tokio::process::Command;
use tokio::process::{ChildStdin, ChildStdout};

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
    child: Option<Box<dyn ChildWrapper>>,
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

impl Drop for ManagedChildStdout {
    fn drop(&mut self) {
        if let Some(child) = self.child.take() {
            tokio::spawn(async move {
                let mut child = child;
                if let Err(error) = Box::into_pin(child.kill()).await {
                    tracing::warn!(%error, "failed to terminate MCP stdio child process");
                }
            });
        }
    }
}

pub(crate) fn build_stdio_transport(
    transport: &MaterializedStdioTransport,
) -> Result<(BoundedStdioTransport, StderrTail)> {
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

    let mut child = command.spawn()?;
    let stdin = child
        .inner_mut()
        .stdin()
        .take()
        .ok_or_else(|| std::io::Error::other("MCP stdio child stdin was not piped"))?;
    let stdout = child
        .inner_mut()
        .stdout()
        .take()
        .ok_or_else(|| std::io::Error::other("MCP stdio child stdout was not piped"))?;
    let stderr = child.inner_mut().stderr().take();
    if let Some(stderr) = stderr {
        stderr_tail.spawn_reader(stderr);
    }

    let reader = BoundedLineReader::new(
        ManagedChildStdout {
            child: Some(child),
            stdout,
        },
        MCP_MAX_STDIO_FRAME_BYTES,
    );
    Ok((AsyncRwTransport::new_client(reader, stdin), stderr_tail))
}
