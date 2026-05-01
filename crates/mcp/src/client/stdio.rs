use crate::runtime::{MaterializedStdioTransport, StderrTail};
use anyhow::Result;
#[cfg(windows)]
use process_wrap::tokio::JobObject;
#[cfg(unix)]
use process_wrap::tokio::ProcessGroup;
use process_wrap::tokio::{CommandWrap, KillOnDrop};
use rmcp::transport::{ConfigureCommandExt, TokioChildProcess};
use std::process::Stdio;
use tokio::process::Command;

pub fn build_stdio_transport(
    transport: &MaterializedStdioTransport,
) -> Result<(TokioChildProcess, StderrTail)> {
    let stderr_tail = StderrTail::new(16 * 1024, transport.secrets.clone());
    let command = Command::new(transport.command.as_str()).configure(|cmd| {
        cmd.args(transport.args.iter().map(String::as_str));
        if let Some(cwd) = transport.cwd.as_deref() {
            cmd.current_dir(cwd);
        }
        cmd.envs(
            transport
                .env
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
        );
    });
    let mut command = CommandWrap::from(command);
    command.wrap(KillOnDrop);
    #[cfg(unix)]
    command.wrap(ProcessGroup::leader());
    #[cfg(windows)]
    command.wrap(JobObject);

    let (transport, stderr) = TokioChildProcess::builder(command)
        .stderr(Stdio::piped())
        .spawn()?;
    if let Some(stderr) = stderr {
        stderr_tail.spawn_reader(stderr);
    }

    Ok((transport, stderr_tail))
}
