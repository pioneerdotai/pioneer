use std::process::ExitCode;

use anyhow::{Context, Result};
use pioneer_postgres_test::{RUN_PREFIX, with_test_run};
use process_wrap::tokio::{CommandWrap, KillOnDrop};
use tokio::process::Command;

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(code) => code,
        Err(error) => {
            eprintln!("{error:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<ExitCode> {
    let mut args = std::env::args_os().skip(1);
    let program = args.next().context("expected a test runner command")?;
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    #[cfg(unix)]
    let termination = terminate.recv();
    #[cfg(not(unix))]
    let termination = std::future::pending::<Option<()>>();

    with_test_run(|prefix| async move {
        let mut command = Command::new(program);
        // Database fixtures choose their own target inside the test process.
        command
            .args(args)
            .env_remove("DATABASE_URL")
            .env(RUN_PREFIX, prefix);
        let mut command = CommandWrap::from(command);
        command.wrap(KillOnDrop);
        #[cfg(unix)]
        command.wrap(process_wrap::tokio::ProcessGroup::leader());
        #[cfg(windows)]
        command.wrap(process_wrap::tokio::JobObject);
        let mut child = command.spawn().context("start test runner")?;
        tokio::select! {
            status = child.wait() => {
                let code = status.context("wait for test runner")?.code();
                Ok(code.and_then(|code| u8::try_from(code).ok())
                    .map_or(ExitCode::FAILURE, ExitCode::from))
            }
            signal = tokio::signal::ctrl_c() => {
                signal.context("listen for test runner interrupt")?;
                child.start_kill().context("stop interrupted test runner")?;
                child.wait().await.context("reap interrupted test runner")?;
                Ok(ExitCode::from(130))
            }
            _ = termination => {
                child.start_kill().context("stop terminated test runner")?;
                child.wait().await.context("reap terminated test runner")?;
                Ok(ExitCode::from(143))
            }
        }
    })
    .await
}
