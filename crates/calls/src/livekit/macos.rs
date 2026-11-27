//! The helper owns the child handle and the runtime lock. Gateway owns only
//! the write end of the lifetime pipe. A Gateway crash closes this pipe in the
//! kernel, even if Gateway's Rust destructors never execute.
use anyhow::{Context, Result, bail, ensure};
use std::{
    ffi::{OsStr, OsString},
    fs::{File, OpenOptions, TryLockError},
    io::Read,
    path::Path,
    process::{Child, Command, Stdio},
    sync::mpsc::{self, RecvTimeoutError},
    time::Duration,
};

pub(super) const ARGUMENT: &str = "__livekit-supervisor";
pub(super) const LOCK_FILE: &str = "livekit-sidecar.lock";
const CHILD_CHECK_INTERVAL: Duration = Duration::from_millis(100);

pub fn dispatch(args: impl IntoIterator<Item = OsString>) -> Option<Result<()>> {
    let mut args = args.into_iter();
    if args.next().as_deref() != Some(OsStr::new(ARGUMENT)) {
        return None;
    }
    Some((|| {
        let (Some(binary), Some(config), Some(lock), None) =
            (args.next(), args.next(), args.next(), args.next())
        else {
            bail!("invalid LiveKit supervisor arguments");
        };
        let mut command = Command::new(binary);
        command.arg("--config").arg(config);
        supervise(Path::new(&lock), std::io::stdin(), command, |_| {})
    })())
}

fn supervise(
    lock_path: &Path,
    mut owner: impl Read + Send + 'static,
    mut command: Command,
    started: impl FnOnce(&Child),
) -> Result<()> {
    let (closed, parent) = mpsc::channel();
    std::thread::Builder::new()
        .name("livekit-owner".into())
        .spawn(move || {
            let mut byte = [0];
            let _ = owner.read(&mut byte);
            let _ = closed.send(());
        })?;
    // Never remove this lock file. The next supervisor must lock the same
    // inode, and can proceed only after this one reaps its own child.
    let lock = open_lock(lock_path)?;
    loop {
        match lock.try_lock() {
            Ok(()) => break,
            Err(TryLockError::WouldBlock) => {
                if parent.recv_timeout(CHILD_CHECK_INTERVAL) != Err(RecvTimeoutError::Timeout) {
                    return Ok(());
                }
            }
            Err(TryLockError::Error(error)) => return Err(error.into()),
        }
    }
    if parent.try_recv() != Err(mpsc::TryRecvError::Empty) {
        return Ok(());
    }

    command
        .env_clear()
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let mut child = command.spawn().context("launch stock LiveKit")?;
    started(&child);
    loop {
        if parent.recv_timeout(CHILD_CHECK_INTERVAL) != Err(RecvTimeoutError::Timeout) {
            // Keep ownership until wait confirms the ports have been released.
            let _ = child.kill();
            child.wait().context("reap owned LiveKit")?;
            return Ok(());
        }
        match child.try_wait() {
            Ok(Some(status)) => {
                ensure!(status.success(), "stock LiveKit exited ({status})");
                return Ok(());
            }
            Ok(None) => {}
            Err(error) => {
                let _ = child.kill();
                child
                    .wait()
                    .context("reap owned LiveKit after wait failure")?;
                return Err(error).context("check stock LiveKit");
            }
        }
    }
}

fn open_lock(path: &Path) -> std::io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(role: &str, dir: &Path) -> Command {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "livekit::macos::tests::crash_fixture",
                "--ignored",
            ])
            .env("PIONEER_TEST_LIVEKIT_OWNER_ROLE", role)
            .env("PIONEER_TEST_LIVEKIT_OWNER_DIR", dir)
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        command
    }

    #[test]
    #[ignore = "subprocess entry for the crash-recovery regression"]
    fn crash_fixture() {
        use std::os::unix::process::CommandExt;
        let Ok(role) = std::env::var("PIONEER_TEST_LIVEKIT_OWNER_ROLE") else {
            return;
        };
        let dir =
            std::path::PathBuf::from(std::env::var_os("PIONEER_TEST_LIVEKIT_OWNER_DIR").unwrap());
        if role == "gateway" {
            let mut child = fixture("supervisor", &dir)
                .process_group(0)
                .stdin(Stdio::piped())
                .spawn()
                .unwrap();
            // Hold the write end until this process is killed.
            let _lease = child.stdin.take().unwrap();
            child.wait().unwrap();
        } else {
            let mut child = Command::new("/bin/sleep");
            child.arg("30");
            supervise(&dir.join(LOCK_FILE), std::io::stdin(), child, |_| {
                std::fs::write(dir.join("ready"), b"").unwrap();
            })
            .unwrap();
        }
    }

    #[test]
    fn sigkill_of_gateway_releases_the_lock_for_the_next_supervisor() {
        let dir = tempfile::tempdir().unwrap();
        let mut gateway = fixture("gateway", dir.path())
            .stdin(Stdio::null())
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !dir.path().join("ready").exists() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }
        let started = dir.path().join("ready").exists();
        let lock = open_lock(&dir.path().join(LOCK_FILE)).unwrap();
        let owned_before_crash = matches!(lock.try_lock(), Err(TryLockError::WouldBlock));
        let kill = gateway.kill();
        gateway.wait().unwrap();
        assert!(started && owned_before_crash);
        kill.unwrap();

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            match lock.try_lock() {
                Ok(()) => break,
                Err(TryLockError::WouldBlock) if std::time::Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(20))
                }
                Err(error) => panic!("replacement could not acquire LiveKit ownership: {error}"),
            }
        }
    }

    #[test]
    fn owner_pipe_closure_reaps_only_its_child_and_releases_the_runtime_lock() {
        let dir = tempfile::tempdir_in(std::env::temp_dir()).unwrap();
        let lock_path = dir.path().join(LOCK_FILE);
        let (reader, writer) = std::io::pipe().unwrap();
        let (started, ready) = mpsc::channel();
        let supervisor = std::thread::spawn({
            let lock_path = lock_path.clone();
            move || {
                let mut command = Command::new("/bin/sleep");
                command.arg("30");
                supervise(&lock_path, reader, command, |child| {
                    started.send(child.id()).unwrap()
                })
            }
        });
        ready.recv_timeout(Duration::from_secs(5)).unwrap();
        let lock = open_lock(&lock_path).unwrap();
        assert!(matches!(lock.try_lock(), Err(TryLockError::WouldBlock)));
        drop(writer);
        supervisor.join().unwrap().unwrap();
        lock.try_lock().unwrap();
    }
}
