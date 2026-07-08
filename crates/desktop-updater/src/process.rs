use std::{
    error::Error,
    fmt, thread,
    time::{Duration, Instant},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessWaitErrorCode {
    Timeout,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessWaitError {
    code: ProcessWaitErrorCode,
    message: String,
}

impl ProcessWaitError {
    fn new(code: ProcessWaitErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn timeout(pid: u32) -> Self {
        Self::new(
            ProcessWaitErrorCode::Timeout,
            format!("desktop process {pid} did not exit before timeout"),
        )
    }

    pub fn code(&self) -> ProcessWaitErrorCode {
        self.code
    }
}

impl ProcessWaitErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Timeout => "process_exit_timeout",
        }
    }
}

impl fmt::Display for ProcessWaitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for ProcessWaitError {}

pub trait ProcessProbe {
    fn is_process_running(&self, pid: u32) -> bool;
    fn now(&self) -> Instant;
    fn sleep(&self, duration: Duration);
}

pub struct SystemProcessProbe;

impl ProcessProbe for SystemProcessProbe {
    fn is_process_running(&self, pid: u32) -> bool {
        is_process_running(pid)
    }

    fn now(&self) -> Instant {
        Instant::now()
    }

    fn sleep(&self, duration: Duration) {
        thread::sleep(duration);
    }
}

pub fn wait_for_process_exit(
    pid: u32,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<(), ProcessWaitError> {
    wait_for_process_exit_with_probe(pid, timeout, poll_interval, &SystemProcessProbe)
}

pub fn wait_for_process_exit_with_probe(
    pid: u32,
    timeout: Duration,
    poll_interval: Duration,
    probe: &impl ProcessProbe,
) -> Result<(), ProcessWaitError> {
    if pid == 0 || !probe.is_process_running(pid) {
        return Ok(());
    }

    let started = probe.now();
    loop {
        if !probe.is_process_running(pid) {
            return Ok(());
        }

        let elapsed = probe.now().saturating_duration_since(started);
        if elapsed >= timeout {
            return Err(ProcessWaitError::timeout(pid));
        }

        let remaining = timeout.saturating_sub(elapsed);
        probe.sleep(poll_interval.min(remaining));
    }
}

#[cfg(unix)]
fn is_process_running(pid: u32) -> bool {
    let result = unsafe { libc::kill(pid as libc::pid_t, 0) };
    if result == 0 {
        return true;
    }

    matches!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::EPERM)
    )
}

#[cfg(windows)]
fn is_process_running(pid: u32) -> bool {
    use windows_sys::Win32::{
        Foundation::{CloseHandle, WAIT_TIMEOUT},
        System::Threading::{
            OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION, SYNCHRONIZE, WaitForSingleObject,
        },
    };

    unsafe {
        let handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION | SYNCHRONIZE, 0, pid);
        if handle.is_null() {
            return false;
        }
        let wait_result = WaitForSingleObject(handle, 0);
        CloseHandle(handle);
        wait_result == WAIT_TIMEOUT
    }
}

#[cfg(not(any(unix, windows)))]
fn is_process_running(_pid: u32) -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::{ProcessProbe, ProcessWaitErrorCode, wait_for_process_exit_with_probe};
    use std::{
        cell::{Cell, RefCell},
        collections::VecDeque,
        time::{Duration, Instant},
    };

    #[test]
    fn process_wait_returns_when_process_exits() {
        let probe = FakeProcessProbe::new([true, true, false]);

        wait_for_process_exit_with_probe(
            123,
            Duration::from_secs(60),
            Duration::from_millis(250),
            &probe,
        )
        .unwrap();

        assert_eq!(probe.sleep_count.get(), 1);
    }

    #[test]
    fn process_wait_times_out_deterministically() {
        let probe = FakeProcessProbe::always_running();

        let error = wait_for_process_exit_with_probe(
            123,
            Duration::from_secs(1),
            Duration::from_millis(250),
            &probe,
        )
        .unwrap_err();

        assert_eq!(error.code(), ProcessWaitErrorCode::Timeout);
        assert_eq!(probe.elapsed.get(), Duration::from_secs(1));
    }

    struct FakeProcessProbe {
        responses: RefCell<VecDeque<bool>>,
        elapsed: Cell<Duration>,
        sleep_count: Cell<u32>,
        started: Instant,
    }

    impl FakeProcessProbe {
        fn new(responses: impl IntoIterator<Item = bool>) -> Self {
            Self {
                responses: RefCell::new(responses.into_iter().collect()),
                elapsed: Cell::new(Duration::ZERO),
                sleep_count: Cell::new(0),
                started: Instant::now(),
            }
        }

        fn always_running() -> Self {
            Self::new(std::iter::repeat(true).take(16))
        }
    }

    impl ProcessProbe for FakeProcessProbe {
        fn is_process_running(&self, _pid: u32) -> bool {
            self.responses.borrow_mut().pop_front().unwrap_or(true)
        }

        fn now(&self) -> Instant {
            self.started + self.elapsed.get()
        }

        fn sleep(&self, duration: Duration) {
            self.elapsed.set(self.elapsed.get() + duration);
            self.sleep_count.set(self.sleep_count.get() + 1);
        }
    }
}
