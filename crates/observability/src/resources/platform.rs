//! Selected-process OS sources. No machine-wide process/disk refresh.
use super::{Field, RawUsage, SourceStatus, Value};

#[cfg(target_os = "linux")]
fn read(path: &str) -> std::io::Result<String> {
    use std::io::Read;
    let mut s = String::new();
    std::fs::File::open(path)?
        .take(64 * 1024)
        .read_to_string(&mut s)?;
    Ok(s)
}

#[cfg(target_os = "linux")]
pub(super) fn sample(pid: u32) -> std::io::Result<RawUsage> {
    let stat = read(&format!("/proc/{pid}/stat"))?;
    // comm can contain spaces and parentheses; fields after its LAST ')' have
    // stable procfs positions. CPU counters include all process threads.
    let tail = stat
        .rsplit_once(')')
        .ok_or_else(|| std::io::Error::other("invalid proc stat"))?
        .1;
    let columns: Vec<_> = tail.split_whitespace().collect();
    let number = |field: usize| -> std::io::Result<u64> {
        columns
            .get(field - 3)
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| std::io::Error::other("invalid proc field"))
    };
    let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    let pages = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if ticks <= 0 || pages <= 0 {
        return Err(std::io::Error::other("sysconf unavailable"));
    }
    let nanos = |v: u64| ((v as u128 * 1_000_000_000) / ticks as u128).min(u64::MAX as u128) as u64;
    let mut raw = RawUsage::new(
        format!("linux-startticks:{}", number(22)?),
        nanos(number(14)?),
        nanos(number(15)?),
        "procfs.stat",
    );
    raw.set(
        Field::RssBytes,
        number(24)? as f64 * pages as f64,
        "procfs.stat",
    );
    raw.set(Field::Threads, number(20)? as f64, "procfs.stat");
    raw.set(Field::MinorFaults, number(10)? as f64, "procfs.stat");
    raw.set(Field::MajorFaults, number(12)? as f64, "procfs.stat");
    if let Ok(io) = read(&format!("/proc/{pid}/io")) {
        for (field, key) in [
            (Field::StorageReadBytes, "read_bytes:"),
            (Field::StorageWriteBytes, "write_bytes:"),
            (Field::LogicalReadBytes, "rchar:"),
            (Field::LogicalWriteBytes, "wchar:"),
        ] {
            if let Some(n) = io
                .lines()
                .find_map(|line| line.strip_prefix(key)?.trim().parse::<u64>().ok())
            {
                raw.set(field, n as f64, "procfs.io");
            }
        }
    } else {
        for field in [
            Field::StorageReadBytes,
            Field::StorageWriteBytes,
            Field::LogicalReadBytes,
            Field::LogicalWriteBytes,
        ] {
            raw.unavailable(field, "procfs.io");
        }
    }
    match std::fs::read_dir(format!("/proc/{pid}/fd")) {
        Ok(entries) => {
            let mut n = 0;
            let mut failed = false;
            for entry in entries.take(16_385) {
                if entry.is_err() {
                    failed = true;
                    break;
                }
                n += 1;
            }
            if failed {
                raw.unavailable(Field::Descriptors, "procfs.fd");
            } else if n <= 16_384 {
                raw.set(Field::Descriptors, n as f64, "procfs.fd");
            } else {
                raw.values[Field::Descriptors as usize] =
                    Value::missing(Field::Descriptors, SourceStatus::Overflow, "procfs.fd");
            }
        }
        Err(_) => raw.unavailable(Field::Descriptors, "procfs.fd"),
    }
    // Reading stat again rejects exit/PID reuse while collecting other files.
    let after = read(&format!("/proc/{pid}/stat"))?;
    let identity_after = after
        .rsplit_once(')')
        .and_then(|(_, s)| s.split_whitespace().nth(19))
        .and_then(|s| s.parse::<u64>().ok());
    if identity_after != Some(number(22)?) {
        return Err(std::io::Error::other("process changed"));
    }
    Ok(raw)
}

#[cfg(target_os = "macos")]
const _: () = {
    assert!(std::mem::size_of::<libc::rusage_info_v2>() == 160);
    assert!(std::mem::offset_of!(libc::rusage_info_v2, ri_user_time) == 16);
    assert!(std::mem::offset_of!(libc::rusage_info_v2, ri_system_time) == 24);
    assert!(std::mem::offset_of!(libc::rusage_info_v2, ri_proc_start_abstime) == 80);
};

// libproc rusage_info_v2 CPU totals are Mach absolute-time ticks (not ns).
// Process-start identity deliberately retains the original absolute tick value.
#[cfg(any(target_os = "macos", test))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MachTimeError {
    Unavailable,
    InvalidTimebase,
    Overflow,
}
#[cfg(any(target_os = "macos", test))]
fn mach_nanoseconds(
    ticks: u64,
    timebase: Result<(u32, u32), MachTimeError>,
) -> Result<u64, MachTimeError> {
    let (numer, denom) = timebase?;
    if numer == 0 || denom == 0 {
        return Err(MachTimeError::InvalidTimebase);
    }
    // u64*u32 fits u128. Conversion back is checked, never clamped to a
    // plausible zero/counter value; rounding is down by less than one ns.
    u64::try_from(u128::from(ticks) * u128::from(numer) / u128::from(denom))
        .map_err(|_| MachTimeError::Overflow)
}
#[cfg(target_os = "macos")]
fn mach_timebase() -> Result<(u32, u32), MachTimeError> {
    static TIMEBASE: std::sync::OnceLock<Result<(u32, u32), MachTimeError>> =
        std::sync::OnceLock::new();
    // SDK mach/mach_time.h: two uint32_t fields, kern_return_t (int32_t).
    // Reuse the adapter's native FFI seam rather than adding a Mach crate for
    // this one immutable ratio (libc's otherwise matching facade is deprecated).
    #[repr(C)]
    struct Timebase {
        numer: u32,
        denom: u32,
    }
    unsafe extern "C" {
        fn mach_timebase_info(info: *mut Timebase) -> i32;
    }
    *TIMEBASE.get_or_init(|| {
        let mut info = Timebase { numer: 0, denom: 0 };
        if unsafe { mach_timebase_info(&mut info) } != 0 {
            return Err(MachTimeError::Unavailable);
        }
        if info.numer == 0 || info.denom == 0 {
            return Err(MachTimeError::InvalidTimebase);
        }
        Ok((info.numer, info.denom))
    })
}
#[cfg(any(target_os = "macos", test))]
fn macos_cpu_nanoseconds(
    user: u64,
    system: u64,
    timebase: Result<(u32, u32), MachTimeError>,
) -> std::io::Result<(u64, u64)> {
    let convert = |ticks| {
        mach_nanoseconds(ticks, timebase).map_err(|error| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                match error {
                    MachTimeError::Unavailable => "mach_timebase_unavailable",
                    MachTimeError::InvalidTimebase => "mach_timebase_invalid",
                    MachTimeError::Overflow => "mach_cpu_nanoseconds_overflow",
                },
            )
        })
    };
    Ok((convert(user)?, convert(system)?))
}

#[cfg(target_os = "macos")]
pub(super) fn sample(pid: u32) -> std::io::Result<RawUsage> {
    use std::mem::{size_of, zeroed};
    let mut usage: libc::rusage_info_v2 = unsafe { zeroed() };
    // rusage_info_t is an opaque pointer in libproc; flavor determines size.
    if unsafe {
        libc::proc_pid_rusage(
            pid as i32,
            libc::RUSAGE_INFO_V2,
            (&mut usage as *mut libc::rusage_info_v2).cast(),
        )
    } != 0
    {
        return Err(std::io::Error::last_os_error());
    }
    let (user_ns, system_ns) =
        macos_cpu_nanoseconds(usage.ri_user_time, usage.ri_system_time, mach_timebase())?;
    let mut raw = RawUsage::new(
        format!("macos-abstime:{}", usage.ri_proc_start_abstime),
        user_ns,
        system_ns,
        "libproc.rusage_v2",
    );
    raw.main_module_id = Some(hex::encode(usage.ri_uuid));
    for (field, n) in [
        (Field::RssBytes, usage.ri_resident_size),
        (Field::FootprintBytes, usage.ri_phys_footprint),
        (Field::StorageReadBytes, usage.ri_diskio_bytesread),
        (Field::StorageWriteBytes, usage.ri_diskio_byteswritten),
        (Field::MajorFaults, usage.ri_pageins),
        (Field::Wakeups, usage.ri_interrupt_wkups),
    ] {
        raw.set(field, n as f64, "libproc.rusage_v2");
    }
    let mut task: libc::proc_taskinfo = unsafe { zeroed() };
    let n = unsafe {
        libc::proc_pidinfo(
            pid as i32,
            libc::PROC_PIDTASKINFO,
            0,
            (&mut task as *mut libc::proc_taskinfo).cast(),
            size_of::<libc::proc_taskinfo>() as i32,
        )
    };
    if n == size_of::<libc::proc_taskinfo>() as i32 {
        raw.set(
            Field::Threads,
            task.pti_threadnum as f64,
            "libproc.taskinfo",
        );
        raw.set(Field::Faults, task.pti_faults as f64, "libproc.taskinfo");
    } else {
        raw.unavailable(Field::Threads, "libproc.taskinfo");
        raw.unavailable(Field::Faults, "libproc.taskinfo");
    }
    // pbi_nfiles is the fd-table allocation, not an open descriptor census.
    // PROC_PIDLISTFDS returns actual records, with an explicit hard cap.
    let allocated = unsafe {
        libc::proc_pidinfo(
            pid as i32,
            libc::PROC_PIDLISTFDS,
            0,
            std::ptr::null_mut(),
            0,
        )
    };
    let cap = 16_384 * std::mem::size_of::<libc::proc_fdinfo>();
    let mut descriptors = vec![
        0_u8;
        (allocated.max(0) as usize)
            .saturating_add(std::mem::size_of::<libc::proc_fdinfo>())
            .min(cap)
    ];
    let n = unsafe {
        libc::proc_pidinfo(
            pid as i32,
            libc::PROC_PIDLISTFDS,
            0,
            descriptors.as_mut_ptr().cast(),
            descriptors.len() as i32,
        )
    };
    if n > 0 && (n as usize) < descriptors.len() {
        raw.set(
            Field::Descriptors,
            (n as usize / std::mem::size_of::<libc::proc_fdinfo>()) as f64,
            "libproc.listfds",
        );
    } else if n as usize == descriptors.len() {
        raw.values[Field::Descriptors as usize] = Value::missing(
            Field::Descriptors,
            SourceStatus::Overflow,
            "libproc.listfds",
        );
    } else {
        raw.unavailable(Field::Descriptors, "libproc.listfds");
    }
    let mut after: libc::rusage_info_v0 = unsafe { zeroed() };
    if unsafe {
        libc::proc_pid_rusage(
            pid as i32,
            libc::RUSAGE_INFO_V0,
            (&mut after as *mut libc::rusage_info_v0).cast(),
        )
    } != 0
        || after.ri_proc_start_abstime != usage.ri_proc_start_abstime
    {
        return Err(std::io::Error::other("process exited or changed"));
    }
    Ok(raw)
}

// GetProcessTimes.lpExitTime is undefined while alive. Only a zero-time
// wait on a process handle with SYNCHRONIZE establishes liveness; no polling
// loop, exit-code convention, privilege change or target signal is involved.
#[cfg(windows)]
use windows_sys::Win32::Foundation::{WAIT_OBJECT_0, WAIT_TIMEOUT};
// Pinned WinAPI status fixtures for compiling the pure classifier on other OS.
#[cfg(all(test, not(windows)))]
const WAIT_OBJECT_0: u32 = 0;
#[cfg(all(test, not(windows)))]
const WAIT_TIMEOUT: u32 = 258;
#[cfg(any(windows, test))]
fn windows_wait_status(result: std::io::Result<u32>) -> std::io::Result<()> {
    match result? {
        WAIT_TIMEOUT => Ok(()), // process is not signaled
        WAIT_OBJECT_0 => Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "process_exited",
        )), // WAIT_OBJECT_0
        _ => Err(std::io::Error::other("process_wait_state_unknown")),
    }
}
#[cfg(any(windows, test))]
fn checked_windows_times(
    read: impl FnOnce() -> std::io::Result<[u64; 4]>,
    wait: impl FnOnce() -> std::io::Result<u32>,
) -> std::io::Result<(u64, u64, u64)> {
    let [created, _undefined_exit, user, kernel] = read()?;
    windows_wait_status(wait())?;
    Ok((created, user, kernel))
}
#[cfg(windows)]
struct WindowsHandle(windows_sys::Win32::Foundation::HANDLE);
#[cfg(windows)]
impl Drop for WindowsHandle {
    fn drop(&mut self) {
        unsafe {
            windows_sys::Win32::Foundation::CloseHandle(self.0);
        }
    }
}
#[cfg(windows)]
fn windows_liveness(handle: windows_sys::Win32::Foundation::HANDLE) -> std::io::Result<u32> {
    use windows_sys::Win32::{Foundation::WAIT_FAILED, System::Threading::WaitForSingleObject};
    let result = unsafe { WaitForSingleObject(handle, 0) };
    if result == WAIT_FAILED {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(result)
    }
}
#[cfg(windows)]
fn windows_process_times(
    handle: windows_sys::Win32::Foundation::HANDLE,
) -> std::io::Result<(u64, u64, u64)> {
    use windows_sys::Win32::{Foundation::FILETIME, System::Threading::GetProcessTimes};
    checked_windows_times(
        || {
            let (mut created, mut exit, mut kernel, mut user): (
                FILETIME,
                FILETIME,
                FILETIME,
                FILETIME,
            ) = unsafe { std::mem::zeroed() };
            if unsafe { GetProcessTimes(handle, &mut created, &mut exit, &mut kernel, &mut user) }
                == 0
            {
                return Err(std::io::Error::last_os_error());
            }
            let ticks =
                |t: FILETIME| (u64::from(t.dwHighDateTime) << 32) | u64::from(t.dwLowDateTime);
            Ok([ticks(created), ticks(exit), ticks(user), ticks(kernel)])
        },
        || windows_liveness(handle),
    )
}
#[cfg(windows)]
pub(super) fn sample(pid: u32) -> std::io::Result<RawUsage> {
    use std::mem::{size_of, zeroed};
    use windows_sys::Win32::System::{
        ProcessStatus::{GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS},
        Threading::{
            GetProcessHandleCount, GetProcessIoCounters, IO_COUNTERS, OpenProcess,
            PROCESS_QUERY_INFORMATION, PROCESS_SYNCHRONIZE, PROCESS_VM_READ,
        },
    };
    let handle = unsafe {
        OpenProcess(
            PROCESS_QUERY_INFORMATION | PROCESS_VM_READ | PROCESS_SYNCHRONIZE,
            0,
            pid,
        )
    };
    if handle.is_null() {
        return Err(std::io::Error::last_os_error());
    }
    let handle = WindowsHandle(handle);
    let (start, user, kernel) = windows_process_times(handle.0)?;
    let mut raw = RawUsage::new(
        format!("windows-filetime:{start}"),
        user.saturating_mul(100),
        kernel.saturating_mul(100),
        "GetProcessTimes",
    );
    let mut memory: PROCESS_MEMORY_COUNTERS = unsafe { zeroed() };
    if unsafe {
        GetProcessMemoryInfo(
            handle.0,
            &mut memory,
            size_of::<PROCESS_MEMORY_COUNTERS>() as u32,
        )
    } != 0
    {
        raw.set(
            Field::RssBytes,
            memory.WorkingSetSize as f64,
            "GetProcessMemoryInfo.working_set",
        );
        raw.set(
            Field::Faults,
            memory.PageFaultCount as f64,
            "GetProcessMemoryInfo",
        );
    } else {
        raw.unavailable(Field::RssBytes, "GetProcessMemoryInfo");
        raw.unavailable(Field::Faults, "GetProcessMemoryInfo");
    }
    let mut count = 0;
    if unsafe { GetProcessHandleCount(handle.0, &mut count) } != 0 {
        raw.set(Field::Handles, count as f64, "GetProcessHandleCount");
    } else {
        raw.unavailable(Field::Handles, "GetProcessHandleCount");
    }
    let mut io: IO_COUNTERS = unsafe { zeroed() };
    if unsafe { GetProcessIoCounters(handle.0, &mut io) } != 0 {
        raw.set(
            Field::LogicalReadBytes,
            io.ReadTransferCount as f64,
            "GetProcessIoCounters",
        );
        raw.set(
            Field::LogicalWriteBytes,
            io.WriteTransferCount as f64,
            "GetProcessIoCounters",
        );
    } else {
        raw.unavailable(Field::LogicalReadBytes, "GetProcessIoCounters");
        raw.unavailable(Field::LogicalWriteBytes, "GetProcessIoCounters");
    }
    windows_wait_status(windows_liveness(handle.0))?;
    Ok(raw)
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
pub(super) fn sample(_pid: u32) -> std::io::Result<RawUsage> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "selected-process adapter unavailable on this target",
    ))
}

pub(super) fn host() -> Vec<Value> {
    let mut values: Vec<_> = [
        Field::HostMemoryBytes,
        Field::HostAvailableBytes,
        Field::HostSwapUsedBytes,
        Field::HostPressure,
        Field::LogicalCpus,
        Field::QuotaCores,
    ]
    .into_iter()
    .map(|f| Value::missing(f, SourceStatus::Unsupported, "none"))
    .collect();
    // OS-supported but currently unreadable is unavailable, never unsupported.
    for value in &mut values {
        let supported = match value.field {
            Field::HostMemoryBytes | Field::LogicalCpus | Field::HostPressure => {
                cfg!(any(target_os = "macos", target_os = "linux", windows))
            }
            Field::HostSwapUsedBytes => cfg!(any(target_os = "macos", target_os = "linux")),
            Field::HostAvailableBytes => cfg!(any(target_os = "linux", windows)),
            Field::QuotaCores => cfg!(target_os = "linux"),
            _ => false,
        };
        if supported {
            value.status = SourceStatus::Unavailable;
            value.source = "platform_source_unavailable".into();
        }
    }
    let mut set = |field, value, source| {
        if let Some(slot) = values.iter_mut().find(|v| v.field == field) {
            *slot = Value::observed(field, value, source);
            if field == Field::HostPressure {
                slot.unit = if source == "sysctl.memorystatus.level" {
                    "{pressure_level_bitmask}"
                } else {
                    "%"
                }
                .into();
            }
        }
    };
    #[cfg(target_os = "linux")]
    {
        if let Ok(mem) = read("/proc/meminfo") {
            let n = |key: &str| {
                mem.lines()
                    .find_map(|l| {
                        l.strip_prefix(key)?
                            .split_whitespace()
                            .next()?
                            .parse::<f64>()
                            .ok()
                    })
                    .map(|v| v * 1024.0)
            };
            if let Some(v) = n("MemTotal:") {
                set(Field::HostMemoryBytes, v, "procfs.meminfo");
            }
            if let Some(v) = n("MemAvailable:") {
                set(Field::HostAvailableBytes, v, "procfs.meminfo");
            }
            if let Some((total, free)) = n("SwapTotal:").zip(n("SwapFree:")) {
                set(Field::HostSwapUsedBytes, total - free, "procfs.meminfo");
            }
        }
        // PSI avg10 is a percentage of wall time with at least one stalled task.
        if let Ok(psi) = read("/proc/pressure/memory") {
            if let Some(v) = psi.lines().find(|l| l.starts_with("some ")).and_then(|l| {
                l.split_whitespace()
                    .find_map(|s| s.strip_prefix("avg10=")?.parse::<f64>().ok())
            }) {
                set(Field::HostPressure, v, "linux.psi.memory.some.avg10");
            }
        }
        let cpus = unsafe { libc::sysconf(libc::_SC_NPROCESSORS_ONLN) };
        if cpus > 0 {
            set(Field::LogicalCpus, cpus as f64, "sysconf.online_cpus");
        }
        // cgroup v2 ancestors, bounded; v1 or inaccessible delegation stays unavailable.
        if let Ok(group) = read("/proc/self/cgroup") {
            if let Some(path) = group.lines().find_map(|s| s.strip_prefix("0::")) {
                let relative = std::path::Path::new(path.trim_start_matches('/'));
                if relative
                    .components()
                    .all(|c| matches!(c, std::path::Component::Normal(_)))
                {
                    let base = std::path::Path::new("/sys/fs/cgroup");
                    let mut at = base.join(relative);
                    let mut limit: Option<f64> = None;
                    for _ in 0..32 {
                        if let Ok(max) = read(&at.join("cpu.max").to_string_lossy()) {
                            let mut parts = max.split_whitespace();
                            if let Some((q, p)) = parts
                                .next()
                                .and_then(|s| s.parse::<f64>().ok())
                                .zip(parts.next().and_then(|s| s.parse::<f64>().ok()))
                            {
                                if p > 0.0 {
                                    limit = Some(limit.map_or(q / p, |old| old.min(q / p)));
                                }
                            }
                        }
                        if at == base || !at.pop() {
                            break;
                        }
                    }
                    if let Some(v) = limit {
                        set(Field::QuotaCores, v, "cgroup_v2.cpu.max.ancestors");
                    }
                }
            }
        }
    }
    #[cfg(target_os = "macos")]
    {
        fn sysctl<T: Copy>(name: &std::ffi::CStr) -> Option<T> {
            let mut value = std::mem::MaybeUninit::<T>::uninit();
            let mut len = std::mem::size_of::<T>();
            if unsafe {
                libc::sysctlbyname(
                    name.as_ptr(),
                    value.as_mut_ptr().cast(),
                    &mut len,
                    std::ptr::null_mut(),
                    0,
                )
            } == 0
                && len == std::mem::size_of::<T>()
            {
                Some(unsafe { value.assume_init() })
            } else {
                None
            }
        }
        if let Some(v) = sysctl::<u64>(c"hw.memsize") {
            set(Field::HostMemoryBytes, v as f64, "sysctl.hw.memsize");
        }
        if let Some(v) = sysctl::<u32>(c"hw.logicalcpu") {
            set(Field::LogicalCpus, v as f64, "sysctl.hw.logicalcpu");
        }
        if let Some(v) = sysctl::<u32>(c"kern.memorystatus_vm_pressure_level") {
            set(Field::HostPressure, v as f64, "sysctl.memorystatus.level");
        }
        #[repr(C)]
        #[derive(Clone, Copy)]
        struct Swap {
            total: u64,
            avail: u64,
            used: u64,
            pagesize: u32,
            encrypted: i32,
        }
        if let Some(v) = sysctl::<Swap>(c"vm.swapusage") {
            set(
                Field::HostSwapUsedBytes,
                v.used as f64,
                "sysctl.vm.swapusage",
            );
        }
    }
    #[cfg(windows)]
    {
        use windows_sys::Win32::System::{
            SystemInformation::{GlobalMemoryStatusEx, MEMORYSTATUSEX},
            Threading::{ALL_PROCESSOR_GROUPS, GetActiveProcessorCount},
        };
        let mut memory: MEMORYSTATUSEX = unsafe { std::mem::zeroed() };
        memory.dwLength = std::mem::size_of::<MEMORYSTATUSEX>() as u32;
        if unsafe { GlobalMemoryStatusEx(&mut memory) } != 0 {
            set(
                Field::HostMemoryBytes,
                memory.ullTotalPhys as f64,
                "GlobalMemoryStatusEx",
            );
            set(
                Field::HostAvailableBytes,
                memory.ullAvailPhys as f64,
                "GlobalMemoryStatusEx",
            );
            set(
                Field::HostPressure,
                memory.dwMemoryLoad as f64,
                "GlobalMemoryStatusEx.memory_load_percent",
            );
        }
        let cpus = unsafe { GetActiveProcessorCount(ALL_PROCESSOR_GROUPS) };
        if cpus > 0 {
            set(Field::LogicalCpus, cpus as f64, "GetActiveProcessorCount");
        }
    }
    values
}

pub(super) fn boot_ns() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let mut t: libc::timespec = unsafe { std::mem::zeroed() };
        if unsafe { libc::clock_gettime(libc::CLOCK_BOOTTIME, &mut t) } == 0 {
            return Some(
                (t.tv_sec as u64)
                    .saturating_mul(1_000_000_000)
                    .saturating_add(t.tv_nsec as u64),
            );
        }
    }
    #[cfg(target_os = "macos")]
    {
        unsafe extern "C" {
            fn mach_continuous_time() -> u64;
        }
        return mach_nanoseconds(unsafe { mach_continuous_time() }, mach_timebase()).ok();
    }
    #[cfg(windows)]
    {
        return Some(
            unsafe { windows_sys::Win32::System::SystemInformation::GetTickCount64() }
                .saturating_mul(1_000_000),
        );
    }
    #[allow(unreachable_code)]
    None
}

/// Minimal identity read for spawn/attach guards; never walks fd tables or IO.
pub(super) fn start(pid: u32) -> std::io::Result<String> {
    #[cfg(target_os = "linux")]
    {
        let stat = read(&format!("/proc/{pid}/stat"))?;
        let ticks = stat
            .rsplit_once(')')
            .and_then(|(_, s)| s.split_whitespace().nth(19))
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or_else(|| std::io::Error::other("invalid process identity"))?;
        return Ok(format!("linux-startticks:{ticks}"));
    }
    #[cfg(target_os = "macos")]
    {
        let mut raw: libc::rusage_info_v0 = unsafe { std::mem::zeroed() };
        if unsafe {
            libc::proc_pid_rusage(
                pid as i32,
                libc::RUSAGE_INFO_V0,
                (&mut raw as *mut libc::rusage_info_v0).cast(),
            )
        } != 0
        {
            return Err(std::io::Error::last_os_error());
        }
        return Ok(format!("macos-abstime:{}", raw.ri_proc_start_abstime));
    }
    #[cfg(windows)]
    {
        use windows_sys::Win32::System::Threading::{
            OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION, PROCESS_SYNCHRONIZE,
        };
        let handle = unsafe {
            OpenProcess(
                PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_SYNCHRONIZE,
                0,
                pid,
            )
        };
        if handle.is_null() {
            return Err(std::io::Error::last_os_error());
        }
        let handle = WindowsHandle(handle);
        let (created, _, _) = windows_process_times(handle.0)?;
        return Ok(format!("windows-filetime:{created}"));
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
    {
        let _ = pid;
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "process identity unavailable",
        ))
    }
}

#[cfg(test)]
mod windows_liveness_tests {
    use super::*;
    use std::io::{Error, ErrorKind};
    #[test]
    fn arbitrary_exit_filetime_never_establishes_liveness() {
        for undefined_exit in [0, 1, u64::MAX] {
            let live =
                checked_windows_times(|| Ok([10, undefined_exit, 0, 0]), || Ok(258)).unwrap();
            assert_eq!(live, (10, 0, 0)); // observed zero CPU, valid creation identity
            assert_eq!(
                checked_windows_times(|| Ok([10, undefined_exit, 0, 0]), || Ok(0))
                    .unwrap_err()
                    .kind(),
                ErrorKind::NotFound
            );
        }
        for kind in [ErrorKind::PermissionDenied, ErrorKind::Other] {
            assert_eq!(
                checked_windows_times(|| Ok([10, 0, 0, 0]), || Err(Error::from(kind)))
                    .unwrap_err()
                    .kind(),
                kind
            );
            assert_eq!(
                checked_windows_times(|| Err(Error::from(kind)), || Ok(0))
                    .unwrap_err()
                    .kind(),
                kind
            );
        }
        assert_eq!(
            windows_wait_status(Ok(99)).unwrap_err().kind(),
            ErrorKind::Other
        );
        let first = checked_windows_times(|| Ok([10, 0, 0, 0]), || Ok(258)).unwrap();
        let reused = checked_windows_times(|| Ok([20, u64::MAX, 0, 0]), || Ok(258)).unwrap();
        assert_ne!(first.0, reused.0);
    }
}

#[cfg(test)]
mod mach_cpu_tests {
    use super::*;
    #[test]
    fn libproc_cpu_ticks_use_native_timebase_before_raw_usage() {
        // Independent oracle: M1 24,000,000 ticks = 1 second, half as
        // many system ticks = 0.5 seconds. Unit timebase is already ns.
        assert_eq!(
            macos_cpu_nanoseconds(24_000_000, 12_000_000, Ok((125, 3))).unwrap(),
            (1_000_000_000, 500_000_000)
        );
        assert_eq!(
            macos_cpu_nanoseconds(1_913_246_000, 0, Ok((1, 1))).unwrap(),
            (1_913_246_000, 0)
        );
        assert_eq!(
            macos_cpu_nanoseconds(u64::MAX, u64::MAX, Ok((3, 3))).unwrap(),
            (u64::MAX, u64::MAX)
        );
        assert_eq!(
            mach_nanoseconds(u64::MAX, Ok((1, 3))).unwrap(),
            6_148_914_691_236_517_205
        );
        assert_eq!(mach_nanoseconds(1, Ok((125, 3))).unwrap(), 41);
    }
    #[test]
    fn invalid_timebase_or_cpu_overflow_never_becomes_observed_zero() {
        for base in [Err(MachTimeError::Unavailable), Ok((125, 0)), Ok((0, 3))] {
            assert!(macos_cpu_nanoseconds(0, 0, base).is_err());
        }
        assert_eq!(
            mach_nanoseconds(u64::MAX, Ok((125, 3))),
            Err(MachTimeError::Overflow)
        );
        assert!(macos_cpu_nanoseconds(0, u64::MAX, Ok((125, 3))).is_err());
        assert_eq!(macos_cpu_nanoseconds(0, 0, Ok((125, 3))).unwrap(), (0, 0));
    }
}
