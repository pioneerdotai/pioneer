//! Host-side capacity measurements used to admit a new installed App VM.
use super::storage::MIB;
use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

const CPU_WINDOW: Duration = Duration::from_secs(10);
const SAMPLE_INTERVAL: Duration = Duration::from_secs(1);
const MAX_SAMPLE_GAP: Duration = Duration::from_secs(3);
const BOOT_OVERHEAD: u64 = 128 * MIB;
const MEMORY_CAPACITY_UNAVAILABLE: &str = "apps.waiting_for_memory_capacity";
pub(super) const MEMORY_HIERARCHY_UNAVAILABLE: &str = "apps.memory_cgroup_hierarchy_unavailable";

#[derive(Default)]
pub(super) struct HostCapacity {
    cpu: sysinfo::System,
    samples: VecDeque<(Instant, Instant, f32)>,
    last_sample: Option<Instant>,
}

pub(super) struct MemoryCharge {
    pub bytes: u64,
    pub ready: Option<(String, i32)>,
}

pub(super) struct SandboxLimits {
    pub memory_mib: u32,
    pub cpus: u8,
}

impl SandboxLimits {
    pub(super) fn launch_memory(&self) -> u64 {
        u64::from(self.memory_mib) * MIB + BOOT_OVERHEAD
    }
}

impl HostCapacity {
    pub(super) fn admit(
        &mut self,
        registry: &str,
        charges: &[MemoryCharge],
    ) -> Result<SandboxLimits, &'static str> {
        let cpu = self.cpu_usage();
        let mut system = sysinfo::System::new();
        system.refresh_memory();
        let memory = system.total_memory();
        if memory == 0 {
            return Err(MEMORY_CAPACITY_UNAVAILABLE);
        }
        #[cfg(target_os = "linux")]
        let memory = linux_memory_limit()?.map_or(memory, |limit| memory.min(limit));
        if cpu.ok_or("apps.waiting_for_cpu_sample")? >= 75.0 {
            return Err("apps.waiting_for_cpu");
        }
        let budget = memory / 2;
        let cpu_count = std::thread::available_parallelism().map_or(1, |count| count.get());
        // msb_krun_vmm rejects boot geometry above MAX_SUPPORTED_VCPUS.
        let limits = SandboxLimits {
            memory_mib: u32::try_from(budget / 2 / MIB).unwrap_or(u32::MAX),
            cpus: (cpu_count / 4).clamp(1, 64) as u8,
        };
        let used = vm_memory(registry, charges);
        if limits.memory_mib == 0 || used.saturating_add(limits.launch_memory()) > budget {
            return Err("apps.waiting_for_memory");
        }
        Ok(limits)
    }

    fn cpu_usage(&mut self) -> Option<f32> {
        let now = Instant::now();
        if self
            .last_sample
            .is_none_or(|last| now.duration_since(last) >= SAMPLE_INTERVAL)
        {
            self.cpu.refresh_cpu_usage();
            let previous = self.last_sample.replace(now);
            match previous {
                Some(last) if now.duration_since(last) <= MAX_SAMPLE_GAP => {
                    let usage = self.cpu.global_cpu_usage();
                    if usage.is_finite() {
                        self.samples.push_back((last, now, usage));
                    } else {
                        self.samples.clear();
                    }
                }
                _ => self.samples.clear(),
            }
        }
        let end = self.last_sample?;
        let start = end.checked_sub(CPU_WINDOW)?;
        while self
            .samples
            .front()
            .is_some_and(|&(_, until, _)| until <= start)
        {
            self.samples.pop_front();
        }
        if self
            .samples
            .front()
            .is_none_or(|&(from, _, _)| from > start)
        {
            return None;
        }
        let usage: f32 = self
            .samples
            .iter()
            .map(|&(from, until, usage)| {
                usage * until.duration_since(from.max(start)).as_secs_f32()
            })
            .sum();
        Some(usage / CPU_WINDOW.as_secs_f32())
    }
}

#[cfg(target_os = "linux")]
fn linux_memory_limit() -> Result<Option<u64>, &'static str> {
    let membership = std::fs::read("/proc/self/cgroup").map_err(|_| MEMORY_CAPACITY_UNAVAILABLE)?;
    let mounts = std::fs::read("/proc/self/mountinfo").map_err(|_| MEMORY_CAPACITY_UNAVAILABLE)?;
    cgroup_memory_limit(&membership, &mounts, |path| std::fs::read_to_string(path))
}

#[cfg(target_os = "linux")]
fn cgroup_memory_limit(
    membership: &[u8],
    mounts: &[u8],
    read: impl Fn(&std::path::Path) -> std::io::Result<String>,
) -> Result<Option<u64>, &'static str> {
    use std::{ffi::OsStr, os::unix::ffi::OsStrExt, path::Path};

    let has_memory = |controllers: &[u8]| {
        controllers
            .split(|&byte| byte == b',')
            .any(|name| name == b"memory")
    };
    let mut memberships = membership.split(|&byte| byte == b'\n').filter_map(|line| {
        let mut parts = line.splitn(3, |&byte| byte == b':');
        parts.next()?;
        Some((parts.next()?, parts.next()?))
    });
    // A controller can belong to only one hierarchy, including hybrid hosts.
    let scope = memberships
        .clone()
        .find(|(controllers, _)| has_memory(controllers))
        .map(|(_, member)| (true, member))
        .or_else(|| {
            memberships
                .find(|(controllers, _)| controllers.is_empty())
                .map(|(_, member)| (false, member))
        });
    let Some((v1, member)) = scope else {
        return Ok(None);
    };
    let member = Path::new(OsStr::from_bytes(member));
    let mut observed = false;
    let mut limit = None;
    let mut failure = if v1 {
        MEMORY_CAPACITY_UNAVAILABLE
    } else {
        MEMORY_HIERARCHY_UNAVAILABLE
    };
    for (root, mount) in mounts.split(|&byte| byte == b'\n').filter_map(|line| {
        let separator = line.windows(3).position(|bytes| bytes == b" - ")?;
        let mut options = line[separator + 3..].split(|&byte| byte == b' ');
        let fs = options.next()?;
        let flags = options.nth(1)?;
        if (v1 && (fs != b"cgroup" || !has_memory(flags))) || (!v1 && fs != b"cgroup2") {
            return None;
        }
        let mut fields = line[..separator].split(|&byte| byte == b' ').skip(3);
        Some((
            mountinfo_path(fields.next()?)?,
            mountinfo_path(fields.next()?)?,
        ))
    }) {
        let Ok(relative) = member.strip_prefix(&root) else {
            continue;
        };
        if relative
            .components()
            .any(|part| part == std::path::Component::ParentDir)
        {
            continue;
        }
        let directory = mount.join(relative);
        let read_scope = || -> Result<Option<u64>, &'static str> {
            if v1 {
                // The kernel's effective value includes ancestors outside a
                // subtree bind mount. A local unlimited sentinel cannot prove
                // that the hierarchy is unlimited.
                let stat = read(&directory.join("memory.stat"))
                    .map_err(|_| MEMORY_CAPACITY_UNAVAILABLE)?;
                let effective = stat
                    .lines()
                    .find_map(|line| {
                        let mut fields = line.split_ascii_whitespace();
                        if fields.next()? != "hierarchical_memory_limit" {
                            return None;
                        }
                        fields.next()?.parse::<u64>().ok()
                    })
                    .ok_or(MEMORY_CAPACITY_UNAVAILABLE)?;
                let visible = directory
                    .ancestors()
                    .take_while(|path| path.starts_with(&mount))
                    .filter_map(|path| {
                        read(&path.join("memory.limit_in_bytes"))
                            .ok()?
                            .trim()
                            .parse::<u64>()
                            .ok()
                    })
                    .min();
                return Ok(Some(
                    visible.map_or(effective, |visible| visible.min(effective)),
                ));
            }

            cgroup_v2_memory_limit(&root, &mount, relative, &read)
        };
        match read_scope() {
            Ok(value) => {
                observed = true;
                limit = limit.into_iter().chain(value).min();
            }
            Err(MEMORY_CAPACITY_UNAVAILABLE) => failure = MEMORY_CAPACITY_UNAVAILABLE,
            Err(_) => {}
        }
    }
    if observed { Ok(limit) } else { Err(failure) }
}

#[cfg(target_os = "linux")]
fn cgroup_v2_memory_limit(
    root: &std::path::Path,
    mount: &std::path::Path,
    relative: &std::path::Path,
    read: impl Fn(&std::path::Path) -> std::io::Result<String>,
) -> Result<Option<u64>, &'static str> {
    use std::{io::ErrorKind, path::Path};

    let unavailable = |error: std::io::Error| match error.kind() {
        ErrorKind::NotFound | ErrorKind::PermissionDenied => MEMORY_HIERARCHY_UNAVAILABLE,
        _ => MEMORY_CAPACITY_UNAVAILABLE,
    };

    // A private namespace can report "/" for a subtree. The kernel exposes
    // cgroup.events in every non-root group, even without memory delegation.
    if root != Path::new("/") {
        return Err(MEMORY_HIERARCHY_UNAVAILABLE);
    }
    match read(&mount.join("cgroup.events")) {
        Ok(_) => return Err(MEMORY_HIERARCHY_UNAVAILABLE),
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(unavailable(error)),
    }

    let mut path = mount.to_owned();
    let mut controllers = read(&path.join("cgroup.subtree_control")).map_err(unavailable)?;
    let mut minimum = None;
    let mut components = relative.iter().peekable();
    while let Some(component) = components.next() {
        // A controller cannot be enabled deeper than its last delegation.
        if !controllers
            .split_ascii_whitespace()
            .any(|name| name == "memory")
        {
            break;
        }
        path.push(component);
        let value = read(&path.join("memory.max")).map_err(unavailable)?;
        if value.trim() != "max" {
            let value: u64 = value
                .trim()
                .parse()
                .map_err(|_| MEMORY_CAPACITY_UNAVAILABLE)?;
            minimum = Some(minimum.map_or(value, |current: u64| current.min(value)));
        }
        if components.peek().is_some() {
            controllers = read(&path.join("cgroup.subtree_control")).map_err(unavailable)?;
        }
    }
    Ok(minimum)
}

#[cfg(target_os = "linux")]
pub(super) fn mountinfo_path(mut field: &[u8]) -> Option<std::path::PathBuf> {
    use std::{ffi::OsString, os::unix::ffi::OsStringExt};

    // Mountinfo paths escape whitespace and backslashes. Superblock option
    // values also escape commas.
    let mut path = Vec::with_capacity(field.len());
    while let Some(escape) = field.iter().position(|&byte| byte == b'\\') {
        path.extend_from_slice(&field[..escape]);
        path.push(match field.get(escape + 1..escape + 4)? {
            b"040" => b' ',
            b"054" => b',',
            b"011" => b'\t',
            b"012" => b'\n',
            b"134" => b'\\',
            _ => return None,
        });
        field = &field[escape + 4..];
    }
    path.extend_from_slice(field);
    Some(OsString::from_vec(path).into())
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::{MEMORY_CAPACITY_UNAVAILABLE, MEMORY_HIERARCHY_UNAVAILABLE, cgroup_memory_limit};
    use std::path::Path;

    use std::io::{Error, ErrorKind};

    #[test]
    fn v2_limit_uses_decoded_mount_and_visible_ancestors() {
        let membership = br"0::/group name\leaf/child";
        let mountinfo = br"19 1 0:1 /group\040name\134leaf/child /local rw - cgroup2 cgroup rw
20 1 0:1 / /mnt/my\040cgroup\134v2 rw - cgroup2 cgroup rw";
        let read = |delegated| {
            move |path: &Path| match path.to_str().unwrap() {
                r"/mnt/my cgroup\v2/cgroup.subtree_control" => Ok("cpu memory\n".into()),
                r"/mnt/my cgroup\v2/group name\leaf/memory.max" => Ok("1073741824\n".into()),
                r"/mnt/my cgroup\v2/group name\leaf/cgroup.subtree_control" => {
                    Ok(if delegated { "memory\n" } else { "\n" }.into())
                }
                r"/mnt/my cgroup\v2/group name\leaf/child/memory.max" if delegated => {
                    Ok("2147483648\n".into())
                }
                _ => Err(Error::from(ErrorKind::NotFound)),
            }
        };
        assert_eq!(
            cgroup_memory_limit(membership, mountinfo, read(true)),
            Ok(Some(1024 * 1024 * 1024))
        );
        // A child without a delegated controller still inherits its parent's limit.
        assert_eq!(
            cgroup_memory_limit(membership, mountinfo, read(false)),
            Ok(Some(1024 * 1024 * 1024))
        );
    }

    #[test]
    fn v1_subtree_bind_uses_effective_parent_limit() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        std::fs::write(root.join("memory.limit_in_bytes"), "9223372036854771712\n").unwrap();
        let stat = root.join("memory.stat");
        std::fs::write(&stat, "rss 0\nhierarchical_memory_limit 2147483648\n").unwrap();
        let mounts = format!(
            "20 1 0:1 /tenant\\040name/gateway {} rw - cgroup cgroup rw,memory",
            root.display()
        );
        let membership = b"5:memory:/tenant name/gateway";
        let read = |path: &Path| std::fs::read_to_string(path);
        assert_eq!(
            cgroup_memory_limit(membership, mounts.as_bytes(), read),
            Ok(Some(2 * 1024 * 1024 * 1024))
        );
        std::fs::remove_file(stat).unwrap();
        assert_eq!(
            cgroup_memory_limit(membership, mounts.as_bytes(), read),
            Err(MEMORY_CAPACITY_UNAVAILABLE)
        );
    }

    #[test]
    fn v2_bind_and_private_namespace_hide_ancestors() {
        let subtree = b"20 1 0:1 /tenant/gateway /cg rw - cgroup2 cgroup rw";
        assert_eq!(
            cgroup_memory_limit(b"0::/tenant/gateway", subtree, |_| Ok("max".into())),
            Err(MEMORY_HIERARCHY_UNAVAILABLE)
        );
        let root = b"20 1 0:1 / /cg rw - cgroup2 cgroup rw";
        assert_eq!(
            cgroup_memory_limit(b"0::/", root, |path| match path.to_str().unwrap() {
                "/cg/cgroup.events" => Ok("populated 1\n".into()),
                "/cg/memory.max" => Ok("2147483648\n".into()),
                _ => Err(Error::from(ErrorKind::NotFound)),
            }),
            Err(MEMORY_HIERARCHY_UNAVAILABLE)
        );
    }

    #[test]
    fn v2_unreadable_parent_is_distinct_from_unlimited() {
        let root = b"20 1 0:1 / /cg rw - cgroup2 cgroup rw";
        assert_eq!(
            cgroup_memory_limit(b"0::/tenant/gateway", root, |path| {
                match path.to_str().unwrap() {
                    "/cg/cgroup.subtree_control" => Ok("memory\n".into()),
                    "/cg/tenant/memory.max" => Err(Error::from(ErrorKind::PermissionDenied)),
                    "/cg/tenant/gateway/memory.max" => Ok("max\n".into()),
                    _ => Err(Error::from(ErrorKind::NotFound)),
                }
            }),
            Err(MEMORY_HIERARCHY_UNAVAILABLE)
        );
        assert_eq!(
            cgroup_memory_limit(b"0::/gateway", root, |path| match path.to_str().unwrap() {
                "/cg/cgroup.subtree_control" => Ok("memory\n".into()),
                "/cg/gateway/memory.max" => Ok("max\n".into()),
                _ => Err(Error::from(ErrorKind::NotFound)),
            }),
            Ok(None)
        );
    }
}

fn vm_memory(registry: &str, charges: &[MemoryCharge]) -> u64 {
    use microsandbox_metrics::{MetricsRegistryReader, REGISTRY_ABI_VERSION};
    use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};

    if charges.iter().all(|charge| charge.ready.is_none()) {
        return charges
            .iter()
            .fold(0u64, |total, charge| total.saturating_add(charge.bytes));
    }
    let samples = MetricsRegistryReader::open(registry, REGISTRY_ABI_VERSION)
        .and_then(|reader| reader.active_snapshot())
        .unwrap_or_default();
    let now = chrono::Utc::now();
    let selected: Vec<_> = charges
        .iter()
        .map(|charge| {
            let (name, id) = charge.ready.as_ref()?;
            samples
                .iter()
                .filter(|sample| {
                    sample.sandbox_id == *id
                        && sample.name == *name
                        && sample.pid > 0
                        && now
                            .signed_duration_since(sample.timestamp)
                            .num_milliseconds()
                            .abs()
                            < 5_000
                })
                .max_by_key(|sample| sample.timestamp)
        })
        .collect();
    let pids: Vec<_> = selected
        .iter()
        .flatten()
        .map(|sample| Pid::from_u32(sample.pid as u32))
        .collect();
    let mut system = System::new();
    system.refresh_processes_specifics(
        ProcessesToUpdate::Some(&pids),
        ProcessRefreshKind::new().with_memory(),
    );

    charges
        .iter()
        .zip(selected)
        .fold(0u64, |total, (charge, sample)| {
            let footprint = sample.and_then(|sample| {
                let process = system.process(Pid::from_u32(sample.pid as u32))?;
                let sampled_at = u64::try_from(sample.timestamp.timestamp()).ok()?;
                if process.start_time() > sampled_at {
                    return None;
                }
                let bytes = process_footprint(sample.pid, process)?;
                Some(bytes.max(sample.memory_host_resident_bytes.unwrap_or(0)))
            });
            total.saturating_add(footprint.unwrap_or(charge.bytes))
        })
}

#[cfg(target_os = "macos")]
fn process_footprint(pid: i32, _: &sysinfo::Process) -> Option<u64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage_info_v2>::uninit();
    // SAFETY: the output buffer matches the selected RUSAGE_INFO_V2 layout.
    let result =
        unsafe { libc::proc_pid_rusage(pid, libc::RUSAGE_INFO_V2, usage.as_mut_ptr().cast()) };
    if result != 0 {
        return None;
    }
    // SAFETY: the kernel initialized the output on success.
    let bytes = unsafe { usage.assume_init() }.ri_phys_footprint;
    (bytes != 0).then_some(bytes)
}

#[cfg(not(target_os = "macos"))]
fn process_footprint(_: i32, process: &sysinfo::Process) -> Option<u64> {
    (process.memory() != 0).then_some(process.memory())
}
