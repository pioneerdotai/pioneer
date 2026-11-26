//! Coordinate App writes against available filesystem capacity.
use anyhow::{Context, Result, bail};
#[cfg(not(all(target_os = "macos", target_arch = "x86_64")))]
use std::collections::BTreeSet;
use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, Mutex},
};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

pub(in crate::message) const MIB: u64 = 1024 * 1024;
#[cfg(not(all(target_os = "macos", target_arch = "x86_64")))]
pub(super) const ROOT_DISK_MIB: u32 = 1024;

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum PoolId {
    Device(u64),
    #[cfg(target_os = "linux")]
    Btrfs([u8; 16]),
    #[cfg(target_os = "macos")]
    Apfs(uuid::Uuid),
}
#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
type PoolId = u64;
#[cfg(windows)]
type PoolId = Vec<u16>;
#[cfg(not(any(unix, windows)))]
type PoolId = std::path::PathBuf;

#[derive(Debug)]
struct DiskCapacity {
    id: PoolId,
    total: u64,
    available: u64,
    block: u64,
}

impl DiskCapacity {
    fn for_path(path: &Path) -> Result<Self> {
        let existing = path
            .ancestors()
            .find_map(|ancestor| match ancestor.canonicalize() {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                result => Some(result),
            })
            .context("apps.disk_capacity_unavailable")??;
        let stats = fs4::statvfs(&existing)?;
        Ok(Self {
            // An unresolved shared filesystem cannot use a mount-local key:
            // that would omit existing reservations against its pool.
            id: pool_id(&existing).context("apps.disk_capacity_unavailable")?,
            total: stats.total_space(),
            available: stats.available_space(),
            block: stats.allocation_granularity(),
        })
    }
}

/// The SDK quota limits growth through the guest's passthrough mount. Deduct
/// existing logical bytes so recreating a VM does not reset that allowance.
/// Native stdio MCP receives this directory as a host APP_DATA path and can
/// write directly, including before the SDK takes its first-write baseline.
/// Enforcing a combined limit for both writers requires a host filesystem quota.
#[cfg(not(all(target_os = "macos", target_arch = "x86_64")))]
pub(super) fn data_write_allowance(path: &Path) -> Result<u32> {
    let ceiling = DiskCapacity::for_path(path)?.total / 20;
    let mut remaining = ceiling;
    let mut directories = vec![std::fs::read_dir(path)?];
    while let Some(entries) = directories.last_mut() {
        let Some(entry) = entries.next() else {
            directories.pop();
            continue;
        };
        let entry = entry?;
        let kind = entry.file_type()?;
        if kind.is_dir() {
            directories.push(std::fs::read_dir(entry.path())?);
        } else if kind.is_file() {
            remaining = remaining.saturating_sub(entry.metadata()?.len());
            if remaining < MIB {
                return Ok(0);
            }
        }
    }
    Ok(u32::try_from(remaining / MIB).unwrap_or(u32::MAX))
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
fn pool_id(path: &Path) -> Result<PoolId> {
    use std::os::unix::fs::MetadataExt;
    Ok(path.metadata()?.dev())
}

#[cfg(target_os = "linux")]
fn pool_id(path: &Path) -> Result<PoolId> {
    let mut path = path.to_owned();
    let mut stats = linux_statfs(&path)?;
    if stats.f_type as u32 == libc::OVERLAYFS_SUPER_MAGIC as u32 {
        let mounts = std::fs::read("/proc/self/mountinfo")?;
        let mut visited = BTreeSet::new();
        while stats.f_type as u32 == libc::OVERLAYFS_SUPER_MAGIC as u32 {
            let mount = linux_mount_id(&path)?;
            anyhow::ensure!(visited.insert(mount), "OverlayFS upperdir cycle");
            let upper =
                overlay_upperdir(&mounts, mount).context("OverlayFS upperdir unavailable")?;
            let upper = upper
                .canonicalize()
                .context("OverlayFS upperdir is not visible")?;
            let backing = linux_statfs(&upper)?;
            // Mountinfo preserves the mount-time option string. After a mount
            // move or in another namespace that path can name a different FS.
            // Require the backing identity reported by ovl_statfs as evidence.
            // An overlay-generated FSID has no public mapping back to its upper.
            let shared = linux_fsid(&stats);
            anyhow::ensure!(
                shared != [0; 2] && shared == linux_fsid(&backing),
                "cannot verify OverlayFS writable backing filesystem"
            );
            path = upper;
            stats = backing;
        }
    }

    if stats.f_type as u32 == libc::BTRFS_SUPER_MAGIC as u32 {
        btrfs_pool_id(&path)
    } else {
        use std::os::unix::fs::MetadataExt;
        Ok(PoolId::Device(path.metadata()?.dev()))
    }
}

#[cfg(target_os = "linux")]
fn linux_statfs(path: &Path) -> Result<libc::statfs> {
    use std::{ffi::CString, os::unix::ffi::OsStrExt};

    let name = CString::new(path.as_os_str().as_bytes())?;
    let mut stats = std::mem::MaybeUninit::<libc::statfs>::uninit();
    // SAFETY: name is a C string and stats points to writable output.
    if unsafe { libc::statfs(name.as_ptr(), stats.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: a successful statfs initialized the output.
    Ok(unsafe { stats.assume_init() })
}

#[cfg(target_os = "linux")]
fn linux_fsid(stats: &libc::statfs) -> [libc::c_int; 2] {
    // SAFETY: Linux fsid_t is exactly two initialized C int fields.
    unsafe { std::mem::transmute(stats.f_fsid) }
}

#[cfg(target_os = "linux")]
fn linux_mount_id(path: &Path) -> Result<u64> {
    use std::{ffi::CString, os::unix::ffi::OsStrExt};

    let name = CString::new(path.as_os_str().as_bytes())?;
    let mut info = std::mem::MaybeUninit::<libc::statx>::zeroed();
    // SAFETY: name is a C string and info points to writable output.
    if unsafe {
        libc::statx(
            libc::AT_FDCWD,
            name.as_ptr(),
            0,
            libc::STATX_MNT_ID,
            info.as_mut_ptr(),
        )
    } != 0
    {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: the buffer started zeroed and statx filled the reported fields.
    let info = unsafe { info.assume_init() };
    anyhow::ensure!(
        info.stx_mask & libc::STATX_MNT_ID != 0,
        "mount ID unavailable"
    );
    Ok(info.stx_mnt_id)
}

#[cfg(target_os = "linux")]
fn overlay_upperdir(mounts: &[u8], id: u64) -> Option<std::path::PathBuf> {
    let id = id.to_string();
    let line = mounts
        .split(|&byte| byte == b'\n')
        .find(|line| line.split(|&byte| byte == b' ').next() == Some(id.as_bytes()))?;
    let separator = line.windows(3).position(|bytes| bytes == b" - ")?;
    let mut fields = line[separator + 3..].split(|&byte| byte == b' ');
    if fields.next()? != b"overlay" {
        return None;
    }
    let encoded = fields
        .nth(1)?
        .split(|&byte| byte == b',')
        .find_map(|option| option.strip_prefix(b"upperdir="))?;
    let upper = super::host_capacity::mountinfo_path(encoded)?;
    upper.is_absolute().then_some(upper)
}

#[cfg(target_os = "linux")]
fn btrfs_pool_id(path: &Path) -> Result<PoolId> {
    use std::os::{fd::AsRawFd, unix::fs::OpenOptionsExt};

    // st_dev and statfs.f_fsid can vary by Btrfs subvolume. FS_INFO returns
    // the filesystem ID, shared by all subvolumes and member devices.
    let metadata = path.metadata()?;
    anyhow::ensure!(
        metadata.is_dir() || metadata.is_file(),
        "apps.disk_capacity_unavailable"
    );
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK | libc::O_NOFOLLOW)
        .open(path)?;
    // linux/btrfs.h: btrfs_ioctl_fs_info_args is 1024 bytes. The fsid
    // follows two u64 fields. Leave all optional input flags zero.
    #[repr(C, align(8))]
    struct FsInfo {
        max_id: u64,
        num_devices: u64,
        fsid: [u8; 16],
        rest: [u8; 992],
    }
    let mut info = FsInfo {
        max_id: 0,
        num_devices: 0,
        fsid: [0; 16],
        rest: [0; 992],
    };
    const BTRFS_IOC_FS_INFO: libc::Ioctl = libc::_IOR::<FsInfo>(0x94, 31);
    // SAFETY: the descriptor is live and info is the initialized UAPI buffer.
    if unsafe {
        libc::ioctl(
            file.as_raw_fd(),
            BTRFS_IOC_FS_INFO,
            &mut info as *mut FsInfo,
        )
    } != 0
    {
        return Err(std::io::Error::last_os_error().into());
    }
    anyhow::ensure!(info.fsid != [0; 16], "apps.disk_capacity_unavailable");
    Ok(PoolId::Btrfs(info.fsid))
}

#[cfg(target_os = "macos")]
fn pool_id(path: &Path) -> Result<PoolId> {
    use std::{
        ffi::{CStr, CString},
        os::unix::{ffi::OsStrExt, fs::MetadataExt},
    };
    let name = CString::new(path.as_os_str().as_bytes())?;
    let mut stats = std::mem::MaybeUninit::<libc::statfs>::uninit();
    // SAFETY: name is NUL-terminated and stats is writable for a statfs.
    if unsafe { libc::statfs(name.as_ptr(), stats.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: the successful syscall initializes stats and its type name.
    let stats = unsafe { stats.assume_init() };
    let filesystem = unsafe { CStr::from_ptr(stats.f_fstypename.as_ptr()) };
    if filesystem.to_bytes() == b"apfs" {
        // The source may be a snapshot name followed by @/dev/disk... .
        let source = unsafe { CStr::from_ptr(stats.f_mntfromname.as_ptr()) };
        let device = source
            .to_bytes()
            .rsplit(|&byte| byte == b'@')
            .next()
            .and_then(|source| source.strip_prefix(b"/dev/"))
            .context("apps.disk_capacity_unavailable")?;
        let device = CString::new(device)?;
        Ok(PoolId::Apfs(
            apfs::container_uuid(&device).context("apps.disk_capacity_unavailable")?,
        ))
    } else {
        Ok(PoolId::Device(path.metadata()?.dev()))
    }
}

#[cfg(target_os = "macos")]
mod apfs {
    use std::ffi::{CStr, c_char, c_void};

    type IoObject = libc::mach_port_t;
    type CfRef = *const c_void;

    #[link(name = "IOKit", kind = "framework")]
    unsafe extern "C" {
        fn IOBSDNameMatching(port: libc::mach_port_t, options: u32, name: *const c_char) -> CfRef;
        fn IOServiceGetMatchingService(port: libc::mach_port_t, matching: CfRef) -> IoObject;
        fn IORegistryEntryGetParentEntry(
            entry: IoObject,
            plane: *const c_char,
            parent: *mut IoObject,
        ) -> libc::kern_return_t;
        fn IOObjectConformsTo(object: IoObject, class_name: *const c_char) -> libc::boolean_t;
        fn IOObjectRelease(object: IoObject) -> libc::kern_return_t;
        fn IORegistryEntryCreateCFProperty(
            entry: IoObject,
            key: CfRef,
            allocator: CfRef,
            options: u32,
        ) -> CfRef;
    }

    #[link(name = "CoreFoundation", kind = "framework")]
    unsafe extern "C" {
        fn CFStringCreateWithCString(
            allocator: CfRef,
            value: *const c_char,
            encoding: u32,
        ) -> CfRef;
        fn CFGetTypeID(value: CfRef) -> usize;
        fn CFStringGetTypeID() -> usize;
        fn CFStringGetCString(value: CfRef, buffer: *mut c_char, size: isize, encoding: u32) -> u8;
        fn CFRelease(value: CfRef);
    }

    struct RegistryEntry(IoObject);
    impl RegistryEntry {
        fn parent(&self) -> Option<Self> {
            let mut parent = 0;
            // SAFETY: we own the entry and the output is writable.
            let result = unsafe {
                IORegistryEntryGetParentEntry(self.0, c"IOService".as_ptr(), &mut parent)
            };
            (result == 0 && parent != 0).then(|| Self(parent))
        }

        fn is_a(&self, class: &CStr) -> bool {
            // SAFETY: the owned entry and the class C string are live.
            unsafe { IOObjectConformsTo(self.0, class.as_ptr()) != 0 }
        }
    }
    impl Drop for RegistryEntry {
        fn drop(&mut self) {
            // SAFETY: this object owns one registry reference.
            unsafe { IOObjectRelease(self.0) };
        }
    }

    const UTF8: u32 = 0x0800_0100;

    pub(super) fn container_uuid(device: &CStr) -> Option<uuid::Uuid> {
        // SAFETY: the name is NUL-terminated; IOKit consumes the matching dictionary.
        let service = unsafe {
            let matching = IOBSDNameMatching(0, 0, device.as_ptr());
            if matching.is_null() {
                return None;
            }
            IOServiceGetMatchingService(0, matching)
        };
        if service == 0 {
            return None;
        }

        let mut entry = RegistryEntry(service);
        // Volumes (and snapshots) are below the container partition scheme.
        // The scheme's IOMedia parent is the synthesized whole disk whose
        // UUID identifies the container even if the BSD names change.
        while !entry.is_a(c"AppleAPFSContainer") {
            entry = entry.parent()?;
        }
        let media = entry.parent()?;
        if !media.is_a(c"IOMedia") || !media.parent()?.is_a(c"AppleAPFSContainerScheme") {
            return None;
        }

        // SAFETY: all CF calls use owned references, default allocators, and
        // a writable buffer. Check the property type before using it as a string.
        unsafe {
            let key = CFStringCreateWithCString(std::ptr::null(), c"UUID".as_ptr(), UTF8);
            if key.is_null() {
                return None;
            }
            let value = IORegistryEntryCreateCFProperty(media.0, key, std::ptr::null(), 0);
            CFRelease(key);
            if value.is_null() {
                return None;
            }
            let mut buf = [0; 37];
            let valid = CFGetTypeID(value) == CFStringGetTypeID()
                && CFStringGetCString(value, buf.as_mut_ptr(), buf.len() as isize, UTF8) != 0;
            CFRelease(value);
            if !valid {
                return None;
            }
            let id = uuid::Uuid::parse_str(CStr::from_ptr(buf.as_ptr()).to_str().ok()?).ok()?;
            (!id.is_nil()).then_some(id)
        }
    }
}

#[cfg(windows)]
fn pool_id(path: &Path) -> Result<PoolId> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{
        GetVolumeNameForVolumeMountPointW, GetVolumePathNameW,
    };

    let path: Vec<_> = path.as_os_str().encode_wide().chain(Some(0)).collect();
    let mut mount = vec![0; 32_768];
    let mut volume = vec![0; 64];
    // SAFETY: input is NUL-terminated and both output lengths match their buffers.
    let found = unsafe {
        GetVolumePathNameW(path.as_ptr(), mount.as_mut_ptr(), mount.len() as u32) != 0
            && GetVolumeNameForVolumeMountPointW(
                mount.as_ptr(),
                volume.as_mut_ptr(),
                volume.len() as u32,
            ) != 0
    };
    anyhow::ensure!(found, std::io::Error::last_os_error());
    volume.truncate(
        volume
            .iter()
            .position(|&value| value == 0)
            .unwrap_or(volume.len()),
    );
    Ok(volume)
}

#[cfg(not(any(unix, windows)))]
fn pool_id(path: &Path) -> Result<PoolId> {
    Ok(path.to_owned())
}

#[derive(Default)]
pub(in crate::message) struct AppStorage {
    reservations: Arc<Mutex<BTreeMap<PoolId, u64>>>,
    install: Arc<AsyncMutex<()>>,
    recovery: Mutex<RecoveryFence>,
}

#[derive(Default)]
enum RecoveryFence {
    #[default]
    Complete,
    #[cfg(not(all(target_os = "macos", target_arch = "x86_64")))]
    Unknown,
    #[cfg(not(all(target_os = "macos", target_arch = "x86_64")))]
    Pools(BTreeSet<PoolId>),
}

impl RecoveryFence {
    fn blocks(&self, _pool: &PoolId) -> bool {
        match self {
            Self::Complete => false,
            #[cfg(not(all(target_os = "macos", target_arch = "x86_64")))]
            Self::Unknown => true,
            #[cfg(not(all(target_os = "macos", target_arch = "x86_64")))]
            Self::Pools(pools) => pools.contains(_pool),
        }
    }
}

pub(in crate::message) struct DiskReservation {
    ledger: Arc<Mutex<BTreeMap<PoolId, u64>>>,
    writes: BTreeMap<PoolId, u64>,
}

impl Drop for DiskReservation {
    fn drop(&mut self) {
        let mut ledger = self
            .ledger
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        for (volume, bytes) in &self.writes {
            let remaining = ledger.get_mut(volume).expect("live disk reservation");
            *remaining -= bytes;
            if *remaining == 0 {
                ledger.remove(volume);
            }
        }
    }
}

pub(in crate::message) struct AppInstallAdmission {
    _install: OwnedMutexGuard<()>,
}

pub(in crate::message) struct AppInstallWrites {
    pub extraction: DiskReservation,
    pub publication: DiskReservation,
    pub admission: Arc<AppInstallAdmission>,
}

impl AppStorage {
    #[cfg(not(all(target_os = "macos", target_arch = "x86_64")))]
    pub(super) fn set_recovering(&self, recovering: bool) {
        let mut state = self
            .recovery
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        *state = if recovering {
            RecoveryFence::Unknown
        } else {
            RecoveryFence::Complete
        };
    }

    #[cfg(not(all(target_os = "macos", target_arch = "x86_64")))]
    pub(super) fn fence_recovery_paths(&self, paths: &[std::path::PathBuf]) -> Result<()> {
        let pools = paths
            .iter()
            .map(|path| DiskCapacity::for_path(path).map(|capacity| capacity.id))
            .collect::<Result<BTreeSet<_>>>()?;
        *self
            .recovery
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = RecoveryFence::Pools(pools);
        Ok(())
    }

    pub(in crate::message) fn error_code(error: &anyhow::Error) -> &'static str {
        match error.to_string().as_str() {
            "apps.install_busy" => "apps.install_busy",
            code => Self::capacity_error(code).unwrap_or("apps.disk_capacity_unavailable"),
        }
    }

    pub(in crate::message) fn capacity_error(code: &str) -> Option<&'static str> {
        match code {
            "apps.disk_capacity" => Some("apps.disk_capacity"),
            "apps.storage_recovering" => Some("apps.storage_recovering"),
            "apps.disk_capacity_unavailable" => Some("apps.disk_capacity_unavailable"),
            _ => None,
        }
    }

    pub(in crate::message) fn install_should_retry(code: &str) -> bool {
        matches!(
            code,
            "apps.install_busy" | "apps.storage_recovering" | "apps.disk_capacity"
        )
    }

    /// Measure while holding the ledger so admission sees the currently active
    /// writers. Charge their entire allowance until they stop writing.
    pub(in crate::message) fn reserve(&self, writes: &[(&Path, u64)]) -> Result<DiskReservation> {
        let [reservation] = self.reserve_stages([writes], false)?;
        Ok(reservation)
    }

    // Check all stages together, including paths sharing a storage pool, but give
    // each writer independent ownership of its share until its work settles.
    fn reserve_stages<const N: usize>(
        &self,
        stages: [&[(&Path, u64)]; N],
        reclaim: bool,
    ) -> Result<[DiskReservation; N]> {
        let recovery = self
            .recovery
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let mut ledger = self
            .reservations
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let mut pools = BTreeMap::new();
        let mut staged: [BTreeMap<PoolId, u64>; N] = std::array::from_fn(|_| BTreeMap::new());
        for (stage, writes) in staged.iter_mut().zip(stages) {
            for &(path, bytes) in writes {
                let volume = DiskCapacity::for_path(path)?;
                anyhow::ensure!(!recovery.blocks(&volume.id), "apps.storage_recovering");
                let reserve = if reclaim {
                    0
                } else {
                    volume.total.div_ceil(10).max(2 * 1024 * MIB)
                };
                anyhow::ensure!(volume.available >= reserve, "apps.disk_capacity");
                let (capacity, planned) =
                    pools.entry(volume.id.clone()).or_insert((u64::MAX, 0u64));
                *capacity = (*capacity).min(volume.available - reserve);
                *planned = planned
                    .checked_add(bytes)
                    .context("apps.disk_capacity_unavailable")?;
                if bytes != 0 {
                    *stage.entry(volume.id).or_default() += bytes;
                }
            }
        }
        for (volume, &(capacity, planned)) in &pools {
            let reserved = ledger.get(volume).copied().unwrap_or_default();
            if reserved > capacity || planned > capacity - reserved {
                bail!("apps.disk_capacity");
            }
        }
        for (volume, (_, bytes)) in pools {
            if bytes != 0 {
                *ledger.entry(volume).or_default() += bytes;
            }
        }
        Ok(staged.map(|writes| DiskReservation {
            ledger: self.reservations.clone(),
            writes,
        }))
    }

    pub(in crate::message) fn reserve_archive(
        &self,
        path: &Path,
        bytes: u64,
    ) -> Result<DiskReservation> {
        self.reserve_file(path, bytes, false)
    }

    fn reserve_file(&self, directory: &Path, bytes: u64, reclaim: bool) -> Result<DiskReservation> {
        let overhead = DiskCapacity::for_path(directory)?.block.saturating_mul(2);
        let [reservation] =
            self.reserve_stages([&[(directory, bytes.saturating_add(overhead))]], reclaim)?;
        Ok(reservation)
    }

    // Some entry points already own a parent lock. A contended global gate
    // returns busy instead of waiting for an owner that might need that parent.
    fn try_install_lock(&self) -> Result<OwnedMutexGuard<()>> {
        self.install
            .clone()
            .try_lock_owned()
            .context("apps.install_busy")
    }

    pub(in crate::message) fn admit_continuation(&self) -> Result<Arc<AppInstallAdmission>> {
        Ok(Arc::new(AppInstallAdmission {
            _install: self.try_install_lock()?,
        }))
    }

    pub(in crate::message) fn reserve_skill_stage(
        &self,
        staging: &Path,
        native_byte_limit: usize,
        package_limits: pioneer_packages::Limits,
    ) -> Result<DiskReservation> {
        let capacity = DiskCapacity::for_path(staging).context("apps.disk_capacity_unavailable")?;
        let bytes = native_byte_limit.min(package_limits.expanded_bytes) as u64;
        let overhead = (package_limits.entries as u64 + 4)
            .saturating_mul(capacity.block)
            .saturating_mul(2);
        self.reserve(&[(staging, bytes.saturating_add(overhead))])
            .map_err(|error| anyhow::anyhow!(Self::error_code(&error)))
    }

    pub(in crate::message) fn reserve_skill_lock(
        &self,
        lock_path: &Path,
        bytes: usize,
        reclaim: bool,
    ) -> Result<DiskReservation> {
        // The atomic write allocates a sibling of the lock file. A durable
        // App removal can use the floor to finish removing native state
        // and reclaim package space. Live write reservations still count.
        let directory = lock_path
            .parent()
            .context("apps.disk_capacity_unavailable")?;
        self.reserve_file(directory, bytes as u64, reclaim)
            .map_err(|error| anyhow::anyhow!(Self::error_code(&error)))
    }

    pub(in crate::message) async fn admit_install(
        self: &Arc<Self>,
        archive_and_extraction: Option<(&Path, &Path)>,
        publication: &Path,
    ) -> Result<AppInstallWrites> {
        let admission = self.admit_continuation()?;
        let archive_and_extraction = archive_and_extraction
            .map(|(archive, extraction)| (archive.to_owned(), extraction.to_owned()));
        let publication = publication.to_owned();
        let storage = self.clone();
        super::super::package_delivery::prepare(move || {
            let limits = if archive_and_extraction.is_some() {
                pioneer_apps::PACKAGE_LIMITS
            } else {
                pioneer_apps::SOURCE_LIMITS
            };
            let allowance = |path: &Path| -> Result<u64> {
                let capacity = DiskCapacity::for_path(path)?;
                // Budget block rounding and directory entries as well as
                // payload. Actual metadata allocation is filesystem-specific.
                Ok((limits.expanded_bytes as u64).saturating_add(
                    (limits.entries as u64 + 2)
                        .saturating_mul(capacity.block)
                        .saturating_mul(2),
                ))
            };
            let publication = [(publication.as_path(), allowance(&publication)?)];
            let extraction = match &archive_and_extraction {
                // The finalized archive already occupies disk space.
                Some((archive, extraction)) => vec![
                    (archive.as_path(), 0),
                    (extraction.as_path(), allowance(extraction)?),
                ],
                None => Vec::new(),
            };
            let [publication, extraction] =
                storage.reserve_stages([&publication, &extraction], false)?;
            Ok(AppInstallWrites {
                extraction,
                publication,
                admission,
            })
        })
        .await?
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::overlay_upperdir;
    use std::path::Path;

    #[test]
    fn overlay_upperdir_uses_mount_id_and_decodes_option_value() {
        let mounts = br"10 1 0:10 / /merge rw - overlay overlay rw,upperdir=/back/unused
11 1 0:11 / /merge rw - overlay overlay rw,lowerdir=/lower,upperdir=/back\040dir\0541\134a,workdir=/work
12 1 0:12 / /other ro - overlay overlay ro,lowerdir=/lower
13 1 8:1 / /direct rw - ext4 /dev/sda1 rw";
        assert_eq!(
            overlay_upperdir(mounts, 11).as_deref(),
            Some(Path::new(r"/back dir,1\a"))
        );
        assert!(overlay_upperdir(mounts, 12).is_none());
        assert!(overlay_upperdir(mounts, 13).is_none());
    }
}
