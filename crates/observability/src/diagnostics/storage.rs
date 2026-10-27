use anyhow::{Result, bail, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime},
};
pub(super) const CAPTURE_BYTES: u64 = 128 * 1024 * 1024;
// Existing generic JSON/readers/API retain their 8 MiB bound. Reserve its
// full amount for SQL manifest: 1024 worst-case filenames (512 UTF-8 bytes,
// <=6x escaping), hashes/u64/pretty punctuation use <3.4 MiB; the remainder
// also admits existing build metadata, including the unconfigured fallback.
pub(super) const JSON_BYTES: u64 = 8 * 1024 * 1024;
pub(super) const SQL_MANIFEST_BYTES: u64 = JSON_BYTES;
// Shared run/native provenance is a subset of the <=8 MiB manifest. SQL-only
// coverage/limits/limitations/counters/envelope need <64 KiB. This does not
// assume every build went through configure_build's smaller 16 KiB cap.
const SQL_METADATA_BYTES: u64 = SQL_MANIFEST_BYTES + 64 * 1024;
// Full collector contract: max-width counters and <=6x key-text escaping.
pub(super) const SQL_CPU_BYTES: u64 = (crate::query_cpu::MAX_GROUPS * 512
    + crate::query_cpu::MAX_GROUP_KEY_BYTES * 6) as u64
    + SQL_METADATA_BYTES;
pub(super) const SQL_INVENTORY_BYTES: u64 = CAPTURE_BYTES - SQL_MANIFEST_BYTES;
pub(super) const INVENTORY_BYTES: u64 = CAPTURE_BYTES - 1024 * 1024;
pub(super) const MAX_FILES: usize = 1024;
pub(super) const STORE_BYTES: u64 = 512 * 1024 * 1024;
pub(super) const STORE_COUNT: usize = 16;
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ArtifactFile {
    pub filename: String,
    pub sha256: String,
    pub bytes: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ArtifactReference {
    pub capture_id: String,
    pub manifest_sha256: String,
    pub local_reference: String,
    pub bytes: u64,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub coverage: String,
    #[serde(default)]
    pub profile_data_present: bool,
}
pub(super) fn private_root(path: &Path) -> Result<PathBuf> {
    match fs::create_dir(path) {
        Ok(()) => {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e.into()),
    }
    validate_private(path)?;
    Ok(path.to_owned())
}
pub(super) fn validate_private(path: &Path) -> Result<()> {
    let meta = fs::symlink_metadata(path)?;
    ensure!(
        meta.is_dir() && !meta.file_type().is_symlink(),
        "diagnostic directory must be an owned directory, not a symlink"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        ensure!(
            meta.uid() == unsafe { libc::geteuid() } && meta.mode() & 0o077 == 0,
            "diagnostic directory must be private to current user"
        );
    }
    Ok(())
}
pub(super) fn validate_private_file(path: &Path) -> Result<()> {
    let meta = fs::symlink_metadata(path)?;
    ensure!(
        meta.is_file() && !meta.file_type().is_symlink(),
        "expected private regular file"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        ensure!(
            meta.uid() == unsafe { libc::geteuid() } && meta.mode() & 0o077 == 0,
            "diagnostic file must be private"
        );
    }
    Ok(())
}
pub(super) fn write_new_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(value)?;
    ensure!(bytes.len() as u64 <= JSON_BYTES, "manifest overflow");
    write_new_bytes(path, &bytes)
}
// Serialize directly from the typed sidecar, without a Value copy of every row.
// A capped writer bounds the temporary buffer even if a future schema grows.
pub(super) fn write_new_sql_cpu_json(path: &Path, value: &impl Serialize) -> Result<u64> {
    struct Bounded(Vec<u8>);
    impl Write for Bounded {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > SQL_CPU_BYTES as usize - self.0.len() {
                return Err(std::io::Error::other("SQL CPU JSON overflow"));
            }
            self.0.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut buffer = Bounded(Vec::with_capacity(SQL_CPU_BYTES as usize));
    serde_json::to_writer(&mut buffer, value)?;
    write_new_bytes(path, &buffer.0)?;
    Ok(buffer.0.len() as u64)
}
fn write_new_bytes(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    if let Err(error) = file.write_all(bytes) {
        drop(file);
        let _ = fs::remove_file(path); // create_new proved this file belongs to this write
        return Err(error.into());
    }
    Ok(())
}
pub(super) fn read_bounded(path: &Path, limit: u64) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    fs::File::open(path)?
        .take(limit + 1)
        .read_to_end(&mut bytes)?;
    ensure!(bytes.len() as u64 <= limit, "file overflow");
    Ok(bytes)
}
pub(super) enum LockKind {
    Storage,
    Preparation,
}
pub(super) fn owned_lock(root: &Path, kind: LockKind) -> Result<fs::File> {
    let (name, owner) = match kind {
        LockKind::Storage => ("capture-storage.lock", "pioneer-capture-storage"),
        LockKind::Preparation => ("memory-preparation.lock", "pioneer-memory-preparation"),
    };
    let path = root.join(name);
    match write_new_json(
        &path,
        &serde_json::json!({"owner":owner,"schema_version":1}),
    ) {
        Ok(()) => {}
        Err(error) if path.exists() => {
            let _ = error;
        }
        Err(error) => return Err(error),
    }
    validate_private_file(&path)?;
    let value: serde_json::Value = serde_json::from_slice(&read_bounded(&path, 2048)?)?;
    ensure!(
        value["owner"] == owner && value["schema_version"] == 1,
        "foreign storage lock preserved"
    );
    let file = fs::OpenOptions::new().read(true).write(true).open(&path)?;
    file.try_lock()
        .map_err(|_| anyhow::anyhow!("capture_storage_busy"))?;
    Ok(file)
}
pub(super) fn capture_dir(root: &Path, id: &str) -> Result<PathBuf> {
    let _storage_lock = owned_lock(root, LockKind::Storage)?;
    let start_identity = crate::resources::process_start_identity(std::process::id())?;
    let captures = private_root(&root.join("captures"))?;
    cleanup(&captures, CAPTURE_BYTES)?;
    create_owned_dir(&captures, id, &start_identity)
}
pub(super) fn preparation_dir(root: &Path, id: &str) -> Result<PathBuf> {
    // Caller holds the preparation lock for the complete owned-child lifetime.
    // Separate namespace/allowance: never charge temporary malloc logs against
    // retained captures, or let their eviction remove a live preparation.
    let preparations = private_root(&root.join("preparations"))?;
    ensure!(
        fs::read_dir(&preparations)?.next().transpose()?.is_none(),
        "previous preparation logs remain; preserved because child exit is unknown"
    );
    // One directory even across launcher crashes. Refuse leftovers rather than
    // deleting logs whose child may have survived its launcher; foreign files stay.
    let start_identity = crate::resources::process_start_identity(std::process::id())?;
    create_owned_dir(&preparations, id, &start_identity)
}
fn create_owned_dir(parent: &Path, id: &str, start_identity: &str) -> Result<PathBuf> {
    let path = parent.join(format!("pioneer-capture-{id}"));
    // Exclusive creation proves rollback never removes a pre-existing/foreign path.
    fs::create_dir(&path)?;
    let prepared = (|| -> Result<()> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o700))?;
        }
        validate_private(&path)?;
        write_new_json(
            &path.join("owner.json"),
            &serde_json::json!({"schema_version":1,"owner":"pioneer-observability","capture_id":id}),
        )?;
        write_new_json(
            &path.join("active.json"),
            &serde_json::json!({"pid":std::process::id(),"start_identity":start_identity}),
        )?;
        Ok(())
    })();
    if let Err(error) = prepared {
        let _ = fs::remove_dir_all(&path);
        return Err(error);
    }
    Ok(path)
}
pub(super) struct CaptureLease(PathBuf);
impl CaptureLease {
    pub(super) fn new(path: &Path) -> Self {
        Self(path.to_owned())
    }
}
impl Drop for CaptureLease {
    fn drop(&mut self) {
        let _ = fs::remove_file(self.0.join("active.json"));
    }
}
fn active_owner(path: &Path) -> bool {
    let active = path.join("active.json");
    match fs::symlink_metadata(&active) {
        Ok(meta) if meta.is_file() && !meta.file_type().is_symlink() => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return false,
        _ => return true, // denied, unknown or non-regular ownership stays reserved
    }
    let Some(value) = read_bounded(&active, 2048)
        .ok()
        .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok())
    else {
        return true;
    };
    let Some((pid, start)) = value["pid"].as_u64().zip(value["start_identity"].as_str()) else {
        return true;
    };
    let Ok(pid) = u32::try_from(pid) else {
        return true;
    };
    identity_holds_lease(start, crate::resources::process_start_identity(pid))
}
fn identity_holds_lease(start: &str, observed: std::io::Result<String>) -> bool {
    match observed {
        Ok(current) => current == start,
        Err(error) => error.kind() != std::io::ErrorKind::NotFound,
    }
}
fn walk(
    root: &Path,
    path: &Path,
    depth: usize,
    out: &mut Vec<(PathBuf, u64)>,
    bytes: &mut u64,
    limit: u64,
    entries: &mut usize,
    deadline: Instant,
    max_files: usize,
) -> Result<()> {
    ensure!(Instant::now() < deadline, "artifact directory deadline");
    ensure!(depth <= 12, "artifact depth overflow");
    for entry in fs::read_dir(path)? {
        ensure!(Instant::now() < deadline, "artifact directory deadline");
        let entry = entry?;
        *entries += 1;
        ensure!(
            // Reserve the sidecar directory entry as well as its file slot.
            *entries <= MAX_FILES + max_files,
            "artifact directory entry overflow"
        );
        let meta = fs::symlink_metadata(entry.path())?;
        ensure!(!meta.file_type().is_symlink(), "artifact symlink rejected");
        if meta.is_dir() {
            walk(
                root,
                &entry.path(),
                depth + 1,
                out,
                bytes,
                limit,
                entries,
                deadline,
                max_files,
            )?;
        } else if meta.is_file() {
            *bytes = bytes.saturating_add(meta.len());
            ensure!(*bytes <= limit, "artifact byte overflow");
            ensure!(out.len() < max_files, "artifact count overflow");
            let relative = entry.path().strip_prefix(root)?.to_owned();
            ensure!(
                relative.to_string_lossy().len() <= 512,
                "artifact filename overflow"
            );
            out.push((relative, meta.len()));
        } else {
            bail!("artifact nonregular file rejected");
        }
    }
    Ok(())
}
pub(super) fn size(root: &Path, limit: u64) -> Result<u64> {
    size_with_file_limit(root, limit, MAX_FILES)
}
pub(super) fn size_with_file_limit(root: &Path, limit: u64, max_files: usize) -> Result<u64> {
    let mut out = Vec::new();
    let mut bytes = 0;
    walk(
        root,
        root,
        0,
        &mut out,
        &mut bytes,
        limit,
        &mut 0,
        Instant::now() + Duration::from_secs(2),
        max_files,
    )?;
    Ok(bytes)
}
pub(super) fn inventory(root: &Path) -> Result<Vec<ArtifactFile>> {
    inventory_with_limits(root, INVENTORY_BYTES, MAX_FILES)
}
pub(super) fn inventory_with_limits(
    root: &Path,
    limit: u64,
    max_files: usize,
) -> Result<Vec<ArtifactFile>> {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut files = Vec::new();
    let mut bytes = 0;
    walk(
        root, root, 0, &mut files, &mut bytes, limit, &mut 0, deadline, max_files,
    )?;
    let mut result = Vec::with_capacity(files.len());
    for (relative, len) in files {
        if relative == Path::new("manifest.json") || relative == Path::new("active.json") {
            continue;
        }
        let mut hash = Sha256::new();
        let mut file = fs::File::open(root.join(&relative))?;
        let mut buffer = [0u8; 64 * 1024];
        let mut read = 0;
        loop {
            ensure!(Instant::now() < deadline, "artifact checksum deadline");
            let n = file.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            read += n as u64;
            ensure!(read <= len, "artifact changed while checksumming");
            hash.update(&buffer[..n]);
        }
        ensure!(read == len, "artifact changed while checksumming");
        result.push(ArtifactFile {
            filename: relative.to_string_lossy().into_owned(),
            sha256: hex::encode(hash.finalize()),
            bytes: len,
        });
    }
    result.sort_by(|a, b| a.filename.cmp(&b.filename));
    Ok(result)
}
pub(super) fn is_owned(path: &Path) -> bool {
    path.file_name()
        .and_then(|s| s.to_str())
        .is_some_and(|s| s.starts_with("pioneer-capture-") && s.len() == 40)
        && validate_private(path).is_ok()
        && read_bounded(&path.join("owner.json"), 2048)
            .ok()
            .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok())
            .is_some_and(|v| v["owner"] == "pioneer-observability" && v["schema_version"] == 1)
}
pub(super) fn cleanup(root: &Path, reserve: u64) -> Result<()> {
    // Only marked, private, correctly named capture directories are candidates.
    // Never remove arbitrary files, even when the global budget cannot be met.
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut entries = Vec::new();
    for (index, entry) in fs::read_dir(root)?.take(1025).enumerate() {
        ensure!(Instant::now() < deadline, "capture cleanup deadline");
        ensure!(
            index < 1024,
            "capture root entry count overflow; foreign entries preserved"
        );
        let path = entry?.path();
        if is_owned(&path) {
            let meta = fs::metadata(&path)?;
            let modified = meta.modified()?;
            let bytes = size(&path, STORE_BYTES)?;
            let active = active_owner(&path);
            // Reserve the FULL allowed future size, not just the few marker
            // bytes present at admission. Unknown owners retain the reservation.
            let charged = if active {
                bytes.max(CAPTURE_BYTES)
            } else {
                bytes
            };
            entries.push((path, modified, charged, active));
        }
    }
    ensure!(entries.len() <= 256, "capture directory count overflow");
    entries.sort_by_key(|e| e.1);
    let mut total: u64 = entries.iter().map(|e| e.2).sum();
    let mut count = entries.len();
    for (path, modified, bytes, active) in entries {
        ensure!(Instant::now() < deadline, "capture cleanup deadline");
        let expired = SystemTime::now()
            .duration_since(modified)
            .unwrap_or_default()
            > Duration::from_secs(7 * 86400);
        if !active
            && (expired || count >= STORE_COUNT || total.saturating_add(reserve) > STORE_BYTES)
        {
            fs::remove_dir_all(path)?;
            total = total.saturating_sub(bytes);
            count -= 1;
        }
    }
    ensure!(
        total.saturating_add(reserve) <= STORE_BYTES && count < STORE_COUNT,
        "capture storage budget exhausted"
    );
    Ok(())
}
// The caller already owns an admitted capture and its live CaptureLease, and
// has stopped/reaped the native tool. Keep the root and markers continuously
// visible to concurrent store admission; this is cleanup, never re-admission.
pub(super) fn clear_owned_capture_output(path: &Path) -> Result<()> {
    validate_private(path)?;
    validate_private_file(&path.join("owner.json"))?;
    validate_private_file(&path.join("active.json"))?;
    ensure!(
        is_owned(path) && active_owner(path),
        "expected active owned capture"
    );
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let name = entry.file_name();
        if name == "owner.json" || name == "active.json" {
            continue;
        }
        let child = entry.path();
        // Inspect the entry itself: unlink symlinks, never their targets.
        // remove_dir_all also does not follow symlinks inside a native subtree.
        if fs::symlink_metadata(&child)?.is_dir() {
            fs::remove_dir_all(child)?;
        } else {
            fs::remove_file(child)?;
        }
    }
    Ok(())
}
pub(super) fn remove_owned(path: &Path) {
    if is_owned(path) {
        let _ = fs::remove_dir_all(path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn expired_walk_deadline_is_reported_before_opening_any_path() {
        let missing = Path::new("/absent-proposal74-artifact-fixture");
        let error = walk(
            missing,
            missing,
            0,
            &mut Vec::new(),
            &mut 0,
            64,
            &mut 0,
            Instant::now() - Duration::from_millis(1),
            MAX_FILES,
        )
        .unwrap_err();
        assert_eq!(error.to_string(), "artifact directory deadline");
    }
    #[test]
    fn inventory_checksums_and_foreign_cleanup() {
        let temp = tempfile::tempdir().unwrap();
        let root = private_root(&temp.path().join("diagnostics")).unwrap();
        let capture = capture_dir(&root, "abcdefghijklmnopqrstuvwx").unwrap();
        fs::write(capture.join("data.txt"), b"abc").unwrap();
        let files = inventory(&capture).unwrap();
        let artifact = files.iter().find(|f| f.filename == "data.txt").unwrap();
        assert_eq!(artifact.bytes, 3);
        assert_eq!(
            artifact.sha256,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        let foreign = root.join("captures/foreign");
        fs::create_dir(&foreign).unwrap();
        fs::write(foreign.join("keep"), b"keep").unwrap();
        cleanup(&root.join("captures"), CAPTURE_BYTES).unwrap();
        assert!(foreign.join("keep").exists());
    }
    #[cfg(unix)]
    #[test]
    fn symlinks_are_never_exported_or_cleaned_as_files() {
        let temp = tempfile::tempdir().unwrap();
        let root = private_root(&temp.path().join("d")).unwrap();
        let outside = temp.path().join("foreign");
        fs::write(&outside, b"sensitive").unwrap();
        std::os::unix::fs::symlink(&outside, root.join("link")).unwrap();
        assert!(inventory(&root).is_err());
        assert!(outside.exists());
    }
    #[test]
    fn collision_does_not_replace_or_rollback_an_existing_capture() {
        let temp = tempfile::tempdir().unwrap();
        let root = private_root(&temp.path().join("diagnostics")).unwrap();
        let id = "abcdefghijklmnopqrstuvwx";
        let first = capture_dir(&root, id).unwrap();
        fs::write(first.join("keep"), b"original").unwrap();
        assert!(capture_dir(&root, id).is_err());
        assert_eq!(fs::read(first.join("keep")).unwrap(), b"original");
        let file = first.join("existing.json");
        fs::write(&file, b"foreign-original").unwrap();
        assert!(write_new_json(&file, &serde_json::json!({"new":true})).is_err());
        assert_eq!(fs::read(file).unwrap(), b"foreign-original");
    }
    #[test]
    fn oversized_json_and_reads_are_rejected_before_replacing_files() {
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("large");
        fs::write(&file, [0u8; 64]).unwrap();
        assert!(read_bounded(&file, 32).is_err());
        let oversized = "x".repeat(SQL_CPU_BYTES as usize);
        let sidecar = temp.path().join("sql-cpu.json");
        assert!(write_new_sql_cpu_json(&sidecar, &oversized).is_err());
        assert!(!sidecar.exists());
        fs::write(&sidecar, b"foreign").unwrap();
        assert!(write_new_sql_cpu_json(&sidecar, &serde_json::json!({})).is_err());
        assert_eq!(fs::read(&sidecar).unwrap(), b"foreign");
    }
}

#[cfg(test)]
mod active_capture_tests {
    use super::*;
    #[test]
    fn unknown_owner_holds_reservation_but_gone_or_reused_does_not() {
        use std::io::{Error, ErrorKind};
        assert!(identity_holds_lease("same", Ok("same".into())));
        assert!(!identity_holds_lease("old", Ok("new".into())));
        assert!(!identity_holds_lease(
            "old",
            Err(Error::new(ErrorKind::NotFound, "normalized process gone"))
        ));
        assert!(identity_holds_lease(
            "old",
            Err(Error::new(ErrorKind::PermissionDenied, "denied"))
        ));
        assert!(identity_holds_lease("old", Err(Error::other("unknown"))));
    }
    #[test]
    fn active_future_growth_is_reserved_and_release_reclaims_allowance() {
        let temp = tempfile::tempdir().unwrap();
        let root = private_root(&temp.path().join("diagnostics")).unwrap();
        let mut paths = Vec::new();
        for _ in 0..4 {
            paths.push(capture_dir(&root, &nanoid::nanoid!(24)).unwrap());
        }
        assert!(capture_dir(&root, &nanoid::nanoid!(24)).is_err());
        // Sparse fixtures model permitted eventual size without native tools.
        for path in &paths {
            fs::File::create(path.join("profile.data"))
                .unwrap()
                .set_len(CAPTURE_BYTES - 1024 * 1024)
                .unwrap();
        }
        assert!(capture_dir(&root, &nanoid::nanoid!(24)).is_err());
        drop(CaptureLease::new(&paths[0]));
        let next = capture_dir(&root, &nanoid::nanoid!(24)).unwrap();
        assert!(next.exists()); // completed old capture can be retired, active ones cannot
        for path in &paths[1..] {
            assert!(path.exists());
        }
        // A verified stale PID-start is dead; malformed/unknown ownership is held.
        let marker = paths[1].join("active.json");
        fs::remove_file(&marker).unwrap();
        write_new_json(
            &marker,
            &serde_json::json!({"pid":std::process::id(),"start_identity":"old-incarnation"}),
        )
        .unwrap();
        assert!(!active_owner(&paths[1]));
        let recovered = capture_dir(&root, &nanoid::nanoid!(24)).unwrap();
        assert!(recovered.is_dir());
        assert!(!paths[1].exists());
        fs::write(paths[2].join("active.json"), b"{").unwrap();
        assert!(active_owner(&paths[2]));
        assert!(capture_dir(&root, &nanoid::nanoid!(24)).is_err());
    }
    #[cfg(unix)]
    #[test]
    fn non_regular_active_owner_is_preserved_as_unknown() {
        let temp = tempfile::tempdir().unwrap();
        let root = private_root(&temp.path().join("diagnostics")).unwrap();
        let path = capture_dir(&root, &nanoid::nanoid!(24)).unwrap();
        fs::remove_file(path.join("active.json")).unwrap();
        let foreign = temp.path().join("foreign");
        fs::write(&foreign, b"{}").unwrap();
        std::os::unix::fs::symlink(&foreign, path.join("active.json")).unwrap();
        assert!(active_owner(&path));
        assert_eq!(fs::read(&foreign).unwrap(), b"{}");
        fs::remove_file(path.join("active.json")).unwrap();
        fs::create_dir(path.join("active.json")).unwrap();
        assert!(active_owner(&path));
    }
    #[test]
    fn shared_cleanup_never_evicts_another_active_capture() {
        let temp = tempfile::tempdir().unwrap();
        let root = private_root(&temp.path().join("diagnostics")).unwrap();
        let active = capture_dir(&root, &nanoid::nanoid!(24)).unwrap();
        let lease = CaptureLease::new(&active);
        for _ in 0..20 {
            let path = capture_dir(&root, &nanoid::nanoid!(24)).unwrap();
            drop(CaptureLease::new(&path));
        }
        assert!(active.is_dir());
        assert!(active.join("active.json").is_file());
        assert_eq!(fs::read_dir(root.join("captures")).unwrap().count(), 16);
        assert!(
            inventory(&active)
                .unwrap()
                .iter()
                .all(|f| f.filename != "active.json")
        );
        drop(lease);
        assert!(!active.join("active.json").exists());
    }
}
