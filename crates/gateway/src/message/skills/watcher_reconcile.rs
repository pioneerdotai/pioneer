//! Resumable root work. Streams are cursors owned and advanced by the one watcher
//! worker; they are not spawned tasks or a persistent job system.
use super::super::storage_relocation::{
    self as storage, ConfiguredRootImportConfig, ManagedRootScanConfig,
    PreparedSkillStorageMetadata, SkillStorageRelocationCandidate, SkillStorageRelocationOutcome,
};
use super::super::*;
use super::{JobFence, Mapping, Progress, cancellable_db, owned_fs};
use anyhow::Context;
use futures_util::{FutureExt, Stream, StreamExt};
use pioneer_skills::{
    MaterializedSkillPreparation, PrepareMaterializedSkillRequest, PreparedMaterializedSkill,
    canonical_skill_install_path, normalize_skill_slug,
};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, ReadDir};
use std::io::{Read, Write};
use std::pin::Pin;
use std::sync::Mutex as StdMutex;

const FILES: usize = 64;
const BYTES: usize = 256 * 1024;

#[derive(Default)]
pub(super) struct Baseline {
    managed: BTreeMap<(String, String, SkillId), (SkillInstallationRecord, Option<[u8; 32]>)>,
}

#[derive(Default)]
struct PackageFacts {
    physical: Option<std::result::Result<Arc<pioneer_skills::MaterializedSkillFacts>, String>>,
    hash: Option<std::result::Result<[u8; 32], String>>,
}
type SourceFacts = Arc<StdMutex<BTreeMap<PathBuf, PackageFacts>>>;

type ImportPaths =
    Arc<StdMutex<Option<BTreeMap<PathBuf, Option<pioneer_crud::SkillReconciliationSnapshot>>>>>;

type Work = Pin<Box<dyn Stream<Item = Result<Progress>> + Send>>;
pub(super) struct Attempt {
    pub(super) owner: PathBuf,
    // In-memory proof of an acknowledged commit. The disk marker remains
    // conservative if persisting the outcome fails; restart revalidates facts.
    pub(super) committed: bool,
}
impl Attempt {
    pub(super) fn new(owner: PathBuf) -> Self {
        Self {
            owner,
            committed: false,
        }
    }
}
pub(super) type Attempts = Arc<StdMutex<BTreeMap<PathBuf, Attempt>>>;

// A marker describes only the worker's temporary directory, not a job queue.
// Publishing means a backup may be the last recoverable copy; only an explicit
// success or completed compensation can turn that directory into garbage.
const ATTEMPT_MARKER: &str = "pioneer-skill-attempt.json";
#[derive(serde::Serialize, serde::Deserialize)]
pub(super) struct AttemptMarker {
    version: u32,
    token: String,
    pub(super) owner: PathBuf,
    pub(super) publishing: bool,
    #[serde(default)]
    pub(super) skill_id: Option<SkillId>,
}
pub(crate) fn new_attempt(parent: &Path, owner: PathBuf) -> Result<PathBuf> {
    let directory = tempfile::Builder::new()
        .prefix(".pioneer-relocation-")
        .tempdir_in(parent)?;
    let path = directory.path().to_path_buf();
    let marker = AttemptMarker {
        version: 1,
        token: path
            .file_name()
            .context("attempt name")?
            .to_string_lossy()
            .into_owned(),
        owner,
        publishing: false,
        skill_id: None,
    };
    write_attempt_marker(&path, &marker)?;
    Ok(directory.keep())
}
pub(super) fn attempt_marker(path: &Path) -> Result<Option<AttemptMarker>> {
    let file = path.join(ATTEMPT_MARKER);
    let metadata = match fs::symlink_metadata(&file) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
        Ok(metadata) => metadata,
    };
    // An exact, versioned marker is required. A similarly named directory or a
    // legitimate dot-package is never treated as our garbage.
    if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() > 16 * 1024 {
        return Ok(None);
    }
    let Ok(marker) = serde_json::from_slice::<AttemptMarker>(&fs::read(file)?) else {
        return Ok(None);
    };
    if marker.version != 1
        || path.file_name().and_then(|p| p.to_str()) != Some(marker.token.as_str())
    {
        return Ok(None);
    }
    Ok(Some(marker))
}
pub(crate) fn set_attempt_publishing(path: &Path, publishing: bool) -> Result<()> {
    let mut marker = attempt_marker(path)?.context("missing owned skill attempt marker")?;
    marker.publishing = publishing;
    write_attempt_marker(path, &marker)
}
pub(crate) fn set_attempt_skill(path: &Path, skill_id: SkillId) -> Result<()> {
    let mut marker = attempt_marker(path)?.context("missing owned skill attempt marker")?;
    marker.skill_id = Some(skill_id);
    write_attempt_marker(path, &marker)
}

fn write_attempt_marker(path: &Path, marker: &AttemptMarker) -> Result<()> {
    let mut file = tempfile::NamedTempFile::new_in(path)?;
    file.write_all(&serde_json::to_vec(marker)?)?;
    file.as_file().sync_all()?;
    file.persist(path.join(ATTEMPT_MARKER))
        .map_err(|error| error.error)?;
    #[cfg(unix)]
    File::open(path)?.sync_all()?;
    Ok(())
}

/// Depth is limited to the two existing configured package layouts. Entries,
/// including unrelated files, consume the quantum. The domain limit chooses the
/// first sorted packages only after complete enumeration, rather than truncating
/// an arbitrary technical page.
struct Discovery {
    directories: Vec<(ReadDir, usize)>,
    packages: BTreeSet<PathBuf>,
    limit: usize,
    attempts: Attempts,
    root: PathBuf,
}
impl Discovery {
    fn new(root: &Path, limit: usize, attempts: Attempts) -> Result<Self> {
        let directories = match fs::symlink_metadata(root) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(error.into()),
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {
                vec![(fs::read_dir(root)?, 0)]
            }
            Ok(_) => bail!("configured skill root is not a directory"),
        };
        Ok(Self {
            directories,
            packages: BTreeSet::new(),
            limit: limit.max(1),
            attempts,
            root: root.to_path_buf(),
        })
    }
    fn step(&mut self) -> Result<bool> {
        for _ in 0..FILES {
            let Some((entries, depth)) = self.directories.last_mut() else {
                return Ok(true);
            };
            let depth = *depth;
            let Some(entry) = entries.next() else {
                self.directories.pop();
                continue;
            };
            let entry = entry?;
            let path = entry.path();
            if self
                .attempts
                .lock()
                .expect("skills attempts lock")
                .contains_key(&path)
            {
                continue;
            }
            let metadata = fs::symlink_metadata(&path)?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                continue;
            }
            if let Some(_marker) = attempt_marker(&path)? {
                self.attempts
                    .lock()
                    .expect("skills attempts lock")
                    .insert(path, Attempt::new(self.root.clone()));
                continue;
            }
            if path.join("SKILL.md").is_file() {
                self.packages.insert(path);
                if self.packages.len() > self.limit {
                    self.packages.pop_last();
                }
            } else if depth == 0 {
                self.directories.push((fs::read_dir(path)?, 1));
            }
        }
        Ok(self.directories.is_empty())
    }
}

struct FileWork {
    input: File,
    output: Option<File>,
    digest: Sha256,
    bytes: u64,
    initial: fs::Metadata,
}

/// One order-independent digest over relative paths, directories and file
/// contents; metadata-only hashing would miss same-size editor atomic saves and
/// asset-only changes. Copy uses this same bounded traversal into an owned stage.
struct Tree {
    root: PathBuf,
    target: Option<PathBuf>,
    directories: Vec<ReadDir>,
    file: Option<FileWork>,
    digest: [u8; 32],
    count: u64,
    max_file_bytes: u64,
    allow_symlinks: bool,
    #[cfg(test)]
    source_signals: Option<Arc<super::Signals>>,
}
impl Tree {
    fn new(
        root: PathBuf,
        target: Option<PathBuf>,
        max_file_bytes: usize,
        allow_symlinks: bool,
    ) -> Result<Self> {
        let metadata = fs::symlink_metadata(&root)?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            bail!("skill package is not a real directory");
        }
        Ok(Self {
            directories: vec![fs::read_dir(&root)?],
            root,
            target,
            file: None,
            digest: [0; 32],
            count: 0,
            max_file_bytes: max_file_bytes.max(1) as u64,
            allow_symlinks,
            #[cfg(test)]
            source_signals: None,
        })
    }
    fn include(&mut self, hash: [u8; 32]) -> Result<()> {
        self.count = self
            .count
            .checked_add(1)
            .context("skill tree entry count exhausted")?;
        for (total, byte) in self.digest.iter_mut().zip(hash) {
            *total ^= byte;
        }
        Ok(())
    }
    fn fingerprint(&self) -> [u8; 32] {
        let mut digest = Sha256::new();
        digest.update(self.digest);
        digest.update(self.count.to_le_bytes());
        digest.finalize().into()
    }
    fn step(&mut self) -> Result<bool> {
        let mut remaining_bytes = BYTES;
        for _ in 0..FILES {
            if let Some(mut file) = self.file.take() {
                let mut buffer = vec![0; remaining_bytes.min(64 * 1024)];
                let read = file.input.read(&mut buffer)?;
                #[cfg(test)]
                if read != 0 {
                    if let Some(signals) = &self.source_signals {
                        signals
                            .source_reads
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
                if read == 0 {
                    let current = file.input.metadata()?;
                    if current.len() != file.initial.len()
                        || current.modified().ok() != file.initial.modified().ok()
                    {
                        bail!("skill file changed during preparation");
                    }
                    if let Some(output) = file.output.take() {
                        output.set_permissions(file.initial.permissions())?;
                    }
                    self.include(file.digest.finalize().into())?;
                } else {
                    file.bytes = file
                        .bytes
                        .checked_add(read as u64)
                        .context("skill file size exhausted")?;
                    if file.bytes > self.max_file_bytes {
                        bail!("skill file exceeds max_install_file_bytes");
                    }
                    file.digest.update(&buffer[..read]);
                    if let Some(output) = &mut file.output {
                        output.write_all(&buffer[..read])?;
                    }
                    self.file = Some(file);
                    remaining_bytes -= read;
                    if remaining_bytes == 0 {
                        return Ok(false);
                    }
                }
                continue;
            }
            let Some(entries) = self.directories.last_mut() else {
                return Ok(true);
            };
            let Some(entry) = entries.next() else {
                self.directories.pop();
                continue;
            };
            let entry = entry?;
            let path = entry.path();
            let relative = path.strip_prefix(&self.root)?;
            let metadata = fs::symlink_metadata(&path)?;
            let mut digest = Sha256::new();
            digest.update(relative.as_os_str().as_encoded_bytes());
            digest.update([0]);
            if metadata.file_type().is_symlink() {
                if !self.allow_symlinks || self.target.is_some() {
                    bail!("skill import does not follow symlinks");
                }
                digest.update(b"symlink\0");
                digest.update(fs::read_link(&path)?.as_os_str().as_encoded_bytes());
                self.include(digest.finalize().into())?;
            } else if metadata.is_dir() {
                digest.update(b"directory\0");
                self.include(digest.finalize().into())?;
                if let Some(target) = &self.target {
                    fs::create_dir(target.join(relative))?;
                }
                self.directories.push(fs::read_dir(path)?);
            } else if metadata.is_file() {
                if metadata.len() > self.max_file_bytes {
                    bail!("skill file exceeds max_install_file_bytes");
                }
                digest.update(b"file\0");
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    digest.update(metadata.permissions().mode().to_le_bytes());
                }
                #[cfg(not(unix))]
                {
                    digest.update([u8::from(metadata.permissions().readonly())]);
                }
                let mut options = fs::OpenOptions::new();
                options.read(true);
                #[cfg(unix)]
                {
                    use std::os::unix::fs::OpenOptionsExt;
                    options.custom_flags(libc::O_NOFOLLOW);
                }
                let input = options.open(&path)?;
                let initial = input.metadata()?;
                let output = self
                    .target
                    .as_ref()
                    .map(|target| File::create(target.join(relative)))
                    .transpose()?;
                self.file = Some(FileWork {
                    input,
                    output,
                    digest,
                    bytes: 0,
                    initial,
                });
            } else {
                bail!("unsupported skill package entry");
            }
        }
        Ok(self.directories.is_empty() && self.file.is_none())
    }
}

/// Delete staged/old trees with postorder iterators. Cleanup consumes the same
/// root quanta as preparation; it never holds the installer lock or DB capacity.
pub(super) struct Removal {
    directories: Vec<(PathBuf, ReadDir)>,
    marker: Option<PathBuf>,
}
impl Removal {
    pub(super) fn new(path: PathBuf) -> Result<Self> {
        let directories = match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {
                vec![(path.clone(), fs::read_dir(&path)?)]
            }
            Ok(_) => {
                fs::remove_file(&path)?;
                Vec::new()
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(error.into()),
        };
        let marker = if !directories.is_empty() && attempt_marker(&path)?.is_some() {
            Some(path.join(ATTEMPT_MARKER))
        } else {
            None
        };
        Ok(Self {
            directories,
            marker,
        })
    }
    pub(super) fn step(&mut self) -> Result<bool> {
        for _ in 0..FILES {
            let Some((parent, entries)) = self.directories.last_mut() else {
                return Ok(true);
            };
            let Some(entry) = entries.next() else {
                let path = parent.clone();
                self.directories.pop();
                // Preserve ownership across cancellation/crash until every child
                // is gone. The marker is the last file removed from the wrapper.
                if self
                    .marker
                    .as_ref()
                    .is_some_and(|marker| marker.parent() == Some(path.as_path()))
                {
                    fs::remove_file(self.marker.take().unwrap())?;
                }
                fs::remove_dir(path)?;
                continue;
            };
            let path = entry?.path();
            if self.marker.as_ref() == Some(&path) {
                continue;
            }
            let metadata = fs::symlink_metadata(&path)?;
            if metadata.is_dir() && !metadata.file_type().is_symlink() {
                self.directories.push((path.clone(), fs::read_dir(path)?));
            } else {
                fs::remove_file(path)?;
            }
        }
        Ok(self.directories.is_empty())
    }
}

fn cleanup(attempts: Attempts, fence: JobFence) -> Work {
    Box::pin(async_stream::try_stream! {
        let mut after = None;
        loop {
            let page = {
                let paths = attempts.lock().expect("skills attempts lock");
                let lower = after
                    .as_ref()
                    .map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
                paths
                    .range::<PathBuf, _>((lower, std::ops::Bound::Unbounded))
                    .take(FILES)
                    .map(|(path, attempt)| (path.clone(), attempt.owner.clone(), attempt.committed))
                    .collect::<Vec<_>>()
            };
            if page.is_empty() {
                break;
            }
            after = page.last().map(|(path, _, _)| path.clone());
            yield Progress::Quantum;
            for (path, owner, committed) in page {
                if owner != fence.root {
                    continue;
                }
                let check = path.clone();
                let removable = owned_fs(&fence.stop, move || Ok(!check.try_exists()? || attempt_marker(&check)?.is_some_and(|marker| !marker.publishing || committed))).await?;
                yield Progress::Quantum;
                if !removable { continue; }
                let garbage = path.clone();
                let start = owned_fs(&fence.stop, move || Removal::new(garbage)).await;
                let Ok(mut removal) = start else {
                    yield Progress::Failed;
                    continue;
                };
                loop {
                    let result = owned_fs(&fence.stop, move || {
                        let done = removal.step()?;
                        Ok((removal, done))
                    })
                    .await;
                    match result {
                        Ok((next, done)) => {
                            removal = next;
                            yield Progress::Quantum;
                            if done {
                                attempts.lock().expect("skills attempts lock").remove(&path);
                                break;
                            }
                        }
                        Err(_) => {
                            yield Progress::Failed;
                            break;
                        }
                    }
                }
            }
        }
    })
}

fn resolved_attempts(attempts: Attempts, fence: JobFence, skill_id: SkillId) -> Work {
    Box::pin(async_stream::try_stream! {
        let mut after = None;
        loop {
            let paths = {
                let tracked = attempts.lock().expect("skills attempts lock");
                tracked.range::<PathBuf, _>((after.as_ref().map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded), std::ops::Bound::Unbounded)).take(FILES).map(|(path, _)| path.clone()).collect::<Vec<_>>()
            };
            if paths.is_empty() { break; }
            after = paths.last().cloned();
            let id = skill_id.clone();
            let tracked = attempts.clone();
            owned_fs(&fence.stop, move || {
                for path in paths {
                    if attempt_marker(&path)?.is_some_and(|marker| marker.skill_id.as_ref() == Some(&id) && marker.publishing) {
                        if let Some(attempt) = tracked.lock().expect("skills attempts lock").get_mut(&path) { attempt.committed = true; }
                        if set_attempt_publishing(&path, false).is_err() { warn!("committed skill artifact outcome persistence deferred; cleanup uses acknowledged in-memory proof"); }
                    }
                }
                Ok(())
            }).await?;
            yield Progress::Quantum;
        }
    })
}
fn has_pending_attempt(
    attempts: Attempts,
    fence: JobFence,
    skill_id: SkillId,
) -> Pin<Box<dyn Stream<Item = Result<Option<PathBuf>>> + Send>> {
    Box::pin(async_stream::try_stream! {
        let mut after = None;
        loop {
            let paths = {
                let tracked = attempts.lock().expect("skills attempts lock");
                tracked.range::<PathBuf, _>((after.as_ref().map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded), std::ops::Bound::Unbounded)).take(FILES).map(|(path, attempt)| (path.clone(), attempt.committed)).collect::<Vec<_>>()
            };
            if paths.is_empty() { break; }
            after = paths.last().map(|(path, _)| path.clone());
            let id = skill_id.clone();
            let pending = owned_fs(&fence.stop, move || {
                for (path, committed) in paths { if !committed && attempt_marker(&path)?.is_some_and(|marker| marker.skill_id.as_ref() == Some(&id) && marker.publishing) { return Ok(Some(path)); } }
                Ok(None)
            }).await?;
            let done = pending.is_some();
            yield pending;
            if done { break; }
        }
    })
}

// Resolve an earlier unknown publication by forwarding current prepared facts
// under fresh row/Workspace/pack guards. A confirmed forward commit makes the
// old backup unnecessary; failure leaves its marker protected across restart.
fn recover_noop(
    this: Arc<MessageProcessor>,
    snapshot: pioneer_crud::SkillReconciliationSnapshot,
    workspace: Option<pioneer_entity::workspace::Model>,
    managed_root: PathBuf,
    path: PathBuf,
    metadata: PreparedSkillStorageMetadata,
    max_bytes: usize,
    attempts: Attempts,
    fence: JobFence,
) -> Work {
    Box::pin(async_stream::try_stream! {
        let row = snapshot.record.clone();
        let mut pending = has_pending_attempt(attempts.clone(), fence.clone(), row.skill_id.clone());
        let mut recover = None;
        while let Some(found) = pending.next().await { recover = recover.or(found?); yield Progress::Quantum; }
        let Some(recover) = recover else { return; };
        let candidate = SkillStorageRelocationCandidate {
            expected_row: row.clone(), source_path: path.clone(), install_root: managed_root.clone(), destination: path,
            prepared_metadata: metadata, remove_managed_source_after_switch: false, managed_path_to_remove_after_switch: None,
            managed_lock_path: Some(managed_root.join("skills-lock.toml")), max_skill_file_bytes: max_bytes.max(1),
        };
        let handle = tokio::runtime::Handle::current(); let guard = fence.clone(); let token = fence.stop.clone();
        let tracked = attempts.clone(); let owner = fence.root.clone();
        let outcome = owned_fs(&fence.stop, move || handle.block_on(storage::publish_watched_candidate(
            &this.crud_store, &this.skills_write_lock, candidate, snapshot, workspace, None, Some(recover), token, move || guard.valid(),
            move |path, committed| { tracked.lock().expect("skills attempts lock").insert(path, Attempt { owner: owner.clone(), committed }); Ok(()) },
        ))).await?;
        if outcome != SkillStorageRelocationOutcome::Switched { Err::<(), anyhow::Error>(anyhow::anyhow!("stale forward recovery"))?; }
        yield Progress::Changed(row.scope_key);
        let mut resolved = resolved_attempts(attempts, fence, row.skill_id);
        while let Some(progress) = resolved.next().await { yield progress?; }
    })
}

fn metadata(prepared: &PreparedMaterializedSkill, slug: String) -> PreparedSkillStorageMetadata {
    definition_metadata(&prepared.definition, &prepared.source_ref, slug)
}
fn definition_metadata(
    definition: &pioneer_skills::SkillDefinition,
    source_ref: &str,
    slug: String,
) -> PreparedSkillStorageMetadata {
    PreparedSkillStorageMetadata {
        owner: definition.identity.owner.clone(),
        slug,
        version: definition.identity.version_hint.clone(),
        trust_level: storage::trust_level_value(&definition.runtime.trust_level).into(),
        fingerprint: definition.identity.fingerprint.clone(),
        source_ref: source_ref.into(),
    }
}

fn changed_availability(
    baseline: &Arc<StdMutex<Baseline>>,
    row: &SkillInstallationRecord,
    content: Option<[u8; 32]>,
) -> bool {
    let key = (
        row.source_kind.clone(),
        row.scope_key.clone(),
        row.skill_id.clone(),
    );
    let previous = baseline
        .lock()
        .expect("skills baseline lock")
        .managed
        .insert(key, (row.clone(), content));
    match previous {
        Some((old_row, old_content)) => old_row != *row || old_content != content,
        None => content.is_none(),
    }
}

pub(super) fn root_job(
    this: Arc<MessageProcessor>,
    root: PathBuf,
    mappings: Vec<Mapping>,
    baseline: Arc<StdMutex<Baseline>>,
    attempts: Attempts,
    fence: JobFence,
) -> Work {
    Box::pin(async_stream::try_stream! {
        let active_scopes = mappings.iter().filter_map(|mapping| match mapping {
            Mapping::Managed(config, _) => Some((config.roots[0].source_kind.as_db_value().to_string(), config.roots[0].scope_key.clone())),
            _ => None,
        }).collect::<BTreeSet<_>>();
        let mut cursor = None;
        loop {
            fence.check()?;
            let keys = {
                let baseline = baseline.lock().expect("skills baseline lock");
                let lower = cursor.as_ref().map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
                baseline.managed.range((lower, std::ops::Bound::Unbounded)).take(FILES).map(|(key, _)| key.clone()).collect::<Vec<_>>()
            };
            if keys.is_empty() { break; }
            cursor = keys.last().cloned();
            {
                let mut baseline = baseline.lock().expect("skills baseline lock");
                for key in keys { if !active_scopes.contains(&(key.0.clone(), key.1.clone())) { baseline.managed.remove(&key); } }
            }
            yield Progress::Quantum;
        }
        let limit = mappings
            .iter()
            .filter_map(|mapping| match mapping {
                Mapping::Import(config, _) => Some(config.max_skills_per_root),
                _ => None,
            })
            .max();
        // Rediscover exact markers after restart, including a managed root which
        // is not itself configured for imports. Enumeration has the same bounded
        // two-layout cursor and never follows symlinks.
        let scan_root = root.clone();
        let tracked = attempts.clone();
        let mut recovery_scan = owned_fs(&fence.stop, move || Discovery::new(&scan_root, limit.unwrap_or(1), tracked)).await?;
        loop {
            fence.check()?;
            let (next, done) = owned_fs(&fence.stop, move || { let done = recovery_scan.step()?; Ok((recovery_scan, done)) }).await?;
            recovery_scan = next;
            yield Progress::Quantum;
            if done { break; }
        }
        let mut old_attempts = cleanup(attempts.clone(), fence.clone());
        while let Some(progress) = old_attempts.next().await {
            yield progress?;
        }
        let packages: Vec<_> = if limit.is_some() { recovery_scan.packages.into_iter().collect() } else { Vec::new() };
        let source_limit = mappings.iter().filter_map(|mapping| match mapping {
            Mapping::Import(config, _) => Some(config.installer_policy.security.max_install_file_bytes.max(1)),
            _ => None,
        }).max().unwrap_or(1);
        let facts: SourceFacts = Arc::default();
        for mapping in mappings {
            fence.check()?;
            match mapping {
                Mapping::Import(config, workspace) => {
                    let paths: ImportPaths = Arc::default();
                    for package in packages.iter().take(config.max_skills_per_root.max(1)) {
                        let mut work = import_job(
                            this.clone(),
                            config.clone(),
                            workspace.clone(),
                            package.clone(),
                            paths.clone(),
                            facts.clone(),
                            source_limit,
                            attempts.clone(),
                            fence.clone(),
                        );
                        loop {
                            match std::panic::AssertUnwindSafe(work.next()).catch_unwind().await {
                                Ok(Some(Ok(progress))) => yield progress,
                                Ok(None) => break,
                                _ => {
                                    yield Progress::Failed;
                                    break;
                                }
                            }
                        }
                        let mut garbage = cleanup(attempts.clone(), fence.clone());
                        while let Some(progress) = garbage.next().await {
                            yield progress?;
                        }
                    }
                }
                Mapping::Managed(config, workspace) => {
                    let mapping = &config.roots[0];
                    let mut after = None;
                    let mut seen = BTreeSet::new();
                    let mut complete = true;
                    loop {
                        fence.check()?;
                        let page = cancellable_db(
                            &fence.stop,
                            this.crud_store.list_skill_reconciliation_page(
                                mapping.source_kind.as_db_value(),
                                &mapping.scope_key,
                                after.as_deref(),
                                16,
                            ),
                        )
                        .await?;
                        if page.is_empty() {
                            break;
                        }
                        after = page.last().map(|row| row.record.skill_id.to_string());
                        yield Progress::Quantum;
                        for row in page {
                            seen.insert(row.record.skill_id.clone());
                            let mut work = managed_job(
                                this.clone(),
                                config.clone(),
                                workspace.clone(),
                                row,
                                baseline.clone(),
                                attempts.clone(),
                                fence.clone(),
                            );
                            loop {
                                match std::panic::AssertUnwindSafe(work.next()).catch_unwind().await {
                                    Ok(Some(Ok(progress))) => {
                                        if matches!(progress, Progress::Failed) { complete = false; }
                                        yield progress;
                                    },
                                    Ok(None) => break,
                                    _ => {
                                        complete = false;
                                        yield Progress::Failed;
                                        break;
                                    }
                                }
                            }
                            let mut garbage = cleanup(attempts.clone(), fence.clone());
                            while let Some(progress) = garbage.next().await { yield progress?; }
                        }
                    }
                    if complete {
                        let mut cursor = None;
                        loop {
                            fence.check()?;
                            let keys = {
                                let baseline = baseline.lock().expect("skills baseline lock");
                                let lower = cursor.as_ref().map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
                                baseline.managed.range((lower, std::ops::Bound::Unbounded)).take(FILES).map(|(key, _)| key.clone()).collect::<Vec<_>>()
                            };
                            if keys.is_empty() { break; }
                            cursor = keys.last().cloned();
                            {
                                let mut baseline = baseline.lock().expect("skills baseline lock");
                                for key in keys {
                                    if key.0 == mapping.source_kind.as_db_value() && key.1 == mapping.scope_key && !seen.contains(&key.2) { baseline.managed.remove(&key); }
                                }
                            }
                            yield Progress::Quantum;
                        }
                    }
                }
            }
        }
    })
}

fn import_job(
    this: Arc<MessageProcessor>,
    config: ConfiguredRootImportConfig,
    workspace: Option<pioneer_entity::workspace::Model>,
    package: PathBuf,
    paths: ImportPaths,
    facts: SourceFacts,
    source_limit: usize,
    attempts: Attempts,
    fence: JobFence,
) -> Work {
    Box::pin(async_stream::try_stream! {
        let root = &config.roots[0];
        let package = owned_fs(&fence.stop, move || {
            storage::normalize_import_source_path(&package)
        })
        .await?;
        let source_ref = storage::import_source_ref(&package)?;
        if root.source_is_pioneer_managed {
            let path = package.clone();
            let root_path = root.source_root.clone();
            let container = owned_fs(&fence.stop, move || {
                let root_path = storage::normalize_import_source_path(&root_path)?;
                Ok(path
                    .parent()
                    .filter(|parent| parent.parent() == Some(root_path.as_path()))
                    .and_then(|parent| parent.file_name())
                    .and_then(|name| name.to_str())
                    .and_then(|id| SkillId::new(id.to_owned()).ok()))
            })
            .await?;
            if let Some(id) = container {
                let row =
                    cancellable_db(&fence.stop, this.crud_store.find_skill_installation(&id)).await?;
                if row.is_some_and(|row| {
                    row.source_kind == root.source_kind.as_db_value() && row.scope_key == root.scope_key
                }) {
                    yield Progress::Quantum;
                    return;
                }
            }
        }
        #[cfg(test)] fence.signals.provenance_lookups.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let provenance = cancellable_db(&fence.stop, this.crud_store.find_skill_import_provenance(
            root.source_kind.as_db_value(), &root.scope_key, &source_ref,
        )).await?;
        yield Progress::Quantum;
        // Only a provenance miss needs the legacy path fallback. Build it once
        // per scope/root round, with FS normalization after releasing each page.
        if provenance.is_none() && paths.lock().expect("import paths lock").is_none() {
            let mut after = None;
            let mut index = BTreeMap::new();
            loop {
                fence.check()?;
                let page = cancellable_db(&fence.stop, this.crud_store.list_skill_reconciliation_page(
                    root.source_kind.as_db_value(), &root.scope_key, after.as_deref(), 64,
                )).await?;
                if page.is_empty() { break; }
                after = page.last().map(|row| row.record.skill_id.to_string());
                #[cfg(test)] fence.signals.fallback_rows.fetch_add(page.len() as u64, std::sync::atomic::Ordering::Relaxed);
                index = owned_fs(&fence.stop, move || {
                    for snapshot in page {
                        if let Ok(path) = storage::normalize_absolute_path(Path::new(&snapshot.record.install_path)) {
                            index.entry(path).and_modify(|row| *row = None).or_insert(Some(snapshot));
                        }
                    }
                    Ok(index)
                }).await?;
                yield Progress::Quantum;
            }
            *paths.lock().expect("import paths lock") = Some(index);
        }
        let path_match = if provenance.is_none() {
            let matched = paths.lock().expect("import paths lock").as_ref().and_then(|index| index.get(&package)).cloned();
            match matched {
                Some(None) => Err(anyhow::anyhow!("ambiguous skill import path"))?,
                Some(Some(row)) => Some(row),
                None => None,
            }
        } else { None };
        let snapshot = if let Some(snapshot) = provenance.or(path_match) {
            snapshot
        } else {
            let id = loop {
                let id = SkillId::new(pioneer_protocol::generate_id(
                    pioneer_protocol::SKILL_ID_LEN,
                ))
                .map_err(|error| anyhow::anyhow!("invalid generated skill ID: {error}"))?;
                if !config.reserved_skill_ids.contains(&id)
                    && cancellable_db(&fence.stop, this.crud_store.find_skill_installation(&id))
                        .await?
                        .is_none()
                {
                    break id;
                }
                yield Progress::Quantum;
            };
            let now = now_timestamp_secs();
            let row = SkillInstallationRecord {
                skill_id: id,
                owner: None,
                slug: package
                    .file_name()
                    .and_then(|name| name.to_str())
                    .map(normalize_skill_slug)
                    .filter(|slug| !slug.is_empty())
                    .unwrap_or_else(|| "unnamed-skill".into()),
                version: None,
                source_kind: root.source_kind.as_db_value().into(),
                scope_key: root.scope_key.clone(),
                source_ref: source_ref.clone(),
                install_path: package.display().to_string(),
                trust_level: "community".into(),
                fingerprint: String::new(),
                updated_at_unix: now,
                pack_id: None,
                pack_member_key: None,
            };
            fence.check()?;
            let _lock = cancellable_db(&fence.stop, async {
                Ok(this.skills_write_lock.lock().await)
            })
            .await?;
            let registered = cancellable_db(
                &fence.stop,
                this.crud_store
                    .register_skill_import_pending(&row, workspace.as_ref(), now),
            )
            .await?
            .context("stale configured import registration")?;
            drop(_lock);
            registered
        };
        let row = snapshot.record.clone();
        if root.source_is_pioneer_managed && row.source_ref != source_ref {
            yield Progress::Quantum;
            return;
        }
        let request = PrepareMaterializedSkillRequest {
            skill_id: row.skill_id.clone(), source_kind: root.source_kind,
            source_ref: source_ref.clone(), materialized_source_path: package.clone(), policy: config.installer_policy.clone(),
        };
        let cached = facts.lock().expect("source facts lock").get(&package).and_then(|facts| facts.physical.clone());
        let physical = if let Some(physical) = cached { physical.map_err(anyhow::Error::msg)? } else {
            #[cfg(test)] fence.signals.preparations.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let mut common_request = request.clone();
            common_request.policy.security.max_install_file_bytes = source_limit;
            let mut preparation = owned_fs(&fence.stop, move || Ok(MaterializedSkillPreparation::new(common_request))).await?;
            let physical = loop {
                fence.check()?;
                let work = owned_fs(&fence.stop, move || { let result = preparation.step_facts(FILES)?; Ok((preparation, result)) }).await;
                let (next, result) = match work {
                    Ok(result) => result,
                    Err(error) => {
                        facts.lock().expect("source facts lock").entry(package.clone()).or_default().physical = Some(Err(format!("{error:#}")));
                        Err::<_, anyhow::Error>(error)?
                    }
                };
                preparation = next;
                yield Progress::Quantum;
                if let Some(physical) = result { break Arc::new(physical); }
            };
            facts.lock().expect("source facts lock").entry(package.clone()).or_default().physical = Some(Ok(physical.clone()));
            physical
        };
        // Same physical facts, distinct source-kind parsing, policy, ID and scope.
        let definition = owned_fs(&fence.stop, move || physical.definition_for(&request)).await?;
        yield Progress::Quantum;
        let metadata = definition_metadata(&definition, &source_ref, definition.identity.slug.clone());
        let destination =
            canonical_skill_install_path(&root.managed_root, &row.skill_id, &metadata.slug)?;
        let destination = storage::normalize_absolute_path(&destination)?;
        let current = storage::normalize_absolute_path(Path::new(&row.install_path))?;
        if current == package && current == destination {
            yield Progress::Quantum;
            return;
        }
        let max_bytes = config.installer_policy.security.max_install_file_bytes;
        let cached_hash = facts.lock().expect("source facts lock").get(&package).and_then(|facts| facts.hash.clone());
        let source_hash = if let Some(hash) = cached_hash { hash.map_err(anyhow::Error::msg)? } else {
        let path = package.clone();

        let mut source_tree = match owned_fs(&fence.stop, move || Tree::new(path, None, source_limit, false)).await {
            Ok(tree) => tree,
            Err(error) => {
                facts.lock().expect("source facts lock").entry(package.clone()).or_default().hash = Some(Err(format!("{error:#}")));
                Err::<_, anyhow::Error>(error)?
            }
        };
        #[cfg(test)] { source_tree.source_signals = Some(fence.signals.clone()); fence.signals.hashes.fetch_add(1, std::sync::atomic::Ordering::Relaxed); }
        let source_hash = loop {
            let work = owned_fs(&fence.stop, move || {
                let done = source_tree.step()?;
                Ok((source_tree, done))
            })
            .await;
            let (next, done) = match work {
                Ok(result) => result,
                Err(error) => {
                    facts.lock().expect("source facts lock").entry(package.clone()).or_default().hash = Some(Err(format!("{error:#}")));
                    Err::<_, anyhow::Error>(error)?
                }
            };
            source_tree = next;
            yield Progress::Quantum;
            if done {
                break source_tree.fingerprint();
            }
        };
        facts.lock().expect("source facts lock").entry(package.clone()).or_default().hash = Some(Ok(source_hash));
        source_hash
        };
        // Full content comparison coalesces own writes, retries and restart. An
        // unchanged SKILL.md alone must not hide changed supporting files.
        if current == destination && storage::row_metadata_matches(&row, &metadata) {
            let path = destination.clone();
            let tree = owned_fs(&fence.stop, move || {
                match Tree::new(path, None, max_bytes, false) {
                    Ok(tree) => Ok(Some(tree)),
                    Err(error)
                        if error
                            .downcast_ref::<std::io::Error>()
                            .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound) =>
                    {
                        Ok(None)
                    }
                    Err(error) => Err(error),
                }
            })
            .await?;
            if let Some(mut tree) = tree {
                let target_hash = loop {
                    let (next, done) = owned_fs(&fence.stop, move || {
                        let done = tree.step()?;
                        Ok((tree, done))
                    })
                    .await?;
                    tree = next;
                    yield Progress::Quantum;
                    if done {
                        break tree.fingerprint();
                    }
                };
                if source_hash == target_hash {
                    let mut recovery = recover_noop(this.clone(), snapshot, workspace, root.managed_root.clone(), destination, metadata, max_bytes, attempts.clone(), fence.clone());
                    while let Some(progress) = recovery.next().await { yield progress?; }
                    return;
                }
            }
        }
        let parent = destination
            .parent()
            .context("skill destination has no parent")?
            .to_path_buf();
        let registry = attempts.clone();
        let owner = fence.root.clone();
        let attempt = owned_fs(&fence.stop, move || {
            fs::create_dir_all(&parent)?;
            let parent = fs::canonicalize(parent)?;
            if registry
                .lock()
                .expect("skills attempts lock")
                .keys()
                .any(|path| path.parent() == Some(parent.as_path()) && attempt_marker(path).is_ok_and(|marker| marker.is_some_and(|marker| !marker.publishing)))
            {
                Err::<(), anyhow::Error>(anyhow::anyhow!("previous skill stage cleanup is pending"))?;
            }
            let path = new_attempt(&parent, owner.clone())?;
            fs::create_dir(path.join("payload"))?;
            registry
                .lock()
                .expect("skills attempts lock")
                .insert(path.clone(), Attempt::new(owner));
            Ok(path)
        })
        .await?;
        let source = package.clone();
        let target = attempt.join("payload");
        let mut copy = owned_fs(&fence.stop, move || {
            Tree::new(source, Some(target), max_bytes, false)
        })
        .await?;
        let copied_hash = loop {
            fence.check()?;
            let (next, done) = owned_fs(&fence.stop, move || {
                let done = copy.step()?;
                Ok((copy, done))
            })
            .await?;
            copy = next;
            yield Progress::Quantum;
            if done {
                break copy.fingerprint();
            }
        };
        if source_hash != copied_hash {
            Err::<(), anyhow::Error>(anyhow::anyhow!("configured source changed during copy"))?;
        }
        let old_managed_path =
            (current != package && current != destination).then_some(current);
        let candidate = SkillStorageRelocationCandidate {
            expected_row: row,
            source_path: package,
            install_root: root.managed_root.clone(),
            destination,
            prepared_metadata: metadata,
            remove_managed_source_after_switch: root.source_is_pioneer_managed,
            managed_path_to_remove_after_switch: old_managed_path,
            managed_lock_path: Some(root.managed_root.join("skills-lock.toml")),
            max_skill_file_bytes: max_bytes.max(1),
        };
        fence.check()?;
        let scope = root.scope_key.clone();
        let publisher = this.clone();
        let handle = tokio::runtime::Handle::current();
        let staged = attempt.join("payload");
        let guard = fence.clone();
        let token = fence.stop.clone();
        let garbage = attempts.clone();
        let owner = fence.root.clone();
        let outcome = owned_fs(&fence.stop, move || {
            handle.block_on(storage::publish_watched_candidate(
                &publisher.crud_store,
                &publisher.skills_write_lock,
                candidate,
                snapshot,
                workspace,
                Some(staged),
                None,
                token,
                move || guard.valid(),
                move |path, committed| {
                    let path = super::physical_root(&path)?;
                    garbage
                        .lock()
                        .expect("skills attempts lock")
                        .insert(path, Attempt { owner: owner.clone(), committed });
                    Ok(())
                },
            ))
        })
        .await?;
        match outcome {
            SkillStorageRelocationOutcome::Switched => {
                yield Progress::Changed(scope);
                let mut resolved = resolved_attempts(attempts.clone(), fence.clone(), definition.identity.skill_id.clone());
                while let Some(progress) = resolved.next().await { yield progress?; }
            },
            SkillStorageRelocationOutcome::Stale => {
                Err::<(), anyhow::Error>(anyhow::anyhow!("stale configured skill projection"))?
            }
        }

    })
}

fn managed_job(
    this: Arc<MessageProcessor>,
    config: ManagedRootScanConfig,
    workspace: Option<pioneer_entity::workspace::Model>,
    snapshot: pioneer_crud::SkillReconciliationSnapshot,
    baseline: Arc<StdMutex<Baseline>>,
    attempts: Attempts,
    fence: JobFence,
) -> Work {
    Box::pin(async_stream::try_stream! {
        let row = snapshot.record.clone();
        let root = &config.roots[0];
        let managed_root = storage::normalize_absolute_path(&root.managed_root)?;
        let container = managed_root.join(row.skill_id.as_str());
        let current = storage::normalize_absolute_path(Path::new(&row.install_path))?;
        if current.parent() != Some(container.as_path()) {
            yield Progress::Quantum;
            return;
        }
        let path = current.clone();
        let parent = container.clone();
        let found = owned_fs(&fence.stop, move || {
            match fs::symlink_metadata(&parent) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(error) => return Err(error.into()),
                Ok(metadata) if !metadata.is_dir() || metadata.file_type().is_symlink() => {
                    return Ok(None);
                }
                Ok(_) => {}
            }
            let direct = fs::symlink_metadata(&path)
                .is_ok_and(|metadata| metadata.is_dir() && !metadata.file_type().is_symlink())
                && path.join("SKILL.md").is_file();
            Ok(Some((direct.then_some(path), fs::read_dir(parent)?)))
        })
        .await?;
        let Some((direct, mut entries)) = found else {
            if fence.valid() && changed_availability(&baseline, &row, None) {
                yield Progress::Changed(root.scope_key.clone());
            }
            return;
        };
        let mut leaves = Vec::new();
        let source = if let Some(direct) = direct {
            Some(direct)
        } else {
            loop {
                let (next, next_leaves, done) = owned_fs(&fence.stop, move || {
                    let mut done = false;
                    for _ in 0..FILES {
                        let Some(entry) = entries.next() else {
                            done = true;
                            break;
                        };
                        let entry = entry?;
                        let path = entry.path();
                        let metadata = fs::symlink_metadata(&path)?;
                        if metadata.is_dir()
                            && !metadata.file_type().is_symlink()
                            && !entry.file_name().to_string_lossy().starts_with('.')
                            && path.join("SKILL.md").is_file()
                        {
                            leaves.push(path);
                            if leaves.len() > 1 {
                                done = true;
                                break;
                            }
                        }
                    }
                    Ok((entries, leaves, done))
                })
                .await?;
                entries = next;
                leaves = next_leaves;
                yield Progress::Quantum;
                if done {
                    break;
                }
            }
            if leaves.len() == 1 {
                leaves.pop()
            } else {
                None
            }
        };
        let Some(source) = source else {
            if fence.valid() && changed_availability(&baseline, &row, None) {
                yield Progress::Changed(root.scope_key.clone());
            }
            return;
        };
        let slug = source
            .file_name()
            .and_then(|name| name.to_str())
            .map(normalize_skill_slug)
            .context("invalid managed skill leaf")?;
        if slug.is_empty() {
            Err::<(), anyhow::Error>(anyhow::anyhow!("empty managed skill slug"))?;
        }
        let request = PrepareMaterializedSkillRequest {
            skill_id: row.skill_id.clone(),
            source_kind: root.source_kind,
            source_ref: row.source_ref.clone(),
            materialized_source_path: source.clone(),
            policy: config.installer_policy.clone(),
        };
        let mut preparation = owned_fs(&fence.stop, move || {
            Ok(MaterializedSkillPreparation::new(request))
        })
        .await?;
        let prepared = loop {
            fence.check()?;
            let (next, result) = owned_fs(&fence.stop, move || {
                let result = preparation.step(FILES);
                Ok((preparation, result))
            })
            .await?;
            preparation = next;
            yield Progress::Quantum;
            match result {
                Ok(Some(prepared)) => break prepared,
                Ok(None) => {}
                Err(_) => {
                    if changed_availability(&baseline, &row, None) {
                        yield Progress::Changed(root.scope_key.clone());
                    }
                    Err::<(), anyhow::Error>(anyhow::anyhow!(
                        "managed skill validation failed; retry retained"
                    ))?;
                }
            }
        };
        let metadata = metadata(&prepared, slug);
        let destination = canonical_skill_install_path(&managed_root, &row.skill_id, &metadata.slug)?;
        let path = source.clone();
        let max_bytes = config.installer_policy.security.max_install_file_bytes;
        let mut tree = owned_fs(&fence.stop, move || Tree::new(path, None, max_bytes, true)).await?;
        let content = loop {
            let (next, done) = owned_fs(&fence.stop, move || {
                let done = tree.step()?;
                Ok((tree, done))
            })
            .await?;
            tree = next;
            yield Progress::Quantum;
            if done {
                break tree.fingerprint();
            }
        };
        if source == destination && storage::row_metadata_matches(&row, &metadata) {
            let availability_changed = fence.valid() && changed_availability(&baseline, &row, Some(content));
            let mut recovery = recover_noop(this.clone(), snapshot, workspace, managed_root, source, metadata, max_bytes, attempts.clone(), fence.clone());
            let mut notified = false;
            while let Some(progress) = recovery.next().await {
                let progress = progress?;
                notified |= matches!(progress, Progress::Changed(_));
                yield progress;
            }
            if availability_changed && !notified { yield Progress::Changed(root.scope_key.clone()); }
            return;
        }
        let candidate = SkillStorageRelocationCandidate {
            expected_row: row.clone(),
            source_path: source,
            install_root: managed_root.clone(),
            destination,
            prepared_metadata: metadata,
            remove_managed_source_after_switch: false,
            managed_path_to_remove_after_switch: None,
            managed_lock_path: Some(managed_root.join("skills-lock.toml")),
            max_skill_file_bytes: max_bytes.max(1),
        };
        fence.check()?;
        let publisher = this.clone();
        let handle = tokio::runtime::Handle::current();
        let guard = fence.clone();
        let token = fence.stop.clone();
        let garbage = attempts.clone();
        let owner = fence.root.clone();
        let outcome = owned_fs(&fence.stop, move || {
            handle.block_on(storage::publish_watched_candidate(
                &publisher.crud_store,
                &publisher.skills_write_lock,
                candidate,
                snapshot,
                workspace,
                None,
                None,
                token,
                move || guard.valid(),
                move |path, committed| {
                    let path = super::physical_root(&path)?;
                    garbage.lock().expect("skills attempts lock").insert(path, Attempt { owner: owner.clone(), committed });
                    Ok(())
                },
            ))
        })
        .await?;
        match outcome {
            SkillStorageRelocationOutcome::Switched => {
                yield Progress::Changed(root.scope_key.clone());
            }
            SkillStorageRelocationOutcome::Stale => {
                Err::<(), anyhow::Error>(anyhow::anyhow!("stale managed skill projection"))?
            }
        }
        let refreshed = cancellable_db(
            &fence.stop,
            this.crud_store.find_skill_installation(&row.skill_id),
        )
        .await?;
        if let Some(refreshed) = refreshed {
            changed_availability(&baseline, &refreshed, Some(content));
        }
        let mut resolved = resolved_attempts(attempts, fence, row.skill_id);
        while let Some(progress) = resolved.next().await { yield progress?; }
    })
}

#[cfg(test)]
#[path = "watcher_reconcile_tests.rs"]
mod tests;
