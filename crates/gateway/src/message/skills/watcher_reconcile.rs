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
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{self, File, ReadDir};
use std::io::{Read, Write};
use std::pin::Pin;
use std::sync::Mutex as StdMutex;

const FILES: usize = 64;
const BYTES: usize = 256 * 1024;

#[derive(Default)]
pub(super) struct Baseline {
    managed: HashMap<(String, String, SkillId), (SkillInstallationRecord, Option<[u8; 32]>)>,
}

type Work = Pin<Box<dyn Stream<Item = Result<Progress>> + Send>>;
pub(super) type Attempts = Arc<StdMutex<BTreeMap<PathBuf, PathBuf>>>;

/// Depth is limited to the two existing configured package layouts. Entries,
/// including unrelated files, consume the quantum. The domain limit chooses the
/// first sorted packages only after complete enumeration, rather than truncating
/// an arbitrary technical page.
struct Discovery {
    directories: Vec<(ReadDir, usize)>,
    packages: BTreeSet<PathBuf>,
    limit: usize,
    attempts: Attempts,
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
        Ok(Self { directories })
    }
    pub(super) fn step(&mut self) -> Result<bool> {
        for _ in 0..FILES {
            let Some((parent, entries)) = self.directories.last_mut() else {
                return Ok(true);
            };
            let Some(entry) = entries.next() else {
                let path = parent.clone();
                self.directories.pop();
                fs::remove_dir(path)?;
                continue;
            };
            let path = entry?.path();
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
                    .map(|(path, owner)| (path.clone(), owner.clone()))
                    .collect::<Vec<_>>()
            };
            if page.is_empty() {
                break;
            }
            after = page.last().map(|(path, _)| path.clone());
            yield Progress::Quantum;
            for (path, owner) in page {
                if owner != fence.root {
                    continue;
                }
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

fn metadata(prepared: &PreparedMaterializedSkill, slug: String) -> PreparedSkillStorageMetadata {
    PreparedSkillStorageMetadata {
        owner: prepared.definition.identity.owner.clone(),
        slug,
        version: prepared.definition.identity.version_hint.clone(),
        trust_level: storage::trust_level_value(&prepared.definition.runtime.trust_level).into(),
        fingerprint: prepared.definition.identity.fingerprint.clone(),
        source_ref: prepared.source_ref.clone(),
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
        let mut old_attempts = cleanup(attempts.clone(), fence.clone());
        while let Some(progress) = old_attempts.next().await {
            yield progress?;
        }
        let limit = mappings
            .iter()
            .filter_map(|mapping| match mapping {
                Mapping::Import(config, _) => Some(config.max_skills_per_root),
                _ => None,
            })
            .max();
        let mut packages = Vec::new();
        if let Some(limit) = limit {
            let path = root.clone();
            let tracked = attempts.clone();
            let mut discovery =
                owned_fs(&fence.stop, move || Discovery::new(&path, limit, tracked)).await?;
            loop {
                fence.check()?;
                let (next, done) = owned_fs(&fence.stop, move || {
                    let done = discovery.step()?;
                    Ok((discovery, done))
                })
                .await?;
                discovery = next;
                yield Progress::Quantum;
                if done {
                    packages = discovery.packages.into_iter().collect();
                    break;
                }
            }
        }
        for mapping in mappings {
            fence.check()?;
            match mapping {
                Mapping::Import(config, workspace) => {
                    for package in &packages {
                        let mut work = import_job(
                            this.clone(),
                            config.clone(),
                            workspace.clone(),
                            package.clone(),
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
                                    Ok(Some(Ok(progress))) => yield progress,
                                    Ok(None) => break,
                                    _ => {
                                        yield Progress::Failed;
                                        break;
                                    }
                                }
                            }
                            let mut garbage = cleanup(attempts.clone(), fence.clone());
                            while let Some(progress) = garbage.next().await { yield progress?; }
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
        // Preserve both exact provenance and legacy normalized-path lookup. Each
        // page is released before path preparation or another root's quantum.
        let mut after = None;
        let mut provenance = None;
        let mut path_match = None;
        let mut path_ambiguous = false;
        loop {
            fence.check()?;
            let page = cancellable_db(
                &fence.stop,
                this.crud_store.list_skill_reconciliation_page(
                    root.source_kind.as_db_value(),
                    &root.scope_key,
                    after.as_deref(),
                    64,
                ),
            )
            .await?;
            if page.is_empty() {
                break;
            }
            after = page.last().map(|row| row.record.skill_id.to_string());
            let source = package.clone();
            let reference = source_ref.clone();
            let (next_provenance, next_path, ambiguous) = owned_fs(&fence.stop, move || {
                for snapshot in page {
                    let row = &snapshot.record;
                    if row.source_ref == reference && provenance.replace(snapshot.clone()).is_some() {
                        Err::<(), anyhow::Error>(anyhow::anyhow!("ambiguous skill import provenance"))?;
                    }
                    if storage::normalize_absolute_path(Path::new(&row.install_path))
                        .ok()
                        .as_deref()
                        == Some(source.as_path())
                    {
                        path_ambiguous |= path_match.replace(snapshot).is_some();
                    }
                }
                Ok((provenance, path_match, path_ambiguous))
            })
            .await?;
            provenance = next_provenance;
            path_match = next_path;
            path_ambiguous = ambiguous;
            yield Progress::Quantum;
        }
        if provenance.is_none() && path_ambiguous {
            Err::<(), anyhow::Error>(anyhow::anyhow!("ambiguous skill import path"))?;
        }
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
            skill_id: row.skill_id.clone(),
            source_kind: root.source_kind,
            source_ref: source_ref.clone(),
            materialized_source_path: package.clone(),
            policy: config.installer_policy.clone(),
        };
        let mut preparation = owned_fs(&fence.stop, move || {
            Ok(MaterializedSkillPreparation::new(request))
        })
        .await?;
        let prepared = loop {
            fence.check()?;
            let (next, result) = owned_fs(&fence.stop, move || {
                let result = preparation.step(FILES)?;
                Ok((preparation, result))
            })
            .await?;
            preparation = next;
            yield Progress::Quantum;
            if let Some(prepared) = result {
                break prepared;
            }
        };
        let metadata = metadata(&prepared, prepared.definition.identity.slug.clone());
        let destination =
            canonical_skill_install_path(&root.managed_root, &row.skill_id, &metadata.slug)?;
        let destination = storage::normalize_absolute_path(&destination)?;
        let current = storage::normalize_absolute_path(Path::new(&row.install_path))?;
        if current == package && current == destination {
            yield Progress::Quantum;
            return;
        }
        let path = package.clone();
        let max_bytes = config.installer_policy.security.max_install_file_bytes;
        let mut source_tree =
            owned_fs(&fence.stop, move || Tree::new(path, None, max_bytes, false)).await?;
        let source_hash = loop {
            let (next, done) = owned_fs(&fence.stop, move || {
                let done = source_tree.step()?;
                Ok((source_tree, done))
            })
            .await?;
            source_tree = next;
            yield Progress::Quantum;
            if done {
                break source_tree.fingerprint();
            }
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
                    yield Progress::Quantum;
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
                .any(|path| path.parent() == Some(parent.as_path()))
            {
                Err::<(), anyhow::Error>(anyhow::anyhow!("previous skill stage cleanup is pending"))?;
            }
            let path = tempfile::Builder::new()
                .prefix(".pioneer-relocation-")
                .tempdir_in(parent)?
                .keep();
            registry
                .lock()
                .expect("skills attempts lock")
                .insert(path.clone(), owner);
            Ok(path)
        })
        .await?;
        let source = package.clone();
        let target = attempt.clone();
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
            (current != prepared.source_path && current != destination).then_some(current);
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
        let staged = attempt.clone();
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
                token,
                move || guard.valid(),
                move |path| {
                    let path = super::physical_root(&path)?;
                    garbage
                        .lock()
                        .expect("skills attempts lock")
                        .insert(path, owner.clone());
                    Ok(())
                },
            ))
        })
        .await?;
        match outcome {
            SkillStorageRelocationOutcome::Switched => yield Progress::Changed(scope),
            SkillStorageRelocationOutcome::Stale => {
                Err::<(), anyhow::Error>(anyhow::anyhow!("stale configured skill projection"))?
            }
        }
        attempts
            .lock()
            .expect("skills attempts lock")
            .remove(&attempt);
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
            if fence.valid() && changed_availability(&baseline, &row, Some(content)) {
                yield Progress::Changed(root.scope_key.clone());
            }
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
                token,
                move || guard.valid(),
                move |path| {
                    let path = super::physical_root(&path)?;
                    garbage.lock().expect("skills attempts lock").insert(path, owner.clone());
                    Ok(())
                },
            ))
        })
        .await?;
        match outcome {
            SkillStorageRelocationOutcome::Switched => {}
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
        yield Progress::Changed(root.scope_key.clone());
    })
}

#[cfg(test)]
#[path = "watcher_reconcile_tests.rs"]
mod tests;
