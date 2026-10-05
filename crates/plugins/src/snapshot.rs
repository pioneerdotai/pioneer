use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::Read,
    path::{Component, Path, PathBuf},
};

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub expanded_bytes: usize,
    pub entry_bytes: usize,
    pub entries: usize,
    pub depth: usize,
    pub components: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            expanded_bytes: 256 * 1024 * 1024,
            entry_bytes: 32 * 1024 * 1024,
            entries: 10_000,
            depth: 32,
            components: 256,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Entry {
    Directory { mode: u32 },
    File { bytes: Vec<u8>, mode: u32 },
    Denied,
}
#[derive(Debug, Clone)]
pub struct Snapshot {
    entries: BTreeMap<String, Entry>,
    limits: Limits,
    root_mode: u32,
}

impl Snapshot {
    /// Blocking bounded filesystem work, intended for a delivery worker outside
    /// database capacity. Two independently verified scans reject mixed trees.
    pub fn capture(root: &Path, limits: Limits, cancelled: impl Fn() -> bool) -> Result<Self> {
        let root = root.canonicalize().context("source_unavailable")?;
        if !root.is_dir() {
            bail!("source_kind");
        }
        let first = capture_once(&root, limits, &cancelled)?;
        let second = capture_once(&root, limits, &cancelled)?;
        if first.entries != second.entries || first.root_mode != second.root_mode {
            bail!("source_changed");
        }
        Ok(first)
    }
    pub fn from_entries(entries: BTreeMap<String, Entry>, limits: Limits) -> Result<Self> {
        if entries.len() > limits.entries {
            bail!("entry_limit");
        }
        let mut total = 0usize;
        for (key, value) in &entries {
            validate_key(key, limits.depth)?;
            // Parents must be real directories, never files/denied paths.
            let mut parent = Path::new(key).parent();
            while let Some(p) = parent.filter(|p| !p.as_os_str().is_empty()) {
                if !matches!(
                    entries.get(p.to_str().context("invalid_path")?),
                    Some(Entry::Directory { .. })
                ) {
                    bail!("invalid_parent");
                }
                parent = p.parent();
            }
            if let Entry::File { bytes, .. } = value {
                if bytes.len() > limits.entry_bytes {
                    bail!("entry_size_limit");
                }
                total = total.checked_add(bytes.len()).context("expanded_limit")?;
                if total > limits.expanded_bytes {
                    bail!("expanded_limit");
                }
            }
        }
        Ok(Self {
            entries,
            limits,
            root_mode: 0o755,
        })
    }
    pub fn entries(&self) -> &BTreeMap<String, Entry> {
        &self.entries
    }
    pub fn limits(&self) -> Limits {
        self.limits
    }
    pub fn file(&self, key: &str) -> Option<&[u8]> {
        match self.entries.get(key) {
            Some(Entry::File { bytes, .. }) => Some(bytes),
            _ => None,
        }
    }
    pub fn tree_digest(&self) -> String {
        let root = Entry::Directory {
            mode: self.root_mode,
        };
        digest_entries(
            std::iter::once(("", &root)).chain(self.entries.iter().map(|(k, v)| (k.as_str(), v))),
        )
    }
    pub fn member_digest(&self, prefix: &str) -> String {
        let root = self.entries.get(prefix);
        let prefix = format!("{prefix}/");
        digest_entries(
            root.into_iter().map(|root| ("", root)).chain(
                self.entries
                    .iter()
                    .filter_map(|(k, v)| k.strip_prefix(&prefix).map(|k| (k, v))),
            ),
        )
    }
}

fn validate_key(key: &str, max_depth: usize) -> Result<()> {
    if key.len() > 4096
        || key.is_empty()
        || key.contains(['\\', '\0'])
        || key.starts_with('/')
        || key
            .split('/')
            .any(|s| s.is_empty() || s == "." || s == ".." || s.contains(':'))
        || key.split('/').count() > max_depth
    {
        bail!("invalid_path");
    }
    if Path::new(key)
        .components()
        .any(|c| !matches!(c, Component::Normal(_)))
    {
        bail!("invalid_path");
    }
    Ok(())
}
fn capture_once(root: &Path, limits: Limits, cancelled: &impl Fn() -> bool) -> Result<Snapshot> {
    let mut entries = BTreeMap::new();
    let mut active = BTreeSet::new();
    let mut total = 0;
    visit(
        root,
        root,
        "",
        limits,
        cancelled,
        &mut entries,
        &mut active,
        &mut total,
    )?;
    let mut snapshot = Snapshot::from_entries(entries, limits)?;
    snapshot.root_mode = mode(&fs::metadata(root)?);
    Ok(snapshot)
}
#[allow(clippy::too_many_arguments)]
fn visit(
    root: &Path,
    path: &Path,
    key: &str,
    limits: Limits,
    cancelled: &impl Fn() -> bool,
    entries: &mut BTreeMap<String, Entry>,
    active: &mut BTreeSet<PathBuf>,
    total: &mut usize,
) -> Result<()> {
    if cancelled() {
        bail!("cancelled");
    }
    if !key.is_empty() {
        validate_key(key, limits.depth)?;
        if entries.len() >= limits.entries {
            bail!("entry_limit");
        }
    }
    let resolved = match path.canonicalize() {
        Ok(p) if p.starts_with(root) => p,
        _ => {
            if !key.is_empty() {
                entries.insert(key.into(), Entry::Denied);
            }
            return Ok(());
        }
    };
    let metadata = fs::metadata(&resolved)?;
    if metadata.is_dir() {
        if !active.insert(resolved.clone()) {
            entries.insert(key.into(), Entry::Denied);
            return Ok(());
        }
        if !key.is_empty() {
            entries.insert(
                key.into(),
                Entry::Directory {
                    mode: mode(&metadata),
                },
            );
        }
        let mut children: Vec<_> = fs::read_dir(&resolved)?
            .take(limits.entries.saturating_sub(entries.len()) + 1)
            .collect::<std::io::Result<_>>()?;
        if children.len() > limits.entries.saturating_sub(entries.len()) {
            bail!("entry_limit");
        }
        children.sort_by_key(|entry| entry.file_name());
        for child in children {
            let name = child
                .file_name()
                .into_string()
                .map_err(|_| anyhow::anyhow!("invalid_path"))?;
            let child_key = if key.len() > 4096 || key.is_empty() {
                name
            } else {
                format!("{key}/{name}")
            };
            if let Err(error) = visit(
                root,
                &child.path(),
                &child_key,
                limits,
                cancelled,
                entries,
                active,
                total,
            ) {
                if error
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|error| error.kind() == std::io::ErrorKind::PermissionDenied)
                {
                    // Discard the partial logical subtree; retain a denied node
                    // so discovery can apply the narrow normative boundary.
                    let prefix = format!("{child_key}/");
                    entries.retain(|key, _| key != &child_key && !key.starts_with(&prefix));
                    entries.insert(child_key, Entry::Denied);
                    if let Ok(resolved) = child.path().canonicalize() {
                        active.retain(|path| !path.starts_with(&resolved));
                    }
                } else {
                    return Err(error);
                }
            }
        }
        if path.canonicalize()? != resolved {
            bail!("source_changed");
        }
        active.remove(&resolved);
    } else if metadata.is_file() {
        if metadata.len() > limits.entry_bytes as u64 {
            bail!("entry_size_limit");
        }
        let mut file = File::open(&resolved)?;
        let before = file.metadata()?;
        if !same_file(&metadata, &before) {
            bail!("source_changed");
        }
        let mut bytes = Vec::new();
        let mut chunk = [0u8; 64 * 1024];
        loop {
            if cancelled() {
                bail!("cancelled");
            }
            let read = file.read(&mut chunk)?;
            if read == 0 {
                break;
            }
            if bytes
                .len()
                .checked_add(read)
                .is_none_or(|size| size > limits.entry_bytes)
            {
                bail!("entry_size_limit");
            }
            bytes.extend_from_slice(&chunk[..read]);
        }
        if bytes.len() > limits.entry_bytes {
            bail!("entry_size_limit");
        }
        if !same_file(&before, &file.metadata()?)
            || path.canonicalize()? != resolved
            || !same_file(&before, &fs::metadata(&resolved)?)
        {
            bail!("source_changed");
        }
        *total = total.checked_add(bytes.len()).context("expanded_limit")?;
        if *total > limits.expanded_bytes {
            bail!("expanded_limit");
        }
        entries.insert(
            key.into(),
            Entry::File {
                bytes,
                mode: mode(&before),
            },
        );
    } else {
        entries.insert(key.into(), Entry::Denied);
    }
    Ok(())
}
fn digest_entries<'a>(entries: impl Iterator<Item = (&'a str, &'a Entry)>) -> String {
    let mut hash = Sha256::new();
    hash.update(b"pioneer-plugin-tree-v1\0");
    for (key, entry) in entries {
        hash.update((key.len() as u64).to_le_bytes());
        hash.update(key.as_bytes());
        match entry {
            Entry::Directory { mode } => {
                hash.update(b"d");
                hash.update(mode.to_le_bytes());
            }
            Entry::File { bytes, mode } => {
                hash.update(b"f");
                hash.update(mode.to_le_bytes());
                hash.update((bytes.len() as u64).to_le_bytes());
                hash.update(bytes);
            }
            Entry::Denied => hash.update(b"x"),
        }
    }
    hex::encode(hash.finalize())
}
fn same_file(a: &fs::Metadata, b: &fs::Metadata) -> bool {
    let common = a.len() == b.len() && a.modified().ok() == b.modified().ok() && mode(a) == mode(b);
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        common
            && a.dev() == b.dev()
            && a.ino() == b.ino()
            && a.ctime() == b.ctime()
            && a.ctime_nsec() == b.ctime_nsec()
    }
    #[cfg(not(unix))]
    {
        common
    }
}
fn mode(metadata: &fs::Metadata) -> u32 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        metadata.permissions().mode() & 0o777
    }
    #[cfg(not(unix))]
    {
        if metadata.permissions().readonly() {
            0o555
        } else {
            0o755
        }
    }
}
