use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::Read,
    path::{Component, Path, PathBuf},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectionKind {
    File,
    Directory,
    FileOrDirectory,
}

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub expanded_bytes: usize,
    pub entry_bytes: usize,
    pub entries: usize,
    pub depth: usize,
    pub components: usize,
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
    /// database capacity. Each file is verified while capturing its bytes.
    pub fn capture(root: &Path, limits: Limits, cancelled: impl Fn() -> bool) -> Result<Self> {
        let root = root.canonicalize().context("source_unavailable")?;
        if !root.is_dir() {
            bail!("source_kind");
        }
        capture_once(&root, limits, &cancelled)
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
    /// Remove unneeded payload subtrees without changing captured bytes, modes or limits.
    pub fn excluding_subtrees(&self, excluded: &[String]) -> Result<Self> {
        let mut entries: BTreeMap<String, Entry> = self
            .entries
            .iter()
            .filter(|(key, _)| {
                !excluded
                    .iter()
                    .any(|prefix| *key == prefix || key.starts_with(&format!("{prefix}/")))
            })
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        // Ancestors which existed only for an excluded variant are not payload.
        let candidates: Vec<_> = entries
            .iter()
            .filter(|(key, entry)| {
                matches!(entry, Entry::Directory { .. })
                    && excluded
                        .iter()
                        .any(|path| path.starts_with(&format!("{key}/")))
            })
            .map(|(key, _)| key.clone())
            .collect();
        for key in candidates.into_iter().rev() {
            if !entries
                .keys()
                .any(|path| path.starts_with(&format!("{key}/")))
            {
                entries.remove(&key);
            }
        }
        let mut selected = Self::from_entries(entries, self.limits)?;
        selected.root_mode = self.root_mode;
        Ok(selected)
    }
    pub fn root_mode(&self) -> u32 {
        self.root_mode
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

impl Snapshot {
    /// Selective capture shared by App folder packaging and offline validation.
    /// Unlike Plugin capture this rejects all selected symlinks and special files.
    pub fn capture_selected(
        root: &Path,
        limits: Limits,
        selected: &BTreeMap<String, SelectionKind>,
        cancelled: &impl Fn() -> bool,
    ) -> Result<Self> {
        if selected_link(&fs::symlink_metadata(root)?) {
            bail!("selected_symlink");
        }
        let root = root.canonicalize()?;
        let root_before = fs::symlink_metadata(&root)?;
        if !root_before.is_dir() {
            bail!("source_kind");
        }
        let selected_root = SelectedRoot::open(root.clone(), &root_before)?;
        let mut entries = BTreeMap::new();
        let mut observed = BTreeMap::new();
        let mut total = 0;
        for (key, kind) in selected {
            validate_key(key, limits.depth)?;
            // Read only the ancestor chain, never its siblings.
            let mut parent = Path::new(key).parent();
            let mut parents = Vec::new();
            while let Some(p) = parent.filter(|p| !p.as_os_str().is_empty()) {
                parents.push(p);
                parent = p.parent();
            }
            for p in parents.into_iter().rev() {
                let key = p.to_str().context("invalid_path")?;
                if !entries.contains_key(key) {
                    let metadata = selected_metadata(&selected_root, key)?;
                    if !metadata.is_dir() {
                        bail!("selected_path_kind: {key}");
                    }
                    if entries.len() >= limits.entries {
                        bail!("entry_limit");
                    }
                    entries.insert(
                        key.into(),
                        Entry::Directory {
                            mode: mode(&metadata),
                        },
                    );
                    observed.insert(key.into(), metadata);
                }
            }
            let metadata = selected_metadata(&selected_root, key)?;
            if (*kind == SelectionKind::File && !metadata.is_file())
                || (*kind == SelectionKind::Directory && !metadata.is_dir())
            {
                bail!("selected_path_kind: {key}");
            }
            visit_selected(
                &selected_root,
                key,
                limits,
                cancelled,
                &mut entries,
                &mut observed,
                &mut total,
            )?;
        }
        // Detect replacement or edits of any captured member/ancestor, including directory membership.
        for (key, before) in &observed {
            if cancelled() {
                bail!("cancelled");
            }
            if !same_file(before, &selected_metadata(&selected_root, key)?) {
                bail!("source_changed: {key}");
            }
        }
        // Root sibling edits are excluded from the package; only root identity/mode matter.
        let root_after = fs::symlink_metadata(&selected_root.path)?;
        if !root_after.is_dir()
            || selected_link(&root_after)
            || !same_identity(&root_before, &root_after)
            || mode(&root_before) != mode(&root_after)
        {
            bail!("source_changed");
        }
        let mut snapshot = Self::from_entries(entries, limits)?;
        snapshot.root_mode = mode(&root_before);
        Ok(snapshot)
    }
}
fn selected_metadata(root: &SelectedRoot, key: &str) -> Result<fs::Metadata> {
    #[cfg(any(unix, windows))]
    {
        return root.open_member(key)?.metadata().map_err(Into::into);
    }
    #[cfg(not(any(unix, windows)))]
    {
        let mut path = root.path.clone();
        // Reject symlinks in ancestors as well as the final selected entry.
        for part in key.split('/') {
            path.push(part);
            let m = fs::symlink_metadata(&path)?;
            if m.file_type().is_symlink() {
                bail!("selected_symlink: {key}");
            }
            if !m.is_file() && !m.is_dir() {
                bail!("selected_special_file: {key}");
            }
        }
        if !path.canonicalize()?.starts_with(&root.path) {
            bail!("package_path_escape");
        }
        Ok(fs::symlink_metadata(path)?)
    }
}
#[allow(clippy::too_many_arguments)]
fn visit_selected(
    root: &SelectedRoot,
    key: &str,
    limits: Limits,
    cancelled: &impl Fn() -> bool,
    entries: &mut BTreeMap<String, Entry>,
    observed: &mut BTreeMap<String, fs::Metadata>,
    total: &mut usize,
) -> Result<()> {
    if cancelled() {
        bail!("cancelled");
    }
    validate_key(key, limits.depth)?;
    // Already captured subtrees are deduplicated, preserving the first immutable bytes.
    if observed.contains_key(key) && matches!(entries.get(key), Some(Entry::File { .. })) {
        return Ok(());
    }
    let metadata = selected_metadata(root, key)?;
    if !entries.contains_key(key) && entries.len() >= limits.entries {
        bail!("entry_limit");
    }
    if metadata.is_dir() {
        entries.insert(
            key.into(),
            Entry::Directory {
                mode: mode(&metadata),
            },
        );
        observed.entry(key.into()).or_insert(metadata);
        let mut children = root.names(key, limits.entries, cancelled)?;
        children.sort();
        for name in children {
            visit_selected(
                root,
                &format!("{key}/{name}"),
                limits,
                cancelled,
                entries,
                observed,
                total,
            )?;
        }
    } else {
        if metadata.len() > limits.entry_bytes as u64 {
            bail!("entry_size_limit");
        }
        let mut file = root.open_member(key)?;
        if !same_file(&metadata, &file.metadata()?) {
            bail!("source_changed: {key}");
        }
        let mut bytes = Vec::new();
        let mut chunk = [0; 64 * 1024];
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
                .is_none_or(|n| n > limits.entry_bytes)
                || total
                    .checked_add(bytes.len())
                    .and_then(|n| n.checked_add(read))
                    .is_none_or(|n| n > limits.expanded_bytes)
            {
                bail!("expanded_limit");
            }
            bytes.extend_from_slice(&chunk[..read]);
        }
        if !same_file(&metadata, &file.metadata()?)
            || !same_file(&metadata, &selected_metadata(root, key)?)
        {
            bail!("source_changed: {key}");
        }
        *total += bytes.len();
        entries.insert(
            key.into(),
            Entry::File {
                bytes,
                mode: mode(&metadata),
            },
        );
        observed.insert(key.into(), metadata);
    }
    Ok(())
}

fn same_identity(a: &fs::Metadata, b: &fs::Metadata) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        a.dev() == b.dev() && a.ino() == b.ino()
    }
    #[cfg(not(unix))]
    {
        a.created().ok() == b.created().ok()
    }
}

// A held root and no-follow opens anchor every Unix member read. Path-only
// metadata followed by File::open would allow an ancestor symlink substitution.
struct SelectedRoot {
    path: PathBuf,
    #[cfg(any(unix, windows))]
    handle: File,
    #[cfg(windows)]
    pins: std::sync::Mutex<BTreeMap<String, File>>,
}
impl SelectedRoot {
    fn open(path: PathBuf, before: &fs::Metadata) -> Result<Self> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            let handle = File::options()
                .read(true)
                .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(&path)?;
            let actual = handle.metadata()?;
            if !same_identity(before, &actual) || mode(before) != mode(&actual) {
                bail!("source_changed");
            }
            Ok(Self { path, handle })
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            // OPEN_REPARSE_POINT | BACKUP_SEMANTICS, shared read only: pin the
            // selected root against writes, junction replacement and rename.
            let handle = File::options()
                .read(true)
                .share_mode(1)
                .custom_flags(0x00200000 | 0x02000000)
                .open(&path)?;
            let actual = handle.metadata()?;
            if selected_link(&actual) || !actual.is_dir() || !same_identity(before, &actual) {
                bail!("selected_symlink_or_source_changed");
            }
            Ok(Self {
                path,
                handle,
                pins: std::sync::Mutex::new(BTreeMap::new()),
            })
        }
        #[cfg(not(any(unix, windows)))]
        {
            let _ = before;
            Ok(Self { path })
        }
    }
    fn open_member(&self, key: &str) -> Result<File> {
        #[cfg(unix)]
        {
            use std::{
                ffi::CString,
                os::fd::{AsRawFd, FromRawFd},
            };
            let mut current = self.handle.try_clone()?;
            let mut parts = key.split('/').peekable();
            while let Some(part) = parts.next() {
                let name = CString::new(part)?;
                let flags = libc::O_RDONLY
                    | libc::O_NOFOLLOW
                    | libc::O_NONBLOCK
                    | libc::O_CLOEXEC
                    | if parts.peek().is_some() {
                        libc::O_DIRECTORY
                    } else {
                        0
                    };
                // SAFETY: parent is held open; name is NUL-terminated; successful
                // descriptors are immediately transferred to their sole File owner.
                let fd = unsafe { libc::openat(current.as_raw_fd(), name.as_ptr(), flags) };
                if fd < 0 {
                    let error = std::io::Error::last_os_error();
                    if error.raw_os_error() == Some(libc::ELOOP) {
                        bail!("selected_symlink: {key}");
                    }
                    return Err(error).with_context(|| format!("selected_path_denied: {key}"));
                }
                current = unsafe { File::from_raw_fd(fd) };
                let metadata = current.metadata()?;
                if !metadata.is_file() && !metadata.is_dir() {
                    bail!("selected_special_file: {key}");
                }
            }
            Ok(current)
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            let mut path = self.path.clone();
            if selected_link(&self.handle.metadata()?) {
                bail!("selected_symlink");
            }
            let mut prefix = String::new();
            let mut parts = key.split('/').peekable();
            let mut final_file = None;
            let mut pins = self.pins.lock().expect("selected source handles poisoned");
            while let Some(part) = parts.next() {
                path.push(part);
                if !prefix.is_empty() {
                    prefix.push('/');
                }
                prefix.push_str(part);
                if let Some(file) = pins.get(&prefix) {
                    final_file = Some(file.try_clone()?);
                    continue;
                }
                // Every ancestor remains pinned before opening its child. No
                // reparse point is followed; write/delete sharing stays denied
                // until the entire immutable snapshot has been captured.
                let file = File::options()
                    .read(true)
                    .share_mode(1)
                    .custom_flags(0x00200000 | 0x02000000)
                    .open(&path)?;
                let metadata = file.metadata()?;
                if selected_link(&metadata) {
                    bail!("selected_symlink: {key}");
                }
                if (!metadata.is_file() && !metadata.is_dir())
                    || (parts.peek().is_some() && !metadata.is_dir())
                {
                    bail!("selected_special_file: {key}");
                }
                final_file = Some(file.try_clone()?);
                pins.insert(prefix.clone(), file);
            }
            final_file.context("invalid_path")
        }
        #[cfg(not(any(unix, windows)))]
        {
            let before = selected_metadata(self, key)?;
            let file = File::open(self.path.join(key))?;
            if !same_file(&before, &file.metadata()?) {
                bail!("source_changed: {key}");
            }
            Ok(file)
        }
    }

    fn names(&self, key: &str, limit: usize, cancelled: &impl Fn() -> bool) -> Result<Vec<String>> {
        #[cfg(all(
            unix,
            any(target_vendor = "apple", target_os = "linux", target_os = "android")
        ))]
        {
            use std::{
                ffi::CStr,
                os::fd::{FromRawFd, IntoRawFd},
            };
            let fd = self.open_member(key)?.into_raw_fd();
            // SAFETY: fd is a uniquely owned directory descriptor. fdopendir
            // takes it on success; a failed call leaves ownership with us.
            let pointer = unsafe { libc::fdopendir(fd) };
            if pointer.is_null() {
                let error = std::io::Error::last_os_error();
                drop(unsafe { File::from_raw_fd(fd) });
                return Err(error.into());
            }
            struct Directory(*mut libc::DIR);
            impl Drop for Directory {
                fn drop(&mut self) {
                    unsafe {
                        libc::closedir(self.0);
                    }
                }
            }
            let directory = Directory(pointer);
            let mut names = Vec::new();
            loop {
                if cancelled() {
                    bail!("cancelled");
                }
                #[cfg(target_vendor = "apple")]
                let errno = unsafe { libc::__error() };
                #[cfg(target_os = "linux")]
                let errno = unsafe { libc::__errno_location() };
                #[cfg(target_os = "android")]
                let errno = unsafe { libc::__errno() };
                // SAFETY: readdir's buffer is copied before the next call; the
                // directory is private to this thread and alive for the loop.
                unsafe {
                    *errno = 0;
                }
                let entry = unsafe { libc::readdir(directory.0) };
                if entry.is_null() {
                    let error = unsafe { *errno };
                    if error != 0 {
                        return Err(std::io::Error::from_raw_os_error(error).into());
                    }
                    break;
                }
                let bytes = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) }.to_bytes();
                if bytes == b"." || bytes == b".." {
                    continue;
                }
                if names.len() >= limit {
                    bail!("entry_limit");
                }
                names.push(
                    std::str::from_utf8(bytes)
                        .context("invalid_path")?
                        .to_owned(),
                );
            }
            Ok(names)
        }
        #[cfg(not(all(
            unix,
            any(target_vendor = "apple", target_os = "linux", target_os = "android")
        )))]
        {
            #[cfg(windows)]
            let _directory = self.open_member(key)?;
            let mut names = Vec::new();
            for entry in fs::read_dir(self.path.join(key))?.take(limit.saturating_add(1)) {
                if cancelled() {
                    bail!("cancelled");
                }
                if names.len() >= limit {
                    bail!("entry_limit");
                }
                names.push(
                    entry?
                        .file_name()
                        .into_string()
                        .map_err(|_| anyhow::anyhow!("invalid_path"))?,
                );
            }
            Ok(names)
        }
    }
}

fn selected_link(metadata: &fs::Metadata) -> bool {
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        metadata.file_attributes() & 0x400 != 0
    }
    #[cfg(not(windows))]
    {
        metadata.file_type().is_symlink()
    }
}
