use anyhow::{Result, bail};
use std::path::{Component, Path, PathBuf};

/// Resolve an existing package path, including contained symlinks. The caller
/// must read through a checked handle or immutable snapshot, not this path after
/// arbitrary external work.
pub fn resolve_contained(root: &Path, path: &Path) -> Result<PathBuf> {
    let root = root.canonicalize()?;
    let resolved = path.canonicalize()?;
    if !resolved.starts_with(&root) {
        bail!("package_path_escape");
    }
    Ok(resolved)
}

/// For a data directory which may not exist yet, normalize first, then check
/// every existing ancestor. No directory is created during preview.
pub fn resolve_data_directory(root: &Path, suffix: &str) -> Result<PathBuf> {
    let root = root.canonicalize()?;
    let mut path = root.clone();
    for part in Path::new(suffix).components() {
        match part {
            Component::Normal(name) => path.push(name),
            Component::CurDir => {}
            Component::ParentDir if path != root => {
                path.pop();
            }
            _ => bail!("data_path_escape"),
        }
        if path.symlink_metadata().is_ok() {
            let resolved = path.canonicalize()?;
            if !resolved.starts_with(&root) || !resolved.is_dir() {
                bail!("data_path_escape");
            }
            path = resolved;
        }
    }
    Ok(path)
}
