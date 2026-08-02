//! Private cross-platform guards for files owned or consumed by the client.

use std::fs::Metadata;
use std::io;
use std::path::{Component, Path};

pub(crate) fn configure_std_no_follow(options: &mut std::fs::OpenOptions) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt as _;
        use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT;

        options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    }
}

pub(crate) fn configure_tokio_no_follow(options: &mut tokio::fs::OpenOptions) {
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW);
    #[cfg(windows)]
    options.custom_flags(
        windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT,
    );
}

pub(crate) fn metadata_is_plain_file(metadata: &Metadata) -> bool {
    if !metadata.file_type().is_file() {
        return false;
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt as _;
        use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;

        if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return false;
        }
    }
    true
}

pub(crate) fn metadata_is_plain_directory(metadata: &Metadata) -> bool {
    if !metadata.file_type().is_dir() {
        return false;
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt as _;
        use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;

        if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return false;
        }
    }
    true
}

/// Creates an application-owned descendant directory without accepting a
/// symlink/reparse point at any component below `owned_root`.
///
/// `owned_root` is the caller-selected app-data authority. Its parent chain is
/// intentionally outside this helper's ownership, but the root itself and all
/// descendants used for private caches must be real directories.
pub(crate) async fn ensure_owned_directory(
    owned_root: &Path,
    directory: &Path,
) -> io::Result<()> {
    let relative = owned_descendant(owned_root, directory)?;
    tokio::fs::create_dir_all(owned_root).await?;
    require_plain_directory(owned_root).await?;

    let mut current = owned_root.to_path_buf();
    for component in relative.components() {
        let Component::Normal(component) = component else {
            return Err(invalid_owned_path());
        };
        current.push(component);
        match tokio::fs::create_dir(current.as_path()).await {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
        require_plain_directory(current.as_path()).await?;
    }
    Ok(())
}

/// Removes an application-owned directory only after proving that every
/// component below `owned_root` is a real directory. A substituted symlink or
/// Windows reparse point is rejected instead of followed.
pub(crate) fn remove_owned_directory(owned_root: &Path, directory: &Path) -> io::Result<()> {
    let relative = owned_descendant(owned_root, directory)?;
    let mut current = owned_root.to_path_buf();
    match std::fs::symlink_metadata(current.as_path()) {
        Ok(metadata) if metadata_is_plain_directory(&metadata) => {}
        Ok(_) => return Err(unsafe_owned_directory()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    }

    let components = relative.components().collect::<Vec<_>>();
    for (index, component) in components.iter().enumerate() {
        let Component::Normal(component) = component else {
            return Err(invalid_owned_path());
        };
        current.push(component);
        let is_target = index + 1 == components.len();
        match std::fs::symlink_metadata(current.as_path()) {
            Ok(metadata) if metadata_is_plain_directory(&metadata) => {
                if is_target {
                    return std::fs::remove_dir_all(current);
                }
            }
            Ok(_) => return Err(unsafe_owned_directory()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn owned_descendant<'a>(owned_root: &'a Path, path: &'a Path) -> io::Result<&'a Path> {
    if !owned_root.is_absolute() || !path.is_absolute() {
        return Err(invalid_owned_path());
    }
    let relative = path.strip_prefix(owned_root).map_err(|_| invalid_owned_path())?;
    if relative
        .components()
        .any(|component| !matches!(component, Component::Normal(_)))
        && !relative.as_os_str().is_empty()
    {
        return Err(invalid_owned_path());
    }
    Ok(relative)
}

async fn require_plain_directory(path: &Path) -> io::Result<()> {
    let metadata = tokio::fs::symlink_metadata(path).await?;
    if metadata_is_plain_directory(&metadata) {
        Ok(())
    } else {
        Err(unsafe_owned_directory())
    }
}

fn unsafe_owned_directory() -> io::Error {
    io::Error::new(
        io::ErrorKind::PermissionDenied,
        "owned cache path is not a plain directory",
    )
}

fn invalid_owned_path() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "owned cache path is outside its app-data root",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[tokio::test]
    async fn owned_cache_helpers_never_follow_a_substituted_directory() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let owned_root = temp.path().join("app-data");
        let outside = temp.path().join("outside");
        std::fs::create_dir_all(owned_root.as_path()).unwrap();
        std::fs::create_dir_all(outside.join("avatars")).unwrap();
        std::fs::write(outside.join("avatars/keep"), b"keep").unwrap();
        symlink(outside.as_path(), owned_root.join("cache")).unwrap();

        assert!(
            ensure_owned_directory(
                owned_root.as_path(),
                owned_root.join("cache/avatars/members").as_path(),
            )
            .await
            .is_err()
        );
        assert!(
            remove_owned_directory(
                owned_root.as_path(),
                owned_root.join("cache/avatars").as_path(),
            )
            .is_err()
        );
        assert_eq!(std::fs::read(outside.join("avatars/keep")).unwrap(), b"keep");
    }
}
