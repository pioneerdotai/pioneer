use std::io;
use std::path::{Component, Path, PathBuf};

use pioneer_tools::apply_patch::file_mutation::open_regular_file;
use tokio::fs;
use tokio::io::AsyncReadExt;

use crate::error::{ArtifactError, ArtifactLocalPathRejectionKind, ArtifactResult};
use crate::mime::{detect_mime_from_bytes, is_safe_visible_name, sanitize_display_name};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactLocalPathPolicy {
    pub allowed_roots: Vec<PathBuf>,
    pub max_file_bytes: u64,
    pub follow_symlinks: bool,
}

impl ArtifactLocalPathPolicy {
    pub const DEFAULT_MAX_FILE_BYTES: u64 = 50 * 1024 * 1024;

    pub fn new(allowed_roots: Vec<PathBuf>) -> Self {
        Self {
            allowed_roots,
            max_file_bytes: Self::DEFAULT_MAX_FILE_BYTES,
            follow_symlinks: false,
        }
    }
}

impl Default for ArtifactLocalPathPolicy {
    fn default() -> Self {
        Self {
            allowed_roots: Vec::new(),
            max_file_bytes: Self::DEFAULT_MAX_FILE_BYTES,
            follow_symlinks: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedLocalFile {
    pub canonical_path: PathBuf,
    pub display_name: String,
    pub original_file_name: Option<String>,
    pub mime_type: String,
    pub bytes: Vec<u8>,
}

pub async fn read_validated_local_file(
    requested_path: &Path,
    policy: &ArtifactLocalPathPolicy,
) -> ArtifactResult<ValidatedLocalFile> {
    validate_requested_path(requested_path)?;

    let allowed_roots = canonical_allowed_roots(policy).await?;
    let symlink_metadata = fs::symlink_metadata(requested_path)
        .await
        .map_err(|source| local_path_open_error(requested_path, source))?;

    if symlink_metadata.file_type().is_symlink() && !policy.follow_symlinks {
        return Err(ArtifactError::local_path_rejected(
            ArtifactLocalPathRejectionKind::SymlinkNotAllowed,
            format!("symlink is not allowed: {}", requested_path.display()),
        ));
    }

    let canonical_path = fs::canonicalize(requested_path)
        .await
        .map_err(|source| local_path_open_error(requested_path, source))?;
    if !allowed_roots
        .iter()
        .any(|allowed_root| canonical_path.starts_with(allowed_root))
    {
        return Err(ArtifactError::local_path_rejected(
            ArtifactLocalPathRejectionKind::OutsideAllowedRoots,
            format!(
                "path is outside allowed roots: {}",
                requested_path.display()
            ),
        ));
    }

    // Canonicalization establishes that the observed target is inside an
    // allowed root, but it is not the read primitive: a parent can be swapped
    // after that check. Open the original no-follow path component-by-
    // component and consume bytes only from the pinned descriptor. When the
    // caller explicitly permits symlinks, the already-authorized canonical
    // path is the descriptor target instead.
    let open_path = if policy.follow_symlinks {
        canonical_path.clone()
    } else {
        requested_path.to_path_buf()
    };
    let diagnostic_path = requested_path.to_path_buf();
    let file = tokio::task::spawn_blocking(move || open_regular_file(open_path.as_path()))
        .await
        .map_err(|error| ArtifactError::LocalPathReadFailed {
            path: diagnostic_path.clone(),
            source: io::Error::other(format!("secure artifact open worker failed: {error}")),
        })?
        .map_err(|source| secure_local_path_open_error(diagnostic_path.as_path(), source))?;
    let metadata = file
        .metadata()
        .map_err(|source| ArtifactError::LocalPathReadFailed {
            path: canonical_path.clone(),
            source,
        })?;
    if metadata.len() > policy.max_file_bytes {
        return Err(ArtifactError::local_path_rejected(
            ArtifactLocalPathRejectionKind::FileTooLarge,
            format!(
                "file size {} exceeds limit {}",
                metadata.len(),
                policy.max_file_bytes
            ),
        ));
    }

    let mut bytes = Vec::new();
    let mut bounded = tokio::fs::File::from_std(file).take(policy.max_file_bytes.saturating_add(1));
    bounded
        .read_to_end(&mut bytes)
        .await
        .map_err(|source| ArtifactError::LocalPathReadFailed {
            path: canonical_path.clone(),
            source,
        })?;
    if bytes.len() as u64 > policy.max_file_bytes {
        return Err(ArtifactError::local_path_rejected(
            ArtifactLocalPathRejectionKind::FileTooLarge,
            format!(
                "file size after read {} exceeds limit {}",
                bytes.len(),
                policy.max_file_bytes
            ),
        ));
    }

    let original_file_name = requested_path
        .file_name()
        .and_then(|value| value.to_str())
        .map(ToOwned::to_owned);
    let display_name = canonical_path
        .file_name()
        .and_then(|value| value.to_str())
        .map(sanitize_display_name)
        .unwrap_or_else(|| "artifact".to_owned());
    let original_file_name = original_file_name
        .as_deref()
        .filter(|value| is_safe_visible_name(value))
        .map(ToOwned::to_owned);
    let mime_type = detect_mime_from_bytes(bytes.as_slice(), Some(&canonical_path));

    Ok(ValidatedLocalFile {
        canonical_path,
        display_name,
        original_file_name,
        mime_type,
        bytes,
    })
}

async fn canonical_allowed_roots(policy: &ArtifactLocalPathPolicy) -> ArtifactResult<Vec<PathBuf>> {
    if policy.allowed_roots.is_empty() {
        return Err(ArtifactError::local_path_rejected(
            ArtifactLocalPathRejectionKind::InvalidAllowedRoot,
            "allowed roots are required",
        ));
    }
    if policy.max_file_bytes == 0 {
        return Err(ArtifactError::local_path_rejected(
            ArtifactLocalPathRejectionKind::InvalidPath,
            "max_file_bytes must be greater than zero",
        ));
    }

    let mut roots = Vec::with_capacity(policy.allowed_roots.len());
    for root in &policy.allowed_roots {
        validate_allowed_root_path(root)?;
        let canonical = fs::canonicalize(root)
            .await
            .map_err(|source| invalid_allowed_root_error(root, source))?;
        let metadata = fs::metadata(&canonical).await.map_err(|source| {
            ArtifactError::LocalPathReadFailed {
                path: canonical.clone(),
                source,
            }
        })?;
        if !metadata.is_dir() {
            return Err(ArtifactError::local_path_rejected(
                ArtifactLocalPathRejectionKind::InvalidAllowedRoot,
                format!("allowed root is not a directory: {}", root.display()),
            ));
        }
        roots.push(canonical);
    }
    Ok(roots)
}

fn validate_requested_path(path: &Path) -> ArtifactResult<()> {
    if path_contains_nul(path) {
        return Err(ArtifactError::local_path_rejected(
            ArtifactLocalPathRejectionKind::InvalidPath,
            format!("path contains null byte: {}", path.display()),
        ));
    }
    if path
        .components()
        .any(|component| component == Component::ParentDir)
    {
        return Err(ArtifactError::local_path_rejected(
            ArtifactLocalPathRejectionKind::InvalidPath,
            format!(
                "path traversal component is not allowed: {}",
                path.display()
            ),
        ));
    }
    Ok(())
}

fn validate_allowed_root_path(path: &Path) -> ArtifactResult<()> {
    if path_contains_nul(path) {
        return Err(ArtifactError::local_path_rejected(
            ArtifactLocalPathRejectionKind::InvalidAllowedRoot,
            format!("allowed root contains null byte: {}", path.display()),
        ));
    }
    Ok(())
}

fn local_path_open_error(path: &Path, source: io::Error) -> ArtifactError {
    if source.kind() == io::ErrorKind::NotFound {
        return ArtifactError::local_path_rejected(
            ArtifactLocalPathRejectionKind::FileNotFound,
            format!("file not found: {}", path.display()),
        );
    }
    ArtifactError::LocalPathReadFailed {
        path: path.to_path_buf(),
        source,
    }
}

fn secure_local_path_open_error(path: &Path, source: io::Error) -> ArtifactError {
    if source.kind() == io::ErrorKind::NotFound {
        return local_path_open_error(path, source);
    }
    if source.kind() == io::ErrorKind::InvalidInput {
        return ArtifactError::local_path_rejected(
            ArtifactLocalPathRejectionKind::NotRegularFile,
            format!("path is not a regular file: {}", path.display()),
        );
    }
    if matches!(
        source.kind(),
        io::ErrorKind::PermissionDenied | io::ErrorKind::NotADirectory
    ) {
        return ArtifactError::local_path_rejected(
            ArtifactLocalPathRejectionKind::SymlinkNotAllowed,
            format!(
                "path changed or contains a symlink component: {}",
                path.display()
            ),
        );
    }
    ArtifactError::LocalPathReadFailed {
        path: path.to_path_buf(),
        source,
    }
}

fn invalid_allowed_root_error(path: &Path, source: io::Error) -> ArtifactError {
    if source.kind() == io::ErrorKind::NotFound {
        return ArtifactError::local_path_rejected(
            ArtifactLocalPathRejectionKind::InvalidAllowedRoot,
            format!("allowed root not found: {}", path.display()),
        );
    }
    ArtifactError::LocalPathReadFailed {
        path: path.to_path_buf(),
        source,
    }
}

#[cfg(unix)]
fn path_contains_nul(path: &Path) -> bool {
    use std::os::unix::ffi::OsStrExt;

    path.as_os_str().as_bytes().contains(&0)
}

#[cfg(windows)]
fn path_contains_nul(path: &Path) -> bool {
    use std::os::windows::ffi::OsStrExt;

    path.as_os_str().encode_wide().any(|unit| unit == 0)
}

#[cfg(not(any(unix, windows)))]
fn path_contains_nul(_path: &Path) -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn safe_regular_file_inside_allowed_root_succeeds() {
        let temp = tempfile::tempdir().expect("tempdir");
        let root = temp.path().join("allowed");
        tokio::fs::create_dir_all(root.as_path())
            .await
            .expect("create root");
        let file = root.join("report.txt");
        tokio::fs::write(file.as_path(), b"hello")
            .await
            .expect("write file");

        let local_file =
            read_validated_local_file(file.as_path(), &ArtifactLocalPathPolicy::new(vec![root]))
                .await
                .expect("read safe file");

        assert_eq!(local_file.display_name, "report.txt");
        assert_eq!(local_file.bytes, b"hello");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn safe_path_rejects_parent_symlink_even_when_it_points_inside_root() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().expect("tempdir");
        let root = temp.path().join("allowed");
        let real = root.join("real");
        tokio::fs::create_dir_all(real.as_path())
            .await
            .expect("create real parent");
        tokio::fs::write(real.join("report.txt"), b"inside")
            .await
            .expect("write report");
        let linked_parent = root.join("linked-parent");
        symlink(real.as_path(), linked_parent.as_path()).expect("create parent symlink");

        let error = read_validated_local_file(
            linked_parent.join("report.txt").as_path(),
            &ArtifactLocalPathPolicy::new(vec![root]),
        )
        .await
        .expect_err("no-follow policy must reject parent symlinks");

        assert_eq!(
            rejection_kind(error),
            ArtifactLocalPathRejectionKind::SymlinkNotAllowed
        );
    }

    #[tokio::test]
    async fn safe_path_outside_allowed_root_is_rejected() {
        let temp = tempfile::tempdir().expect("tempdir");
        let root = temp.path().join("allowed");
        tokio::fs::create_dir_all(root.as_path())
            .await
            .expect("create root");
        let outside = temp.path().join("outside.txt");
        tokio::fs::write(outside.as_path(), b"outside")
            .await
            .expect("write outside");

        let error =
            read_validated_local_file(outside.as_path(), &ArtifactLocalPathPolicy::new(vec![root]))
                .await
                .expect_err("outside path should be rejected");

        assert_eq!(
            rejection_kind(error),
            ArtifactLocalPathRejectionKind::OutsideAllowedRoots
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn safe_path_null_byte_input_is_rejected() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let temp = tempfile::tempdir().expect("tempdir");
        let root = temp.path().join("allowed");
        tokio::fs::create_dir_all(root.as_path())
            .await
            .expect("create root");
        let invalid = PathBuf::from(OsString::from_vec(b"bad\0path.txt".to_vec()));

        let error =
            read_validated_local_file(invalid.as_path(), &ArtifactLocalPathPolicy::new(vec![root]))
                .await
                .expect_err("null byte path should be rejected");

        assert_eq!(
            rejection_kind(error),
            ArtifactLocalPathRejectionKind::InvalidPath
        );
    }

    fn rejection_kind(error: ArtifactError) -> ArtifactLocalPathRejectionKind {
        match error {
            ArtifactError::LocalPathRejected { kind, .. } => kind,
            other => panic!("unexpected error: {other}"),
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn safe_path_leaf_symlinks_are_rejected_before_target_authorization() {
        use std::os::unix::fs::symlink;

        for (target_relative, payload, link_name, write_message, expected) in [
            (
                "allowed/target.txt",
                b"target".as_slice(),
                "link.txt",
                "write target",
                ArtifactLocalPathRejectionKind::SymlinkNotAllowed,
            ),
            (
                "outside.txt",
                b"outside".as_slice(),
                "outside-link.txt",
                "write outside",
                ArtifactLocalPathRejectionKind::SymlinkNotAllowed,
            ),
        ] {
            let temp = tempfile::tempdir().expect("tempdir");
            let root = temp.path().join("allowed");
            tokio::fs::create_dir_all(root.as_path())
                .await
                .expect("create root");
            let target = temp.path().join(target_relative);
            tokio::fs::write(target.as_path(), payload)
                .await
                .expect(write_message);
            let link = root.join(link_name);
            symlink(target.as_path(), link.as_path()).expect("create symlink");

            let policy = ArtifactLocalPathPolicy::new(vec![root]);
            let error = read_validated_local_file(link.as_path(), &policy)
                .await
                .expect_err("symlink should be rejected");

            assert_eq!(rejection_kind(error), expected, "{target_relative}");
        }
    }

    #[test]
    fn empty_allowed_root_preserves_directory_traversal_and_missing_error_kinds() {
        for (components, expected, diagnostic) in [
            (
                &[][..],
                ArtifactLocalPathRejectionKind::NotRegularFile,
                "directory should be rejected",
            ),
            (
                &["nested", "..", "file.txt"][..],
                ArtifactLocalPathRejectionKind::InvalidPath,
                "traversal path should be rejected",
            ),
            (
                &["missing.txt"][..],
                ArtifactLocalPathRejectionKind::FileNotFound,
                "missing path should be rejected",
            ),
        ] {
            let body = async {
                let temp = tempfile::tempdir().expect("tempdir");
                let root = temp.path().join("allowed");
                tokio::fs::create_dir_all(root.as_path())
                    .await
                    .expect("create root");
                let mut requested = root.clone();
                for component in components {
                    requested.push(component);
                }
                let error = read_validated_local_file(
                    requested.as_path(),
                    &ArtifactLocalPathPolicy::new(vec![root]),
                )
                .await
                .expect_err(diagnostic);
                assert_eq!(rejection_kind(error), expected, "{components:?}");
            };
            let body = std::pin::pin!(body);
            let body: std::pin::Pin<&mut dyn std::future::Future<Output = ()>> = body;
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime")
                .block_on(body);
        }
    }
}
