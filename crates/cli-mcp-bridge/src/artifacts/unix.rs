use super::{PrivateArtifactError, encode_bootstrap};
use crate::BootstrapDocument;
use std::fs::{self, DirBuilder, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use zeroize::Zeroize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Identity {
    device: u64,
    inode: u64,
    owner: u32,
}

#[derive(Debug)]
pub struct PrivateSessionDirectory {
    path: PathBuf,
    identity: Identity,
    cleanup_armed: bool,
}

#[derive(Debug)]
pub struct PrivateBootstrapArtifact {
    path: PathBuf,
    identity: Identity,
    cleanup_armed: bool,
}

pub(super) fn create(
    root: &Path,
    directory_name: &str,
) -> Result<PrivateSessionDirectory, PrivateArtifactError> {
    ensure_owner_directory(root, true)?;
    let path = root.join(directory_name);
    let mut builder = DirBuilder::new();
    builder.mode(0o700);
    match builder.create(&path) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            return Err(PrivateArtifactError::AlreadyExists);
        }
        Err(error) => return Err(error.into()),
    }
    fs::set_permissions(&path, fs::Permissions::from_mode(0o700))?;
    let identity = directory_identity(&path)?;
    Ok(PrivateSessionDirectory {
        path,
        identity,
        cleanup_armed: true,
    })
}

impl PrivateSessionDirectory {
    pub fn path(&self) -> &Path {
        self.path.as_path()
    }

    pub fn write_bootstrap(
        &self,
        document: &BootstrapDocument,
    ) -> Result<PrivateBootstrapArtifact, PrivateArtifactError> {
        revalidate_directory(&self.path, self.identity)?;
        let path = self.path.join("bootstrap.json");
        let mut encoded = encode_bootstrap(document)?;
        let result = write_new_private_file(&path, encoded.as_slice());
        encoded.zeroize();
        let identity = result?;
        Ok(PrivateBootstrapArtifact {
            path,
            identity,
            cleanup_armed: true,
        })
    }

    pub fn cleanup(&mut self) -> Result<(), PrivateArtifactError> {
        if !self.cleanup_armed {
            return Ok(());
        }
        revalidate_directory(&self.path, self.identity)?;
        fs::remove_dir(&self.path)?;
        self.cleanup_armed = false;
        Ok(())
    }
}

impl Drop for PrivateSessionDirectory {
    fn drop(&mut self) {
        let _ = self.cleanup();
    }
}

impl PrivateBootstrapArtifact {
    pub fn path(&self) -> &Path {
        self.path.as_path()
    }

    pub fn is_consumed(&self) -> Result<bool, PrivateArtifactError> {
        match fs::symlink_metadata(&self.path) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(true),
            Err(error) => Err(error.into()),
            Ok(_) => {
                if file_identity(&self.path)? != self.identity {
                    return Err(PrivateArtifactError::Replaced);
                }
                Ok(false)
            }
        }
    }

    pub fn cleanup(&mut self) -> Result<(), PrivateArtifactError> {
        if !self.cleanup_armed {
            return Ok(());
        }
        match fs::symlink_metadata(&self.path) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
            Ok(_) => {
                if file_identity(&self.path)? != self.identity {
                    return Err(PrivateArtifactError::Replaced);
                }
                fs::remove_file(&self.path)?;
            }
        }
        self.cleanup_armed = false;
        Ok(())
    }
}

impl Drop for PrivateBootstrapArtifact {
    fn drop(&mut self) {
        let _ = self.cleanup();
    }
}

fn ensure_owner_directory(path: &Path, create: bool) -> Result<(), PrivateArtifactError> {
    if create {
        match fs::symlink_metadata(path) {
            Ok(metadata)
                if metadata.file_type().is_symlink()
                    || !metadata.is_dir()
                    || metadata.uid() != current_uid() =>
            {
                return Err(PrivateArtifactError::InsecureRoot);
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                fs::create_dir_all(path)?;
            }
            Err(error) => return Err(error.into()),
        }
    }
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() || metadata.uid() != current_uid() {
        return Err(PrivateArtifactError::InsecureRoot);
    }
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink()
        || !metadata.is_dir()
        || metadata.uid() != current_uid()
        || metadata.mode() & 0o077 != 0
    {
        return Err(PrivateArtifactError::InsecureRoot);
    }
    Ok(())
}

fn write_new_private_file(path: &Path, bytes: &[u8]) -> Result<Identity, PrivateArtifactError> {
    let mut options = OpenOptions::new();
    options
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW);
    let mut file = options.open(path).map_err(|error| {
        if error.kind() == io::ErrorKind::AlreadyExists {
            PrivateArtifactError::AlreadyExists
        } else {
            error.into()
        }
    })?;
    let result = (|| {
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
        file_identity(path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(path);
    }
    result
}

fn directory_identity(path: &Path) -> Result<Identity, PrivateArtifactError> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink()
        || !metadata.is_dir()
        || metadata.uid() != current_uid()
        || metadata.mode() & 0o077 != 0
    {
        return Err(PrivateArtifactError::InsecureRoot);
    }
    Ok(identity(&metadata))
}

fn file_identity(path: &Path) -> Result<Identity, PrivateArtifactError> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.nlink() != 1
        || metadata.uid() != current_uid()
        || metadata.mode() & 0o077 != 0
    {
        return Err(PrivateArtifactError::Replaced);
    }
    Ok(identity(&metadata))
}

fn identity(metadata: &fs::Metadata) -> Identity {
    Identity {
        device: metadata.dev(),
        inode: metadata.ino(),
        owner: metadata.uid(),
    }
}

fn revalidate_directory(path: &Path, expected: Identity) -> Result<(), PrivateArtifactError> {
    if directory_identity(path)? != expected {
        return Err(PrivateArtifactError::Replaced);
    }
    Ok(())
}

fn current_uid() -> u32 {
    // SAFETY: getuid has no preconditions and cannot fail.
    unsafe { libc::getuid() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        BootstrapNonce, BridgeEndpoint, BridgeEndpointKind, BridgeGeneration, BridgeSessionId,
    };

    fn document(directory: &Path) -> BootstrapDocument {
        BootstrapDocument {
            session_id: BridgeSessionId::new("artifact-test").expect("session"),
            generation: BridgeGeneration::new(1).expect("generation"),
            endpoint: BridgeEndpoint::new(
                BridgeEndpointKind::UnixDomainSocket,
                directory.join("bridge.sock").to_string_lossy(),
            )
            .expect("endpoint"),
            nonce: BootstrapNonce::new([7; crate::NONCE_BYTES]).expect("nonce"),
            expires_at_unix_ms: 1,
        }
    }

    #[test]
    fn private_artifacts_are_owner_only_and_exact_identity_cleaned() {
        let temp = tempfile::tempdir().expect("temp");
        let root = temp.path().join("managed");
        let mut directory = create(&root, "session-one").expect("directory");
        let mut bootstrap = directory
            .write_bootstrap(&document(directory.path()))
            .expect("bootstrap");
        assert_eq!(
            fs::symlink_metadata(directory.path())
                .expect("directory metadata")
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(
            fs::symlink_metadata(bootstrap.path())
                .expect("bootstrap metadata")
                .mode()
                & 0o777,
            0o600
        );
        bootstrap.cleanup().expect("bootstrap cleanup");
        directory.cleanup().expect("directory cleanup");
        assert!(!directory.path().exists());
    }

    #[test]
    fn private_artifact_cleanup_preserves_replacement_directory() {
        let temp = tempfile::tempdir().expect("temp");
        let root = temp.path().join("managed");
        let mut directory = create(&root, "session-one").expect("directory");
        let original = directory.path().with_extension("old");
        fs::rename(directory.path(), &original).expect("move original");
        fs::create_dir(directory.path()).expect("replacement");
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))
            .expect("replacement permissions");
        assert!(matches!(
            directory.cleanup(),
            Err(PrivateArtifactError::Replaced)
        ));
        assert!(directory.path().is_dir());
    }

    #[test]
    fn symlinked_artifact_root_is_rejected_without_chmod_target() {
        let temp = tempfile::tempdir().expect("temp");
        let target = temp.path().join("target");
        fs::create_dir(&target).expect("target");
        fs::set_permissions(&target, fs::Permissions::from_mode(0o755)).expect("target mode");
        let link = temp.path().join("managed");
        std::os::unix::fs::symlink(&target, &link).expect("link");
        assert!(matches!(
            create(&link, "session-one"),
            Err(PrivateArtifactError::InsecureRoot)
        ));
        assert_eq!(
            fs::symlink_metadata(&target)
                .expect("target metadata")
                .mode()
                & 0o777,
            0o755
        );
    }
}
