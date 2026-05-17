use std::io::ErrorKind;
use std::path::{Component, Path, PathBuf};

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::blob_store::{
    ArtifactBlobInput, ArtifactBlobStore, ArtifactReadHandle, StoredArtifactBlob,
};
use crate::error::{ArtifactError, ArtifactResult};
use crate::ids::{
    is_lower_hex_sha256, is_safe_file_name, new_operation_id, sha256_storage_key, workspace_segment,
};

const LOCAL_STORAGE_BACKEND: &str = "local";
const BUFFER_SIZE: usize = 64 * 1024;

#[derive(Debug, Clone)]
pub struct LocalArtifactBlobStore {
    runtime_home: PathBuf,
}

impl LocalArtifactBlobStore {
    pub fn new(runtime_home: impl Into<PathBuf>) -> Self {
        Self {
            runtime_home: runtime_home.into(),
        }
    }

    pub fn runtime_home(&self) -> &Path {
        &self.runtime_home
    }

    pub fn artifact_root(&self) -> PathBuf {
        self.runtime_home.join("artifacts")
    }

    pub fn workspace_root(&self, workspace_id: &str) -> ArtifactResult<PathBuf> {
        Ok(self
            .artifact_root()
            .join("workspaces")
            .join(workspace_segment(workspace_id)?))
    }

    pub fn blob_path(&self, workspace_id: &str, storage_key: &str) -> ArtifactResult<PathBuf> {
        let blob_root = self.workspace_root(workspace_id)?.join("blobs");
        let relative = validate_storage_key(storage_key)?;
        let path = blob_root.join(relative);
        if !path.starts_with(&blob_root) {
            return Err(ArtifactError::StorageKeyTraversal {
                storage_key: storage_key.to_owned(),
            });
        }
        Ok(path)
    }

    fn tmp_path(&self, workspace_id: &str) -> ArtifactResult<PathBuf> {
        Ok(self
            .workspace_root(workspace_id)?
            .join("tmp")
            .join(format!("{}.part", new_operation_id("put"))))
    }
}

#[async_trait]
impl ArtifactBlobStore for LocalArtifactBlobStore {
    fn local_artifact_root(&self) -> Option<PathBuf> {
        Some(self.artifact_root())
    }

    async fn put_bytes(
        &self,
        workspace_id: &str,
        input: ArtifactBlobInput,
    ) -> ArtifactResult<StoredArtifactBlob> {
        let tmp_path = self.tmp_path(workspace_id)?;
        let tmp_parent = tmp_path.parent().ok_or_else(|| ArtifactError::Io {
            message: format!("artifact temp path has no parent: {}", tmp_path.display()),
            source: ErrorKind::InvalidInput.into(),
        })?;
        fs::create_dir_all(tmp_parent)
            .await
            .map_err(|source| ArtifactError::Io {
                message: format!(
                    "failed to create artifact temp dir {}",
                    tmp_parent.display()
                ),
                source,
            })?;

        let mut tmp_file =
            fs::File::create(&tmp_path)
                .await
                .map_err(|source| ArtifactError::TempWriteFailed {
                    path: tmp_path.clone(),
                    source,
                })?;

        let mut hasher = Sha256::new();
        let mut size_bytes = 0_u64;

        match input {
            ArtifactBlobInput::Bytes(bytes) => {
                hasher.update(&bytes);
                size_bytes = bytes.len() as u64;
                tmp_file.write_all(&bytes).await.map_err(|source| {
                    ArtifactError::TempWriteFailed {
                        path: tmp_path.clone(),
                        source,
                    }
                })?;
            }
            ArtifactBlobInput::TempFile { path } => {
                let mut source_file =
                    fs::File::open(&path)
                        .await
                        .map_err(|source| ArtifactError::Io {
                            message: format!(
                                "failed to open temp artifact input {}",
                                path.display()
                            ),
                            source,
                        })?;
                let mut buffer = vec![0_u8; BUFFER_SIZE];
                loop {
                    let read = source_file.read(&mut buffer).await.map_err(|source| {
                        ArtifactError::Io {
                            message: format!(
                                "failed to read temp artifact input {}",
                                path.display()
                            ),
                            source,
                        }
                    })?;
                    if read == 0 {
                        break;
                    }
                    hasher.update(&buffer[..read]);
                    size_bytes += read as u64;
                    tmp_file
                        .write_all(&buffer[..read])
                        .await
                        .map_err(|source| ArtifactError::TempWriteFailed {
                            path: tmp_path.clone(),
                            source,
                        })?;
                }
            }
        }

        tmp_file
            .flush()
            .await
            .map_err(|source| ArtifactError::TempWriteFailed {
                path: tmp_path.clone(),
                source,
            })?;
        drop(tmp_file);

        let sha256 = hex::encode(hasher.finalize());
        let storage_key = sha256_storage_key(&sha256);
        let final_path = self.blob_path(workspace_id, &storage_key)?;

        if fs::try_exists(&final_path)
            .await
            .map_err(|source| ArtifactError::Io {
                message: format!("failed to inspect artifact blob {}", final_path.display()),
                source,
            })?
        {
            self.verify_existing_blob(&final_path, &storage_key, &sha256, size_bytes)
                .await?;
            remove_file_if_exists(&tmp_path).await?;
            return Ok(StoredArtifactBlob {
                sha256,
                size_bytes,
                storage_backend: LOCAL_STORAGE_BACKEND.to_owned(),
                storage_key,
                deduplicated: true,
            });
        }

        let final_parent = final_path.parent().ok_or_else(|| ArtifactError::Io {
            message: format!("artifact blob path has no parent: {}", final_path.display()),
            source: ErrorKind::InvalidInput.into(),
        })?;
        fs::create_dir_all(final_parent)
            .await
            .map_err(|source| ArtifactError::Io {
                message: format!(
                    "failed to create artifact blob dir {}",
                    final_parent.display()
                ),
                source,
            })?;
        fs::rename(&tmp_path, &final_path).await.map_err(|source| {
            ArtifactError::FinalRenameFailed {
                from: tmp_path.clone(),
                to: final_path.clone(),
                source,
            }
        })?;

        Ok(StoredArtifactBlob {
            sha256,
            size_bytes,
            storage_backend: LOCAL_STORAGE_BACKEND.to_owned(),
            storage_key,
            deduplicated: false,
        })
    }

    async fn open_read(
        &self,
        workspace_id: &str,
        storage_key: &str,
    ) -> ArtifactResult<ArtifactReadHandle> {
        let path = self.blob_path(workspace_id, storage_key)?;
        let metadata = fs::metadata(&path).await.map_err(|source| {
            if source.kind() == ErrorKind::NotFound {
                ArtifactError::ReadMissingBlob {
                    storage_key: storage_key.to_owned(),
                }
            } else {
                ArtifactError::Io {
                    message: format!("failed to stat artifact blob {}", path.display()),
                    source,
                }
            }
        })?;
        let file = fs::File::open(&path).await.map_err(|source| {
            if source.kind() == ErrorKind::NotFound {
                ArtifactError::ReadMissingBlob {
                    storage_key: storage_key.to_owned(),
                }
            } else {
                ArtifactError::Io {
                    message: format!("failed to open artifact blob {}", path.display()),
                    source,
                }
            }
        })?;
        Ok(ArtifactReadHandle::new(
            file,
            storage_key.to_owned(),
            metadata.len(),
        ))
    }

    async fn delete_unreferenced(
        &self,
        workspace_id: &str,
        storage_key: &str,
    ) -> ArtifactResult<()> {
        let path = self.blob_path(workspace_id, storage_key)?;
        remove_file_if_exists(&path).await
    }

    async fn materialize_temp(
        &self,
        workspace_id: &str,
        storage_key: &str,
        safe_name: &str,
    ) -> ArtifactResult<PathBuf> {
        let source_path = self.blob_path(workspace_id, storage_key)?;
        if !is_safe_file_name(safe_name) {
            return Err(ArtifactError::MaterializedPathEscape {
                path: PathBuf::from(safe_name),
                root: self.workspace_root(workspace_id)?.join("materialized"),
            });
        }

        let materialized_root = self.workspace_root(workspace_id)?.join("materialized");
        let session_root = materialized_root.join(new_operation_id("materialized"));
        let destination = session_root.join(safe_name);
        if !destination.starts_with(&materialized_root) {
            return Err(ArtifactError::MaterializedPathEscape {
                path: destination,
                root: materialized_root,
            });
        }

        fs::create_dir_all(&session_root)
            .await
            .map_err(|source| ArtifactError::Io {
                message: format!(
                    "failed to create materialized artifact dir {}",
                    session_root.display()
                ),
                source,
            })?;
        fs::copy(&source_path, &destination)
            .await
            .map_err(|source| match source.kind() {
                ErrorKind::NotFound => ArtifactError::ReadMissingBlob {
                    storage_key: storage_key.to_owned(),
                },
                _ => ArtifactError::Io {
                    message: format!(
                        "failed to materialize artifact blob {} to {}",
                        source_path.display(),
                        destination.display()
                    ),
                    source,
                },
            })?;

        Ok(destination)
    }
}

impl LocalArtifactBlobStore {
    async fn verify_existing_blob(
        &self,
        final_path: &Path,
        storage_key: &str,
        expected_sha256: &str,
        expected_size_bytes: u64,
    ) -> ArtifactResult<()> {
        let metadata = fs::metadata(final_path)
            .await
            .map_err(|source| ArtifactError::Io {
                message: format!(
                    "failed to stat existing artifact blob {}",
                    final_path.display()
                ),
                source,
            })?;
        let actual_size_bytes = metadata.len();
        if actual_size_bytes != expected_size_bytes {
            return Err(ArtifactError::ExistingBlobCorruption {
                storage_key: storage_key.to_owned(),
                expected_sha256: expected_sha256.to_owned(),
                expected_size_bytes,
                actual_sha256: None,
                actual_size_bytes: Some(actual_size_bytes),
            });
        }

        let actual_sha256 = hash_file(final_path).await?;
        if actual_sha256 != expected_sha256 {
            return Err(ArtifactError::ExistingBlobCorruption {
                storage_key: storage_key.to_owned(),
                expected_sha256: expected_sha256.to_owned(),
                expected_size_bytes,
                actual_sha256: Some(actual_sha256),
                actual_size_bytes: Some(actual_size_bytes),
            });
        }

        Ok(())
    }
}

fn validate_storage_key(storage_key: &str) -> ArtifactResult<PathBuf> {
    if storage_key.is_empty() || Path::new(storage_key).is_absolute() {
        return Err(ArtifactError::InvalidStorageKey {
            storage_key: storage_key.to_owned(),
        });
    }

    let mut parts = Vec::new();
    for component in Path::new(storage_key).components() {
        match component {
            Component::Normal(value) => {
                let value = value.to_string_lossy();
                if value.is_empty() || value.contains('\\') || value.contains('\0') {
                    return Err(ArtifactError::InvalidStorageKey {
                        storage_key: storage_key.to_owned(),
                    });
                }
                parts.push(value.to_string());
            }
            Component::ParentDir => {
                return Err(ArtifactError::StorageKeyTraversal {
                    storage_key: storage_key.to_owned(),
                });
            }
            Component::CurDir | Component::RootDir | Component::Prefix(_) => {
                return Err(ArtifactError::InvalidStorageKey {
                    storage_key: storage_key.to_owned(),
                });
            }
        }
    }

    if parts.len() != 4
        || parts[0] != "sha256"
        || parts[1].len() != 2
        || parts[2].len() != 2
        || !is_lower_hex_sha256(&parts[3])
        || parts[1] != parts[3][0..2]
        || parts[2] != parts[3][2..4]
    {
        return Err(ArtifactError::InvalidStorageKey {
            storage_key: storage_key.to_owned(),
        });
    }

    Ok(parts.iter().collect())
}

async fn hash_file(path: &Path) -> ArtifactResult<String> {
    let mut file = fs::File::open(path)
        .await
        .map_err(|source| ArtifactError::Io {
            message: format!(
                "failed to open artifact blob for hashing {}",
                path.display()
            ),
            source,
        })?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; BUFFER_SIZE];
    loop {
        let read = file
            .read(&mut buffer)
            .await
            .map_err(|source| ArtifactError::Io {
                message: format!("failed to hash artifact blob {}", path.display()),
                source,
            })?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex::encode(hasher.finalize()))
}

async fn remove_file_if_exists(path: &Path) -> ArtifactResult<()> {
    fs::remove_file(path)
        .await
        .map_err(|source| {
            if source.kind() == ErrorKind::NotFound {
                ArtifactError::Io {
                    message: "artifact blob already absent".to_owned(),
                    source,
                }
            } else {
                ArtifactError::Io {
                    message: format!("failed to delete artifact blob {}", path.display()),
                    source,
                }
            }
        })
        .or_else(|error| match error {
            ArtifactError::Io { source, .. } if source.kind() == ErrorKind::NotFound => Ok(()),
            other => Err(other),
        })
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use tokio::io::AsyncReadExt;

    use super::*;

    fn store(runtime_home: &Path) -> LocalArtifactBlobStore {
        LocalArtifactBlobStore::new(runtime_home)
    }

    #[tokio::test]
    async fn put_bytes_writes_expected_workspace_scoped_path() {
        let temp = tempfile::tempdir().expect("temp dir");
        let store = store(temp.path());

        let stored = store
            .put_bytes("wsA123", ArtifactBlobInput::Bytes(b"hello".to_vec()))
            .await
            .expect("put bytes");

        assert_eq!(stored.storage_backend, "local");
        assert!(!stored.deduplicated);
        assert_eq!(stored.size_bytes, 5);
        assert_eq!(stored.storage_key, sha256_storage_key(&stored.sha256));

        let path = temp
            .path()
            .join("artifacts/workspaces/wsA123/blobs")
            .join(&stored.storage_key);
        assert!(path.exists(), "expected blob at {}", path.display());
    }

    #[tokio::test]
    async fn open_read_returns_original_bytes() {
        let temp = tempfile::tempdir().expect("temp dir");
        let store = store(temp.path());
        let stored = store
            .put_bytes("wsA123", ArtifactBlobInput::Bytes(b"read me".to_vec()))
            .await
            .expect("put bytes");

        let mut handle = store
            .open_read("wsA123", &stored.storage_key)
            .await
            .expect("open read");
        let mut bytes = Vec::new();
        handle.read_to_end(&mut bytes).await.expect("read handle");

        assert_eq!(bytes, b"read me");
        assert_eq!(handle.storage_key(), stored.storage_key);
        assert_eq!(handle.size_bytes(), 7);
    }

    #[tokio::test]
    async fn second_put_same_workspace_returns_deduplicated() {
        let temp = tempfile::tempdir().expect("temp dir");
        let store = store(temp.path());

        let first = store
            .put_bytes("wsA123", ArtifactBlobInput::Bytes(b"same".to_vec()))
            .await
            .expect("first put");
        let second = store
            .put_bytes("wsA123", ArtifactBlobInput::Bytes(b"same".to_vec()))
            .await
            .expect("second put");

        assert!(!first.deduplicated);
        assert!(second.deduplicated);
        assert_eq!(first.storage_key, second.storage_key);
    }

    #[tokio::test]
    async fn same_bytes_in_different_workspaces_are_separate_files() {
        let temp = tempfile::tempdir().expect("temp dir");
        let store = store(temp.path());

        let first = store
            .put_bytes("wsA123", ArtifactBlobInput::Bytes(b"same".to_vec()))
            .await
            .expect("first put");
        let second = store
            .put_bytes("wsB456", ArtifactBlobInput::Bytes(b"same".to_vec()))
            .await
            .expect("second put");

        assert_eq!(first.storage_key, second.storage_key);
        assert!(!second.deduplicated);
        assert!(
            store
                .blob_path("wsA123", &first.storage_key)
                .unwrap()
                .exists()
        );
        assert!(
            store
                .blob_path("wsB456", &second.storage_key)
                .unwrap()
                .exists()
        );
    }

    #[tokio::test]
    async fn invalid_workspace_id_is_rejected() {
        let temp = tempfile::tempdir().expect("temp dir");
        let store = store(temp.path());

        let error = store
            .put_bytes("../ws", ArtifactBlobInput::Bytes(b"bad".to_vec()))
            .await
            .expect_err("workspace id should be rejected");

        assert!(matches!(error, ArtifactError::InvalidWorkspaceId { .. }));
    }

    #[tokio::test]
    async fn empty_workspace_id_is_rejected() {
        let temp = tempfile::tempdir().expect("temp dir");
        let store = store(temp.path());

        let error = store
            .put_bytes("", ArtifactBlobInput::Bytes(b"bad".to_vec()))
            .await
            .expect_err("workspace id should be rejected");

        assert!(matches!(error, ArtifactError::EmptyWorkspaceId));
    }

    #[tokio::test]
    async fn invalid_storage_key_with_traversal_is_rejected() {
        let temp = tempfile::tempdir().expect("temp dir");
        let store = store(temp.path());

        let error = store
            .open_read("wsA123", "../outside")
            .await
            .expect_err("storage key should be rejected");

        assert!(matches!(error, ArtifactError::StorageKeyTraversal { .. }));
    }

    #[tokio::test]
    async fn materialize_temp_writes_readable_copy_with_safe_name() {
        let temp = tempfile::tempdir().expect("temp dir");
        let store = store(temp.path());
        let stored = store
            .put_bytes("wsA123", ArtifactBlobInput::Bytes(b"materialized".to_vec()))
            .await
            .expect("put bytes");

        let path = store
            .materialize_temp("wsA123", &stored.storage_key, "preview.txt")
            .await
            .expect("materialize");
        let bytes = fs::read(&path).await.expect("read materialized");

        assert_eq!(bytes, b"materialized");
        assert!(path.ends_with("preview.txt"));
        assert!(path.starts_with(temp.path().join("artifacts/workspaces/wsA123/materialized")));
    }

    #[tokio::test]
    async fn materialize_temp_rejects_path_escape_name() {
        let temp = tempfile::tempdir().expect("temp dir");
        let store = store(temp.path());
        let stored = store
            .put_bytes("wsA123", ArtifactBlobInput::Bytes(b"materialized".to_vec()))
            .await
            .expect("put bytes");

        let error = store
            .materialize_temp("wsA123", &stored.storage_key, "../preview.txt")
            .await
            .expect_err("unsafe materialized name should be rejected");

        assert!(matches!(
            error,
            ArtifactError::MaterializedPathEscape { .. }
        ));
    }

    #[tokio::test]
    async fn delete_unreferenced_removes_only_requested_workspace_file() {
        let temp = tempfile::tempdir().expect("temp dir");
        let store = store(temp.path());

        let first = store
            .put_bytes("wsA123", ArtifactBlobInput::Bytes(b"same".to_vec()))
            .await
            .expect("first put");
        let second = store
            .put_bytes("wsB456", ArtifactBlobInput::Bytes(b"same".to_vec()))
            .await
            .expect("second put");

        store
            .delete_unreferenced("wsA123", &first.storage_key)
            .await
            .expect("delete first");

        assert!(
            !store
                .blob_path("wsA123", &first.storage_key)
                .unwrap()
                .exists()
        );
        assert!(
            store
                .blob_path("wsB456", &second.storage_key)
                .unwrap()
                .exists()
        );
    }

    #[tokio::test]
    async fn dedup_detects_existing_blob_corruption() {
        let temp = tempfile::tempdir().expect("temp dir");
        let store = store(temp.path());
        let stored = store
            .put_bytes("wsA123", ArtifactBlobInput::Bytes(b"same".to_vec()))
            .await
            .expect("put bytes");
        let blob_path = store.blob_path("wsA123", &stored.storage_key).unwrap();
        fs::write(&blob_path, b"xxxx").await.expect("corrupt blob");

        let error = store
            .put_bytes("wsA123", ArtifactBlobInput::Bytes(b"same".to_vec()))
            .await
            .expect_err("corrupt existing blob should be rejected");

        assert!(matches!(
            error,
            ArtifactError::ExistingBlobCorruption { .. }
        ));
    }

    #[tokio::test]
    async fn put_temp_file_hashes_and_stores_contents() {
        let temp = tempfile::tempdir().expect("temp dir");
        let input_path = temp.path().join("input.part");
        fs::write(&input_path, b"from temp")
            .await
            .expect("write temp");
        let store = store(temp.path());

        let stored = store
            .put_bytes("wsA123", ArtifactBlobInput::TempFile { path: input_path })
            .await
            .expect("put temp file");
        let bytes = fs::read(store.blob_path("wsA123", &stored.storage_key).unwrap())
            .await
            .expect("read blob");

        assert_eq!(bytes, b"from temp");
    }
}
