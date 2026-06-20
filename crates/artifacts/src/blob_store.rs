use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};

use async_trait::async_trait;
use tokio::fs::File;
use tokio::io::{AsyncRead, ReadBuf};

use crate::error::{ArtifactError, ArtifactResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArtifactBlobInput {
    Bytes(Vec<u8>),
    TempFile { path: PathBuf },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredArtifactBlob {
    pub sha256: String,
    pub size_bytes: u64,
    pub storage_backend: String,
    pub storage_key: String,
    pub deduplicated: bool,
}

#[derive(Debug)]
pub struct ArtifactReadHandle {
    file: File,
    storage_key: String,
    size_bytes: u64,
}

impl ArtifactReadHandle {
    pub fn new(file: File, storage_key: impl Into<String>, size_bytes: u64) -> Self {
        Self {
            file,
            storage_key: storage_key.into(),
            size_bytes,
        }
    }

    pub fn storage_key(&self) -> &str {
        &self.storage_key
    }

    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    pub fn into_inner(self) -> File {
        self.file
    }
}

impl AsyncRead for ArtifactReadHandle {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.file).poll_read(cx, buf)
    }
}

#[async_trait]
pub trait ArtifactBlobStore: Send + Sync {
    fn local_artifact_root(&self) -> Option<PathBuf> {
        None
    }

    async fn put_bytes(
        &self,
        workspace_id: &str,
        input: ArtifactBlobInput,
    ) -> ArtifactResult<StoredArtifactBlob>;

    async fn open_read(
        &self,
        workspace_id: &str,
        storage_key: &str,
    ) -> ArtifactResult<ArtifactReadHandle>;

    async fn delete_unreferenced(
        &self,
        workspace_id: &str,
        storage_key: &str,
    ) -> ArtifactResult<()>;

    async fn materialize_readable_copy(
        &self,
        workspace_id: &str,
        storage_key: &str,
        safe_name: &str,
    ) -> Result<PathBuf, ArtifactError>;
}
