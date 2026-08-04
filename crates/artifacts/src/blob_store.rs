use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncSeek, ReadBuf, SeekFrom};

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

pub trait ArtifactReader: AsyncRead + AsyncSeek + Send + Unpin {}

impl<T> ArtifactReader for T where T: AsyncRead + AsyncSeek + Send + Unpin {}

pub struct ArtifactReadHandle {
    reader: Box<dyn ArtifactReader>,
    storage_key: String,
    size_bytes: u64,
}

impl ArtifactReadHandle {
    pub fn new<R>(reader: R, storage_key: impl Into<String>, size_bytes: u64) -> Self
    where
        R: ArtifactReader + 'static,
    {
        Self {
            reader: Box::new(reader),
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

    pub fn into_inner(self) -> Box<dyn ArtifactReader> {
        self.reader
    }
}

impl std::fmt::Debug for ArtifactReadHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ArtifactReadHandle")
            .field("size_bytes", &self.size_bytes)
            .finish_non_exhaustive()
    }
}

impl AsyncRead for ArtifactReadHandle {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut *self.reader).poll_read(cx, buf)
    }
}

impl AsyncSeek for ArtifactReadHandle {
    fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> std::io::Result<()> {
        Pin::new(&mut *self.reader).start_seek(position)
    }

    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<u64>> {
        Pin::new(&mut *self.reader).poll_complete(cx)
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
