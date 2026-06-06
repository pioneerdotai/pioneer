use crate::ClientResult;
pub use crate::artifacts::download::ArtifactDownloadCache as ArtifactCache;
use crate::error::ClientError;
use sha2::{Digest, Sha256};
use std::{
    future::Future,
    io::{Cursor, Read},
    path::PathBuf,
    pin::Pin,
    time::{Duration, SystemTime},
};

pub type BoxClientFuture<'a, T> = Pin<Box<dyn Future<Output = ClientResult<T>> + Send + 'a>>;
pub type ClientFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
pub struct ClientPath(PathBuf);

impl ClientPath {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self(path.into())
    }

    pub fn as_path(&self) -> &std::path::Path {
        &self.0
    }

    pub fn into_path_buf(self) -> PathBuf {
        self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientFileMetadata {
    pub len: u64,
    pub modified: Option<SystemTime>,
    pub is_file: bool,
    pub is_dir: bool,
}

pub trait ClientExecutor: Send + Sync {
    fn spawn_background(&self, task: ClientFuture);

    fn spawn_timer(&self, delay: Duration, task: ClientFuture);
}

pub trait ClientStorage: Send + Sync {
    fn read_text(&self, key: &str) -> ClientResult<Option<String>>;

    fn write_text(&self, key: &str, value: &str) -> ClientResult<()>;

    fn remove(&self, key: &str) -> ClientResult<()>;
}

pub trait SecretStore: Send + Sync {
    fn load_secret<'a>(&'a self, key: &'a str) -> BoxClientFuture<'a, Option<String>>;

    fn store_secret<'a>(&'a self, key: &'a str, value: &'a str) -> BoxClientFuture<'a, ()>;

    fn delete_secret<'a>(&'a self, key: &'a str) -> BoxClientFuture<'a, ()>;
}

pub trait ClientFileSystem: Send + Sync {
    fn read_file(&self, path: &ClientPath) -> ClientResult<Vec<u8>>;

    fn metadata(&self, path: &ClientPath) -> ClientResult<ClientFileMetadata>;

    fn write_cache_file(&self, key: &str, bytes: &[u8]) -> ClientResult<ClientPath>;

    fn open_read(&self, path: &ClientPath) -> ClientResult<Box<dyn ClientFileReader>> {
        Ok(Box::new(Cursor::new(self.read_file(path)?)))
    }

    fn file_name(&self, path: &ClientPath) -> ClientResult<String> {
        path.as_path()
            .file_name()
            .and_then(|value| value.to_str())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .ok_or_else(|| ClientError::platform("artifact upload path has no file name"))
    }

    fn mime_type(&self, path: &ClientPath) -> ClientResult<Option<String>> {
        Ok(mime_guess::from_path(path.as_path())
            .first()
            .map(|mime| mime.essence_str().to_owned()))
    }

    fn sha256_file(&self, path: &ClientPath) -> ClientResult<String> {
        let mut reader = self.open_read(path)?;
        let mut hasher = Sha256::new();
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            let read = reader.read_chunk(&mut buffer)?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
        }
        Ok(hex::encode(hasher.finalize()))
    }
}

pub trait ClientFileReader: Send {
    fn read_chunk(&mut self, buffer: &mut [u8]) -> ClientResult<usize>;
}

impl<T> ClientFileReader for T
where
    T: Read + Send,
{
    fn read_chunk(&mut self, buffer: &mut [u8]) -> ClientResult<usize> {
        self.read(buffer)
            .map_err(|error| ClientError::platform(format!("failed to read client file: {error}")))
    }
}

pub trait ArtifactFileOpener: Send + Sync {
    fn open_file(&self, path: &ClientPath) -> ClientResult<()>;

    fn reveal_file(&self, path: &ClientPath) -> ClientResult<()>;
}
