use pioneer_client::{
    ClientError, ClientResult,
    platform::{ClientFileMetadata, ClientFileSystem, ClientPath},
};
use std::fs;

#[derive(Clone, Copy, Debug)]
pub(crate) struct DesktopClientFileSystem;

impl ClientFileSystem for DesktopClientFileSystem {
    fn read_file(&self, path: &ClientPath) -> ClientResult<Vec<u8>> {
        fs::read(path.as_path()).map_err(|error| {
            ClientError::platform(format!(
                "failed to read `{}`: {error}",
                path.as_path().display()
            ))
        })
    }

    fn metadata(&self, path: &ClientPath) -> ClientResult<ClientFileMetadata> {
        let metadata = fs::metadata(path.as_path()).map_err(|error| {
            ClientError::platform(format!(
                "failed to stat `{}`: {error}",
                path.as_path().display()
            ))
        })?;
        Ok(ClientFileMetadata {
            len: metadata.len(),
            modified: metadata.modified().ok(),
            is_file: metadata.is_file(),
            is_dir: metadata.is_dir(),
        })
    }

    fn write_cache_file(&self, _key: &str, _bytes: &[u8]) -> ClientResult<ClientPath> {
        Err(ClientError::platform(
            "cache writes are not supported by artifact upload filesystem adapter",
        ))
    }

    fn open_read(
        &self,
        path: &ClientPath,
    ) -> ClientResult<Box<dyn pioneer_client::platform::ClientFileReader>> {
        let file = fs::File::open(path.as_path()).map_err(|error| {
            ClientError::platform(format!(
                "failed to open `{}`: {error}",
                path.as_path().display()
            ))
        })?;
        Ok(Box::new(file))
    }
}
