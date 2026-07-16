use crate::{BootstrapDocument, BridgeGeneration, BridgeSessionId};
use std::fmt;
use std::io;
use std::path::{Component, Path};

#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;

#[cfg(unix)]
pub use unix::{PrivateBootstrapArtifact, PrivateSessionDirectory};
#[cfg(windows)]
pub use windows::{PrivateBootstrapArtifact, PrivateSessionDirectory};

#[derive(Debug)]
pub enum PrivateArtifactError {
    InvalidRoot,
    InsecureRoot,
    AlreadyExists,
    Replaced,
    Bootstrap(crate::BootstrapEncodeError),
    Io(io::Error),
}

impl fmt::Display for PrivateArtifactError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRoot => formatter.write_str("invalid CLI MCP artifact root"),
            Self::InsecureRoot => formatter.write_str("CLI MCP artifact root is not owner-only"),
            Self::AlreadyExists => formatter.write_str("CLI MCP session artifacts already exist"),
            Self::Replaced => formatter.write_str("CLI MCP session artifact was replaced"),
            Self::Bootstrap(error) => error.fmt(formatter),
            Self::Io(error) => write!(formatter, "CLI MCP artifact I/O failed: {error}"),
        }
    }
}

impl std::error::Error for PrivateArtifactError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Bootstrap(error) => Some(error),
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for PrivateArtifactError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<crate::BootstrapEncodeError> for PrivateArtifactError {
    fn from(value: crate::BootstrapEncodeError) -> Self {
        Self::Bootstrap(value)
    }
}

pub fn create_private_session_directory(
    root: &Path,
    session_id: &BridgeSessionId,
    generation: BridgeGeneration,
) -> Result<PrivateSessionDirectory, PrivateArtifactError> {
    validate_absolute(root)?;
    let directory_name = format!("session-{}-g{}", session_id.as_str(), generation.get());
    platform_create(root, directory_name.as_str())
}

#[cfg(unix)]
fn platform_create(
    root: &Path,
    directory_name: &str,
) -> Result<PrivateSessionDirectory, PrivateArtifactError> {
    unix::create(root, directory_name)
}

#[cfg(windows)]
fn platform_create(
    root: &Path,
    directory_name: &str,
) -> Result<PrivateSessionDirectory, PrivateArtifactError> {
    windows::create(root, directory_name)
}

fn validate_absolute(path: &Path) -> Result<(), PrivateArtifactError> {
    if !path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, Component::ParentDir | Component::CurDir))
    {
        return Err(PrivateArtifactError::InvalidRoot);
    }
    Ok(())
}

pub(crate) fn encode_bootstrap(
    document: &BootstrapDocument,
) -> Result<Vec<u8>, PrivateArtifactError> {
    document.encode().map_err(Into::into)
}
