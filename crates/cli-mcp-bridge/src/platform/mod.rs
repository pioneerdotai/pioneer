use crate::bootstrap::{BridgeEndpoint, BridgeGeneration, BridgeSessionId};
use crate::framing::{
    BridgeFrame, BridgeFrameError, FRAME_HEADER_BYTES, MAX_FRAME_PAYLOAD_BYTES, decode_header,
    encode_frame,
};
use async_trait::async_trait;
use std::fmt;
use std::io;
use std::path::PathBuf;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use zeroize::Zeroize;

#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;

#[cfg(unix)]
pub use unix::{
    PlatformConnection, PlatformListener, bind_private_endpoint, connect_private_endpoint,
    private_endpoint_descriptor,
};
#[cfg(windows)]
pub use windows::{
    PlatformConnection, PlatformListener, bind_private_endpoint, connect_private_endpoint,
    private_endpoint_descriptor,
};

/// Inputs needed to derive one private endpoint for one bridge generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrivateEndpointConfig {
    pub managed_directory: PathBuf,
    pub session_id: BridgeSessionId,
    pub generation: BridgeGeneration,
    /// When present, the accepted/connected peer must have this process id.
    pub expected_peer_pid: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerIdentity {
    pub process_id: Option<u32>,
    pub user_id: Option<u32>,
}

#[derive(Debug)]
pub enum PrivateIpcError {
    InvalidManagedDirectory,
    InsecureManagedDirectory,
    InvalidEndpoint,
    EndpointAlreadyExists,
    StaleGeneration,
    EndpointReplaced,
    WrongPeer,
    UnsupportedPlatform,
    Frame(BridgeFrameError),
    Io(io::Error),
}

impl fmt::Display for PrivateIpcError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidManagedDirectory => {
                formatter.write_str("invalid CLI MCP bridge managed directory")
            }
            Self::InsecureManagedDirectory => {
                formatter.write_str("CLI MCP bridge managed directory is not owner-only")
            }
            Self::InvalidEndpoint => formatter.write_str("invalid CLI MCP bridge endpoint"),
            Self::EndpointAlreadyExists => {
                formatter.write_str("CLI MCP bridge endpoint already exists")
            }
            Self::StaleGeneration => formatter.write_str("stale CLI MCP bridge generation"),
            Self::EndpointReplaced => formatter.write_str("CLI MCP bridge endpoint was replaced"),
            Self::WrongPeer => formatter.write_str("CLI MCP bridge peer identity was rejected"),
            Self::UnsupportedPlatform => {
                formatter.write_str("CLI MCP bridge IPC is unsupported on this platform")
            }
            Self::Frame(error) => error.fmt(formatter),
            Self::Io(error) => write!(formatter, "CLI MCP bridge IPC failed: {error}"),
        }
    }
}

impl std::error::Error for PrivateIpcError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Frame(error) => Some(error),
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<BridgeFrameError> for PrivateIpcError {
    fn from(value: BridgeFrameError) -> Self {
        Self::Frame(value)
    }
}

impl From<io::Error> for PrivateIpcError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

#[async_trait]
pub trait BridgeFrameTransport: Send {
    fn endpoint(&self) -> &BridgeEndpoint;
    fn peer_identity(&self) -> PeerIdentity;

    async fn receive_frame(&mut self) -> Result<Option<BridgeFrame>, PrivateIpcError>;
    async fn send_frame(&mut self, frame: &BridgeFrame) -> Result<(), PrivateIpcError>;
    async fn shutdown(&mut self) -> Result<(), PrivateIpcError>;
}

pub(crate) async fn read_async_frame<S>(
    stream: &mut S,
) -> Result<Option<BridgeFrame>, PrivateIpcError>
where
    S: AsyncRead + Unpin,
{
    let mut header = [0_u8; FRAME_HEADER_BYTES];
    let mut header_read = 0;
    while header_read < FRAME_HEADER_BYTES {
        match stream.read(&mut header[header_read..]).await {
            Ok(0) if header_read == 0 => return Ok(None),
            Ok(0) => {
                return Err(BridgeFrameError::TruncatedHeader {
                    actual: header_read,
                }
                .into());
            }
            Ok(read) => header_read += read,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error.into()),
        }
    }

    let (frame_type, payload_len) = decode_header(&header, MAX_FRAME_PAYLOAD_BYTES)?;
    let mut payload = vec![0_u8; payload_len];
    let mut payload_read = 0;
    while payload_read < payload_len {
        match stream.read(&mut payload[payload_read..]).await {
            Ok(0) => {
                payload.zeroize();
                return Err(BridgeFrameError::TruncatedPayload {
                    expected: payload_len,
                    actual: payload_read,
                }
                .into());
            }
            Ok(read) => payload_read += read,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => {
                payload.zeroize();
                return Err(error.into());
            }
        }
    }
    BridgeFrame::new(frame_type, payload)
        .map(Some)
        .map_err(Into::into)
}

pub(crate) async fn write_async_frame<S>(
    stream: &mut S,
    frame: &BridgeFrame,
) -> Result<(), PrivateIpcError>
where
    S: AsyncWrite + Unpin,
{
    let mut encoded = encode_frame(frame)?;
    let result = async {
        stream.write_all(encoded.as_slice()).await?;
        stream.flush().await
    }
    .await;
    encoded.zeroize();
    result.map_err(Into::into)
}

pub(crate) async fn shutdown_async<S>(stream: &mut S) -> Result<(), PrivateIpcError>
where
    S: AsyncWrite + Unpin,
{
    stream.shutdown().await.map_err(Into::into)
}
