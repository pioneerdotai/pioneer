use crate::bootstrap::{
    BootstrapDecodeError, BootstrapDocument, BootstrapEncodeError, BridgeEndpointKind,
};
use crate::framing::{BridgeFrame, BridgeFrameError, BridgeFrameType};
use crate::platform::{
    BridgeFrameTransport, PrivateEndpointConfig, PrivateIpcError, connect_private_endpoint,
    private_endpoint_descriptor,
};
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::io;
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use zeroize::Zeroize;

#[cfg(unix)]
#[path = "helper/unix.rs"]
mod secure_bootstrap;
#[cfg(windows)]
#[path = "helper/windows.rs"]
mod secure_bootstrap;

use secure_bootstrap::OpenedBootstrap;

pub const HIDDEN_HELPER_COMMAND: &str = "__cli-mcp-stdio";
pub const BOOTSTRAP_FILE_FLAG: &str = "--bootstrap-file";
pub const HELPER_STDIO_CHUNK_BYTES: usize = 64 * 1024;
pub const MAX_HELPER_DIAGNOSTIC_BYTES: usize = 1024;

#[derive(Debug)]
pub enum HelperError {
    InvalidArguments,
    InvalidBootstrapPath,
    InsecureBootstrap,
    BootstrapExpired,
    BootstrapScopeMismatch,
    Bootstrap(BootstrapDecodeError),
    BootstrapEncoding(BootstrapEncodeError),
    Frame(BridgeFrameError),
    Ipc(PrivateIpcError),
    AttachRejected,
    UnexpectedFrame,
    Cancelled,
    Io(io::Error),
}

impl fmt::Display for HelperError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidArguments => {
                formatter.write_str("invalid hidden CLI MCP helper arguments")
            }
            Self::InvalidBootstrapPath => formatter.write_str("invalid CLI MCP bootstrap path"),
            Self::InsecureBootstrap => {
                formatter.write_str("CLI MCP bootstrap permissions or type are insecure")
            }
            Self::BootstrapExpired => formatter.write_str("CLI MCP bootstrap has expired"),
            Self::BootstrapScopeMismatch => {
                formatter.write_str("CLI MCP bootstrap scope does not match its private endpoint")
            }
            Self::Bootstrap(error) => error.fmt(formatter),
            Self::BootstrapEncoding(error) => error.fmt(formatter),
            Self::Frame(error) => error.fmt(formatter),
            Self::Ipc(error) => error.fmt(formatter),
            Self::AttachRejected => formatter.write_str("CLI MCP bridge attach was rejected"),
            Self::UnexpectedFrame => {
                formatter.write_str("unexpected CLI MCP bridge frame during helper relay")
            }
            Self::Cancelled => formatter.write_str("CLI MCP helper was cancelled"),
            Self::Io(error) => write!(formatter, "CLI MCP helper I/O failed: {error}"),
        }
    }
}

impl std::error::Error for HelperError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Bootstrap(error) => Some(error),
            Self::BootstrapEncoding(error) => Some(error),
            Self::Frame(error) => Some(error),
            Self::Ipc(error) => Some(error),
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<BootstrapDecodeError> for HelperError {
    fn from(value: BootstrapDecodeError) -> Self {
        Self::Bootstrap(value)
    }
}

impl From<BootstrapEncodeError> for HelperError {
    fn from(value: BootstrapEncodeError) -> Self {
        Self::BootstrapEncoding(value)
    }
}

impl From<BridgeFrameError> for HelperError {
    fn from(value: BridgeFrameError) -> Self {
        Self::Frame(value)
    }
}

impl From<PrivateIpcError> for HelperError {
    fn from(value: PrivateIpcError) -> Self {
        Self::Ipc(value)
    }
}

impl From<io::Error> for HelperError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

/// Parses only the exact hidden helper spelling. All ordinary commands return
/// `Ok(None)` so pioneer-cli can continue with its normal startup.
pub fn parse_hidden_helper_args(
    mut args: impl Iterator<Item = OsString>,
) -> Result<Option<PathBuf>, HelperError> {
    let Some(command) = args.next() else {
        return Ok(None);
    };
    if command != OsStr::new(HIDDEN_HELPER_COMMAND) {
        return Ok(None);
    }
    if args.next().as_deref() != Some(OsStr::new(BOOTSTRAP_FILE_FLAG)) {
        return Err(HelperError::InvalidArguments);
    }
    let path = args.next().ok_or(HelperError::InvalidArguments)?;
    if args.next().is_some() || path.is_empty() {
        return Err(HelperError::InvalidArguments);
    }
    Ok(Some(PathBuf::from(path)))
}

pub async fn run_hidden_helper(bootstrap_path: &Path) -> Result<(), HelperError> {
    let input = tokio::io::stdin();
    let output = tokio::io::stdout();
    run_hidden_helper_with_io(bootstrap_path, input, output).await
}

pub async fn run_hidden_helper_with_io<R, W>(
    bootstrap_path: &Path,
    input: R,
    output: W,
) -> Result<(), HelperError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    validate_absolute_path(bootstrap_path)?;
    let mut opened = OpenedBootstrap::open(bootstrap_path)?;
    let mut raw = opened.read_bounded()?;
    let document = BootstrapDocument::decode(raw.as_slice());
    raw.zeroize();
    let document = document?;
    validate_expiry(&document)?;

    #[cfg(unix)]
    let managed_directory = Path::new(document.endpoint.address())
        .parent()
        .ok_or(HelperError::InvalidBootstrapPath)?
        .to_path_buf();
    #[cfg(windows)]
    let managed_directory = bootstrap_path
        .parent()
        .ok_or(HelperError::InvalidBootstrapPath)?
        .to_path_buf();
    let config = PrivateEndpointConfig {
        managed_directory,
        session_id: document.session_id.clone(),
        generation: document.generation,
        expected_peer_pid: None,
    };
    validate_scope(&document, &config)?;

    let mut connection = connect_private_endpoint(&config).await?;
    let attach_payload = document.attach_request().encode()?;
    let attach = BridgeFrame::new(BridgeFrameType::Attach, attach_payload)?;
    connection.send_frame(&attach).await?;
    let mut sent_attach_payload = attach.into_payload();
    sent_attach_payload.zeroize();
    match connection.receive_frame().await? {
        Some(frame)
            if frame.frame_type() == BridgeFrameType::Result && frame.payload().is_empty() => {}
        Some(frame) if frame.frame_type() == BridgeFrameType::Error => {
            return Err(HelperError::AttachRejected);
        }
        Some(_) => return Err(HelperError::UnexpectedFrame),
        None => return Err(HelperError::AttachRejected),
    }

    // The nonce has been accepted by Gateway. Delete the identity-checked file
    // and verify disappearance before any provider protocol bytes are relayed.
    opened.consume()?;
    drop(document);
    relay(input, output, &mut connection).await
}

async fn relay<R, W, T>(mut input: R, mut output: W, connection: &mut T) -> Result<(), HelperError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
    T: BridgeFrameTransport,
{
    let mut stdin_buffer = [0_u8; HELPER_STDIO_CHUNK_BYTES];
    loop {
        tokio::select! {
            read = input.read(&mut stdin_buffer) => {
                let read = read?;
                if read == 0 {
                    let shutdown = BridgeFrame::new(BridgeFrameType::Shutdown, Vec::new())?;
                    connection.send_frame(&shutdown).await?;
                    connection.shutdown().await?;
                    stdin_buffer.zeroize();
                    return Ok(());
                }
                let frame = BridgeFrame::new(
                    BridgeFrameType::Payload,
                    stdin_buffer[..read].to_vec(),
                )?;
                stdin_buffer[..read].zeroize();
                connection.send_frame(&frame).await?;
            }
            frame = connection.receive_frame() => {
                match frame? {
                    Some(frame) if frame.frame_type() == BridgeFrameType::Payload => {
                        output.write_all(frame.payload()).await?;
                        output.flush().await?;
                    }
                    Some(frame) if frame.frame_type() == BridgeFrameType::Cancellation => {
                        stdin_buffer.zeroize();
                        return Err(HelperError::Cancelled);
                    }
                    Some(frame) if frame.frame_type() == BridgeFrameType::Shutdown => {
                        output.shutdown().await?;
                        stdin_buffer.zeroize();
                        return Ok(());
                    }
                    Some(frame) if frame.frame_type() == BridgeFrameType::Error => {
                        stdin_buffer.zeroize();
                        return Err(HelperError::UnexpectedFrame);
                    }
                    Some(_) => {
                        stdin_buffer.zeroize();
                        return Err(HelperError::UnexpectedFrame);
                    }
                    None => {
                        output.shutdown().await?;
                        stdin_buffer.zeroize();
                        return Ok(());
                    }
                }
            }
        }
    }
}

fn validate_absolute_path(path: &Path) -> Result<(), HelperError> {
    if !path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, Component::ParentDir | Component::CurDir))
        || !matches!(path.file_name(), Some(name) if !name.is_empty())
    {
        return Err(HelperError::InvalidBootstrapPath);
    }
    Ok(())
}

fn validate_expiry(document: &BootstrapDocument) -> Result<(), HelperError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| HelperError::BootstrapExpired)?
        .as_millis();
    if u128::from(document.expires_at_unix_ms) <= now {
        return Err(HelperError::BootstrapExpired);
    }
    Ok(())
}

fn validate_scope(
    document: &BootstrapDocument,
    config: &PrivateEndpointConfig,
) -> Result<(), HelperError> {
    #[cfg(unix)]
    let expected_kind = BridgeEndpointKind::UnixDomainSocket;
    #[cfg(windows)]
    let expected_kind = BridgeEndpointKind::WindowsNamedPipe;

    let expected = private_endpoint_descriptor(config)?;
    if document.endpoint.kind() != expected_kind || document.endpoint != expected {
        return Err(HelperError::BootstrapScopeMismatch);
    }
    Ok(())
}

/// Produces a bounded, secret-free diagnostic for the early CLI dispatcher.
pub fn bounded_diagnostic(error: &HelperError) -> String {
    let mut message = error.to_string();
    if message.len() > MAX_HELPER_DIAGNOSTIC_BYTES {
        message.truncate(MAX_HELPER_DIAGNOSTIC_BYTES);
    }
    message
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::collections::VecDeque;
    use tokio::io::AsyncReadExt;

    struct MockTransport {
        endpoint: crate::BridgeEndpoint,
        inbound: VecDeque<Result<Option<BridgeFrame>, PrivateIpcError>>,
        sent: Vec<BridgeFrame>,
        shutdown: bool,
    }

    impl MockTransport {
        fn with_frames(frames: impl IntoIterator<Item = BridgeFrame>) -> Self {
            Self {
                endpoint: crate::BridgeEndpoint::new(
                    #[cfg(unix)]
                    BridgeEndpointKind::UnixDomainSocket,
                    #[cfg(windows)]
                    BridgeEndpointKind::WindowsNamedPipe,
                    #[cfg(unix)]
                    "/private/mock.sock",
                    #[cfg(windows)]
                    r"\\.\pipe\pioneer-cli-mcp-mock-g1",
                )
                .expect("endpoint"),
                inbound: frames.into_iter().map(|frame| Ok(Some(frame))).collect(),
                sent: Vec::new(),
                shutdown: false,
            }
        }
    }

    #[async_trait]
    impl BridgeFrameTransport for MockTransport {
        fn endpoint(&self) -> &crate::BridgeEndpoint {
            &self.endpoint
        }

        fn peer_identity(&self) -> crate::PeerIdentity {
            crate::PeerIdentity {
                process_id: Some(std::process::id()),
                user_id: None,
            }
        }

        async fn receive_frame(&mut self) -> Result<Option<BridgeFrame>, PrivateIpcError> {
            match self.inbound.pop_front() {
                Some(result) => result,
                None => std::future::pending().await,
            }
        }

        async fn send_frame(&mut self, frame: &BridgeFrame) -> Result<(), PrivateIpcError> {
            self.sent.push(frame.clone());
            Ok(())
        }

        async fn shutdown(&mut self) -> Result<(), PrivateIpcError> {
            self.shutdown = true;
            Ok(())
        }
    }

    #[test]
    fn helper_hidden_args_require_exact_non_secret_shape() {
        assert!(
            parse_hidden_helper_args(std::iter::empty())
                .unwrap()
                .is_none()
        );
        assert!(
            parse_hidden_helper_args([OsString::from("status")].into_iter())
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            parse_hidden_helper_args([OsString::from(HIDDEN_HELPER_COMMAND)].into_iter()),
            Err(HelperError::InvalidArguments)
        ));
        assert_eq!(
            parse_hidden_helper_args(
                [
                    OsString::from(HIDDEN_HELPER_COMMAND),
                    OsString::from(BOOTSTRAP_FILE_FLAG),
                    OsString::from("/private/bootstrap"),
                ]
                .into_iter(),
            )
            .expect("parse"),
            Some(PathBuf::from("/private/bootstrap"))
        );
    }

    #[test]
    fn helper_diagnostics_are_bounded_and_do_not_include_nonce_material() {
        let diagnostic = bounded_diagnostic(&HelperError::AttachRejected);
        assert!(diagnostic.len() <= MAX_HELPER_DIAGNOSTIC_BYTES);
        assert!(!diagnostic.contains("nonce"));
    }

    #[test]
    fn helper_rejects_expired_bootstrap_before_connect() {
        let document = BootstrapDocument {
            session_id: crate::BridgeSessionId::new("expired").expect("session"),
            generation: crate::BridgeGeneration::new(1).expect("generation"),
            endpoint: crate::BridgeEndpoint::new(
                #[cfg(unix)]
                BridgeEndpointKind::UnixDomainSocket,
                #[cfg(windows)]
                BridgeEndpointKind::WindowsNamedPipe,
                "expired",
            )
            .expect("endpoint"),
            nonce: crate::BootstrapNonce::new([7; crate::NONCE_BYTES]).expect("nonce"),
            expires_at_unix_ms: 1,
        };
        assert!(matches!(
            validate_expiry(&document),
            Err(HelperError::BootstrapExpired)
        ));
    }

    #[test]
    fn helper_rejects_endpoint_scope_mismatch() {
        let config = PrivateEndpointConfig {
            managed_directory: PathBuf::from("/private"),
            session_id: crate::BridgeSessionId::new("scope-test").expect("session"),
            generation: crate::BridgeGeneration::new(1).expect("generation"),
            expected_peer_pid: None,
        };
        let document = BootstrapDocument {
            session_id: config.session_id.clone(),
            generation: config.generation,
            endpoint: crate::BridgeEndpoint::new(
                #[cfg(unix)]
                BridgeEndpointKind::UnixDomainSocket,
                #[cfg(windows)]
                BridgeEndpointKind::WindowsNamedPipe,
                "wrong-endpoint",
            )
            .expect("endpoint"),
            nonce: crate::BootstrapNonce::new([8; crate::NONCE_BYTES]).expect("nonce"),
            expires_at_unix_ms: u64::MAX,
        };
        assert!(matches!(
            validate_scope(&document, &config),
            Err(HelperError::BootstrapScopeMismatch)
        ));
    }

    #[tokio::test]
    async fn helper_eof_sends_shutdown_without_stdout() {
        let mut transport = MockTransport::with_frames([]);
        relay(tokio::io::empty(), tokio::io::sink(), &mut transport)
            .await
            .expect("clean eof");
        assert!(transport.shutdown);
        assert_eq!(transport.sent.len(), 1);
        assert_eq!(transport.sent[0].frame_type(), BridgeFrameType::Shutdown);
    }

    #[tokio::test]
    async fn helper_cancellation_stops_relay_without_protocol_output() {
        let cancellation =
            BridgeFrame::new(BridgeFrameType::Cancellation, Vec::new()).expect("frame");
        let mut transport = MockTransport::with_frames([cancellation]);
        let (_input_writer, input_reader) = tokio::io::duplex(16);
        let (output_writer, mut output_reader) = tokio::io::duplex(16);
        assert!(matches!(
            relay(input_reader, output_writer, &mut transport).await,
            Err(HelperError::Cancelled)
        ));
        let mut output = Vec::new();
        output_reader
            .read_to_end(&mut output)
            .await
            .expect("read output");
        assert!(output.is_empty(), "stdout must stay protocol-clean");
    }

    #[tokio::test]
    async fn helper_peer_shutdown_keeps_stdout_clean() {
        let shutdown = BridgeFrame::new(BridgeFrameType::Shutdown, Vec::new()).expect("frame");
        let mut transport = MockTransport::with_frames([shutdown]);
        let (_input_writer, input_reader) = tokio::io::duplex(16);
        let (output_writer, mut output_reader) = tokio::io::duplex(16);
        relay(input_reader, output_writer, &mut transport)
            .await
            .expect("peer shutdown");
        let mut output = Vec::new();
        output_reader
            .read_to_end(&mut output)
            .await
            .expect("read output");
        assert!(output.is_empty(), "stdout must stay protocol-clean");
    }
}
