use super::{
    BridgeFrameTransport, PeerIdentity, PrivateEndpointConfig, PrivateIpcError, read_async_frame,
    shutdown_async, write_async_frame,
};
use crate::bootstrap::{BridgeEndpoint, BridgeEndpointKind};
use crate::framing::BridgeFrame;
use async_trait::async_trait;
use std::fs;
use std::io;
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
use tokio::net::{UnixListener, UnixStream};

#[cfg(test)]
const OWNER_DIRECTORY_MODE: u32 = 0o700;
const OWNER_SOCKET_MODE: u32 = 0o600;
// Conservative across supported Unix sockaddr_un.sun_path implementations.
const MAX_SOCKET_PATH_BYTES: usize = 103;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct EndpointIdentity {
    device: u64,
    inode: u64,
    owner: u32,
}

pub struct PlatformListener {
    listener: UnixListener,
    endpoint: BridgeEndpoint,
    path: PathBuf,
    identity: EndpointIdentity,
    expected_peer_pid: Option<u32>,
}

pub struct PlatformConnection {
    stream: UnixStream,
    endpoint: BridgeEndpoint,
    peer: PeerIdentity,
}

impl PlatformListener {
    pub fn endpoint(&self) -> &BridgeEndpoint {
        &self.endpoint
    }

    /// Pins the listener to the helper pid after the helper has been spawned
    /// but before the first accept.
    pub fn set_expected_peer_pid(&mut self, process_id: u32) {
        self.expected_peer_pid = Some(process_id);
    }

    pub async fn accept(&mut self) -> Result<PlatformConnection, PrivateIpcError> {
        revalidate_endpoint(&self.path, self.identity)?;
        let (stream, _address) = self.listener.accept().await?;
        let peer = validate_peer(&stream, self.expected_peer_pid)?;
        revalidate_endpoint(&self.path, self.identity)?;
        Ok(PlatformConnection {
            stream,
            endpoint: self.endpoint.clone(),
            peer,
        })
    }
}

impl Drop for PlatformListener {
    fn drop(&mut self) {
        // Never unlink a path that another process replaced after this listener
        // was created. A failed validation intentionally leaves the path alone.
        if revalidate_endpoint(&self.path, self.identity).is_ok() {
            let _ = fs::remove_file(&self.path);
        }
    }
}

#[async_trait]
impl BridgeFrameTransport for PlatformConnection {
    fn endpoint(&self) -> &BridgeEndpoint {
        &self.endpoint
    }

    fn peer_identity(&self) -> PeerIdentity {
        self.peer
    }

    async fn receive_frame(&mut self) -> Result<Option<BridgeFrame>, PrivateIpcError> {
        read_async_frame(&mut self.stream).await
    }

    async fn send_frame(&mut self, frame: &BridgeFrame) -> Result<(), PrivateIpcError> {
        write_async_frame(&mut self.stream, frame).await
    }

    async fn shutdown(&mut self) -> Result<(), PrivateIpcError> {
        shutdown_async(&mut self.stream).await
    }
}

pub fn bind_private_endpoint(
    config: &PrivateEndpointConfig,
) -> Result<PlatformListener, PrivateIpcError> {
    validate_managed_directory(&config.managed_directory)?;
    let path = endpoint_path(config)?;
    reject_other_generation(config, &path)?;
    match fs::symlink_metadata(&path) {
        Ok(_) => return Err(PrivateIpcError::EndpointAlreadyExists),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }

    let listener = UnixListener::bind(&path)?;
    if let Err(error) = fs::set_permissions(&path, fs::Permissions::from_mode(OWNER_SOCKET_MODE)) {
        let _ = fs::remove_file(&path);
        return Err(error.into());
    }
    let identity = match socket_identity(&path) {
        Ok(identity) => identity,
        Err(error) => {
            let _ = fs::remove_file(&path);
            return Err(error);
        }
    };
    let endpoint = private_endpoint_descriptor(config)?;

    Ok(PlatformListener {
        listener,
        endpoint,
        path,
        identity,
        expected_peer_pid: config.expected_peer_pid,
    })
}

pub async fn connect_private_endpoint(
    config: &PrivateEndpointConfig,
) -> Result<PlatformConnection, PrivateIpcError> {
    validate_managed_directory(&config.managed_directory)?;
    let path = endpoint_path(config)?;
    let before = match socket_identity(&path) {
        Ok(identity) => identity,
        Err(PrivateIpcError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
            reject_other_generation(config, &path)?;
            return Err(PrivateIpcError::Io(error));
        }
        Err(error) => return Err(error),
    };
    let stream = UnixStream::connect(&path).await?;
    let peer = validate_peer(&stream, config.expected_peer_pid)?;
    revalidate_endpoint(&path, before)?;
    let endpoint = private_endpoint_descriptor(config)?;
    Ok(PlatformConnection {
        stream,
        endpoint,
        peer,
    })
}

pub fn private_endpoint_descriptor(
    config: &PrivateEndpointConfig,
) -> Result<BridgeEndpoint, PrivateIpcError> {
    let path = endpoint_path(config)?;
    BridgeEndpoint::new(
        BridgeEndpointKind::UnixDomainSocket,
        path.to_string_lossy().into_owned(),
    )
    .map_err(|_| PrivateIpcError::InvalidEndpoint)
}

fn validate_managed_directory(path: &Path) -> Result<(), PrivateIpcError> {
    if !path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, Component::ParentDir | Component::CurDir))
    {
        return Err(PrivateIpcError::InvalidManagedDirectory);
    }
    let metadata = fs::symlink_metadata(path).map_err(PrivateIpcError::Io)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(PrivateIpcError::InvalidManagedDirectory);
    }
    if metadata.uid() != current_uid() || metadata.mode() & 0o077 != 0 {
        return Err(PrivateIpcError::InsecureManagedDirectory);
    }
    Ok(())
}

fn endpoint_path(config: &PrivateEndpointConfig) -> Result<PathBuf, PrivateIpcError> {
    // The owner-only managed directory is already unique to the session and
    // generation. Repeating the opaque session id in the socket filename can
    // make every production-shaped path exceed macOS `sockaddr_un::sun_path`,
    // even beneath a short runtime root. Keep only the generation in the leaf
    // while the bootstrap document continues to authenticate the session id.
    let file_name = format!("bridge-g{}.sock", config.generation.get());
    let path = config.managed_directory.join(file_name);
    if path.as_os_str().as_encoded_bytes().len() > MAX_SOCKET_PATH_BYTES
        || path.parent() != Some(config.managed_directory.as_path())
    {
        return Err(PrivateIpcError::InvalidEndpoint);
    }
    Ok(path)
}

fn reject_other_generation(
    config: &PrivateEndpointConfig,
    expected_path: &Path,
) -> Result<(), PrivateIpcError> {
    let prefix = "bridge-g";
    for entry in fs::read_dir(&config.managed_directory)? {
        let entry = entry?;
        let path = entry.path();
        if path == expected_path {
            continue;
        }
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if name.starts_with(prefix) && name.ends_with(".sock") {
            return Err(PrivateIpcError::StaleGeneration);
        }
    }
    Ok(())
}

fn socket_identity(path: &Path) -> Result<EndpointIdentity, PrivateIpcError> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink()
        || !metadata.file_type().is_socket()
        || metadata.uid() != current_uid()
        || metadata.mode() & 0o077 != 0
    {
        return Err(PrivateIpcError::InvalidEndpoint);
    }
    Ok(EndpointIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
        owner: metadata.uid(),
    })
}

fn revalidate_endpoint(path: &Path, expected: EndpointIdentity) -> Result<(), PrivateIpcError> {
    let current = socket_identity(path).map_err(|_| PrivateIpcError::EndpointReplaced)?;
    if current != expected {
        return Err(PrivateIpcError::EndpointReplaced);
    }
    Ok(())
}

fn validate_peer(
    stream: &UnixStream,
    expected_peer_pid: Option<u32>,
) -> Result<PeerIdentity, PrivateIpcError> {
    // A peer may disconnect immediately after discovering that the opposite
    // endpoint does not match its pinned PID. Failure to obtain credentials
    // is still an authentication failure, not a generic transport failure.
    let credentials = stream.peer_cred().map_err(|_| PrivateIpcError::WrongPeer)?;
    let process_id = credentials.pid().and_then(|pid| u32::try_from(pid).ok());
    if credentials.uid() != current_uid()
        || expected_peer_pid.is_some_and(|expected| process_id != Some(expected))
    {
        return Err(PrivateIpcError::WrongPeer);
    }
    Ok(PeerIdentity {
        process_id,
        user_id: Some(credentials.uid()),
    })
}

fn current_uid() -> u32 {
    // SAFETY: getuid has no preconditions and cannot fail.
    unsafe { libc::getuid() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framing::{BridgeFrameType, MAX_FRAME_PAYLOAD_BYTES};
    use tempfile::TempDir;

    fn private_directory() -> TempDir {
        let directory = tempfile::tempdir().expect("temp directory");
        fs::set_permissions(
            directory.path(),
            fs::Permissions::from_mode(OWNER_DIRECTORY_MODE),
        )
        .expect("owner-only directory");
        directory
    }

    fn config(directory: &Path, generation: u64) -> PrivateEndpointConfig {
        PrivateEndpointConfig {
            managed_directory: directory.to_path_buf(),
            session_id: crate::BridgeSessionId::new("platform-test").expect("session id"),
            generation: crate::BridgeGeneration::new(generation).expect("generation"),
            expected_peer_pid: Some(std::process::id()),
        }
    }

    #[tokio::test]
    async fn platform_unix_private_connection_relays_bounded_frames() {
        let directory = private_directory();
        let mut listener = bind_private_endpoint(&config(directory.path(), 1)).expect("bind");
        let accept = async {
            let mut connection = listener.accept().await.expect("accept");
            let frame = connection
                .receive_frame()
                .await
                .expect("receive")
                .expect("frame");
            assert_eq!(frame.payload(), b"private-frame");
            connection.send_frame(&frame).await.expect("reply");
        };
        let connect = async {
            let mut connection = connect_private_endpoint(&config(directory.path(), 1))
                .await
                .expect("connect");
            let frame = BridgeFrame::new(BridgeFrameType::Payload, b"private-frame".to_vec())
                .expect("frame");
            connection.send_frame(&frame).await.expect("send");
            assert_eq!(
                connection
                    .receive_frame()
                    .await
                    .expect("receive")
                    .expect("frame"),
                frame
            );
        };
        tokio::join!(accept, connect);
    }

    #[test]
    fn ipc_permissions_reject_broad_or_symlinked_managed_directory() {
        let directory = tempfile::tempdir().expect("temp directory");
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o755))
            .expect("broad permissions");
        assert!(matches!(
            bind_private_endpoint(&config(directory.path(), 1)),
            Err(PrivateIpcError::InsecureManagedDirectory)
        ));

        let owner = private_directory();
        let link = owner.path().with_extension("link");
        std::os::unix::fs::symlink(owner.path(), &link).expect("symlink");
        assert!(matches!(
            bind_private_endpoint(&config(&link, 1)),
            Err(PrivateIpcError::InvalidManagedDirectory)
        ));
        fs::remove_file(link).expect("remove link");
    }

    #[tokio::test]
    async fn platform_rejects_stale_generation_and_preserves_replacement() {
        let directory = private_directory();
        let listener = bind_private_endpoint(&config(directory.path(), 1)).expect("bind");
        assert!(matches!(
            bind_private_endpoint(&config(directory.path(), 2)),
            Err(PrivateIpcError::StaleGeneration)
        ));

        let path = PathBuf::from(listener.endpoint().address());
        fs::remove_file(&path).expect("remove original endpoint");
        std::os::unix::fs::symlink("replacement", &path).expect("replacement symlink");
        drop(listener);
        assert!(
            fs::symlink_metadata(&path).is_ok(),
            "replacement must remain"
        );
        fs::remove_file(path).expect("remove replacement");
    }

    #[tokio::test]
    async fn platform_rejects_wrong_peer_process() {
        let directory = private_directory();
        let mut wrong = config(directory.path(), 1);
        wrong.expected_peer_pid = Some(std::process::id().saturating_add(1));
        let mut listener = bind_private_endpoint(&wrong).expect("bind");
        let (accepted, connected) =
            tokio::join!(listener.accept(), connect_private_endpoint(&wrong));
        match accepted {
            Err(PrivateIpcError::WrongPeer) => {}
            Err(error) => panic!("expected wrong server peer, got {error:?}"),
            Ok(_) => panic!("server accepted wrong peer process"),
        }
        match connected {
            Err(PrivateIpcError::WrongPeer) => {}
            Err(error) => panic!("expected wrong client peer, got {error:?}"),
            Ok(_) => panic!("client accepted wrong peer process"),
        }
    }

    #[tokio::test]
    async fn platform_unix_disconnect_is_observed_as_end_of_stream() {
        let directory = private_directory();
        let mut listener = bind_private_endpoint(&config(directory.path(), 6)).expect("bind");
        let connection_config = config(directory.path(), 6);
        let (accepted, connected) = tokio::join!(
            listener.accept(),
            connect_private_endpoint(&connection_config)
        );
        let mut accepted = accepted.expect("accept");
        drop(connected.expect("connect"));
        assert!(accepted.receive_frame().await.expect("read").is_none());
    }

    #[test]
    fn platform_frame_limit_remains_globally_bounded() {
        assert_eq!(MAX_FRAME_PAYLOAD_BYTES, 8 * 1024 * 1024);
    }
}
