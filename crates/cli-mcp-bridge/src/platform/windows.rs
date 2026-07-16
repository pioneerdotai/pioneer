use super::{
    BridgeFrameTransport, PeerIdentity, PrivateEndpointConfig, PrivateIpcError, read_async_frame,
    shutdown_async, write_async_frame,
};
use crate::bootstrap::{BridgeEndpoint, BridgeEndpointKind};
use crate::framing::BridgeFrame;
use async_trait::async_trait;
use std::ffi::{OsStr, c_void};
use std::io;
use std::mem::size_of;
use std::os::windows::ffi::OsStrExt;
use std::os::windows::io::AsRawHandle;
use std::ptr;
use tokio::net::windows::named_pipe::{
    ClientOptions, NamedPipeClient, NamedPipeServer, ServerOptions,
};
use windows_sys::Win32::Foundation::{CloseHandle, HANDLE, LocalFree};
use windows_sys::Win32::Security::Authorization::{
    ConvertSidToStringSidW, ConvertStringSecurityDescriptorToSecurityDescriptorW, SDDL_REVISION_1,
};
use windows_sys::Win32::Security::{
    GetTokenInformation, PSECURITY_DESCRIPTOR, SECURITY_ATTRIBUTES, TOKEN_QUERY, TOKEN_USER,
    TokenUser,
};
use windows_sys::Win32::System::Pipes::{GetNamedPipeClientProcessId, GetNamedPipeServerProcessId};
use windows_sys::Win32::System::Threading::{
    GetCurrentProcess, OpenProcess, OpenProcessToken, PROCESS_QUERY_LIMITED_INFORMATION,
};

const PIPE_PREFIX: &str = r"\\.\pipe\pioneer-cli-mcp-";

pub struct PlatformListener {
    server: Option<NamedPipeServer>,
    endpoint: BridgeEndpoint,
    expected_peer_pid: Option<u32>,
}

enum PipeStream {
    Server(NamedPipeServer),
    Client(NamedPipeClient),
}

pub struct PlatformConnection {
    stream: PipeStream,
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
        let server = self
            .server
            .take()
            .ok_or(PrivateIpcError::EndpointReplaced)?;
        server.connect().await?;
        let peer_pid = named_pipe_peer_pid(server.as_raw_handle() as HANDLE, true)?;
        validate_peer_process(peer_pid, self.expected_peer_pid)?;
        Ok(PlatformConnection {
            stream: PipeStream::Server(server),
            endpoint: self.endpoint.clone(),
            peer: PeerIdentity {
                process_id: Some(peer_pid),
                user_id: None,
            },
        })
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
        match &mut self.stream {
            PipeStream::Server(stream) => read_async_frame(stream).await,
            PipeStream::Client(stream) => read_async_frame(stream).await,
        }
    }

    async fn send_frame(&mut self, frame: &BridgeFrame) -> Result<(), PrivateIpcError> {
        match &mut self.stream {
            PipeStream::Server(stream) => write_async_frame(stream, frame).await,
            PipeStream::Client(stream) => write_async_frame(stream, frame).await,
        }
    }

    async fn shutdown(&mut self) -> Result<(), PrivateIpcError> {
        match &mut self.stream {
            PipeStream::Server(stream) => shutdown_async(stream).await,
            PipeStream::Client(stream) => shutdown_async(stream).await,
        }
    }
}

pub fn bind_private_endpoint(
    config: &PrivateEndpointConfig,
) -> Result<PlatformListener, PrivateIpcError> {
    let pipe_name = pipe_name(config)?;
    let current_sid = process_sid(None)?;
    let descriptor = OwnedSecurityDescriptor::owner_only(current_sid.as_str())?;
    let mut attributes = SECURITY_ATTRIBUTES {
        nLength: u32::try_from(size_of::<SECURITY_ATTRIBUTES>())
            .map_err(|_| PrivateIpcError::InvalidEndpoint)?,
        lpSecurityDescriptor: descriptor.as_ptr(),
        bInheritHandle: 0,
    };
    let mut options = ServerOptions::new();
    options
        .first_pipe_instance(true)
        .reject_remote_clients(true);
    // SAFETY: `attributes` and its owned descriptor remain alive for the
    // duration of CreateNamedPipeW. Handles are explicitly non-inheritable.
    let server = unsafe {
        options.create_with_security_attributes_raw(
            pipe_name.as_str(),
            (&mut attributes as *mut SECURITY_ATTRIBUTES).cast::<c_void>(),
        )?
    };
    let endpoint = private_endpoint_descriptor(config)?;
    Ok(PlatformListener {
        server: Some(server),
        endpoint,
        expected_peer_pid: config.expected_peer_pid,
    })
}

pub async fn connect_private_endpoint(
    config: &PrivateEndpointConfig,
) -> Result<PlatformConnection, PrivateIpcError> {
    let pipe_name = pipe_name(config)?;
    let client = ClientOptions::new().open(pipe_name.as_str())?;
    let peer_pid = named_pipe_peer_pid(client.as_raw_handle() as HANDLE, false)?;
    validate_peer_process(peer_pid, config.expected_peer_pid)?;
    let endpoint = private_endpoint_descriptor(config)?;
    Ok(PlatformConnection {
        stream: PipeStream::Client(client),
        endpoint,
        peer: PeerIdentity {
            process_id: Some(peer_pid),
            user_id: None,
        },
    })
}

pub fn private_endpoint_descriptor(
    config: &PrivateEndpointConfig,
) -> Result<BridgeEndpoint, PrivateIpcError> {
    BridgeEndpoint::new(BridgeEndpointKind::WindowsNamedPipe, pipe_name(config)?)
        .map_err(|_| PrivateIpcError::InvalidEndpoint)
}

fn pipe_name(config: &PrivateEndpointConfig) -> Result<String, PrivateIpcError> {
    let name = format!(
        "{PIPE_PREFIX}{}-g{}",
        config.session_id.as_str(),
        config.generation.get()
    );
    if name.len() > 256 || !name.starts_with(PIPE_PREFIX) {
        return Err(PrivateIpcError::InvalidEndpoint);
    }
    Ok(name)
}

fn named_pipe_peer_pid(handle: HANDLE, server_side: bool) -> Result<u32, PrivateIpcError> {
    let mut process_id = 0_u32;
    // SAFETY: `handle` is a live named-pipe handle owned by Tokio and the
    // output points to initialized writable storage.
    let success = unsafe {
        if server_side {
            GetNamedPipeClientProcessId(handle, &mut process_id)
        } else {
            GetNamedPipeServerProcessId(handle, &mut process_id)
        }
    };
    if success == 0 || process_id == 0 {
        return Err(io::Error::last_os_error().into());
    }
    Ok(process_id)
}

fn validate_peer_process(
    process_id: u32,
    expected_process_id: Option<u32>,
) -> Result<(), PrivateIpcError> {
    if expected_process_id.is_some_and(|expected| expected != process_id)
        || process_sid(Some(process_id))? != process_sid(None)?
    {
        return Err(PrivateIpcError::WrongPeer);
    }
    Ok(())
}

struct OwnedHandle(HANDLE);

impl Drop for OwnedHandle {
    fn drop(&mut self) {
        if !self.0.is_null() {
            // SAFETY: this wrapper exclusively owns handles returned by
            // OpenProcess/OpenProcessToken.
            unsafe { CloseHandle(self.0) };
        }
    }
}

fn process_sid(process_id: Option<u32>) -> Result<String, PrivateIpcError> {
    let process = match process_id {
        Some(process_id) => {
            // SAFETY: no pointer inputs; the returned handle is checked.
            let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, process_id) };
            if handle.is_null() {
                return Err(PrivateIpcError::WrongPeer);
            }
            Some(OwnedHandle(handle))
        }
        None => None,
    };
    // SAFETY: GetCurrentProcess returns a valid pseudo-handle. An owned peer
    // process handle remains alive through the token query.
    let process_handle = process
        .as_ref()
        .map_or_else(|| unsafe { GetCurrentProcess() }, |handle| handle.0);
    let mut raw_token: HANDLE = ptr::null_mut();
    // SAFETY: process handle is valid and raw_token is writable.
    if unsafe { OpenProcessToken(process_handle, TOKEN_QUERY, &mut raw_token) } == 0 {
        return Err(PrivateIpcError::WrongPeer);
    }
    let token = OwnedHandle(raw_token);

    let mut required = 0_u32;
    // The first call intentionally obtains the required buffer size.
    unsafe { GetTokenInformation(token.0, TokenUser, ptr::null_mut(), 0, &mut required) };
    if required == 0 {
        return Err(PrivateIpcError::WrongPeer);
    }
    let mut token_info = vec![0_u8; required as usize];
    // SAFETY: token_info has exactly the size reported by Windows and remains
    // alive while its TOKEN_USER/SID pointer is used.
    if unsafe {
        GetTokenInformation(
            token.0,
            TokenUser,
            token_info.as_mut_ptr().cast(),
            required,
            &mut required,
        )
    } == 0
    {
        return Err(PrivateIpcError::WrongPeer);
    }
    let user = unsafe { &*(token_info.as_ptr().cast::<TOKEN_USER>()) };
    let mut sid_string = ptr::null_mut();
    // SAFETY: the SID belongs to token_info and sid_string is writable.
    if unsafe { ConvertSidToStringSidW(user.User.Sid, &mut sid_string) } == 0
        || sid_string.is_null()
    {
        return Err(PrivateIpcError::WrongPeer);
    }
    let sid = unsafe {
        let mut length = 0;
        while *sid_string.add(length) != 0 {
            length += 1;
        }
        String::from_utf16(&std::slice::from_raw_parts(sid_string, length))
    }
    .map_err(|_| PrivateIpcError::WrongPeer);
    // SAFETY: ConvertSidToStringSidW allocates with LocalAlloc.
    unsafe { LocalFree(sid_string.cast()) };
    sid
}

struct OwnedSecurityDescriptor(PSECURITY_DESCRIPTOR);

impl OwnedSecurityDescriptor {
    fn owner_only(sid: &str) -> Result<Self, PrivateIpcError> {
        // Protected DACL with exactly one generic-all ACE for the current
        // user/service identity. No inherited ACL is admitted.
        let sddl = owner_only_sddl(sid);
        let wide = OsStr::new(sddl.as_str())
            .encode_wide()
            .chain(std::iter::once(0))
            .collect::<Vec<_>>();
        let mut descriptor = ptr::null_mut();
        // SAFETY: wide is NUL-terminated and descriptor is writable.
        if unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                wide.as_ptr(),
                SDDL_REVISION_1,
                &mut descriptor,
                ptr::null_mut(),
            )
        } == 0
            || descriptor.is_null()
        {
            return Err(io::Error::last_os_error().into());
        }
        Ok(Self(descriptor))
    }

    fn as_ptr(&self) -> *mut c_void {
        self.0.cast()
    }
}

fn owner_only_sddl(sid: &str) -> String {
    format!("D:P(A;;GA;;;{sid})")
}

impl Drop for OwnedSecurityDescriptor {
    fn drop(&mut self) {
        if !self.0.is_null() {
            // SAFETY: the descriptor was allocated by the SDDL conversion API.
            unsafe { LocalFree(self.0.cast()) };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(generation: u64) -> PrivateEndpointConfig {
        PrivateEndpointConfig {
            managed_directory: Default::default(),
            session_id: crate::BridgeSessionId::new(format!(
                "platform-windows-{}",
                std::process::id()
            ))
            .expect("session id"),
            generation: crate::BridgeGeneration::new(generation).expect("generation"),
            expected_peer_pid: Some(std::process::id()),
        }
    }

    #[tokio::test]
    async fn platform_windows_private_connection_uses_owner_acl_and_peer_identity() {
        let connection_config = config(1);
        let mut listener = bind_private_endpoint(&connection_config).expect("bind");
        let (accepted, connected) = tokio::join!(
            listener.accept(),
            connect_private_endpoint(&connection_config)
        );
        let accepted = accepted.expect("accept");
        let connected = connected.expect("connect");
        assert_eq!(
            accepted.peer_identity().process_id,
            Some(std::process::id())
        );
        assert_eq!(
            connected.peer_identity().process_id,
            Some(std::process::id())
        );
    }

    #[test]
    fn ipc_permissions_use_protected_single_owner_dacl() {
        assert_eq!(owner_only_sddl("S-1-5-21-1"), "D:P(A;;GA;;;S-1-5-21-1)");
    }

    #[test]
    fn ipc_permissions_named_pipe_name_contains_exact_generation() {
        let first = pipe_name(&config(1)).expect("first name");
        let second = pipe_name(&config(2)).expect("second name");
        assert_ne!(first, second);
        assert!(first.ends_with("-g1"));
        assert!(second.ends_with("-g2"));
    }

    #[test]
    fn platform_rejects_named_pipe_endpoint_reuse() {
        let _listener = bind_private_endpoint(&config(3)).expect("first bind");
        assert!(bind_private_endpoint(&config(3)).is_err());
    }

    #[tokio::test]
    async fn platform_rejects_wrong_named_pipe_peer_process() {
        let mut wrong = config(4);
        wrong.expected_peer_pid = Some(std::process::id().saturating_add(1));
        let mut listener = bind_private_endpoint(&wrong).expect("bind");
        let (accepted, connected) =
            tokio::join!(listener.accept(), connect_private_endpoint(&wrong));
        assert!(matches!(accepted, Err(PrivateIpcError::WrongPeer)));
        assert!(matches!(connected, Err(PrivateIpcError::WrongPeer)));
    }

    #[tokio::test]
    async fn platform_windows_disconnect_is_observed() {
        let connection_config = config(5);
        let mut listener = bind_private_endpoint(&connection_config).expect("bind");
        let (accepted, connected) = tokio::join!(
            listener.accept(),
            connect_private_endpoint(&connection_config)
        );
        let mut accepted = accepted.expect("accept");
        drop(connected.expect("connect"));
        let outcome = accepted.receive_frame().await;
        assert!(
            matches!(outcome, Ok(None) | Err(PrivateIpcError::Io(_))),
            "pipe disconnect must not produce a frame"
        );
    }
}
