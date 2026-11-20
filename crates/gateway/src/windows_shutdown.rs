//! Current-user service control for Windows Task Scheduler.

use std::{
    io,
    os::windows::{
        ffi::OsStrExt,
        io::{AsRawHandle, FromRawHandle, OwnedHandle},
    },
    path::Path,
    time::Duration,
};

use sha2::{Digest, Sha256};
use windows_sys::Win32::{
    Foundation::{ERROR_FILE_NOT_FOUND, WAIT_FAILED, WAIT_OBJECT_0, WAIT_TIMEOUT},
    System::Threading::{
        CreateEventW, EVENT_MODIFY_STATE, OpenEventW, SetEvent, WaitForSingleObject,
    },
};

fn event_name(runtime_home: &Path) -> io::Result<Vec<u16>> {
    let canonical = runtime_home.canonicalize()?;
    let mut hash = Sha256::new();
    for unit in canonical.as_os_str().encode_wide() {
        hash.update(unit.to_le_bytes());
    }
    // Task Scheduler and the interactive CLI may run in different sessions.
    // The descriptor derives from the creating user's Windows token.
    Ok(format!(
        "Global\\PioneerGatewayShutdown-{}",
        hex::encode(hash.finalize())
    )
    .encode_utf16()
    .chain(Some(0))
    .collect())
}

pub(crate) struct ShutdownEvent(OwnedHandle);

impl ShutdownEvent {
    pub(crate) fn listen(runtime_home: &Path) -> io::Result<Self> {
        let name = event_name(runtime_home)?;
        // The UTF-16 name is terminated and remains alive through the call.
        let handle = unsafe { CreateEventW(std::ptr::null(), 1, 0, name.as_ptr()) };
        if handle.is_null() {
            return Err(io::Error::last_os_error());
        }
        // A successful call returns an owned, non-inheritable handle.
        Ok(Self(unsafe { OwnedHandle::from_raw_handle(handle) }))
    }

    pub(crate) async fn wait(&self) -> io::Result<()> {
        loop {
            // A zero timeout keeps the wait cancellable and never blocks Tokio.
            match unsafe { WaitForSingleObject(self.0.as_raw_handle(), 0) } {
                WAIT_OBJECT_0 => return Ok(()),
                WAIT_TIMEOUT => tokio::time::sleep(Duration::from_millis(250)).await,
                WAIT_FAILED => return Err(io::Error::last_os_error()),
                _ => return Err(io::Error::other("unexpected Gateway shutdown event state")),
            }
        }
    }
}

/// Request the running Gateway to finish its shutdown path. A missing event
/// indicates that the configured Gateway has yet to start or predates this
/// service control.
pub fn signal_windows_gateway_shutdown(runtime_home: &Path) -> io::Result<bool> {
    let name = match event_name(runtime_home) {
        Ok(name) => name,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    let handle = unsafe { OpenEventW(EVENT_MODIFY_STATE, 0, name.as_ptr()) };
    if handle.is_null() {
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(ERROR_FILE_NOT_FOUND as i32) {
            return Ok(false);
        }
        return Err(error);
    }
    let handle = unsafe { OwnedHandle::from_raw_handle(handle) };
    if unsafe { SetEvent(handle.as_raw_handle()) } == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn shutdown_signal_is_latched_and_scoped_to_the_runtime_directory() {
        let runtime = tempfile::tempdir().unwrap();
        let other = tempfile::tempdir().unwrap();
        let event = ShutdownEvent::listen(runtime.path()).unwrap();
        assert!(!signal_windows_gateway_shutdown(other.path()).unwrap());
        assert!(signal_windows_gateway_shutdown(runtime.path()).unwrap());
        tokio::time::timeout(Duration::from_secs(1), event.wait())
            .await
            .unwrap()
            .unwrap();
        drop(event);
        assert!(!signal_windows_gateway_shutdown(runtime.path()).unwrap());
    }
}
