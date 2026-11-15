//! Windows host duties around the published portable-pty API.
use super::pty::PtyInput;
use anyhow::{Result, bail};
use std::{io, sync::mpsc::SyncSender};

pub(super) const CURSOR_REPLY: &[u8] = b"\x1b[1;1R";

pub(super) fn check_supported() -> Result<()> {
    use windows_sys::Win32::System::LibraryLoader::{GetModuleHandleW, GetProcAddress};
    let name: Vec<u16> = "kernel32.dll\0".encode_utf16().collect();
    // SAFETY: NUL-terminated names, borrowed module handle, no calls through
    // the returned pointers. Avoid portable-pty's panic on pre-ConPTY Windows.
    unsafe {
        let module = GetModuleHandleW(name.as_ptr());
        if module.is_null()
            || [
                b"CreatePseudoConsole\0".as_slice(),
                b"ResizePseudoConsole\0".as_slice(),
                b"ClosePseudoConsole\0".as_slice(),
            ]
            .iter()
            .any(|symbol| GetProcAddress(module, symbol.as_ptr()).is_none())
        {
            bail!("host.pty_unsupported_platform");
        }
    }
    Ok(())
}

pub(super) struct CursorHandshake {
    reply: SyncSender<PtyInput>,
    prefix: Vec<u8>,
    complete: bool,
}
impl CursorHandshake {
    pub(super) fn new(reply: SyncSender<PtyInput>) -> Self {
        Self {
            reply,
            prefix: Vec::with_capacity(4),
            complete: false,
        }
    }
    pub(super) fn pending(&self) -> bool {
        !self.complete
    }
    pub(super) fn filter(&mut self, bytes: &[u8]) -> io::Result<Vec<u8>> {
        const QUERY: &[u8] = b"\x1b[6n";
        let mut output = Vec::with_capacity(bytes.len() + self.prefix.len());
        for (index, byte) in bytes.iter().enumerate() {
            if self.complete {
                output.extend_from_slice(&bytes[index..]);
                break;
            }
            self.prefix.push(*byte);
            while !QUERY.starts_with(&self.prefix) {
                output.push(self.prefix.remove(0));
            }
            if self.prefix == QUERY {
                // Each session starts with an empty viewport. Answer only
                // ConPTY's initial inheritance query, even without a view.
                // Later application cursor queries belong to the renderer.
                self.prefix.clear();
                self.complete = true;
                self.reply
                    .send(PtyInput::CursorReply)
                    .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "PTY input closed"))?;
            }
        }
        Ok(output)
    }
    pub(super) fn finish(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.prefix)
    }
}
