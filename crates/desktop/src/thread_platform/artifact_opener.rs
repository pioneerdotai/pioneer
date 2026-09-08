use anyhow::{Context as _, Result};
use std::{path::Path, process::Command};
#[cfg(target_os = "macos")]
pub(crate) fn spawn_open_url(url: &str) -> Result<()> {
    spawn_command(Command::new("open").arg(url), "open artifact view")
}

#[cfg(all(unix, not(target_os = "macos")))]
pub(crate) fn spawn_open_url(url: &str) -> Result<()> {
    spawn_command(Command::new("xdg-open").arg(url), "open artifact view")
}

#[cfg(windows)]
pub(crate) fn spawn_open_url(url: &str) -> Result<()> {
    shell_open(std::ffi::OsStr::new(url), "open artifact view")
}

#[cfg(target_os = "macos")]
pub(crate) fn spawn_open_file(path: &Path) -> Result<()> {
    spawn_command(Command::new("open").arg(path), "open artifact file")
}

#[cfg(target_os = "macos")]
pub(crate) fn spawn_reveal_file(path: &Path) -> Result<()> {
    spawn_command(
        Command::new("open").arg("-R").arg(path),
        "reveal artifact file",
    )
}

#[cfg(all(unix, not(target_os = "macos")))]
pub(crate) fn spawn_open_file(path: &Path) -> Result<()> {
    spawn_command(Command::new("xdg-open").arg(path), "open artifact file")
}

#[cfg(all(unix, not(target_os = "macos")))]
pub(crate) fn spawn_reveal_file(path: &Path) -> Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    spawn_command(Command::new("xdg-open").arg(parent), "reveal artifact file")
}

#[cfg(windows)]
pub(crate) fn spawn_open_file(path: &Path) -> Result<()> {
    shell_open(path.as_os_str(), "open artifact file")
}

#[cfg(windows)]
pub(crate) fn spawn_reveal_file(path: &Path) -> Result<()> {
    spawn_command(
        Command::new("explorer").arg("/select,").arg(path),
        "reveal artifact file",
    )
}

#[cfg(windows)]
fn shell_open(target: &std::ffi::OsStr, action: &str) -> Result<()> {
    use std::os::windows::ffi::OsStrExt as _;
    use windows_sys::Win32::UI::Shell::ShellExecuteW;
    use windows_sys::Win32::UI::WindowsAndMessaging::SW_SHOWNORMAL;

    let mut target = target.encode_wide().collect::<Vec<_>>();
    if target.contains(&0) {
        anyhow::bail!("failed to {action}: target contains a NUL character");
    }
    target.push(0);
    let operation = "open\0".encode_utf16().collect::<Vec<_>>();
    let result = unsafe {
        ShellExecuteW(
            std::ptr::null_mut(),
            operation.as_ptr(),
            target.as_ptr(),
            std::ptr::null(),
            std::ptr::null(),
            SW_SHOWNORMAL,
        )
    };
    if result as isize <= 32 {
        anyhow::bail!("failed to {action}: ShellExecuteW returned {result:?}");
    }
    Ok(())
}

fn spawn_command(command: &mut Command, action: &str) -> Result<()> {
    command
        .spawn()
        .with_context(|| format!("failed to {action}"))?;
    Ok(())
}
