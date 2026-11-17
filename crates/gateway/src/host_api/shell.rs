//! Host shell environment. Credentials are never inherited wholesale, and locale
//! probes use private locale objects rather than changing the Gateway's locale.
use anyhow::Result;
#[cfg(unix)]
use anyhow::{Context as _, bail};
use portable_pty::CommandBuilder;
#[cfg(unix)]
use std::ffi::CString;

#[cfg(unix)]
pub(super) fn configure_environment(command: &mut CommandBuilder) -> Result<()> {
    command.env_clear();
    for name in [
        "HOME",
        "PATH",
        "USER",
        "LOGNAME",
        "SHELL",
        "LANG",
        "LANGUAGE",
        "LC_ALL",
        "LC_CTYPE",
        "LC_COLLATE",
        "LC_MESSAGES",
        "LC_MONETARY",
        "LC_NUMERIC",
        "LC_TIME",
        "LC_PAPER",
        "LC_NAME",
        "LC_ADDRESS",
        "LC_TELEPHONE",
        "LC_MEASUREMENT",
        "LC_IDENTIFICATION",
    ] {
        if let Some(value) = std::env::var_os(name) {
            command.env(name, value);
        }
    }
    // Normalize an invalid/missing inherited SHELL to the same executable
    // that portable-pty will choose for the account's default program.
    let shell = command.get_shell();
    command.env("SHELL", shell);
    command.env("TERM", "xterm-256color");
    // LC_ALL takes precedence over both LANG and LC_CTYPE. Keeping LC_ALL=C
    // while setting LANG=UTF-8 would leave readline in byte-oriented mode.
    if valid_utf8(command.get_env("LC_ALL")) {
        return Ok(());
    }
    command.env_remove("LC_ALL");
    let inherited = [command.get_env("LANG"), command.get_env("LC_CTYPE")]
        .into_iter()
        .find(|name| valid_utf8(*name))
        .flatten()
        .map(str_from_os)
        .transpose()?;
    let selected = match inherited {
        Some(name) => name,
        None => ["C.UTF-8", "en_US.UTF-8", "UTF-8"]
            .into_iter()
            .find(|name| valid_utf8(Some(std::ffi::OsStr::new(name))))
            .map(str::to_owned)
            .context("host.utf8_locale_unavailable")?,
    };
    if !valid_utf8(command.get_env("LANG")) {
        command.env("LANG", &selected);
    }
    if !valid_utf8(command.get_env("LC_CTYPE")) {
        command.env("LC_CTYPE", &selected);
    }
    Ok(())
}

#[cfg(unix)]
fn str_from_os(value: &std::ffi::OsStr) -> Result<String> {
    value
        .to_str()
        .map(str::to_owned)
        .context("host.utf8_locale_unavailable")
}

#[cfg(unix)]
fn valid_utf8(value: Option<&std::ffi::OsStr>) -> bool {
    let Some(name) = value.and_then(std::ffi::OsStr::to_str) else {
        return false;
    };
    let encoding = name
        .split('@')
        .next()
        .unwrap_or(name)
        .replace('-', "")
        .to_ascii_lowercase();
    if !encoding.ends_with("utf8") {
        return false;
    }
    let Ok(name) = CString::new(name) else {
        return false;
    };
    // SAFETY: NUL-terminated locale name and a fresh owned locale object. This
    // neither calls setlocale nor installs a thread/process-global locale.
    unsafe {
        let locale = libc::newlocale(libc::LC_CTYPE_MASK, name.as_ptr(), std::ptr::null_mut());
        if locale.is_null() {
            return false;
        }
        libc::freelocale(locale);
    }
    true
}

#[cfg(unix)]
pub(super) fn configure_utf8_input(fd: std::os::fd::RawFd) -> Result<()> {
    // IUTF8 controls canonical erase of multibyte input. Shells may subsequently
    // select raw mode; leave their echo and line-discipline choices intact.
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    // SAFETY: fd belongs to the newly opened PTY, remains owned by its master,
    // and the initialized attributes are passed back to that same descriptor.
    unsafe {
        let mut attributes = std::mem::MaybeUninit::<libc::termios>::uninit();
        if libc::tcgetattr(fd, attributes.as_mut_ptr()) != 0 {
            bail!("host.operation_failed");
        }
        let mut attributes = attributes.assume_init();
        attributes.c_iflag |= libc::IUTF8;
        if libc::tcsetattr(fd, libc::TCSANOW, &attributes) != 0 {
            bail!("host.operation_failed");
        }
    }
    Ok(())
}

#[cfg(windows)]
pub(super) fn configure_environment(command: &mut CommandBuilder) -> Result<()> {
    // CommandBuilder reads the Windows environment case-insensitively (and
    // merges the account's registry environment). Retain only shell/runtime
    // essentials, never Gateway execution credentials or provider secrets.
    let environment: Vec<_> = [
        "SystemRoot",
        "WINDIR",
        "ComSpec",
        "PATH",
        "PATHEXT",
        "USERPROFILE",
        "HOMEDRIVE",
        "HOMEPATH",
        "USERNAME",
        "USERDOMAIN",
        "APPDATA",
        "LOCALAPPDATA",
        "ProgramData",
        "ALLUSERSPROFILE",
        "ProgramFiles",
        "ProgramFiles(x86)",
        "ProgramW6432",
        "TEMP",
        "TMP",
        "PSModulePath",
        "PROCESSOR_ARCHITECTURE",
        "PROCESSOR_IDENTIFIER",
        "NUMBER_OF_PROCESSORS",
    ]
    .into_iter()
    .filter_map(|name| command.get_env(name).map(|value| (name, value.to_owned())))
    .collect();
    command.env_clear();
    for (name, value) in environment {
        command.env(name, value);
    }
    // Resolve a missing/invalid ComSpec using the OS installation, rather
    // than searching the Gateway working directory for a cmd.exe.
    if !command.get_env("ComSpec").is_some_and(|value| {
        let path = std::path::Path::new(value);
        path.is_absolute() && path.is_file()
    }) {
        let root = command
            .get_env("SystemRoot")
            .ok_or_else(|| anyhow::anyhow!("host.operation_failed"))?;
        let shell = std::path::Path::new(root).join("System32").join("cmd.exe");
        if !shell.is_file() {
            anyhow::bail!("host.operation_failed");
        }
        command.env("ComSpec", shell);
    }
    let profile = match command.get_env("USERPROFILE") {
        Some(value)
            if std::path::Path::new(value).is_absolute()
                && std::path::Path::new(value).is_dir() =>
        {
            std::path::PathBuf::from(value)
        }
        _ => windows_profile_directory()?,
    };
    command.env("USERPROFILE", &profile);
    if command.get_cwd().is_none() {
        command.cwd(profile);
    }
    command.env("TERM", "xterm-256color");
    // ConPTY's pipe protocol is always UTF-8; POSIX locales and termios flags
    // do not configure it. Console applications retain Windows codepage rules.
    Ok(())
}

#[cfg(windows)]
fn windows_profile_directory() -> Result<std::path::PathBuf> {
    use std::os::windows::{
        ffi::OsStringExt,
        io::{AsRawHandle, FromRawHandle, OwnedHandle},
    };
    use windows_sys::Win32::{
        Security::TOKEN_QUERY,
        System::Threading::{GetCurrentProcess, OpenProcessToken},
        UI::Shell::GetUserProfileDirectoryW,
    };
    let mut token = std::ptr::null_mut();
    // SAFETY: writable out handle; the successful result is immediately owned.
    if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
        anyhow::bail!("host.operation_failed");
    }
    let token = unsafe { OwnedHandle::from_raw_handle(token) };
    let mut length = 0;
    // Query the required UTF-16 length, including its terminating NUL.
    unsafe {
        GetUserProfileDirectoryW(token.as_raw_handle(), std::ptr::null_mut(), &mut length);
    }
    if length == 0 || length > 32768 {
        anyhow::bail!("host.operation_failed");
    }
    let mut profile = vec![0; length as usize];
    // SAFETY: profile owns the queried capacity; the token is live throughout.
    if unsafe { GetUserProfileDirectoryW(token.as_raw_handle(), profile.as_mut_ptr(), &mut length) }
        == 0
    {
        anyhow::bail!("host.operation_failed");
    }
    let end = profile
        .iter()
        .position(|unit| *unit == 0)
        .unwrap_or(profile.len());
    let directory = std::path::PathBuf::from(std::ffi::OsString::from_wide(&profile[..end]));
    if !directory.is_absolute() || !directory.is_dir() {
        anyhow::bail!("host.operation_failed");
    }
    Ok(directory)
}
