use std::{
    collections::BTreeMap,
    ffi::OsStr,
    io,
    mem::{size_of, size_of_val},
    os::windows::{
        ffi::OsStrExt,
        io::{AsRawHandle, FromRawHandle, OwnedHandle},
        process::ExitStatusExt,
    },
    process::{Command, ExitStatus},
    ptr::{null, null_mut},
};

use tokio::task::JoinHandle;
use windows_sys::Win32::{
    Foundation::{HANDLE, WAIT_OBJECT_0, WAIT_TIMEOUT},
    Security::{
        CheckTokenMembership, CreateRestrictedToken, CreateWellKnownSid, DISABLE_MAX_PRIVILEGE,
        SECURITY_MAX_SID_SIZE, SID_AND_ATTRIBUTES, TOKEN_ALL_ACCESS, WELL_KNOWN_SID_TYPE,
        WinBuiltinAdministratorsSid, WinBuiltinPowerUsersSid,
    },
    System::{
        JobObjects::{
            AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
            JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
            SetInformationJobObject,
        },
        Threading::{
            CREATE_NO_WINDOW, CREATE_SUSPENDED, CREATE_UNICODE_ENVIRONMENT, CreateProcessAsUserW,
            GetCurrentProcess, GetExitCodeProcess, INFINITE, OpenProcessToken, PROCESS_INFORMATION,
            ResumeThread, STARTUPINFOW, TerminateProcess, WaitForSingleObject,
        },
    },
};

type SidBuffer = [u32; (SECURITY_MAX_SID_SIZE as usize).div_ceil(size_of::<u32>())];

fn group_sid(kind: WELL_KNOWN_SID_TYPE) -> io::Result<SidBuffer> {
    let mut sid = SidBuffer::default();
    let mut size = size_of_val(&sid) as u32;
    check(unsafe { CreateWellKnownSid(kind, null_mut(), sid.as_mut_ptr().cast(), &mut size) })?;
    Ok(sid)
}

pub(super) fn has_administrator_group() -> io::Result<bool> {
    for kind in [WinBuiltinAdministratorsSid, WinBuiltinPowerUsersSid] {
        let mut sid = group_sid(kind)?;
        let mut member = 0;
        check(unsafe { CheckTokenMembership(null_mut(), sid.as_mut_ptr().cast(), &mut member) })?;
        if member != 0 {
            return Ok(true);
        }
    }
    Ok(false)
}

fn restricted_token() -> io::Result<OwnedHandle> {
    let mut token = null_mut();
    check(unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_ALL_ACCESS, &mut token) })?;
    let token = unsafe { OwnedHandle::from_raw_handle(token) };
    let mut administrators = group_sid(WinBuiltinAdministratorsSid)?;
    let mut power_users = group_sid(WinBuiltinPowerUsersSid)?;
    let disabled = [
        SID_AND_ATTRIBUTES {
            Sid: administrators.as_mut_ptr().cast(),
            Attributes: 0,
        },
        SID_AND_ATTRIBUTES {
            Sid: power_users.as_mut_ptr().cast(),
            Attributes: 0,
        },
    ];
    let mut restricted = null_mut();
    check(unsafe {
        CreateRestrictedToken(
            token.as_raw_handle(),
            DISABLE_MAX_PRIVILEGE,
            disabled.len() as u32,
            disabled.as_ptr(),
            0,
            null(),
            0,
            null(),
            &mut restricted,
        )
    })?;
    Ok(unsafe { OwnedHandle::from_raw_handle(restricted) })
}

pub(super) struct RestrictedChild {
    process: OwnedHandle,
    _job: OwnedHandle,
    wait: Option<JoinHandle<io::Result<ExitStatus>>>,
}

impl RestrictedChild {
    /// The postmaster rejects administrator tokens. Launch under the same
    /// user's restricted token, keeping Gateway as the direct parent.
    pub(super) fn spawn(command: &Command) -> io::Result<Self> {
        let token = restricted_token()?;
        let job = unsafe { CreateJobObjectW(null(), null()) };
        if job.is_null() {
            return Err(io::Error::last_os_error());
        }
        let job = unsafe { OwnedHandle::from_raw_handle(job) };
        let mut limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
        limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        check(unsafe {
            SetInformationJobObject(
                job.as_raw_handle(),
                JobObjectExtendedLimitInformation,
                (&raw const limits).cast(),
                size_of_val(&limits) as u32,
            )
        })?;
        let executable = wide(command.get_program())?;
        let mut arguments = Vec::new();
        for arg in std::iter::once(command.get_program()).chain(command.get_args()) {
            if !arguments.is_empty() {
                arguments.push(b' ' as u16);
            }
            quote_arg(&mut arguments, arg)?;
        }
        arguments.push(0);
        let mut environment = environment(command)?;
        let startup = STARTUPINFOW {
            cb: size_of::<STARTUPINFOW>() as u32,
            ..Default::default()
        };
        let mut child = PROCESS_INFORMATION::default();
        check(unsafe {
            CreateProcessAsUserW(
                token.as_raw_handle(),
                executable.as_ptr(),
                arguments.as_mut_ptr(),
                null(),
                null(),
                0,
                CREATE_SUSPENDED | CREATE_NO_WINDOW | CREATE_UNICODE_ENVIRONMENT,
                environment.as_mut_ptr().cast(),
                null(),
                &startup,
                &mut child,
            )
        })?;
        let process = unsafe { OwnedHandle::from_raw_handle(child.hProcess) };
        let thread = unsafe { OwnedHandle::from_raw_handle(child.hThread) };
        // Assign the suspended process before it can start any descendants.
        if let Err(error) =
            check(unsafe { AssignProcessToJobObject(job.as_raw_handle(), process.as_raw_handle()) })
        {
            unsafe { TerminateProcess(process.as_raw_handle(), 1) };
            return Err(error);
        }
        if unsafe { ResumeThread(thread.as_raw_handle()) } == u32::MAX {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            process,
            _job: job,
            wait: None,
        })
    }

    pub(super) fn try_wait(&self) -> io::Result<Option<ExitStatus>> {
        wait_for_exit(self.process.as_raw_handle(), 0)
    }

    pub(super) async fn wait(&mut self) -> io::Result<ExitStatus> {
        if let Some(status) = self.try_wait()? {
            return Ok(status);
        }
        if self.wait.is_none() {
            let process = self.process.try_clone()?;
            self.wait = Some(tokio::task::spawn_blocking(move || {
                wait_for_exit(process.as_raw_handle(), INFINITE)?
                    .ok_or_else(|| io::Error::other("unexpected PostgreSQL wait timeout"))
            }));
        }
        self.wait
            .as_mut()
            .expect("PostgreSQL process wait installed")
            .await?
    }
}

fn wait_for_exit(process: HANDLE, timeout: u32) -> io::Result<Option<ExitStatus>> {
    match unsafe { WaitForSingleObject(process, timeout) } {
        WAIT_OBJECT_0 => {
            let mut status = 0;
            check(unsafe { GetExitCodeProcess(process, &mut status) })?;
            Ok(Some(ExitStatus::from_raw(status)))
        }
        WAIT_TIMEOUT => Ok(None),
        _ => Err(io::Error::last_os_error()),
    }
}

fn check(success: i32) -> io::Result<()> {
    if success == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

fn wide(value: &OsStr) -> io::Result<Vec<u16>> {
    let mut value: Vec<_> = value.encode_wide().collect();
    if value.contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "embedded NUL in PostgreSQL command",
        ));
    }
    value.push(0);
    Ok(value)
}

fn quote_arg(line: &mut Vec<u16>, arg: &OsStr) -> io::Result<()> {
    const QUOTE: u16 = b'"' as u16;
    const SLASH: u16 = b'\\' as u16;
    line.push(QUOTE);
    let mut slashes = 0;
    for unit in wide(arg)?.into_iter().take_while(|unit| *unit != 0) {
        if unit == SLASH {
            slashes += 1;
            continue;
        }
        if unit == QUOTE {
            slashes = 2 * slashes + 1;
        }
        line.extend(std::iter::repeat_n(SLASH, slashes));
        slashes = 0;
        line.push(unit);
    }
    line.extend(std::iter::repeat_n(SLASH, 2 * slashes));
    line.push(QUOTE);
    Ok(())
}

fn environment(command: &Command) -> io::Result<Vec<u16>> {
    let mut values: BTreeMap<_, _> = std::env::vars_os()
        .map(|(key, value)| (key.to_string_lossy().to_uppercase(), (key, value)))
        .collect();
    for (key, value) in command.get_envs() {
        let folded = key.to_string_lossy().to_uppercase();
        if let Some(value) = value {
            values.insert(folded, (key.to_owned(), value.to_owned()));
        } else {
            values.remove(&folded);
        }
    }
    let mut block = Vec::new();
    for (key, value) in values.into_values() {
        let mut key = wide(&key)?;
        *key.last_mut().expect("NUL-terminated environment key") = b'=' as u16;
        block.extend(key);
        block.extend(wide(&value)?);
    }
    block.push(0);
    Ok(block)
}
