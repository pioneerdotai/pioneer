//! Native Windows adapter regressions. These require ConPTY and Windows
//! PowerShell; they do not start a Gateway server, VM, browser, or desktop.
use super::*;
use crate::host_api::HostApi;
use std::os::windows::io::{AsRawHandle, BorrowedHandle, OwnedHandle};
use windows_sys::Win32::{Foundation::WAIT_OBJECT_0, System::Threading::WaitForSingleObject};

fn bounded(body: impl FnOnce() + Send + 'static) {
    let (send, receive) = std::sync::mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(body));
        let _ = send.send(result);
    });
    match receive
        .recv_timeout(Duration::from_secs(20))
        .expect("ConPTY startup/teardown deadlocked")
    {
        Ok(()) => {}
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

fn powershell(script: &str) -> Value {
    let executable = std::path::PathBuf::from(std::env::var_os("SystemRoot").unwrap())
        .join("System32/WindowsPowerShell/v1.0/powershell.exe");
    json!([executable, "-NoLogo", "-NoProfile", "-Command", script])
}

fn input(owner: &mut PtyOwner, id: &str, sequence: u64, text: &str) -> Value {
    owner
        .call(
            "host.pty.input",
            json!({"session_id":id,"sequence":sequence,"bytes":text.as_bytes()}),
        )
        .unwrap()
}

fn collect_until(
    owner: &mut PtyOwner,
    id: &str,
    cursor: &mut u64,
    until: impl Fn(&Value, &[u8]) -> bool,
) -> (Value, Vec<u8>) {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut bytes = Vec::new();
    loop {
        let frame = owner
            .call("host.pty.read", json!({"session_id":id,"after":*cursor}))
            .unwrap();
        assert_eq!(frame["failed"], false, "native PTY I/O failed: {frame}");
        *cursor = frame["next"].as_u64().unwrap();
        bytes.extend(
            frame["bytes"]
                .as_array()
                .unwrap()
                .iter()
                .map(|byte| byte.as_u64().unwrap() as u8),
        );
        if until(&frame, &bytes) {
            return (frame, bytes);
        }
        assert!(
            Instant::now() < deadline,
            "ConPTY did not produce the required output/EOF"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn process_handle(owner: &PtyOwner, id: &str) -> OwnedHandle {
    let process = owner.sessions[id].process.lock().unwrap();
    let handle = process.child.as_ref().unwrap().as_raw_handle().unwrap();
    // SAFETY: Process owns this handle throughout duplication under the lock.
    unsafe { BorrowedHandle::borrow_raw(handle) }
        .try_clone_to_owned()
        .unwrap()
}

#[test]
fn windows_default_program_starts_after_failed_spawn_and_close_is_idempotent() {
    bounded(|| {
        let mut owner = PtyOwner::default();
        let directory = tempfile::tempdir().unwrap();
        for attempt in 0..10 {
            assert!(
                owner
                    .call(
                        "host.pty.create",
                        json!({
                            "key":format!("failed-{attempt}"),
                            "command":[directory.path().join("missing.exe")],
                            "dimensions":{"rows":24,"cols":80}
                        })
                    )
                    .is_err()
            );
        }
        let created = owner
            .call(
                "host.pty.create",
                json!({
                    "key":"default", "dimensions":{"rows":24,"cols":80}
                }),
            )
            .expect("Windows must support the omitted-command host default");
        let id = created["session_id"].as_str().unwrap();
        let handle = process_handle(&owner, id);
        owner
            .call("host.pty.close", json!({"session_id":id}))
            .unwrap();
        // SAFETY: handle is an owned process handle retained across teardown.
        assert_eq!(
            unsafe { WaitForSingleObject(handle.as_raw_handle(), 5000) },
            WAIT_OBJECT_0
        );
        owner
            .call("host.pty.close", json!({"session_id":id}))
            .unwrap();
        assert!(
            owner
                .call("host.pty.keepalive", json!({"session_id":id}))
                .is_err()
        );
    });
}

#[test]
fn windows_cyrillic_input_replay_resize_and_final_exit_work_without_a_renderer() {
    bounded(|| {
        let mut owner = PtyOwner::default();
        let request = json!({
            "key":"unicode",
            "command":powershell("[Console]::WriteLine('READY'); $a=[Console]::ReadLine(); $b=[Console]::ReadLine(); [Console]::WriteLine(('RESULT:{0}:{1}' -f $a,$b)); [Console]::WriteLine(('SIZE:{0}:{1}' -f [Console]::WindowWidth,[Console]::WindowHeight)); exit 259"),
            "dimensions":{"rows":24,"cols":80}
        });
        let created = owner.call("host.pty.create", request.clone()).unwrap();
        let id = created["session_id"].as_str().unwrap();
        assert_eq!(
            owner.call("host.pty.create", request).unwrap()["session_id"],
            id
        );
        assert!(
            PtyOwner::default()
                .call("host.pty.read", json!({"session_id":id,"after":0}))
                .is_err()
        );
        let mut cursor = 0;
        collect_until(&mut owner, id, &mut cursor, |_, bytes| {
            String::from_utf8_lossy(bytes).contains("READY")
        });
        owner.call("host.pty.resize", json!({"session_id":id,"dimensions":{"rows":40,"cols":120,"pixel_width":960,"pixel_height":800}})).unwrap();
        assert_eq!(
            input(&mut owner, id, 1, "Приветя\u{7f}\r")["accepted_sequence"],
            1
        );
        assert_eq!(input(&mut owner, id, 2, "второй\r")["accepted_sequence"], 2);
        // Lost earlier ACK after later acceptance must not write line 1 again.
        assert_eq!(
            input(&mut owner, id, 1, "Приветя\u{7f}\r")["accepted_sequence"],
            1
        );
        let (frame, bytes) = collect_until(&mut owner, id, &mut cursor, |frame, _| {
            !frame["exit_status"].is_null()
        });
        let text = String::from_utf8(bytes).expect("ConPTY output must be UTF-8");
        assert!(
            text.contains("RESULT:Привет:второй"),
            "corrupt or duplicated input: {text:?}"
        );
        assert!(
            text.contains("SIZE:120:40"),
            "resize did not reach the console: {text:?}"
        );
        assert_eq!(
            frame["exit_status"], 259,
            "STILL_ACTIVE value must also work as a completed exit code"
        );
        owner
            .call("host.pty.close", json!({"session_id":id}))
            .unwrap();
    });
}

#[test]
fn windows_revocation_stops_a_live_console_with_output_and_without_a_view() {
    bounded(|| {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let _entered = runtime.enter();
        let api = HostApi::default();
        let (credential, authority) = api.issue(
            "app".into(),
            "workspace".into(),
            "execution".into(),
            tokio_util::sync::CancellationToken::new(),
        );
        let created = authority.operate("host.pty.create", json!({
            "key":"live-output",
            "command":powershell("[Console]::WriteLine('RUNNING'); while ($true) { [Console]::WriteLine(('x'*80)); Start-Sleep -Milliseconds 1 }"),
            "dimensions":{"rows":24,"cols":80}
        })).unwrap();
        let id = created["session_id"].as_str().unwrap();
        let handle = {
            let mut owner = authority.ptys.lock().unwrap();
            collect_until(&mut owner, id, &mut 0, |_, bytes| {
                String::from_utf8_lossy(bytes).contains("RUNNING")
            });
            process_handle(&owner, id)
        };
        api.revoke_execution("execution");
        assert!(authority.cancel.is_cancelled());
        assert!(api.admit(&credential).is_none());
        // SAFETY: owned process handle, independent of the cleared PTY owner.
        assert_eq!(
            unsafe { WaitForSingleObject(handle.as_raw_handle(), 5000) },
            WAIT_OBJECT_0,
            "revoke returned while the native shell survived"
        );
        assert!(
            authority
                .operate("host.pty.keepalive", json!({"session_id":id}))
                .is_err()
        );
    });
}

#[test]
fn windows_cursor_inheritance_is_answered_once_across_pipe_read_boundaries() {
    use super::super::windows::CursorHandshake;
    let bytes = b"\x1b[?25l\x1b[6nREADY\x1b[6n";
    // Preserve surrounding VT output and the application's subsequent DSR.
    // A response must not depend on how the pipe split its reads.
    for split in 0..=bytes.len() {
        let (send, receive) = std::sync::mpsc::sync_channel(2);
        let mut cursor = CursorHandshake::new(send);
        let mut output = cursor.filter(&bytes[..split]).unwrap();
        output.extend(cursor.filter(&bytes[split..]).unwrap());
        output.extend(cursor.finish());
        assert_eq!(output, b"\x1b[?25lREADY\x1b[6n", "read boundary {split}");
        assert!(matches!(receive.try_recv().unwrap(), PtyInput::CursorReply));
        assert!(matches!(
            receive.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));
    }
}

#[test]
fn windows_incomplete_control_prefix_is_not_lost_at_eof() {
    use super::super::windows::CursorHandshake;
    let (send, receive) = std::sync::mpsc::sync_channel(1);
    let mut cursor = CursorHandshake::new(send);
    assert_eq!(cursor.filter(b"tail\x1b[6").unwrap(), b"tail");
    assert_eq!(cursor.finish(), b"\x1b[6");
    assert!(matches!(
        receive.try_recv(),
        Err(std::sync::mpsc::TryRecvError::Empty)
    ));
}
