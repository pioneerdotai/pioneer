//! Execution admission and capability dispatch. App credentials never enter user RPC.
mod pty;
pub(crate) mod render;
#[cfg(any(unix, windows))]
mod shell;
mod stream;
#[cfg(windows)]
mod windows;
use crate::transport::GatewayHttpState;
use anyhow::bail;
use axum::{
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use futures_util::SinkExt;
use pioneer_protocol::{
    JsonRpcErrorResponse, JsonRpcNotification, JsonRpcRequest as Request, JsonRpcResponse,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio_util::sync::CancellationToken;

pub(crate) struct ExecutionAuthority {
    pub(crate) app_id: String,
    pub(crate) workspace_id: String,
    pub(crate) execution_id: String,
    pub(crate) cancel: CancellationToken,
    ptys: Mutex<pty::PtyOwner>,
    output: Arc<tokio::sync::Notify>,
    changes: tokio::sync::watch::Sender<()>,
    activity: tokio::sync::Notify,
    connections: Arc<tokio::sync::Semaphore>,
}
impl ExecutionAuthority {
    fn operate(&self, method: &str, params: Value) -> anyhow::Result<Value> {
        let mut owner = self.ptys.lock().unwrap_or_else(|e| e.into_inner());
        // Final admission shares revocation's resource lock. Dropping an async
        // waiter cannot cancel a blocking operation already queued on this lock.
        if self.cancel.is_cancelled() {
            bail!("host.revoked");
        }
        owner.call(method, params)
    }
}
#[derive(Default)]
pub(crate) struct HostApi {
    executions: Mutex<HashMap<String, Arc<ExecutionAuthority>>>,
}
impl HostApi {
    pub(crate) fn issue(
        &self,
        app_id: String,
        workspace_id: String,
        execution_id: String,
        cancel: CancellationToken,
    ) -> (String, Arc<ExecutionAuthority>) {
        let credential = hex::encode(rand::random::<[u8; 32]>());
        let authority = self.register(&credential, app_id, workspace_id, execution_id, cancel);
        (credential, authority)
    }

    pub(crate) fn register(
        &self,
        credential: &str,
        app_id: String,
        workspace_id: String,
        execution_id: String,
        cancel: CancellationToken,
    ) -> Arc<ExecutionAuthority> {
        let digest = hex::encode(Sha256::digest(credential.as_bytes()));
        let mut executions = self.executions.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(scope) = executions
            .get(&digest)
            .filter(|scope| !scope.cancel.is_cancelled())
        {
            return scope.clone();
        }
        let output = Arc::new(tokio::sync::Notify::new());
        let (changes, _) = tokio::sync::watch::channel(());
        let authority = Arc::new(ExecutionAuthority {
            app_id,
            workspace_id,
            execution_id,
            cancel,
            ptys: Mutex::new(pty::PtyOwner::with_output(output.clone())),
            output,
            changes,
            activity: tokio::sync::Notify::new(),
            connections: Arc::new(tokio::sync::Semaphore::new(2)),
        });
        if let Some(previous) = executions.insert(digest, authority.clone()) {
            previous
                .ptys
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clear();
        }
        drop(executions);
        let expiry = authority.clone();
        tokio::spawn(async move {
            loop {
                let delay = expiry
                    .ptys
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .until_expiry();
                let deadline = async {
                    match delay {
                        Some(delay) => tokio::time::sleep(delay).await,
                        None => std::future::pending::<()>().await,
                    }
                };
                tokio::select! { _ = expiry.cancel.cancelled() => break, _ = expiry.activity.notified() => {}, _ = expiry.output.notified() => { expiry.changes.send_replace(()); }, _ = deadline => {} }
                expiry
                    .ptys
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .expire();
            }
        });
        authority
    }
    /// Fence admission immediately; the existing runtime stop/lease owner
    /// performs resource cleanup. No VM wait or database read is needed here.
    pub(crate) fn fence_app(&self, app: &str) {
        for scope in self
            .executions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
        {
            if scope.app_id == app {
                scope.cancel.cancel();
            }
        }
    }

    pub(crate) fn revoke_execution(&self, execution: &str) {
        self.executions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|_, scope| {
                if scope.execution_id == execution {
                    scope.cancel.cancel();
                    scope.ptys.lock().unwrap_or_else(|e| e.into_inner()).clear();
                    false
                } else {
                    true
                }
            });
    }
    pub(crate) fn revoke_workspace(&self, workspace: &str) -> Vec<String> {
        let mut apps = Vec::new();
        self.executions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|_, scope| {
                if scope.workspace_id == workspace {
                    apps.push(scope.app_id.clone());
                    scope.cancel.cancel();
                    scope.ptys.lock().unwrap_or_else(|e| e.into_inner()).clear();
                    false
                } else {
                    true
                }
            });
        apps
    }
    pub(crate) fn admit(&self, credential: &str) -> Option<Arc<ExecutionAuthority>> {
        if credential.len() != 64 {
            return None;
        }
        self.executions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&hex::encode(Sha256::digest(credential.as_bytes())))
            .filter(|scope| !scope.cancel.is_cancelled())
            .cloned()
    }
}
pub(crate) async fn websocket(
    State(state): State<GatewayHttpState>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    if !state.is_ready() {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    // Browser code cannot use an execution credential, even if an App leaks one.
    if headers.contains_key("origin") {
        return StatusCode::FORBIDDEN.into_response();
    }
    let credential = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    let Some(scope) = credential.and_then(|v| state.message_processor.host_api().admit(v)) else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    if state
        .message_processor
        .validate_host_execution(&scope)
        .await
        .is_err()
    {
        return StatusCode::FORBIDDEN.into_response();
    }
    let Ok(permit) = scope.connections.clone().try_acquire_owned() else {
        return StatusCode::TOO_MANY_REQUESTS.into_response();
    };
    if scope.cancel.is_cancelled() {
        return StatusCode::FORBIDDEN.into_response();
    }
    ws.max_message_size(128 * 1024)
        .max_frame_size(128 * 1024)
        .on_upgrade(move |socket| async move {
            let _permit = permit;
            connection(socket, scope).await;
        })
        .into_response()
}
async fn connection(mut socket: WebSocket, scope: Arc<ExecutionAuthority>) {
    let mut output = scope.changes.subscribe();
    let mut streams = stream::Streams::default();
    let mut output_pending = false;
    loop {
        let message = tokio::select! {
            _ = scope.cancel.cancelled() => break,
            _ = output.changed() => {
                output_pending = true;
                // Existing installed backends can still use read/changed until
                // repackaged. Streaming connections do not receive this extra
                // notification or need a read round trip.
                if !streams.is_streaming() {
                    let heads = scope.ptys.lock().unwrap_or_else(|e| e.into_inner()).output_heads();
                    let Ok(event) = JsonRpcNotification::from_params("host.pty.changed", &json!({"sessions":heads})).and_then(|event| serde_json::to_string(&event)) else { break; };
                    let sent = tokio::select! {
                        _ = scope.cancel.cancelled() => false,
                        sent = tokio::time::timeout(Duration::from_secs(10), socket.send(Message::Text(event.into()))) => matches!(sent, Ok(Ok(()))),
                    };
                    if !sent { break; }
                }
                continue;
            },
            _ = std::future::ready(()), if output_pending => {
                // At most one bounded frame per turn. Socket input competes
                // fairly with a busy producer; no timer gates either direction.
                let Some(event) = streams.next_event(&scope) else {
                    output_pending = false;
                    continue;
                };
                let sent = tokio::select! {
                    _ = scope.cancel.cancelled() => false,
                    sent = tokio::time::timeout(Duration::from_secs(10), socket.send(Message::Text(event.into()))) => matches!(sent, Ok(Ok(()))),
                };
                if !sent { break; }
                continue;
            },
            message = socket.recv() => message,
        };
        let Some(Ok(message)) = message else {
            break;
        };
        let Message::Text(text) = message else {
            if matches!(message, Message::Close(_)) {
                break;
            }
            continue;
        };
        let request: Request = match serde_json::from_str(&text) {
            Ok(v) => v,
            Err(_) => break,
        };
        if request.jsonrpc != "2.0" || request.method.len() > 128 {
            break;
        }
        let operation = streams.dispatch(
            &scope,
            &request.method,
            request.params.unwrap_or(Value::Null),
        );
        let result =
            tokio::select! { _ = scope.cancel.cancelled() => break, result = operation => result };
        output_pending = true;
        scope.activity.notify_one();
        let response = match result {
            Ok(value) => JsonRpcResponse::from_result(request.id, &value)
                .and_then(|response| serde_json::to_string(&response)),
            Err(error) => {
                let text = error.to_string();
                let code = match text.as_str() {
                    "host.revoked"
                    | "host.method_not_allowed"
                    | "host.invalid_params"
                    | "host.invalid_dimensions"
                    | "host.capacity"
                    | "host.idempotency_conflict"
                    | "host.session_expired"
                    | "host.session_unavailable"
                    | "host.input_limit"
                    | "host.input_sequence_conflict"
                    | "host.input_outcome_unknown"
                    | "host.input_backpressure"
                    | "host.output_cursor_expired"
                    | "host.invalid_cursor"
                    | "host.pty_unsupported_platform"
                    | "host.utf8_locale_unavailable" => text.as_str(),
                    _ => "host.operation_failed",
                };
                serde_json::to_string(&JsonRpcErrorResponse::new(Some(request.id), -32001, code))
            }
        };
        let Ok(response) = response else {
            break;
        };
        let sent = tokio::select! { _ = scope.cancel.cancelled() => false, sent = tokio::time::timeout(Duration::from_secs(10), socket.send(Message::Text(response.into()))) => matches!(sent, Ok(Ok(()))) };
        if !sent {
            break;
        }
    }
    let _ = tokio::time::timeout(Duration::from_secs(2), socket.close()).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn execution_credentials_are_registry_scoped_and_revocation_closes_live_authority() {
        let api = HostApi::default();
        let other = HostApi::default();
        let (first, authority) = api.issue(
            "app-a".into(),
            "ws".into(),
            "execution-a".into(),
            CancellationToken::new(),
        );
        let (second, unaffected) = api.issue(
            "app-b".into(),
            "ws".into(),
            "execution-b".into(),
            CancellationToken::new(),
        );
        assert!(api.admit(&first).is_some());
        assert!(other.admit(&first).is_none());
        api.revoke_execution("execution-a");
        assert!(authority.cancel.is_cancelled());
        assert!(api.admit(&first).is_none());
        assert!(!unaffected.cancel.is_cancelled());
        assert!(api.admit(&second).is_some());
        assert_eq!(api.revoke_workspace("ws"), vec!["app-b"]);
        assert!(unaffected.cancel.is_cancelled());
        assert!(api.admit(&second).is_none());
    }
}

#[cfg(all(test, unix))]
mod revoke_admission_regression {
    use super::*;
    #[test]
    fn queued_create_cannot_spawn_after_revoke_and_admitted_child_is_reaped() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let _enter = runtime.enter();
        let api = Arc::new(HostApi::default());
        let (_, authority) = api.issue(
            "app".into(),
            "ws".into(),
            "execution".into(),
            CancellationToken::new(),
        );
        let directory = tempfile::tempdir().unwrap();
        let admitted = directory.path().join("admitted");
        let forbidden = directory.path().join("forbidden");
        let create = |key: &str, path: &std::path::Path| json!({"key":key,"command":["/bin/sh","-c", "printf '%s' \"$$\" > \"$1\"; exec sleep 60", "sh",path.to_str().unwrap()],"dimensions":{"rows":24,"cols":80}});
        authority
            .operate("host.pty.create", create("admitted", &admitted))
            .unwrap();
        let mut pid = None;
        for _ in 0..100 {
            pid = std::fs::read_to_string(&admitted)
                .ok()
                .and_then(|value| value.parse::<libc::pid_t>().ok());
            if pid.is_some() {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let pid = pid.expect("ordinary admitted operation must execute");
        let hold = authority.ptys.lock().unwrap();
        let queued = authority.clone();
        let params = create("forbidden", &forbidden);
        let (ready, wait) = std::sync::mpsc::sync_channel(0);
        let operation = std::thread::spawn(move || {
            ready.send(()).unwrap();
            queued.operate("host.pty.create", params)
        });
        wait.recv().unwrap();
        let revoke_api = api.clone();
        let revoke = std::thread::spawn(move || revoke_api.revoke_execution("execution"));
        // Cancellation precedes the locked clear, so the queued operation must
        // lose admission regardless of which waiter obtains the lock first.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !authority.cancel.is_cancelled() {
            assert!(std::time::Instant::now() < deadline);
            std::thread::yield_now();
        }
        drop(hold);
        assert!(operation.join().unwrap().is_err());
        revoke.join().unwrap();
        assert!(
            !forbidden.exists(),
            "revoked queued create must never run its command"
        );
        assert_eq!(authority.ptys.lock().unwrap().output_heads(), json!([]));
        assert_eq!(
            unsafe { libc::kill(pid, 0) },
            -1,
            "admitted child must be reaped before revoke returns"
        );
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::ESRCH)
        );
    }
}
