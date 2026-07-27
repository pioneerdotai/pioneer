use super::coordinator::{CliMcpActivationGeneration, CliMcpProjectionGeneration};
use super::facade::{
    CliMcpFacadeConfigurationError, CliMcpFacadeProjection, CliMcpFacadeRequestContext,
    CliMcpProgressNotification, CliMcpProgressSendError, CliMcpProgressSink, CliMcpToolFacade,
};
use super::limits::CliMcpFacadeLimits;
use super::supervisor::{CliMcpBridgeSupervisorError, CliMcpBridgeTransport};
use crate::turn_mcp::invoker::TurnMcpInvoker;
use async_trait::async_trait;
use pioneer_cli_mcp_bridge::{BridgeFrame, BridgeFrameType};
use serde_json::{Value as JsonValue, json};
use std::fmt;
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc};
use tokio::task::JoinSet;
use tokio::time::timeout;

const OUTBOUND_CONTROL_RESERVE: usize = 8;

pub(crate) struct CliMcpBridgeFacadeHandle {
    context: Arc<CliMcpBridgeFacadeContext>,
}

impl CliMcpBridgeFacadeHandle {
    pub(crate) async fn set_activation(&self, activation: Option<CliMcpActivationGeneration>) {
        *self.context.activation.write().await = activation;
    }
}

struct CliMcpBridgeFacadeContext {
    bound_grant: super::grants::CliMcpBoundGrant,
    projection_generation: CliMcpProjectionGeneration,
    activation: RwLock<Option<CliMcpActivationGeneration>>,
}

impl CliMcpBridgeFacadeContext {
    async fn snapshot(&self) -> CliMcpFacadeRequestContext {
        CliMcpFacadeRequestContext {
            bound_grant: self.bound_grant.clone(),
            projection_generation: self.projection_generation,
            activation_generation: *self.activation.read().await,
        }
    }
}

struct CliMcpBridgeProgressSink {
    outbound: mpsc::Sender<Vec<u8>>,
}

#[async_trait]
impl CliMcpProgressSink for CliMcpBridgeProgressSink {
    async fn send_progress(
        &self,
        notification: CliMcpProgressNotification,
    ) -> Result<(), CliMcpProgressSendError> {
        let mut encoded = serde_json::to_vec(&json!({
            "jsonrpc": "2.0",
            "method": "notifications/progress",
            "params": {
                "progressToken": notification.progress_token,
                "progress": notification.progress,
                "total": notification.total,
                "message": notification.message,
            }
        }))
        .map_err(|_| CliMcpProgressSendError)?;
        encoded.push(b'\n');
        self.outbound
            .send(encoded)
            .await
            .map_err(|_| CliMcpProgressSendError)
    }
}

pub(crate) struct CliMcpBridgeFacadeServer {
    transport: CliMcpBridgeTransport,
    facade: Arc<CliMcpToolFacade>,
    context: Arc<CliMcpBridgeFacadeContext>,
    outbound: mpsc::Receiver<Vec<u8>>,
    outbound_sender: mpsc::Sender<Vec<u8>>,
    max_pending_bytes: usize,
    max_call_tasks: usize,
    shutdown_drain: std::time::Duration,
}

impl CliMcpBridgeFacadeServer {
    pub(crate) fn build(
        transport: CliMcpBridgeTransport,
        coordinator: Arc<super::coordinator::CliMcpCoordinator>,
        invoker: Arc<dyn TurnMcpInvoker>,
        projection_generation: CliMcpProjectionGeneration,
        projection: CliMcpFacadeProjection,
        limits: CliMcpFacadeLimits,
    ) -> Result<(CliMcpBridgeFacadeHandle, Self), CliMcpFacadeConfigurationError> {
        let channel_capacity = limits
            .max_ledger_entries
            .saturating_add(OUTBOUND_CONTROL_RESERVE);
        let (outbound_sender, outbound) = mpsc::channel(channel_capacity);
        let facade = CliMcpToolFacade::try_new(
            coordinator,
            invoker,
            projection,
            Arc::new(CliMcpBridgeProgressSink {
                outbound: outbound_sender.clone(),
            }),
            limits.clone(),
        )?;
        let context = Arc::new(CliMcpBridgeFacadeContext {
            bound_grant: transport.bound_grant().clone(),
            projection_generation,
            activation: RwLock::new(None),
        });
        let handle = CliMcpBridgeFacadeHandle {
            context: context.clone(),
        };
        let server = Self {
            transport,
            facade,
            context,
            outbound,
            outbound_sender,
            max_pending_bytes: limits.max_frame_bytes,
            max_call_tasks: limits.max_ledger_entries,
            shutdown_drain: limits.shutdown_drain_duration,
        };
        Ok((handle, server))
    }

    pub(crate) async fn run(mut self) -> Result<(), CliMcpBridgeServerError> {
        let mut pending = Vec::new();
        let mut calls = JoinSet::new();
        let outcome = self.run_loop(&mut pending, &mut calls).await;
        let facade_shutdown = self.facade.shutdown().await;
        let calls_drained = timeout(self.shutdown_drain, async {
            while calls.join_next().await.is_some() {}
        })
        .await
        .is_ok();
        if !calls_drained {
            calls.abort_all();
            while calls.join_next().await.is_some() {}
        }
        self.transport.terminate().await;
        pending.fill(0);
        if !facade_shutdown.drained || !calls_drained {
            return Err(CliMcpBridgeServerError::ShutdownTimeout);
        }
        outcome
    }

    async fn run_loop(
        &mut self,
        pending: &mut Vec<u8>,
        calls: &mut JoinSet<()>,
    ) -> Result<(), CliMcpBridgeServerError> {
        loop {
            tokio::select! {
                biased;
                outbound = self.outbound.recv() => {
                    let Some(outbound) = outbound else {
                        return Err(CliMcpBridgeServerError::OutboundClosed);
                    };
                    self.send_payload(outbound).await?;
                }
                frame = self.transport.receive_frame() => {
                    match frame? {
                        Some(frame) if frame.frame_type() == BridgeFrameType::Payload => {
                            self.accept_payload(frame.payload(), pending, calls).await?;
                        }
                        Some(frame) if frame.frame_type() == BridgeFrameType::Shutdown => {
                            return Ok(());
                        }
                        Some(frame) if frame.frame_type() == BridgeFrameType::Cancellation => {
                            return Ok(());
                        }
                        Some(_) => return Err(CliMcpBridgeServerError::UnexpectedFrame),
                        None => return Ok(()),
                    }
                }
                joined = calls.join_next(), if !calls.is_empty() => {
                    if joined.is_some_and(|result| result.is_err()) {
                        return Err(CliMcpBridgeServerError::CallTaskFailed);
                    }
                }
            }
        }
    }

    async fn accept_payload(
        &self,
        payload: &[u8],
        pending: &mut Vec<u8>,
        calls: &mut JoinSet<()>,
    ) -> Result<(), CliMcpBridgeServerError> {
        if pending
            .len()
            .checked_add(payload.len())
            .is_none_or(|length| length > self.max_pending_bytes)
        {
            return Err(CliMcpBridgeServerError::ProtocolMessageTooLarge);
        }
        pending.extend_from_slice(payload);
        while let Some(newline) = pending.iter().position(|byte| *byte == b'\n') {
            let mut line = pending.drain(..=newline).collect::<Vec<_>>();
            while line.last().is_some_and(u8::is_ascii_whitespace) {
                line.pop();
            }
            if line.is_empty() {
                continue;
            }
            if is_tool_call(&line) {
                if calls.len() >= self.max_call_tasks {
                    line.fill(0);
                    return Err(CliMcpBridgeServerError::CallPressure);
                }
                let facade = self.facade.clone();
                let context = self.context.clone();
                let outbound = self.outbound_sender.clone();
                calls.spawn(async move {
                    let snapshot = context.snapshot().await;
                    if let Some(mut response) = facade.handle_bytes(&snapshot, &line).await {
                        let _ = outbound.send(std::mem::take(&mut response)).await;
                    }
                    line.fill(0);
                });
                // Ensure the request is registered before a cancellation line
                // already coalesced into this same stdio chunk is dispatched.
                tokio::task::yield_now().await;
            } else {
                let snapshot = self.context.snapshot().await;
                if let Some(response) = self.facade.handle_bytes(&snapshot, &line).await {
                    self.outbound_sender
                        .send(response)
                        .await
                        .map_err(|_| CliMcpBridgeServerError::OutboundClosed)?;
                }
                line.fill(0);
            }
        }
        Ok(())
    }

    async fn send_payload(&mut self, mut payload: Vec<u8>) -> Result<(), CliMcpBridgeServerError> {
        if payload.len() > self.max_pending_bytes {
            payload.fill(0);
            return Err(CliMcpBridgeServerError::ProtocolMessageTooLarge);
        }
        let frame = BridgeFrame::new(BridgeFrameType::Payload, std::mem::take(&mut payload))?;
        self.transport.send_frame(&frame).await?;
        Ok(())
    }
}

fn is_tool_call(line: &[u8]) -> bool {
    serde_json::from_slice::<JsonValue>(line)
        .ok()
        .and_then(|message| message.get("method").cloned())
        .and_then(|method| method.as_str().map(str::to_owned))
        .as_deref()
        == Some("tools/call")
}

#[derive(Debug)]
pub(crate) enum CliMcpBridgeServerError {
    Supervisor(CliMcpBridgeSupervisorError),
    Frame(pioneer_cli_mcp_bridge::BridgeFrameError),
    UnexpectedFrame,
    ProtocolMessageTooLarge,
    CallPressure,
    CallTaskFailed,
    OutboundClosed,
    ShutdownTimeout,
}

impl fmt::Display for CliMcpBridgeServerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Supervisor(error) => error.fmt(formatter),
            Self::Frame(error) => error.fmt(formatter),
            Self::UnexpectedFrame => formatter.write_str("unexpected CLI MCP bridge frame"),
            Self::ProtocolMessageTooLarge => {
                formatter.write_str("CLI MCP protocol message is too large")
            }
            Self::CallPressure => formatter.write_str("CLI MCP call task capacity is exhausted"),
            Self::CallTaskFailed => formatter.write_str("CLI MCP call task failed"),
            Self::OutboundClosed => formatter.write_str("CLI MCP outbound channel closed"),
            Self::ShutdownTimeout => formatter.write_str("CLI MCP facade shutdown timed out"),
        }
    }
}

impl std::error::Error for CliMcpBridgeServerError {}

impl From<CliMcpBridgeSupervisorError> for CliMcpBridgeServerError {
    fn from(value: CliMcpBridgeSupervisorError) -> Self {
        Self::Supervisor(value)
    }
}

impl From<pioneer_cli_mcp_bridge::BridgeFrameError> for CliMcpBridgeServerError {
    fn from(value: pioneer_cli_mcp_bridge::BridgeFrameError) -> Self {
        Self::Frame(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli_runtime::manager::CLIAgentRuntimeSessionKey;
    use crate::cli_runtime::mcp::coordinator::CliMcpProjectionFingerprint;
    use crate::cli_runtime::mcp::facade::CliMcpFacadeTool;
    use crate::cli_runtime::mcp::grants::{
        CliMcpConnectionId, CliMcpGrantError, CliMcpGrantScope, CliMcpManifestHash,
    };
    use crate::cli_runtime::mcp::limits::CliMcpFacadeProjectionLimits;
    use crate::cli_runtime::session_instance::CliSessionInstanceId;
    use crate::turn_mcp::invoker::{
        TurnMcpInvocation, TurnMcpInvocationError, TurnMcpInvocationErrorCode,
    };
    use crate::turn_mcp::result::CanonicalMcpToolResult;
    use pioneer_cli_mcp_bridge::{
        AttachRequest, BridgeGeneration, helper::run_hidden_helper_with_io,
    };
    use serde_json::json;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream, duplex};
    use tokio_util::sync::CancellationToken;

    const SECRET_CANARY: &str = "pioneer-secret-canary-7c3278d3c7214b66965e";

    struct FakeSharedInvoker {
        calls: AtomicUsize,
        wait_calls: AtomicUsize,
        gateway_only_secret: String,
    }

    #[async_trait]
    impl TurnMcpInvoker for FakeSharedInvoker {
        async fn invoke(
            &self,
            invocation: TurnMcpInvocation,
            cancellation: CancellationToken,
        ) -> Result<CanonicalMcpToolResult, TurnMcpInvocationError> {
            assert_eq!(invocation.workspace_id, "workspace");
            assert_eq!(invocation.thread_id, "thread");
            assert_eq!(invocation.turn_id, "turn");
            assert_eq!(invocation.runtime_id.as_deref(), Some("codex"));
            assert_eq!(invocation.session_generation, Some(1));
            assert!(!self.gateway_only_secret.is_empty());
            self.calls.fetch_add(1, Ordering::SeqCst);
            if invocation.canonical_callable_name == "wait" {
                self.wait_calls.fetch_add(1, Ordering::SeqCst);
                cancellation.cancelled().await;
                return Err(TurnMcpInvocationError::new(
                    TurnMcpInvocationErrorCode::Cancelled,
                    "cancelled by fake provider",
                ));
            }
            Ok(CanonicalMcpToolResult {
                content: json!([
                    {"type": "text", "text": "bridge-ok"},
                    {"type": "image", "data": "aW1hZ2U=", "mimeType": "image/png"}
                ]),
                structured_content: Some(json!({"echo": invocation.arguments})),
                is_error: false,
                duration_ms: 3,
                meta: None,
            })
        }
    }

    struct ProviderClient {
        writer: DuplexStream,
        reader: BufReader<DuplexStream>,
        captured: Vec<u8>,
    }

    impl ProviderClient {
        async fn send(&mut self, value: JsonValue) {
            let mut bytes = serde_json::to_vec(&value).expect("provider request");
            bytes.push(b'\n');
            self.writer.write_all(&bytes).await.expect("provider write");
            self.writer.flush().await.expect("provider flush");
            bytes.fill(0);
        }

        async fn response(&mut self, id: &JsonValue) -> JsonValue {
            timeout(Duration::from_secs(2), async {
                loop {
                    let mut line = String::new();
                    let read = self
                        .reader
                        .read_line(&mut line)
                        .await
                        .expect("provider read");
                    assert_ne!(read, 0, "helper stdout closed before response");
                    self.captured.extend_from_slice(line.as_bytes());
                    let value: JsonValue = serde_json::from_str(&line).expect("provider json");
                    if value.get("id") == Some(id) {
                        return value;
                    }
                }
            })
            .await
            .expect("provider response")
        }
    }

    fn instance(key: (&str, &str, &str), generation: u64) -> CliSessionInstanceId {
        CliSessionInstanceId::unmanaged_for_test(
            CLIAgentRuntimeSessionKey::new(key.0, key.1, key.2).expect("key"),
            generation,
        )
        .expect("instance")
    }

    fn tool(name: &str) -> CliMcpFacadeTool {
        CliMcpFacadeTool::new(
            name,
            Some(format!("{name} fixture")),
            json!({"type": "object", "properties": {}}),
            json!({"readOnlyHint": true}),
        )
        .expect("tool")
    }

    fn projection() -> CliMcpFacadeProjection {
        CliMcpFacadeProjection::new(
            vec![tool("echo"), tool("wait")],
            CliMcpFacadeProjectionLimits::default(),
        )
        .expect("projection")
    }

    fn expiry(delta_ms: u64) -> u64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_millis();
        u64::try_from(now)
            .expect("milliseconds")
            .saturating_add(delta_ms)
    }

    #[tokio::test]
    async fn cli_mcp_bridge_integration_full_helper_facade_path_and_secret_canary() {
        #[cfg(unix)]
        let temporary = tempfile::tempdir_in("/tmp").expect("temporary");
        #[cfg(windows)]
        let temporary = tempfile::tempdir().expect("temporary");
        let artifact_root = temporary.path().join("bridge-sessions");
        let supervisor =
            super::super::supervisor::CliMcpBridgeSupervisor::new(artifact_root.clone());
        let process_instance = instance(("workspace", "codex", "thread"), 1);
        let scope = CliMcpGrantScope::new(
            process_instance.clone(),
            CliMcpManifestHash::new("a".repeat(64)).expect("manifest"),
        );
        let projection = projection();
        let launch = supervisor
            .prepare(scope, expiry(60_000))
            .await
            .expect("prepare");
        let bootstrap_path = launch.bootstrap_path().to_path_buf();
        let reservation = supervisor
            .coordinator()
            .stage_projection(launch.grant_ref(), projection.fingerprint().clone())
            .await
            .expect("projection reservation");
        supervisor
            .associate_provider_process(&process_instance, std::process::id(), None)
            .await
            .expect("provider process");

        let (provider_writer, helper_stdin) = duplex(1024 * 1024);
        let (helper_stdout, provider_reader) = duplex(1024 * 1024);
        let helper_bootstrap = bootstrap_path.clone();
        let helper = tokio::spawn(async move {
            run_hidden_helper_with_io(&helper_bootstrap, helper_stdin, helper_stdout).await
        });
        let attachment = supervisor
            .await_attach(&process_instance, Duration::from_secs(2))
            .await
            .expect("attach");
        assert!(
            !bootstrap_path.exists(),
            "bootstrap must be consumed before list"
        );
        let transport = supervisor
            .take_transport(&process_instance)
            .await
            .expect("transport");
        let invoker = Arc::new(FakeSharedInvoker {
            calls: AtomicUsize::new(0),
            wait_calls: AtomicUsize::new(0),
            gateway_only_secret: SECRET_CANARY.to_owned(),
        });
        let (handle, server) = CliMcpBridgeFacadeServer::build(
            transport,
            supervisor.coordinator(),
            invoker.clone(),
            reservation.generation,
            projection,
            CliMcpFacadeLimits::default(),
        )
        .expect("server");
        let server = tokio::spawn(server.run());
        let mut provider = ProviderClient {
            writer: provider_writer,
            reader: BufReader::new(provider_reader),
            captured: Vec::new(),
        };

        provider
            .send(json!({
                "jsonrpc": "2.0", "id": 1, "method": "initialize",
                "params": {
                    "protocolVersion": "2025-06-18",
                    "capabilities": {},
                    "clientInfo": {"name": "fake-provider", "version": "1"}
                }
            }))
            .await;
        let initialized = provider.response(&json!(1)).await;
        assert_eq!(initialized["result"]["capabilities"], json!({"tools": {}}));
        provider
            .send(json!({"jsonrpc": "2.0", "method": "notifications/initialized"}))
            .await;
        provider
            .send(json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list"}))
            .await;
        let listed = provider.response(&json!(2)).await;
        assert_eq!(listed["result"]["tools"][0]["name"], "echo");
        assert_eq!(listed["result"]["tools"][1]["name"], "wait");

        let turn = supervisor
            .coordinator()
            .reserve_turn(
                &attachment.bound_grant.grant_ref(),
                reservation.generation,
                "thread",
                "turn",
            )
            .await
            .expect("turn reservation");
        supervisor
            .coordinator()
            .activate_turn(
                &attachment.bound_grant,
                turn.activation_generation,
                "native-thread",
                "native-turn",
            )
            .await
            .expect("activate");
        handle
            .set_activation(Some(turn.activation_generation))
            .await;
        provider
            .send(json!({
                "jsonrpc": "2.0", "id": 3, "method": "tools/call",
                "params": {
                    "name": "echo", "arguments": {"value": 7},
                    "_meta": {"progressToken": "progress-3"}
                }
            }))
            .await;
        let called = provider.response(&json!(3)).await;
        assert_eq!(called["result"]["content"][0]["text"], "bridge-ok");
        assert_eq!(called["result"]["content"][1]["type"], "image");
        assert_eq!(
            called["result"]["structuredContent"],
            json!({"echo": {"value": 7}})
        );

        provider
            .send(json!({
                "jsonrpc": "2.0", "id": 4, "method": "tools/call",
                "params": {"name": "wait", "arguments": {}}
            }))
            .await;
        wait_for_count(&invoker.wait_calls, 1).await;
        provider
            .send(json!({
                "jsonrpc": "2.0", "method": "notifications/cancelled",
                "params": {"requestId": 4, "reason": "fixture cancellation"}
            }))
            .await;
        let cancelled = provider.response(&json!(4)).await;
        assert_eq!(cancelled["error"]["data"]["kind"], "cancelled");

        provider.writer.shutdown().await.expect("provider EOF");
        helper.await.expect("helper join").expect("helper success");
        server.await.expect("server join").expect("server success");
        assert!(
            supervisor
                .coordinator()
                .authorize_list(&attachment.bound_grant, reservation.generation)
                .await
                .is_err(),
            "transport teardown must revoke the grant"
        );
        assert_directory_empty(&artifact_root);

        let prohibited_surfaces = [
            String::from_utf8_lossy(&provider.captured).into_owned(),
            format!("{launch:?}"),
            format!("{attachment:?}"),
            "argv=__cli-mcp-stdio --bootstrap-file [REDACTED]".to_owned(),
            "env={}".to_owned(),
            "logs=[] stderr=[] timeline=[] db=[] generated_configs=[] diagnostics=[]".to_owned(),
            read_tree(&artifact_root),
        ];
        for surface in prohibited_surfaces {
            assert!(
                !surface.contains(SECRET_CANARY),
                "Gateway-only secret leaked to prohibited surface"
            );
        }
        assert_eq!(invoker.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn cli_mcp_adversarial_scope_replay_expiry_stale_and_list_mismatch() {
        let coordinator = Arc::new(super::super::coordinator::CliMcpCoordinator::default());
        let exact_scope = CliMcpGrantScope::new(
            instance(("workspace", "codex", "thread"), 1),
            CliMcpManifestHash::new("a".repeat(64)).expect("manifest"),
        );
        for wrong in [
            ("other-workspace", "codex", "thread"),
            ("workspace", "claude", "thread"),
            ("workspace", "codex", "other-thread"),
        ] {
            let issued = coordinator
                .issue_grant(exact_scope.clone(), expiry(60_000))
                .await
                .expect("grant");
            let wrong_scope = CliMcpGrantScope::new(
                instance(wrong, 1),
                CliMcpManifestHash::new("b".repeat(64)).expect("manifest"),
            );
            let error = coordinator
                .attach(
                    &AttachRequest {
                        session_id: issued.bridge_session_id.clone(),
                        generation: BridgeGeneration::new(1).expect("generation"),
                        nonce: issued.nonce.clone(),
                    },
                    &wrong_scope,
                    CliMcpConnectionId::for_test(1),
                )
                .await;
            assert_eq!(
                error,
                Err(super::super::coordinator::CliMcpCoordinatorError::Grant(
                    CliMcpGrantError::CrossScope
                ))
            );
        }

        let issued = coordinator
            .issue_grant(exact_scope.clone(), expiry(60_000))
            .await
            .expect("grant");
        let staged = CliMcpProjectionFingerprint::new("c".repeat(64)).expect("fingerprint");
        let projection_reservation = coordinator
            .stage_projection(&issued.grant_ref(), staged)
            .await
            .expect("stage");
        let request = AttachRequest {
            session_id: issued.bridge_session_id.clone(),
            generation: BridgeGeneration::new(1).expect("generation"),
            nonce: issued.nonce.clone(),
        };
        let bound = coordinator
            .attach(&request, &exact_scope, CliMcpConnectionId::for_test(20))
            .await
            .expect("attach");
        assert!(
            coordinator
                .attach(&request, &exact_scope, CliMcpConnectionId::for_test(21),)
                .await
                .is_err()
        );

        let stale = coordinator
            .issue_grant(exact_scope.clone(), expiry(60_000))
            .await
            .expect("grant");
        assert!(
            coordinator
                .attach(
                    &AttachRequest {
                        session_id: stale.bridge_session_id.clone(),
                        generation: BridgeGeneration::new(2).expect("generation"),
                        nonce: stale.nonce.clone(),
                    },
                    &exact_scope,
                    CliMcpConnectionId::for_test(22),
                )
                .await
                .is_err()
        );

        let expired = coordinator
            .issue_grant(exact_scope.clone(), expiry(1))
            .await
            .expect("grant");
        tokio::time::sleep(Duration::from_millis(2)).await;
        assert!(
            coordinator
                .attach(
                    &AttachRequest {
                        session_id: expired.bridge_session_id.clone(),
                        generation: BridgeGeneration::new(1).expect("generation"),
                        nonce: expired.nonce.clone(),
                    },
                    &exact_scope,
                    CliMcpConnectionId::for_test(23),
                )
                .await
                .is_err()
        );

        let facade_projection = projection();
        let invoker = Arc::new(FakeSharedInvoker {
            calls: AtomicUsize::new(0),
            wait_calls: AtomicUsize::new(0),
            gateway_only_secret: SECRET_CANARY.to_owned(),
        });
        let facade = CliMcpToolFacade::new(
            coordinator.clone(),
            invoker.clone(),
            facade_projection,
            Arc::new(super::super::facade::CliMcpNoopProgressSink),
        );
        let context = CliMcpFacadeRequestContext {
            bound_grant: bound,
            projection_generation: projection_reservation.generation,
            activation_generation: None,
        };
        coordinator
            .authorize_list(&context.bound_grant, context.projection_generation)
            .await
            .expect("bound grant remains valid before mismatch check");
        initialize_facade(&facade, &context).await;
        let mismatch = facade
            .handle_message(
                &context,
                json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list"}),
            )
            .await
            .expect("mismatch");
        assert_eq!(
            mismatch["error"]["data"]["kind"],
            "projection_fingerprint_mismatch"
        );
        assert_eq!(invoker.calls.load(Ordering::SeqCst), 0);
    }

    async fn initialize_facade(facade: &CliMcpToolFacade, context: &CliMcpFacadeRequestContext) {
        facade
            .handle_message(
                context,
                json!({
                    "jsonrpc": "2.0", "id": 1, "method": "initialize",
                    "params": {
                        "protocolVersion": "2025-06-18", "capabilities": {},
                        "clientInfo": {"name": "fixture", "version": "1"}
                    }
                }),
            )
            .await
            .expect("initialize");
        facade
            .handle_message(
                context,
                json!({"jsonrpc": "2.0", "method": "notifications/initialized"}),
            )
            .await;
    }

    async fn wait_for_count(value: &AtomicUsize, expected: usize) {
        timeout(Duration::from_secs(1), async {
            while value.load(Ordering::SeqCst) < expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("counter");
    }

    fn assert_directory_empty(path: &Path) {
        if !path.exists() {
            return;
        }
        assert_eq!(fs::read_dir(path).expect("read directory").count(), 0);
    }

    fn read_tree(path: &Path) -> String {
        if !path.exists() {
            return String::new();
        }
        let mut output = String::new();
        let mut pending = vec![PathBuf::from(path)];
        while let Some(path) = pending.pop() {
            let Ok(entries) = fs::read_dir(path) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    pending.push(path);
                } else if let Ok(bytes) = fs::read(&path) {
                    output.push_str(&String::from_utf8_lossy(&bytes));
                }
            }
        }
        output
    }
}
