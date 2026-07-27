//! Hermetic production bridge contract used by the provider conformance gates.

use super::facade::{CliMcpFacadeProjection, CliMcpFacadeTool};
use super::grants::{CliMcpGrantScope, CliMcpManifestHash};
use super::limits::CliMcpFacadeLimits;
use super::limits::CliMcpFacadeProjectionLimits;
use super::server::CliMcpBridgeFacadeServer;
use super::supervisor::CliMcpBridgeSupervisor;
use crate::cli_runtime::manager::CLIAgentRuntimeSessionKey;
use crate::cli_runtime::session_instance::CliSessionGenerationAllocator;
use crate::turn_mcp::invoker::{
    TurnMcpInvocation, TurnMcpInvocationError, TurnMcpInvocationErrorCode, TurnMcpInvoker,
};
use crate::turn_mcp::result::CanonicalMcpToolResult;
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use pioneer_cli_mcp_bridge::helper::run_hidden_helper_with_io;
use serde_json::{Value as JsonValue, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream, duplex};
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;

const SECRET_CANARY: &str = "pioneer-bridge-secret-canary-53";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CliMcpBridgeConformanceEvidence {
    pub(crate) concurrent_isolation_observed: bool,
    pub(crate) turn_blocked_before_list: bool,
    pub(crate) helper_attached: bool,
    pub(crate) bootstrap_consumed_before_list: bool,
    pub(crate) exact_list_observed: bool,
    pub(crate) successful_call_observed: bool,
    pub(crate) cancellation_propagated: bool,
    pub(crate) grant_revoked_on_eof: bool,
    pub(crate) artifacts_removed: bool,
    pub(crate) secret_canary_absent: bool,
}

struct ConformanceInvoker {
    runtime_id: String,
    thread_id: String,
    session_generation: u64,
    calls: AtomicUsize,
    waiting: AtomicUsize,
    wait_callable: String,
    gateway_only_secret: String,
}

#[async_trait]
impl TurnMcpInvoker for ConformanceInvoker {
    async fn invoke(
        &self,
        invocation: TurnMcpInvocation,
        cancellation: CancellationToken,
    ) -> Result<CanonicalMcpToolResult, TurnMcpInvocationError> {
        assert_eq!(invocation.workspace_id, "workspace");
        assert_eq!(invocation.thread_id, self.thread_id);
        assert_eq!(invocation.turn_id, "turn");
        assert_eq!(
            invocation.runtime_id.as_deref(),
            Some(self.runtime_id.as_str())
        );
        assert_eq!(invocation.session_generation, Some(self.session_generation));
        assert!(!self.gateway_only_secret.is_empty());
        self.calls.fetch_add(1, Ordering::SeqCst);
        if invocation.canonical_callable_name == self.wait_callable {
            self.waiting.fetch_add(1, Ordering::SeqCst);
            cancellation.cancelled().await;
            return Err(TurnMcpInvocationError::new(
                TurnMcpInvocationErrorCode::Cancelled,
                "cancelled by deterministic provider",
            ));
        }
        Ok(CanonicalMcpToolResult {
            content: json!([{"type": "text", "text": "bridge-ok"}]),
            structured_content: Some(json!({"echo": invocation.arguments})),
            is_error: false,
            duration_ms: 1,
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
    async fn send(&mut self, value: JsonValue) -> Result<()> {
        let mut bytes = serde_json::to_vec(&value).context("encode provider request")?;
        bytes.push(b'\n');
        self.writer
            .write_all(&bytes)
            .await
            .context("write provider request")?;
        self.writer
            .flush()
            .await
            .context("flush provider request")?;
        bytes.fill(0);
        Ok(())
    }

    async fn response(&mut self, id: &JsonValue) -> Result<JsonValue> {
        timeout(Duration::from_secs(2), async {
            loop {
                let mut line = String::new();
                let read = self.reader.read_line(&mut line).await?;
                ensure!(read != 0, "helper stdout closed before response");
                self.captured.extend_from_slice(line.as_bytes());
                let value: JsonValue = serde_json::from_str(&line)?;
                if value.get("id") == Some(id) {
                    return Ok(value);
                }
            }
        })
        .await
        .context("provider response timeout")?
    }
}

fn tool(name: &str) -> Result<CliMcpFacadeTool> {
    CliMcpFacadeTool::new(
        name,
        Some(format!("{name} deterministic fixture")),
        json!({"type": "object", "properties": {}}),
        json!({"readOnlyHint": true}),
    )
    .map_err(|error| anyhow::anyhow!("build facade tool: {error}"))
}

fn expiry(delta_ms: u64) -> Result<u64> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock predates Unix epoch")?
        .as_millis();
    Ok(u64::try_from(now)?.saturating_add(delta_ms))
}

async fn wait_for_count(counter: &AtomicUsize, expected: usize) -> Result<()> {
    timeout(Duration::from_secs(2), async {
        while counter.load(Ordering::SeqCst) < expected {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .context("deterministic invocation did not start")?;
    Ok(())
}

#[derive(Debug)]
struct SingleBridgeConformanceEvidence {
    turn_blocked_before_list: bool,
    helper_attached: bool,
    bootstrap_consumed_before_list: bool,
    listed_names: Vec<String>,
    successful_call_observed: bool,
    cancellation_propagated: bool,
    grant_revoked_on_eof: bool,
    artifacts_removed: bool,
    secret_canary_absent: bool,
}

async fn run_single_bridge_conformance(
    runtime_id: &str,
    thread_id: &str,
    callable_name: &str,
    wait_callable: &str,
) -> Result<SingleBridgeConformanceEvidence> {
    #[cfg(unix)]
    let temporary = tempfile::tempdir_in("/tmp")?;
    #[cfg(windows)]
    let temporary = tempfile::tempdir()?;
    let artifact_root = temporary.path().join("bridge-sessions");
    let supervisor = CliMcpBridgeSupervisor::new(artifact_root.clone());
    let process_instance = CliSessionGenerationAllocator::default().allocate(
        CLIAgentRuntimeSessionKey::new("workspace", runtime_id, thread_id)?,
    )?;
    let session_generation = process_instance.generation();
    let scope = CliMcpGrantScope::new(
        process_instance.clone(),
        CliMcpManifestHash::new("a".repeat(64))
            .map_err(|error| anyhow::anyhow!("manifest scope: {error:?}"))?,
    );
    let projection = CliMcpFacadeProjection::new(
        vec![tool(callable_name)?, tool(wait_callable)?],
        CliMcpFacadeProjectionLimits::transport_bounded(2),
    )
    .map_err(|error| anyhow::anyhow!("build facade projection: {error}"))?;
    let projection_fingerprint = projection.fingerprint().clone();
    let launch = supervisor.prepare(scope, expiry(60_000)?).await?;
    let bootstrap_path = launch.bootstrap_path().to_path_buf();
    let reservation = supervisor
        .coordinator()
        .stage_projection(launch.grant_ref(), projection_fingerprint.clone())
        .await
        .map_err(|error| anyhow::anyhow!("stage projection: {error:?}"))?;
    supervisor
        .associate_provider_process(&process_instance, std::process::id(), None)
        .await?;

    let (provider_writer, helper_stdin) = duplex(1024 * 1024);
    let (helper_stdout, provider_reader) = duplex(1024 * 1024);
    let helper_bootstrap = bootstrap_path.clone();
    let helper = tokio::spawn(async move {
        run_hidden_helper_with_io(&helper_bootstrap, helper_stdin, helper_stdout).await
    });
    let attachment = supervisor
        .await_attach(&process_instance, Duration::from_secs(2))
        .await?;
    let helper_attached = attachment.process_instance == process_instance;
    let bootstrap_consumed_before_list = !bootstrap_path.exists();
    ensure!(
        helper_attached,
        "helper attached to the wrong process generation"
    );
    ensure!(
        bootstrap_consumed_before_list,
        "bootstrap survived the required attach barrier"
    );

    let transport = supervisor.take_transport(&process_instance).await?;
    let invoker = Arc::new(ConformanceInvoker {
        runtime_id: runtime_id.to_owned(),
        thread_id: thread_id.to_owned(),
        session_generation,
        calls: AtomicUsize::new(0),
        waiting: AtomicUsize::new(0),
        wait_callable: wait_callable.to_owned(),
        gateway_only_secret: SECRET_CANARY.to_owned(),
    });
    let (handle, server) = CliMcpBridgeFacadeServer::build(
        transport,
        supervisor.coordinator(),
        invoker.clone(),
        reservation.generation,
        projection,
        CliMcpFacadeLimits::default(),
    )?;
    let server = tokio::spawn(server.run());
    let mut provider = ProviderClient {
        writer: provider_writer,
        reader: BufReader::new(provider_reader),
        captured: Vec::new(),
    };
    let turn_blocked_before_list = timeout(
        Duration::from_millis(50),
        supervisor.coordinator().wait_projection_ready(
            &attachment.bound_grant,
            reservation.generation,
            &projection_fingerprint,
        ),
    )
    .await
    .is_err();
    ensure!(
        turn_blocked_before_list,
        "required exact tools/list readiness completed before the provider listed tools"
    );

    provider
        .send(json!({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "deterministic-provider", "version": "1"}
            }
        }))
        .await?;
    let initialized = provider.response(&json!(1)).await?;
    ensure!(
        initialized["result"]["capabilities"] == json!({"tools": {}}),
        "facade initialize contract drifted"
    );
    provider
        .send(json!({"jsonrpc": "2.0", "method": "notifications/initialized"}))
        .await?;
    provider
        .send(json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list"}))
        .await?;
    let listed = provider.response(&json!(2)).await?;
    let listed_names = listed["result"]["tools"]
        .as_array()
        .context("tools/list omitted tools")?
        .iter()
        .filter_map(|tool| tool["name"].as_str().map(str::to_owned))
        .collect::<Vec<_>>();
    let exact_list_observed = listed_names == [callable_name, wait_callable];
    ensure!(exact_list_observed, "facade tools/list was not exact");

    let turn = supervisor
        .coordinator()
        .reserve_turn(
            &attachment.bound_grant.grant_ref(),
            reservation.generation,
            "thread",
            "turn",
        )
        .await
        .map_err(|error| anyhow::anyhow!("reserve turn: {error:?}"))?;
    supervisor
        .coordinator()
        .activate_turn(
            &attachment.bound_grant,
            turn.activation_generation,
            "native-thread",
            "native-turn",
        )
        .await
        .map_err(|error| anyhow::anyhow!("activate turn: {error:?}"))?;
    handle
        .set_activation(Some(turn.activation_generation))
        .await;

    provider
        .send(json!({
            "jsonrpc": "2.0", "id": 3, "method": "tools/call",
            "params": {"name": callable_name, "arguments": {"value": 53}}
        }))
        .await?;
    let called = provider.response(&json!(3)).await?;
    let successful_call_observed = called["result"]["content"][0]["text"] == "bridge-ok"
        && called["result"]["structuredContent"] == json!({"echo": {"value": 53}});
    ensure!(successful_call_observed, "facade result contract drifted");

    provider
        .send(json!({
            "jsonrpc": "2.0", "id": 4, "method": "tools/call",
            "params": {"name": wait_callable, "arguments": {}}
        }))
        .await?;
    wait_for_count(&invoker.waiting, 1).await?;
    provider
        .send(json!({
            "jsonrpc": "2.0", "method": "notifications/cancelled",
            "params": {"requestId": 4, "reason": "deterministic cancellation"}
        }))
        .await?;
    let cancelled = provider.response(&json!(4)).await?;
    let cancellation_propagated = cancelled["error"]["data"]["kind"] == "cancelled";
    ensure!(
        cancellation_propagated,
        "cancellation did not reach the invoker"
    );

    provider.writer.shutdown().await?;
    helper.await.context("helper join")??;
    server.await.context("facade join")??;
    let grant_revoked_on_eof = supervisor
        .coordinator()
        .authorize_list(&attachment.bound_grant, reservation.generation)
        .await
        .is_err();
    let artifacts_removed = !artifact_root.exists()
        || artifact_root
            .read_dir()
            .context("read bridge artifact root")?
            .next()
            .is_none();
    let prohibited = format!(
        "{} {launch:?} {attachment:?}",
        String::from_utf8_lossy(&provider.captured)
    );
    let secret_canary_absent = !prohibited.contains(SECRET_CANARY);
    ensure!(grant_revoked_on_eof, "EOF did not revoke the bridge grant");
    ensure!(
        artifacts_removed,
        "bridge artifacts survived terminal cleanup"
    );
    ensure!(
        secret_canary_absent,
        "Gateway-only secret reached a provider surface"
    );
    ensure!(
        invoker.calls.load(Ordering::SeqCst) == 2,
        "facade invocation count drifted"
    );

    Ok(SingleBridgeConformanceEvidence {
        turn_blocked_before_list,
        helper_attached,
        bootstrap_consumed_before_list,
        listed_names,
        successful_call_observed,
        cancellation_propagated,
        grant_revoked_on_eof,
        artifacts_removed,
        secret_canary_absent,
    })
}

pub(crate) async fn run_cli_mcp_bridge_conformance(
    runtime_id: &str,
) -> Result<CliMcpBridgeConformanceEvidence> {
    let runtime_a = format!("{runtime_id}-conformance-a");
    let runtime_b = format!("{runtime_id}-conformance-b");
    let (a, b) = tokio::try_join!(
        run_single_bridge_conformance(runtime_a.as_str(), "thread-a", "fixture_a", "wait_a"),
        run_single_bridge_conformance(runtime_b.as_str(), "thread-b", "fixture_b", "wait_b")
    )?;
    let concurrent_isolation_observed = a.listed_names == ["fixture_a", "wait_a"]
        && b.listed_names == ["fixture_b", "wait_b"]
        && a.listed_names
            .iter()
            .all(|name| !b.listed_names.contains(name));
    ensure!(
        concurrent_isolation_observed,
        "concurrent bridge generations cross-contaminated their exact lists"
    );

    Ok(CliMcpBridgeConformanceEvidence {
        concurrent_isolation_observed,
        turn_blocked_before_list: a.turn_blocked_before_list && b.turn_blocked_before_list,
        helper_attached: a.helper_attached && b.helper_attached,
        bootstrap_consumed_before_list: a.bootstrap_consumed_before_list
            && b.bootstrap_consumed_before_list,
        exact_list_observed: true,
        successful_call_observed: a.successful_call_observed && b.successful_call_observed,
        cancellation_propagated: a.cancellation_propagated && b.cancellation_propagated,
        grant_revoked_on_eof: a.grant_revoked_on_eof && b.grant_revoked_on_eof,
        artifacts_removed: a.artifacts_removed && b.artifacts_removed,
        secret_canary_absent: a.secret_canary_absent && b.secret_canary_absent,
    })
}
