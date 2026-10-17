//! ACP's standard MCP descriptor uses the existing managed stdio facade.
//! Schemas and invocation identities pass through unchanged. ACP toolCallId
//! belongs only to activity display and is never an MCP execution identity.

use super::mcp::facade::{CliMcpFacadeProjection, CliMcpFacadeTool};
use super::mcp::limits::CliMcpFacadeProjectionLimits;
use crate::turn_mcp::projection::ResolvedMcpTurnProjection;
use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use std::fmt;

use super::manager::CLIAgentRuntimeMcpTurnMetadata;
use super::mcp::{
    coordinator::{CliMcpActivationGeneration, CliMcpProjectionGeneration},
    grants::{CliMcpGrantScope, CliMcpManifestHash},
    limits::CliMcpRuntimeLimits,
    server::{CliMcpBridgeFacadeHandle, CliMcpBridgeFacadeServer},
    supervisor::{CliMcpBridgeLaunch, CliMcpBridgeSupervisor},
};
use super::session_instance::CliSessionInstanceId;
use crate::turn_mcp::invoker::TurnMcpInvoker;
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{sync::Mutex, task::JoinHandle};
use tokio_util::sync::CancellationToken;

pub(crate) const ACP_MCP_ADAPTER_KIND: &str = "acp_standard_mcp";

pub(crate) const ACP_SCHEMA_SHA256: &str =
    "4bcd1d2051ff7ed379bd9b234fb9b09c5ab9245478698e9fab805bd9b52a33a6";

pub(crate) async fn readiness(
    instance: &pioneer_config::EffectiveGatewayCliAgentRuntimeInstanceConfig,
    version: Option<String>,
    max_tools: usize,
    max_schema_bytes: usize,
) -> Result<pioneer_protocol::CliMcpAdapterReadiness> {
    use std::io::Read;
    let executable = instance.binary_path.clone();
    let fingerprint = tokio::task::spawn_blocking(move || -> Result<String> {
        let path = std::path::Path::new(&executable);
        let resolved = if path.components().count() > 1 || path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default())
                .map(|root| root.join(path))
                .find(|path| path.is_file())
                .context("configured ACP executable is not on PATH")?
        };
        let mut file = std::fs::File::open(resolved)?;
        let before = file.metadata()?;
        anyhow::ensure!(
            before.is_file() && before.len() <= 512 * 1024 * 1024,
            "ACP executable exceeds fingerprint budget"
        );
        let mut hash = Sha256::new();
        let mut buffer = [0u8; 64 * 1024];
        let mut bytes = 0u64;
        loop {
            let count = file.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            bytes += count as u64;
            anyhow::ensure!(
                bytes <= 512 * 1024 * 1024,
                "ACP executable changed while hashing"
            );
            hash.update(&buffer[..count]);
        }
        let after = file.metadata()?;
        anyhow::ensure!(
            bytes == before.len()
                && before.len() == after.len()
                && before.modified()? == after.modified()?,
            "ACP executable changed while hashing"
        );
        Ok(hex::encode(hash.finalize()))
    })
    .await??;
    let contract = hex::encode(Sha256::digest(serde_json::to_vec(
        &serde_json::json!({"schema":ACP_SCHEMA_SHA256,"protocol":1,"mcp":"raw_standard_stdio","activation":"exact_list_before_calls","version":1}),
    )?));
    Ok(pioneer_protocol::CliMcpAdapterReadiness {
        supported: true,
        injection: pioneer_protocol::CliMcpInjectionKind::AcpStandardStdioMcp,
        projection_update: pioneer_protocol::CliMcpProjectionUpdateKind::AcpNewOrNegotiatedSession,
        // This describes the agent process, not the exact managed facade.
        // Standard ACP cannot attest isolation from agent-local MCP config.
        strict_isolation: false,
        contract_fingerprint: contract,
        local_executable_fingerprint: fingerprint,
        provider_version: version,
        max_tools,
        max_schema_bytes,
        diagnostics: Vec::new(),
    })
}

#[derive(Clone)]
pub(crate) struct AcpMcpSessionLaunchProjection {
    pub(crate) canonical_projection: ResolvedMcpTurnProjection,
    pub(crate) contract_fingerprint: String,
    pub(crate) skill_tooling: Option<Arc<super::acp_skills::AcpSkillTooling>>,
    semantic_fingerprint: String,
}

#[derive(Default)]
struct BridgeState {
    handle: Option<CliMcpBridgeFacadeHandle>,
    active: Option<(String, CliMcpActivationGeneration)>,
    failed: bool,
}

/// One transport projection of the existing supervisor/coordinator. ACP does
/// not prescribe eager MCP discovery, so attachment is served concurrently
/// with session creation and prompt. No ACP tool id enters this execution path.
pub(crate) struct AcpMcpBridge {
    supervisor: Arc<CliMcpBridgeSupervisor>,
    instance: CliSessionInstanceId,
    launch: CliMcpBridgeLaunch,
    generation: CliMcpProjectionGeneration,
    projection: CliMcpFacadeProjection,
    identity: AcpMcpSessionLaunchProjection,
    state: Arc<Mutex<BridgeState>>,
    cancellation: CancellationToken,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl AcpMcpBridge {
    pub(crate) async fn prepare(
        supervisor: Arc<CliMcpBridgeSupervisor>,
        instance: CliSessionInstanceId,
        identity: AcpMcpSessionLaunchProjection,
        limits: CliMcpRuntimeLimits,
    ) -> Result<Self> {
        let projection = identity.facade_projection(limits.facade_projection_limits())?;
        let manifest = CliMcpManifestHash::new(identity.canonical_projection.manifest_hash.clone())
            .map_err(|e| anyhow::anyhow!("invalid MCP manifest: {e:?}"))?;
        let expires = SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX);
        let launch = supervisor
            .prepare(
                CliMcpGrantScope::new(instance.clone(), manifest),
                expires.saturating_add(24 * 60 * 60 * 1000),
            )
            .await?;
        let reservation = match supervisor
            .coordinator()
            .stage_projection(launch.grant_ref(), projection.fingerprint().clone())
            .await
        {
            Ok(value) => value,
            Err(error) => {
                supervisor.revoke_session(&instance).await;
                anyhow::bail!("cannot stage ACP MCP projection: {error:?}");
            }
        };
        Ok(Self {
            supervisor,
            instance,
            launch,
            generation: reservation.generation,
            projection,
            identity,
            state: Arc::new(Mutex::new(BridgeState::default())),
            cancellation: CancellationToken::new(),
            worker: Mutex::new(None),
        })
    }

    pub(crate) fn descriptor(&self) -> Result<pioneer_cli_agent_runtime::acp::schema::McpServer> {
        use pioneer_cli_agent_runtime::acp::schema::{McpServer, McpServerStdio};
        let executable = super::config::resolve_current_pioneer_cli_mcp_helper()?;
        Ok(McpServer::Stdio(
            McpServerStdio::new("pioneer", executable).args(vec![
                "__cli-mcp-stdio".into(),
                "--bootstrap-file".into(),
                self.launch.bootstrap_path().to_string_lossy().into_owned(),
            ]),
        ))
    }

    pub(crate) async fn serve(
        &self,
        process_id: u32,
        invoker: Arc<dyn TurnMcpInvoker>,
        limits: CliMcpRuntimeLimits,
    ) -> Result<()> {
        self.supervisor
            .associate_provider_process(&self.instance, process_id, None)
            .await?;
        let supervisor = self.supervisor.clone();
        let instance = self.instance.clone();
        let projection = self.projection.clone();
        let generation = self.generation;
        let state = self.state.clone();
        let cancellation = self.cancellation.clone();
        let mut owner = self.worker.lock().await;
        anyhow::ensure!(owner.is_none(), "ACP MCP server already started");
        *owner = Some(tokio::spawn(async move {
            let serving = async {
                supervisor
                    .await_attach(&instance, Duration::from_secs(24 * 60 * 60))
                    .await?;
                let transport = supervisor.take_transport(&instance).await?;
                let (handle, server) = CliMcpBridgeFacadeServer::build(
                    transport,
                    supervisor.coordinator(),
                    invoker,
                    generation,
                    projection,
                    limits.facade_limits(),
                )
                .map_err(|e| anyhow::anyhow!("ACP MCP facade: {e}"))?;
                {
                    let mut state = state.lock().await;
                    handle
                        .set_activation(state.active.as_ref().map(|(_, generation)| *generation))
                        .await;
                    state.handle = Some(handle);
                }
                server.run().await.map_err(anyhow::Error::new)
            };
            tokio::select! {
                _ = cancellation.cancelled() => {},
                result = serving => {
                    state.lock().await.failed = true;
                    if result.is_err() {
                        tracing::warn!(generation = instance.generation(), "ACP managed MCP facade stopped with an error");
                    }
                }
            }
            // Drop cancellation must revoke the helper/grant too, even when
            // the session owner cannot complete its ordinary async close.
            supervisor.revoke_session(&instance).await;
        }));
        Ok(())
    }

    pub(crate) async fn prepare_turn(
        &self,
        thread: &str,
        turn: &str,
    ) -> Result<CLIAgentRuntimeMcpTurnMetadata> {
        let mut state = self.state.lock().await;
        anyhow::ensure!(
            !state.failed && state.active.is_none(),
            "ACP MCP generation unavailable or busy"
        );
        let reserved = self
            .supervisor
            .coordinator()
            .reserve_turn(self.launch.grant_ref(), self.generation, thread, turn)
            .await
            .map_err(|e| anyhow::anyhow!("MCP reservation: {e:?}"))?;
        state.active = Some((turn.to_owned(), reserved.activation_generation));
        Ok(CLIAgentRuntimeMcpTurnMetadata {
            adapter_kind: ACP_MCP_ADAPTER_KIND.into(),
            manifest_hash: self.identity.canonical_projection.manifest_hash.clone(),
            projection_fingerprint: self.projection.fingerprint().as_str().into(),
            provider_contract_fingerprint: self.identity.contract_fingerprint.clone(),
            isolation_contract_fingerprint: self.identity.semantic_restart_fingerprint().into(),
            session_generation: self.instance.generation(),
            projection_activation_generation: reserved.activation_generation.get(),
        })
    }

    pub(crate) async fn activate_and_dispatch<T>(
        &self,
        turn: &str,
        native_thread: &str,
        native_turn: &str,
        dispatch: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        let state = self.state.lock().await;
        let (reserved, generation) = state.active.as_ref().context("ACP MCP turn not reserved")?;
        anyhow::ensure!(
            !state.failed && reserved == turn,
            "ACP MCP stale turn binding"
        );
        if let Some(handle) = &state.handle {
            handle.set_activation(Some(*generation)).await;
        }
        self.supervisor
            .coordinator()
            .arm_turn_and_dispatch(
                self.launch.grant_ref(),
                *generation,
                native_thread.into(),
                native_turn.into(),
                dispatch,
            )
            .await
            .map_err(|e| anyhow::anyhow!("MCP activation: {e:?}"))?
    }

    pub(crate) async fn terminal(&self, turn: &str) -> Result<()> {
        let mut state = self.state.lock().await;
        if let Some((reserved, generation)) = &state.active {
            anyhow::ensure!(reserved == turn, "ACP MCP stale terminal binding");
            self.supervisor
                .coordinator()
                .terminal_reserved_turn(self.launch.grant_ref(), *generation)
                .await
                .map_err(|e| anyhow::anyhow!("MCP terminal: {e:?}"))?;
            if let Some(handle) = &state.handle {
                handle.set_activation(None).await;
            }
            state.active = None;
        }
        Ok(())
    }

    pub(crate) async fn close(&self) {
        self.cancellation.cancel();
        self.supervisor.revoke_session(&self.instance).await;
        if let Some(worker) = self.worker.lock().await.take() {
            let _ = worker.await;
        }
    }
}

impl Drop for AcpMcpBridge {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

impl fmt::Debug for AcpMcpSessionLaunchProjection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AcpMcpSessionLaunchProjection")
            .field("manifest", &self.canonical_projection.manifest_hash)
            .field("contract", &self.contract_fingerprint)
            .field("tool_count", &self.canonical_projection.tools.len())
            .finish()
    }
}

impl PartialEq for AcpMcpSessionLaunchProjection {
    fn eq(&self, other: &Self) -> bool {
        self.semantic_fingerprint == other.semantic_fingerprint
    }
}
impl Eq for AcpMcpSessionLaunchProjection {}

impl AcpMcpSessionLaunchProjection {
    pub(crate) fn new(
        projection: ResolvedMcpTurnProjection,
        contract_fingerprint: String,
    ) -> Result<Self> {
        anyhow::ensure!(
            projection.manifest_hash.len() == 64 && contract_fingerprint.len() == 64,
            "ACP MCP launch requires exact manifest and protocol contract fingerprints"
        );
        let mut presentation = projection
            .tools
            .iter()
            .map(|tool| (&tool.canonical_callable_name, &tool.description))
            .collect::<Vec<_>>();
        presentation.sort_by(|left, right| left.0.cmp(right.0));
        let semantic_fingerprint =
            hex::encode(Sha256::digest(serde_json::to_vec(&serde_json::json!({
                "contract": contract_fingerprint,
                "manifest": projection.manifest_hash,
                "transport": ACP_MCP_ADAPTER_KIND,
                "presentation": presentation,
            }))?));
        Ok(Self {
            canonical_projection: projection,
            skill_tooling: None,
            contract_fingerprint,
            semantic_fingerprint,
        })
    }

    pub(crate) fn with_skill_tooling(
        mut self,
        tooling: Option<Arc<super::acp_skills::AcpSkillTooling>>,
    ) -> Self {
        self.skill_tooling = tooling;
        self
    }

    pub(crate) fn semantic_restart_fingerprint(&self) -> &str {
        &self.semantic_fingerprint
    }

    pub(crate) fn facade_projection(
        &self,
        mut limits: CliMcpFacadeProjectionLimits,
    ) -> Result<CliMcpFacadeProjection> {
        let tools = self
            .canonical_projection
            .tools
            .iter()
            .map(|tool| {
                CliMcpFacadeTool::new(
                    tool.canonical_callable_name.clone(),
                    tool.description.clone(),
                    tool.input_schema.clone(),
                    serde_json::to_value(tool.annotations.clone().unwrap_or_default())?,
                )
                .map_err(anyhow::Error::new)
            })
            .collect::<Result<Vec<_>>>()?;
        limits.max_tools += self.canonical_projection.html_tool_count();
        CliMcpFacadeProjection::new(tools, limits)
            .context("ACP managed MCP facade exceeds its transport budget")
    }
}
