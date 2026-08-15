use crate::cli_runtime::codex_mcp::{
    CodexMcpSessionLaunchProjection, build_codex_managed_mcp_config,
};
use crate::cli_runtime::config::{
    codex_account_probe_config_from_instance, codex_account_probe_config_from_instance_with_proxy,
    load_effective_cli_runtime_instances, resolve_current_pioneer_cli_mcp_helper,
};
use crate::cli_runtime::continuation::{
    CliMcpSessionLaunch, CliProviderContinuation, CliSessionLaunchSpec,
};
use crate::cli_runtime::manager::CLIAgentRuntimeSessionKey;
use crate::cli_runtime::manager::{
    CLIAgentRuntimeCodexEventReceivers, CLIAgentRuntimeManager, CLIAgentRuntimeMcpTurnMetadata,
    CLIAgentRuntimeNativeMcpApprovalRequest, CLIAgentRuntimeObservedTurnStatus,
    CLIAgentRuntimeSession, CLIAgentRuntimeSessionFactory, CLIAgentRuntimeSessionStartOptions,
    CLIAgentRuntimeThreadForkRequest, CLIAgentRuntimeThreadForkResult,
    CLIAgentRuntimeThreadNameSetRequest, CLIAgentRuntimeThreadNameSetResult,
    CLIAgentRuntimeThreadOpenParams, CLIAgentRuntimeThreadOpenSnapshot,
    CLIAgentRuntimeTurnLivenessProbe, CLIAgentRuntimeTurnObservation,
    CLIAgentRuntimeTurnStartParams, CLIAgentRuntimeTurnStartSnapshot,
    CLIAgentRuntimeTurnSteerRequest, CLIAgentRuntimeTurnSteerResult,
};
use crate::cli_runtime::mcp::coordinator::{
    CliMcpProjectionFingerprint, CliMcpProjectionGeneration,
};
use crate::cli_runtime::mcp::facade::{CliMcpFacadeProjection, CliMcpFacadeTool};
use crate::cli_runtime::mcp::grants::{CliMcpGrantScope, CliMcpManifestHash};
use crate::cli_runtime::mcp::limits::{
    CliMcpFacadeLimits, CliMcpFacadeProjectionLimits, CliMcpRuntimeLimits,
};
use crate::cli_runtime::mcp::server::{CliMcpBridgeFacadeHandle, CliMcpBridgeFacadeServer};
use crate::cli_runtime::mcp::supervisor::{CliMcpBridgeLaunch, CliMcpBridgeSupervisor};
use crate::cli_runtime::session_instance::{CliSessionGenerationAllocator, CliSessionInstanceId};
use crate::turn_mcp::invoker::{
    TurnMcpInvocation, TurnMcpInvocationError, TurnMcpInvocationErrorCode, TurnMcpInvoker,
};
use crate::turn_mcp::result::CanonicalMcpToolResult;
use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use pioneer_cli_agent_runtime::codex::{
    CodexAppServerClient, CodexCollaborationMode, CodexConfigReadSnapshot,
    CodexGenerationOverlayDescriptor, CodexGenerationOverlayIdentity, CodexHomeOverlayPolicy,
    CodexJsonlRpcClient, CodexJsonlRpcNotificationEvent, CodexManagedMcpConfigInput,
    CodexManagedMcpConfigLimits, CodexManagedMcpSemanticInput, CodexManagedMcpToolIdentity,
    CodexThreadForkParams, CodexThreadNameSetParams, CodexThreadOpenSnapshot,
    CodexThreadStartParams, CodexTurnStartParams, CodexTurnSteerParams,
    cleanup_codex_generation_overlay, codex_config_read_max_origins,
    codex_config_value_fingerprint, codex_generation_app_server_process_config,
    recover_codex_stale_rollout_path, serialize_codex_managed_mcp_config,
    stage_codex_generation_mcp_config, verify_codex_generation_mcp_config,
};
use pioneer_cli_agent_runtime::codex_attestation::sha256_json;
use pioneer_cli_agent_runtime::driver::JsonlRpcId;
use pioneer_cli_agent_runtime::event::{
    RuntimeEvent, RuntimeEventMappingOptions, map_codex_notification_event,
};
use pioneer_cli_agent_runtime::process::{CLIAgentProcess, spawn_cli_agent_process};
use pioneer_cli_agent_runtime::reserved_args::validate_codex_custom_args;
use pioneer_config::{
    EffectiveGatewayCliAgentRuntimeInstanceConfig, GatewayCliAgentRuntimeKindConfig,
};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::BufReader;
use tokio::task::JoinHandle;
use tokio::time::timeout as tokio_timeout;
use tokio_util::sync::CancellationToken;

const CODEX_READINESS_ALLOWED_TOOL: &str = "pioneer_readiness_allowed";
const CODEX_READINESS_FORBIDDEN_SENTINEL: &str = "pioneer_readiness_forbidden_sentinel";

#[derive(Debug, Clone)]
pub(crate) struct CodexMcpLocalProviderProbeEvidence {
    pub(crate) tool_count: usize,
    pub(crate) max_config_origins: usize,
    pub(crate) config_artifact_digest: String,
    pub(crate) effective_mcp_servers_fingerprint: String,
    pub(crate) semantic_restart_fingerprint: String,
    pub(crate) projection_fingerprint: String,
    pub(crate) overlay_policy_version: u32,
    pub(crate) helper_binary_sha256: String,
}

struct CodexReadinessNeverInvoke;

#[async_trait]
impl TurnMcpInvoker for CodexReadinessNeverInvoke {
    async fn invoke(
        &self,
        _invocation: TurnMcpInvocation,
        _cancellation: CancellationToken,
    ) -> std::result::Result<CanonicalMcpToolResult, TurnMcpInvocationError> {
        Err(TurnMcpInvocationError::new(
            TurnMcpInvocationErrorCode::TurnNotActive,
            "readiness probe never permits tools/call",
        ))
    }
}

pub(crate) fn cli_runtime_manager(
    runtime_home: PathBuf,
    idle_session_ttl: Duration,
    mcp_limits: CliMcpRuntimeLimits,
    turn_mcp_invoker: Arc<dyn TurnMcpInvoker>,
    crud_store: Arc<pioneer_crud::CrudStore>,
) -> Result<Arc<CLIAgentRuntimeManager>> {
    let bridge_supervisor =
        CliMcpBridgeSupervisor::new(runtime_home.join("cli-runtime").join("mcp-bridge-sessions"));
    let factory = Arc::new(DispatchingCLIAgentRuntimeSessionFactory {
        runtime_home,
        bridge_supervisor: bridge_supervisor.clone(),
        mcp_limits,
        turn_mcp_invoker,
        crud_store,
    });
    Ok(Arc::new(CLIAgentRuntimeManager::new_with_lifecycle(
        factory,
        idle_session_ttl,
        bridge_supervisor,
    )?))
}

/// Execute the no-model Codex/provider portion of local MCP attestation through
/// the same overlay, config/read, ephemeral thread/start, private bridge, and
/// required-list paths used by a real session. No model turn is created.
pub(crate) async fn run_codex_mcp_local_provider_probe(
    instance: &EffectiveGatewayCliAgentRuntimeInstanceConfig,
    runtime_home: &std::path::Path,
    proxy_url: Option<&str>,
    max_tools: usize,
) -> Result<CodexMcpLocalProviderProbeEvidence> {
    let max_config_origins = codex_config_read_max_origins(max_tools)
        .context("invalid Codex readiness config origin budget")?;
    validate_codex_custom_args(&instance.app_server_args)
        .context("Codex readiness custom launch arguments are invalid")?;
    let key = CLIAgentRuntimeSessionKey::new(
        "pioneer-readiness",
        instance.id.as_str(),
        format!("probe-{}", std::process::id()),
    )?;
    let allocator = CliSessionGenerationAllocator::default();
    let process_instance = allocator.allocate(key.clone())?;
    let second_instance = allocator.allocate(key)?;
    let mut probe_config = codex_account_probe_config_from_instance_with_proxy(instance, proxy_url);
    probe_config.cwd = std::env::current_dir().ok();
    let fallback_root = runtime_home
        .join("cli-runtime")
        .join("codex-readiness-overlays");
    let identity = codex_generation_overlay_identity(&process_instance)?;
    let second_identity = codex_generation_overlay_identity(&second_instance)?;
    let (mut process_config, first_overlay) = codex_generation_app_server_process_config(
        &probe_config,
        fallback_root.as_path(),
        identity,
    )
    .map_err(|error| anyhow!("failed to create first readiness overlay: {error}"))?;
    let mut overlay_guard = CodexGenerationOverlayStartupGuard::new(first_overlay);
    let (_, second_overlay) = codex_generation_app_server_process_config(
        &probe_config,
        fallback_root.as_path(),
        second_identity,
    )
    .map_err(|error| anyhow!("failed to create second readiness overlay: {error}"))?;
    let overlays_are_isolated = overlay_guard.descriptor().effective_home_path
        != second_overlay.effective_home_path
        && overlay_guard.descriptor().shared_home_path == second_overlay.shared_home_path
        && overlay_guard.descriptor().policy_version == CodexHomeOverlayPolicy::v1().version
        && second_overlay.policy_version == CodexHomeOverlayPolicy::v1().version;
    cleanup_codex_generation_overlay(&second_overlay)
        .map_err(|error| anyhow!("failed to clean second readiness overlay: {error}"))?;
    if !overlays_are_isolated
        || !CodexHomeOverlayPolicy::v1()
            .shared_directory_names
            .contains(&"sessions")
        || !CodexHomeOverlayPolicy::v1()
            .shared_directory_names
            .contains(&"sqlite")
    {
        bail!("Codex generation overlay isolation or shared-state policy is unavailable");
    }

    let tool_names = codex_readiness_tool_names(max_tools)?;
    let projection = CliMcpFacadeProjection::new(
        tool_names
            .iter()
            .map(|name| {
                CliMcpFacadeTool::new(
                    name.clone(),
                    None,
                    serde_json::json!({"type": "object", "additionalProperties": false}),
                    serde_json::json!({"readOnlyHint": true}),
                )
            })
            .collect::<std::result::Result<Vec<_>, _>>()?,
        CliMcpFacadeProjectionLimits::transport_bounded(max_tools),
    )?;
    let projection_fingerprint = projection.fingerprint().as_str().to_owned();
    let supervisor = CliMcpBridgeSupervisor::new(
        runtime_home
            .join("cli-runtime")
            .join("codex-readiness-bridge"),
    );
    let manifest_hash = sha256_json(&serde_json::json!({
        "probe": "codex-local-readiness",
        "projection": projection_fingerprint,
    }))?;
    let scope = CliMcpGrantScope::new(
        process_instance.clone(),
        CliMcpManifestHash::new(manifest_hash.clone())
            .map_err(|error| anyhow!("invalid readiness manifest hash: {error:?}"))?,
    );
    let bootstrap_root = overlay_guard
        .descriptor()
        .effective_home_path
        .join("bootstrap");
    let launch = supervisor
        .prepare_in_overlay(
            scope,
            codex_mcp_bootstrap_expiry(instance.startup_probe_timeout_ms)?,
            bootstrap_root.as_path(),
        )
        .await
        .map_err(|error| anyhow!("failed to prepare readiness bridge: {error}"))?;
    let reservation = supervisor
        .coordinator()
        .stage_projection(launch.grant_ref(), projection.fingerprint().clone())
        .await
        .map_err(|error| anyhow!("failed to stage readiness projection: {error:?}"))?;
    let helper_path = resolve_current_pioneer_cli_mcp_helper()?;
    let helper_binary_sha256 =
        pioneer_cli_agent_runtime::codex_attestation::sha256_file_contents(helper_path.as_path())?;
    let transform_contract_fingerprint = sha256_json(&serde_json::json!({
        "contract": "codex-readiness-schema-v1",
    }))?;
    let semantic_tools = tool_names
        .iter()
        .map(|name| {
            let tool_fingerprint = sha256_json(&serde_json::json!({
                "name": name,
                "schema": {"type": "object", "additionalProperties": false},
            }))?;
            Ok(CodexManagedMcpToolIdentity {
                canonical_callable_name: name.clone(),
                canonical_schema_fingerprint: tool_fingerprint.clone(),
                transformed_schema_fingerprint: tool_fingerprint.clone(),
                transform_contract_fingerprint: transform_contract_fingerprint.clone(),
                transformed_fingerprint: tool_fingerprint,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let artifact = serialize_codex_managed_mcp_config(CodexManagedMcpConfigInput {
        semantic: CodexManagedMcpSemanticInput {
            canonical_manifest_hash: manifest_hash,
            provider_manifest_hash: sha256_json(&serde_json::json!({
                "provider": "codex",
                "tools": tool_names.as_slice(),
                "projection": projection_fingerprint.as_str(),
            }))?,
            provider_contract_fingerprint: sha256_json(&serde_json::json!({
                "contract": "codex-managed-mcp-readiness-v1",
            }))?,
            overlay_policy_version: CodexHomeOverlayPolicy::v1().version,
            tools: semantic_tools,
        },
        limits: CodexManagedMcpConfigLimits { max_tools },
        helper_path: Some(helper_path),
        bootstrap_path: Some(launch.bootstrap_path().to_path_buf()),
    })?;
    if artifact.enabled_tools != tool_names
        || artifact
            .config_toml
            .contains(CODEX_READINESS_FORBIDDEN_SENTINEL)
    {
        supervisor.revoke_session(&process_instance).await;
        bail!("Codex exact raw-tool filtering sentinel failed");
    }
    stage_codex_generation_mcp_config(
        overlay_guard.descriptor_mut(),
        &artifact,
        Some(launch.bootstrap_path()),
    )
    .map_err(|error| anyhow!("failed to stage readiness MCP config: {error}"))?;
    verify_codex_generation_mcp_config(overlay_guard.descriptor())
        .map_err(|error| anyhow!("readiness MCP config verification failed: {error}"))?;

    process_config = process_config
        .with_process_generation(process_instance.generation())
        .context("failed to bind readiness process generation")?;
    process_config.args.extend(instance.app_server_args.clone());
    let mut process = spawn_cli_agent_process(&process_config)
        .context("failed to spawn Codex readiness app-server")?;
    let stderr_ring = process.stderr();
    let provider_process_id = process
        .id()
        .filter(|process_id| *process_id != 0)
        .ok_or_else(|| anyhow!("Codex readiness process identity is unavailable"))?;
    supervisor
        .associate_provider_process(&process_instance, provider_process_id, None)
        .await
        .map_err(|error| anyhow!("failed to bind readiness provider process: {error}"))?;
    let (stdout, stdin) = process
        .take_stdio()
        .context("failed to open Codex readiness stdio")?;
    let rpc = CodexJsonlRpcClient::new(BufReader::new(stdout), stdin);
    let client = CodexAppServerClient::new(rpc.clone());
    let timeout = Duration::from_millis(instance.startup_probe_timeout_ms.max(1));
    let coordinator = supervisor.coordinator();
    let projection_for_server = projection.clone();
    let supervisor_for_server = supervisor.clone();
    let instance_for_server = process_instance.clone();
    let bridge = async move {
        let attachment = supervisor_for_server
            .await_attach(&instance_for_server, timeout)
            .await
            .map_err(|error| anyhow!("Codex readiness helper attach failed: {error}"))?;
        let transport = supervisor_for_server
            .take_transport(&instance_for_server)
            .await
            .map_err(|error| anyhow!("Codex readiness transport failed: {error}"))?;
        let (_, server) = CliMcpBridgeFacadeServer::build(
            transport,
            supervisor_for_server.coordinator(),
            Arc::new(CodexReadinessNeverInvoke),
            reservation.generation,
            projection_for_server.clone(),
            CliMcpFacadeLimits::default(),
        )?;
        let server = tokio::spawn(server.run());
        tokio_timeout(
            timeout,
            supervisor_for_server.coordinator().wait_projection_ready(
                &attachment.bound_grant,
                reservation.generation,
                projection_for_server.fingerprint(),
            ),
        )
        .await
        .map_err(|_| anyhow!("Codex readiness tools/list barrier timed out"))?
        .map_err(|error| anyhow!("Codex readiness tools/list barrier failed: {error:?}"))?;
        Ok::<_, anyhow::Error>(server)
    };
    let probe_result = async {
        let initialize = client
            .initialize(timeout)
            .await
            .map_err(|error| anyhow!("Codex readiness initialization failed: {error}"))?;
        if initialize
            .version
            .as_deref()
            .unwrap_or_default()
            .trim()
            .is_empty()
            && initialize
                .user_agent
                .as_deref()
                .unwrap_or_default()
                .trim()
                .is_empty()
        {
            bail!("Codex readiness initialize identity is unavailable");
        }
        let cwd = probe_config
            .cwd
            .as_deref()
            .and_then(std::path::Path::to_str)
            .unwrap_or("/")
            .to_owned();
        let expectation = CodexMcpAttestationExpectation {
            server_names: vec!["pioneer".to_owned()],
            staged_mcp_servers_fingerprint: artifact.staged_mcp_servers_fingerprint.clone(),
            effective_mcp_servers_fingerprint: artifact.effective_mcp_servers_fingerprint.clone(),
            requires_staged_artifact: true,
            max_config_origins,
        };
        attest_codex_exact_isolation(
            &client,
            cwd.as_str(),
            &expectation,
            Some(overlay_guard.descriptor()),
            timeout,
        )
        .await
        .map_err(anyhow::Error::from)?;
        // Codex starts configured MCP servers during thread/start, not during
        // app-server initialize. An ephemeral read-only thread exercises the
        // exact production startup barrier without persisting history or
        // issuing a provider inference turn/start.
        let open = client.thread_start(
            CodexThreadStartParams {
                cwd,
                approval_policy: "never".to_owned(),
                ephemeral: true,
                sandbox: Some("read-only".to_owned()),
                permissions: None,
                model: None,
                service_tier: None,
            },
            timeout,
        );
        let (opened, server) = tokio::try_join!(
            async {
                open.await
                    .map_err(|error| anyhow!("Codex readiness thread/start failed: {error}"))
            },
            bridge
        )?;
        if opened.native_thread_id.trim().is_empty() {
            server.abort();
            bail!("Codex readiness thread/start identity is unavailable");
        }
        Ok::<_, anyhow::Error>(server)
    }
    .await;
    let _ = rpc.shutdown().await;
    let _ = process.terminate_with_grace(Duration::from_secs(2)).await;
    let (probe_error, cleanup_error) = match probe_result {
        Ok(mut server) => match tokio_timeout(Duration::from_secs(2), &mut server).await {
            Ok(Ok(Ok(()))) => (None, None),
            Ok(Ok(Err(error))) => (
                None,
                Some(anyhow!("Codex readiness bridge cleanup failed: {error}")),
            ),
            Ok(Err(error)) => (
                None,
                Some(anyhow!("Codex readiness bridge task failed: {error}")),
            ),
            Err(_) => {
                server.abort();
                let _ = server.await;
                (
                    None,
                    Some(anyhow!("Codex readiness bridge cleanup timed out")),
                )
            }
        },
        Err(error) => (Some(error), None),
    };
    let _ = supervisor.revoke_session(&process_instance).await;
    supervisor.shutdown().await;
    if let Some(error) = probe_error {
        let stderr = stderr_ring.lines().await.join(" | ");
        if stderr.is_empty() {
            return Err(error);
        }
        return Err(error.context(format!(
            "Codex readiness stderr: {}",
            bounded_codex_readiness_diagnostic(stderr.as_str())
        )));
    }
    if let Some(error) = cleanup_error {
        return Err(error);
    }
    if launch.bootstrap_path().exists() {
        bail!("Codex readiness helper cancellation or cleanup failed");
    }
    drop(coordinator);
    Ok(CodexMcpLocalProviderProbeEvidence {
        tool_count: max_tools,
        max_config_origins,
        config_artifact_digest: artifact.artifact_digest,
        effective_mcp_servers_fingerprint: artifact.effective_mcp_servers_fingerprint,
        semantic_restart_fingerprint: artifact.semantic_restart_fingerprint,
        projection_fingerprint,
        overlay_policy_version: CodexHomeOverlayPolicy::v1().version,
        helper_binary_sha256,
    })
}

fn codex_readiness_tool_names(max_tools: usize) -> Result<Vec<String>> {
    if max_tools == 0 {
        bail!("Codex readiness tool limit must be greater than zero");
    }
    let mut names = Vec::with_capacity(max_tools);
    names.push(CODEX_READINESS_ALLOWED_TOOL.to_owned());
    for index in 1..max_tools {
        names.push(format!("pioneer_readiness_allowed_{index:08}"));
    }
    Ok(names)
}

fn bounded_codex_readiness_diagnostic(value: &str) -> String {
    const MAX_BYTES: usize = 2_048;
    if value.len() <= MAX_BYTES {
        return value.to_owned();
    }
    let mut end = MAX_BYTES;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &value[..end])
}

struct DispatchingCLIAgentRuntimeSessionFactory {
    runtime_home: PathBuf,
    bridge_supervisor: Arc<CliMcpBridgeSupervisor>,
    mcp_limits: CliMcpRuntimeLimits,
    turn_mcp_invoker: Arc<dyn TurnMcpInvoker>,
    crud_store: Arc<pioneer_crud::CrudStore>,
}

#[async_trait]
impl CLIAgentRuntimeSessionFactory for DispatchingCLIAgentRuntimeSessionFactory {
    async fn start_session(
        &self,
        process_instance: &CliSessionInstanceId,
    ) -> Result<Arc<dyn CLIAgentRuntimeSession>> {
        self.start_session_with_options(
            process_instance,
            &CLIAgentRuntimeSessionStartOptions::default(),
        )
        .await
    }

    async fn start_session_with_options(
        &self,
        process_instance: &CliSessionInstanceId,
        options: &CLIAgentRuntimeSessionStartOptions,
    ) -> Result<Arc<dyn CLIAgentRuntimeSession>> {
        let key = process_instance.key();
        let instance = load_effective_cli_runtime_instances(self.runtime_home.as_path())?
            .into_iter()
            .find(|instance| instance.id == key.runtime_id)
            .ok_or_else(|| anyhow!("unknown CLI runtime `{}`", key.runtime_id))?;
        match instance.kind {
            GatewayCliAgentRuntimeKindConfig::Codex => {
                CodexCLIAgentRuntimeSessionFactory {
                    runtime_home: self.runtime_home.clone(),
                    bridge_supervisor: self.bridge_supervisor.clone(),
                    mcp_limits: self.mcp_limits,
                    turn_mcp_invoker: self.turn_mcp_invoker.clone(),
                }
                .start_session_with_options(process_instance, options)
                .await
            }
            GatewayCliAgentRuntimeKindConfig::Claude => {
                crate::cli_runtime::claude_session::ClaudeCLIAgentRuntimeSessionFactory::new_with_bridge(
                    self.runtime_home.clone(),
                    self.bridge_supervisor.clone(),
                    self.mcp_limits,
                    self.turn_mcp_invoker.clone(),
                    self.crud_store.clone(),
                )
                .start_session_with_options(process_instance, options)
                .await
            }
        }
    }

    async fn start_session_with_launch_spec(
        &self,
        process_instance: &CliSessionInstanceId,
        launch_spec: &CliSessionLaunchSpec,
    ) -> Result<Arc<dyn CLIAgentRuntimeSession>> {
        let key = process_instance.key();
        let instance = load_effective_cli_runtime_instances(self.runtime_home.as_path())?
            .into_iter()
            .find(|instance| instance.id == key.runtime_id)
            .ok_or_else(|| anyhow!("unknown CLI runtime `{}`", key.runtime_id))?;
        match instance.kind {
            GatewayCliAgentRuntimeKindConfig::Codex => {
                CodexCLIAgentRuntimeSessionFactory {
                    runtime_home: self.runtime_home.clone(),
                    bridge_supervisor: self.bridge_supervisor.clone(),
                    mcp_limits: self.mcp_limits,
                    turn_mcp_invoker: self.turn_mcp_invoker.clone(),
                }
                .start_session_with_launch_spec(process_instance, launch_spec)
                .await
            }
            GatewayCliAgentRuntimeKindConfig::Claude => {
                crate::cli_runtime::claude_session::ClaudeCLIAgentRuntimeSessionFactory::new_with_bridge(
                    self.runtime_home.clone(),
                    self.bridge_supervisor.clone(),
                    self.mcp_limits,
                    self.turn_mcp_invoker.clone(),
                    self.crud_store.clone(),
                )
                .start_session_with_launch_spec(process_instance, launch_spec)
                .await
            }
        }
    }
}

struct CodexCLIAgentRuntimeSessionFactory {
    runtime_home: PathBuf,
    bridge_supervisor: Arc<CliMcpBridgeSupervisor>,
    mcp_limits: CliMcpRuntimeLimits,
    turn_mcp_invoker: Arc<dyn TurnMcpInvoker>,
}

struct CodexPreparedMcpBridge {
    launch: CliMcpBridgeLaunch,
    projection: CliMcpFacadeProjection,
    projection_generation: CliMcpProjectionGeneration,
    canonical_manifest_hash: String,
    provider_contract_fingerprint: String,
    isolation_contract_fingerprint: String,
}

struct CodexRequiredMcpBridge {
    supervisor: Arc<CliMcpBridgeSupervisor>,
    process_instance: CliSessionInstanceId,
    launch: CliMcpBridgeLaunch,
    projection: CliMcpFacadeProjection,
    projection_generation: CliMcpProjectionGeneration,
    projection_fingerprint: CliMcpProjectionFingerprint,
    canonical_manifest_hash: String,
    provider_contract_fingerprint: String,
    isolation_contract_fingerprint: String,
    invoker: Arc<dyn TurnMcpInvoker>,
    facade_limits: CliMcpFacadeLimits,
    native_items: Arc<CodexNativeMcpCorrelationLedger>,
    state: tokio::sync::Mutex<CodexRequiredMcpBridgeState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CodexNativeMcpItemKey {
    native_thread_id: String,
    native_turn_id: String,
    native_item_id: String,
}

struct CodexNativeMcpItemCorrelation {
    canonical_callable_name: String,
    arguments_fingerprint: String,
    sequence: u64,
    facade_request_id: Option<String>,
}

#[derive(Default)]
struct CodexNativeMcpCorrelationLedger {
    items: StdMutex<HashMap<CodexNativeMcpItemKey, CodexNativeMcpItemCorrelation>>,
    next_sequence: AtomicU64,
    changed: tokio::sync::Notify,
}

impl CodexNativeMcpCorrelationLedger {
    fn register(
        &self,
        binding: crate::cli_runtime::codex_mcp::CodexNativeMcpItemBinding,
    ) -> Result<()> {
        let key = CodexNativeMcpItemKey {
            native_thread_id: binding.native_thread_id,
            native_turn_id: binding.native_turn_id,
            native_item_id: binding.native_item_id,
        };
        let mut items = self
            .items
            .lock()
            .expect("Codex native MCP correlation ledger should not be poisoned");
        if let Some(existing) = items.get(&key) {
            if existing.canonical_callable_name != binding.canonical_callable_name
                || existing.arguments_fingerprint != binding.arguments_fingerprint
            {
                bail!("Codex native MCP item identity changed during replay")
            }
            return Ok(());
        }
        items.insert(
            key,
            CodexNativeMcpItemCorrelation {
                canonical_callable_name: binding.canonical_callable_name,
                arguments_fingerprint: binding.arguments_fingerprint,
                sequence: self.next_sequence.fetch_add(1, AtomicOrdering::Relaxed),
                facade_request_id: None,
            },
        );
        drop(items);
        self.changed.notify_waiters();
        Ok(())
    }

    fn callable(&self, key: &CodexNativeMcpItemKey) -> Option<String> {
        self.items
            .lock()
            .expect("Codex native MCP correlation ledger should not be poisoned")
            .get(key)
            .map(|item| item.canonical_callable_name.clone())
    }

    fn contains(&self, key: &CodexNativeMcpItemKey) -> bool {
        self.items
            .lock()
            .expect("Codex native MCP correlation ledger should not be poisoned")
            .contains_key(key)
    }

    async fn claim(
        &self,
        canonical_callable_name: &str,
        arguments_fingerprint: &str,
        facade_request_id: &str,
    ) -> Option<CodexNativeMcpItemKey> {
        let notified = self.changed.notified();
        if let Some(key) = self.claim_now(
            canonical_callable_name,
            arguments_fingerprint,
            facade_request_id,
        ) {
            return Some(key);
        }
        let _ = tokio_timeout(Duration::from_millis(250), notified).await;
        self.claim_now(
            canonical_callable_name,
            arguments_fingerprint,
            facade_request_id,
        )
    }

    fn claim_now(
        &self,
        canonical_callable_name: &str,
        arguments_fingerprint: &str,
        facade_request_id: &str,
    ) -> Option<CodexNativeMcpItemKey> {
        let mut items = self
            .items
            .lock()
            .expect("Codex native MCP correlation ledger should not be poisoned");
        let key = items
            .iter()
            .filter(|(_, item)| {
                item.facade_request_id.is_none()
                    && item.canonical_callable_name == canonical_callable_name
                    && item.arguments_fingerprint == arguments_fingerprint
            })
            .min_by_key(|(_, item)| item.sequence)
            .map(|(key, _)| key.clone())?;
        items
            .get_mut(&key)
            .expect("selected Codex MCP correlation must exist")
            .facade_request_id = Some(facade_request_id.to_owned());
        Some(key)
    }

    fn clear_turn(&self, native_thread_id: Option<&str>, native_turn_id: Option<&str>) {
        let (Some(native_thread_id), Some(native_turn_id)) = (native_thread_id, native_turn_id)
        else {
            return;
        };
        self.items
            .lock()
            .expect("Codex native MCP correlation ledger should not be poisoned")
            .retain(|key, _| {
                key.native_thread_id != native_thread_id || key.native_turn_id != native_turn_id
            });
    }
}

struct CodexCorrelatingTurnMcpInvoker {
    inner: Arc<dyn TurnMcpInvoker>,
    native_items: Arc<CodexNativeMcpCorrelationLedger>,
}

pub(crate) fn codex_native_approval_fallback_response(
    requested_permissions: JsonValue,
    exact_active_binding: bool,
) -> Option<JsonValue> {
    exact_active_binding.then(|| {
        serde_json::json!({
            "permissions": requested_permissions,
            "scope": "turn",
            "strictAutoReview": false,
        })
    })
}

#[async_trait]
impl TurnMcpInvoker for CodexCorrelatingTurnMcpInvoker {
    async fn invoke(
        &self,
        mut invocation: crate::turn_mcp::invoker::TurnMcpInvocation,
        cancellation: CancellationToken,
    ) -> std::result::Result<
        crate::turn_mcp::result::CanonicalMcpToolResult,
        crate::turn_mcp::invoker::TurnMcpInvocationError,
    > {
        let arguments_fingerprint =
            crate::cli_runtime::codex_mcp::canonical_value_fingerprint(&invocation.arguments)
                .map_err(|_| {
                    crate::turn_mcp::invoker::TurnMcpInvocationError::new(
                        crate::turn_mcp::invoker::TurnMcpInvocationErrorCode::InvalidRequest,
                        "Codex MCP invocation arguments are not canonicalizable",
                    )
                })?;
        let facade_request_id = invocation.provider_call_id.clone();
        let native_item = tokio::select! {
            biased;
            () = cancellation.cancelled() => None,
            native_item = self.native_items.claim(
                invocation.canonical_callable_name.as_str(),
                arguments_fingerprint.as_str(),
                facade_request_id.as_str(),
            ) => native_item,
        }
        .ok_or_else(|| {
            crate::turn_mcp::invoker::TurnMcpInvocationError::new(
                crate::turn_mcp::invoker::TurnMcpInvocationErrorCode::SessionBindingUnavailable,
                "Codex MCP facade call has no matching native lifecycle item",
            )
        })?;
        invocation.provider_call_id = native_item.native_item_id;
        self.inner.invoke(invocation, cancellation).await
    }
}

struct CodexActiveMcpTurn {
    pioneer_turn_id: String,
    activation_generation: crate::cli_runtime::mcp::coordinator::CliMcpActivationGeneration,
    native_thread_id: Option<String>,
    native_turn_id: Option<String>,
}

enum CodexRequiredMcpBridgeState {
    Pending,
    Serving {
        bound_grant: crate::cli_runtime::mcp::grants::CliMcpBoundGrant,
        handle: CliMcpBridgeFacadeHandle,
        server: JoinHandle<Result<(), crate::cli_runtime::mcp::server::CliMcpBridgeServerError>>,
    },
    Ready {
        bound_grant: crate::cli_runtime::mcp::grants::CliMcpBoundGrant,
        handle: CliMcpBridgeFacadeHandle,
        server: JoinHandle<Result<(), crate::cli_runtime::mcp::server::CliMcpBridgeServerError>>,
        active_turn: Option<CodexActiveMcpTurn>,
    },
    Failed,
}

impl CodexRequiredMcpBridge {
    async fn ensure_ready(&self, readiness_timeout: Duration) -> Result<()> {
        if readiness_timeout.is_zero() {
            bail!("Codex required MCP readiness timeout must be non-zero");
        }
        let bound = {
            let mut state = self.state.lock().await;
            match &*state {
                CodexRequiredMcpBridgeState::Ready { .. } => return Ok(()),
                CodexRequiredMcpBridgeState::Failed => {
                    bail!("Codex required MCP bridge generation is failed")
                }
                CodexRequiredMcpBridgeState::Serving { bound_grant, .. } => bound_grant.clone(),
                CodexRequiredMcpBridgeState::Pending => {
                    let attachment = self
                        .supervisor
                        .await_attach(&self.process_instance, readiness_timeout)
                        .await
                        .map_err(|error| {
                            anyhow!("Codex required MCP helper attach failed: {error}")
                        })?;
                    let transport = self
                        .supervisor
                        .take_transport(&self.process_instance)
                        .await
                        .map_err(|error| anyhow!("Codex required MCP transport failed: {error}"))?;
                    let (handle, server) = CliMcpBridgeFacadeServer::build(
                        transport,
                        self.supervisor.coordinator(),
                        Arc::new(CodexCorrelatingTurnMcpInvoker {
                            inner: self.invoker.clone(),
                            native_items: self.native_items.clone(),
                        }),
                        self.projection_generation,
                        self.projection.clone(),
                        self.facade_limits.clone(),
                    )
                    .map_err(|error| anyhow!("Codex MCP facade build failed: {error}"))?;
                    let server = tokio::spawn(server.run());
                    let bound = attachment.bound_grant;
                    *state = CodexRequiredMcpBridgeState::Serving {
                        bound_grant: bound.clone(),
                        handle,
                        server,
                    };
                    bound
                }
            }
        };
        tokio_timeout(
            readiness_timeout,
            self.supervisor.coordinator().wait_projection_ready(
                &bound,
                self.projection_generation,
                &self.projection_fingerprint,
            ),
        )
        .await
        .map_err(|_| anyhow!("Codex required MCP tools/list readiness timed out"))?
        .map_err(|error| anyhow!("Codex required MCP tools/list readiness failed: {error:?}"))?;
        let mut state = self.state.lock().await;
        match std::mem::replace(&mut *state, CodexRequiredMcpBridgeState::Failed) {
            CodexRequiredMcpBridgeState::Serving {
                bound_grant,
                handle,
                server,
            } => {
                *state = CodexRequiredMcpBridgeState::Ready {
                    bound_grant,
                    handle,
                    server,
                    active_turn: None,
                };
                Ok(())
            }
            ready @ CodexRequiredMcpBridgeState::Ready { .. } => {
                *state = ready;
                Ok(())
            }
            other => {
                *state = other;
                bail!("Codex required MCP bridge changed state before readiness")
            }
        }
    }

    async fn fail_closed(&self) {
        let mut state = self.state.lock().await;
        let previous = std::mem::replace(&mut *state, CodexRequiredMcpBridgeState::Failed);
        match previous {
            CodexRequiredMcpBridgeState::Serving { server, .. }
            | CodexRequiredMcpBridgeState::Ready { server, .. } => server.abort(),
            CodexRequiredMcpBridgeState::Pending | CodexRequiredMcpBridgeState::Failed => {}
        }
        drop(state);
        self.supervisor.revoke_session(&self.process_instance).await;
    }

    fn register_native_item(
        &self,
        binding: crate::cli_runtime::codex_mcp::CodexNativeMcpItemBinding,
    ) -> Result<()> {
        self.native_items.register(binding)
    }

    fn native_item_callable(&self, event: &RuntimeEvent) -> Option<String> {
        let RuntimeEvent::ItemDelta(delta) = event else {
            return None;
        };
        let key = CodexNativeMcpItemKey {
            native_thread_id: delta.native_thread_id.clone()?,
            native_turn_id: delta.native_turn_id.clone(),
            native_item_id: delta.native_item_id.clone(),
        };
        self.native_items.callable(&key)
    }

    async fn prepare_turn(
        &self,
        pioneer_thread_id: &str,
        pioneer_turn_id: &str,
    ) -> Result<CLIAgentRuntimeMcpTurnMetadata> {
        self.ensure_ready(Duration::from_secs(30)).await?;
        let mut state = self.state.lock().await;
        let CodexRequiredMcpBridgeState::Ready {
            active_turn,
            bound_grant: _,
            handle: _,
            server: _,
        } = &mut *state
        else {
            bail!("Codex MCP bridge is not ready for turn reservation")
        };
        if active_turn.is_some() {
            bail!("Codex MCP bridge already has a non-terminal turn lease")
        }
        let reservation = self
            .supervisor
            .coordinator()
            .reserve_turn(
                self.launch.grant_ref(),
                self.projection_generation,
                pioneer_thread_id,
                pioneer_turn_id,
            )
            .await
            .map_err(|error| anyhow!("failed to reserve Codex MCP turn: {error:?}"))?;
        *active_turn = Some(CodexActiveMcpTurn {
            pioneer_turn_id: pioneer_turn_id.to_owned(),
            activation_generation: reservation.activation_generation,
            native_thread_id: None,
            native_turn_id: None,
        });
        Ok(CLIAgentRuntimeMcpTurnMetadata {
            adapter_kind: "codex_synthetic_mcp".to_owned(),
            manifest_hash: self.canonical_manifest_hash.clone(),
            projection_fingerprint: self.projection_fingerprint.as_str().to_owned(),
            provider_contract_fingerprint: self.provider_contract_fingerprint.clone(),
            isolation_contract_fingerprint: self.isolation_contract_fingerprint.clone(),
            session_generation: self.process_instance.generation(),
            projection_activation_generation: reservation.activation_generation.get(),
        })
    }

    async fn activate_turn(
        &self,
        pioneer_turn_id: &str,
        native_thread_id: &str,
        native_turn_id: &str,
    ) -> Result<()> {
        let mut state = self.state.lock().await;
        let CodexRequiredMcpBridgeState::Ready {
            bound_grant,
            handle,
            active_turn,
            ..
        } = &mut *state
        else {
            bail!("Codex MCP bridge is not ready for turn activation")
        };
        let turn = active_turn
            .as_mut()
            .ok_or_else(|| anyhow!("Codex MCP turn was not reserved"))?;
        if turn.pioneer_turn_id != pioneer_turn_id {
            bail!("Codex MCP turn reservation does not match the Pioneer turn")
        }
        if turn
            .native_thread_id
            .as_deref()
            .is_some_and(|staged| staged != native_thread_id)
        {
            bail!("Codex MCP staged segment belongs to a different native thread")
        }
        let effective_native_thread_id = turn
            .native_thread_id
            .clone()
            .unwrap_or_else(|| native_thread_id.to_owned());
        let effective_native_turn_id = turn
            .native_turn_id
            .clone()
            .unwrap_or_else(|| native_turn_id.to_owned());
        self.supervisor
            .coordinator()
            .activate_turn(
                bound_grant,
                turn.activation_generation,
                effective_native_thread_id.as_str(),
                effective_native_turn_id.as_str(),
            )
            .await
            .map_err(|error| anyhow!("failed to activate Codex MCP turn: {error:?}"))?;
        handle
            .set_activation(Some(turn.activation_generation))
            .await;
        turn.native_thread_id = Some(effective_native_thread_id);
        turn.native_turn_id = Some(effective_native_turn_id);
        Ok(())
    }

    async fn retarget_turn(
        &self,
        pioneer_turn_id: &str,
        native_thread_id: &str,
        native_turn_id: &str,
    ) -> Result<()> {
        let mut state = self.state.lock().await;
        let CodexRequiredMcpBridgeState::Ready {
            bound_grant,
            active_turn: Some(turn),
            ..
        } = &mut *state
        else {
            return Ok(());
        };
        if turn.pioneer_turn_id != pioneer_turn_id {
            bail!("Codex MCP retarget does not match the active Pioneer turn")
        }
        self.supervisor
            .coordinator()
            .retarget_turn(
                bound_grant,
                turn.activation_generation,
                native_thread_id,
                native_turn_id,
            )
            .await
            .map_err(|error| anyhow!("failed to retarget Codex MCP turn: {error:?}"))?;
        self.native_items.clear_turn(
            turn.native_thread_id.as_deref(),
            turn.native_turn_id.as_deref(),
        );
        turn.native_thread_id = Some(native_thread_id.to_owned());
        turn.native_turn_id = Some(native_turn_id.to_owned());
        Ok(())
    }

    async fn terminal_turn(&self, pioneer_turn_id: &str) -> Result<()> {
        let mut state = self.state.lock().await;
        let CodexRequiredMcpBridgeState::Ready {
            bound_grant,
            handle,
            active_turn,
            ..
        } = &mut *state
        else {
            return Ok(());
        };
        let Some(turn) = active_turn.take() else {
            return Ok(());
        };
        if turn.pioneer_turn_id != pioneer_turn_id {
            *active_turn = Some(turn);
            bail!("Codex MCP terminal turn does not match the active lease")
        }
        self.supervisor
            .coordinator()
            .terminal_turn(bound_grant, turn.activation_generation)
            .await
            .map_err(|error| anyhow!("failed to terminalize Codex MCP turn: {error:?}"))?;
        handle.set_activation(None).await;
        self.native_items.clear_turn(
            turn.native_thread_id.as_deref(),
            turn.native_turn_id.as_deref(),
        );
        Ok(())
    }

    async fn native_approval_response(
        &self,
        request: CLIAgentRuntimeNativeMcpApprovalRequest,
    ) -> Result<Option<JsonValue>> {
        let key = CodexNativeMcpItemKey {
            native_thread_id: request.native_thread_id.clone(),
            native_turn_id: request.native_turn_id.clone(),
            native_item_id: request.native_item_id.clone(),
        };
        let notified = self.native_items.changed.notified();
        let known = self.native_items.contains(&key);
        if !known {
            let _ = tokio_timeout(Duration::from_millis(250), notified).await;
        }
        if !self.native_items.contains(&key) {
            return Ok(None);
        }
        let state = self.state.lock().await;
        let CodexRequiredMcpBridgeState::Ready {
            bound_grant,
            active_turn: Some(turn),
            ..
        } = &*state
        else {
            return Ok(None);
        };
        if turn.native_thread_id.as_deref() != Some(request.native_thread_id.as_str())
            || turn.native_turn_id.as_deref() != Some(request.native_turn_id.as_str())
        {
            return Ok(None);
        }
        if self
            .supervisor
            .coordinator()
            .authorize_call(bound_grant, turn.activation_generation)
            .await
            .is_err()
        {
            return Ok(None);
        }
        Ok(codex_native_approval_fallback_response(
            request.requested_permissions,
            true,
        ))
    }
}

#[async_trait]
impl CLIAgentRuntimeSessionFactory for CodexCLIAgentRuntimeSessionFactory {
    async fn start_session(
        &self,
        process_instance: &CliSessionInstanceId,
    ) -> Result<Arc<dyn CLIAgentRuntimeSession>> {
        self.start_session_with_options(
            process_instance,
            &CLIAgentRuntimeSessionStartOptions::default(),
        )
        .await
    }

    async fn start_session_with_options(
        &self,
        process_instance: &CliSessionInstanceId,
        options: &CLIAgentRuntimeSessionStartOptions,
    ) -> Result<Arc<dyn CLIAgentRuntimeSession>> {
        self.start_session_with_launch_spec(
            process_instance,
            &CliSessionLaunchSpec::unmanaged_codex(options.clone()),
        )
        .await
    }

    async fn start_session_with_launch_spec(
        &self,
        process_instance: &CliSessionInstanceId,
        launch_spec: &CliSessionLaunchSpec,
    ) -> Result<Arc<dyn CLIAgentRuntimeSession>> {
        let options = &launch_spec.options;
        if options.elevated_instructions.is_some() {
            bail!(
                "Codex elevated instructions are turn-local and must be delivered through turn/start collaborationMode"
            );
        }
        let expected_native_thread_id = match &launch_spec.continuation {
            CliProviderContinuation::CodexRpcThread { native_thread_id } => {
                native_thread_id.clone()
            }
            CliProviderContinuation::ClaudeNew { .. }
            | CliProviderContinuation::ClaudeResume { .. } => {
                bail!("Codex CLI runtime requires a typed Codex thread continuation")
            }
        };
        let launch_projection = match &launch_spec.mcp {
            CliMcpSessionLaunch::Disabled | CliMcpSessionLaunch::ManagementOnly => None,
            CliMcpSessionLaunch::Codex(projection) => Some(projection),
            CliMcpSessionLaunch::Claude(_) => {
                bail!("Codex CLI runtime cannot consume a Claude MCP launch projection")
            }
        };
        let mcp_launch_projection = launch_projection.cloned();
        let key = process_instance.key();
        let instance = self.runtime_instance(key.runtime_id.as_str())?;
        if !instance.enabled {
            bail!("CLI runtime `{}` is disabled", instance.id);
        }
        if instance.kind != GatewayCliAgentRuntimeKindConfig::Codex {
            bail!(
                "CLI runtime `{}` is configured as unsupported kind `{:?}`",
                instance.id,
                instance.kind
            );
        }

        let mut probe_config = codex_account_probe_config_from_instance(&instance);
        probe_config.cwd = Some(
            options
                .cwd
                .clone()
                .context("Codex session requires an authorized workspace working directory")?,
        );
        validate_codex_custom_args(&instance.app_server_args)
            .context("Codex instance custom launch arguments are invalid")?;
        validate_codex_custom_args(&options.app_server_args)
            .context("Codex session custom launch arguments are invalid")?;
        let overlay_identity = codex_generation_overlay_identity(process_instance)?;
        let fallback_overlay_root = self
            .runtime_home
            .join("cli-runtime")
            .join("codex-generation-overlays");
        let (mut process_config, overlay) = codex_generation_app_server_process_config(
            &probe_config,
            fallback_overlay_root.as_path(),
            overlay_identity,
        )
        .map_err(|error| anyhow!("failed to prepare Codex generation overlay: {error}"))?;
        let mut overlay_guard = CodexGenerationOverlayStartupGuard::new(overlay);
        let mut prepared_mcp_bridge = None;
        let mut mcp_attestation = CodexMcpAttestationExpectation::unmanaged_empty(
            self.mcp_limits.max_codex_config_origins(),
        )?;
        if let Some(launch_projection) = launch_projection {
            let setup = async {
                let prepared = if launch_projection.preflight.tools.is_empty() {
                    None
                } else {
                    let projection = launch_projection
                        .facade_projection(self.mcp_limits.facade_projection_limits())
                        .map_err(|error| {
                            anyhow!("failed to build Codex facade projection: {error}")
                        })?;
                    let manifest_hash = CliMcpManifestHash::new(
                        launch_projection.preflight.canonical_manifest_hash.clone(),
                    )
                    .map_err(|error| anyhow!("invalid Codex MCP launch manifest: {error:?}"))?;
                    let scope = CliMcpGrantScope::new(process_instance.clone(), manifest_hash);
                    let overlay_bootstrap_root = overlay_guard
                        .descriptor()
                        .effective_home_path
                        .join("bootstrap");
                    let launch = self
                        .bridge_supervisor
                        .prepare_in_overlay(
                            scope,
                            codex_mcp_bootstrap_expiry(instance.startup_probe_timeout_ms)?,
                            overlay_bootstrap_root.as_path(),
                        )
                        .await
                        .map_err(|error| anyhow!("failed to prepare Codex MCP bridge: {error}"))?;
                    let reservation = self
                        .bridge_supervisor
                        .coordinator()
                        .stage_projection(launch.grant_ref(), projection.fingerprint().clone())
                        .await
                        .map_err(|error| {
                            anyhow!("failed to stage Codex MCP list identity: {error:?}")
                        })?;
                    Some(CodexPreparedMcpBridge {
                        launch,
                        projection,
                        projection_generation: reservation.generation,
                        canonical_manifest_hash: launch_projection
                            .preflight
                            .canonical_manifest_hash
                            .clone(),
                        provider_contract_fingerprint: launch_projection
                            .preflight
                            .provider_contract_fingerprint
                            .clone(),
                        isolation_contract_fingerprint: launch_projection
                            .semantic_restart_fingerprint()
                            .to_owned(),
                    })
                };
                let bootstrap_path = prepared
                    .as_ref()
                    .map(|prepared| prepared.launch.bootstrap_path().to_path_buf());
                let artifact = build_codex_managed_mcp_config(
                    &launch_projection.preflight,
                    bootstrap_path.as_deref(),
                    self.mcp_limits.max_tools(),
                )
                .map_err(|error| anyhow!("failed to build managed Codex MCP config: {error}"))?;
                if artifact.semantic_restart_fingerprint
                    != launch_projection.semantic_restart_fingerprint()
                {
                    bail!("Codex MCP semantic launch identity changed during staging");
                }
                stage_codex_generation_mcp_config(
                    overlay_guard.descriptor_mut(),
                    &artifact,
                    bootstrap_path.as_deref(),
                )
                .map_err(|error| anyhow!("failed to stage managed Codex MCP config: {error}"))?;
                Ok::<_, anyhow::Error>((artifact, prepared))
            }
            .await;
            let (artifact, prepared) = match setup {
                Ok(setup) => setup,
                Err(error) => {
                    self.bridge_supervisor
                        .revoke_session(process_instance)
                        .await;
                    return Err(error);
                }
            };
            prepared_mcp_bridge = prepared;
            mcp_attestation = CodexMcpAttestationExpectation {
                server_names: (!artifact.enabled_tools.is_empty())
                    .then(|| vec!["pioneer".to_owned()])
                    .unwrap_or_default(),
                staged_mcp_servers_fingerprint: artifact.staged_mcp_servers_fingerprint.clone(),
                effective_mcp_servers_fingerprint: artifact
                    .effective_mcp_servers_fingerprint
                    .clone(),
                requires_staged_artifact: true,
                max_config_origins: self.mcp_limits.max_codex_config_origins(),
            };
        }
        process_config = process_config
            .with_process_generation(process_instance.generation())
            .context("failed to bind Codex process generation")?;
        process_config.args.extend(instance.app_server_args.clone());
        process_config.args.extend(options.app_server_args.clone());
        process_config = process_config.with_environment(&options.env);
        let mut process = spawn_cli_agent_process(&process_config).with_context(|| {
            format!(
                "failed to spawn Codex app-server for CLI runtime `{}`",
                instance.id
            )
        })?;
        let stderr = process.stderr();
        let required_mcp_bridge = if let Some(prepared) = prepared_mcp_bridge {
            let provider_process_id = match process.id() {
                Some(process_id) if process_id != 0 => process_id,
                _ => {
                    cleanup_failed_codex_startup(
                        &self.bridge_supervisor,
                        process_instance,
                        &mut process,
                    )
                    .await;
                    bail!("Codex app-server process identity is unavailable");
                }
            };
            if let Err(error) = self
                .bridge_supervisor
                .associate_provider_process(process_instance, provider_process_id, None)
                .await
            {
                cleanup_failed_codex_startup(
                    &self.bridge_supervisor,
                    process_instance,
                    &mut process,
                )
                .await;
                bail!("failed to bind Codex MCP bridge to app-server process: {error}");
            }
            let projection_fingerprint = prepared.projection.fingerprint().clone();
            Some(Arc::new(CodexRequiredMcpBridge {
                supervisor: self.bridge_supervisor.clone(),
                process_instance: process_instance.clone(),
                launch: prepared.launch,
                projection: prepared.projection,
                projection_generation: prepared.projection_generation,
                projection_fingerprint,
                canonical_manifest_hash: prepared.canonical_manifest_hash,
                provider_contract_fingerprint: prepared.provider_contract_fingerprint,
                isolation_contract_fingerprint: prepared.isolation_contract_fingerprint,
                invoker: self.turn_mcp_invoker.clone(),
                facade_limits: self.mcp_limits.facade_limits(),
                native_items: Arc::new(CodexNativeMcpCorrelationLedger::default()),
                state: tokio::sync::Mutex::new(CodexRequiredMcpBridgeState::Pending),
            }))
        } else {
            None
        };
        let rpc_setup = (|| -> Result<_> {
            let (stdout, stdin) = process.take_stdio()?;
            let rpc = CodexJsonlRpcClient::new_with_channel_capacity_and_budget(
                BufReader::new(stdout),
                stdin,
                instance.event_channel_capacity,
                instance.event_channel_capacity,
                instance.event_channel_capacity,
                launch_spec.native_event_budget,
            );
            let notifications = rpc
                .take_notification_receiver()
                .ok_or_else(|| anyhow!("Codex notification receiver was already taken"))?;
            let server_requests = rpc
                .take_server_request_receiver()
                .ok_or_else(|| anyhow!("Codex server request receiver was already taken"))?;
            let diagnostics = rpc
                .take_diagnostic_receiver()
                .ok_or_else(|| anyhow!("Codex diagnostic receiver was already taken"))?;
            Ok((rpc, notifications, server_requests, diagnostics))
        })();
        let (rpc, notifications, server_requests, diagnostics) = match rpc_setup {
            Ok(setup) => setup,
            Err(error) => {
                if let Some(bridge) = required_mcp_bridge.as_ref() {
                    bridge.fail_closed().await;
                }
                cleanup_failed_codex_startup(
                    &self.bridge_supervisor,
                    process_instance,
                    &mut process,
                )
                .await;
                return Err(error);
            }
        };
        let client = CodexAppServerClient::new(rpc);
        if let Err(error) = client
            .initialize(Duration::from_millis(instance.startup_probe_timeout_ms))
            .await
        {
            if let Some(bridge) = required_mcp_bridge.as_ref() {
                bridge.fail_closed().await;
            }
            cleanup_failed_codex_startup(&self.bridge_supervisor, process_instance, &mut process)
                .await;
            return Err(anyhow!(error)).context("Codex initialize handshake failed");
        }
        let generation_overlay = overlay_guard.disarm();

        Ok(Arc::new(CodexCLIAgentRuntimeSession {
            client,
            process: tokio::sync::Mutex::new(process),
            request_timeout: Duration::from_millis(instance.request_timeout_ms),
            shutdown_grace: Duration::from_secs(2),
            event_receivers: StdMutex::new(Some(CLIAgentRuntimeCodexEventReceivers {
                process_instance: process_instance.clone(),
                native_event_budget: launch_spec.native_event_budget,
                notifications,
                server_requests,
                diagnostics,
            })),
            _stderr: stderr,
            generation_overlay: StdMutex::new(Some(generation_overlay)),
            required_mcp_bridge,
            mcp_attestation,
            mcp_launch_projection,
            continuation: tokio::sync::Mutex::new(CodexThreadContinuationState {
                expected_native_thread_id,
                bound_native_thread_id: None,
            }),
        }))
    }
}

async fn cleanup_failed_codex_startup(
    supervisor: &CliMcpBridgeSupervisor,
    process_instance: &CliSessionInstanceId,
    process: &mut CLIAgentProcess,
) {
    supervisor.revoke_session(process_instance).await;
    let _ = process.terminate_with_grace(Duration::from_secs(2)).await;
}

fn codex_mcp_bootstrap_expiry(startup_timeout_ms: u64) -> Result<u64> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_millis();
    let ttl_ms = u128::from(startup_timeout_ms.max(10_000)).saturating_add(60_000);
    let expiry = now
        .checked_add(ttl_ms)
        .ok_or_else(|| anyhow!("Codex MCP bootstrap expiry overflow"))?;
    u64::try_from(expiry).context("Codex MCP bootstrap expiry exceeds u64")
}

fn codex_generation_overlay_identity(
    process_instance: &CliSessionInstanceId,
) -> Result<CodexGenerationOverlayIdentity> {
    let key = process_instance.key();
    CodexGenerationOverlayIdentity::new(
        key.workspace_id.clone(),
        key.runtime_id.clone(),
        key.thread_id.clone(),
        process_instance.boot_id().as_str(),
        process_instance.generation(),
    )
    .map_err(|error| anyhow!("failed to identify Codex generation overlay: {error}"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CodexIsolationAttestationFailureKind {
    InvalidCwd,
    ConfigRead,
    NativeMcpPresent,
    ExtensionConfigPresent,
    IsolationFeatureEnabled,
    ForbiddenLayerContribution,
    PersistedThreadMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CodexIsolationAttestationError {
    kind: CodexIsolationAttestationFailureKind,
    detail: String,
}

impl CodexIsolationAttestationError {
    fn new(kind: CodexIsolationAttestationFailureKind, detail: impl Into<String>) -> Self {
        Self {
            kind,
            detail: detail.into(),
        }
    }
}

impl fmt::Display for CodexIsolationAttestationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Codex pre-thread isolation attestation failed ({:?}): {}",
            self.kind, self.detail
        )
    }
}

impl Error for CodexIsolationAttestationError {}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CodexMcpAttestationExpectation {
    server_names: Vec<String>,
    staged_mcp_servers_fingerprint: String,
    effective_mcp_servers_fingerprint: String,
    requires_staged_artifact: bool,
    max_config_origins: usize,
}

impl CodexMcpAttestationExpectation {
    fn unmanaged_empty(max_config_origins: usize) -> Result<Self> {
        if max_config_origins == 0 {
            bail!("Codex config origin budget must be greater than zero");
        }
        Ok(Self {
            server_names: Vec::new(),
            staged_mcp_servers_fingerprint: codex_config_value_fingerprint(&serde_json::json!({}))
                .map_err(|error| {
                    anyhow!("failed to fingerprint empty Codex MCP config: {error}")
                })?,
            effective_mcp_servers_fingerprint: codex_config_value_fingerprint(&serde_json::json!(
                {}
            ))
            .map_err(|error| anyhow!("failed to fingerprint empty Codex MCP config: {error}"))?,
            requires_staged_artifact: false,
            max_config_origins,
        })
    }
}

async fn start_codex_thread_with_exact_isolation(
    client: &CodexAppServerClient,
    params: CLIAgentRuntimeThreadOpenParams,
    expectation: &CodexMcpAttestationExpectation,
    overlay: Option<&CodexGenerationOverlayDescriptor>,
    required_bridge: Option<&CodexRequiredMcpBridge>,
    timeout: Duration,
) -> Result<CodexThreadOpenSnapshot> {
    let cwd = match validate_codex_attestation_cwd(params.cwd.as_str()) {
        Ok(cwd) => cwd,
        Err(error) => {
            if let Some(required_bridge) = required_bridge {
                required_bridge.fail_closed().await;
            }
            return Err(error.into());
        }
    };
    if let Err(error) =
        attest_codex_exact_isolation(client, cwd.as_str(), expectation, overlay, timeout).await
    {
        if let Some(required_bridge) = required_bridge {
            required_bridge.fail_closed().await;
        }
        return Err(error.into());
    }
    let open_params = codex_read_only_thread_open_params(params, cwd);
    let Some(required_bridge) = required_bridge else {
        return client
            .thread_start(open_params, timeout)
            .await
            .context("Codex thread/start failed after isolation attestation");
    };
    let outcome = tokio::try_join!(
        async {
            client
                .thread_start(open_params, timeout)
                .await
                .context("Codex thread/start failed after isolation attestation")
        },
        required_bridge.ensure_ready(timeout),
    );
    match outcome {
        Ok((opened, ())) => Ok(opened),
        Err(error) => {
            required_bridge.fail_closed().await;
            Err(error)
        }
    }
}

async fn resume_codex_thread_with_exact_isolation(
    client: &CodexAppServerClient,
    native_thread_id: &str,
    params: CLIAgentRuntimeThreadOpenParams,
    expectation: &CodexMcpAttestationExpectation,
    overlay: Option<&CodexGenerationOverlayDescriptor>,
    required_bridge: Option<&CodexRequiredMcpBridge>,
    timeout: Duration,
) -> Result<CodexThreadOpenSnapshot> {
    let persisted = match client
        .thread_read_metadata(native_thread_id, timeout)
        .await
        .map_err(|_| {
            CodexIsolationAttestationError::new(
                CodexIsolationAttestationFailureKind::PersistedThreadMetadata,
                "persisted native-thread cwd is unavailable or malformed",
            )
        }) {
        Ok(persisted) => persisted,
        Err(error) => {
            if let Some(required_bridge) = required_bridge {
                required_bridge.fail_closed().await;
            }
            return Err(error.into());
        }
    };
    let persisted_cwd = match validate_codex_attestation_cwd(persisted.cwd.as_str()) {
        Ok(cwd) => cwd,
        Err(error) => {
            if let Some(required_bridge) = required_bridge {
                required_bridge.fail_closed().await;
            }
            return Err(error.into());
        }
    };
    let stable_rollout_path = if let Some(persisted_path) = persisted.rollout_path.as_deref() {
        let resolved = overlay
            .ok_or_else(|| {
                anyhow!("Codex generation overlay is unavailable for rollout resolution")
            })
            .and_then(|overlay| {
                recover_codex_stale_rollout_path(overlay, persisted_path).map_err(|error| {
                    anyhow!("failed to resolve stable Codex rollout path: {error}")
                })
            });
        match resolved {
            Ok(path) => path,
            Err(error) => {
                if let Some(required_bridge) = required_bridge {
                    required_bridge.fail_closed().await;
                }
                return Err(error);
            }
        }
    } else {
        None
    };
    if let Err(error) = attest_codex_exact_isolation(
        client,
        persisted_cwd.as_str(),
        expectation,
        overlay,
        timeout,
    )
    .await
    {
        if let Some(required_bridge) = required_bridge {
            required_bridge.fail_closed().await;
        }
        return Err(error.into());
    }
    let open_params = codex_read_only_thread_open_params(params, persisted_cwd);
    let expected_cwd = open_params.cwd.clone();
    let resume_result = match required_bridge {
        None => client
            .thread_resume_at_path(native_thread_id, open_params, stable_rollout_path, timeout)
            .await
            .context("Codex thread/resume failed after isolation attestation"),
        Some(required_bridge) => tokio::try_join!(
            async {
                client
                    .thread_resume_at_path(
                        native_thread_id,
                        open_params,
                        stable_rollout_path,
                        timeout,
                    )
                    .await
                    .context("Codex thread/resume failed after isolation attestation")
            },
            required_bridge.ensure_ready(timeout),
        )
        .map(|(opened, ())| opened),
    };
    let opened = match resume_result {
        Ok(opened) => opened,
        Err(error) => {
            if let Some(required_bridge) = required_bridge {
                required_bridge.fail_closed().await;
            }
            return Err(error);
        }
    };
    match verify_oversized_codex_resume(
        client,
        opened,
        native_thread_id,
        expected_cwd.as_str(),
        timeout,
    )
    .await
    {
        Ok(opened) => Ok(opened),
        Err(error) => {
            if let Some(required_bridge) = required_bridge {
                required_bridge.fail_closed().await;
            }
            Err(error)
        }
    }
}

async fn verify_oversized_codex_resume(
    client: &CodexAppServerClient,
    mut opened: CodexThreadOpenSnapshot,
    expected_native_thread_id: &str,
    expected_cwd: &str,
    timeout: Duration,
) -> Result<CodexThreadOpenSnapshot> {
    if !opened.response_was_oversized {
        return Ok(opened);
    }
    let verified = client
        .thread_read_metadata(expected_native_thread_id, timeout)
        .await
        .context("Codex thread/read failed after discarding oversized thread/resume history")?;
    if verified.native_thread_id != expected_native_thread_id || verified.cwd != expected_cwd {
        bail!("Codex thread/resume post-verification returned mismatched native-thread metadata");
    }
    opened.native_thread_id = verified.native_thread_id.clone();
    opened.cwd = Some(verified.cwd.clone());
    opened.raw = serde_json::json!({
        "thread": {
            "id": verified.native_thread_id,
            "cwd": verified.cwd,
            "path": verified.rollout_path,
        }
    });
    opened.response_was_oversized = false;
    Ok(opened)
}

async fn attest_codex_exact_isolation(
    client: &CodexAppServerClient,
    cwd: &str,
    expectation: &CodexMcpAttestationExpectation,
    overlay: Option<&CodexGenerationOverlayDescriptor>,
    timeout: Duration,
) -> Result<(), CodexIsolationAttestationError> {
    if expectation.requires_staged_artifact {
        let overlay = overlay.ok_or_else(|| {
            CodexIsolationAttestationError::new(
                CodexIsolationAttestationFailureKind::ConfigRead,
                "managed config overlay evidence is unavailable",
            )
        })?;
        verify_codex_generation_mcp_config(overlay).map_err(|error| {
            CodexIsolationAttestationError::new(
                CodexIsolationAttestationFailureKind::ConfigRead,
                format!("staged managed config digest or generation fence did not match: {error}"),
            )
        })?;
    }
    let snapshot = client
        .config_read(cwd, expectation.max_config_origins, timeout)
        .await
        .map_err(|error| {
            CodexIsolationAttestationError::new(
                CodexIsolationAttestationFailureKind::ConfigRead,
                format!(
                    "read-only effective config evidence was unavailable or malformed: {error}"
                ),
            )
        })?;
    validate_codex_exact_isolation_snapshot(&snapshot, expectation)
}

fn validate_codex_exact_isolation_snapshot(
    snapshot: &CodexConfigReadSnapshot,
    expectation: &CodexMcpAttestationExpectation,
) -> Result<(), CodexIsolationAttestationError> {
    if snapshot.effective.mcp_server_names != expectation.server_names
        || snapshot.effective.mcp_servers_fingerprint
            != expectation.effective_mcp_servers_fingerprint
    {
        return Err(CodexIsolationAttestationError::new(
            CodexIsolationAttestationFailureKind::NativeMcpPresent,
            "effective native MCP configuration does not match the staged exact projection",
        ));
    }
    if snapshot.effective.has_forbidden_extension_entries() {
        return Err(CodexIsolationAttestationError::new(
            CodexIsolationAttestationFailureKind::ExtensionConfigPresent,
            "effective plugin, marketplace, app, or project-trust config is not empty",
        ));
    }
    if !snapshot.effective.isolation_features.all_disabled() {
        return Err(CodexIsolationAttestationError::new(
            CodexIsolationAttestationFailureKind::IsolationFeatureEnabled,
            "an MCP-capable app, plugin, or skill installation feature is enabled",
        ));
    }
    let mut mcp_layers = snapshot
        .layers
        .iter()
        .filter(|layer| !layer.mcp_server_names.is_empty());
    let mcp_layers_match = if expectation.server_names.is_empty() {
        mcp_layers.next().is_none()
    } else {
        mcp_layers.next().is_some_and(|layer| {
            layer.mcp_server_names == expectation.server_names
                && layer.mcp_servers_fingerprint == expectation.staged_mcp_servers_fingerprint
        }) && mcp_layers.next().is_none()
    };
    let extension_layer_present = snapshot.layers.iter().any(|layer| {
        !layer.plugin_names.is_empty()
            || !layer.marketplace_names.is_empty()
            || !layer.app_names.is_empty()
            || !layer.trusted_project_names.is_empty()
    });
    if !mcp_layers_match || extension_layer_present {
        return Err(CodexIsolationAttestationError::new(
            CodexIsolationAttestationFailureKind::ForbiddenLayerContribution,
            "a user, project, system, or session layer contributes unmanaged or mismatched MCP-capable config",
        ));
    }
    Ok(())
}

fn validate_codex_attestation_cwd(cwd: &str) -> Result<String, CodexIsolationAttestationError> {
    let cwd = cwd.trim();
    if cwd.is_empty() || !PathBuf::from(cwd).is_absolute() {
        return Err(CodexIsolationAttestationError::new(
            CodexIsolationAttestationFailureKind::InvalidCwd,
            "effective cwd must be a non-empty absolute path",
        ));
    }
    Ok(cwd.to_owned())
}

fn codex_read_only_thread_open_params(
    params: CLIAgentRuntimeThreadOpenParams,
    effective_cwd: String,
) -> CodexThreadStartParams {
    CodexThreadStartParams {
        cwd: effective_cwd,
        approval_policy: params
            .approval_policy
            .unwrap_or_else(|| "default".to_owned()),
        ephemeral: false,
        sandbox: Some("read-only".to_owned()),
        permissions: None,
        model: params.model,
        service_tier: params.service_tier,
    }
}

fn codex_turn_collaboration_mode(
    params: &CLIAgentRuntimeTurnStartParams,
) -> Result<CodexCollaborationMode> {
    let instructions = &params.elevated_instructions;
    let model = params
        .model
        .as_deref()
        .map(str::trim)
        .filter(|model| !model.is_empty())
        .context("Codex turn/start cannot deliver elevated instructions without a model")?;
    Ok(CodexCollaborationMode::default_with_developer_instructions(
        model.to_owned(),
        params.effort.clone(),
        instructions.text().to_owned(),
    ))
}

fn normalize_codex_turn_sandbox_policy(value: Option<JsonValue>) -> Result<Option<JsonValue>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let mode = match &value {
        JsonValue::String(mode) => Some(mode.as_str()),
        JsonValue::Object(object) => {
            if let Some(policy_type) = object.get("type").and_then(JsonValue::as_str) {
                if matches!(
                    policy_type,
                    "readOnly" | "workspaceWrite" | "dangerFullAccess" | "externalSandbox"
                ) {
                    return Ok(Some(value));
                }
                bail!("unsupported Codex turn sandbox policy type");
            }
            (object.len() == 1)
                .then(|| object.get("mode").and_then(JsonValue::as_str))
                .flatten()
        }
        _ => bail!("invalid Codex turn sandbox policy"),
    }
    .ok_or_else(|| anyhow!("Codex turn sandbox policy has no mode"))?;
    let policy = match mode {
        "read-only" | "readOnly" => serde_json::json!({
            "type": "readOnly",
            "networkAccess": false,
        }),
        "workspace-write" | "workspaceWrite" => serde_json::json!({
            "type": "workspaceWrite",
            "writableRoots": [],
            "networkAccess": false,
            "excludeTmpdirEnvVar": false,
            "excludeSlashTmp": false,
        }),
        "danger-full-access" | "dangerFullAccess" => {
            serde_json::json!({"type": "dangerFullAccess"})
        }
        "external-sandbox" | "externalSandbox" => serde_json::json!({
            "type": "externalSandbox",
            "networkAccess": "restricted",
        }),
        _ => bail!("unsupported Codex turn sandbox mode"),
    };
    Ok(Some(policy))
}

impl CodexCLIAgentRuntimeSessionFactory {
    fn runtime_instance(
        &self,
        runtime_id: &str,
    ) -> Result<EffectiveGatewayCliAgentRuntimeInstanceConfig> {
        load_effective_cli_runtime_instances(self.runtime_home.as_path())?
            .into_iter()
            .find(|instance| instance.id == runtime_id)
            .ok_or_else(|| anyhow!("unknown CLI runtime `{runtime_id}`"))
    }
}

struct CodexCLIAgentRuntimeSession {
    client: CodexAppServerClient,
    process: tokio::sync::Mutex<CLIAgentProcess>,
    request_timeout: Duration,
    shutdown_grace: Duration,
    event_receivers: StdMutex<Option<CLIAgentRuntimeCodexEventReceivers>>,
    _stderr: pioneer_cli_agent_runtime::process::StderrRing,
    generation_overlay: StdMutex<Option<CodexGenerationOverlayDescriptor>>,
    required_mcp_bridge: Option<Arc<CodexRequiredMcpBridge>>,
    mcp_attestation: CodexMcpAttestationExpectation,
    mcp_launch_projection: Option<CodexMcpSessionLaunchProjection>,
    continuation: tokio::sync::Mutex<CodexThreadContinuationState>,
}

struct CodexThreadContinuationState {
    expected_native_thread_id: Option<String>,
    bound_native_thread_id: Option<String>,
}

async fn probe_codex_turn_liveness(
    client: &CodexAppServerClient,
    native_thread_id: &str,
    timeout: Duration,
) -> Result<CLIAgentRuntimeTurnLivenessProbe> {
    let snapshot = client
        .thread_read_raw(native_thread_id, false, timeout)
        .await
        .context("Codex thread/read liveness probe failed")?;
    if snapshot.pointer("/thread/id").and_then(JsonValue::as_str) != Some(native_thread_id) {
        bail!("Codex thread/read liveness probe returned a different native thread id");
    }
    Ok(
        match snapshot
            .pointer("/thread/status/type")
            .and_then(JsonValue::as_str)
        {
            Some("active") => CLIAgentRuntimeTurnLivenessProbe::ConfirmedActive,
            Some("idle" | "notLoaded" | "systemError") => {
                CLIAgentRuntimeTurnLivenessProbe::SnapshotRequired
            }
            _ => CLIAgentRuntimeTurnLivenessProbe::Unavailable,
        },
    )
}

async fn load_codex_turn_snapshot(
    client: &CodexAppServerClient,
    native_thread_id: &str,
    native_turn_id: &str,
    timeout: Duration,
) -> Result<Option<CLIAgentRuntimeTurnObservation>> {
    let snapshot = client
        .thread_read_turn_snapshot_raw(native_thread_id, native_turn_id, timeout)
        .await
        .context("Codex thread/read terminal snapshot failed")?;
    codex_turn_observation_from_snapshot(&snapshot, native_thread_id, native_turn_id)
}

fn codex_turn_observation_from_snapshot(
    snapshot: &JsonValue,
    native_thread_id: &str,
    native_turn_id: &str,
) -> Result<Option<CLIAgentRuntimeTurnObservation>> {
    if snapshot.pointer("/thread/id").and_then(JsonValue::as_str) != Some(native_thread_id) {
        bail!("Codex terminal snapshot returned a different native thread id");
    }
    let Some(turn) = snapshot
        .pointer("/thread/turns")
        .and_then(JsonValue::as_array)
        .and_then(|turns| {
            turns
                .iter()
                .find(|turn| turn.get("id").and_then(JsonValue::as_str) == Some(native_turn_id))
        })
    else {
        return Ok(None);
    };
    let status = match turn.get("status").and_then(JsonValue::as_str) {
        Some("inProgress" | "in_progress") => CLIAgentRuntimeObservedTurnStatus::InProgress,
        Some("completed") => CLIAgentRuntimeObservedTurnStatus::Completed,
        Some("failed") => CLIAgentRuntimeObservedTurnStatus::Failed,
        Some("blocked") => CLIAgentRuntimeObservedTurnStatus::Blocked,
        Some("interrupted") => CLIAgentRuntimeObservedTurnStatus::Interrupted,
        _ => return Ok(None),
    };
    let message = turn
        .pointer("/error/message")
        .and_then(JsonValue::as_str)
        .map(str::to_owned);
    let reconciliation_events = if status == CLIAgentRuntimeObservedTurnStatus::InProgress {
        Vec::new()
    } else {
        turn.get("items")
            .and_then(JsonValue::as_array)
            .into_iter()
            .flatten()
            .map(|item| {
                let params = serde_json::json!({
                    "threadId": native_thread_id,
                    "turnId": native_turn_id,
                    "item": item,
                });
                map_codex_notification_event(
                    &CodexJsonlRpcNotificationEvent {
                        method: "item/completed".to_owned(),
                        params: Some(params.clone()),
                        raw: serde_json::json!({
                            "method": "item/completed",
                            "params": params,
                        }),
                    },
                    RuntimeEventMappingOptions::default(),
                )
            })
            .collect()
    };
    Ok(Some(CLIAgentRuntimeTurnObservation {
        status,
        message,
        reconciliation_events,
    }))
}

#[async_trait]
impl CLIAgentRuntimeSession for CodexCLIAgentRuntimeSession {
    async fn close(&self) -> Result<()> {
        if let Some(bridge) = self.required_mcp_bridge.as_ref() {
            bridge.fail_closed().await;
        }
        if let Err(error) = self.client.rpc().shutdown().await {
            tracing::warn!(
                error = %format!("{error:#}"),
                "failed to request Codex CLI runtime shutdown"
            );
        }
        let mut process = self.process.lock().await;
        let _ = process.terminate_with_grace(self.shutdown_grace).await?;
        drop(process);
        let overlay = self
            .generation_overlay
            .lock()
            .expect("Codex generation overlay mutex should not be poisoned")
            .clone();
        if let Some(overlay) = overlay {
            cleanup_codex_generation_overlay(&overlay)
                .map_err(|error| anyhow!("failed to clean up Codex generation overlay: {error}"))?;
            self.generation_overlay
                .lock()
                .expect("Codex generation overlay mutex should not be poisoned")
                .take();
        }
        Ok(())
    }

    fn take_codex_event_receivers(&self) -> Option<CLIAgentRuntimeCodexEventReceivers> {
        self.event_receivers
            .lock()
            .expect("Codex event receiver mutex should not be poisoned")
            .take()
    }

    fn supports_thread_name_sync(&self) -> bool {
        true
    }

    async fn start_thread(
        &self,
        params: CLIAgentRuntimeThreadOpenParams,
        timeout: Duration,
    ) -> Result<CLIAgentRuntimeThreadOpenSnapshot> {
        let mut continuation = self.continuation.lock().await;
        if continuation.expected_native_thread_id.is_some()
            || continuation.bound_native_thread_id.is_some()
        {
            bail!(
                "Codex thread/start is forbidden for a process generation prepared for resume or already bound"
            );
        }
        let overlay = self
            .generation_overlay
            .lock()
            .expect("Codex generation overlay mutex should not be poisoned")
            .clone()
            .ok_or_else(|| anyhow!("Codex generation overlay is unavailable"))?;
        let opened = start_codex_thread_with_exact_isolation(
            &self.client,
            params,
            &self.mcp_attestation,
            Some(&overlay),
            self.required_mcp_bridge.as_deref(),
            timeout,
        )
        .await
        .context("Codex isolated thread/start failed")?;
        continuation.bound_native_thread_id = Some(opened.native_thread_id.clone());
        Ok(CLIAgentRuntimeThreadOpenSnapshot {
            native_thread_id: opened.native_thread_id,
            cwd: opened.cwd,
            model: opened.model,
            raw: opened.raw,
        })
    }

    async fn resume_thread(
        &self,
        native_thread_id: &str,
        params: CLIAgentRuntimeThreadOpenParams,
        timeout: Duration,
    ) -> Result<CLIAgentRuntimeThreadOpenSnapshot> {
        let mut continuation = self.continuation.lock().await;
        let expected_native_thread_id = continuation
            .bound_native_thread_id
            .as_ref()
            .or(continuation.expected_native_thread_id.as_ref())
            .ok_or_else(|| {
                anyhow!(
                    "Codex thread/resume is forbidden before a typed native continuation is bound"
                )
            })?;
        if expected_native_thread_id != native_thread_id {
            bail!(
                "Codex thread/resume requested native thread `{native_thread_id}` but process generation is fenced to `{expected_native_thread_id}`"
            );
        }
        let overlay = self
            .generation_overlay
            .lock()
            .expect("Codex generation overlay mutex should not be poisoned")
            .clone()
            .ok_or_else(|| anyhow!("Codex generation overlay is unavailable"))?;
        let opened = resume_codex_thread_with_exact_isolation(
            &self.client,
            native_thread_id,
            params,
            &self.mcp_attestation,
            Some(&overlay),
            self.required_mcp_bridge.as_deref(),
            timeout,
        )
        .await
        .context("Codex isolated thread/resume failed")?;
        continuation.bound_native_thread_id = Some(opened.native_thread_id.clone());
        Ok(CLIAgentRuntimeThreadOpenSnapshot {
            native_thread_id: opened.native_thread_id,
            cwd: opened.cwd,
            model: opened.model,
            raw: opened.raw,
        })
    }

    async fn start_turn(
        &self,
        params: CLIAgentRuntimeTurnStartParams,
        timeout: Duration,
    ) -> Result<CLIAgentRuntimeTurnStartSnapshot> {
        if let Some(bridge) = self.required_mcp_bridge.as_ref() {
            bridge
                .ensure_ready(timeout)
                .await
                .context("Codex turn/start blocked by required MCP readiness")?;
        }
        let collaboration_mode = codex_turn_collaboration_mode(&params)?;
        let input = serde_json::from_value(params.input)
            .context("failed to decode generic CLI runtime input for Codex")?;
        let sandbox_policy = if params.permissions.is_none() {
            normalize_codex_turn_sandbox_policy(params.sandbox)?
        } else {
            None
        };
        let started = self
            .client
            .turn_start(
                CodexTurnStartParams {
                    thread_id: params.native_thread_id,
                    input,
                    cwd: params.cwd,
                    approval_policy: params.approval_policy,
                    sandbox_policy,
                    permissions: params.permissions,
                    model: params.model,
                    effort: params.effort,
                    personality: params.personality,
                    summary: params.summary,
                    collaboration_mode: Some(collaboration_mode),
                },
                timeout,
            )
            .await
            .context("Codex turn/start failed")?;
        Ok(CLIAgentRuntimeTurnStartSnapshot {
            native_thread_id: started.native_thread_id,
            native_turn_id: started.native_turn_id,
            raw: started.raw,
        })
    }

    async fn prepare_mcp_turn(
        &self,
        pioneer_thread_id: &str,
        pioneer_turn_id: &str,
    ) -> Result<Option<CLIAgentRuntimeMcpTurnMetadata>> {
        let Some(bridge) = self.required_mcp_bridge.as_ref() else {
            return Ok(None);
        };
        bridge
            .prepare_turn(pioneer_thread_id, pioneer_turn_id)
            .await
            .map(Some)
    }

    async fn activate_mcp_turn(
        &self,
        pioneer_turn_id: &str,
        native_thread_id: &str,
        native_turn_id: &str,
    ) -> Result<()> {
        let Some(bridge) = self.required_mcp_bridge.as_ref() else {
            return Ok(());
        };
        bridge
            .activate_turn(pioneer_turn_id, native_thread_id, native_turn_id)
            .await
    }

    async fn retarget_mcp_turn(
        &self,
        pioneer_turn_id: &str,
        native_thread_id: &str,
        native_turn_id: &str,
    ) -> Result<()> {
        let Some(bridge) = self.required_mcp_bridge.as_ref() else {
            return Ok(());
        };
        bridge
            .retarget_turn(pioneer_turn_id, native_thread_id, native_turn_id)
            .await
    }

    async fn terminal_mcp_turn(&self, pioneer_turn_id: &str) -> Result<()> {
        let Some(bridge) = self.required_mcp_bridge.as_ref() else {
            return Ok(());
        };
        bridge.terminal_turn(pioneer_turn_id).await
    }

    async fn reset_native_thread_goal(&self, native_thread_id: &str) -> Result<()> {
        if self
            .client
            .thread_goal_get(native_thread_id, self.request_timeout)
            .await
            .context("Codex thread/goal/get failed")?
            .is_some()
        {
            self.client
                .thread_goal_clear(native_thread_id, self.request_timeout)
                .await
                .context("Codex thread/goal/clear failed")?;
        }
        Ok(())
    }

    async fn clear_native_thread_goal(&self, native_thread_id: &str) -> Result<()> {
        self.client
            .thread_goal_clear(native_thread_id, self.request_timeout)
            .await
            .context("Codex thread/goal/clear failed")?;
        Ok(())
    }

    async fn native_mcp_approval_response(
        &self,
        request: CLIAgentRuntimeNativeMcpApprovalRequest,
    ) -> Result<Option<JsonValue>> {
        let Some(bridge) = self.required_mcp_bridge.as_ref() else {
            return Ok(None);
        };
        bridge.native_approval_response(request).await
    }

    fn enrich_runtime_event(&self, event: &mut RuntimeEvent) -> Result<()> {
        let Some(projection) = self.mcp_launch_projection.as_ref() else {
            if is_codex_native_mcp_event(event) {
                bail!("unmanaged Codex MCP lifecycle event was rejected")
            }
            return Ok(());
        };
        let binding = projection
            .enrich_native_event(
                self.required_mcp_bridge
                    .as_ref()
                    .map(|bridge| bridge.process_instance.key().runtime_id.as_str())
                    .unwrap_or("codex"),
                self.required_mcp_bridge
                    .as_ref()
                    .map(|bridge| bridge.process_instance.generation())
                    .unwrap_or_default(),
                event,
            )
            .map_err(|error| anyhow!("invalid Codex MCP lifecycle event: {error}"))?;
        if let Some(binding) = binding {
            let bridge = self
                .required_mcp_bridge
                .as_ref()
                .ok_or_else(|| anyhow!("Codex MCP event has no managed bridge generation"))?;
            bridge.register_native_item(binding)?;
        } else if is_codex_native_mcp_event(event) {
            let bridge = self
                .required_mcp_bridge
                .as_ref()
                .ok_or_else(|| anyhow!("Codex MCP progress has no managed bridge generation"))?;
            let callable = bridge.native_item_callable(event).ok_or_else(|| {
                anyhow!("Codex MCP progress does not match a registered native item")
            })?;
            projection
                .enrich_native_progress(
                    bridge.process_instance.key().runtime_id.as_str(),
                    bridge.process_instance.generation(),
                    callable.as_str(),
                    event,
                )
                .map_err(|error| anyhow!("invalid Codex MCP progress event: {error}"))?;
        }
        Ok(())
    }

    async fn respond_to_request(
        &self,
        native_request_id: JsonValue,
        response: JsonValue,
    ) -> Result<()> {
        let id: JsonlRpcId = serde_json::from_value(native_request_id)
            .context("failed to decode Codex native request id")?;
        self.client
            .rpc()
            .respond_to_server_request(id, response)
            .await
            .context("failed to respond to Codex server request")
    }

    async fn fail_request(
        &self,
        native_request_id: JsonValue,
        code: i64,
        message: String,
        data: Option<JsonValue>,
    ) -> Result<()> {
        let id: JsonlRpcId = serde_json::from_value(native_request_id)
            .context("failed to decode Codex native request id")?;
        self.client
            .rpc()
            .fail_server_request(id, code, message, data)
            .await
            .context("failed to reject Codex server request")
    }

    async fn interrupt_turn(
        &self,
        native_thread_id: Option<&str>,
        native_turn_id: Option<&str>,
    ) -> Result<()> {
        let native_thread_id =
            native_thread_id.ok_or_else(|| anyhow!("Codex interrupt requires native thread id"))?;
        let native_turn_id =
            native_turn_id.ok_or_else(|| anyhow!("Codex interrupt requires native turn id"))?;
        self.client
            .interrupt_turn(native_thread_id, native_turn_id, self.request_timeout)
            .await
            .context("Codex turn/interrupt failed")?;
        Ok(())
    }

    async fn probe_turn_liveness(
        &self,
        native_thread_id: &str,
        _native_turn_id: &str,
    ) -> Result<CLIAgentRuntimeTurnLivenessProbe> {
        probe_codex_turn_liveness(&self.client, native_thread_id, self.request_timeout).await
    }

    async fn load_turn_snapshot(
        &self,
        native_thread_id: &str,
        native_turn_id: &str,
    ) -> Result<Option<CLIAgentRuntimeTurnObservation>> {
        load_codex_turn_snapshot(
            &self.client,
            native_thread_id,
            native_turn_id,
            self.request_timeout,
        )
        .await
    }

    async fn set_thread_name(
        &self,
        request: CLIAgentRuntimeThreadNameSetRequest,
    ) -> Result<CLIAgentRuntimeThreadNameSetResult> {
        let snapshot = self
            .client
            .thread_name_set(
                CodexThreadNameSetParams {
                    thread_id: request.native_thread_id,
                    name: request.name,
                },
                self.request_timeout,
            )
            .await
            .context("Codex thread/name/set failed")?;
        Ok(CLIAgentRuntimeThreadNameSetResult {
            native_thread_id: snapshot.native_thread_id,
            raw: Some(snapshot.raw),
        })
    }

    async fn fork_thread(
        &self,
        request: CLIAgentRuntimeThreadForkRequest,
    ) -> Result<CLIAgentRuntimeThreadForkResult> {
        let snapshot = self
            .client
            .thread_fork(
                CodexThreadForkParams {
                    thread_id: request.native_thread_id,
                },
                self.request_timeout,
            )
            .await
            .context("Codex thread/fork failed")?;
        Ok(CLIAgentRuntimeThreadForkResult {
            native_thread_id: snapshot.native_thread_id,
            native_cwd: snapshot.cwd,
            native_model: snapshot.model,
            raw: Some(snapshot.raw),
        })
    }

    async fn steer_turn(
        &self,
        request: CLIAgentRuntimeTurnSteerRequest,
    ) -> Result<CLIAgentRuntimeTurnSteerResult> {
        let snapshot = self
            .client
            .turn_steer(
                CodexTurnSteerParams {
                    thread_id: request.native_thread_id,
                    expected_turn_id: request.native_turn_id,
                    input: vec![
                        pioneer_cli_agent_runtime::input::CLIRuntimeTurnInputItem::Text {
                            text: request.message,
                        },
                    ],
                },
                self.request_timeout,
            )
            .await
            .context("Codex turn/steer failed")?;
        Ok(CLIAgentRuntimeTurnSteerResult {
            native_thread_id: snapshot.native_thread_id,
            native_turn_id: snapshot.native_turn_id,
            raw: Some(snapshot.raw),
        })
    }
}

fn is_codex_native_mcp_event(event: &RuntimeEvent) -> bool {
    let kind = match event {
        RuntimeEvent::ItemStarted(item) => item.item_kind.as_str(),
        RuntimeEvent::ItemCompleted(item) => item.item_kind.as_str(),
        RuntimeEvent::ItemDelta(item) => item.item_kind.as_str(),
        _ => return false,
    };
    kind.chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .eq("mcptoolcall".chars())
}

struct CodexGenerationOverlayStartupGuard {
    descriptor: Option<CodexGenerationOverlayDescriptor>,
}

impl CodexGenerationOverlayStartupGuard {
    fn new(descriptor: CodexGenerationOverlayDescriptor) -> Self {
        Self {
            descriptor: Some(descriptor),
        }
    }

    fn disarm(&mut self) -> CodexGenerationOverlayDescriptor {
        self.descriptor
            .take()
            .expect("Codex generation overlay guard must be armed")
    }

    fn descriptor(&self) -> &CodexGenerationOverlayDescriptor {
        self.descriptor
            .as_ref()
            .expect("Codex generation overlay guard must be armed")
    }

    fn descriptor_mut(&mut self) -> &mut CodexGenerationOverlayDescriptor {
        self.descriptor
            .as_mut()
            .expect("Codex generation overlay guard must be armed")
    }
}

impl Drop for CodexGenerationOverlayStartupGuard {
    fn drop(&mut self) {
        let Some(descriptor) = self.descriptor.take() else {
            return;
        };
        if let Err(error) = cleanup_codex_generation_overlay(&descriptor) {
            tracing::warn!(
                error = %error,
                overlay = %descriptor.effective_home_path.display(),
                "failed to clean up Codex generation overlay after startup failure"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli_runtime::manager::CLIAgentRuntimeSessionKey;
    use crate::cli_runtime::mcp::facade::CliMcpFacadeTool;
    use crate::cli_runtime::mcp::limits::CliMcpFacadeProjectionLimits;
    use crate::cli_runtime::session_instance::CliSessionGenerationAllocator;
    use crate::turn_mcp::invoker::{
        TurnMcpInvocation, TurnMcpInvocationError, TurnMcpInvocationErrorCode,
    };
    use crate::turn_mcp::result::CanonicalMcpToolResult;
    use async_trait::async_trait;
    use pioneer_cli_agent_runtime::codex::{
        CodexAccountProbeConfig, CodexConfigIsolationEvidence, CodexConfigIsolationFeatures,
        CodexConfigLayerIsolationEvidence, CodexConfigLayerSourceKind,
    };
    use pioneer_cli_agent_runtime::instructions::CLIRuntimeElevatedInstructions;
    use pioneer_cli_agent_runtime::process::SensitiveEnvironment;
    use pioneer_cli_mcp_bridge::helper::run_hidden_helper_with_io;
    use serde_json::{Value as JsonValue, json};
    use sha2::{Digest, Sha256};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, duplex, split};
    use tokio_util::sync::CancellationToken;

    struct NeverCalledInvoker;

    #[async_trait]
    impl TurnMcpInvoker for NeverCalledInvoker {
        async fn invoke(
            &self,
            _invocation: TurnMcpInvocation,
            _cancellation: CancellationToken,
        ) -> Result<CanonicalMcpToolResult, TurnMcpInvocationError> {
            Err(TurnMcpInvocationError::new(
                TurnMcpInvocationErrorCode::Internal,
                "tools/call must remain unavailable during readiness",
            ))
        }
    }

    #[derive(Default)]
    struct RecordingProviderCallInvoker {
        provider_call_id: StdMutex<Option<String>>,
    }

    #[async_trait]
    impl TurnMcpInvoker for RecordingProviderCallInvoker {
        async fn invoke(
            &self,
            invocation: TurnMcpInvocation,
            _cancellation: CancellationToken,
        ) -> Result<CanonicalMcpToolResult, TurnMcpInvocationError> {
            *self
                .provider_call_id
                .lock()
                .expect("recording invoker should not be poisoned") =
                Some(invocation.provider_call_id);
            Err(TurnMcpInvocationError::new(
                TurnMcpInvocationErrorCode::Internal,
                "recorded",
            ))
        }
    }

    struct FakeCodexIsolationServer {
        client: CodexAppServerClient,
        reader: BufReader<tokio::io::ReadHalf<tokio::io::DuplexStream>>,
        writer: tokio::io::WriteHalf<tokio::io::DuplexStream>,
    }

    impl FakeCodexIsolationServer {
        fn new() -> Self {
            let (client_stream, server_stream) = tokio::io::duplex(64 * 1024);
            let (client_read, client_write) = split(client_stream);
            let (server_read, server_write) = split(server_stream);
            let rpc = CodexJsonlRpcClient::new(BufReader::new(client_read), client_write);
            Self {
                client: CodexAppServerClient::new(rpc),
                reader: BufReader::new(server_read),
                writer: server_write,
            }
        }

        async fn read_request(&mut self) -> JsonValue {
            let mut line = String::new();
            self.reader
                .read_line(&mut line)
                .await
                .expect("fake Codex server should read request");
            serde_json::from_str(line.trim()).expect("request should be JSON")
        }

        async fn write_result(&mut self, id: JsonValue, result: JsonValue) {
            let payload = json!({ "id": id, "result": result }).to_string();
            self.writer
                .write_all(format!("{payload}\n").as_bytes())
                .await
                .expect("fake Codex server should write response");
        }
    }

    #[tokio::test]
    async fn codex_liveness_probe_confirms_active_thread_without_loading_turns() {
        let mut fake = FakeCodexIsolationServer::new();
        let client = fake.client.clone();
        let probe = tokio::spawn(async move {
            probe_codex_turn_liveness(&client, "native-thread", Duration::from_secs(2)).await
        });

        let request = fake.read_request().await;
        assert_eq!(request["method"], json!("thread/read"));
        assert_eq!(request["params"]["threadId"], json!("native-thread"));
        assert_eq!(request["params"]["includeTurns"], json!(false));
        fake.write_result(
            request["id"].clone(),
            json!({
                "thread": {
                    "id": "native-thread",
                    "cwd": "/workspace",
                    "status": {"type": "active", "activeFlags": []}
                }
            }),
        )
        .await;

        assert_eq!(
            probe
                .await
                .expect("probe task should join")
                .expect("active probe should succeed"),
            CLIAgentRuntimeTurnLivenessProbe::ConfirmedActive
        );
    }

    #[tokio::test]
    async fn codex_inactive_probe_defers_history_to_terminal_snapshot() {
        let mut fake = FakeCodexIsolationServer::new();
        let client = fake.client.clone();
        let probe_client = client.clone();
        let probe = tokio::spawn(async move {
            probe_codex_turn_liveness(&probe_client, "native-thread", Duration::from_secs(2)).await
        });

        let request = fake.read_request().await;
        assert_eq!(request["params"]["includeTurns"], json!(false));
        fake.write_result(
            request["id"].clone(),
            json!({
                "thread": {
                    "id": "native-thread",
                    "cwd": "/workspace",
                    "status": {"type": "idle"}
                }
            }),
        )
        .await;
        assert_eq!(
            probe
                .await
                .expect("probe task should join")
                .expect("idle probe should succeed"),
            CLIAgentRuntimeTurnLivenessProbe::SnapshotRequired
        );

        let snapshot = tokio::spawn(async move {
            load_codex_turn_snapshot(
                &client,
                "native-thread",
                "native-turn",
                Duration::from_secs(2),
            )
            .await
        });
        let request = fake.read_request().await;
        assert_eq!(request["method"], json!("thread/read"));
        assert_eq!(request["params"]["includeTurns"], json!(true));
        fake.write_result(
            request["id"].clone(),
            json!({
                "thread": {
                    "id": "native-thread",
                    "turns": [{
                        "id": "native-turn",
                        "status": "completed",
                        "items": [{
                            "id": "message",
                            "type": "agentMessage",
                            "text": "done"
                        }]
                    }]
                }
            }),
        )
        .await;
        let observation = snapshot
            .await
            .expect("snapshot task should join")
            .expect("terminal snapshot should load")
            .expect("native Turn should exist");
        assert_eq!(
            observation.status,
            CLIAgentRuntimeObservedTurnStatus::Completed
        );
        assert_eq!(observation.reconciliation_events.len(), 1);
    }

    fn safe_empty_config_read_result() -> JsonValue {
        json!({
            "config": {
                "mcp_servers": {},
                "plugins": {},
                "marketplaces": {},
                "apps": null,
                "projects": null,
                "features": {
                    "apps": false,
                    "enable_mcp_apps": false,
                    "plugins": false,
                    "remote_plugin": false,
                    "skill_mcp_dependency_install": false
                }
            },
            "origins": {},
            "layers": [{
                "name": { "type": "user", "file": "/managed/config.toml", "profile": null },
                "version": "sha256:managed",
                "config": {
                    "features": {
                        "apps": false,
                        "enable_mcp_apps": false,
                        "plugins": false,
                        "remote_plugin": false,
                        "skill_mcp_dependency_install": false
                    }
                }
            }]
        })
    }

    fn exact_pioneer_config_read_result(mcp_servers: JsonValue) -> JsonValue {
        json!({
            "config": {
                "mcp_servers": mcp_servers.clone(),
                "plugins": {},
                "marketplaces": {},
                "apps": null,
                "projects": null,
                "features": {
                    "apps": false,
                    "enable_mcp_apps": false,
                    "plugins": false,
                    "remote_plugin": false,
                    "skill_mcp_dependency_install": false
                }
            },
            "origins": {},
            "layers": [{
                "name": { "type": "user", "file": "/managed/config.toml", "profile": null },
                "version": "sha256:managed",
                "config": {
                    "mcp_servers": mcp_servers,
                    "features": {
                        "apps": false,
                        "enable_mcp_apps": false,
                        "plugins": false,
                        "remote_plugin": false,
                        "skill_mcp_dependency_install": false
                    }
                }
            }]
        })
    }

    fn with_managed_config_origins(mut result: JsonValue, enabled_tools: &[String]) -> JsonValue {
        let fixed_keys = [
            "mcp_servers.pioneer.command",
            "mcp_servers.pioneer.args.0",
            "mcp_servers.pioneer.args.1",
            "mcp_servers.pioneer.args.2",
            "mcp_servers.pioneer.required",
            "features.apps",
            "features.enable_mcp_apps",
            "features.plugins",
            "features.remote_plugin",
            "features.skill_mcp_dependency_install",
            "personality",
        ];
        let keys = fixed_keys.into_iter().map(str::to_owned).chain(
            enabled_tools.iter().enumerate().flat_map(|(index, name)| {
                [
                    format!("mcp_servers.pioneer.enabled_tools.{index}"),
                    format!("mcp_servers.pioneer.tools.{name}.approval_mode"),
                ]
            }),
        );
        result["origins"] = JsonValue::Object(
            keys.map(|key| {
                (
                    key,
                    json!({
                        "name": {
                            "type": "user",
                            "file": "/managed/config.toml",
                            "profile": null
                        },
                        "version": "sha256:managed"
                    }),
                )
            })
            .collect(),
        );
        result
    }

    fn thread_open_params(cwd: &str) -> CLIAgentRuntimeThreadOpenParams {
        CLIAgentRuntimeThreadOpenParams {
            cwd: cwd.to_owned(),
            model: Some("gpt-test".to_owned()),
            approval_policy: Some("never".to_owned()),
            sandbox: Some(json!("danger-full-access")),
            permissions: Some("full-access".to_owned()),
            service_tier: None,
        }
    }

    #[test]
    fn codex_turn_maps_elevated_prompt_to_collaboration_mode() {
        let text = "Pioneer elevated instructions for Codex";
        let fingerprint = Sha256::digest(text.as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let params = CLIAgentRuntimeTurnStartParams {
            native_thread_id: "native-thread".to_owned(),
            input: json!([]),
            cwd: Some("/workspace".to_owned()),
            model: Some("gpt-5".to_owned()),
            approval_policy: Some("never".to_owned()),
            sandbox: None,
            permissions: Some(":read-only".to_owned()),
            effort: Some("high".to_owned()),
            personality: None,
            summary: None,
            elevated_instructions: CLIRuntimeElevatedInstructions::try_new(text, fingerprint)
                .expect("valid elevated instructions"),
        };

        let mapped = codex_turn_collaboration_mode(&params)
            .expect("elevated instructions should produce a collaboration mode");
        assert_eq!(
            serde_json::to_value(mapped).expect("serialize collaboration mode"),
            json!({
                "mode": "default",
                "settings": {
                    "model": "gpt-5",
                    "reasoning_effort": "high",
                    "developer_instructions": text
                }
            })
        );

        let opened = codex_read_only_thread_open_params(
            thread_open_params("/workspace"),
            "/workspace".to_owned(),
        );
        assert_eq!(opened.sandbox.as_deref(), Some("read-only"));
    }

    #[test]
    fn codex_turn_rejects_elevated_prompt_without_model() {
        let text = "Pioneer elevated instructions for Codex";
        let fingerprint = Sha256::digest(text.as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let params = CLIAgentRuntimeTurnStartParams {
            native_thread_id: "native-thread".to_owned(),
            input: json!([]),
            cwd: Some("/workspace".to_owned()),
            model: None,
            approval_policy: Some("never".to_owned()),
            sandbox: None,
            permissions: Some(":read-only".to_owned()),
            effort: None,
            personality: None,
            summary: None,
            elevated_instructions: CLIRuntimeElevatedInstructions::try_new(text, fingerprint)
                .expect("valid elevated instructions"),
        };

        let error = codex_turn_collaboration_mode(&params)
            .expect_err("elevated instructions without a model must fail closed");
        assert_eq!(
            error.to_string(),
            "Codex turn/start cannot deliver elevated instructions without a model"
        );
    }

    #[test]
    fn codex_turn_sandbox_policy_normalizes_known_modes_and_rejects_unknown() {
        assert_eq!(
            normalize_codex_turn_sandbox_policy(Some(json!("read-only")))
                .expect("read-only policy"),
            Some(json!({"type": "readOnly", "networkAccess": false}))
        );
        assert_eq!(
            normalize_codex_turn_sandbox_policy(Some(json!({"type": "dangerFullAccess"})))
                .expect("typed policy"),
            Some(json!({"type": "dangerFullAccess"}))
        );
        assert!(normalize_codex_turn_sandbox_policy(Some(json!("unknown"))).is_err());
        assert!(
            normalize_codex_turn_sandbox_policy(Some(json!({"mode": "read-only", "extra": true})))
                .is_err()
        );
    }

    fn empty_attestation() -> CodexMcpAttestationExpectation {
        CodexMcpAttestationExpectation::unmanaged_empty(
            codex_config_read_max_origins(512).expect("origin budget"),
        )
        .expect("empty attestation")
    }

    #[test]
    fn codex_readiness_probe_materializes_the_configured_tool_limit() {
        let names = codex_readiness_tool_names(512).expect("readiness tool names");
        assert_eq!(names.len(), 512);
        assert_eq!(
            names.first().map(String::as_str),
            Some(CODEX_READINESS_ALLOWED_TOOL)
        );
        assert_eq!(
            names.last().map(String::as_str),
            Some("pioneer_readiness_allowed_00000511")
        );
        assert_eq!(
            names.iter().collect::<std::collections::HashSet<_>>().len(),
            512
        );
        assert!(codex_readiness_tool_names(0).is_err());
    }

    fn exact_pioneer_snapshot(mcp_servers: JsonValue) -> CodexConfigReadSnapshot {
        let fingerprint =
            codex_config_value_fingerprint(&mcp_servers).expect("MCP config fingerprint");
        CodexConfigReadSnapshot {
            effective: CodexConfigIsolationEvidence {
                mcp_server_names: vec!["pioneer".to_owned()],
                mcp_servers_fingerprint: fingerprint.clone(),
                plugin_names: Vec::new(),
                marketplace_names: Vec::new(),
                app_names: Vec::new(),
                trusted_project_names: Vec::new(),
                isolation_features: CodexConfigIsolationFeatures {
                    apps: false,
                    enable_mcp_apps: false,
                    plugins: false,
                    remote_plugin: false,
                    skill_mcp_dependency_install: false,
                },
            },
            layers: vec![CodexConfigLayerIsolationEvidence {
                source: CodexConfigLayerSourceKind::User,
                source_fingerprint: "managed-layer".to_owned(),
                version: "sha256:managed".to_owned(),
                mcp_server_names: vec!["pioneer".to_owned()],
                mcp_servers_fingerprint: fingerprint,
                plugin_names: Vec::new(),
                marketplace_names: Vec::new(),
                app_names: Vec::new(),
                trusted_project_names: Vec::new(),
            }],
        }
    }

    #[test]
    fn codex_overlay_gateway_identity_binds_logical_key_boot_and_generation() {
        let allocator = CliSessionGenerationAllocator::default();
        let first = allocator
            .allocate(
                CLIAgentRuntimeSessionKey::new("workspace-a", "codex-personal", "thread-a")
                    .unwrap(),
            )
            .unwrap();
        let second = allocator
            .allocate(
                CLIAgentRuntimeSessionKey::new("workspace-a", "codex-personal", "thread-b")
                    .unwrap(),
            )
            .unwrap();

        let first_overlay = codex_generation_overlay_identity(&first).unwrap();
        let second_overlay = codex_generation_overlay_identity(&second).unwrap();
        assert_eq!(first_overlay.workspace_id, "workspace-a");
        assert_eq!(first_overlay.runtime_id, "codex-personal");
        assert_eq!(first_overlay.logical_thread_id, "thread-a");
        assert_eq!(first_overlay.process_generation, 1);
        assert_eq!(second_overlay.logical_thread_id, "thread-b");
        assert_eq!(second_overlay.process_generation, 2);
        assert_eq!(
            first_overlay.gateway_boot_id,
            second_overlay.gateway_boot_id
        );
    }

    #[tokio::test]
    async fn codex_isolation_attests_before_start_and_forces_read_only_startup() {
        let mut fake = FakeCodexIsolationServer::new();
        let client = fake.client.clone();
        let start = tokio::spawn(async move {
            start_codex_thread_with_exact_isolation(
                &client,
                thread_open_params("/workspace"),
                &empty_attestation(),
                None,
                None,
                Duration::from_secs(2),
            )
            .await
        });

        let config_read = fake.read_request().await;
        assert_eq!(config_read["method"], json!("config/read"));
        assert_eq!(
            config_read["params"],
            json!({ "cwd": "/workspace", "includeLayers": true })
        );
        fake.write_result(config_read["id"].clone(), safe_empty_config_read_result())
            .await;
        let thread_start = fake.read_request().await;
        assert_eq!(thread_start["method"], json!("thread/start"));
        assert_eq!(thread_start["params"]["cwd"], json!("/workspace"));
        assert_eq!(thread_start["params"]["sandbox"], json!("read-only"));
        assert!(thread_start["params"].get("permissions").is_none());
        fake.write_result(
            thread_start["id"].clone(),
            json!({ "thread": { "id": "native-thread", "cwd": "/workspace", "model": "gpt-test" } }),
        )
        .await;
        assert!(start.await.expect("start task should join").is_ok());
    }

    #[tokio::test]
    async fn codex_isolation_accepts_production_shaped_config_origins() {
        let enabled_tools = (0..306)
            .map(|index| format!("mcp__appstoreconnect__tool_{index:03}"))
            .collect::<Vec<_>>();
        let tools = enabled_tools
            .iter()
            .map(|name| (name.clone(), json!({"approval_mode": "approve"})))
            .collect::<serde_json::Map<_, _>>();
        let mcp_servers = json!({
            "pioneer": {
                "command": "/opt/pioneer/pioneer",
                "args": ["__cli-mcp-stdio", "--bootstrap-file", "/private/bootstrap"],
                "required": true,
                "enabled_tools": enabled_tools.as_slice(),
                "tools": tools
            }
        });
        let fingerprint =
            codex_config_value_fingerprint(&mcp_servers).expect("managed MCP fingerprint");
        let expectation = CodexMcpAttestationExpectation {
            server_names: vec!["pioneer".to_owned()],
            staged_mcp_servers_fingerprint: fingerprint.clone(),
            effective_mcp_servers_fingerprint: fingerprint,
            requires_staged_artifact: false,
            max_config_origins: codex_config_read_max_origins(512).expect("origin budget"),
        };
        let mut fake = FakeCodexIsolationServer::new();
        let client = fake.client.clone();
        let attestation = tokio::spawn(async move {
            attest_codex_exact_isolation(
                &client,
                "/workspace",
                &expectation,
                None,
                Duration::from_secs(2),
            )
            .await
        });

        let config_read = fake.read_request().await;
        assert_eq!(config_read["method"], json!("config/read"));
        let result = with_managed_config_origins(
            exact_pioneer_config_read_result(mcp_servers),
            enabled_tools.as_slice(),
        );
        assert_eq!(
            result["origins"].as_object().map(|origins| origins.len()),
            Some(623)
        );
        fake.write_result(config_read["id"].clone(), result).await;

        attestation
            .await
            .expect("attestation task should join")
            .expect("623 origins must fit the configured 512-tool contract");
    }

    #[test]
    fn codex_mcp_attestation_requires_exact_effective_server_and_layer_fingerprint() {
        let exact = json!({
            "pioneer": {
                "command": "/opt/pioneer/pioneer",
                "args": ["__cli-mcp-stdio", "--bootstrap-file", "/private/bootstrap"],
                "required": true,
                "enabled_tools": ["mcp__server__tool"],
                "tools": {"mcp__server__tool": {"approval_mode": "approve"}}
            }
        });
        let expectation = CodexMcpAttestationExpectation {
            server_names: vec!["pioneer".to_owned()],
            staged_mcp_servers_fingerprint: codex_config_value_fingerprint(&exact)
                .expect("expected staged fingerprint"),
            effective_mcp_servers_fingerprint: codex_config_value_fingerprint(&exact)
                .expect("expected fingerprint"),
            requires_staged_artifact: true,
            max_config_origins: codex_config_read_max_origins(512).expect("origin budget"),
        };
        let accepted = exact_pioneer_snapshot(exact.clone());
        validate_codex_exact_isolation_snapshot(&accepted, &expectation)
            .expect("exact managed Pioneer layer must be accepted");

        let mut drifted = exact;
        drifted["pioneer"]["enabled_tools"] = json!(["mcp__server__other"]);
        let rejected = exact_pioneer_snapshot(drifted);
        let error = validate_codex_exact_isolation_snapshot(&rejected, &expectation)
            .expect_err("effective config drift must fail closed");
        assert_eq!(
            error.kind,
            CodexIsolationAttestationFailureKind::NativeMcpPresent
        );

        let mut layered = accepted;
        layered.layers.push(layered.layers[0].clone());
        let error = validate_codex_exact_isolation_snapshot(&layered, &expectation)
            .expect_err("a second MCP-bearing layer must fail closed");
        assert_eq!(
            error.kind,
            CodexIsolationAttestationFailureKind::ForbiddenLayerContribution
        );
    }

    #[tokio::test]
    async fn codex_mcp_timeline_correlation_uses_native_item_identity_in_invoker_audit() {
        let ledger = Arc::new(CodexNativeMcpCorrelationLedger::default());
        let arguments = json!({"value": 7});
        ledger
            .register(crate::cli_runtime::codex_mcp::CodexNativeMcpItemBinding {
                native_thread_id: "native-thread".to_owned(),
                native_turn_id: "native-turn".to_owned(),
                native_item_id: "native-item".to_owned(),
                canonical_callable_name: "mcp__server__tool".to_owned(),
                arguments_fingerprint: crate::cli_runtime::codex_mcp::canonical_value_fingerprint(
                    &arguments,
                )
                .expect("arguments fingerprint"),
            })
            .expect("native lifecycle registration");
        let inner = Arc::new(RecordingProviderCallInvoker::default());
        let invoker = CodexCorrelatingTurnMcpInvoker {
            inner: inner.clone(),
            native_items: ledger,
        };
        let error = invoker
            .invoke(
                TurnMcpInvocation {
                    workspace_id: "workspace".to_owned(),
                    thread_id: "thread".to_owned(),
                    turn_id: "turn".to_owned(),
                    runtime_id: Some("codex".to_owned()),
                    session_generation: Some(1),
                    provider_call_id: "jsonrpc:7".to_owned(),
                    canonical_callable_name: "mcp__server__tool".to_owned(),
                    arguments,
                    origin: crate::turn_mcp::invoker::TurnMcpInvocationOrigin::CliFacade,
                },
                CancellationToken::new(),
            )
            .await
            .expect_err("recording invoker returns a sentinel error");
        assert_eq!(error.code, TurnMcpInvocationErrorCode::Internal);
        assert_eq!(
            inner
                .provider_call_id
                .lock()
                .expect("recording invoker should not be poisoned")
                .as_deref(),
            Some("native-item")
        );
    }

    #[tokio::test]
    async fn codex_pre_model_barrier_and_codex_mcp_approval_are_exact_and_fail_closed() {
        #[cfg(unix)]
        let temporary = tempfile::tempdir_in("/tmp").expect("temporary root");
        #[cfg(windows)]
        let temporary = tempfile::tempdir().expect("temporary root");
        let supervisor = CliMcpBridgeSupervisor::new(temporary.path().join("bridge"));
        let process_instance = CliSessionGenerationAllocator::default()
            .allocate(
                CLIAgentRuntimeSessionKey::new("workspace", "codex", "thread")
                    .expect("session key"),
            )
            .expect("process instance");
        let scope = CliMcpGrantScope::new(
            process_instance.clone(),
            CliMcpManifestHash::new("a".repeat(64)).expect("manifest"),
        );
        let projection = CliMcpFacadeProjection::new(
            vec![
                CliMcpFacadeTool::new(
                    "mcp__server__tool",
                    Some("fixture".to_owned()),
                    json!({"type": "object", "properties": {}}),
                    json!({}),
                )
                .expect("facade tool"),
            ],
            CliMcpFacadeProjectionLimits::default(),
        )
        .expect("facade projection");
        let launch = supervisor
            .prepare(scope, codex_mcp_bootstrap_expiry(2_000).expect("expiry"))
            .await
            .expect("bridge launch");
        let bootstrap_path = launch.bootstrap_path().to_path_buf();
        let reservation = supervisor
            .coordinator()
            .stage_projection(launch.grant_ref(), projection.fingerprint().clone())
            .await
            .expect("projection reservation");
        supervisor
            .associate_provider_process(&process_instance, std::process::id(), None)
            .await
            .expect("provider identity");
        let projection_fingerprint = projection.fingerprint().clone();
        let required_bridge = Arc::new(CodexRequiredMcpBridge {
            supervisor: supervisor.clone(),
            process_instance,
            launch,
            projection,
            projection_generation: reservation.generation,
            projection_fingerprint,
            canonical_manifest_hash: "a".repeat(64),
            provider_contract_fingerprint: "b".repeat(64),
            isolation_contract_fingerprint: "c".repeat(64),
            invoker: Arc::new(NeverCalledInvoker),
            facade_limits: CliMcpFacadeLimits::default(),
            native_items: Arc::new(CodexNativeMcpCorrelationLedger::default()),
            state: tokio::sync::Mutex::new(CodexRequiredMcpBridgeState::Pending),
        });
        let mcp_servers = json!({
            "pioneer": {
                "command": "/opt/pioneer/pioneer",
                "args": ["__cli-mcp-stdio", "--bootstrap-file", bootstrap_path],
                "required": true,
                "enabled_tools": ["mcp__server__tool"],
                "tools": {"mcp__server__tool": {"approval_mode": "approve"}}
            }
        });
        let expectation = CodexMcpAttestationExpectation {
            server_names: vec!["pioneer".to_owned()],
            staged_mcp_servers_fingerprint: codex_config_value_fingerprint(&mcp_servers)
                .expect("expected staged fingerprint"),
            effective_mcp_servers_fingerprint: codex_config_value_fingerprint(&mcp_servers)
                .expect("expected fingerprint"),
            requires_staged_artifact: false,
            max_config_origins: codex_config_read_max_origins(512).expect("origin budget"),
        };
        let mut fake = FakeCodexIsolationServer::new();
        let client = fake.client.clone();
        let bridge_for_start = required_bridge.clone();
        let start = tokio::spawn(async move {
            start_codex_thread_with_exact_isolation(
                &client,
                thread_open_params("/workspace"),
                &expectation,
                None,
                Some(bridge_for_start.as_ref()),
                Duration::from_secs(2),
            )
            .await
        });

        let config_read = fake.read_request().await;
        assert_eq!(config_read["method"], json!("config/read"));
        fake.write_result(
            config_read["id"].clone(),
            exact_pioneer_config_read_result(mcp_servers),
        )
        .await;
        let thread_start = fake.read_request().await;
        assert_eq!(thread_start["method"], json!("thread/start"));
        assert!(
            !start.is_finished(),
            "native thread result is still pending"
        );

        let (mut provider_writer, helper_stdin) = duplex(64 * 1024);
        let (helper_stdout, provider_reader) = duplex(64 * 1024);
        let helper = tokio::spawn(async move {
            run_hidden_helper_with_io(&bootstrap_path, helper_stdin, helper_stdout).await
        });
        let mut provider_reader = BufReader::new(provider_reader);
        provider_writer
            .write_all(
                b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{\"protocolVersion\":\"2025-06-18\",\"capabilities\":{},\"clientInfo\":{\"name\":\"codex-test\",\"version\":\"1\"}}}\n",
            )
            .await
            .expect("send initialize");
        let mut line = String::new();
        provider_reader
            .read_line(&mut line)
            .await
            .expect("read initialize response");
        assert_eq!(
            serde_json::from_str::<JsonValue>(&line).expect("initialize JSON")["id"],
            json!(1)
        );
        provider_writer
            .write_all(
                b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\"}\n",
            )
            .await
            .expect("send list");
        line.clear();
        provider_reader
            .read_line(&mut line)
            .await
            .expect("read list response");
        let listed: JsonValue = serde_json::from_str(&line).expect("list JSON");
        assert_eq!(listed["result"]["tools"][0]["name"], "mcp__server__tool");
        assert!(
            !start.is_finished(),
            "thread/start response remains part of barrier"
        );

        fake.write_result(
            thread_start["id"].clone(),
            json!({ "thread": { "id": "native-thread", "cwd": "/workspace", "model": "gpt-test" } }),
        )
        .await;
        start
            .await
            .expect("start task should join")
            .expect("exact list and native thread must complete barrier");

        required_bridge
            .prepare_turn("pioneer-thread", "pioneer-turn")
            .await
            .expect("reserve exact MCP turn");
        required_bridge
            .activate_turn("pioneer-turn", "native-thread", "native-turn")
            .await
            .expect("activate exact native binding");
        required_bridge
            .register_native_item(crate::cli_runtime::codex_mcp::CodexNativeMcpItemBinding {
                native_thread_id: "native-thread".to_owned(),
                native_turn_id: "native-turn".to_owned(),
                native_item_id: "native-item".to_owned(),
                canonical_callable_name: "mcp__server__tool".to_owned(),
                arguments_fingerprint: crate::cli_runtime::codex_mcp::canonical_value_fingerprint(
                    &json!({"value": 7}),
                )
                .expect("arguments fingerprint"),
            })
            .expect("register exact native item");
        let requested_permissions = json!({"network": ["example.com"]});
        let exact = required_bridge
            .native_approval_response(CLIAgentRuntimeNativeMcpApprovalRequest {
                native_thread_id: "native-thread".to_owned(),
                native_turn_id: "native-turn".to_owned(),
                native_item_id: "native-item".to_owned(),
                requested_permissions: requested_permissions.clone(),
            })
            .await
            .expect("exact native approval validation");
        assert_eq!(
            exact,
            Some(json!({
                "permissions": requested_permissions,
                "scope": "turn",
                "strictAutoReview": false,
            }))
        );

        let stale = required_bridge
            .native_approval_response(CLIAgentRuntimeNativeMcpApprovalRequest {
                native_thread_id: "native-thread".to_owned(),
                native_turn_id: "native-turn".to_owned(),
                native_item_id: "unknown-item".to_owned(),
                requested_permissions: json!({}),
            })
            .await
            .expect("unknown native approval validation");
        assert_eq!(stale, None, "unknown native item must be denied");

        required_bridge
            .terminal_turn("pioneer-turn")
            .await
            .expect("terminalize exact MCP turn");
        let terminal = required_bridge
            .native_approval_response(CLIAgentRuntimeNativeMcpApprovalRequest {
                native_thread_id: "native-thread".to_owned(),
                native_turn_id: "native-turn".to_owned(),
                native_item_id: "native-item".to_owned(),
                requested_permissions: json!({}),
            })
            .await
            .expect("terminal native approval validation");
        assert_eq!(terminal, None, "terminal native item must be denied");

        required_bridge.fail_closed().await;
        drop(provider_writer);
        let _ = tokio_timeout(Duration::from_secs(1), helper).await;
    }

    #[tokio::test]
    async fn codex_required_bridge_helper_failure_revokes_staged_generation() {
        #[cfg(unix)]
        let temporary = tempfile::tempdir_in("/tmp").expect("temporary root");
        #[cfg(windows)]
        let temporary = tempfile::tempdir().expect("temporary root");
        let supervisor = CliMcpBridgeSupervisor::new(temporary.path().join("bridge"));
        let process_instance = CliSessionGenerationAllocator::default()
            .allocate(
                CLIAgentRuntimeSessionKey::new("workspace", "codex", "helper-failure")
                    .expect("session key"),
            )
            .expect("process instance");
        let scope = CliMcpGrantScope::new(
            process_instance.clone(),
            CliMcpManifestHash::new("a".repeat(64)).expect("manifest"),
        );
        let projection = CliMcpFacadeProjection::new(
            vec![
                CliMcpFacadeTool::new(
                    "mcp__server__tool",
                    None,
                    json!({"type": "object"}),
                    json!({}),
                )
                .expect("tool"),
            ],
            CliMcpFacadeProjectionLimits::default(),
        )
        .expect("projection");
        let launch = supervisor
            .prepare(scope, codex_mcp_bootstrap_expiry(2_000).expect("expiry"))
            .await
            .expect("bridge launch");
        let bootstrap_path = launch.bootstrap_path().to_path_buf();
        let reservation = supervisor
            .coordinator()
            .stage_projection(launch.grant_ref(), projection.fingerprint().clone())
            .await
            .expect("projection reservation");
        supervisor
            .associate_provider_process(&process_instance, std::process::id(), None)
            .await
            .expect("provider identity");
        let projection_fingerprint = projection.fingerprint().clone();
        let bridge = CodexRequiredMcpBridge {
            supervisor: supervisor.clone(),
            process_instance: process_instance.clone(),
            launch,
            projection,
            projection_generation: reservation.generation,
            projection_fingerprint,
            canonical_manifest_hash: "a".repeat(64),
            provider_contract_fingerprint: "b".repeat(64),
            isolation_contract_fingerprint: "c".repeat(64),
            invoker: Arc::new(NeverCalledInvoker),
            facade_limits: CliMcpFacadeLimits::default(),
            native_items: Arc::new(CodexNativeMcpCorrelationLedger::default()),
            state: tokio::sync::Mutex::new(CodexRequiredMcpBridgeState::Pending),
        };

        bridge
            .ensure_ready(Duration::from_millis(10))
            .await
            .expect_err("missing helper must fail the bounded attach barrier");
        bridge.fail_closed().await;
        assert!(!bootstrap_path.exists());
        assert!(!supervisor.revoke_session(&process_instance).await);
        assert!(matches!(
            *bridge.state.lock().await,
            CodexRequiredMcpBridgeState::Failed
        ));
    }

    #[tokio::test]
    async fn codex_thread_resume_attests_and_uses_persisted_native_cwd() {
        let mut fake = FakeCodexIsolationServer::new();
        let client = fake.client.clone();
        let resume = tokio::spawn(async move {
            resume_codex_thread_with_exact_isolation(
                &client,
                "native-thread",
                thread_open_params("/new-request-cwd"),
                &empty_attestation(),
                None,
                None,
                Duration::from_secs(2),
            )
            .await
        });

        let thread_read = fake.read_request().await;
        assert_eq!(thread_read["method"], json!("thread/read"));
        fake.write_result(
            thread_read["id"].clone(),
            json!({ "thread": { "id": "native-thread", "cwd": "/persisted-cwd" } }),
        )
        .await;
        let config_read = fake.read_request().await;
        assert_eq!(config_read["method"], json!("config/read"));
        assert_eq!(config_read["params"]["cwd"], json!("/persisted-cwd"));
        fake.write_result(config_read["id"].clone(), safe_empty_config_read_result())
            .await;
        let thread_resume = fake.read_request().await;
        assert_eq!(thread_resume["method"], json!("thread/resume"));
        assert_eq!(thread_resume["params"]["cwd"], json!("/persisted-cwd"));
        assert_eq!(thread_resume["params"]["sandbox"], json!("read-only"));
        fake.write_result(
            thread_resume["id"].clone(),
            json!({ "thread": { "id": "native-thread", "cwd": "/persisted-cwd", "model": "gpt-test" } }),
        )
        .await;
        assert!(resume.await.expect("resume task should join").is_ok());
    }

    #[tokio::test]
    async fn codex_thread_resume_postverifies_after_discarding_oversized_history() {
        let mut fake = FakeCodexIsolationServer::new();
        let client = fake.client.clone();
        let resume = tokio::spawn(async move {
            resume_codex_thread_with_exact_isolation(
                &client,
                "native-thread",
                thread_open_params("/new-request-cwd"),
                &empty_attestation(),
                None,
                None,
                Duration::from_secs(5),
            )
            .await
        });

        let thread_read = fake.read_request().await;
        fake.write_result(
            thread_read["id"].clone(),
            json!({ "thread": { "id": "native-thread", "cwd": "/persisted-cwd" } }),
        )
        .await;
        let config_read = fake.read_request().await;
        fake.write_result(config_read["id"].clone(), safe_empty_config_read_result())
            .await;
        let thread_resume = fake.read_request().await;
        assert_eq!(thread_resume["method"], json!("thread/resume"));
        fake.write_result(
            thread_resume["id"].clone(),
            json!({
                "thread": {
                    "id": "native-thread",
                    "cwd": "/persisted-cwd",
                    "turns": ["x".repeat(1024 * 1024 + 256)]
                }
            }),
        )
        .await;

        let postverify = fake.read_request().await;
        assert_eq!(postverify["method"], json!("thread/read"));
        assert_eq!(postverify["params"]["includeTurns"], json!(false));
        fake.write_result(
            postverify["id"].clone(),
            json!({ "thread": { "id": "native-thread", "cwd": "/persisted-cwd" } }),
        )
        .await;

        let opened = resume
            .await
            .expect("resume task should join")
            .expect("oversized history should be discarded and verified");
        assert_eq!(opened.native_thread_id, "native-thread");
        assert_eq!(opened.cwd.as_deref(), Some("/persisted-cwd"));
        assert!(!opened.response_was_oversized);
        assert!(opened.raw["thread"].get("turns").is_none());
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn codex_thread_resume_uses_shared_rollout_after_previous_generation_cleanup() {
        let temporary = tempfile::tempdir_in("/tmp").expect("temporary root");
        let shared_home = temporary.path().join("shared-codex-home");
        let managed_root = temporary.path().join("managed-overlays");
        std::fs::create_dir_all(shared_home.as_path()).expect("create shared Codex home");
        let config = CodexAccountProbeConfig {
            executable: "codex".to_owned(),
            home_path: shared_home.to_string_lossy().into_owned(),
            shadow_home_path: None,
            cwd: Some(temporary.path().to_path_buf()),
            home_dir: None,
            env: SensitiveEnvironment::new(),
            initialize_timeout: Duration::from_secs(2),
            request_timeout: Duration::from_secs(2),
            shutdown_grace: Duration::from_millis(10),
            stderr_ring_lines: 16,
        };
        let first_identity = CodexGenerationOverlayIdentity::new(
            "workspace",
            "codex",
            "logical-thread",
            "gateway-boot",
            1,
        )
        .expect("first identity");
        let (_, first_overlay) = codex_generation_app_server_process_config(
            &config,
            managed_root.as_path(),
            first_identity,
        )
        .expect("first overlay");
        let relative_rollout = PathBuf::from("2026/08/03/rollout-native-thread.jsonl");
        let shared_rollout = shared_home
            .join("sessions")
            .join(relative_rollout.as_path());
        std::fs::create_dir_all(shared_rollout.parent().expect("rollout parent"))
            .expect("create rollout parent");
        std::fs::write(shared_rollout.as_path(), "{}\n").expect("write rollout");
        let stale_rollout = first_overlay
            .effective_home_path
            .join("sessions")
            .join(relative_rollout.as_path());
        cleanup_codex_generation_overlay(&first_overlay).expect("clean stopped generation");
        assert!(!stale_rollout.exists());

        let second_identity = CodexGenerationOverlayIdentity::new(
            "workspace",
            "codex",
            "logical-thread",
            "gateway-boot",
            2,
        )
        .expect("second identity");
        let (_, second_overlay) = codex_generation_app_server_process_config(
            &config,
            managed_root.as_path(),
            second_identity,
        )
        .expect("second overlay");
        let resume_overlay = second_overlay.clone();
        let mut fake = FakeCodexIsolationServer::new();
        let client = fake.client.clone();
        let stale_rollout_for_response = stale_rollout.clone();
        let resume = tokio::spawn(async move {
            resume_codex_thread_with_exact_isolation(
                &client,
                "native-thread",
                thread_open_params("/new-request-cwd"),
                &empty_attestation(),
                Some(&resume_overlay),
                None,
                Duration::from_secs(2),
            )
            .await
        });

        let thread_read = fake.read_request().await;
        assert_eq!(thread_read["method"], json!("thread/read"));
        fake.write_result(
            thread_read["id"].clone(),
            json!({
                "thread": {
                    "id": "native-thread",
                    "cwd": "/persisted-cwd",
                    "path": stale_rollout_for_response
                }
            }),
        )
        .await;
        let config_read = fake.read_request().await;
        assert_eq!(config_read["method"], json!("config/read"));
        fake.write_result(config_read["id"].clone(), safe_empty_config_read_result())
            .await;
        let thread_resume = fake.read_request().await;
        assert_eq!(thread_resume["method"], json!("thread/resume"));
        assert_eq!(
            thread_resume["params"]["path"],
            json!(
                std::fs::canonicalize(shared_rollout.as_path()).expect("canonical shared rollout")
            )
        );
        fake.write_result(
            thread_resume["id"].clone(),
            json!({
                "thread": {
                    "id": "native-thread",
                    "cwd": "/persisted-cwd",
                    "model": "gpt-test"
                }
            }),
        )
        .await;
        resume
            .await
            .expect("resume task should join")
            .expect("resume should use the durable rollout path");

        cleanup_codex_generation_overlay(&second_overlay).expect("clean resumed generation");
    }

    #[tokio::test]
    async fn malicious_mcp_sentinel_is_rejected_before_native_thread_start() {
        static SENTINEL_EXECUTIONS: AtomicUsize = AtomicUsize::new(0);
        SENTINEL_EXECUTIONS.store(0, Ordering::SeqCst);
        let mut fake = FakeCodexIsolationServer::new();
        let client = fake.client.clone();
        let start = tokio::spawn(async move {
            start_codex_thread_with_exact_isolation(
                &client,
                thread_open_params("/workspace"),
                &empty_attestation(),
                None,
                None,
                Duration::from_secs(2),
            )
            .await
        });

        let config_read = fake.read_request().await;
        fake.write_result(
            config_read["id"].clone(),
            json!({
                "config": {
                    "mcp_servers": { "malicious_sentinel": { "command": "/sentinel" } },
                    "plugins": {},
                    "marketplaces": {},
                    "apps": null,
                    "projects": null,
                    "features": {
                        "apps": false,
                        "enable_mcp_apps": false,
                        "plugins": false,
                        "remote_plugin": false,
                        "skill_mcp_dependency_install": false
                    }
                },
                "origins": {},
                "layers": [{
                    "name": { "type": "project", "dotCodexFolder": "/workspace/.codex" },
                    "version": "sha256:malicious",
                    "config": { "mcp_servers": { "malicious_sentinel": { "command": "/sentinel" } } }
                }]
            }),
        )
        .await;
        let error = start
            .await
            .expect("start task should join")
            .expect_err("unmanaged MCP must fail closed");
        assert!(error.to_string().contains("isolation attestation failed"));
        assert_eq!(SENTINEL_EXECUTIONS.load(Ordering::SeqCst), 0);
        if tokio::time::timeout(Duration::from_millis(25), fake.read_request())
            .await
            .is_ok()
        {
            SENTINEL_EXECUTIONS.fetch_add(1, Ordering::SeqCst);
        }
        assert_eq!(
            SENTINEL_EXECUTIONS.load(Ordering::SeqCst),
            0,
            "no native thread or MCP status request may follow failed attestation"
        );
    }
}
