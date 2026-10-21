use pioneer_cli_agent_runtime::claude::{
    CLAUDE_MCP_SCHEMA_TRANSFORM_CONTRACT_VERSION, ClaudeExecutableAttestation,
    ClaudeExecutableCacheIdentity, attest_claude_executable_identity,
    claude_continuation_contract_fingerprint, claude_executable_cache_identity,
    probe_claude_help_contract, validate_recorded_claude_mcp_decoder_fixtures,
};
use pioneer_cli_agent_runtime::codex::CODEX_MCP_SCHEMA_TRANSFORM_CONTRACT_VERSION;
use pioneer_cli_agent_runtime::codex_attestation::{
    CodexExecutableAttestation, CodexExecutableCacheIdentity, attest_codex_executable_identity,
    codex_executable_cache_identity, sha256_file_contents, sha256_json,
    validate_recorded_codex_mcp_decoder_fixtures, validate_unknown_codex_request_response,
};
use pioneer_config::EffectiveGatewayCliAgentRuntimeInstanceConfig;
use pioneer_protocol::{
    CliMcpAdapterReadiness, CliMcpInjectionKind, CliMcpProjectionUpdateKind, RuntimeDiagnostic,
    RuntimeDiagnosticLevel,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock, Weak};

pub(crate) const CODEX_MCP_LOCAL_PROBE_CONTRACT_VERSION: u32 = 2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CliRuntimeCapabilityPolicy {
    supports_skills: bool,
    supports_mcp_tools: bool,
    diagnostic_code: String,
    diagnostic_message: String,
}

impl CliRuntimeCapabilityPolicy {
    pub(crate) fn phase_zero(supports_skills: bool) -> Self {
        Self::unsupported(
            supports_skills,
            "cli_runtime.mcp.readiness_unavailable",
            "MCP tool readiness is not available",
        )
    }

    pub(crate) fn from_readiness(
        supports_skills: bool,
        readiness: Option<&CliMcpAdapterReadiness>,
    ) -> Self {
        let Some(readiness) = readiness else {
            return Self::phase_zero(supports_skills);
        };
        if readiness.supported {
            return Self {
                supports_skills,
                supports_mcp_tools: true,
                diagnostic_code: "cli_runtime.mcp.ready".to_owned(),
                diagnostic_message: "MCP tools are ready for this runtime".to_owned(),
            };
        }
        let diagnostic = readiness.diagnostics.iter().find(|diagnostic| {
            matches!(
                diagnostic.level,
                RuntimeDiagnosticLevel::Warning | RuntimeDiagnosticLevel::Error
            )
        });
        Self::unsupported(
            supports_skills,
            diagnostic
                .map(|diagnostic| diagnostic.code.as_str())
                .unwrap_or("cli_runtime.mcp.readiness_unavailable"),
            diagnostic
                .map(|diagnostic| diagnostic.message.as_str())
                .unwrap_or("MCP tool readiness is not available"),
        )
    }

    fn unsupported(supports_skills: bool, code: &str, message: &str) -> Self {
        Self {
            supports_skills,
            supports_mcp_tools: false,
            diagnostic_code: code.to_owned(),
            diagnostic_message: message.to_owned(),
        }
    }

    pub(crate) const fn supports_skills(&self) -> bool {
        self.supports_skills
    }

    pub(crate) const fn supports_mcp_tools(&self) -> bool {
        self.supports_mcp_tools
    }

    pub(crate) fn mcp_diagnostic(&self) -> (&str, &str) {
        (
            self.diagnostic_code.as_str(),
            self.diagnostic_message.as_str(),
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CodexMcpLocalProbeKind {
    ExactRawToolFiltering,
    ControlledConfigIsolation,
    UniqueOverlayIsolation,
    SharedStateContinuity,
    RequiredBridgeListBarrier,
    UnmanagedMcpExcluded,
    UnknownRequestResponse,
    HelperAttachCancellationCleanup,
    NativeEventDecoderFixtures,
}

impl CodexMcpLocalProbeKind {
    pub(crate) const ALL: [Self; 9] = [
        Self::ExactRawToolFiltering,
        Self::ControlledConfigIsolation,
        Self::UniqueOverlayIsolation,
        Self::SharedStateContinuity,
        Self::RequiredBridgeListBarrier,
        Self::UnmanagedMcpExcluded,
        Self::UnknownRequestResponse,
        Self::HelperAttachCancellationCleanup,
        Self::NativeEventDecoderFixtures,
    ];

    const fn diagnostic(self) -> (&'static str, &'static str) {
        match self {
            Self::ExactRawToolFiltering => (
                "cli_runtime.mcp.codex_raw_tool_filter_failed",
                "Codex exact MCP tool filtering self-probe failed",
            ),
            Self::ControlledConfigIsolation => (
                "cli_runtime.mcp.strict_isolation_failed",
                "Codex controlled config isolation self-probe failed",
            ),
            Self::UniqueOverlayIsolation => (
                "cli_runtime.mcp.codex_overlay_isolation_failed",
                "Codex per-generation overlay isolation self-probe failed",
            ),
            Self::SharedStateContinuity => (
                "cli_runtime.mcp.codex_continuity_prerequisite_failed",
                "Codex shared-state continuity prerequisite self-probe failed",
            ),
            Self::RequiredBridgeListBarrier => (
                "cli_runtime.mcp.codex_required_list_failed",
                "Codex required MCP list barrier self-probe failed",
            ),
            Self::UnmanagedMcpExcluded => (
                "cli_runtime.mcp.strict_isolation_failed",
                "Unmanaged Codex MCP exclusion self-probe failed",
            ),
            Self::UnknownRequestResponse => (
                "cli_runtime.mcp.codex_unknown_request_failed",
                "Codex unknown-request response self-probe failed",
            ),
            Self::HelperAttachCancellationCleanup => (
                "cli_runtime.mcp.bridge_unavailable",
                "Pioneer MCP helper lifecycle self-probe failed",
            ),
            Self::NativeEventDecoderFixtures => (
                "cli_runtime.mcp.codex_decoder_fixture_failed",
                "Codex native MCP event decoder self-probe failed",
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CodexMcpLocalProbeResult {
    pub(crate) passed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) safe_detail: Option<String>,
}

impl CodexMcpLocalProbeResult {
    pub(crate) fn passed() -> Self {
        Self {
            passed: true,
            safe_detail: None,
        }
    }

    pub(crate) fn failed(safe_detail: impl Into<String>) -> Self {
        Self {
            passed: false,
            safe_detail: Some(bound_safe_detail(safe_detail.into().as_str())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CodexMcpLocalAttestation {
    pub(crate) contract_version: u32,
    pub(crate) executable: CodexExecutableAttestation,
    pub(crate) adapter_contract_fingerprint: String,
    pub(crate) isolation_contract_fingerprint: String,
    pub(crate) schema_contract_fingerprint: String,
    pub(crate) bridge_contract_fingerprint: String,
    pub(crate) local_attestation_fingerprint: String,
    pub(crate) probes: BTreeMap<CodexMcpLocalProbeKind, CodexMcpLocalProbeResult>,
}

impl CodexMcpLocalAttestation {
    pub(crate) fn build(
        executable: CodexExecutableAttestation,
        adapter_contract_fingerprint: String,
        isolation_contract_fingerprint: String,
        schema_contract_fingerprint: String,
        bridge_contract_fingerprint: String,
        probes: BTreeMap<CodexMcpLocalProbeKind, CodexMcpLocalProbeResult>,
    ) -> anyhow::Result<Self> {
        for (label, fingerprint) in [
            ("adapter contract", adapter_contract_fingerprint.as_str()),
            (
                "isolation contract",
                isolation_contract_fingerprint.as_str(),
            ),
            ("schema contract", schema_contract_fingerprint.as_str()),
            ("bridge contract", bridge_contract_fingerprint.as_str()),
        ] {
            validate_fingerprint(label, fingerprint)?;
        }
        for probe in CodexMcpLocalProbeKind::ALL {
            if !probes.contains_key(&probe) {
                anyhow::bail!("Codex local attestation is missing probe `{probe:?}`");
            }
        }
        if probes.len() != CodexMcpLocalProbeKind::ALL.len() {
            anyhow::bail!("Codex local attestation contains unknown probe entries");
        }
        let fingerprint = sha256_json(&json!({
            "contractVersion": CODEX_MCP_LOCAL_PROBE_CONTRACT_VERSION,
            "localExecutableFingerprint": executable.local_executable_fingerprint,
            "adapterContractFingerprint": adapter_contract_fingerprint,
            "isolationContractFingerprint": isolation_contract_fingerprint,
            "schemaContractFingerprint": schema_contract_fingerprint,
            "bridgeContractFingerprint": bridge_contract_fingerprint,
            "probes": probes,
        }))?;
        Ok(Self {
            contract_version: CODEX_MCP_LOCAL_PROBE_CONTRACT_VERSION,
            executable,
            adapter_contract_fingerprint,
            isolation_contract_fingerprint,
            schema_contract_fingerprint,
            bridge_contract_fingerprint,
            local_attestation_fingerprint: fingerprint,
            probes,
        })
    }

    pub(crate) fn all_probes_passed(&self) -> bool {
        CodexMcpLocalProbeKind::ALL
            .into_iter()
            .all(|probe| self.probes.get(&probe).is_some_and(|result| result.passed))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CodexMcpReadinessInput<'a> {
    pub(crate) runtime_enabled: bool,
    pub(crate) platform_ipc_available: bool,
    pub(crate) facade_supervisor_healthy: bool,
    pub(crate) max_tools: usize,
    pub(crate) max_schema_bytes: usize,
    pub(crate) local_attestation: Option<&'a CodexMcpLocalAttestation>,
}

pub(crate) fn evaluate_codex_mcp_readiness(
    input: CodexMcpReadinessInput<'_>,
) -> CliMcpAdapterReadiness {
    let local = input.local_attestation;
    let provider_version =
        local.map(|local| local.executable.cache_identity.provider_version.clone());
    let local_executable_fingerprint = local
        .map(|local| local.executable.local_executable_fingerprint.clone())
        .unwrap_or_default();
    let contract_fingerprint = local
        .map(|local| local.adapter_contract_fingerprint.clone())
        .unwrap_or_default();
    let mut diagnostics = Vec::new();
    if !input.runtime_enabled {
        diagnostics.push(diagnostic(
            RuntimeDiagnosticLevel::Info,
            "cli_runtime.mcp.runtime_not_ready",
            "The Codex runtime is disabled",
        ));
    }
    if !input.platform_ipc_available {
        diagnostics.push(diagnostic(
            RuntimeDiagnosticLevel::Error,
            "cli_runtime.mcp.bridge_unavailable",
            "Private MCP bridge IPC is unavailable on this platform",
        ));
    }
    if !input.facade_supervisor_healthy {
        diagnostics.push(diagnostic(
            RuntimeDiagnosticLevel::Error,
            "cli_runtime.mcp.bridge_unavailable",
            "The MCP bridge supervisor is unavailable",
        ));
    }
    match local {
        None => diagnostics.push(diagnostic(
            RuntimeDiagnosticLevel::Warning,
            "cli_runtime.mcp.provider_probe_failed",
            "Codex local MCP attestation is unavailable",
        )),
        Some(local) => {
            for probe in CodexMcpLocalProbeKind::ALL {
                let result = local.probes.get(&probe);
                if !result.is_some_and(|result| result.passed) {
                    let (code, message) = probe.diagnostic();
                    let message = result
                        .and_then(|result| result.safe_detail.as_deref())
                        .map(|detail| format!("{message}: {detail}"))
                        .unwrap_or_else(|| message.to_owned());
                    diagnostics.push(diagnostic(RuntimeDiagnosticLevel::Error, code, &message));
                }
            }
        }
    }

    let supported = input.runtime_enabled
        && input.platform_ipc_available
        && input.facade_supervisor_healthy
        && local.is_some_and(CodexMcpLocalAttestation::all_probes_passed);
    if supported {
        diagnostics.push(diagnostic(
            RuntimeDiagnosticLevel::Info,
            "cli_runtime.mcp.ready",
            "Codex MCP tools passed exact local technical readiness checks",
        ));
    }
    CliMcpAdapterReadiness {
        supported,
        injection: CliMcpInjectionKind::CodexManagedStdioMcp,
        projection_update: CliMcpProjectionUpdateKind::CodexRestartAppServerResumeThread,
        strict_isolation: local.is_some_and(|local| {
            [
                CodexMcpLocalProbeKind::ControlledConfigIsolation,
                CodexMcpLocalProbeKind::UniqueOverlayIsolation,
                CodexMcpLocalProbeKind::UnmanagedMcpExcluded,
            ]
            .into_iter()
            .all(|probe| local.probes.get(&probe).is_some_and(|result| result.passed))
        }),
        contract_fingerprint,
        local_executable_fingerprint,
        provider_version,
        max_tools: input.max_tools,
        max_schema_bytes: input.max_schema_bytes,
        diagnostics,
    }
}

/// The technical probes have their own slots: background warmup already holds
/// a different permit and turn preflight must not acquire that permit again.
/// No path holding a technical slot waits for a background warmup slot.
fn local_mcp_probe_slots() -> &'static Arc<tokio::sync::Semaphore> {
    static SLOTS: OnceLock<Arc<tokio::sync::Semaphore>> = OnceLock::new();
    SLOTS.get_or_init(|| Arc::new(tokio::sync::Semaphore::new(2)))
}

/// Coalesce only concurrent checks of one exact executable/contract identity.
/// OnceCell releases initialization on cancellation. Weak entries let failed
/// results disappear after the current callers finish, without caching failure
/// forever or detaching work from its caller's timeout/cancellation scope.
struct LocalMcpProbeFlights<K, V> {
    slots: Arc<tokio::sync::Semaphore>,
    entries: Mutex<HashMap<K, Weak<tokio::sync::OnceCell<Option<V>>>>>,
}
impl<K, V> Default for LocalMcpProbeFlights<K, V> {
    fn default() -> Self {
        Self {
            slots: local_mcp_probe_slots().clone(),
            entries: Mutex::new(HashMap::new()),
        }
    }
}
impl<K: Eq + std::hash::Hash, V: Clone> LocalMcpProbeFlights<K, V> {
    async fn run<F>(
        &self,
        identity: K,
        probe: impl FnOnce(tokio::sync::OwnedSemaphorePermit) -> F,
    ) -> Option<V>
    where
        F: std::future::Future<Output = Option<V>>,
    {
        let cell = {
            let mut entries = self.entries.lock().expect("MCP probe flights poisoned");
            entries.retain(|_, entry| entry.strong_count() > 0);
            if let Some(cell) = entries.get(&identity).and_then(Weak::upgrade) {
                cell
            } else {
                let cell = Arc::new(tokio::sync::OnceCell::new());
                entries.insert(identity, Arc::downgrade(&cell));
                cell
            }
        };
        cell.get_or_init(|| async {
            let slot = self.slots.clone().acquire_owned().await.ok()?;
            probe(slot).await
        })
        .await
        .clone()
    }
}

/// OpenCode hashes in spawn_blocking, which cannot be cancelled once running.
/// Keep its shared cell inside that work so cancellation of an async waiter
/// cannot start a second hash of the same identity.
#[derive(Default)]
struct OpenCodeMcpFingerprintFlights {
    entries: Mutex<HashMap<CodexExecutableCacheIdentity, Weak<OnceLock<Option<(String, String)>>>>>,
}
impl OpenCodeMcpFingerprintFlights {
    async fn run(
        &self,
        identity: CodexExecutableCacheIdentity,
        probe: impl FnOnce() -> Option<(String, String)> + Send + 'static,
    ) -> Option<(String, String)> {
        let cell = {
            let mut entries = self.entries.lock().expect("OpenCode MCP flights poisoned");
            entries.retain(|_, entry| entry.strong_count() > 0);
            entries
                .get(&identity)
                .and_then(Weak::upgrade)
                .unwrap_or_else(|| {
                    let cell = Arc::new(OnceLock::new());
                    entries.insert(identity, Arc::downgrade(&cell));
                    cell
                })
        };
        let slot = local_mcp_probe_slots().clone().acquire_owned().await.ok()?;
        tokio::task::spawn_blocking(move || {
            let _slot = slot;
            cell.get_or_init(probe).clone()
        })
        .await
        .ok()
        .flatten()
    }
}

#[derive(Default)]
pub(crate) struct CodexMcpReadinessCache {
    entries: Mutex<HashMap<CodexExecutableCacheIdentity, CodexMcpLocalAttestation>>,
    flights: LocalMcpProbeFlights<CodexExecutableCacheIdentity, CodexMcpLocalAttestation>,
}

impl CodexMcpReadinessCache {
    pub(crate) fn get(
        &self,
        identity: &CodexExecutableCacheIdentity,
    ) -> Option<CodexMcpLocalAttestation> {
        self.entries
            .lock()
            .expect("Codex MCP readiness cache should not be poisoned")
            .get(identity)
            .cloned()
    }

    pub(crate) fn insert(&self, attestation: CodexMcpLocalAttestation) {
        self.entries
            .lock()
            .expect("Codex MCP readiness cache should not be poisoned")
            .insert(attestation.executable.cache_identity.clone(), attestation);
    }

    async fn get_or_probe<F>(
        &self,
        identity: CodexExecutableCacheIdentity,
        probe: impl FnOnce(tokio::sync::OwnedSemaphorePermit) -> F,
    ) -> Option<CodexMcpLocalAttestation>
    where
        F: std::future::Future<Output = Option<CodexMcpLocalAttestation>>,
    {
        if let Some(cached) = self.get(&identity) {
            return Some(cached);
        }
        self.flights
            .run(identity.clone(), |slot| async move {
                if let Some(cached) = self.get(&identity) {
                    return Some(cached);
                }
                let attestation = probe(slot).await?;
                if attestation.executable.cache_identity != identity {
                    return None;
                }
                if attestation.all_probes_passed() {
                    self.insert(attestation.clone());
                }
                Some(attestation)
            })
            .await
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries
            .lock()
            .expect("Codex MCP readiness cache should not be poisoned")
            .len()
    }
}

pub(crate) fn codex_mcp_probe_contract_hash() -> anyhow::Result<String> {
    sha256_json(&json!({
        "contractVersion": CODEX_MCP_LOCAL_PROBE_CONTRACT_VERSION,
        "provider": "codex",
        "schemaTransformContractVersion": CODEX_MCP_SCHEMA_TRANSFORM_CONTRACT_VERSION,
        "injection": "managed_stdio_mcp",
        "projectionUpdate": "restart_app_server_resume_thread",
        "localProbes": CodexMcpLocalProbeKind::ALL,
        "nativeEventFixture": include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../cli-agent-runtime/tests/fixtures/codex_mcp_lifecycle_0_144_1.json"
        )),
    }))
}

fn codex_mcp_probe_contract_hash_for_instance(
    instance: &EffectiveGatewayCliAgentRuntimeInstanceConfig,
    max_tools: usize,
    max_schema_bytes: usize,
) -> anyhow::Result<String> {
    let helper = crate::cli_runtime::config::resolve_current_pioneer_cli_mcp_helper()?;
    let helper_metadata = std::fs::metadata(helper.as_path())?;
    let helper_modified_unix_nanos = helper_metadata
        .modified()?
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| anyhow::anyhow!("Pioneer helper modification time predates Unix epoch"))?
        .as_nanos()
        .min(u64::MAX as u128) as u64;
    sha256_json(&json!({
        "baseContractHash": codex_mcp_probe_contract_hash()?,
        "gatewayPackageVersion": env!("CARGO_PKG_VERSION"),
        "helperRealpath": helper,
        "helperSize": helper_metadata.len(),
        "helperModifiedUnixNanos": helper_modified_unix_nanos,
        "runtimeId": instance.id,
        "binaryPath": instance.binary_path,
        "homePath": instance.home_path,
        "shadowHomePath": instance.shadow_home_path,
        "appServerArgs": instance.app_server_args,
        "startupProbeTimeoutMs": instance.startup_probe_timeout_ms,
        "requestTimeoutMs": instance.request_timeout_ms,
        "maxTools": max_tools,
        "maxSchemaBytes": max_schema_bytes,
    }))
}

fn global_codex_mcp_readiness_cache() -> &'static CodexMcpReadinessCache {
    static CACHE: OnceLock<CodexMcpReadinessCache> = OnceLock::new();
    CACHE.get_or_init(CodexMcpReadinessCache::default)
}

/// One production readiness path shared by catalog refresh and turn preflight.
/// It performs no account/model request. Exact executable and local protocol
/// checks establish support independently of background account diagnostics.
/// Release strings are diagnostic metadata; binary and contract identities are
/// the cache keys, so an absent account response cannot change a launch contract.
pub(crate) async fn codex_mcp_readiness_for_instance(
    instance: &EffectiveGatewayCliAgentRuntimeInstanceConfig,
    runtime_home: &Path,
    provider_version: Option<&str>,
    proxy_url: Option<&str>,
    max_tools: usize,
    max_schema_bytes: usize,
) -> CliMcpAdapterReadiness {
    if !instance.enabled {
        return evaluate_codex_mcp_readiness(CodexMcpReadinessInput {
            runtime_enabled: instance.enabled,
            platform_ipc_available: cfg!(any(unix, windows)),
            facade_supervisor_healthy: true,
            max_tools,
            max_schema_bytes,
            local_attestation: None,
        });
    }

    let local = build_or_load_codex_local_attestation(
        instance,
        runtime_home,
        "",
        proxy_url,
        max_tools,
        max_schema_bytes,
    )
    .await;
    let mut readiness = evaluate_codex_mcp_readiness(CodexMcpReadinessInput {
        runtime_enabled: instance.enabled,
        platform_ipc_available: cfg!(any(unix, windows)),
        facade_supervisor_healthy: true,
        max_tools,
        max_schema_bytes,
        local_attestation: local.as_ref(),
    });
    readiness.provider_version = provider_version.map(str::to_owned);
    readiness
}

async fn build_or_load_codex_local_attestation(
    instance: &EffectiveGatewayCliAgentRuntimeInstanceConfig,
    runtime_home: &Path,
    provider_version: &str,
    proxy_url: Option<&str>,
    max_tools: usize,
    max_schema_bytes: usize,
) -> Option<CodexMcpLocalAttestation> {
    let contract_hash =
        codex_mcp_probe_contract_hash_for_instance(instance, max_tools, max_schema_bytes).ok()?;
    let cache_identity = codex_executable_cache_identity(
        instance.binary_path.as_str(),
        provider_version,
        contract_hash.as_str(),
    )
    .ok()?;
    if let Some(cached) = global_codex_mcp_readiness_cache().get(&cache_identity) {
        return Some(cached);
    }
    let probe_identity = cache_identity.clone();
    let result = global_codex_mcp_readiness_cache()
        .get_or_probe(cache_identity.clone(), |slot| async move {
            let _slot = slot;
            probe_codex_local_attestation(
                instance,
                runtime_home,
                proxy_url,
                max_tools,
                max_schema_bytes,
                probe_identity,
            )
            .await
        })
        .await;
    // The executable/helper contract can change while this caller waits for
    // another probe or a slot. Never return its predecessor's confirmation.
    let current_contract =
        codex_mcp_probe_contract_hash_for_instance(instance, max_tools, max_schema_bytes).ok()?;
    let current_identity =
        codex_executable_cache_identity(&instance.binary_path, provider_version, &current_contract)
            .ok()?;
    (current_identity == cache_identity)
        .then_some(result)
        .flatten()
}

async fn probe_codex_local_attestation(
    instance: &EffectiveGatewayCliAgentRuntimeInstanceConfig,
    runtime_home: &Path,
    proxy_url: Option<&str>,
    max_tools: usize,
    max_schema_bytes: usize,
    cache_identity: CodexExecutableCacheIdentity,
) -> Option<CodexMcpLocalAttestation> {
    let contract_hash = cache_identity.probe_contract_hash.clone();
    let provider_version = cache_identity.provider_version.as_str();
    let executable = attest_codex_executable_identity(cache_identity.clone()).ok()?;
    let provider_probe = crate::cli_runtime::codex_session::run_codex_mcp_local_provider_probe(
        instance,
        runtime_home,
        proxy_url,
        max_tools,
    )
    .await;
    let provider_detail = provider_probe
        .as_ref()
        .err()
        .map(|error| bound_safe_detail(format!("{error:#}").as_str()));
    let unknown_request = validate_unknown_codex_request_response().await;
    let decoder_fixtures = validate_recorded_codex_mcp_decoder_fixtures();
    let mut probes = BTreeMap::new();
    for probe in [
        CodexMcpLocalProbeKind::ExactRawToolFiltering,
        CodexMcpLocalProbeKind::ControlledConfigIsolation,
        CodexMcpLocalProbeKind::UniqueOverlayIsolation,
        CodexMcpLocalProbeKind::SharedStateContinuity,
        CodexMcpLocalProbeKind::RequiredBridgeListBarrier,
        CodexMcpLocalProbeKind::UnmanagedMcpExcluded,
        CodexMcpLocalProbeKind::HelperAttachCancellationCleanup,
    ] {
        probes.insert(
            probe,
            match provider_detail.as_deref() {
                Some(detail) => CodexMcpLocalProbeResult::failed(detail),
                None => CodexMcpLocalProbeResult::passed(),
            },
        );
    }
    probes.insert(
        CodexMcpLocalProbeKind::UnknownRequestResponse,
        match unknown_request {
            Ok(()) => CodexMcpLocalProbeResult::passed(),
            Err(error) => CodexMcpLocalProbeResult::failed(format!("{error:#}")),
        },
    );
    probes.insert(
        CodexMcpLocalProbeKind::NativeEventDecoderFixtures,
        match decoder_fixtures {
            Ok(()) => CodexMcpLocalProbeResult::passed(),
            Err(error) => CodexMcpLocalProbeResult::failed(format!("{error:#}")),
        },
    );
    let provider_evidence = provider_probe.ok();
    let adapter_contract_fingerprint = sha256_json(&json!({
        "probeContractHash": contract_hash,
        "gatewayPackageVersion": env!("CARGO_PKG_VERSION"),
        "providerVersion": provider_version,
        "appServerArgs": instance.app_server_args,
        "injection": "managed_stdio_mcp",
        "projectionUpdate": "restart_app_server_resume_thread",
        "maxTools": max_tools,
        "maxSchemaBytes": max_schema_bytes,
    }))
    .ok()?;
    let isolation_contract_fingerprint = sha256_json(&json!({
        "contract": "codex-controlled-home-config-read-v1",
        "artifactDigest": provider_evidence
            .as_ref()
            .map(|evidence| evidence.config_artifact_digest.as_str()),
        "effectiveMcpServersFingerprint": provider_evidence
            .as_ref()
            .map(|evidence| evidence.effective_mcp_servers_fingerprint.as_str()),
        "overlayPolicyVersion": provider_evidence
            .as_ref()
            .map(|evidence| evidence.overlay_policy_version),
        "toolCount": provider_evidence
            .as_ref()
            .map(|evidence| evidence.tool_count),
        "maxConfigOrigins": provider_evidence
            .as_ref()
            .map(|evidence| evidence.max_config_origins),
    }))
    .ok()?;
    let schema_contract_fingerprint = sha256_json(&json!({
        "contract": "codex-schema-transform-and-native-decoder-v1",
        "fixture": include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../cli-agent-runtime/tests/fixtures/codex_mcp_lifecycle_0_144_1.json"
        )),
    }))
    .ok()?;
    let bridge_contract_fingerprint = sha256_json(&json!({
        "contract": "pioneer-private-mcp-bridge-required-list-v1",
        "projectionFingerprint": provider_evidence
            .as_ref()
            .map(|evidence| evidence.projection_fingerprint.as_str()),
        "semanticRestartFingerprint": provider_evidence
            .as_ref()
            .map(|evidence| evidence.semantic_restart_fingerprint.as_str()),
        "helperBinarySha256": provider_evidence
            .as_ref()
            .map(|evidence| evidence.helper_binary_sha256.as_str()),
        "toolCount": provider_evidence
            .as_ref()
            .map(|evidence| evidence.tool_count),
    }))
    .ok()?;
    let attestation = CodexMcpLocalAttestation::build(
        executable,
        adapter_contract_fingerprint,
        isolation_contract_fingerprint,
        schema_contract_fingerprint,
        bridge_contract_fingerprint,
        probes,
    )
    .ok()?;
    let current_contract =
        codex_mcp_probe_contract_hash_for_instance(instance, max_tools, max_schema_bytes).ok()?;
    let current_identity =
        codex_executable_cache_identity(&instance.binary_path, provider_version, &current_contract)
            .ok()?;
    if current_identity != cache_identity {
        return None;
    }
    Some(attestation)
}

pub(crate) const CLAUDE_MCP_LOCAL_PROBE_CONTRACT_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ClaudeMcpLocalProbeKind {
    BinaryHelpReservedFlags,
    StrictConfigLaunch,
    ManagedSentinelAttachCleanup,
    UnmanagedMcpExcluded,
    SafeModeStrictConfigMatrix,
    OwnerOnlyArtifacts,
    NativeEventDecoderFixtures,
    ResumeContinuationContract,
}

impl ClaudeMcpLocalProbeKind {
    pub(crate) const ALL: [Self; 8] = [
        Self::BinaryHelpReservedFlags,
        Self::StrictConfigLaunch,
        Self::ManagedSentinelAttachCleanup,
        Self::UnmanagedMcpExcluded,
        Self::SafeModeStrictConfigMatrix,
        Self::OwnerOnlyArtifacts,
        Self::NativeEventDecoderFixtures,
        Self::ResumeContinuationContract,
    ];

    const fn diagnostic(self) -> (&'static str, &'static str) {
        match self {
            Self::BinaryHelpReservedFlags => (
                "cli_runtime.mcp.claude_flag_contract_failed",
                "Claude managed launch flag contract self-probe failed",
            ),
            Self::StrictConfigLaunch => (
                "cli_runtime.mcp.strict_isolation_failed",
                "Claude strict generated MCP config launch self-probe failed",
            ),
            Self::ManagedSentinelAttachCleanup => (
                "cli_runtime.mcp.bridge_unavailable",
                "Claude managed stdio helper lifecycle self-probe failed",
            ),
            Self::UnmanagedMcpExcluded => (
                "cli_runtime.mcp.strict_isolation_failed",
                "Unmanaged Claude MCP exclusion self-probe failed",
            ),
            Self::SafeModeStrictConfigMatrix => (
                "cli_runtime.mcp.claude_safe_mode_contract_failed",
                "Claude safe-mode and strict-config launch matrix self-probe failed",
            ),
            Self::OwnerOnlyArtifacts => (
                "cli_runtime.mcp.claude_artifact_hygiene_failed",
                "Claude managed MCP artifact hygiene self-probe failed",
            ),
            Self::NativeEventDecoderFixtures => (
                "cli_runtime.mcp.claude_decoder_fixture_failed",
                "Claude native MCP event decoder fixture self-probe failed",
            ),
            Self::ResumeContinuationContract => (
                "cli_runtime.mcp.claude_resume_contract_failed",
                "Claude real provider-session continuation self-probe failed",
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ClaudeMcpLocalProbeResult {
    pub(crate) passed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) safe_detail: Option<String>,
}

impl ClaudeMcpLocalProbeResult {
    pub(crate) fn passed() -> Self {
        Self {
            passed: true,
            safe_detail: None,
        }
    }

    pub(crate) fn failed(detail: impl Into<String>) -> Self {
        Self {
            passed: false,
            safe_detail: Some(bound_safe_detail(detail.into().as_str())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ClaudeMcpLocalAttestation {
    pub(crate) contract_version: u32,
    pub(crate) executable: ClaudeExecutableAttestation,
    pub(crate) adapter_contract_fingerprint: String,
    pub(crate) isolation_contract_fingerprint: String,
    pub(crate) schema_contract_fingerprint: String,
    pub(crate) bridge_contract_fingerprint: String,
    pub(crate) continuation_contract_fingerprint: String,
    pub(crate) local_attestation_fingerprint: String,
    pub(crate) probes: BTreeMap<ClaudeMcpLocalProbeKind, ClaudeMcpLocalProbeResult>,
}

impl ClaudeMcpLocalAttestation {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn build(
        executable: ClaudeExecutableAttestation,
        adapter_contract_fingerprint: String,
        isolation_contract_fingerprint: String,
        schema_contract_fingerprint: String,
        bridge_contract_fingerprint: String,
        continuation_contract_fingerprint: String,
        probes: BTreeMap<ClaudeMcpLocalProbeKind, ClaudeMcpLocalProbeResult>,
    ) -> anyhow::Result<Self> {
        for (label, value) in [
            ("adapter contract", adapter_contract_fingerprint.as_str()),
            (
                "isolation contract",
                isolation_contract_fingerprint.as_str(),
            ),
            ("schema contract", schema_contract_fingerprint.as_str()),
            ("bridge contract", bridge_contract_fingerprint.as_str()),
            (
                "continuation contract",
                continuation_contract_fingerprint.as_str(),
            ),
        ] {
            validate_fingerprint(label, value)?;
        }
        for probe in ClaudeMcpLocalProbeKind::ALL {
            if !probes.contains_key(&probe) {
                anyhow::bail!("Claude local attestation is missing probe `{probe:?}`");
            }
        }
        if probes.len() != ClaudeMcpLocalProbeKind::ALL.len() {
            anyhow::bail!("Claude local attestation contains unknown probe entries");
        }
        let local_attestation_fingerprint = sha256_json(&json!({
            "contractVersion": CLAUDE_MCP_LOCAL_PROBE_CONTRACT_VERSION,
            "localExecutableFingerprint": executable.local_executable_fingerprint,
            "adapterContractFingerprint": adapter_contract_fingerprint,
            "isolationContractFingerprint": isolation_contract_fingerprint,
            "schemaContractFingerprint": schema_contract_fingerprint,
            "bridgeContractFingerprint": bridge_contract_fingerprint,
            "continuationContractFingerprint": continuation_contract_fingerprint,
            "probes": probes,
        }))?;
        Ok(Self {
            contract_version: CLAUDE_MCP_LOCAL_PROBE_CONTRACT_VERSION,
            executable,
            adapter_contract_fingerprint,
            isolation_contract_fingerprint,
            schema_contract_fingerprint,
            bridge_contract_fingerprint,
            continuation_contract_fingerprint,
            local_attestation_fingerprint,
            probes,
        })
    }

    pub(crate) fn all_probes_passed(&self) -> bool {
        ClaudeMcpLocalProbeKind::ALL
            .into_iter()
            .all(|probe| self.probes.get(&probe).is_some_and(|result| result.passed))
    }
}

pub(crate) struct ClaudeMcpReadinessInput<'a> {
    pub(crate) runtime_enabled: bool,
    pub(crate) platform_ipc_available: bool,
    pub(crate) facade_supervisor_healthy: bool,
    pub(crate) max_tools: usize,
    pub(crate) max_schema_bytes: usize,
    pub(crate) local_attestation: Option<&'a ClaudeMcpLocalAttestation>,
}

pub(crate) fn evaluate_claude_mcp_readiness(
    input: ClaudeMcpReadinessInput<'_>,
) -> CliMcpAdapterReadiness {
    let local = input.local_attestation;
    let mut diagnostics = Vec::new();
    if !input.runtime_enabled {
        diagnostics.push(diagnostic(
            RuntimeDiagnosticLevel::Info,
            "cli_runtime.mcp.runtime_not_ready",
            "The Claude runtime is disabled",
        ));
    }
    if !input.platform_ipc_available || !input.facade_supervisor_healthy {
        diagnostics.push(diagnostic(
            RuntimeDiagnosticLevel::Error,
            "cli_runtime.mcp.bridge_unavailable",
            "The private Claude MCP bridge is unavailable",
        ));
    }
    match local {
        None => diagnostics.push(diagnostic(
            RuntimeDiagnosticLevel::Warning,
            "cli_runtime.mcp.provider_probe_failed",
            "Claude local MCP attestation is unavailable",
        )),
        Some(local) => {
            for probe in ClaudeMcpLocalProbeKind::ALL {
                let result = local.probes.get(&probe);
                if !result.is_some_and(|result| result.passed) {
                    let (code, base) = probe.diagnostic();
                    let message = result
                        .and_then(|result| result.safe_detail.as_deref())
                        .map(|detail| format!("{base}: {detail}"))
                        .unwrap_or_else(|| base.to_owned());
                    diagnostics.push(diagnostic(RuntimeDiagnosticLevel::Error, code, &message));
                }
            }
        }
    }
    let supported = input.runtime_enabled
        && input.platform_ipc_available
        && input.facade_supervisor_healthy
        && local.is_some_and(ClaudeMcpLocalAttestation::all_probes_passed);
    if supported {
        diagnostics.push(diagnostic(
            RuntimeDiagnosticLevel::Info,
            "cli_runtime.mcp.ready",
            "Claude MCP tools passed exact local technical readiness checks",
        ));
    }
    CliMcpAdapterReadiness {
        supported,
        injection: CliMcpInjectionKind::ClaudeStrictStdioMcp,
        projection_update: CliMcpProjectionUpdateKind::ClaudeRestartProcessResumeSession,
        strict_isolation: local.is_some_and(|local| {
            [
                ClaudeMcpLocalProbeKind::StrictConfigLaunch,
                ClaudeMcpLocalProbeKind::UnmanagedMcpExcluded,
                ClaudeMcpLocalProbeKind::SafeModeStrictConfigMatrix,
                ClaudeMcpLocalProbeKind::OwnerOnlyArtifacts,
            ]
            .into_iter()
            .all(|probe| local.probes.get(&probe).is_some_and(|result| result.passed))
        }),
        contract_fingerprint: local
            .map(|local| local.adapter_contract_fingerprint.clone())
            .unwrap_or_default(),
        local_executable_fingerprint: local
            .map(|local| local.executable.local_executable_fingerprint.clone())
            .unwrap_or_default(),
        provider_version: local
            .map(|local| local.executable.cache_identity.provider_version.clone()),
        max_tools: input.max_tools,
        max_schema_bytes: input.max_schema_bytes,
        diagnostics,
    }
}

#[derive(Default)]
pub(crate) struct ClaudeMcpReadinessCache {
    entries: Mutex<HashMap<ClaudeExecutableCacheIdentity, ClaudeMcpLocalAttestation>>,
    flights: LocalMcpProbeFlights<ClaudeExecutableCacheIdentity, ClaudeMcpLocalAttestation>,
}

impl ClaudeMcpReadinessCache {
    fn get(&self, identity: &ClaudeExecutableCacheIdentity) -> Option<ClaudeMcpLocalAttestation> {
        self.entries
            .lock()
            .expect("Claude MCP readiness cache should not be poisoned")
            .get(identity)
            .cloned()
    }

    fn insert(&self, attestation: ClaudeMcpLocalAttestation) {
        self.entries
            .lock()
            .expect("Claude MCP readiness cache should not be poisoned")
            .insert(attestation.executable.cache_identity.clone(), attestation);
    }

    async fn get_or_probe<F>(
        &self,
        identity: ClaudeExecutableCacheIdentity,
        probe: impl FnOnce(tokio::sync::OwnedSemaphorePermit) -> F,
    ) -> Option<ClaudeMcpLocalAttestation>
    where
        F: std::future::Future<Output = Option<ClaudeMcpLocalAttestation>>,
    {
        if let Some(cached) = self.get(&identity) {
            return Some(cached);
        }
        self.flights
            .run(identity.clone(), |slot| async move {
                if let Some(cached) = self.get(&identity) {
                    return Some(cached);
                }
                let attestation = probe(slot).await?;
                if attestation.executable.cache_identity != identity {
                    return None;
                }
                if attestation.all_probes_passed() {
                    self.insert(attestation.clone());
                }
                Some(attestation)
            })
            .await
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries
            .lock()
            .expect("Claude MCP readiness cache should not be poisoned")
            .len()
    }
}

pub(crate) fn claude_mcp_probe_contract_hash() -> anyhow::Result<String> {
    sha256_json(&json!({
        "contractVersion": CLAUDE_MCP_LOCAL_PROBE_CONTRACT_VERSION,
        "provider": "claude",
        "schemaTransformContractVersion": CLAUDE_MCP_SCHEMA_TRANSFORM_CONTRACT_VERSION,
        "injection": "strict_stdio_mcp",
        "projectionUpdate": "restart_process_resume_session",
        "localProbes": ClaudeMcpLocalProbeKind::ALL,
        "nativeEventFixture": include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../cli-agent-runtime/tests/fixtures/claude_mcp/lifecycle.json"
        )),
        "schemaFixture": include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../cli-agent-runtime/tests/fixtures/claude_mcp_schema_2_1_197_contract.json"
        )),
    }))
}

fn claude_mcp_probe_contract_hash_for_instance(
    instance: &EffectiveGatewayCliAgentRuntimeInstanceConfig,
) -> anyhow::Result<String> {
    let helper = crate::cli_runtime::config::resolve_current_pioneer_cli_mcp_helper()?;
    let helper_metadata = std::fs::metadata(helper.as_path())?;
    let helper_modified_unix_nanos = helper_metadata
        .modified()?
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| anyhow::anyhow!("Pioneer helper modification time predates Unix epoch"))?
        .as_nanos()
        .min(u64::MAX as u128) as u64;
    sha256_json(&json!({
        "baseContractHash": claude_mcp_probe_contract_hash()?,
        "gatewayPackageVersion": env!("CARGO_PKG_VERSION"),
        "helperRealpath": helper,
        "helperSize": helper_metadata.len(),
        "helperModifiedUnixNanos": helper_modified_unix_nanos,
        "runtimeId": instance.id,
        "binaryPath": instance.binary_path,
        "homePath": instance.home_path,
        "shadowHomePath": instance.shadow_home_path,
        "appServerArgs": instance.app_server_args,
        "startupProbeTimeoutMs": instance.startup_probe_timeout_ms,
        "requestTimeoutMs": instance.request_timeout_ms,
    }))
}

fn global_claude_mcp_readiness_cache() -> &'static ClaudeMcpReadinessCache {
    static CACHE: OnceLock<ClaudeMcpReadinessCache> = OnceLock::new();
    CACHE.get_or_init(ClaudeMcpReadinessCache::default)
}

pub(crate) async fn claude_mcp_readiness_for_instance(
    instance: &EffectiveGatewayCliAgentRuntimeInstanceConfig,
    runtime_home: &Path,
    provider_version: Option<&str>,
    proxy_url: Option<&str>,
    max_tools: usize,
    max_schema_bytes: usize,
) -> CliMcpAdapterReadiness {
    if !instance.enabled {
        return evaluate_claude_mcp_readiness(ClaudeMcpReadinessInput {
            runtime_enabled: instance.enabled,
            platform_ipc_available: cfg!(any(unix, windows)),
            facade_supervisor_healthy: true,
            max_tools,
            max_schema_bytes,
            local_attestation: None,
        });
    }
    let local = build_or_load_claude_local_attestation(instance, runtime_home, "", proxy_url).await;
    let mut readiness = evaluate_claude_mcp_readiness(ClaudeMcpReadinessInput {
        runtime_enabled: instance.enabled,
        platform_ipc_available: cfg!(any(unix, windows)),
        facade_supervisor_healthy: true,
        max_tools,
        max_schema_bytes,
        local_attestation: local.as_ref(),
    });
    readiness.provider_version = provider_version.map(str::to_owned);
    readiness
}

async fn build_or_load_claude_local_attestation(
    instance: &EffectiveGatewayCliAgentRuntimeInstanceConfig,
    runtime_home: &Path,
    provider_version: &str,
    proxy_url: Option<&str>,
) -> Option<ClaudeMcpLocalAttestation> {
    let contract_hash = claude_mcp_probe_contract_hash_for_instance(instance).ok()?;
    let cache_identity = claude_executable_cache_identity(
        instance.binary_path.as_str(),
        provider_version,
        contract_hash.as_str(),
    )
    .ok()?;
    if let Some(cached) = global_claude_mcp_readiness_cache().get(&cache_identity) {
        return Some(cached);
    }
    let probe_identity = cache_identity.clone();
    let result = global_claude_mcp_readiness_cache()
        .get_or_probe(cache_identity.clone(), |slot| async move {
            let _slot = slot;
            probe_claude_local_attestation(instance, runtime_home, proxy_url, probe_identity).await
        })
        .await;
    // The executable/helper contract can change while this caller waits for
    // another probe or a slot. Never return its predecessor's confirmation.
    let current_contract = claude_mcp_probe_contract_hash_for_instance(instance).ok()?;
    let current_identity = claude_executable_cache_identity(
        &instance.binary_path,
        provider_version,
        &current_contract,
    )
    .ok()?;
    (current_identity == cache_identity)
        .then_some(result)
        .flatten()
}

async fn probe_claude_local_attestation(
    instance: &EffectiveGatewayCliAgentRuntimeInstanceConfig,
    runtime_home: &Path,
    proxy_url: Option<&str>,
    cache_identity: ClaudeExecutableCacheIdentity,
) -> Option<ClaudeMcpLocalAttestation> {
    let contract_hash = cache_identity.probe_contract_hash.clone();
    let provider_version = cache_identity.provider_version.as_str();
    let executable = attest_claude_executable_identity(cache_identity.clone()).ok()?;
    let wait = std::time::Duration::from_millis(instance.startup_probe_timeout_ms.max(1));
    let help = probe_claude_help_contract(executable.cache_identity.realpath.as_path(), wait).await;
    let provider_probe = crate::cli_runtime::claude_session::run_claude_mcp_local_provider_probe(
        instance,
        runtime_home,
        proxy_url,
    )
    .await;
    let decoder = validate_recorded_claude_mcp_decoder_fixtures();
    let continuation = claude_continuation_contract_fingerprint();
    let provider_detail = provider_probe
        .as_ref()
        .err()
        .map(|error| bound_safe_detail(format!("{error:#}").as_str()));
    let mut probes = BTreeMap::new();
    probes.insert(
        ClaudeMcpLocalProbeKind::BinaryHelpReservedFlags,
        match &help {
            Ok(_) => ClaudeMcpLocalProbeResult::passed(),
            Err(error) => ClaudeMcpLocalProbeResult::failed(format!("{error:#}")),
        },
    );
    for probe in [
        ClaudeMcpLocalProbeKind::StrictConfigLaunch,
        ClaudeMcpLocalProbeKind::ManagedSentinelAttachCleanup,
        ClaudeMcpLocalProbeKind::UnmanagedMcpExcluded,
        ClaudeMcpLocalProbeKind::SafeModeStrictConfigMatrix,
        ClaudeMcpLocalProbeKind::OwnerOnlyArtifacts,
    ] {
        probes.insert(
            probe,
            match provider_detail.as_deref() {
                Some(detail) => ClaudeMcpLocalProbeResult::failed(detail),
                None => ClaudeMcpLocalProbeResult::passed(),
            },
        );
    }
    probes.insert(
        ClaudeMcpLocalProbeKind::NativeEventDecoderFixtures,
        match decoder {
            Ok(()) => ClaudeMcpLocalProbeResult::passed(),
            Err(error) => ClaudeMcpLocalProbeResult::failed(format!("{error:#}")),
        },
    );
    probes.insert(
        ClaudeMcpLocalProbeKind::ResumeContinuationContract,
        match &continuation {
            Ok(_) => ClaudeMcpLocalProbeResult::passed(),
            Err(error) => ClaudeMcpLocalProbeResult::failed(format!("{error:#}")),
        },
    );
    let provider_evidence = provider_probe.ok();
    let help_evidence = help.ok();
    let continuation_contract_fingerprint = continuation.ok()?;
    let adapter_contract_fingerprint = sha256_json(&json!({
        "probeContractHash": contract_hash,
        "gatewayPackageVersion": env!("CARGO_PKG_VERSION"),
        "providerVersion": provider_version,
        "helpSha256": help_evidence.as_ref().map(|evidence| evidence.help_sha256.as_str()),
        "requiredFlags": help_evidence.as_ref().map(|evidence| evidence.required_flags.as_slice()),
        "hiddenManagedFlags": help_evidence
            .as_ref()
            .map(|evidence| evidence.hidden_managed_flags.as_slice()),
        "appServerArgs": instance.app_server_args,
        "injection": "strict_stdio_mcp",
        "projectionUpdate": "restart_process_resume_session",
    }))
    .ok()?;
    let isolation_contract_fingerprint = sha256_json(&json!({
        "contract": "claude-strict-generated-config-v1",
        "configArtifactDigest": provider_evidence
            .as_ref()
            .map(|evidence| evidence.config_artifact_digest.as_str()),
        "strictLaunchFingerprint": provider_evidence
            .as_ref()
            .map(|evidence| evidence.strict_launch_fingerprint.as_str()),
    }))
    .ok()?;
    let schema_contract_fingerprint = sha256_json(&json!({
        "contract": "claude-schema-transform-and-native-decoder-v1",
        "fixture": include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../cli-agent-runtime/tests/fixtures/claude_mcp_schema_2_1_197_contract.json"
        )),
    }))
    .ok()?;
    let bridge_contract_fingerprint = sha256_json(&json!({
        "contract": "pioneer-private-claude-mcp-bridge-v1",
        "projectionFingerprint": provider_evidence
            .as_ref()
            .map(|evidence| evidence.projection_fingerprint.as_str()),
        "helperBinarySha256": provider_evidence
            .as_ref()
            .map(|evidence| evidence.helper_binary_sha256.as_str()),
    }))
    .ok()?;
    if provider_evidence.as_ref().is_some_and(|evidence| {
        evidence.continuation_fingerprint != continuation_contract_fingerprint
    }) {
        probes.insert(
            ClaudeMcpLocalProbeKind::ResumeContinuationContract,
            ClaudeMcpLocalProbeResult::failed("spawn-time continuation fingerprint mismatch"),
        );
    }
    let attestation = ClaudeMcpLocalAttestation::build(
        executable,
        adapter_contract_fingerprint,
        isolation_contract_fingerprint,
        schema_contract_fingerprint,
        bridge_contract_fingerprint,
        continuation_contract_fingerprint,
        probes,
    )
    .ok()?;
    let current_contract = claude_mcp_probe_contract_hash_for_instance(instance).ok()?;
    let current_identity = claude_executable_cache_identity(
        &instance.binary_path,
        provider_version,
        &current_contract,
    )
    .ok()?;
    if current_identity != cache_identity {
        return None;
    }
    Some(attestation)
}

/// OpenCode MCP readiness pins the exact executable and adapter contract;
/// session startup then verifies dynamic MCP registration and the private
/// helper list barrier before admitting a turn.
pub(crate) async fn opencode_mcp_readiness_for_instance(
    instance: &EffectiveGatewayCliAgentRuntimeInstanceConfig,
    provider_version: Option<&str>,
    max_tools: usize,
    max_schema_bytes: usize,
) -> CliMcpAdapterReadiness {
    let mut diagnostics = Vec::new();
    if !instance.enabled {
        diagnostics.push(diagnostic(
            RuntimeDiagnosticLevel::Info,
            "cli_runtime.mcp.runtime_not_ready",
            "The OpenCode runtime is disabled",
        ));
    }
    if !cfg!(any(unix, windows)) {
        diagnostics.push(diagnostic(
            RuntimeDiagnosticLevel::Error,
            "cli_runtime.mcp.bridge_unavailable",
            "Private MCP bridge IPC is unavailable on this platform",
        ));
    }

    let version = provider_version
        .map(str::trim)
        .filter(|version| !version.is_empty());
    let fingerprints = if instance.enabled {
        let contract =
            crate::cli_runtime::opencode_session::opencode_provider_contract_fingerprint();
        let identity = codex_executable_cache_identity(&instance.binary_path, "", &contract).ok();
        if let Some(identity) = identity {
            static FLIGHTS: OnceLock<OpenCodeMcpFingerprintFlights> = OnceLock::new();
            let configured_binary = instance.binary_path.clone();
            let result = FLIGHTS
                .get_or_init(OpenCodeMcpFingerprintFlights::default)
                .run(identity.clone(), move || {
                    opencode_executable_fingerprints(&configured_binary, None).ok()
                })
                .await;
            let current =
                codex_executable_cache_identity(&instance.binary_path, "", &contract).ok();
            if current.as_ref() == Some(&identity) {
                result
            } else {
                None
            }
        } else {
            None
        }
    } else {
        None
    };
    if fingerprints.is_none() && instance.enabled {
        diagnostics.push(diagnostic(
            RuntimeDiagnosticLevel::Error,
            "cli_runtime.mcp.provider_probe_failed",
            "OpenCode executable attestation is unavailable",
        ));
    }
    let supported = instance.enabled && cfg!(any(unix, windows)) && fingerprints.is_some();
    if supported {
        diagnostics.push(diagnostic(
            RuntimeDiagnosticLevel::Info,
            "cli_runtime.mcp.ready",
            "OpenCode MCP executable and adapter contract are available; session barriers remain required",
        ));
    }
    let (contract_fingerprint, local_executable_fingerprint) = fingerprints.unwrap_or_default();
    CliMcpAdapterReadiness {
        supported,
        injection: CliMcpInjectionKind::OpenCodeDynamicStdioMcp,
        projection_update: CliMcpProjectionUpdateKind::OpenCodeRestartServerResumeSession,
        strict_isolation: supported,
        contract_fingerprint,
        local_executable_fingerprint,
        provider_version: version.map(str::to_owned),
        max_tools,
        max_schema_bytes,
        diagnostics,
    }
}

pub(crate) fn opencode_executable_fingerprints(
    configured_binary: &str,
    _version: Option<&str>,
) -> anyhow::Result<(String, String)> {
    let base_contract =
        crate::cli_runtime::opencode_session::opencode_provider_contract_fingerprint();
    let identity = codex_executable_cache_identity(configured_binary, "", &base_contract)?;
    let binary_sha256 = sha256_file_contents(identity.realpath.as_path())?;
    let local_executable_fingerprint = sha256_json(&json!({
        "provider": "opencode",
        "realpath": identity.realpath,
        "size": identity.size,
        "modifiedUnixNanos": identity.modified_unix_nanos,
        "binarySha256": binary_sha256,
    }))?;
    let adapter_contract_fingerprint = sha256_json(&json!({
        "baseContract": base_contract,
        "localExecutableFingerprint": local_executable_fingerprint,
        "strictIsolation": "v2_runtime_mcp_override_and_session_permissions",
        "dynamicMcpServer": "pioneer",
        "projectionUpdate": "restart_server_resume_session",
    }))?;
    Ok((adapter_contract_fingerprint, local_executable_fingerprint))
}

fn diagnostic(level: RuntimeDiagnosticLevel, code: &str, message: &str) -> RuntimeDiagnostic {
    RuntimeDiagnostic {
        level,
        code: code.to_owned(),
        message: bound_safe_detail(message),
    }
}

fn bound_safe_detail(value: &str) -> String {
    value
        .chars()
        .filter(|character| !character.is_control() || *character == '\t')
        .take(300)
        .collect()
}

fn validate_fingerprint(label: &str, value: &str) -> anyhow::Result<()> {
    if !is_fingerprint(value) {
        anyhow::bail!("{label} must be a SHA-256 fingerprint");
    }
    Ok(())
}

fn is_fingerprint(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_cli_agent_runtime::codex_attestation::CodexExecutableCacheIdentity;
    use pioneer_config::GatewayCliAgentRuntimeKindConfig;
    use std::path::PathBuf;

    fn all_passed_probes() -> BTreeMap<CodexMcpLocalProbeKind, CodexMcpLocalProbeResult> {
        CodexMcpLocalProbeKind::ALL
            .into_iter()
            .map(|probe| (probe, CodexMcpLocalProbeResult::passed()))
            .collect()
    }

    fn local_attestation(seed: char) -> CodexMcpLocalAttestation {
        let fingerprint = seed.to_string().repeat(64);
        CodexMcpLocalAttestation::build(
            CodexExecutableAttestation {
                cache_identity: CodexExecutableCacheIdentity {
                    realpath: PathBuf::from("/opt/codex"),
                    size: 42,
                    modified_unix_nanos: 7,
                    provider_version: "0.144.1".to_owned(),
                    probe_contract_hash: "9".repeat(64),
                },
                binary_sha256: "8".repeat(64),
                platform: "macos".to_owned(),
                architecture: "aarch64".to_owned(),
                local_executable_fingerprint: fingerprint.clone(),
            },
            fingerprint,
            "2".repeat(64),
            "3".repeat(64),
            "4".repeat(64),
            all_passed_probes(),
        )
        .expect("local attestation")
    }

    fn runtime_instance() -> EffectiveGatewayCliAgentRuntimeInstanceConfig {
        EffectiveGatewayCliAgentRuntimeInstanceConfig {
            id: "codex".to_owned(),
            kind: GatewayCliAgentRuntimeKindConfig::Codex,
            display_name: "Codex".to_owned(),
            enabled: true,
            binary_path: "codex".to_owned(),
            home_path: "~/.codex".to_owned(),
            shadow_home_path: None,
            custom_models: Vec::new(),
            app_server_args: Vec::new(),
            startup_probe_timeout_ms: 5_000,
            request_timeout_ms: 5_000,
            idle_session_ttl_secs: 300,
            event_channel_capacity: 64,
            stderr_ring_lines: 64,
            debug_native_events: false,
        }
    }

    fn input(local: Option<&CodexMcpLocalAttestation>) -> CodexMcpReadinessInput<'_> {
        CodexMcpReadinessInput {
            runtime_enabled: true,
            platform_ipc_available: true,
            facade_supervisor_healthy: true,
            max_tools: 128,
            max_schema_bytes: 1_048_576,
            local_attestation: local,
        }
    }

    #[tokio::test]
    async fn technical_cache_coalesces_success_failure_and_releases_cancelled_leaders() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        macro_rules! verify_cache {
            ($cache:ident, $fixture:ident) => {{
                for fail in [false, true] {
                    let cache = $cache {
                        flights: LocalMcpProbeFlights { slots: Arc::new(tokio::sync::Semaphore::new(2)), ..Default::default() },
                        ..Default::default()
                    };
                    let local = $fixture('1');
                    let id = local.executable.cache_identity.clone();
                    let calls = AtomicUsize::new(0);
                    let (release, wait) = tokio::sync::oneshot::channel();
                    let mut background = Box::pin(cache.get_or_probe(id.clone(), |slot| async {
                        let _slot = slot;
                        calls.fetch_add(1, Ordering::SeqCst);
                        wait.await.unwrap();
                        if fail { None } else { Some(local.clone()) }
                    }));
                    let mut preflight = Box::pin(cache.get_or_probe(id.clone(), |slot| async {
                        let _slot = slot;
                        calls.fetch_add(1, Ordering::SeqCst);
                        Some(local.clone())
                    }));
                    assert!(futures_util::poll!(background.as_mut()).is_pending());
                    assert!(futures_util::poll!(preflight.as_mut()).is_pending());
                    assert_eq!(calls.load(Ordering::SeqCst), 1);
                    release.send(()).unwrap();
                    let (a, b) = tokio::join!(background, preflight);
                    assert_eq!(a, b);
                    assert_eq!(a.is_none(), fail);
                    assert_eq!(calls.load(Ordering::SeqCst), 1);
                    assert_eq!(cache.get(&id).is_none(), fail);
                    let recovered = cache.get_or_probe(id.clone(), |slot| async {
                        let _slot = slot;
                        calls.fetch_add(1, Ordering::SeqCst);
                        Some(local.clone())
                    }).await;
                    assert!(recovered.is_some());
                    assert_eq!(calls.load(Ordering::SeqCst), if fail { 2 } else { 1 });
                    let mut changed = id.clone();
                    changed.probe_contract_hash = "b".repeat(64);
                    assert!(cache.get_or_probe(changed, |slot| async {
                        let _slot = slot;
                        Some(local.clone()) // A foreign result must not be published for the new key.
                    }).await.is_none());
                }
                let cache = $cache {
                    flights: LocalMcpProbeFlights { slots: Arc::new(tokio::sync::Semaphore::new(2)), ..Default::default() },
                    ..Default::default()
                };
                let local = $fixture('1');
                let id = local.executable.cache_identity.clone();
                let calls = AtomicUsize::new(0);
                let mut cancelled = Box::pin(cache.get_or_probe(id.clone(), |slot| async {
                    let _slot = slot;
                    calls.fetch_add(1, Ordering::SeqCst);
                    std::future::pending().await
                }));
                let mut survivor = Box::pin(cache.get_or_probe(id, |slot| async {
                    let _slot = slot;
                    calls.fetch_add(1, Ordering::SeqCst);
                    Some(local.clone())
                }));
                assert!(futures_util::poll!(cancelled.as_mut()).is_pending());
                assert!(futures_util::poll!(survivor.as_mut()).is_pending());
                drop(cancelled);
                assert!(tokio::time::timeout(std::time::Duration::from_secs(1), survivor).await
                    .expect("cancellation must release initialization").is_some());
                assert_eq!(calls.load(Ordering::SeqCst), 2);
            }};
        }
        verify_cache!(CodexMcpReadinessCache, local_attestation);
        verify_cache!(ClaudeMcpReadinessCache, claude_local_attestation);
    }

    #[tokio::test]
    async fn technical_probe_slots_bound_distinct_identities_and_recover_on_cancellation() {
        let flights = LocalMcpProbeFlights::<u8, u8> {
            slots: Arc::new(tokio::sync::Semaphore::new(2)),
            ..Default::default()
        };
        let (a_tx, a_rx) = tokio::sync::oneshot::channel::<()>();
        let (b_tx, b_rx) = tokio::sync::oneshot::channel::<()>();
        let (c_tx, c_rx) = tokio::sync::oneshot::channel::<()>();
        let mut a = Box::pin(flights.run(1, |slot| async move {
            let _slot = slot;
            a_rx.await.ok()?;
            Some(1)
        }));
        let mut b = Box::pin(flights.run(2, |slot| async move {
            let _slot = slot;
            b_rx.await.ok()?;
            Some(2)
        }));
        let mut c = Box::pin(flights.run(3, |slot| async move {
            let _slot = slot;
            c_rx.await.ok()?;
            Some(3)
        }));
        assert!(futures_util::poll!(a.as_mut()).is_pending());
        assert!(futures_util::poll!(b.as_mut()).is_pending());
        assert!(futures_util::poll!(c.as_mut()).is_pending());
        assert_eq!(flights.slots.available_permits(), 0);
        // c has not consumed its receiver yet: even a released barrier cannot finish it without a slot.
        c_tx.send(()).unwrap();
        assert!(futures_util::poll!(c.as_mut()).is_pending());
        drop(a);
        assert_eq!(c.await, Some(3));
        b_tx.send(()).unwrap();
        assert_eq!(b.await, Some(2));
        assert!(a_tx.send(()).is_err());
        assert_eq!(flights.slots.available_permits(), 2);
    }

    #[tokio::test]
    async fn opencode_hash_remains_shared_when_its_async_leader_is_cancelled() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let flights = OpenCodeMcpFingerprintFlights::default();
        let id = local_attestation('1').executable.cache_identity;
        let calls = Arc::new(AtomicUsize::new(0));
        let (started, ready) = tokio::sync::oneshot::channel();
        let (release, wait) = std::sync::mpsc::sync_channel(1);
        let first_calls = calls.clone();
        let mut leader = Box::pin(flights.run(id.clone(), move || {
            first_calls.fetch_add(1, Ordering::SeqCst);
            started.send(()).unwrap();
            wait.recv().unwrap();
            Some(("current".into(), "binary".into()))
        }));
        assert!(futures_util::poll!(leader.as_mut()).is_pending());
        ready.await.unwrap();
        drop(leader);
        let second_calls = calls.clone();
        let mut survivor = Box::pin(flights.run(id.clone(), move || {
            second_calls.fetch_add(1, Ordering::SeqCst);
            Some(("wrong".into(), "duplicate".into()))
        }));
        assert!(futures_util::poll!(survivor.as_mut()).is_pending());
        release.send(()).unwrap();
        assert_eq!(survivor.await, Some(("current".into(), "binary".into())));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let mut changed = id;
        changed.probe_contract_hash = "c".repeat(64);
        assert_eq!(
            flights
                .run(changed, || Some(("new".into(), "contract".into())))
                .await,
            Some(("new".into(), "contract".into()))
        );
    }

    #[tokio::test]
    async fn account_version_failure_cannot_change_a_current_technical_contract() {
        let temp = tempfile::tempdir().unwrap();
        let binary = temp.path().join("synthetic-cli");
        std::fs::write(&binary, b"fixture, never executed").unwrap();
        let mut instance = runtime_instance();
        instance.binary_path = binary.to_string_lossy().into_owned();
        instance.home_path = temp.path().to_string_lossy().into_owned();
        let codex_contract =
            codex_mcp_probe_contract_hash_for_instance(&instance, 128, 1_048_576).unwrap();
        let mut codex = local_attestation('1');
        codex.executable = attest_codex_executable_identity(
            codex_executable_cache_identity(&instance.binary_path, "", &codex_contract).unwrap(),
        )
        .unwrap();
        let codex = CodexMcpLocalAttestation::build(
            codex.executable,
            codex.adapter_contract_fingerprint,
            codex.isolation_contract_fingerprint,
            codex.schema_contract_fingerprint,
            codex.bridge_contract_fingerprint,
            codex.probes,
        )
        .unwrap();
        global_codex_mcp_readiness_cache().insert(codex);
        let ready = codex_mcp_readiness_for_instance(
            &instance,
            temp.path(),
            Some("reported-version"),
            None,
            128,
            1_048_576,
        )
        .await;
        let unavailable_account =
            codex_mcp_readiness_for_instance(&instance, temp.path(), None, None, 128, 1_048_576)
                .await;
        assert!(ready.supported && unavailable_account.supported);
        assert_eq!(
            ready.contract_fingerprint,
            unavailable_account.contract_fingerprint
        );
        assert_eq!(
            ready.local_executable_fingerprint,
            unavailable_account.local_executable_fingerprint
        );

        instance.kind = GatewayCliAgentRuntimeKindConfig::Claude;
        let claude_contract = claude_mcp_probe_contract_hash_for_instance(&instance).unwrap();
        let mut claude = claude_local_attestation('2');
        claude.executable = attest_claude_executable_identity(
            claude_executable_cache_identity(&instance.binary_path, "", &claude_contract).unwrap(),
        )
        .unwrap();
        let claude = ClaudeMcpLocalAttestation::build(
            claude.executable,
            claude.adapter_contract_fingerprint,
            claude.isolation_contract_fingerprint,
            claude.schema_contract_fingerprint,
            claude.bridge_contract_fingerprint,
            claude.continuation_contract_fingerprint,
            claude.probes,
        )
        .unwrap();
        global_claude_mcp_readiness_cache().insert(claude);
        let ready = claude_mcp_readiness_for_instance(
            &instance,
            temp.path(),
            Some("reported-version"),
            None,
            128,
            1_048_576,
        )
        .await;
        let unavailable_account =
            claude_mcp_readiness_for_instance(&instance, temp.path(), None, None, 128, 1_048_576)
                .await;
        assert!(ready.supported && unavailable_account.supported);
        assert_eq!(
            ready.contract_fingerprint,
            unavailable_account.contract_fingerprint
        );
        assert_eq!(
            ready.local_executable_fingerprint,
            unavailable_account.local_executable_fingerprint
        );
    }

    #[tokio::test]
    async fn opencode_technical_identity_requires_the_current_binary_independently_of_account() {
        let temp = tempfile::tempdir().unwrap();
        let binary = temp.path().join("synthetic-opencode");
        std::fs::write(&binary, b"fixture, never executed").unwrap();
        let mut instance = runtime_instance();
        instance.kind = GatewayCliAgentRuntimeKindConfig::OpenCode;
        instance.binary_path = binary.to_string_lossy().into_owned();
        let first =
            opencode_mcp_readiness_for_instance(&instance, Some("version"), 128, 1_048_576).await;
        let no_account = opencode_mcp_readiness_for_instance(&instance, None, 128, 1_048_576).await;
        assert_eq!(first.supported, cfg!(any(unix, windows)));
        assert_eq!(first.contract_fingerprint, no_account.contract_fingerprint);
        std::fs::write(&binary, b"different binary contents").unwrap();
        let changed = opencode_mcp_readiness_for_instance(&instance, None, 128, 1_048_576).await;
        assert_ne!(first.contract_fingerprint, changed.contract_fingerprint);
        std::fs::remove_file(&binary).unwrap();
        assert!(
            !opencode_mcp_readiness_for_instance(&instance, None, 128, 1_048_576)
                .await
                .supported
        );
        instance.enabled = false;
        assert!(
            !opencode_mcp_readiness_for_instance(&instance, None, 128, 1_048_576)
                .await
                .supported
        );
    }

    #[test]
    fn codex_mcp_readiness_without_local_attestation_fails_closed() {
        let readiness = evaluate_codex_mcp_readiness(input(None));
        assert!(!readiness.supported);
        assert!(
            readiness
                .diagnostics
                .iter()
                .any(|diagnostic| { diagnostic.code == "cli_runtime.mcp.provider_probe_failed" })
        );
    }

    #[test]
    fn codex_mcp_readiness_all_local_prerequisites_enable_automatically() {
        let local = local_attestation('1');
        let readiness = evaluate_codex_mcp_readiness(input(Some(&local)));
        assert!(readiness.supported);
        assert!(readiness.strict_isolation);
        let policy = CliRuntimeCapabilityPolicy::from_readiness(true, Some(&readiness));
        assert!(policy.supports_mcp_tools());
    }

    #[test]
    fn codex_mcp_readiness_each_failed_local_probe_has_typed_safe_diagnostic() {
        for probe in CodexMcpLocalProbeKind::ALL {
            let mut local = local_attestation('1');
            local
                .probes
                .insert(probe, CodexMcpLocalProbeResult::failed("safe failure"));
            let readiness = evaluate_codex_mcp_readiness(input(Some(&local)));
            assert!(!readiness.supported, "probe {probe:?} must fail closed");
            let (code, _) = probe.diagnostic();
            assert!(
                readiness
                    .diagnostics
                    .iter()
                    .any(|diagnostic| diagnostic.code == code)
            );
        }
    }

    #[test]
    fn codex_mcp_readiness_cache_invalidates_on_exact_identity_change() {
        let cache = CodexMcpReadinessCache::default();
        let first = local_attestation('1');
        let mut second = local_attestation('2');
        second.executable.cache_identity.realpath = PathBuf::from("/opt/codex-new");
        cache.insert(first.clone());
        assert!(cache.get(&first.executable.cache_identity).is_some());
        assert!(cache.get(&second.executable.cache_identity).is_none());
        cache.insert(second);
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn codex_mcp_readiness_contract_cache_key_changes_with_runtime_config() {
        let first = runtime_instance();
        let mut changed = first.clone();
        changed
            .app_server_args
            .push("--some-future-safe-flag".to_owned());
        assert_ne!(
            codex_mcp_probe_contract_hash_for_instance(&first, 512, 4 * 1024 * 1024)
                .expect("first contract"),
            codex_mcp_probe_contract_hash_for_instance(&changed, 512, 4 * 1024 * 1024)
                .expect("changed contract")
        );
        assert_ne!(
            codex_mcp_probe_contract_hash_for_instance(&first, 128, 4 * 1024 * 1024)
                .expect("old limit contract"),
            codex_mcp_probe_contract_hash_for_instance(&first, 512, 4 * 1024 * 1024)
                .expect("new limit contract")
        );
    }

    #[test]
    fn phase_zero_keeps_skills_and_mcp_independent() {
        let skills_only = CliRuntimeCapabilityPolicy::phase_zero(true);
        assert!(skills_only.supports_skills());
        assert!(!skills_only.supports_mcp_tools());
        assert_eq!(
            skills_only.mcp_diagnostic().0,
            "cli_runtime.mcp.readiness_unavailable"
        );

        let configured_but_unprobed = CliRuntimeCapabilityPolicy::phase_zero(false);
        assert!(!configured_but_unprobed.supports_skills());
        assert!(!configured_but_unprobed.supports_mcp_tools());
        assert_eq!(
            configured_but_unprobed.mcp_diagnostic().0,
            "cli_runtime.mcp.readiness_unavailable"
        );
    }

    #[test]
    fn codex_mcp_readiness_contract_is_locally_fingerprinted() {
        assert!(is_fingerprint(
            codex_mcp_probe_contract_hash()
                .expect("probe contract hash")
                .as_str()
        ));
    }

    fn claude_all_passed_probes() -> BTreeMap<ClaudeMcpLocalProbeKind, ClaudeMcpLocalProbeResult> {
        ClaudeMcpLocalProbeKind::ALL
            .into_iter()
            .map(|probe| (probe, ClaudeMcpLocalProbeResult::passed()))
            .collect()
    }

    fn claude_local_attestation(seed: char) -> ClaudeMcpLocalAttestation {
        let fingerprint = seed.to_string().repeat(64);
        ClaudeMcpLocalAttestation::build(
            ClaudeExecutableAttestation {
                cache_identity: ClaudeExecutableCacheIdentity {
                    realpath: PathBuf::from("/opt/claude"),
                    size: 42,
                    modified_unix_nanos: 7,
                    provider_version: "2.1.197".to_owned(),
                    probe_contract_hash: "9".repeat(64),
                },
                binary_sha256: "8".repeat(64),
                platform: "macos".to_owned(),
                architecture: "aarch64".to_owned(),
                local_executable_fingerprint: fingerprint.clone(),
            },
            fingerprint,
            "2".repeat(64),
            "3".repeat(64),
            "4".repeat(64),
            "7".repeat(64),
            claude_all_passed_probes(),
        )
        .expect("Claude local attestation")
    }

    fn claude_input(local: Option<&ClaudeMcpLocalAttestation>) -> ClaudeMcpReadinessInput<'_> {
        ClaudeMcpReadinessInput {
            runtime_enabled: true,
            platform_ipc_available: true,
            facade_supervisor_healthy: true,
            max_tools: 128,
            max_schema_bytes: 1_048_576,
            local_attestation: local,
        }
    }

    #[test]
    fn claude_mcp_readiness_without_local_attestation_fails_closed() {
        let readiness = evaluate_claude_mcp_readiness(claude_input(None));
        assert!(!readiness.supported);
        assert!(
            readiness
                .diagnostics
                .iter()
                .any(|diagnostic| { diagnostic.code == "cli_runtime.mcp.provider_probe_failed" })
        );
    }

    #[test]
    fn claude_mcp_readiness_all_local_prerequisites_enable_automatically() {
        let local = claude_local_attestation('1');
        let readiness = evaluate_claude_mcp_readiness(claude_input(Some(&local)));
        assert!(readiness.supported);
        assert!(readiness.strict_isolation);
        assert_eq!(
            readiness.injection,
            CliMcpInjectionKind::ClaudeStrictStdioMcp
        );
        assert_eq!(
            readiness.projection_update,
            CliMcpProjectionUpdateKind::ClaudeRestartProcessResumeSession
        );
    }

    #[test]
    fn claude_mcp_readiness_each_failed_probe_has_safe_adapter_diagnostic() {
        for probe in ClaudeMcpLocalProbeKind::ALL {
            let mut local = claude_local_attestation('1');
            local.probes.insert(
                probe,
                ClaudeMcpLocalProbeResult::failed("safe failure\nsecret"),
            );
            let readiness = evaluate_claude_mcp_readiness(claude_input(Some(&local)));
            assert!(!readiness.supported, "probe {probe:?} must fail closed");
            let (code, _) = probe.diagnostic();
            assert!(readiness.diagnostics.iter().any(|value| value.code == code));
            assert!(
                readiness
                    .diagnostics
                    .iter()
                    .all(|value| !value.message.contains('\n'))
            );
        }
    }

    #[test]
    fn claude_mcp_readiness_general_runtime_disable_is_fail_closed() {
        let local = claude_local_attestation('1');
        let mut input = claude_input(Some(&local));
        input.runtime_enabled = false;
        let readiness = evaluate_claude_mcp_readiness(input);
        assert!(!readiness.supported);
        assert!(
            readiness
                .diagnostics
                .iter()
                .any(|diagnostic| { diagnostic.code == "cli_runtime.mcp.runtime_not_ready" })
        );
    }

    #[test]
    fn claude_mcp_readiness_cache_invalidates_on_binary_or_contract_change() {
        let cache = ClaudeMcpReadinessCache::default();
        let first = claude_local_attestation('1');
        let mut second = claude_local_attestation('2');
        second.executable.cache_identity.probe_contract_hash = "a".repeat(64);
        cache.insert(first.clone());
        assert!(cache.get(&first.executable.cache_identity).is_some());
        assert!(cache.get(&second.executable.cache_identity).is_none());
        cache.insert(second);
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn claude_mcp_readiness_contract_is_locally_fingerprinted() {
        assert!(is_fingerprint(
            claude_mcp_probe_contract_hash()
                .expect("Claude probe contract")
                .as_str()
        ));
    }
}
