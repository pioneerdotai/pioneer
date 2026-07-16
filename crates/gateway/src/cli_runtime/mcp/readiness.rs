use pioneer_cli_agent_runtime::claude::{
    CLAUDE_MCP_SCHEMA_TRANSFORM_CONTRACT_VERSION, ClaudeExecutableAttestation,
    ClaudeExecutableCacheIdentity, attest_claude_executable_identity,
    claude_continuation_contract_fingerprint, claude_executable_cache_identity,
    probe_claude_help_contract, validate_recorded_claude_mcp_decoder_fixtures,
};
use pioneer_cli_agent_runtime::codex::CODEX_MCP_SCHEMA_TRANSFORM_CONTRACT_VERSION;
use pioneer_cli_agent_runtime::codex_attestation::{
    CodexExecutableAttestation, CodexExecutableCacheIdentity, attest_codex_executable_identity,
    codex_executable_cache_identity, sha256_json, validate_recorded_codex_mcp_decoder_fixtures,
    validate_unknown_codex_request_response,
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
use std::sync::{Mutex, OnceLock};

pub(crate) const CODEX_MCP_LOCAL_PROBE_CONTRACT_VERSION: u32 = 1;

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
        normal_runtime_ready: bool,
        readiness: Option<&CliMcpAdapterReadiness>,
    ) -> Self {
        if !normal_runtime_ready {
            return Self::unsupported(
                supports_skills,
                "cli_runtime.mcp.runtime_not_ready",
                "The CLI runtime is not ready for MCP tools",
            );
        }
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
    pub(crate) normal_runtime_ready: bool,
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
    if !input.normal_runtime_ready {
        diagnostics.push(diagnostic(
            RuntimeDiagnosticLevel::Warning,
            "cli_runtime.mcp.runtime_not_ready",
            "The Codex runtime is not ready for MCP tools",
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
        && input.normal_runtime_ready
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

#[derive(Default)]
pub(crate) struct CodexMcpReadinessCache {
    entries: Mutex<HashMap<CodexExecutableCacheIdentity, CodexMcpLocalAttestation>>,
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
    }))
}

fn global_codex_mcp_readiness_cache() -> &'static CodexMcpReadinessCache {
    static CACHE: OnceLock<CodexMcpReadinessCache> = OnceLock::new();
    CACHE.get_or_init(CodexMcpReadinessCache::default)
}

/// One production readiness path shared by catalog refresh and turn preflight.
/// It performs no model request and enables MCP automatically after exact local checks.
pub(crate) async fn codex_mcp_readiness_for_instance(
    instance: &EffectiveGatewayCliAgentRuntimeInstanceConfig,
    runtime_home: &Path,
    normal_runtime_ready: bool,
    provider_version: Option<&str>,
    proxy_url: Option<&str>,
    max_tools: usize,
    max_schema_bytes: usize,
) -> CliMcpAdapterReadiness {
    if !instance.enabled || !normal_runtime_ready {
        return evaluate_codex_mcp_readiness(CodexMcpReadinessInput {
            runtime_enabled: instance.enabled,
            normal_runtime_ready,
            platform_ipc_available: cfg!(any(unix, windows)),
            facade_supervisor_healthy: true,
            max_tools,
            max_schema_bytes,
            local_attestation: None,
        });
    }

    let local = match provider_version {
        Some(provider_version) if !provider_version.trim().is_empty() => {
            build_or_load_codex_local_attestation(
                instance,
                runtime_home,
                provider_version,
                proxy_url,
            )
            .await
        }
        _ => None,
    };
    evaluate_codex_mcp_readiness(CodexMcpReadinessInput {
        runtime_enabled: instance.enabled,
        normal_runtime_ready,
        platform_ipc_available: cfg!(any(unix, windows)),
        facade_supervisor_healthy: true,
        max_tools,
        max_schema_bytes,
        local_attestation: local.as_ref(),
    })
}

async fn build_or_load_codex_local_attestation(
    instance: &EffectiveGatewayCliAgentRuntimeInstanceConfig,
    runtime_home: &Path,
    provider_version: &str,
    proxy_url: Option<&str>,
) -> Option<CodexMcpLocalAttestation> {
    let contract_hash = codex_mcp_probe_contract_hash_for_instance(instance).ok()?;
    let cache_identity = codex_executable_cache_identity(
        instance.binary_path.as_str(),
        provider_version,
        contract_hash.as_str(),
    )
    .ok()?;
    if let Some(cached) = global_codex_mcp_readiness_cache().get(&cache_identity) {
        return Some(cached);
    }
    let executable = attest_codex_executable_identity(cache_identity).ok()?;
    let provider_probe = crate::cli_runtime::codex_session::run_codex_mcp_local_provider_probe(
        instance,
        runtime_home,
        proxy_url,
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
    if attestation.all_probes_passed() {
        global_codex_mcp_readiness_cache().insert(attestation.clone());
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
    pub(crate) normal_runtime_ready: bool,
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
    if !input.normal_runtime_ready {
        diagnostics.push(diagnostic(
            RuntimeDiagnosticLevel::Warning,
            "cli_runtime.mcp.runtime_not_ready",
            "The Claude runtime is not ready for MCP tools",
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
        && input.normal_runtime_ready
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
    normal_runtime_ready: bool,
    provider_version: Option<&str>,
    proxy_url: Option<&str>,
    max_tools: usize,
    max_schema_bytes: usize,
) -> CliMcpAdapterReadiness {
    if !instance.enabled || !normal_runtime_ready {
        return evaluate_claude_mcp_readiness(ClaudeMcpReadinessInput {
            runtime_enabled: instance.enabled,
            normal_runtime_ready,
            platform_ipc_available: cfg!(any(unix, windows)),
            facade_supervisor_healthy: true,
            max_tools,
            max_schema_bytes,
            local_attestation: None,
        });
    }
    let local = match provider_version {
        Some(version) if !version.trim().is_empty() => {
            build_or_load_claude_local_attestation(instance, runtime_home, version, proxy_url).await
        }
        _ => None,
    };
    evaluate_claude_mcp_readiness(ClaudeMcpReadinessInput {
        runtime_enabled: instance.enabled,
        normal_runtime_ready,
        platform_ipc_available: cfg!(any(unix, windows)),
        facade_supervisor_healthy: true,
        max_tools,
        max_schema_bytes,
        local_attestation: local.as_ref(),
    })
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
    let executable = attest_claude_executable_identity(cache_identity).ok()?;
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
    if attestation.all_probes_passed() {
        global_claude_mcp_readiness_cache().insert(attestation.clone());
    }
    Some(attestation)
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
            normal_runtime_ready: true,
            platform_ipc_available: true,
            facade_supervisor_healthy: true,
            max_tools: 128,
            max_schema_bytes: 1_048_576,
            local_attestation: local,
        }
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
        let policy = CliRuntimeCapabilityPolicy::from_readiness(true, true, Some(&readiness));
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
            codex_mcp_probe_contract_hash_for_instance(&first).expect("first contract"),
            codex_mcp_probe_contract_hash_for_instance(&changed).expect("changed contract")
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
            normal_runtime_ready: true,
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
