//! Claude CLI streaming runtime primitives.

use crate::codex::CodexMcpSchemaTransformer;
use crate::instructions::CLIRuntimeElevatedInstructions;
use crate::mcp::{
    CanonicalMcpToolSchema, McpSchemaIncompatibility, McpSchemaTransformContract,
    McpSchemaTransformer,
};
use crate::process::{SensitiveEnvironment, expand_home_path, scrub_inherited_cli_environment};
use pioneer_protocol::normalize_metadata_reasoning_effort;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::env;
use std::error::Error;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tokio::time::timeout;

const MINIMUM_CLAUDE_CODE_VERSION: &str = "2.0.0";
const CLAUDE_EMPTY_MCP_CONFIG_FILE_NAME: &str = "mcp-config.json";
const CLAUDE_EMPTY_MCP_CONFIG_MARKER_FILE_NAME: &str = ".pioneer-claude-mcp-config.json";
const CLAUDE_SYSTEM_PROMPT_EXTENSION_FILE_NAME: &str = "pioneer-system-prompt.md";
const CLAUDE_PIONEER_MCP_SERVER_NAME: &str = "pioneer";
const CLAUDE_PIONEER_MCP_SERVER_TYPE: &str = "stdio";
const CLAUDE_PIONEER_HELPER_SUBCOMMAND: &str = "__cli-mcp-stdio";
const CLAUDE_PIONEER_BOOTSTRAP_OPTION: &str = "--bootstrap-file";
const CLAUDE_MANAGED_PATH_MAX_BYTES: usize = 4_096;
const CLAUDE_SCHEMA_TRANSFORMER_ID: &str = "claude-code.mcp-schema";
pub const CLAUDE_MCP_SCHEMA_TRANSFORM_CONTRACT_VERSION: u32 = 2;
pub const CLAUDE_MCP_LOCAL_ATTESTATION_CONTRACT_VERSION: u32 = 1;
static CLAUDE_MCP_CONFIG_NONCE_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, PartialEq, Eq)]
pub enum ClaudeProviderSessionLaunch {
    New(uuid::Uuid),
    Resume(uuid::Uuid),
}

impl fmt::Debug for ClaudeProviderSessionLaunch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::New(_) => formatter.write_str("ClaudeProviderSessionLaunch::New(<redacted>)"),
            Self::Resume(_) => {
                formatter.write_str("ClaudeProviderSessionLaunch::Resume(<redacted>)")
            }
        }
    }
}

impl ClaudeProviderSessionLaunch {
    pub fn new(provider_session_id: uuid::Uuid) -> Result<Self, ClaudeProviderSessionLaunchError> {
        validate_claude_provider_session_id(provider_session_id)?;
        Ok(Self::New(provider_session_id))
    }

    pub fn resume(
        provider_session_id: uuid::Uuid,
    ) -> Result<Self, ClaudeProviderSessionLaunchError> {
        validate_claude_provider_session_id(provider_session_id)?;
        Ok(Self::Resume(provider_session_id))
    }

    pub fn append_process_args(&self, args: &mut Vec<String>) {
        match self {
            Self::New(provider_session_id) => {
                args.push("--session-id".to_owned());
                args.push(provider_session_id.to_string());
            }
            Self::Resume(provider_session_id) => {
                args.push("--resume".to_owned());
                args.push(provider_session_id.to_string());
            }
        }
    }

    pub fn provider_session_id(&self) -> uuid::Uuid {
        match self {
            Self::New(provider_session_id) | Self::Resume(provider_session_id) => {
                *provider_session_id
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeProviderSessionLaunchError;

impl fmt::Display for ClaudeProviderSessionLaunchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Claude provider session UUID must be non-nil")
    }
}

impl Error for ClaudeProviderSessionLaunchError {}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClaudeExecutableCacheIdentity {
    pub realpath: PathBuf,
    pub size: u64,
    pub modified_unix_nanos: u64,
    pub provider_version: String,
    pub probe_contract_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClaudeExecutableAttestation {
    pub cache_identity: ClaudeExecutableCacheIdentity,
    pub binary_sha256: String,
    pub platform: String,
    pub architecture: String,
    pub local_executable_fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeHelpContractEvidence {
    pub help_sha256: String,
    pub required_flags: Vec<String>,
    pub hidden_managed_flags: Vec<String>,
}

pub fn claude_executable_cache_identity(
    configured_executable: &str,
    provider_version: &str,
    probe_contract_hash: &str,
) -> anyhow::Result<ClaudeExecutableCacheIdentity> {
    claude_validate_sha256("probe contract hash", probe_contract_hash)?;
    let provider_version = provider_version.trim();
    if provider_version.is_empty()
        || provider_version.len() > 256
        || provider_version.contains('\0')
    {
        anyhow::bail!("Claude provider version is unavailable or invalid");
    }
    let realpath = resolve_claude_executable(configured_executable)?;
    let metadata = fs::metadata(realpath.as_path()).map_err(|error| {
        anyhow::anyhow!(
            "failed to inspect Claude executable `{}`: {error}",
            realpath.display()
        )
    })?;
    if !metadata.is_file() {
        anyhow::bail!("configured Claude executable is not a regular file");
    }
    let modified = metadata
        .modified()
        .map_err(|error| anyhow::anyhow!("failed to read Claude executable mtime: {error}"))?
        .duration_since(UNIX_EPOCH)
        .map_err(|_| anyhow::anyhow!("Claude executable modification time predates Unix epoch"))?;
    Ok(ClaudeExecutableCacheIdentity {
        realpath,
        size: metadata.len(),
        modified_unix_nanos: modified.as_nanos().min(u64::MAX as u128) as u64,
        provider_version: provider_version.to_owned(),
        probe_contract_hash: probe_contract_hash.to_owned(),
    })
}

pub fn attest_claude_executable_identity(
    cache_identity: ClaudeExecutableCacheIdentity,
) -> anyhow::Result<ClaudeExecutableAttestation> {
    let metadata = fs::metadata(cache_identity.realpath.as_path()).map_err(|error| {
        anyhow::anyhow!(
            "failed to re-inspect Claude executable `{}`: {error}",
            cache_identity.realpath.display()
        )
    })?;
    let modified = metadata
        .modified()
        .map_err(|error| anyhow::anyhow!("failed to re-read Claude executable mtime: {error}"))?
        .duration_since(UNIX_EPOCH)
        .map_err(|_| anyhow::anyhow!("Claude executable modification time predates Unix epoch"))?;
    let modified_unix_nanos = modified.as_nanos().min(u64::MAX as u128) as u64;
    if !metadata.is_file()
        || metadata.len() != cache_identity.size
        || modified_unix_nanos != cache_identity.modified_unix_nanos
    {
        anyhow::bail!("Claude executable changed while it was being attested");
    }
    let binary_sha256 =
        crate::codex_attestation::sha256_file_contents(cache_identity.realpath.as_path())?;
    let platform = env::consts::OS.to_owned();
    let architecture = env::consts::ARCH.to_owned();
    let local_executable_fingerprint = crate::codex_attestation::sha256_json(&serde_json::json!({
        "contractVersion": CLAUDE_MCP_LOCAL_ATTESTATION_CONTRACT_VERSION,
        "binarySha256": binary_sha256,
        "binarySize": cache_identity.size,
        "providerVersion": cache_identity.provider_version,
        "probeContractHash": cache_identity.probe_contract_hash,
        "platform": platform,
        "architecture": architecture,
    }))?;
    Ok(ClaudeExecutableAttestation {
        cache_identity,
        binary_sha256,
        platform,
        architecture,
        local_executable_fingerprint,
    })
}

pub async fn probe_claude_help_contract(
    executable: &Path,
    wait: Duration,
) -> anyhow::Result<ClaudeHelpContractEvidence> {
    if wait.is_zero() {
        anyhow::bail!("Claude help probe timeout must be non-zero");
    }
    let mut command = Command::new(executable);
    scrub_inherited_cli_environment(&mut command);
    command
        .arg("--help")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let output = timeout(wait, command.output())
        .await
        .map_err(|_| anyhow::anyhow!("Claude help probe timed out"))?
        .map_err(|error| anyhow::anyhow!("failed to run Claude help probe: {error}"))?;
    if !output.status.success() {
        anyhow::bail!("Claude help probe exited unsuccessfully");
    }
    let help = String::from_utf8(output.stdout)
        .map_err(|_| anyhow::anyhow!("Claude help output is not UTF-8"))?;
    let required_flags = [
        "--mcp-config",
        "--strict-mcp-config",
        "--allowedTools",
        "--setting-sources",
        "--safe-mode",
        "--session-id",
        "--resume",
        "--no-session-persistence",
        "--input-format",
        "--output-format",
    ];
    let missing = required_flags
        .iter()
        .filter(|flag| !help.contains(**flag))
        .copied()
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        anyhow::bail!(
            "Claude help is missing required managed flags: {}",
            missing.join(", ")
        );
    }
    Ok(ClaudeHelpContractEvidence {
        help_sha256: crate::codex_attestation::sha256_json(&serde_json::json!({
            "help": help,
        }))?,
        required_flags: required_flags.into_iter().map(str::to_owned).collect(),
        // Claude accepts this SDK control-channel flag but intentionally omits
        // it from public help. The real strict-launch probe below exercises it;
        // recording it here keeps that hidden contract in the attestation.
        hidden_managed_flags: vec!["--permission-prompt-tool".to_owned()],
    })
}

pub fn validate_recorded_claude_mcp_decoder_fixtures() -> anyhow::Result<()> {
    let fixture: JsonValue =
        serde_json::from_str(include_str!("../tests/fixtures/claude_mcp/lifecycle.json"))?;
    if fixture["providerVersion"]
        .as_str()
        .unwrap_or_default()
        .trim()
        .is_empty()
    {
        anyhow::bail!("recorded Claude MCP fixture has no provider identity");
    }
    let messages = fixture["messages"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("recorded Claude MCP fixture has no messages"))?;
    let decoded = messages
        .iter()
        .flat_map(decode_claude_mcp_content_blocks)
        .collect::<Vec<_>>();
    if decoded.len() != 6 || decoded.first() != decoded.get(4) || decoded.get(2) != decoded.get(5) {
        anyhow::bail!("recorded Claude MCP fixture replay/parallel contract changed");
    }
    if !matches!(
        decoded.get(3),
        Some(ClaudeMcpContentBlock::ToolResult { is_error: true, .. })
    ) {
        anyhow::bail!("recorded Claude MCP error result contract changed");
    }
    Ok(())
}

pub fn claude_continuation_contract_fingerprint() -> anyhow::Result<String> {
    crate::codex_attestation::sha256_json(&serde_json::json!({
        "contractVersion": 1,
        "new": ["--session-id", "<real-provider-uuid>"],
        "resume": ["--resume", "<same-real-provider-uuid>"],
        "fallbackToNewConversation": false,
        "checkpointBeforeReplacement": true,
    }))
}

fn resolve_claude_executable(configured_executable: &str) -> anyhow::Result<PathBuf> {
    let configured_executable = configured_executable.trim();
    if configured_executable.is_empty() || configured_executable.contains('\0') {
        anyhow::bail!("configured Claude executable is empty or invalid");
    }
    let configured = Path::new(configured_executable);
    if configured.is_absolute() || configured.components().count() > 1 {
        return fs::canonicalize(configured).map_err(|error| {
            anyhow::anyhow!(
                "failed to resolve configured Claude executable `{configured_executable}`: {error}"
            )
        });
    }
    let path = env::var_os("PATH").ok_or_else(|| anyhow::anyhow!("PATH is unavailable"))?;
    for directory in env::split_paths(&path) {
        let candidate = directory.join(configured);
        if candidate.is_file() {
            return fs::canonicalize(candidate)
                .map_err(|error| anyhow::anyhow!("failed to resolve Claude executable: {error}"));
        }
        #[cfg(windows)]
        for extension in ["exe", "cmd", "bat"] {
            let candidate = directory.join(format!("{configured_executable}.{extension}"));
            if candidate.is_file() {
                return fs::canonicalize(candidate).map_err(|error| {
                    anyhow::anyhow!("failed to resolve Claude executable: {error}")
                });
            }
        }
    }
    anyhow::bail!("configured Claude executable was not found")
}

fn claude_validate_sha256(label: &str, value: &str) -> anyhow::Result<()> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        anyhow::bail!("{label} must be a SHA-256 fingerprint");
    }
    Ok(())
}

fn validate_claude_provider_session_id(
    provider_session_id: uuid::Uuid,
) -> Result<(), ClaudeProviderSessionLaunchError> {
    if provider_session_id.is_nil() {
        return Err(ClaudeProviderSessionLaunchError);
    }
    Ok(())
}

/// Opaque MCP schema transport for Claude. Pioneer enforces the same wire and
/// resource limits as the Codex bridge, but does not interpret schema dialects
/// or keywords. Claude remains the authority on schemas it can consume.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeMcpSchemaTransformer {
    provider_contract_fingerprint: String,
    opaque_transport: CodexMcpSchemaTransformer,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaudeMcpSchemaTransformerError {
    InvalidProviderContractFingerprint,
}

impl fmt::Display for ClaudeMcpSchemaTransformerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("invalid Claude executable/contract fingerprint")
    }
}

impl Error for ClaudeMcpSchemaTransformerError {}

impl ClaudeMcpSchemaTransformer {
    pub fn new(
        provider_contract_fingerprint: impl Into<String>,
    ) -> Result<Self, ClaudeMcpSchemaTransformerError> {
        let provider_contract_fingerprint = provider_contract_fingerprint.into();
        let opaque_transport =
            CodexMcpSchemaTransformer::new(provider_contract_fingerprint.clone())
                .map_err(|_| ClaudeMcpSchemaTransformerError::InvalidProviderContractFingerprint)?;
        Ok(Self {
            provider_contract_fingerprint,
            opaque_transport,
        })
    }

    pub fn provider_contract_fingerprint(&self) -> &str {
        self.provider_contract_fingerprint.as_str()
    }
}

impl McpSchemaTransformer for ClaudeMcpSchemaTransformer {
    fn contract(&self) -> McpSchemaTransformContract {
        McpSchemaTransformContract {
            transformer_id: CLAUDE_SCHEMA_TRANSFORMER_ID.to_owned(),
            contract_version: CLAUDE_MCP_SCHEMA_TRANSFORM_CONTRACT_VERSION,
            provider_contract_fingerprint: self.provider_contract_fingerprint.clone(),
        }
    }

    fn transform(
        &self,
        canonical: &CanonicalMcpToolSchema,
    ) -> Result<JsonValue, McpSchemaIncompatibility> {
        self.opaque_transport
            .transform(canonical)
            .map_err(|incompatibility| {
                McpSchemaIncompatibility::new(
                    incompatibility
                        .code
                        .replacen("codex.schema.", "claude.schema.", 1),
                    incompatibility.message.replace("Codex", "Claude"),
                )
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClaudeManagedMcpConfigIdentity {
    pub workspace_id: String,
    pub runtime_id: String,
    pub logical_thread_id: String,
    pub gateway_boot_id: String,
    pub process_generation: u64,
}

impl ClaudeManagedMcpConfigIdentity {
    pub fn new(
        workspace_id: impl Into<String>,
        runtime_id: impl Into<String>,
        logical_thread_id: impl Into<String>,
        gateway_boot_id: impl Into<String>,
        process_generation: u64,
    ) -> Result<Self, ClaudeManagedMcpConfigError> {
        let identity = Self {
            workspace_id: workspace_id.into(),
            runtime_id: runtime_id.into(),
            logical_thread_id: logical_thread_id.into(),
            gateway_boot_id: gateway_boot_id.into(),
            process_generation,
        };
        for (field, value) in [
            ("workspace_id", identity.workspace_id.as_str()),
            ("runtime_id", identity.runtime_id.as_str()),
            ("logical_thread_id", identity.logical_thread_id.as_str()),
            ("gateway_boot_id", identity.gateway_boot_id.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(ClaudeManagedMcpConfigError::new(format!(
                    "Claude managed MCP config {field} must not be empty"
                )));
            }
        }
        if process_generation == 0 {
            return Err(ClaudeManagedMcpConfigError::new(
                "Claude managed MCP config process_generation must be greater than zero",
            ));
        }
        Ok(identity)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeManagedMcpConfigDescriptor {
    pub identity: ClaudeManagedMcpConfigIdentity,
    pub managed_root_path: PathBuf,
    pub session_root_path: PathBuf,
    pub config_path: PathBuf,
    pub artifact_digest: String,
    pub has_pioneer_server: bool,
    materialization_nonce: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ClaudeManagedMcpConfigInput {
    pub helper_path: Option<PathBuf>,
    pub bootstrap_path: Option<PathBuf>,
}

impl ClaudeManagedMcpConfigInput {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn pioneer(helper_path: PathBuf, bootstrap_path: PathBuf) -> Self {
        Self {
            helper_path: Some(helper_path),
            bootstrap_path: Some(bootstrap_path),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeManagedMcpConfigArtifact {
    config_json: String,
    artifact_digest: String,
    has_pioneer_server: bool,
}

impl ClaudeManagedMcpConfigArtifact {
    pub fn config_json(&self) -> &str {
        self.config_json.as_str()
    }

    pub fn artifact_digest(&self) -> &str {
        self.artifact_digest.as_str()
    }

    pub fn has_pioneer_server(&self) -> bool {
        self.has_pioneer_server
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeManagedMcpConfigError {
    detail: String,
}

impl ClaudeManagedMcpConfigError {
    fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }

    fn with_context(action: &str, path: &Path, error: impl fmt::Display) -> Self {
        Self::new(format!("{action} `{}` failed: {error}", path.display()))
    }
}

impl fmt::Display for ClaudeManagedMcpConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.detail.as_str())
    }
}

impl Error for ClaudeManagedMcpConfigError {}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ClaudeManagedMcpConfigDocument {
    mcp_servers: BTreeMap<String, ClaudeManagedMcpServer>,
}

#[derive(Debug, Serialize)]
struct ClaudeManagedMcpServer {
    #[serde(rename = "type")]
    transport: &'static str,
    command: String,
    args: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ClaudeManagedMcpConfigMarker {
    identity: ClaudeManagedMcpConfigIdentity,
    materialization_nonce: String,
    artifact_digest: String,
    has_pioneer_server: bool,
}

pub fn serialize_claude_managed_mcp_config(
    input: ClaudeManagedMcpConfigInput,
) -> Result<ClaudeManagedMcpConfigArtifact, ClaudeManagedMcpConfigError> {
    let mut mcp_servers = BTreeMap::new();
    let has_pioneer_server = match (input.helper_path, input.bootstrap_path) {
        (None, None) => false,
        (Some(helper_path), Some(bootstrap_path)) => {
            let helper = claude_validate_managed_helper_path(helper_path.as_path())?;
            let bootstrap = claude_validate_managed_bootstrap_path(bootstrap_path.as_path())?;
            mcp_servers.insert(
                CLAUDE_PIONEER_MCP_SERVER_NAME.to_owned(),
                ClaudeManagedMcpServer {
                    transport: CLAUDE_PIONEER_MCP_SERVER_TYPE,
                    command: helper,
                    args: vec![
                        CLAUDE_PIONEER_HELPER_SUBCOMMAND.to_owned(),
                        CLAUDE_PIONEER_BOOTSTRAP_OPTION.to_owned(),
                        bootstrap,
                    ],
                },
            );
            true
        }
        _ => {
            return Err(ClaudeManagedMcpConfigError::new(
                "Claude Pioneer MCP config requires both helper and bootstrap paths",
            ));
        }
    };
    let document = ClaudeManagedMcpConfigDocument { mcp_servers };
    let mut config_json = serde_json::to_string(&document).map_err(|error| {
        ClaudeManagedMcpConfigError::new(format!(
            "serialize managed Claude MCP config failed: {error}"
        ))
    })?;
    config_json.push('\n');
    let artifact_digest = claude_sha256_hex(config_json.as_bytes());
    Ok(ClaudeManagedMcpConfigArtifact {
        config_json,
        artifact_digest,
        has_pioneer_server,
    })
}

pub fn materialize_claude_managed_mcp_config(
    managed_root_path: &Path,
    identity: ClaudeManagedMcpConfigIdentity,
    artifact: &ClaudeManagedMcpConfigArtifact,
) -> Result<ClaudeManagedMcpConfigDescriptor, ClaudeManagedMcpConfigError> {
    if claude_sha256_hex(artifact.config_json.as_bytes()) != artifact.artifact_digest {
        return Err(ClaudeManagedMcpConfigError::new(
            "Claude managed MCP config artifact digest does not match its contents",
        ));
    }

    let managed_root_path = claude_normalize_absolute_path(managed_root_path)?;
    claude_ensure_directory_no_follow(managed_root_path.as_path(), true)?;
    let session_root_path = managed_root_path
        .join(claude_mcp_identity_component(
            "workspace",
            identity.workspace_id.as_str(),
        ))
        .join(claude_mcp_identity_component(
            "runtime",
            identity.runtime_id.as_str(),
        ))
        .join(claude_mcp_identity_component(
            "thread",
            identity.logical_thread_id.as_str(),
        ))
        .join(claude_mcp_identity_component(
            "boot",
            identity.gateway_boot_id.as_str(),
        ))
        .join(format!("generation-{:020}", identity.process_generation));
    let session_existed = match fs::symlink_metadata(session_root_path.as_path()) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            return Err(ClaudeManagedMcpConfigError::new(format!(
                "Claude managed MCP session root `{}` is not a real directory",
                session_root_path.display()
            )));
        }
        Ok(_) => true,
        Err(error) if error.kind() == ErrorKind::NotFound => false,
        Err(error) => {
            return Err(ClaudeManagedMcpConfigError::with_context(
                "inspect Claude managed MCP session root",
                session_root_path.as_path(),
                error,
            ));
        }
    };
    claude_ensure_directory_no_follow(session_root_path.as_path(), true)?;

    let marker_path = session_root_path.join(CLAUDE_EMPTY_MCP_CONFIG_MARKER_FILE_NAME);
    let marker = if session_existed {
        let marker = claude_read_mcp_config_marker(marker_path.as_path()).map_err(|error| {
            ClaudeManagedMcpConfigError::new(format!(
                "stale or unmanaged Claude MCP config root `{}`: {error}",
                session_root_path.display()
            ))
        })?;
        if marker.identity != identity {
            return Err(ClaudeManagedMcpConfigError::new(
                "stale Claude managed MCP config identity",
            ));
        }
        marker
    } else {
        let marker = ClaudeManagedMcpConfigMarker {
            identity: identity.clone(),
            materialization_nonce: claude_mcp_config_nonce(&identity),
            artifact_digest: artifact.artifact_digest.clone(),
            has_pioneer_server: artifact.has_pioneer_server,
        };
        let bytes = serde_json::to_vec(&marker).map_err(|error| {
            ClaudeManagedMcpConfigError::new(format!(
                "serialize Claude managed MCP config marker failed: {error}"
            ))
        })?;
        claude_write_new_owner_only_file(marker_path.as_path(), bytes.as_slice())?;
        marker
    };
    if marker.artifact_digest != artifact.artifact_digest
        || marker.has_pioneer_server != artifact.has_pioneer_server
    {
        return Err(ClaudeManagedMcpConfigError::new(
            "Claude managed MCP generation already contains a different projection",
        ));
    }
    let config_path = session_root_path.join(CLAUDE_EMPTY_MCP_CONFIG_FILE_NAME);
    claude_ensure_exact_owner_only_file(config_path.as_path(), artifact.config_json.as_bytes())?;

    Ok(ClaudeManagedMcpConfigDescriptor {
        identity,
        managed_root_path,
        session_root_path,
        config_path,
        artifact_digest: artifact.artifact_digest.clone(),
        has_pioneer_server: artifact.has_pioneer_server,
        materialization_nonce: marker.materialization_nonce,
    })
}

pub fn materialize_claude_empty_mcp_config(
    managed_root_path: &Path,
    identity: ClaudeManagedMcpConfigIdentity,
) -> Result<ClaudeManagedMcpConfigDescriptor, ClaudeManagedMcpConfigError> {
    let artifact = serialize_claude_managed_mcp_config(ClaudeManagedMcpConfigInput::empty())?;
    materialize_claude_managed_mcp_config(managed_root_path, identity, &artifact)
}

pub fn materialize_claude_system_prompt_extension(
    descriptor: &ClaudeManagedMcpConfigDescriptor,
    instructions: &CLIRuntimeElevatedInstructions,
) -> Result<PathBuf, ClaudeManagedMcpConfigError> {
    let managed_root = claude_normalize_absolute_path(descriptor.managed_root_path.as_path())?;
    let session_root = claude_normalize_absolute_path(descriptor.session_root_path.as_path())?;
    if session_root == managed_root || !session_root.starts_with(managed_root.as_path()) {
        return Err(ClaudeManagedMcpConfigError::new(
            "Claude system prompt extension path escapes the managed root",
        ));
    }
    claude_validate_directory_chain_no_follow(session_root.as_path())?;
    let marker = claude_read_mcp_config_marker(
        session_root
            .join(CLAUDE_EMPTY_MCP_CONFIG_MARKER_FILE_NAME)
            .as_path(),
    )?;
    if marker.identity != descriptor.identity
        || marker.materialization_nonce != descriptor.materialization_nonce
        || marker.artifact_digest != descriptor.artifact_digest
        || marker.has_pioneer_server != descriptor.has_pioneer_server
    {
        return Err(ClaudeManagedMcpConfigError::new(
            "Claude system prompt extension refused a replacement managed root",
        ));
    }
    let prompt_path = session_root.join(CLAUDE_SYSTEM_PROMPT_EXTENSION_FILE_NAME);
    claude_ensure_exact_owner_only_file(prompt_path.as_path(), instructions.text().as_bytes())?;
    Ok(prompt_path)
}

pub fn cleanup_claude_managed_mcp_config(
    descriptor: &ClaudeManagedMcpConfigDescriptor,
) -> Result<(), ClaudeManagedMcpConfigError> {
    let managed_root = claude_normalize_absolute_path(descriptor.managed_root_path.as_path())?;
    let session_root = claude_normalize_absolute_path(descriptor.session_root_path.as_path())?;
    if session_root == managed_root || !session_root.starts_with(managed_root.as_path()) {
        return Err(ClaudeManagedMcpConfigError::new(
            "Claude managed MCP cleanup path escapes the managed root",
        ));
    }
    claude_validate_directory_chain_no_follow(managed_root.as_path())?;
    claude_validate_directory_chain_no_follow(session_root.parent().ok_or_else(|| {
        ClaudeManagedMcpConfigError::new("Claude managed MCP session root has no parent")
    })?)?;
    match fs::symlink_metadata(session_root.as_path()) {
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(ClaudeManagedMcpConfigError::with_context(
                "inspect Claude managed MCP session root for cleanup",
                session_root.as_path(),
                error,
            ));
        }
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            return Err(ClaudeManagedMcpConfigError::new(
                "Claude managed MCP cleanup target is not a real directory",
            ));
        }
        Ok(_) => {}
    }
    let marker = claude_read_mcp_config_marker(
        session_root
            .join(CLAUDE_EMPTY_MCP_CONFIG_MARKER_FILE_NAME)
            .as_path(),
    )?;
    if marker.identity != descriptor.identity
        || marker.materialization_nonce != descriptor.materialization_nonce
        || marker.artifact_digest != descriptor.artifact_digest
        || marker.has_pioneer_server != descriptor.has_pioneer_server
    {
        return Err(ClaudeManagedMcpConfigError::new(
            "Claude managed MCP cleanup refused a replacement path",
        ));
    }
    fs::remove_dir_all(session_root.as_path()).map_err(|error| {
        ClaudeManagedMcpConfigError::with_context(
            "remove Claude managed MCP session root",
            session_root.as_path(),
            error,
        )
    })
}

fn claude_validate_managed_helper_path(path: &Path) -> Result<String, ClaudeManagedMcpConfigError> {
    let normalized = claude_validate_managed_file_path(path, "helper")?;
    let metadata = fs::symlink_metadata(normalized.as_path()).map_err(|error| {
        ClaudeManagedMcpConfigError::with_context(
            "inspect Claude Pioneer helper",
            normalized.as_path(),
            error,
        )
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(ClaudeManagedMcpConfigError::new(
            "Claude Pioneer helper must be a real regular file",
        ));
    }
    Ok(normalized.to_string_lossy().into_owned())
}

fn claude_validate_managed_bootstrap_path(
    path: &Path,
) -> Result<String, ClaudeManagedMcpConfigError> {
    let normalized = claude_validate_managed_file_path(path, "bootstrap")?;
    let metadata = fs::symlink_metadata(normalized.as_path()).map_err(|error| {
        ClaudeManagedMcpConfigError::with_context(
            "inspect Claude Pioneer bootstrap",
            normalized.as_path(),
            error,
        )
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(ClaudeManagedMcpConfigError::new(
            "Claude Pioneer bootstrap must be a real regular file",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(ClaudeManagedMcpConfigError::new(
                "Claude Pioneer bootstrap must be owner-only",
            ));
        }
    }
    Ok(normalized.to_string_lossy().into_owned())
}

fn claude_validate_managed_file_path(
    path: &Path,
    label: &str,
) -> Result<PathBuf, ClaudeManagedMcpConfigError> {
    if !path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(ClaudeManagedMcpConfigError::new(format!(
            "Claude Pioneer {label} path must be absolute and traversal-free"
        )));
    }
    let encoded = path.to_str().filter(|value| {
        !value.is_empty()
            && value.len() <= CLAUDE_MANAGED_PATH_MAX_BYTES
            && !value.chars().any(char::is_control)
    });
    if encoded.is_none() {
        return Err(ClaudeManagedMcpConfigError::new(format!(
            "Claude Pioneer {label} path is not safely representable"
        )));
    }
    let normalized = claude_normalize_absolute_path(path)?;
    let parent = normalized.parent().ok_or_else(|| {
        ClaudeManagedMcpConfigError::new(format!(
            "Claude Pioneer {label} path has no parent directory"
        ))
    })?;
    claude_validate_directory_chain_no_follow(parent)?;
    Ok(normalized)
}

fn claude_sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

fn claude_normalize_absolute_path(path: &Path) -> Result<PathBuf, ClaudeManagedMcpConfigError> {
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|error| {
                ClaudeManagedMcpConfigError::new(format!(
                    "resolve current directory for Claude MCP config failed: {error}"
                ))
            })?
            .join(path)
    };
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::Normal(part) => normalized.push(part),
            Component::ParentDir => {
                return Err(ClaudeManagedMcpConfigError::new(
                    "Claude managed MCP path contains parent traversal",
                ));
            }
        }
    }
    Ok(normalized)
}

fn claude_ensure_directory_no_follow(
    path: &Path,
    owner_only: bool,
) -> Result<(), ClaudeManagedMcpConfigError> {
    let path = claude_normalize_absolute_path(path)?;
    let mut current = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => current.push(prefix.as_os_str()),
            Component::RootDir => {
                current.push(component.as_os_str());
                continue;
            }
            Component::CurDir => continue,
            Component::ParentDir => unreachable!("path was normalized"),
            Component::Normal(part) => current.push(part),
        }
        loop {
            match fs::symlink_metadata(current.as_path()) {
                Ok(metadata)
                    if metadata.file_type().is_symlink()
                        && !claude_trusted_platform_root_symlink(current.as_path()) =>
                {
                    return Err(ClaudeManagedMcpConfigError::new(format!(
                        "Claude managed MCP path `{}` traverses a symlink",
                        current.display()
                    )));
                }
                Ok(metadata)
                    if metadata.file_type().is_symlink()
                        && claude_trusted_platform_root_symlink(current.as_path()) =>
                {
                    break;
                }
                Ok(metadata) if metadata.is_dir() => break,
                Ok(_) => {
                    return Err(ClaudeManagedMcpConfigError::new(format!(
                        "Claude managed MCP path component `{}` is not a directory",
                        current.display()
                    )));
                }
                Err(error) if error.kind() == ErrorKind::NotFound => {
                    match fs::create_dir(current.as_path()) {
                        Ok(()) => break,
                        Err(error) if error.kind() == ErrorKind::AlreadyExists => continue,
                        Err(error) => {
                            return Err(ClaudeManagedMcpConfigError::with_context(
                                "create Claude managed MCP directory",
                                current.as_path(),
                                error,
                            ));
                        }
                    }
                }
                Err(error) => {
                    return Err(ClaudeManagedMcpConfigError::with_context(
                        "inspect Claude managed MCP directory",
                        current.as_path(),
                        error,
                    ));
                }
            }
        }
    }
    if owner_only {
        claude_set_owner_only_directory(path.as_path())?;
    }
    Ok(())
}

fn claude_validate_directory_chain_no_follow(
    path: &Path,
) -> Result<(), ClaudeManagedMcpConfigError> {
    let path = claude_normalize_absolute_path(path)?;
    let mut current = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => current.push(prefix.as_os_str()),
            Component::RootDir => {
                current.push(component.as_os_str());
                continue;
            }
            Component::CurDir => continue,
            Component::ParentDir => unreachable!("path was normalized"),
            Component::Normal(part) => current.push(part),
        }
        let metadata = fs::symlink_metadata(current.as_path()).map_err(|error| {
            ClaudeManagedMcpConfigError::with_context(
                "validate Claude managed MCP directory chain",
                current.as_path(),
                error,
            )
        })?;
        if (metadata.file_type().is_symlink()
            && !claude_trusted_platform_root_symlink(current.as_path()))
            || (!metadata.file_type().is_symlink() && !metadata.is_dir())
        {
            return Err(ClaudeManagedMcpConfigError::new(
                "Claude managed MCP directory chain is not a real directory",
            ));
        }
    }
    Ok(())
}

/// macOS exposes compatibility roots such as `/var -> /private/var` before
/// any application-owned path component. Only these immutable platform roots
/// are accepted; configured or nested managed-path symlinks remain rejected.
fn claude_trusted_platform_root_symlink(path: &Path) -> bool {
    #[cfg(target_os = "macos")]
    {
        if !matches!(path.to_str(), Some("/var" | "/tmp" | "/etc")) {
            return false;
        }
        return fs::canonicalize(path)
            .ok()
            .and_then(|resolved| fs::metadata(resolved).ok())
            .is_some_and(|metadata| metadata.is_dir());
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = path;
        false
    }
}

fn claude_ensure_exact_owner_only_file(
    path: &Path,
    expected: &[u8],
) -> Result<(), ClaudeManagedMcpConfigError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            return Err(ClaudeManagedMcpConfigError::new(
                "Claude managed MCP config must be a real file",
            ));
        }
        Ok(_) => {
            let mut actual = Vec::new();
            claude_open_file_no_follow(path)?
                .take(64 * 1024)
                .read_to_end(&mut actual)
                .map_err(|error| {
                    ClaudeManagedMcpConfigError::with_context(
                        "read Claude managed MCP config",
                        path,
                        error,
                    )
                })?;
            if actual != expected {
                return Err(ClaudeManagedMcpConfigError::new(
                    "existing Claude managed MCP config has unexpected contents",
                ));
            }
            claude_set_owner_only_file(path)
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {
            claude_write_new_owner_only_file(path, expected)
        }
        Err(error) => Err(ClaudeManagedMcpConfigError::with_context(
            "inspect Claude managed MCP config",
            path,
            error,
        )),
    }
}

fn claude_write_new_owner_only_file(
    path: &Path,
    contents: &[u8],
) -> Result<(), ClaudeManagedMcpConfigError> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    let mut file = options.open(path).map_err(|error| {
        ClaudeManagedMcpConfigError::with_context("create Claude managed MCP file", path, error)
    })?;
    file.write_all(contents).map_err(|error| {
        ClaudeManagedMcpConfigError::with_context("write Claude managed MCP file", path, error)
    })?;
    file.sync_all().map_err(|error| {
        ClaudeManagedMcpConfigError::with_context("sync Claude managed MCP file", path, error)
    })?;
    claude_set_owner_only_file(path)
}

fn claude_open_file_no_follow(path: &Path) -> Result<File, ClaudeManagedMcpConfigError> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    options.open(path).map_err(|error| {
        ClaudeManagedMcpConfigError::with_context("open Claude managed MCP file", path, error)
    })
}

fn claude_read_mcp_config_marker(
    path: &Path,
) -> Result<ClaudeManagedMcpConfigMarker, ClaudeManagedMcpConfigError> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        ClaudeManagedMcpConfigError::with_context("inspect Claude managed MCP marker", path, error)
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(ClaudeManagedMcpConfigError::new(
            "Claude managed MCP marker must be a real file",
        ));
    }
    let mut serialized = Vec::new();
    claude_open_file_no_follow(path)?
        .take(64 * 1024)
        .read_to_end(&mut serialized)
        .map_err(|error| {
            ClaudeManagedMcpConfigError::with_context("read Claude managed MCP marker", path, error)
        })?;
    serde_json::from_slice(serialized.as_slice()).map_err(|error| {
        ClaudeManagedMcpConfigError::new(format!(
            "decode Claude managed MCP marker failed: {error}"
        ))
    })
}

fn claude_mcp_identity_component(label: &str, value: &str) -> String {
    let digest = Sha256::digest(value.as_bytes());
    let mut result = String::with_capacity(label.len() + 65);
    result.push_str(label);
    result.push('-');
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(result, "{byte:02x}");
    }
    result
}

fn claude_mcp_config_nonce(identity: &ClaudeManagedMcpConfigIdentity) -> String {
    let counter = CLAUDE_MCP_CONFIG_NONCE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let input = format!(
        "{}:{}:{}:{}:{}:{nanos}:{}:{counter}",
        identity.workspace_id,
        identity.runtime_id,
        identity.logical_thread_id,
        identity.gateway_boot_id,
        identity.process_generation,
        std::process::id()
    );
    claude_mcp_identity_component("materialization", input.as_str())
}

#[cfg(unix)]
fn claude_set_owner_only_directory(path: &Path) -> Result<(), ClaudeManagedMcpConfigError> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(|error| {
        ClaudeManagedMcpConfigError::with_context(
            "set Claude managed MCP directory permissions",
            path,
            error,
        )
    })
}

#[cfg(not(unix))]
fn claude_set_owner_only_directory(_path: &Path) -> Result<(), ClaudeManagedMcpConfigError> {
    Ok(())
}

#[cfg(unix)]
fn claude_set_owner_only_file(path: &Path) -> Result<(), ClaudeManagedMcpConfigError> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(|error| {
        ClaudeManagedMcpConfigError::with_context(
            "set Claude managed MCP file permissions",
            path,
            error,
        )
    })
}

#[cfg(not(unix))]
fn claude_set_owner_only_file(_path: &Path) -> Result<(), ClaudeManagedMcpConfigError> {
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeAccountProbeConfig {
    pub executable: String,
    pub config_dir_path: String,
    pub home_dir: Option<PathBuf>,
    pub env: SensitiveEnvironment,
    pub request_timeout: Duration,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClaudeAccountProbeSnapshot {
    pub status: ClaudeAccountProbeStatus,
    pub message: Option<String>,
    pub version: Option<String>,
    pub account: Option<ClaudeAccountSnapshot>,
    pub diagnostics: Vec<ClaudeProbeDiagnostic>,
    pub stderr: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaudeAccountProbeStatus {
    Ready,
    NeedsAuth,
    MissingBinary,
    SpawnFailed,
    UnsupportedVersion,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeAccountSnapshot {
    pub authenticated: bool,
    pub email: Option<String>,
    pub plan: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaudeProbeDiagnosticLevel {
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeProbeDiagnostic {
    pub level: ClaudeProbeDiagnosticLevel,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClaudeModelListSnapshot {
    pub models: Vec<ClaudeModelSnapshot>,
    pub diagnostics: Vec<ClaudeProbeDiagnostic>,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClaudeModelSnapshot {
    pub id: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub family: Option<String>,
    pub active: Option<bool>,
    pub effort_options: Vec<String>,
    pub input_modalities: Vec<String>,
    pub output_modalities: Vec<String>,
    pub supports_reasoning: Option<bool>,
    pub supports_vision: Option<bool>,
    pub max_input_tokens: Option<u64>,
    pub max_output_tokens: Option<u64>,
}

pub struct ClaudeProbe;

impl ClaudeProbe {
    pub async fn account_read(config: ClaudeAccountProbeConfig) -> ClaudeAccountProbeSnapshot {
        let config_dir =
            match expand_home_path(config.config_dir_path.as_str(), config.home_dir.as_deref()) {
                Ok(path) => path,
                Err(error) => {
                    return ClaudeAccountProbeSnapshot {
                        status: ClaudeAccountProbeStatus::Error,
                        message: Some(format!("failed to resolve Claude config dir: {error:#}")),
                        version: None,
                        account: None,
                        diagnostics: vec![ClaudeProbeDiagnostic {
                            level: ClaudeProbeDiagnosticLevel::Error,
                            code: "claude_probe.config_dir_invalid".to_owned(),
                            message: format!("failed to resolve Claude config dir: {error:#}"),
                        }],
                        stderr: Vec::new(),
                    };
                }
            };

        let mut command = Command::new(config.executable.as_str());
        scrub_inherited_cli_environment(&mut command);
        command.arg("--version");
        command.env("CLAUDE_CONFIG_DIR", &config_dir);
        command.env("CLAUDE_CODE_ENTRYPOINT", "sdk-rs");
        command.env("CLAUDE_AGENT_SDK_CLIENT_APP", "pioneer");
        command.envs(config.env.expose_iter());
        command.env_remove("CLAUDECODE");
        // Account probes are now owned by a cancellable Gateway supervisor.
        // A timeout, shutdown, or superseding configuration generation must
        // not leave a detached `claude --version` process behind.
        command.kill_on_drop(true);

        let output = match timeout(config.request_timeout, command.output()).await {
            Ok(Ok(output)) => output,
            Ok(Err(error)) => return spawn_error_probe_snapshot(&config, error),
            Err(_) => {
                let message = format!(
                    "Claude CLI version probe timed out after {} ms",
                    config.request_timeout.as_millis()
                );
                return ClaudeAccountProbeSnapshot {
                    status: ClaudeAccountProbeStatus::Error,
                    message: Some(message.clone()),
                    version: None,
                    account: None,
                    diagnostics: vec![ClaudeProbeDiagnostic {
                        level: ClaudeProbeDiagnosticLevel::Error,
                        code: "claude_probe.version_timeout".to_owned(),
                        message,
                    }],
                    stderr: Vec::new(),
                };
            }
        };

        let stdout = config
            .env
            .redact_text(String::from_utf8_lossy(&output.stdout).into_owned());
        let stderr = stderr_lines(
            config
                .env
                .redact_text(String::from_utf8_lossy(&output.stderr).into_owned()),
        );
        if !output.status.success() {
            let message = stderr.first().cloned().unwrap_or_else(|| {
                format!("Claude CLI version probe exited with {}", output.status)
            });
            return ClaudeAccountProbeSnapshot {
                status: ClaudeAccountProbeStatus::Error,
                message: Some(message.clone()),
                version: None,
                account: None,
                diagnostics: vec![ClaudeProbeDiagnostic {
                    level: ClaudeProbeDiagnosticLevel::Error,
                    code: "claude_probe.version_failed".to_owned(),
                    message,
                }],
                stderr,
            };
        }

        let version = parse_semver(stdout.as_str())
            .or_else(|| stderr.iter().find_map(|line| parse_semver(line.as_str())));
        let Some(version) = version else {
            let message = "Claude CLI version probe did not return a parseable version".to_owned();
            return ClaudeAccountProbeSnapshot {
                status: ClaudeAccountProbeStatus::Error,
                message: Some(message.clone()),
                version: None,
                account: None,
                diagnostics: vec![ClaudeProbeDiagnostic {
                    level: ClaudeProbeDiagnosticLevel::Error,
                    code: "claude_probe.version_unparseable".to_owned(),
                    message,
                }],
                stderr,
            };
        };

        if semver_lt(version.as_str(), MINIMUM_CLAUDE_CODE_VERSION) {
            let message = format!(
                "Claude CLI version {version} is older than required {MINIMUM_CLAUDE_CODE_VERSION}"
            );
            return ClaudeAccountProbeSnapshot {
                status: ClaudeAccountProbeStatus::UnsupportedVersion,
                message: Some(message.clone()),
                version: Some(version),
                account: None,
                diagnostics: vec![ClaudeProbeDiagnostic {
                    level: ClaudeProbeDiagnosticLevel::Error,
                    code: "claude_probe.unsupported_version".to_owned(),
                    message,
                }],
                stderr,
            };
        }

        match run_initialize_probe(&config, config_dir.as_path()).await {
            Ok(initialize) => {
                let mut diagnostics = vec![ClaudeProbeDiagnostic {
                    level: ClaudeProbeDiagnosticLevel::Info,
                    code: "claude_probe.ready".to_owned(),
                    message: "Claude CLI initialize probe succeeded".to_owned(),
                }];
                diagnostics.extend(initialize.diagnostics.clone());
                let mut stderr = stderr;
                stderr.extend(initialize.stderr);
                ClaudeAccountProbeSnapshot {
                    status: ClaudeAccountProbeStatus::Ready,
                    message: None,
                    version: Some(version),
                    account: initialize.account,
                    diagnostics,
                    stderr,
                }
            }
            Err(error) => error.into_account_snapshot(Some(version), stderr),
        }
    }

    pub async fn model_list(
        config: ClaudeAccountProbeConfig,
        custom_models: &[String],
    ) -> ClaudeModelListSnapshot {
        let config_dir =
            match expand_home_path(config.config_dir_path.as_str(), config.home_dir.as_deref()) {
                Ok(path) => path,
                Err(error) => {
                    return ClaudeModelListSnapshot {
                        models: Vec::new(),
                        diagnostics: vec![ClaudeProbeDiagnostic {
                            level: ClaudeProbeDiagnosticLevel::Error,
                            code: "claude_probe.config_dir_invalid".to_owned(),
                            message: format!("failed to resolve Claude config dir: {error:#}"),
                        }],
                        error_message: Some(format!(
                            "failed to resolve Claude config dir: {error:#}"
                        )),
                    };
                }
            };

        let initialize = match run_initialize_probe(&config, config_dir.as_path()).await {
            Ok(initialize) => initialize,
            Err(error) => {
                return ClaudeModelListSnapshot {
                    models: Vec::new(),
                    diagnostics: error.diagnostics(),
                    error_message: Some(error.message()),
                };
            }
        };
        let mut models = if initialize.models.is_empty() {
            default_claude_models()
        } else {
            initialize.models
        };
        append_custom_models(&mut models, custom_models);
        ClaudeModelListSnapshot {
            models,
            diagnostics: initialize.diagnostics,
            error_message: None,
        }
    }
}

#[derive(Debug)]
struct ClaudeInitializeProbeSnapshot {
    account: Option<ClaudeAccountSnapshot>,
    models: Vec<ClaudeModelSnapshot>,
    diagnostics: Vec<ClaudeProbeDiagnostic>,
    stderr: Vec<String>,
}

#[derive(Debug)]
enum ClaudeInitializeProbeError {
    NeedsAuth {
        message: String,
        stderr: Vec<String>,
    },
    SpawnFailed {
        message: String,
        stderr: Vec<String>,
    },
    Timeout {
        timeout: Duration,
    },
    Protocol {
        message: String,
        stderr: Vec<String>,
    },
}

impl ClaudeInitializeProbeError {
    fn message(&self) -> String {
        match self {
            Self::NeedsAuth { message, .. }
            | Self::SpawnFailed { message, .. }
            | Self::Protocol { message, .. } => message.clone(),
            Self::Timeout { timeout } => {
                format!(
                    "Claude CLI initialize probe timed out after {} ms",
                    timeout.as_millis()
                )
            }
        }
    }

    fn diagnostics(&self) -> Vec<ClaudeProbeDiagnostic> {
        let (level, code) = match self {
            Self::NeedsAuth { .. } => (
                ClaudeProbeDiagnosticLevel::Warning,
                "claude_probe.needs_auth",
            ),
            Self::SpawnFailed { .. } => (
                ClaudeProbeDiagnosticLevel::Error,
                "claude_probe.initialize_failed",
            ),
            Self::Timeout { .. } => (
                ClaudeProbeDiagnosticLevel::Error,
                "claude_probe.initialize_timeout",
            ),
            Self::Protocol { .. } => (
                ClaudeProbeDiagnosticLevel::Error,
                "claude_probe.initialize_invalid",
            ),
        };
        vec![ClaudeProbeDiagnostic {
            level,
            code: code.to_owned(),
            message: self.message(),
        }]
    }

    fn stderr(self) -> Vec<String> {
        match self {
            Self::NeedsAuth { stderr, .. }
            | Self::SpawnFailed { stderr, .. }
            | Self::Protocol { stderr, .. } => stderr,
            Self::Timeout { .. } => Vec::new(),
        }
    }

    fn status(&self) -> ClaudeAccountProbeStatus {
        match self {
            Self::NeedsAuth { .. } => ClaudeAccountProbeStatus::NeedsAuth,
            Self::SpawnFailed { .. } => ClaudeAccountProbeStatus::SpawnFailed,
            Self::Timeout { .. } | Self::Protocol { .. } => ClaudeAccountProbeStatus::Error,
        }
    }

    fn into_account_snapshot(
        self,
        version: Option<String>,
        mut prior_stderr: Vec<String>,
    ) -> ClaudeAccountProbeSnapshot {
        let status = self.status();
        let message = self.message();
        let diagnostics = self.diagnostics();
        prior_stderr.extend(self.stderr());
        ClaudeAccountProbeSnapshot {
            status,
            message: Some(message),
            version,
            account: None,
            diagnostics,
            stderr: prior_stderr,
        }
    }
}

async fn run_initialize_probe(
    config: &ClaudeAccountProbeConfig,
    config_dir: &std::path::Path,
) -> Result<ClaudeInitializeProbeSnapshot, ClaudeInitializeProbeError> {
    let mut command = Command::new(config.executable.as_str());
    scrub_inherited_cli_environment(&mut command);
    command
        .args([
            "--output-format",
            "stream-json",
            "--verbose",
            "--permission-prompt-tool",
            "stdio",
            "--permission-mode",
            "default",
            "--safe-mode",
            "--setting-sources=",
            "--include-partial-messages",
            "--input-format",
            "stream-json",
        ])
        .env("CLAUDE_CONFIG_DIR", config_dir)
        .env("CLAUDE_CODE_ENTRYPOINT", "sdk-rs")
        .env("CLAUDE_AGENT_SDK_CLIENT_APP", "pioneer")
        .env("CLAUDE_AGENT_SDK_VERSION", env!("CARGO_PKG_VERSION"))
        .envs(config.env.expose_iter())
        .env_remove("CLAUDECODE")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    let mut child = command
        .spawn()
        .map_err(|error| ClaudeInitializeProbeError::SpawnFailed {
            message: format!("failed to spawn Claude CLI initialize probe: {error}"),
            stderr: Vec::new(),
        })?;
    if let Some(mut stdin) = child.stdin.take() {
        let initialize_line = serde_json::json!({
            "type": "control_request",
            "request_id": "pioneer_probe_initialize",
            "request": { "subtype": "initialize", "hooks": null },
        })
        .to_string();
        if let Err(error) = stdin.write_all(initialize_line.as_bytes()).await {
            return Err(ClaudeInitializeProbeError::SpawnFailed {
                message: format!("failed to write Claude initialize probe: {error}"),
                stderr: Vec::new(),
            });
        }
        if let Err(error) = stdin.write_all(b"\n").await {
            return Err(ClaudeInitializeProbeError::SpawnFailed {
                message: format!("failed to finish Claude initialize probe: {error}"),
                stderr: Vec::new(),
            });
        }
    }

    let output = match timeout(config.request_timeout, child.wait_with_output()).await {
        Ok(Ok(output)) => output,
        Ok(Err(error)) => {
            return Err(ClaudeInitializeProbeError::SpawnFailed {
                message: format!("Claude CLI initialize probe failed: {error}"),
                stderr: Vec::new(),
            });
        }
        Err(_) => {
            return Err(ClaudeInitializeProbeError::Timeout {
                timeout: config.request_timeout,
            });
        }
    };
    let stderr = stderr_lines(
        config
            .env
            .redact_text(String::from_utf8_lossy(&output.stderr).into_owned()),
    );
    if !output.status.success() {
        let message = stderr.first().cloned().unwrap_or_else(|| {
            format!("Claude CLI initialize probe exited with {}", output.status)
        });
        return Err(classify_initialize_error(message, stderr));
    }

    let stdout = config
        .env
        .redact_text(String::from_utf8_lossy(&output.stdout).into_owned());
    parse_initialize_probe_stdout(stdout.as_bytes(), stderr)
}

fn parse_initialize_probe_stdout(
    stdout: &[u8],
    stderr: Vec<String>,
) -> Result<ClaudeInitializeProbeSnapshot, ClaudeInitializeProbeError> {
    for line in String::from_utf8_lossy(stdout).lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<JsonValue>(trimmed) else {
            continue;
        };
        match value.get("type").and_then(JsonValue::as_str) {
            Some("control_response") => {
                let response = value.get("response").cloned().unwrap_or(JsonValue::Null);
                if response.get("request_id").and_then(JsonValue::as_str)
                    != Some("pioneer_probe_initialize")
                {
                    continue;
                }
                if response.get("subtype").and_then(JsonValue::as_str) == Some("error") {
                    let message = response
                        .get("error")
                        .and_then(JsonValue::as_str)
                        .unwrap_or("Claude initialize probe failed")
                        .to_owned();
                    return Err(classify_initialize_error(message, stderr));
                }
                let body = response.get("response").cloned().unwrap_or(JsonValue::Null);
                let models = body
                    .get("models")
                    .and_then(JsonValue::as_array)
                    .map(|models| {
                        models
                            .iter()
                            .filter_map(claude_model_from_initialize_model)
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                let diagnostics = if models.is_empty() {
                    vec![ClaudeProbeDiagnostic {
                        level: ClaudeProbeDiagnosticLevel::Warning,
                        code: "claude_probe.models_missing".to_owned(),
                        message: "Claude initialize probe did not return model metadata; using built-in defaults".to_owned(),
                    }]
                } else {
                    Vec::new()
                };
                return Ok(ClaudeInitializeProbeSnapshot {
                    account: body
                        .get("account")
                        .and_then(claude_account_from_initialize_account),
                    models,
                    diagnostics,
                    stderr,
                });
            }
            Some("auth_status") => {
                let message = value
                    .get("error")
                    .or_else(|| value.get("output"))
                    .and_then(JsonValue::as_str)
                    .unwrap_or("Claude CLI authentication is required")
                    .to_owned();
                return Err(ClaudeInitializeProbeError::NeedsAuth { message, stderr });
            }
            Some("error") => {
                let message = value
                    .get("error")
                    .and_then(JsonValue::as_str)
                    .unwrap_or("Claude initialize probe failed")
                    .to_owned();
                return Err(classify_initialize_error(message, stderr));
            }
            _ => {}
        }
    }
    Err(ClaudeInitializeProbeError::Protocol {
        message: "Claude initialize probe did not return an initialize response".to_owned(),
        stderr,
    })
}

fn classify_initialize_error(message: String, stderr: Vec<String>) -> ClaudeInitializeProbeError {
    let lowered = message.to_ascii_lowercase();
    if lowered.contains("auth")
        || lowered.contains("login")
        || lowered.contains("oauth")
        || lowered.contains("credential")
    {
        ClaudeInitializeProbeError::NeedsAuth { message, stderr }
    } else {
        ClaudeInitializeProbeError::Protocol { message, stderr }
    }
}

fn spawn_error_probe_snapshot(
    config: &ClaudeAccountProbeConfig,
    error: std::io::Error,
) -> ClaudeAccountProbeSnapshot {
    let is_missing_binary = error.kind() == ErrorKind::NotFound;
    let status = if is_missing_binary {
        ClaudeAccountProbeStatus::MissingBinary
    } else {
        ClaudeAccountProbeStatus::SpawnFailed
    };
    let code = if is_missing_binary {
        "claude_probe.missing_binary"
    } else {
        "claude_probe.spawn_failed"
    };
    let message = if is_missing_binary {
        format!("Claude CLI binary `{}` was not found", config.executable)
    } else {
        format!("failed to spawn Claude CLI: {error}")
    };
    ClaudeAccountProbeSnapshot {
        status,
        message: Some(message.clone()),
        version: None,
        account: None,
        diagnostics: vec![ClaudeProbeDiagnostic {
            level: ClaudeProbeDiagnosticLevel::Error,
            code: code.to_owned(),
            message,
        }],
        stderr: Vec::new(),
    }
}

fn default_claude_models() -> Vec<ClaudeModelSnapshot> {
    [
        (
            "default",
            "Default (recommended)",
            "Claude CLI default model",
        ),
        ("opus", "Opus", "Claude Opus"),
        (
            "opus[1m]",
            "Opus (1M context)",
            "Claude Opus with 1M context",
        ),
        ("sonnet", "Sonnet", "Claude Sonnet"),
        (
            "sonnet[1m]",
            "Sonnet (1M context)",
            "Claude Sonnet with 1M context",
        ),
        ("haiku", "Haiku", "Claude Haiku"),
    ]
    .into_iter()
    .map(|(id, name, description)| ClaudeModelSnapshot {
        id: id.to_owned(),
        name: Some(name.to_owned()),
        description: Some(description.to_owned()),
        family: Some("claude".to_owned()),
        active: (id == "default").then_some(true),
        effort_options: Vec::new(),
        input_modalities: vec!["text".to_owned(), "image".to_owned()],
        output_modalities: vec!["text".to_owned()],
        supports_reasoning: Some(true),
        supports_vision: Some(true),
        max_input_tokens: None,
        max_output_tokens: None,
    })
    .collect()
}

fn append_custom_models(models: &mut Vec<ClaudeModelSnapshot>, custom_models: &[String]) {
    for custom in custom_models {
        let id = custom.trim();
        if id.is_empty() || models.iter().any(|model| model.id == id) {
            continue;
        }
        models.push(ClaudeModelSnapshot {
            id: id.to_owned(),
            name: Some(id.to_owned()),
            description: Some("Custom Claude CLI model".to_owned()),
            family: Some("claude".to_owned()),
            active: None,
            effort_options: Vec::new(),
            input_modalities: vec!["text".to_owned(), "image".to_owned()],
            output_modalities: vec!["text".to_owned()],
            supports_reasoning: Some(true),
            supports_vision: Some(true),
            max_input_tokens: None,
            max_output_tokens: None,
        });
    }
}

fn claude_account_from_initialize_account(value: &JsonValue) -> Option<ClaudeAccountSnapshot> {
    let email = value
        .get("email")
        .and_then(JsonValue::as_str)
        .map(str::to_owned);
    let plan = value
        .get("subscriptionType")
        .or_else(|| value.get("subscription_type"))
        .or_else(|| value.get("apiProvider"))
        .and_then(JsonValue::as_str)
        .map(str::to_owned);
    let authenticated = email.is_some()
        || value
            .get("apiProvider")
            .and_then(JsonValue::as_str)
            .is_some()
        || value
            .get("apiKeySource")
            .and_then(JsonValue::as_str)
            .is_some()
        || value
            .get("tokenSource")
            .and_then(JsonValue::as_str)
            .is_some();
    if !authenticated && plan.is_none() {
        return None;
    }
    Some(ClaudeAccountSnapshot {
        authenticated,
        email,
        plan,
    })
}

fn claude_model_from_initialize_model(value: &JsonValue) -> Option<ClaudeModelSnapshot> {
    let id = value
        .get("value")
        .or_else(|| value.get("id"))
        .and_then(JsonValue::as_str)?
        .trim();
    if id.is_empty() {
        return None;
    }
    let supports_effort = value
        .get("supportsEffort")
        .or_else(|| value.get("supports_effort"))
        .and_then(JsonValue::as_bool)
        .unwrap_or(false);
    let effort_options = value
        .get("supportedEffortLevels")
        .or_else(|| value.get("supported_effort_levels"))
        .and_then(JsonValue::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(JsonValue::as_str)
                .filter_map(normalize_metadata_reasoning_effort)
                .collect::<Vec<_>>()
        })
        .filter(|values| !values.is_empty())
        .unwrap_or_else(|| {
            if supports_effort {
                ["low", "medium", "high", "max"]
                    .into_iter()
                    .map(str::to_owned)
                    .collect()
            } else {
                Vec::new()
            }
        });
    let supports_adaptive_thinking = value
        .get("supportsAdaptiveThinking")
        .or_else(|| value.get("supports_adaptive_thinking"))
        .and_then(JsonValue::as_bool)
        .unwrap_or(false);
    Some(ClaudeModelSnapshot {
        id: id.to_owned(),
        name: value
            .get("displayName")
            .or_else(|| value.get("display_name"))
            .and_then(JsonValue::as_str)
            .map(str::to_owned),
        description: value
            .get("description")
            .and_then(JsonValue::as_str)
            .map(str::to_owned),
        family: Some("claude".to_owned()),
        active: (id == "default").then_some(true),
        effort_options,
        input_modalities: vec!["text".to_owned(), "image".to_owned()],
        output_modalities: vec!["text".to_owned()],
        supports_reasoning: Some(supports_effort || supports_adaptive_thinking),
        supports_vision: Some(true),
        max_input_tokens: None,
        max_output_tokens: None,
    })
}

fn stderr_lines(value: String) -> Vec<String> {
    value
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_owned)
        .collect()
}

fn parse_semver(value: &str) -> Option<String> {
    value
        .split(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '.' || ch == '-' || ch == '+'))
        .find(|part| {
            let mut pieces = part.split('.');
            pieces
                .next()
                .is_some_and(|major| major.chars().all(|ch| ch.is_ascii_digit()))
                && pieces
                    .next()
                    .is_some_and(|minor| minor.chars().all(|ch| ch.is_ascii_digit()))
                && pieces
                    .next()
                    .is_some_and(|patch| patch.chars().next().is_some_and(|ch| ch.is_ascii_digit()))
        })
        .map(str::to_owned)
}

fn semver_lt(left: &str, right: &str) -> bool {
    let left = semver_core(left);
    let right = semver_core(right);
    left < right
}

fn semver_core(value: &str) -> (u64, u64, u64) {
    let mut parts = value.split('.');
    let major = parts.next().and_then(parse_semver_part).unwrap_or(0);
    let minor = parts.next().and_then(parse_semver_part).unwrap_or(0);
    let patch = parts.next().and_then(parse_semver_part).unwrap_or(0);
    (major, minor, patch)
}

fn parse_semver_part(value: &str) -> Option<u64> {
    let digits = value
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .collect::<String>();
    digits.parse().ok()
}

pub fn claude_redacted_native_event(method: impl Into<String>, raw: JsonValue) -> JsonValue {
    serde_json::json!({
        "method": method.into(),
        "raw": redact_claude_native_payload(raw),
    })
}

pub fn redact_claude_native_payload(value: JsonValue) -> JsonValue {
    match value {
        JsonValue::Object(map) => JsonValue::Object(
            map.into_iter()
                .map(|(key, value)| {
                    let lowered = key.to_ascii_lowercase();
                    if lowered.contains("token")
                        || lowered.contains("secret")
                        || lowered.contains("password")
                        || lowered.contains("authorization")
                    {
                        (key, JsonValue::String("<redacted>".to_owned()))
                    } else {
                        (key, redact_claude_native_payload(value))
                    }
                })
                .collect(),
        ),
        JsonValue::Array(items) => JsonValue::Array(
            items
                .into_iter()
                .map(redact_claude_native_payload)
                .collect(),
        ),
        other => other,
    }
}

pub const CLAUDE_PIONEER_MCP_TOOL_PREFIX: &str = "mcp__pioneer__";

#[derive(Debug, Clone, PartialEq)]
pub enum ClaudeMcpContentBlock {
    ToolUse {
        tool_use_id: String,
        qualified_tool_name: String,
        input: JsonValue,
    },
    ToolResult {
        tool_use_id: String,
        content: JsonValue,
        is_error: bool,
    },
}

/// Decode only the provider-native blocks relevant to the synthetic Pioneer
/// MCP server. Session identity remains an outer stream invariant enforced by
/// the Gateway before any decoded block is admitted to its invocation ledger.
pub fn decode_claude_mcp_content_blocks(value: &JsonValue) -> Vec<ClaudeMcpContentBlock> {
    let content = value
        .get("message")
        .and_then(|message| message.get("content"))
        .and_then(JsonValue::as_array)
        .into_iter()
        .flatten();
    let mut decoded = Vec::new();
    for block in content {
        match block.get("type").and_then(JsonValue::as_str) {
            Some("tool_use") => {
                let Some(tool_use_id) = block
                    .get("id")
                    .and_then(JsonValue::as_str)
                    .filter(|value| !value.trim().is_empty())
                else {
                    continue;
                };
                let Some(qualified_tool_name) = block
                    .get("name")
                    .and_then(JsonValue::as_str)
                    .filter(|name| name.starts_with(CLAUDE_PIONEER_MCP_TOOL_PREFIX))
                else {
                    continue;
                };
                decoded.push(ClaudeMcpContentBlock::ToolUse {
                    tool_use_id: tool_use_id.to_owned(),
                    qualified_tool_name: qualified_tool_name.to_owned(),
                    input: block
                        .get("input")
                        .cloned()
                        .unwrap_or_else(|| JsonValue::Object(serde_json::Map::new())),
                });
            }
            Some("tool_result") => {
                let Some(tool_use_id) = block
                    .get("tool_use_id")
                    .and_then(JsonValue::as_str)
                    .filter(|value| !value.trim().is_empty())
                else {
                    continue;
                };
                decoded.push(ClaudeMcpContentBlock::ToolResult {
                    tool_use_id: tool_use_id.to_owned(),
                    content: block.get("content").cloned().unwrap_or(JsonValue::Null),
                    is_error: block
                        .get("is_error")
                        .and_then(JsonValue::as_bool)
                        .unwrap_or(false),
                });
            }
            _ => {}
        }
    }
    decoded
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp::{canonical_mcp_schema_fingerprint, transform_mcp_tool_schema};
    use serde_json::json;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    fn claude_mcp_config_temp_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "pioneer-claude-mcp-config-{label}-{}-{nanos}",
            std::process::id()
        ));
        fs::create_dir_all(path.as_path()).expect("create temp directory");
        path
    }

    fn claude_mcp_config_identity(generation: u64) -> ClaudeManagedMcpConfigIdentity {
        ClaudeManagedMcpConfigIdentity::new(
            "workspace",
            "claude",
            "thread",
            "gateway-boot",
            generation,
        )
        .expect("identity")
    }

    fn transform_claude_schema(schema: JsonValue) -> crate::mcp::TransformedMcpToolSchema {
        let input = CanonicalMcpToolSchema {
            canonical_callable_name: "mcp_pioneer_fixture".to_owned(),
            canonical_schema_fingerprint: canonical_mcp_schema_fingerprint(&schema)
                .expect("canonical fingerprint"),
            canonical_schema: schema,
        };
        transform_mcp_tool_schema(
            &input,
            &ClaudeMcpSchemaTransformer::new("a".repeat(64)).expect("transformer"),
        )
        .expect("Claude schema transform")
    }

    #[test]
    fn claude_session_id_launch_is_typed_exact_and_redacted() {
        let provider_session_id =
            uuid::Uuid::parse_str("01900000-0000-7000-8000-000000000031").expect("UUID");
        let new = ClaudeProviderSessionLaunch::new(provider_session_id).expect("new launch");
        let resume =
            ClaudeProviderSessionLaunch::resume(provider_session_id).expect("resume launch");
        let mut new_args = Vec::new();
        let mut resume_args = Vec::new();
        new.append_process_args(&mut new_args);
        resume.append_process_args(&mut resume_args);
        assert_eq!(
            new_args,
            vec!["--session-id".to_owned(), provider_session_id.to_string()]
        );
        assert_eq!(
            resume_args,
            vec!["--resume".to_owned(), provider_session_id.to_string()]
        );
        assert_eq!(new.provider_session_id(), provider_session_id);
        assert!(!format!("{new:?}").contains(provider_session_id.to_string().as_str()));
        assert!(!format!("{resume:?}").contains(provider_session_id.to_string().as_str()));
        assert!(ClaudeProviderSessionLaunch::new(uuid::Uuid::nil()).is_err());
        assert!(ClaudeProviderSessionLaunch::resume(uuid::Uuid::nil()).is_err());
    }

    #[test]
    fn claude_mcp_event_fixtures_decode_success_error_parallel_and_replay() {
        let fixture: JsonValue =
            serde_json::from_str(include_str!("../tests/fixtures/claude_mcp/lifecycle.json"))
                .expect("Claude MCP lifecycle fixture");
        let messages = fixture["messages"].as_array().expect("messages");
        let decoded = messages
            .iter()
            .flat_map(decode_claude_mcp_content_blocks)
            .collect::<Vec<_>>();
        assert_eq!(decoded.len(), 6);
        assert!(matches!(
            &decoded[0],
            ClaudeMcpContentBlock::ToolUse { tool_use_id, qualified_tool_name, .. }
                if tool_use_id == "call-a"
                    && qualified_tool_name == "mcp__pioneer__mcp_server_tool_a"
        ));
        assert!(matches!(
            &decoded[2],
            ClaudeMcpContentBlock::ToolResult { tool_use_id, is_error: false, .. }
                if tool_use_id == "call-a"
        ));
        assert!(matches!(
            &decoded[3],
            ClaudeMcpContentBlock::ToolResult { tool_use_id, is_error: true, .. }
                if tool_use_id == "call-b"
        ));
        assert_eq!(decoded[0], decoded[4], "replayed tool_use fixture");
        assert_eq!(decoded[2], decoded[5], "replayed tool_result fixture");
    }

    #[test]
    fn claude_schema_is_deterministic_opaque_pass_through() {
        let schema = json!({
            "$schema": "https://example.test/custom-mcp-dialect",
            "type": "object",
            "oneOf": [
                {"properties": {"value": {"type": "string", "nullable": true}}},
                false
            ],
            "patternProperties": {
                "^x-": {"$dynamicRef": "https://example.test/external"}
            },
            "x-provider-extension": {"arbitrary": [true, false, null]}
        });
        let first = transform_claude_schema(schema.clone());
        let second = transform_claude_schema(schema.clone());
        assert_eq!(first, second);
        assert_eq!(first.transformed_schema, schema);
        assert_eq!(
            first.canonical_schema_fingerprint,
            first.transformed_schema_fingerprint
        );
        assert_eq!(
            transform_claude_schema(json!({})).transformed_schema,
            json!({})
        );
    }

    #[test]
    fn claude_schema_arbitrary_keywords_pass_and_contract_identity_remains_exact() {
        let schema = json!({
            "type": "object",
            "properties": {
                "value": {
                    "oneOf": [{"type": "string"}, {"type": "integer"}],
                    "unevaluatedProperties": false
                }
            }
        });
        let input = CanonicalMcpToolSchema {
            canonical_callable_name: "mcp_pioneer_fixture".to_owned(),
            canonical_schema_fingerprint: canonical_mcp_schema_fingerprint(&schema)
                .expect("canonical fingerprint"),
            canonical_schema: schema.clone(),
        };
        let passed = transform_mcp_tool_schema(
            &input,
            &ClaudeMcpSchemaTransformer::new("a".repeat(64)).expect("transformer"),
        )
        .expect("arbitrary schema pass-through");
        assert_eq!(passed.transformed_schema, schema);
        let first = transform_mcp_tool_schema(
            &input,
            &ClaudeMcpSchemaTransformer::new("a".repeat(64)).expect("first"),
        )
        .expect("first transform");
        let second = transform_mcp_tool_schema(
            &input,
            &ClaudeMcpSchemaTransformer::new("b".repeat(64)).expect("second"),
        )
        .expect("second transform");
        assert_eq!(
            first.transformed_schema_fingerprint,
            second.transformed_schema_fingerprint
        );
        assert_ne!(
            first.transformed_fingerprint,
            second.transformed_fingerprint
        );
    }

    #[test]
    fn claude_schema_contract_fixture_is_version_pinned() {
        let evidence: JsonValue = serde_json::from_str(include_str!(
            "../tests/fixtures/claude_mcp_schema_2_1_197_contract.json"
        ))
        .expect("Claude schema evidence");
        assert_eq!(evidence["claudeVersion"], "2.1.197");
        assert_eq!(evidence["transformerId"], CLAUDE_SCHEMA_TRANSFORMER_ID);
        assert_eq!(
            evidence["contractVersion"],
            CLAUDE_MCP_SCHEMA_TRANSFORM_CONTRACT_VERSION
        );
        assert_eq!(evidence["qualifiedToolPrefix"], "mcp__pioneer__");
        assert_eq!(evidence["strictMcpConfig"], true);
    }

    #[test]
    fn claude_attestation_cache_identity_and_contract_fingerprint_are_exact() {
        let executable = std::env::current_exe().expect("current executable");
        let identity = claude_executable_cache_identity(
            executable.to_string_lossy().as_ref(),
            "2.1.197",
            "a".repeat(64).as_str(),
        )
        .expect("cache identity");
        let attestation = attest_claude_executable_identity(identity.clone())
            .expect("Claude executable attestation");
        assert_eq!(attestation.cache_identity, identity);
        assert_eq!(attestation.binary_sha256.len(), 64);
        assert_eq!(attestation.local_executable_fingerprint.len(), 64);
        assert_ne!(
            claude_continuation_contract_fingerprint().expect("continuation contract"),
            attestation.local_executable_fingerprint
        );
        validate_recorded_claude_mcp_decoder_fixtures().expect("decoder fixtures");
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn claude_attestation_help_tracks_hidden_sdk_flag_separately() {
        let root = claude_mcp_config_temp_dir("help-contract");
        let executable = root.join("claude-help-fixture");
        fs::write(
            executable.as_path(),
            b"#!/bin/sh\nprintf '%s\\n' '--mcp-config --strict-mcp-config --allowedTools --setting-sources --safe-mode --session-id --resume --no-session-persistence --input-format --output-format'\n",
        )
        .expect("help fixture");
        fs::set_permissions(executable.as_path(), fs::Permissions::from_mode(0o700))
            .expect("help fixture permissions");
        // Exercise the same bounded startup window used by Gateway readiness.
        // A one-second test-only budget made the shell spawn itself the oracle
        // under a parallel workspace test load, rather than the help contract.
        let evidence = probe_claude_help_contract(executable.as_path(), Duration::from_secs(5))
            .await
            .expect("public help contract");
        assert!(
            !evidence
                .required_flags
                .iter()
                .any(|flag| flag == "--permission-prompt-tool")
        );
        assert_eq!(evidence.hidden_managed_flags, ["--permission-prompt-tool"]);
        fs::remove_dir_all(root).expect("cleanup help fixture");
    }

    #[test]
    #[cfg(unix)]
    fn claude_mcp_launch_config_serializes_strict_empty_and_exact_pioneer() {
        let root = claude_mcp_config_temp_dir("launch-config");
        let helper = std::env::current_exe().expect("current test executable");
        let bootstrap = root.join("bootstrap.json");
        fs::write(bootstrap.as_path(), b"{}").expect("bootstrap");
        fs::set_permissions(bootstrap.as_path(), fs::Permissions::from_mode(0o600))
            .expect("bootstrap permissions");

        let empty = serialize_claude_managed_mcp_config(ClaudeManagedMcpConfigInput::empty())
            .expect("empty artifact");
        let exact = serialize_claude_managed_mcp_config(ClaudeManagedMcpConfigInput::pioneer(
            helper.clone(),
            bootstrap.clone(),
        ))
        .expect("exact artifact");

        assert_eq!(empty.config_json(), "{\"mcpServers\":{}}\n");
        assert!(!empty.has_pioneer_server());
        assert!(exact.has_pioneer_server());
        assert_eq!(
            exact.artifact_digest(),
            claude_sha256_hex(exact.config_json().as_bytes())
        );
        let value: JsonValue = serde_json::from_str(exact.config_json()).expect("exact JSON");
        assert_eq!(
            value["mcpServers"]
                .as_object()
                .expect("servers")
                .keys()
                .collect::<Vec<_>>(),
            ["pioneer"]
        );
        assert_eq!(value["mcpServers"]["pioneer"]["type"], "stdio");
        assert_eq!(
            value["mcpServers"]["pioneer"]["command"],
            helper.to_string_lossy().as_ref()
        );
        assert_eq!(
            value["mcpServers"]["pioneer"]["args"],
            json!([
                "__cli-mcp-stdio",
                "--bootstrap-file",
                bootstrap.to_string_lossy()
            ])
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    #[cfg(unix)]
    fn claude_mcp_config_is_exact_owner_only_generation_scoped_and_idempotent() {
        let root = claude_mcp_config_temp_dir("exact");
        let managed = root.join("managed");
        let first =
            materialize_claude_empty_mcp_config(managed.as_path(), claude_mcp_config_identity(1))
                .expect("first config");
        let first_again =
            materialize_claude_empty_mcp_config(managed.as_path(), claude_mcp_config_identity(1))
                .expect("idempotent config");
        let second =
            materialize_claude_empty_mcp_config(managed.as_path(), claude_mcp_config_identity(2))
                .expect("second config");

        assert_eq!(first, first_again);
        assert_ne!(first.config_path, second.config_path);
        assert_eq!(
            fs::read(first.config_path.as_path()).expect("read empty config"),
            b"{\"mcpServers\":{}}\n"
        );
        assert_eq!(
            fs::metadata(first.session_root_path.as_path())
                .expect("session root metadata")
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(first.config_path.as_path())
                .expect("config metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        cleanup_claude_managed_mcp_config(&first).expect("cleanup first");
        cleanup_claude_managed_mcp_config(&first_again).expect("idempotent cleanup");
        cleanup_claude_managed_mcp_config(&second).expect("cleanup second");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    #[cfg(unix)]
    fn claude_system_prompt_extension_is_exact_owner_only_and_generation_scoped() {
        let root = claude_mcp_config_temp_dir("system-prompt-extension");
        let managed = root.join("managed");
        let descriptor =
            materialize_claude_empty_mcp_config(managed.as_path(), claude_mcp_config_identity(1))
                .expect("managed config");
        let text = "Pioneer elevated system instructions";
        let instructions =
            CLIRuntimeElevatedInstructions::try_new(text, claude_sha256_hex(text.as_bytes()))
                .expect("elevated instructions");

        let prompt_path = materialize_claude_system_prompt_extension(&descriptor, &instructions)
            .expect("system prompt extension");
        assert_eq!(
            prompt_path.parent(),
            Some(descriptor.session_root_path.as_path())
        );
        assert_eq!(
            fs::read(prompt_path.as_path()).expect("prompt text"),
            text.as_bytes()
        );
        assert_eq!(
            fs::metadata(prompt_path.as_path())
                .expect("prompt metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );

        cleanup_claude_managed_mcp_config(&descriptor).expect("cleanup managed session");
        assert!(!prompt_path.exists());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    #[cfg(unix)]
    fn claude_mcp_config_cleanup_rejects_replacement_and_symlinked_root() {
        let root = claude_mcp_config_temp_dir("replacement");
        let managed = root.join("managed");
        let stale =
            materialize_claude_empty_mcp_config(managed.as_path(), claude_mcp_config_identity(1))
                .expect("stale config");
        cleanup_claude_managed_mcp_config(&stale).expect("clean stale config");
        let replacement =
            materialize_claude_empty_mcp_config(managed.as_path(), claude_mcp_config_identity(1))
                .expect("replacement config");
        let error = cleanup_claude_managed_mcp_config(&stale)
            .expect_err("stale cleanup must reject replacement");
        assert!(error.to_string().contains("replacement path"));
        assert!(replacement.config_path.exists());
        cleanup_claude_managed_mcp_config(&replacement).expect("clean replacement");

        fs::remove_dir_all(managed.as_path()).expect("remove managed root");
        let outside = root.join("outside");
        fs::create_dir_all(outside.as_path()).expect("create outside root");
        std::os::unix::fs::symlink(outside.as_path(), managed.as_path())
            .expect("create symlinked managed root");
        let error =
            materialize_claude_empty_mcp_config(managed.as_path(), claude_mcp_config_identity(2))
                .expect_err("symlinked managed root must fail closed");
        assert!(error.to_string().contains("traverses a symlink"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn claude_model_parser_preserves_camel_case_effort_metadata() {
        let model = claude_model_from_initialize_model(&json!({
            "value": "claude-sonnet-4-6",
            "displayName": "Claude Sonnet 4.6",
            "supportsEffort": true,
            "supportedEffortLevels": ["low", "medium", "high", "max"]
        }))
        .expect("model metadata");

        assert_eq!(model.id, "claude-sonnet-4-6");
        assert_eq!(model.name.as_deref(), Some("Claude Sonnet 4.6"));
        assert_eq!(model.effort_options, vec!["low", "medium", "high", "max"]);
        assert_eq!(model.supports_reasoning, Some(true));
    }

    #[test]
    fn claude_model_parser_preserves_snake_case_effort_metadata() {
        let model = claude_model_from_initialize_model(&json!({
            "id": "claude-opus-4-8",
            "display_name": "Claude Opus 4.8",
            "supports_effort": true,
            "supported_effort_levels": ["low", "medium", "high", "extra-high", "maximum"]
        }))
        .expect("model metadata");

        assert_eq!(model.id, "claude-opus-4-8");
        assert_eq!(
            model.effort_options,
            vec!["low", "medium", "high", "xhigh", "max"]
        );
        assert_eq!(model.supports_reasoning, Some(true));
    }

    #[test]
    fn claude_model_parser_preserves_runtime_defined_effort_metadata() {
        let model = claude_model_from_initialize_model(&json!({
            "id": "claude-future",
            "supportsEffort": true,
            "supportedEffortLevels": ["low", "future-level"]
        }))
        .expect("model metadata");

        assert_eq!(model.effort_options, vec!["low", "future-level"]);
    }

    #[test]
    fn claude_model_parser_defaults_effort_levels_when_only_support_flag_exists() {
        let model = claude_model_from_initialize_model(&json!({
            "id": "claude-custom",
            "supportsEffort": true
        }))
        .expect("model metadata");

        assert_eq!(model.effort_options, vec!["low", "medium", "high", "max"]);
        assert_eq!(model.supports_reasoning, Some(true));
    }
}
