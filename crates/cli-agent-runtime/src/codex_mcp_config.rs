use serde::Serialize;
use serde_json::{Map as JsonMap, Value as JsonValue};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashSet};
use std::error::Error;
use std::fmt;
use std::path::{Path, PathBuf};

const CONFIG_CONTRACT_VERSION: u32 = 1;
const SYNTHETIC_SERVER_NAME: &str = "pioneer";
const HELPER_SUBCOMMAND: &str = "__cli-mcp-stdio";
const HELPER_BOOTSTRAP_OPTION: &str = "--bootstrap-file";
const APPROVAL_MODE_APPROVE: &str = "approve";
const CODEX_DEFAULT_PERSONALITY: &str = "pragmatic";
const MAX_TOOLS: usize = 128;
const MAX_CALLABLE_NAME_BYTES: usize = 64;
const MAX_MANAGED_PATH_BYTES: usize = 4_096;
const MAX_CONFIG_BYTES: usize = 1024 * 1024;

const ISOLATION_FEATURE_NAMES: &[&str] = &[
    "apps",
    "enable_mcp_apps",
    "plugins",
    "remote_plugin",
    "skill_mcp_dependency_install",
];

/// Provider-transformed identity used by both the exact Codex config and its
/// semantic restart decision. It deliberately contains no upstream MCP
/// address, command, environment, header, or credential.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexManagedMcpToolIdentity {
    pub canonical_callable_name: String,
    pub canonical_schema_fingerprint: String,
    pub transformed_schema_fingerprint: String,
    pub transform_contract_fingerprint: String,
    pub transformed_fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexManagedMcpSemanticInput {
    pub canonical_manifest_hash: String,
    pub provider_manifest_hash: String,
    pub provider_contract_fingerprint: String,
    pub overlay_policy_version: u32,
    pub tools: Vec<CodexManagedMcpToolIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexManagedMcpConfigInput {
    pub semantic: CodexManagedMcpSemanticInput,
    /// Required only for a non-empty projection. The production caller must
    /// resolve this from the signed, running Pioneer `current_exe()`.
    pub helper_path: Option<PathBuf>,
    /// Required only for a non-empty projection and scoped to one bridge
    /// process generation.
    pub bootstrap_path: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexManagedMcpConfigArtifact {
    pub config_toml: String,
    pub artifact_digest: String,
    pub staged_mcp_servers_fingerprint: String,
    pub effective_mcp_servers_fingerprint: String,
    pub semantic_restart_fingerprint: String,
    pub enabled_tools: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodexManagedMcpConfigError {
    InvalidHash { field: &'static str },
    InvalidOverlayPolicyVersion,
    TooManyTools { actual: usize, maximum: usize },
    InvalidCallableName { name: String },
    DuplicateCallableName { name: String },
    MissingManagedPath { field: &'static str },
    UnexpectedManagedPath { field: &'static str },
    InvalidManagedPath { field: &'static str },
    Serialization(String),
    ConfigTooLarge { actual: usize, maximum: usize },
    InvalidSerializedConfig,
}

impl fmt::Display for CodexManagedMcpConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidHash { field } => write!(formatter, "invalid Codex MCP {field}"),
            Self::InvalidOverlayPolicyVersion => {
                formatter.write_str("Codex MCP overlay policy version must be greater than zero")
            }
            Self::TooManyTools { actual, maximum } => write!(
                formatter,
                "Codex MCP config contains {actual} tools, maximum is {maximum}"
            ),
            Self::InvalidCallableName { name } => {
                write!(formatter, "invalid Codex MCP callable name `{name}`")
            }
            Self::DuplicateCallableName { name } => {
                write!(formatter, "duplicate Codex MCP callable name `{name}`")
            }
            Self::MissingManagedPath { field } => {
                write!(formatter, "non-empty Codex MCP config requires {field}")
            }
            Self::UnexpectedManagedPath { field } => {
                write!(formatter, "empty Codex MCP config must not retain {field}")
            }
            Self::InvalidManagedPath { field } => {
                write!(formatter, "invalid absolute Codex MCP {field}")
            }
            Self::Serialization(message) => {
                write!(formatter, "failed to serialize Codex MCP config: {message}")
            }
            Self::ConfigTooLarge { actual, maximum } => write!(
                formatter,
                "Codex MCP config is {actual} bytes, maximum is {maximum}"
            ),
            Self::InvalidSerializedConfig => {
                formatter.write_str("serialized Codex MCP config failed structural validation")
            }
        }
    }
}

impl Error for CodexManagedMcpConfigError {}

#[derive(Serialize)]
struct CodexManagedConfigDocument {
    personality: &'static str,
    features: BTreeMap<&'static str, bool>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    mcp_servers: BTreeMap<&'static str, CodexManagedStdioServer>,
}

#[derive(Serialize)]
struct CodexManagedStdioServer {
    command: String,
    args: Vec<String>,
    required: bool,
    enabled_tools: Vec<String>,
    tools: BTreeMap<String, CodexManagedToolPolicy>,
}

#[derive(Serialize)]
struct CodexManagedToolPolicy {
    approval_mode: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CodexSemanticRestartIdentity<'a> {
    config_contract_version: u32,
    synthetic_server_name: &'static str,
    required: bool,
    isolation_features_disabled: &'static [&'static str],
    overlay_policy_version: u32,
    canonical_manifest_hash: &'a str,
    provider_manifest_hash: &'a str,
    provider_contract_fingerprint: &'a str,
    tools: Vec<CodexSemanticToolIdentity<'a>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CodexSemanticToolIdentity<'a> {
    canonical_callable_name: &'a str,
    canonical_schema_fingerprint: &'a str,
    transformed_schema_fingerprint: &'a str,
    transform_contract_fingerprint: &'a str,
    transformed_fingerprint: &'a str,
}

/// Serialize the exact, Pioneer-owned Codex configuration. This function is
/// pure and creates no files or processes, so every constraint is checked
/// before WP 03 stages the artifact in a generation overlay.
pub fn serialize_codex_managed_mcp_config(
    mut input: CodexManagedMcpConfigInput,
) -> Result<CodexManagedMcpConfigArtifact, CodexManagedMcpConfigError> {
    validate_semantic_input(&mut input.semantic)?;

    let non_empty = !input.semantic.tools.is_empty();
    let helper_path =
        validate_projection_path(input.helper_path.as_deref(), "helper path", non_empty)?;
    let bootstrap_path =
        validate_projection_path(input.bootstrap_path.as_deref(), "bootstrap path", non_empty)?;

    let enabled_tools = input
        .semantic
        .tools
        .iter()
        .map(|tool| tool.canonical_callable_name.clone())
        .collect::<Vec<_>>();
    let semantic_restart_fingerprint =
        semantic_restart_fingerprint_from_validated(&input.semantic)?;

    let mut mcp_servers = BTreeMap::new();
    if non_empty {
        let tools = enabled_tools
            .iter()
            .map(|name| {
                (
                    name.clone(),
                    CodexManagedToolPolicy {
                        approval_mode: APPROVAL_MODE_APPROVE,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        mcp_servers.insert(
            SYNTHETIC_SERVER_NAME,
            CodexManagedStdioServer {
                command: helper_path.expect("validated non-empty helper path"),
                args: vec![
                    HELPER_SUBCOMMAND.to_owned(),
                    HELPER_BOOTSTRAP_OPTION.to_owned(),
                    bootstrap_path.expect("validated non-empty bootstrap path"),
                ],
                required: true,
                enabled_tools: enabled_tools.clone(),
                tools,
            },
        );
    }
    let document = CodexManagedConfigDocument {
        // Codex 0.144.1 otherwise migrates a fresh generation config in place
        // before thread/resume. Pinning its own default keeps the exact staged
        // artifact immutable without changing the effective model policy.
        personality: CODEX_DEFAULT_PERSONALITY,
        features: ISOLATION_FEATURE_NAMES
            .iter()
            .copied()
            .map(|name| (name, false))
            .collect(),
        mcp_servers,
    };
    let config_toml = toml::to_string(&document)
        .map_err(|error| CodexManagedMcpConfigError::Serialization(error.to_string()))?;
    if config_toml.len() > MAX_CONFIG_BYTES {
        return Err(CodexManagedMcpConfigError::ConfigTooLarge {
            actual: config_toml.len(),
            maximum: MAX_CONFIG_BYTES,
        });
    }
    validate_serialized_config(config_toml.as_str(), enabled_tools.as_slice())?;
    let staged_mcp_servers = serde_json::to_value(&document.mcp_servers)
        .map_err(|error| CodexManagedMcpConfigError::Serialization(error.to_string()))?;
    let staged_mcp_servers_fingerprint = codex_config_value_fingerprint(&staged_mcp_servers)?;
    let effective_mcp_servers_fingerprint =
        codex_config_value_fingerprint(&normalized_codex_mcp_servers(staged_mcp_servers)?)?;

    Ok(CodexManagedMcpConfigArtifact {
        artifact_digest: sha256_hex(config_toml.as_bytes()),
        staged_mcp_servers_fingerprint,
        effective_mcp_servers_fingerprint,
        semantic_restart_fingerprint,
        config_toml,
        enabled_tools,
    })
}

/// Model the bounded MCP portion returned by Codex `config/read`. Codex adds
/// stable runtime defaults to a configured stdio server even when the source
/// layer omits them. Staged-layer and effective fingerprints therefore have
/// separate domains and must never be compared as if they were identical.
fn normalized_codex_mcp_servers(
    mut staged: JsonValue,
) -> Result<JsonValue, CodexManagedMcpConfigError> {
    let Some(servers) = staged.as_object_mut() else {
        return Err(CodexManagedMcpConfigError::InvalidSerializedConfig);
    };
    if servers.is_empty() {
        return Ok(staged);
    }
    let pioneer = servers
        .get_mut(SYNTHETIC_SERVER_NAME)
        .and_then(JsonValue::as_object_mut)
        .ok_or(CodexManagedMcpConfigError::InvalidSerializedConfig)?;
    pioneer.insert("enabled".to_owned(), JsonValue::Bool(true));
    pioneer.insert(
        "environment_id".to_owned(),
        JsonValue::String("local".to_owned()),
    );
    pioneer.insert("tool_timeout_sec".to_owned(), JsonValue::Null);
    Ok(staged)
}

/// Stable semantic fingerprint for bounded effective-config evidence. Object
/// keys are sorted recursively so provider JSON serialization order is not a
/// correctness input.
pub fn codex_config_value_fingerprint(
    value: &JsonValue,
) -> Result<String, CodexManagedMcpConfigError> {
    let encoded = serde_json::to_vec(&canonical_json(value)).map_err(|error| {
        CodexManagedMcpConfigError::Serialization(format!(
            "effective Codex config fingerprint failed: {error}"
        ))
    })?;
    Ok(sha256_hex(encoded.as_slice()))
}

fn canonical_json(value: &JsonValue) -> JsonValue {
    match value {
        JsonValue::Object(object) => {
            let mut keys = object.keys().collect::<Vec<_>>();
            keys.sort();
            let mut canonical = JsonMap::new();
            for key in keys {
                canonical.insert(key.clone(), canonical_json(&object[key]));
            }
            JsonValue::Object(canonical)
        }
        JsonValue::Array(values) => {
            JsonValue::Array(values.iter().map(canonical_json).collect::<Vec<_>>())
        }
        _ => value.clone(),
    }
}

/// Compute session-reuse identity before any generation-local helper or
/// bootstrap path exists. The input is normalized exactly as config
/// serialization normalizes it.
pub fn codex_managed_mcp_semantic_restart_fingerprint(
    mut input: CodexManagedMcpSemanticInput,
) -> Result<String, CodexManagedMcpConfigError> {
    validate_semantic_input(&mut input)?;
    semantic_restart_fingerprint_from_validated(&input)
}

fn validate_semantic_input(
    input: &mut CodexManagedMcpSemanticInput,
) -> Result<(), CodexManagedMcpConfigError> {
    validate_hash(
        "canonical manifest hash",
        input.canonical_manifest_hash.as_str(),
    )?;
    validate_hash(
        "provider manifest hash",
        input.provider_manifest_hash.as_str(),
    )?;
    validate_hash(
        "provider contract fingerprint",
        input.provider_contract_fingerprint.as_str(),
    )?;
    if input.overlay_policy_version == 0 {
        return Err(CodexManagedMcpConfigError::InvalidOverlayPolicyVersion);
    }
    if input.tools.len() > MAX_TOOLS {
        return Err(CodexManagedMcpConfigError::TooManyTools {
            actual: input.tools.len(),
            maximum: MAX_TOOLS,
        });
    }

    input.tools.sort_by(|left, right| {
        left.canonical_callable_name
            .cmp(&right.canonical_callable_name)
    });
    let mut names = HashSet::with_capacity(input.tools.len());
    for tool in &input.tools {
        validate_callable_name(tool.canonical_callable_name.as_str())?;
        if !names.insert(tool.canonical_callable_name.as_str()) {
            return Err(CodexManagedMcpConfigError::DuplicateCallableName {
                name: tool.canonical_callable_name.clone(),
            });
        }
        for (field, value) in [
            (
                "canonical schema fingerprint",
                tool.canonical_schema_fingerprint.as_str(),
            ),
            (
                "transformed schema fingerprint",
                tool.transformed_schema_fingerprint.as_str(),
            ),
            (
                "transform contract fingerprint",
                tool.transform_contract_fingerprint.as_str(),
            ),
            (
                "transformed fingerprint",
                tool.transformed_fingerprint.as_str(),
            ),
        ] {
            validate_hash(field, value)?;
        }
    }

    Ok(())
}

fn semantic_restart_fingerprint_from_validated(
    input: &CodexManagedMcpSemanticInput,
) -> Result<String, CodexManagedMcpConfigError> {
    let semantic_tools = input
        .tools
        .iter()
        .map(|tool| CodexSemanticToolIdentity {
            canonical_callable_name: tool.canonical_callable_name.as_str(),
            canonical_schema_fingerprint: tool.canonical_schema_fingerprint.as_str(),
            transformed_schema_fingerprint: tool.transformed_schema_fingerprint.as_str(),
            transform_contract_fingerprint: tool.transform_contract_fingerprint.as_str(),
            transformed_fingerprint: tool.transformed_fingerprint.as_str(),
        })
        .collect();
    let semantic_identity = CodexSemanticRestartIdentity {
        config_contract_version: CONFIG_CONTRACT_VERSION,
        synthetic_server_name: SYNTHETIC_SERVER_NAME,
        required: !input.tools.is_empty(),
        isolation_features_disabled: ISOLATION_FEATURE_NAMES,
        overlay_policy_version: input.overlay_policy_version,
        canonical_manifest_hash: input.canonical_manifest_hash.as_str(),
        provider_manifest_hash: input.provider_manifest_hash.as_str(),
        provider_contract_fingerprint: input.provider_contract_fingerprint.as_str(),
        tools: semantic_tools,
    };
    let semantic_bytes = serde_json::to_vec(&semantic_identity)
        .map_err(|error| CodexManagedMcpConfigError::Serialization(error.to_string()))?;

    Ok(sha256_hex(semantic_bytes.as_slice()))
}

fn validate_hash(field: &'static str, value: &str) -> Result<(), CodexManagedMcpConfigError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(CodexManagedMcpConfigError::InvalidHash { field });
    }
    Ok(())
}

fn validate_callable_name(name: &str) -> Result<(), CodexManagedMcpConfigError> {
    if name.is_empty()
        || name.len() > MAX_CALLABLE_NAME_BYTES
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Err(CodexManagedMcpConfigError::InvalidCallableName {
            name: name.to_owned(),
        });
    }
    Ok(())
}

fn validate_projection_path(
    path: Option<&Path>,
    field: &'static str,
    required: bool,
) -> Result<Option<String>, CodexManagedMcpConfigError> {
    let Some(path) = path else {
        return if required {
            Err(CodexManagedMcpConfigError::MissingManagedPath { field })
        } else {
            Ok(None)
        };
    };
    if !required {
        return Err(CodexManagedMcpConfigError::UnexpectedManagedPath { field });
    }
    let value = path
        .to_str()
        .filter(|value| {
            !value.is_empty()
                && value.len() <= MAX_MANAGED_PATH_BYTES
                && !value.chars().any(char::is_control)
        })
        .ok_or(CodexManagedMcpConfigError::InvalidManagedPath { field })?;
    if !path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        return Err(CodexManagedMcpConfigError::InvalidManagedPath { field });
    }
    Ok(Some(value.to_owned()))
}

fn validate_serialized_config(
    config_toml: &str,
    enabled_tools: &[String],
) -> Result<(), CodexManagedMcpConfigError> {
    let value: toml::Value = toml::from_str(config_toml)
        .map_err(|_| CodexManagedMcpConfigError::InvalidSerializedConfig)?;
    let root = value
        .as_table()
        .ok_or(CodexManagedMcpConfigError::InvalidSerializedConfig)?;
    if root
        .keys()
        .any(|key| key != "personality" && key != "features" && key != "mcp_servers")
        || root.get("personality").and_then(toml::Value::as_str) != Some(CODEX_DEFAULT_PERSONALITY)
    {
        return Err(CodexManagedMcpConfigError::InvalidSerializedConfig);
    }
    let features = root
        .get("features")
        .and_then(toml::Value::as_table)
        .ok_or(CodexManagedMcpConfigError::InvalidSerializedConfig)?;
    if features.len() != ISOLATION_FEATURE_NAMES.len()
        || ISOLATION_FEATURE_NAMES
            .iter()
            .any(|name| features.get(*name).and_then(toml::Value::as_bool) != Some(false))
    {
        return Err(CodexManagedMcpConfigError::InvalidSerializedConfig);
    }

    let Some(servers) = root.get("mcp_servers") else {
        return if enabled_tools.is_empty() {
            Ok(())
        } else {
            Err(CodexManagedMcpConfigError::InvalidSerializedConfig)
        };
    };
    let servers = servers
        .as_table()
        .ok_or(CodexManagedMcpConfigError::InvalidSerializedConfig)?;
    if enabled_tools.is_empty()
        || servers.len() != 1
        || !servers.contains_key(SYNTHETIC_SERVER_NAME)
    {
        return Err(CodexManagedMcpConfigError::InvalidSerializedConfig);
    }
    let pioneer = servers[SYNTHETIC_SERVER_NAME]
        .as_table()
        .ok_or(CodexManagedMcpConfigError::InvalidSerializedConfig)?;
    let allowed_server_keys = ["command", "args", "required", "enabled_tools", "tools"];
    if pioneer
        .keys()
        .any(|key| !allowed_server_keys.contains(&key.as_str()))
        || pioneer.get("required").and_then(toml::Value::as_bool) != Some(true)
    {
        return Err(CodexManagedMcpConfigError::InvalidSerializedConfig);
    }
    let serialized_enabled = pioneer
        .get("enabled_tools")
        .and_then(toml::Value::as_array)
        .ok_or(CodexManagedMcpConfigError::InvalidSerializedConfig)?
        .iter()
        .map(toml::Value::as_str)
        .collect::<Option<Vec<_>>>()
        .ok_or(CodexManagedMcpConfigError::InvalidSerializedConfig)?;
    if serialized_enabled != enabled_tools.iter().map(String::as_str).collect::<Vec<_>>() {
        return Err(CodexManagedMcpConfigError::InvalidSerializedConfig);
    }
    let policies = pioneer
        .get("tools")
        .and_then(toml::Value::as_table)
        .ok_or(CodexManagedMcpConfigError::InvalidSerializedConfig)?;
    if policies.len() != enabled_tools.len()
        || enabled_tools.iter().any(|name| {
            policies
                .get(name)
                .and_then(toml::Value::as_table)
                .filter(|policy| policy.len() == 1)
                .and_then(|policy| policy.get("approval_mode"))
                .and_then(toml::Value::as_str)
                != Some(APPROVAL_MODE_APPROVE)
        })
    {
        return Err(CodexManagedMcpConfigError::InvalidSerializedConfig);
    }
    Ok(())
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write;
        write!(&mut output, "{byte:02x}").expect("writing to a String cannot fail");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash(byte: char) -> String {
        std::iter::repeat_n(byte, 64).collect()
    }

    fn fingerprint(seed: usize) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        hash(HEX[seed % HEX.len()] as char)
    }

    fn tool(name: &str, seed: usize) -> CodexManagedMcpToolIdentity {
        CodexManagedMcpToolIdentity {
            canonical_callable_name: name.to_owned(),
            canonical_schema_fingerprint: fingerprint(seed),
            transformed_schema_fingerprint: fingerprint(seed + 1),
            transform_contract_fingerprint: fingerprint(seed + 2),
            transformed_fingerprint: fingerprint(seed + 3),
        }
    }

    fn input(tools: Vec<CodexManagedMcpToolIdentity>) -> CodexManagedMcpConfigInput {
        let non_empty = !tools.is_empty();
        CodexManagedMcpConfigInput {
            semantic: CodexManagedMcpSemanticInput {
                canonical_manifest_hash: hash('a'),
                provider_manifest_hash: hash('b'),
                provider_contract_fingerprint: hash('c'),
                overlay_policy_version: 1,
                tools,
            },
            helper_path: non_empty.then(|| PathBuf::from("/opt/pioneer/pioneer")),
            bootstrap_path: non_empty.then(|| PathBuf::from("/private/pioneer/session/bootstrap")),
        }
    }

    #[test]
    fn codex_mcp_config_empty_golden_has_no_server_entry() {
        let artifact = serialize_codex_managed_mcp_config(input(Vec::new())).expect("empty config");
        assert_eq!(
            artifact.config_toml,
            include_str!("../tests/fixtures/codex_mcp_config_empty.toml")
        );
        assert!(artifact.enabled_tools.is_empty());
        assert!(!artifact.config_toml.contains("mcp_servers"));
    }

    #[test]
    fn codex_mcp_config_exact_pioneer_golden_is_required_and_per_tool_approved() {
        let artifact = serialize_codex_managed_mcp_config(input(vec![
            tool("mcp_beta", 5),
            tool("mcp_alpha", 1),
        ]))
        .expect("managed config");
        assert_eq!(
            artifact.config_toml,
            include_str!("../tests/fixtures/codex_mcp_config_exact.toml")
        );
        assert_eq!(artifact.enabled_tools, ["mcp_alpha", "mcp_beta"]);
        for forbidden in [
            "default_tools_approval_mode",
            "disabled_tools",
            "wildcard",
            "bypass",
            "bearer_token",
            "http_headers",
            "env =",
            "url =",
        ] {
            assert!(!artifact.config_toml.contains(forbidden));
        }
    }

    #[test]
    fn codex_mcp_config_artifact_and_semantic_fingerprints_have_separate_domains() {
        let first = serialize_codex_managed_mcp_config(input(vec![tool("mcp_alpha", 1)]))
            .expect("first config");
        let mut moved_input = input(vec![tool("mcp_alpha", 1)]);
        moved_input.bootstrap_path = Some(PathBuf::from("/private/pioneer/next/bootstrap"));
        let moved = serialize_codex_managed_mcp_config(moved_input).expect("moved config");
        assert_ne!(first.config_toml, moved.config_toml);
        assert_ne!(first.artifact_digest, moved.artifact_digest);
        assert_eq!(
            first.semantic_restart_fingerprint,
            moved.semantic_restart_fingerprint
        );

        let repeated = serialize_codex_managed_mcp_config(input(vec![tool("mcp_alpha", 1)]))
            .expect("repeated config");
        assert_eq!(first, repeated);
    }

    #[test]
    fn codex_mcp_config_rejects_stale_empty_paths_and_invalid_names_before_artifact() {
        let mut stale = input(Vec::new());
        stale.helper_path = Some(PathBuf::from("/stale/pioneer"));
        assert!(matches!(
            serialize_codex_managed_mcp_config(stale),
            Err(CodexManagedMcpConfigError::UnexpectedManagedPath { .. })
        ));

        let invalid = input(vec![tool("*", 1)]);
        assert!(matches!(
            serialize_codex_managed_mcp_config(invalid),
            Err(CodexManagedMcpConfigError::InvalidCallableName { .. })
        ));
    }
}
