use crate::{
    ArtifactRef, MarkdownDocument, McpScopeKind, SandboxPolicy, SkillId, SkillPackId, TaskTurnItem,
    ThreadMode,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ByteRange {
    pub start: usize,
    pub end: usize,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
#[serde(transparent)]
pub struct ToolMetadata {
    fields: BTreeMap<String, ToolMetadataValue>,
}

impl ToolMetadata {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn from_json(value: JsonValue) -> Self {
        match value {
            JsonValue::Object(map) => Self {
                fields: map
                    .into_iter()
                    .map(|(key, value)| {
                        let metadata_value =
                            ToolMetadataValue::from_json_with_key(value, Some(key.as_str()));
                        (key, metadata_value)
                    })
                    .collect(),
            },
            JsonValue::Null => Self::default(),
            value => {
                let mut fields = BTreeMap::new();
                fields.insert(
                    "value".to_owned(),
                    ToolMetadataValue::from_json_with_key(value, Some("value")),
                );
                Self { fields }
            }
        }
    }

    pub fn to_json(&self) -> JsonValue {
        JsonValue::Object(
            self.fields
                .iter()
                .map(|(key, value)| (key.clone(), value.to_json()))
                .collect(),
        )
    }

    pub fn get(&self, key: &str) -> Option<&ToolMetadataValue> {
        self.fields.get(key)
    }

    pub fn insert(&mut self, key: impl Into<String>, value: ToolMetadataValue) {
        self.fields.insert(key.into(), value);
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }
}

impl From<JsonValue> for ToolMetadata {
    fn from(value: JsonValue) -> Self {
        Self::from_json(value)
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum ToolMetadataValue {
    Null,
    Bool {
        value: bool,
    },
    Number {
        value: String,
    },
    String {
        value: String,
    },
    Array {
        values: Vec<ToolMetadataValue>,
    },
    Object {
        fields: BTreeMap<String, ToolMetadataValue>,
    },
    RedactedRaw {
        raw_kind: ToolMetadataRawKind,
        sha256: String,
        bytes: usize,
        value_kind: String,
    },
}

impl ToolMetadataValue {
    pub fn from_json(value: JsonValue) -> Self {
        Self::from_json_with_key(value, None)
    }

    fn from_json_with_key(value: JsonValue, key_hint: Option<&str>) -> Self {
        if key_hint.is_some_and(is_raw_like_metadata_key) && !value.is_null() {
            let serialized = serde_json::to_vec(&value).unwrap_or_default();
            return Self::RedactedRaw {
                raw_kind: ToolMetadataRawKind::from_key_hint(key_hint.unwrap_or_default()),
                sha256: sha256_hex(serialized.as_slice()),
                bytes: serialized.len(),
                value_kind: json_value_kind(&value).to_owned(),
            };
        }

        match value {
            JsonValue::Null => Self::Null,
            JsonValue::Bool(value) => Self::Bool { value },
            JsonValue::Number(value) => Self::Number {
                value: value.to_string(),
            },
            JsonValue::String(value) => Self::String { value },
            JsonValue::Array(values) => Self::Array {
                values: values.into_iter().map(Self::from_json).collect(),
            },
            JsonValue::Object(map) => Self::Object {
                fields: map
                    .into_iter()
                    .map(|(key, value)| {
                        let metadata_value = Self::from_json_with_key(value, Some(key.as_str()));
                        (key, metadata_value)
                    })
                    .collect(),
            },
        }
    }

    pub fn to_json(&self) -> JsonValue {
        match self {
            Self::Null => JsonValue::Null,
            Self::Bool { value } => JsonValue::Bool(*value),
            Self::Number { value } => metadata_number_to_json(value),
            Self::String { value } => JsonValue::String(value.clone()),
            Self::Array { values } => JsonValue::Array(
                values
                    .iter()
                    .map(ToolMetadataValue::to_json)
                    .collect::<Vec<_>>(),
            ),
            Self::Object { fields } => JsonValue::Object(
                fields
                    .iter()
                    .map(|(key, value)| (key.clone(), value.to_json()))
                    .collect(),
            ),
            Self::RedactedRaw {
                raw_kind,
                sha256,
                bytes,
                value_kind,
            } => serde_json::json!({
                "kind": "redacted_raw",
                "rawKind": raw_kind,
                "sha256": sha256,
                "bytes": bytes,
                "valueKind": value_kind,
            }),
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String { value } => Some(value.as_str()),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool { value } => Some(*value),
            _ => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Self::Number { value } => value.parse::<u64>().ok(),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Number { value } => value.parse::<i64>().ok(),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&[ToolMetadataValue]> {
        match self {
            Self::Array { values } => Some(values.as_slice()),
            _ => None,
        }
    }

    pub fn as_object(&self) -> Option<&BTreeMap<String, ToolMetadataValue>> {
        match self {
            Self::Object { fields } => Some(fields),
            _ => None,
        }
    }
}

fn metadata_number_to_json(value: &str) -> JsonValue {
    if let Ok(value) = value.parse::<u64>() {
        return JsonValue::Number(value.into());
    }
    if let Ok(value) = value.parse::<i64>() {
        return JsonValue::Number(value.into());
    }
    serde_json::Number::from_f64(value.parse::<f64>().unwrap_or_default())
        .map(JsonValue::Number)
        .unwrap_or(JsonValue::Null)
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolMetadataRawKind {
    Content,
    Body,
    Blob,
    Base64,
    Bytes,
    Data,
    Html,
    Image,
    Output,
    Screenshot,
    Stdout,
    Stderr,
    Text,
    Unknown,
}

impl ToolMetadataRawKind {
    fn from_key_hint(key: &str) -> Self {
        match normalize_metadata_key(key).as_str() {
            "content" => Self::Content,
            "body" => Self::Body,
            "blob" => Self::Blob,
            "base64" => Self::Base64,
            "bytes" => Self::Bytes,
            "data" | "dataurl" | "data_url" => Self::Data,
            "html" => Self::Html,
            "image" => Self::Image,
            "output" | "outputjson" | "output_json" => Self::Output,
            "screenshot" => Self::Screenshot,
            "stdout" => Self::Stdout,
            "stderr" => Self::Stderr,
            "text" => Self::Text,
            _ => Self::Unknown,
        }
    }
}

fn is_raw_like_metadata_key(key: &str) -> bool {
    matches!(
        normalize_metadata_key(key).as_str(),
        "content"
            | "body"
            | "blob"
            | "base64"
            | "bytes"
            | "data"
            | "dataurl"
            | "data_url"
            | "html"
            | "image"
            | "output"
            | "outputjson"
            | "output_json"
            | "screenshot"
            | "stdout"
            | "stderr"
            | "text"
    )
}

fn normalize_metadata_key(key: &str) -> String {
    key.chars()
        .filter(|ch| ch.is_ascii_alphanumeric() || *ch == '_')
        .collect::<String>()
        .to_ascii_lowercase()
}

fn json_value_kind(value: &JsonValue) -> &'static str {
    match value {
        JsonValue::Null => "null",
        JsonValue::Bool(_) => "bool",
        JsonValue::Number(_) => "number",
        JsonValue::String(_) => "string",
        JsonValue::Array(_) => "array",
        JsonValue::Object(_) => "object",
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
pub struct TurnStartParams {
    pub thread_id: String,
    pub turn_id: String,
    #[serde(default)]
    pub input: Vec<UserInput>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<TurnCapability>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sandbox_policy: Option<SandboxPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<ThreadMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_backend: Option<AgentExecutionBackend>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<TurnReasoningSelection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permission_profile: Option<TurnPermissionProfileSelection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cli_runtime_options: Option<TurnCLIRuntimeOptions>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TurnPermissionMode {
    #[default]
    FullAccess,
    AutoAcceptEdits,
    Supervised,
}

impl TurnPermissionMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FullAccess => "full_access",
            Self::AutoAcceptEdits => "auto_accept_edits",
            Self::Supervised => "supervised",
        }
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnPermissionProfileSelection {
    pub mode: TurnPermissionMode,
}

impl TurnPermissionProfileSelection {
    pub fn full_access() -> Self {
        Self {
            mode: TurnPermissionMode::FullAccess,
        }
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnPermissionProfileSnapshot {
    pub mode: TurnPermissionMode,
    pub source: TurnPermissionProfileSource,
    pub effective_policy: ToolPermissionPolicySnapshot,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnPermissionProfileCap {
    pub mode: TurnPermissionMode,
    pub effective_policy: ToolPermissionPolicySnapshot,
}

pub const TURN_EXECUTION_SECURITY_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnExecutionSecuritySnapshot {
    pub schema_version: u32,
    pub version: u32,
    pub source: TurnSecuritySnapshotSource,
    pub permission_profile: TurnPermissionProfileSnapshot,
    pub sandbox: TurnSandboxSnapshot,
    pub process: TurnProcessPolicySnapshot,
    pub network: TurnNetworkPolicySnapshot,
    pub approval: TurnApprovalScopePolicySnapshot,
    pub backend: TurnSecurityBackendSnapshot,
    pub enforcement: TurnSecurityEnforcementStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_cap: Option<TurnSecurityParentCapSnapshot>,
    pub created_at_unix_ms: i64,
}

impl TurnExecutionSecuritySnapshot {
    pub fn audit_id(&self, turn_id: &str) -> String {
        format!("{turn_id}:security:v{}", self.version)
    }

    pub fn unrestricted_full_access(cwd: impl Into<String>, created_at_unix_ms: i64) -> Self {
        let permission_profile = TurnPermissionProfileSnapshot::from_mode(
            TurnPermissionMode::FullAccess,
            TurnPermissionProfileSource::Composer,
        );
        let network = TurnNetworkPolicySnapshot::enabled();
        Self {
            schema_version: TURN_EXECUTION_SECURITY_SNAPSHOT_SCHEMA_VERSION,
            version: 1,
            source: TurnSecuritySnapshotSource::ComposerSelection,
            sandbox: TurnSandboxSnapshot {
                mode: TurnSandboxMode::Unrestricted,
                cwd: cwd.into(),
                filesystem: TurnFilesystemSandboxPolicy::unrestricted(),
                tmp: TurnTmpPolicy::unrestricted(),
                network: network.clone(),
                backend_requirement: SandboxBackendRequirement::Optional,
                backend_preference: Vec::new(),
            },
            process: TurnProcessPolicySnapshot::unrestricted(),
            approval: TurnApprovalScopePolicySnapshot::full_access(),
            backend: TurnSecurityBackendSnapshot::native_unrestricted(),
            enforcement: TurnSecurityEnforcementStatus::Active,
            parent_cap: None,
            created_at_unix_ms,
            permission_profile,
            network,
        }
    }

    pub fn read_only(
        permission_profile: TurnPermissionProfileSnapshot,
        cwd: impl Into<String>,
        read_roots: Vec<TurnFilesystemSandboxEntry>,
        created_at_unix_ms: i64,
    ) -> Self {
        let network = TurnNetworkPolicySnapshot::disabled();
        Self {
            schema_version: TURN_EXECUTION_SECURITY_SNAPSHOT_SCHEMA_VERSION,
            version: 1,
            source: TurnSecuritySnapshotSource::ComposerSelection,
            sandbox: TurnSandboxSnapshot {
                mode: TurnSandboxMode::ReadOnly,
                cwd: cwd.into(),
                filesystem: TurnFilesystemSandboxPolicy {
                    kind: TurnFilesystemSandboxKind::Restricted,
                    entries: read_roots,
                },
                tmp: TurnTmpPolicy::isolated(),
                network: network.clone(),
                backend_requirement: SandboxBackendRequirement::Required,
                backend_preference: vec![SandboxBackendKind::Nono],
            },
            process: TurnProcessPolicySnapshot::restricted(),
            approval: TurnApprovalScopePolicySnapshot::supervised(),
            backend: TurnSecurityBackendSnapshot::native_required(SandboxBackendKind::Nono),
            enforcement: TurnSecurityEnforcementStatus::Active,
            parent_cap: None,
            created_at_unix_ms,
            permission_profile,
            network,
        }
    }

    pub fn workspace_write(
        permission_profile: TurnPermissionProfileSnapshot,
        cwd: impl Into<String>,
        read_write_roots: Vec<TurnFilesystemSandboxEntry>,
        created_at_unix_ms: i64,
    ) -> Self {
        let network = TurnNetworkPolicySnapshot::disabled();
        Self {
            schema_version: TURN_EXECUTION_SECURITY_SNAPSHOT_SCHEMA_VERSION,
            version: 1,
            source: TurnSecuritySnapshotSource::ComposerSelection,
            sandbox: TurnSandboxSnapshot {
                mode: TurnSandboxMode::WorkspaceWrite,
                cwd: cwd.into(),
                filesystem: TurnFilesystemSandboxPolicy {
                    kind: TurnFilesystemSandboxKind::Restricted,
                    entries: read_write_roots,
                },
                tmp: TurnTmpPolicy::isolated(),
                network: network.clone(),
                backend_requirement: SandboxBackendRequirement::Required,
                backend_preference: vec![SandboxBackendKind::Nono],
            },
            process: TurnProcessPolicySnapshot::restricted(),
            approval: TurnApprovalScopePolicySnapshot::auto_accept_edits(),
            backend: TurnSecurityBackendSnapshot::native_required(SandboxBackendKind::Nono),
            enforcement: TurnSecurityEnforcementStatus::Active,
            parent_cap: None,
            created_at_unix_ms,
            permission_profile,
            network,
        }
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TurnSecuritySnapshotSource {
    ComposerSelection,
    GatewayDefault,
    TaskInherited,
    ReviewerInherited,
    RevisionInherited,
    RuntimeRecovery,
    BackfilledLegacy,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnSandboxSnapshot {
    pub mode: TurnSandboxMode,
    pub cwd: String,
    pub filesystem: TurnFilesystemSandboxPolicy,
    pub tmp: TurnTmpPolicy,
    pub network: TurnNetworkPolicySnapshot,
    pub backend_requirement: SandboxBackendRequirement,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub backend_preference: Vec<SandboxBackendKind>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TurnSandboxMode {
    Unrestricted,
    ReadOnly,
    WorkspaceWrite,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnFilesystemSandboxPolicy {
    pub kind: TurnFilesystemSandboxKind,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub entries: Vec<TurnFilesystemSandboxEntry>,
}

impl TurnFilesystemSandboxPolicy {
    pub fn unrestricted() -> Self {
        Self {
            kind: TurnFilesystemSandboxKind::Unrestricted,
            entries: Vec::new(),
        }
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TurnFilesystemSandboxKind {
    Restricted,
    Unrestricted,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnFilesystemSandboxEntry {
    pub path: TurnFilesystemSandboxPath,
    pub access: TurnFilesystemAccess,
    pub provenance: TurnSecurityRuleProvenance,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_path: Option<String>,
}

impl TurnFilesystemSandboxEntry {
    pub fn current_working_directory(
        access: TurnFilesystemAccess,
        resolved_path: impl Into<String>,
    ) -> Self {
        Self {
            path: TurnFilesystemSandboxPath::CurrentWorkingDirectory,
            access,
            provenance: TurnSecurityRuleProvenance::Runtime,
            resolved_path: Some(resolved_path.into()),
        }
    }

    pub fn workspace_root(access: TurnFilesystemAccess, resolved_path: impl Into<String>) -> Self {
        Self {
            path: TurnFilesystemSandboxPath::WorkspaceRoot,
            access,
            provenance: TurnSecurityRuleProvenance::Workspace,
            resolved_path: Some(resolved_path.into()),
        }
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TurnFilesystemAccess {
    None,
    Read,
    Write,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Hash)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TurnFilesystemSandboxPath {
    Root,
    CurrentWorkingDirectory,
    WorkspaceRoot,
    ProjectRoot {
        id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        subpath: Option<String>,
    },
    SlashTmp,
    Tmpdir,
    RuntimeHome,
    ExplicitPath {
        path: String,
    },
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TurnSecurityRuleProvenance {
    ComposerSelection,
    Workspace,
    Project,
    Runtime,
    TaskCap,
    System,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnTmpPolicy {
    pub mode: TurnTmpMode,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub writable_roots: Vec<String>,
}

impl TurnTmpPolicy {
    pub fn unrestricted() -> Self {
        Self {
            mode: TurnTmpMode::Host,
            writable_roots: Vec::new(),
        }
    }

    pub fn isolated() -> Self {
        Self {
            mode: TurnTmpMode::Isolated,
            writable_roots: Vec::new(),
        }
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TurnTmpMode {
    Host,
    Isolated,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnNetworkPolicySnapshot {
    pub mode: TurnNetworkMode,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_domains: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub denied_domains: Vec<String>,
    pub allow_localhost: bool,
    pub allow_unix_sockets: bool,
}

impl TurnNetworkPolicySnapshot {
    pub fn enabled() -> Self {
        Self {
            mode: TurnNetworkMode::Enabled,
            allowed_domains: Vec::new(),
            denied_domains: Vec::new(),
            allow_localhost: true,
            allow_unix_sockets: true,
        }
    }

    pub fn disabled() -> Self {
        Self {
            mode: TurnNetworkMode::Disabled,
            allowed_domains: Vec::new(),
            denied_domains: Vec::new(),
            allow_localhost: false,
            allow_unix_sockets: false,
        }
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TurnNetworkMode {
    Disabled,
    Restricted,
    Enabled,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnProcessPolicySnapshot {
    pub shell: TurnShellPolicy,
    pub environment: TurnEnvironmentPolicy,
    pub timeout: TurnProcessTimeoutPolicy,
    pub command_risk: TurnCommandRiskPolicy,
}

impl TurnProcessPolicySnapshot {
    pub fn unrestricted() -> Self {
        Self {
            shell: TurnShellPolicy::enabled(),
            environment: TurnEnvironmentPolicy::unrestricted(),
            timeout: TurnProcessTimeoutPolicy::default(),
            command_risk: TurnCommandRiskPolicy::default(),
        }
    }

    pub fn restricted() -> Self {
        Self {
            shell: TurnShellPolicy::enabled(),
            environment: TurnEnvironmentPolicy::restricted(),
            timeout: TurnProcessTimeoutPolicy::default(),
            command_risk: TurnCommandRiskPolicy::default(),
        }
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnShellPolicy {
    pub enabled: bool,
    pub allow_stdin: bool,
    pub allow_session_inheritance: bool,
}

impl TurnShellPolicy {
    pub fn enabled() -> Self {
        Self {
            enabled: true,
            allow_stdin: true,
            allow_session_inheritance: true,
        }
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnEnvironmentPolicy {
    pub inherit: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_vars: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub denied_patterns: Vec<String>,
}

impl TurnEnvironmentPolicy {
    pub fn unrestricted() -> Self {
        Self {
            inherit: true,
            allowed_vars: Vec::new(),
            denied_patterns: Vec::new(),
        }
    }

    pub fn restricted() -> Self {
        Self {
            inherit: true,
            allowed_vars: Vec::new(),
            denied_patterns: vec![
                ".*TOKEN.*".to_owned(),
                ".*SECRET.*".to_owned(),
                ".*PASSWORD.*".to_owned(),
            ],
        }
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnProcessTimeoutPolicy {
    pub max_duration_ms: u64,
}

impl Default for TurnProcessTimeoutPolicy {
    fn default() -> Self {
        Self {
            max_duration_ms: 30 * 60 * 1000,
        }
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
pub struct TurnCommandRiskPolicy {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub denied_commands: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_command_families: Vec<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnApprovalScopePolicySnapshot {
    pub allow_once: bool,
    pub allow_for_turn: bool,
    pub allow_for_session: bool,
    pub allow_always: bool,
    pub request_permissions: bool,
}

impl TurnApprovalScopePolicySnapshot {
    pub fn full_access() -> Self {
        Self {
            allow_once: false,
            allow_for_turn: false,
            allow_for_session: false,
            allow_always: false,
            request_permissions: false,
        }
    }

    pub fn auto_accept_edits() -> Self {
        Self {
            allow_once: true,
            allow_for_turn: true,
            allow_for_session: false,
            allow_always: false,
            request_permissions: true,
        }
    }

    pub fn supervised() -> Self {
        Self {
            allow_once: true,
            allow_for_turn: true,
            allow_for_session: false,
            allow_always: false,
            request_permissions: true,
        }
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnSecurityBackendSnapshot {
    pub execution_backend: TurnSecurityExecutionBackendKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sandbox_backend: Option<SandboxBackendKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    pub capabilities: BackendSecurityCapabilities,
}

impl TurnSecurityBackendSnapshot {
    pub fn native_unrestricted() -> Self {
        Self {
            execution_backend: TurnSecurityExecutionBackendKind::Native,
            sandbox_backend: None,
            provider: None,
            capabilities: BackendSecurityCapabilities::unrestricted(),
        }
    }

    pub fn native_required(sandbox_backend: SandboxBackendKind) -> Self {
        Self {
            execution_backend: TurnSecurityExecutionBackendKind::Native,
            sandbox_backend: Some(sandbox_backend),
            provider: None,
            capabilities: BackendSecurityCapabilities::native_sandboxed(),
        }
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TurnSecurityExecutionBackendKind {
    Native,
    CodexCli,
    ClaudeCli,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum SandboxBackendKind {
    Nono,
    WindowsRestrictedToken,
    ProviderNative,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum SandboxBackendRequirement {
    None,
    Optional,
    Required,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct BackendSecurityCapabilities {
    pub can_enforce_filesystem: bool,
    pub can_enforce_network: bool,
    pub can_enforce_process: bool,
    pub supports_turn_scope_approval: bool,
    pub supports_session_scope_approval: bool,
    pub supports_request_permissions: bool,
}

impl BackendSecurityCapabilities {
    pub fn unrestricted() -> Self {
        Self {
            can_enforce_filesystem: false,
            can_enforce_network: false,
            can_enforce_process: false,
            supports_turn_scope_approval: false,
            supports_session_scope_approval: false,
            supports_request_permissions: false,
        }
    }

    pub fn native_sandboxed() -> Self {
        Self {
            can_enforce_filesystem: true,
            can_enforce_network: true,
            can_enforce_process: true,
            supports_turn_scope_approval: true,
            supports_session_scope_approval: false,
            supports_request_permissions: true,
        }
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum TurnSecurityEnforcementStatus {
    Active,
    PartiallyActive {
        degraded: Vec<TurnSecurityDegradation>,
    },
    Unavailable {
        reason: String,
    },
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnSecurityDegradation {
    pub capability: TurnSecurityCapabilityKind,
    pub reason: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TurnSecurityCapabilityKind {
    Filesystem,
    Network,
    Process,
    Approval,
    SandboxBackend,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnSecurityParentCapSnapshot {
    pub parent_turn_id: String,
    pub max_permission_profile: TurnPermissionProfileCap,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub max_filesystem_entries: Vec<TurnFilesystemSandboxEntry>,
    pub max_network_policy: TurnNetworkPolicySnapshot,
    pub max_sandbox_mode: TurnSandboxMode,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnPermissionApprovalRequest {
    pub request_id: String,
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub visible_thread_ids: Vec<String>,
    pub tool_name: String,
    pub action: TurnPermissionActionKind,
    pub scope_hash: String,
    pub reason: TurnPermissionDecisionReason,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub details: Vec<TurnPermissionApprovalRequestDetail>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TurnPermissionApprovalRequestDetail {
    pub label: String,
    pub value: String,
    #[serde(default)]
    pub monospace: bool,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TurnPermissionActionKind {
    FileRead,
    FileWrite,
    ShellCommand,
    Network,
    McpRead,
    McpWriteOrUnknown,
    DynamicSkillTool,
    ComputerUse,
    TaskSubagent,
    Internal,
    Unknown,
}

impl TurnPermissionActionKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FileRead => "file_read",
            Self::FileWrite => "file_write",
            Self::ShellCommand => "shell_command",
            Self::Network => "network",
            Self::McpRead => "mcp_read",
            Self::McpWriteOrUnknown => "mcp_write_or_unknown",
            Self::DynamicSkillTool => "dynamic_skill_tool",
            Self::ComputerUse => "computer_use",
            Self::TaskSubagent => "task_subagent",
            Self::Internal => "internal",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TurnPermissionDecisionReason {
    FullAccess,
    PolicyAllowsAction,
    PolicyRequiresApproval,
    PolicyDeniesAction,
    CachedApproval,
    UserApproved,
    UserDenied,
    Cancelled,
    Expired,
    UnknownActionDefault,
    SandboxDenied,
}

impl TurnPermissionDecisionReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FullAccess => "full_access",
            Self::PolicyAllowsAction => "policy_allows_action",
            Self::PolicyRequiresApproval => "policy_requires_approval",
            Self::PolicyDeniesAction => "policy_denies_action",
            Self::CachedApproval => "cached_approval",
            Self::UserApproved => "user_approved",
            Self::UserDenied => "user_denied",
            Self::Cancelled => "cancelled",
            Self::Expired => "expired",
            Self::UnknownActionDefault => "unknown_action_default",
            Self::SandboxDenied => "sandbox_denied",
        }
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TurnPermissionApprovalResolution {
    AllowOnce,
    AllowForTurn,
    Deny,
    Cancelled,
    Expired,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnPermissionRequestOpenedNotification {
    pub request: TurnPermissionApprovalRequest,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnPermissionRequestResolvedNotification {
    pub request_id: String,
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub resolution: TurnPermissionApprovalResolution,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnPermissionRequestRespondParams {
    pub request_id: String,
    pub resolution: TurnPermissionApprovalResolution,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnPermissionRequestRespondResponse {
    pub request_id: String,
    pub resolution: TurnPermissionApprovalResolution,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TurnPermissionProfileSource {
    Composer,
    Defaulted,
    InheritedFromParentTurn,
    TaskPermissionCap,
    System,
}

impl TurnPermissionProfileSource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Composer => "composer",
            Self::Defaulted => "defaulted",
            Self::InheritedFromParentTurn => "inherited_from_parent_turn",
            Self::TaskPermissionCap => "task_permission_cap",
            Self::System => "system",
        }
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ToolPermissionPolicySnapshot {
    pub default_behavior: PermissionBehavior,
    pub file_read: PermissionBehavior,
    pub file_write: PermissionBehavior,
    pub shell_command: PermissionBehavior,
    pub network: PermissionBehavior,
    pub mcp_read: PermissionBehavior,
    pub mcp_write_or_unknown: PermissionBehavior,
    pub dynamic_skill_tool: PermissionBehavior,
    pub computer_use: PermissionBehavior,
    pub task_subagent: PermissionBehavior,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_tools: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub denied_tools: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_paths: Vec<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum PermissionBehavior {
    Allow,
    Ask,
    Deny,
}

impl PermissionBehavior {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::Ask => "ask",
            Self::Deny => "deny",
        }
    }
}

impl ToolPermissionPolicySnapshot {
    pub fn all(behavior: PermissionBehavior) -> Self {
        Self {
            default_behavior: behavior,
            file_read: behavior,
            file_write: behavior,
            shell_command: behavior,
            network: behavior,
            mcp_read: behavior,
            mcp_write_or_unknown: behavior,
            dynamic_skill_tool: behavior,
            computer_use: behavior,
            task_subagent: behavior,
            allowed_tools: Vec::new(),
            denied_tools: Vec::new(),
            allowed_paths: Vec::new(),
        }
    }

    pub fn for_mode(mode: TurnPermissionMode) -> Self {
        crate::turn_permissions::permission_policy_for_mode(mode)
    }
}

impl TurnPermissionProfileSnapshot {
    pub fn from_mode(mode: TurnPermissionMode, source: TurnPermissionProfileSource) -> Self {
        crate::turn_permissions::compile_turn_permission_profile(mode, source)
    }
}

pub fn resolve_turn_permission_profile(
    selection: Option<&TurnPermissionProfileSelection>,
) -> TurnPermissionProfileSnapshot {
    crate::turn_permissions::resolve_turn_permission_profile(selection)
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnReasoningSelection {
    /// String-valued because CLI runtimes may advertise efforts newer than
    /// Pioneer API-provider adapters understand.
    pub effort: String,
}

/// Closed effort set implemented by Pioneer API-provider adapters.
/// CLI runtime efforts remain metadata-defined strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReasoningEffort {
    Max,
    XHigh,
    High,
    Medium,
    Low,
    Minimal,
    None,
}

impl ReasoningEffort {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Max => "max",
            Self::XHigh => "xhigh",
            Self::High => "high",
            Self::Medium => "medium",
            Self::Low => "low",
            Self::Minimal => "minimal",
            Self::None => "none",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        let compact = value
            .trim()
            .chars()
            .filter(|ch| ch.is_ascii_alphanumeric())
            .flat_map(char::to_lowercase)
            .collect::<String>();

        match compact.as_str() {
            "max" | "maximum" => Some(Self::Max),
            "xhigh" | "extrahigh" | "xtrahigh" => Some(Self::XHigh),
            "high" => Some(Self::High),
            "medium" | "med" => Some(Self::Medium),
            "low" => Some(Self::Low),
            "minimal" | "min" => Some(Self::Minimal),
            "none" | "off" | "disabled" => Some(Self::None),
            _ => None,
        }
    }

    pub fn canonical_value(value: &str) -> Option<&'static str> {
        Self::from_str(value).map(Self::as_str)
    }
}

/// Canonicalize known aliases while preserving values declared by model
/// metadata so CLI runtimes remain forward compatible.
pub fn normalize_metadata_reasoning_effort(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }

    ReasoningEffort::canonical_value(trimmed)
        .map(str::to_owned)
        .or_else(|| Some(trimmed.to_owned()))
}

/// Build a stable key for matching a user selection to model metadata.
pub fn reasoning_effort_comparison_key(value: &str) -> Option<String> {
    normalize_metadata_reasoning_effort(value).map(|value| value.to_ascii_lowercase())
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "type")]
pub enum AgentExecutionBackend {
    #[serde(rename = "apiProvider")]
    ApiProvider { provider: String },
    #[serde(rename = "cliAgentRuntime")]
    CLIAgentRuntime {
        runtime_id: String,
        runtime_kind: CLIAgentRuntimeKind,
    },
    #[serde(rename = "acpAgentRuntime")]
    ACPAgentRuntime { runtime_id: String },
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CLIAgentRuntimeKind {
    Codex,
    Claude,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
pub struct TurnCLIRuntimeOptions {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sandbox: Option<CLIAgentRuntimeSandboxPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub personality: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub steer_if_active: Option<bool>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(transparent)]
pub struct CLIAgentRuntimeSandboxPolicy(pub JsonValue);

#[derive(Serialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TurnCapability {
    pub id: String,
    pub kind: TurnCapabilityKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TurnCapabilityWire {
    id: String,
    kind: TurnCapabilityKind,
    #[serde(default)]
    label: Option<String>,
}

impl<'de> Deserialize<'de> for TurnCapability {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = TurnCapabilityWire::deserialize(deserializer)?;
        let expected = match &wire.kind {
            TurnCapabilityKind::Skill { skill_id, .. } => Some(skill_capability_key(skill_id)),
            TurnCapabilityKind::SkillPack { pack_id } => Some(skill_pack_capability_key(pack_id)),
            TurnCapabilityKind::McpServer { .. } | TurnCapabilityKind::McpTool { .. } => None,
        };
        if let Some(expected) = expected
            && wire.id != expected
        {
            return Err(serde::de::Error::custom(format!(
                "capability id must be {expected}"
            )));
        }
        Ok(Self {
            id: wire.id,
            kind: wire.kind,
            label: wire.label,
        })
    }
}

/// Builds the canonical internal key for a selected skill capability.
pub fn skill_capability_key(skill_id: &SkillId) -> String {
    format!("skill:{skill_id}")
}

/// Builds the canonical internal key for a selected skill pack capability.
pub fn skill_pack_capability_key(pack_id: &SkillPackId) -> String {
    format!("skill_pack:{pack_id}")
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum TurnCapabilityKind {
    Skill {
        #[serde(rename = "skillId")]
        skill_id: SkillId,
        #[serde(rename = "packId", default, skip_serializing_if = "Option::is_none")]
        pack_id: Option<SkillPackId>,
    },
    SkillPack {
        #[serde(rename = "packId")]
        pack_id: SkillPackId,
    },
    McpServer {
        name: String,
        #[serde(rename = "scopeKind")]
        scope_kind: McpScopeKind,
    },
    McpTool {
        #[serde(rename = "serverName")]
        server_name: String,
        #[serde(rename = "rawToolName")]
        raw_tool_name: String,
        #[serde(rename = "scopeKind")]
        scope_kind: McpScopeKind,
    },
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnStartResponse {
    pub turn: Turn,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
pub struct TurnCancelParams {
    pub thread_id: String,
    pub turn_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnCancelResponse {
    pub thread_id: String,
    pub workspace_id: String,
    pub turn: Turn,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
pub struct TurnResumeParams {
    pub thread_id: String,
    pub turn_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_job_id: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnResumeResponse {
    pub thread_id: String,
    pub workspace_id: String,
    pub turn: Turn,
    pub recovery_job_id: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
pub struct TurnGetParams {
    pub thread_id: String,
    pub turn_id: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnGetResponse {
    pub thread_id: String,
    pub workspace_id: String,
    pub turn: Turn,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
pub struct TurnItemsParams {
    pub thread_id: String,
    pub turn_id: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnItemsResponse {
    pub thread_id: String,
    pub workspace_id: String,
    pub turn_id: String,
    #[serde(default)]
    pub events: Vec<TurnItemEvent>,
    pub last_sequence: i64,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TimelineOriginKind {
    ParentTurn,
    TaskEvent,
    ChildTurn,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TimelineLane {
    Parent,
    Task,
    ChildAgent,
    ChildTool,
    ChildReasoning,
    ChildResult,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TimelineOrigin {
    pub kind: TimelineOriginKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_event_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_turn_item_id: Option<String>,
    pub origin_sequence: i64,
    pub occurred_at: i64,
    pub lane: TimelineLane,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum TurnItemEventPayload {
    #[serde(rename_all = "camelCase")]
    ItemStarted {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item: TurnItem,
    },
    #[serde(rename_all = "camelCase")]
    ItemDelta {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item_id: String,
        delta: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        stream: Option<ItemDeltaStream>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        payload: Option<JsonValue>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        markdown: Option<MarkdownDocument>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        markdown_version: Option<u16>,
    },
    #[serde(rename_all = "camelCase")]
    ItemCompleted {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item: TurnItem,
    },
    #[serde(rename_all = "camelCase")]
    ItemUpdated {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item: TurnItem,
    },
    #[serde(rename_all = "camelCase")]
    ItemTimeoutDetected {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        attempt_number: u32,
        reason: TurnItemTimeoutReason,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        recovery_job_id: Option<String>,
    },
    #[serde(rename_all = "camelCase")]
    ItemRecoveryOpened {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        recovery_job_id: String,
        trigger: RecoveryTrigger,
        action: RecoveryAction,
        attempt_number: u32,
    },
    #[serde(rename_all = "camelCase")]
    ItemRecoveryAttached {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        recovery_job_id: String,
        recovery_item_id: String,
        recovery_item_type: TurnItemType,
        trigger: RecoveryTrigger,
        action: RecoveryAction,
        existing_status: RecoveryJobStatus,
        next_attempt_number: u32,
    },
    #[serde(rename_all = "camelCase")]
    ItemRetryScheduled {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        recovery_job_id: String,
        attempt_number: u32,
        next_run_at_unix: i64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    #[serde(rename_all = "camelCase")]
    ItemRetryAttemptStarted {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        recovery_job_id: String,
        attempt_number: u32,
    },
    #[serde(rename_all = "camelCase")]
    ItemRecoverySucceeded {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        recovery_job_id: String,
        attempt_number: u32,
    },
    #[serde(rename_all = "camelCase")]
    ItemRecoveryExhausted {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        recovery_job_id: String,
        attempt_number: u32,
        status: RecoveryJobStatus,
        error_message: String,
    },
    #[serde(rename_all = "camelCase")]
    ItemToolRetryScheduled {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        tool_retry_episode_id: String,
        tool_name: String,
        attempt_number: u32,
        error_class: ToolRetryErrorClass,
        retry_hint: String,
        budgets: Vec<ToolRetryBudgetUsage>,
        failure_signature_fingerprint: String,
        reason: String,
    },
    #[serde(rename_all = "camelCase")]
    ItemToolRetryResolved {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        tool_retry_episode_id: String,
        tool_name: String,
        attempt_number: u32,
        resolution: ToolRetryResolution,
        budgets: Vec<ToolRetryBudgetUsage>,
        reason: String,
    },
    #[serde(rename_all = "camelCase")]
    ItemToolRetryExhausted {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        tool_retry_episode_id: String,
        tool_name: String,
        attempt_number: u32,
        error_class: ToolRetryErrorClass,
        exhaustion_kind: ToolRetryExhaustionKind,
        budgets: Vec<ToolRetryBudgetUsage>,
        failure_signature_fingerprint: String,
        reason: String,
    },
    #[serde(rename_all = "camelCase")]
    TurnToolLoopBudgetExceeded {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        limit_kind: ToolLoopBudgetLimitKind,
        limit: u32,
        observed: u32,
        action: ToolLoopBudgetAction,
        reason: String,
    },
    #[serde(rename_all = "camelCase")]
    TurnExecutionWindowStarted(TurnExecutionWindowStartedNotification),
    #[serde(rename_all = "camelCase")]
    TurnExecutionWindowExhausted(TurnExecutionWindowExhaustedNotification),
    #[serde(rename_all = "camelCase")]
    TurnExecutionWindowCheckpointed(TurnExecutionWindowCheckpointedNotification),
    #[serde(rename_all = "camelCase")]
    TurnExecutionWindowContinued(TurnExecutionWindowContinuedNotification),
    #[serde(rename_all = "camelCase")]
    TurnExecutionWindowBlocked(TurnExecutionWindowBlockedNotification),
    #[serde(rename_all = "camelCase")]
    TurnPermissionAudit(crate::TurnPermissionAuditEvent),
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnItemEvent {
    pub sequence: i64,
    pub created_at: i64,
    pub payload: TurnItemEventPayload,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct Turn {
    pub id: String,
    pub status: TurnStatus,
    #[serde(default)]
    pub turn_kind: TurnKind,
    #[serde(default)]
    pub origin: TurnOrigin,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_manifest: Option<PromptManifest>,
    pub permission_profile: TurnPermissionProfileSnapshot,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum TurnKind {
    #[default]
    Conversation,
    TaskRun,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum TurnOrigin {
    #[default]
    User,
    ScheduledTask,
    DetachedTask,
    AttachedTask,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct PromptManifest {
    pub compiler_version: String,
    pub profile: PromptManifestProfile,
    #[serde(default)]
    pub section_ids: Vec<String>,
    pub fingerprint_stable: String,
    pub fingerprint_dynamic: String,
    pub fingerprint_full: String,
    #[serde(default)]
    pub diagnostics: Vec<PromptManifestDiagnostic>,
    #[serde(default)]
    pub hook_sources: Vec<PromptManifestHookSourceEntry>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PromptManifestProfile {
    AssistantFull,
    AssistantMinimal,
    AssistantNone,
    CliRuntime,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct PromptManifestDiagnostic {
    pub code: PromptManifestDiagnosticCode,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub section_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hook_source: Option<PromptManifestHookSource>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PromptManifestDiagnosticCode {
    MissingFile,
    FileReadError,
    FileTruncated,
    TotalBudgetTruncated,
    FileFilteredByProfile,
    DynamicSectionTruncated,
    DynamicSectionOmitted,
    HookDiagnostic,
    HookBestEffortFailed,
    CapabilityRejected,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct PromptManifestHookSourceEntry {
    pub source: PromptManifestHookSource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub section_id: Option<String>,
    pub contribution_kind: PromptManifestHookContributionKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_count: Option<usize>,
    pub truncation: PromptManifestHookTruncation,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct PromptManifestHookSource {
    pub hook_id: String,
    pub subscription_id: String,
    pub phase: PromptManifestHookPhase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contribution_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contribution_hash: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PromptManifestHookContributionKind {
    PromptContext,
    ThreadContext,
    PromptSection,
    PromptManifestDiagnostic,
    RuntimeFailure,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PromptManifestHookTruncation {
    None,
    Hook,
    Prompt,
    HookAndPrompt,
    Unknown,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PromptManifestHookPhase {
    TurnPrePromptContext,
    TurnPostPreflightPromptContext,
    TurnPrePromptCompile,
    RuntimeTurnPreContext,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnStatus {
    InProgress,
    Completed,
    Failed,
    Interrupted,
    Blocked,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionWindowStatus {
    Running,
    Exhausted,
    Checkpointed,
    Continued,
    Completed,
    Interrupted,
    Blocked,
    Failed,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionWindowExhaustionReason {
    MaxAgentRoundsPerWindow,
    MaxToolCallsPerWindow,
    MaxWallClockMsPerWindow,
    MaxProviderTokensPerWindow,
    ProviderFailureContinuation,
    RuntimeShutdownContinuation,
}

pub const EXECUTION_CHECKPOINT_PAYLOAD_SCHEMA_VERSION: u32 = 1;
pub const EXECUTION_CHECKPOINT_DEFAULT_TOOL_DETAIL_LIMIT: usize = 32;
pub const EXECUTION_CHECKPOINT_TEXT_PREVIEW_MAX_CHARS: usize = 512;
pub const EXECUTION_CHECKPOINT_METADATA_MAX_CHARS: usize = 256;
pub const EXECUTION_CHECKPOINT_METADATA_MAX_FIELDS: usize = 16;
pub const EXECUTION_CHECKPOINT_METADATA_MAX_ARRAY_ITEMS: usize = 8;

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ExecutionCheckpointPayload {
    pub schema_version: u32,
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub original_request: ExecutionCheckpointOriginalRequestSummary,
    pub window: ExecutionCheckpointWindowSummary,
    pub provider_budget: ExecutionCheckpointProviderBudgetSummary,
    pub tools: ExecutionCheckpointToolSummary,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub strict_obligations: Vec<ExecutionCheckpointStrictObligation>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ExecutionCheckpointOriginalRequestSummary {
    pub input_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text_preview: Option<String>,
    pub text_truncated: bool,
    pub attachment_count: u32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachment_kinds: Vec<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ExecutionCheckpointWindowSummary {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window_id: Option<String>,
    pub window_index: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at_unix_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at_unix_ms: Option<i64>,
    pub agent_round_count: u32,
    pub tool_call_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_token_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exhaustion_reason: Option<ExecutionWindowExhaustionReason>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ExecutionCheckpointProviderBudgetSummary {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_provider: Option<String>,
    pub agent_round_count: u32,
    pub tool_call_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_token_count: Option<u64>,
    pub provider_usage_available: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exhaustion_reason: Option<ExecutionWindowExhaustionReason>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exhausted_limit: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exhausted_observed: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionCheckpointProviderBudgetInput {
    pub model: Option<String>,
    pub model_provider: Option<String>,
    pub agent_round_count: u32,
    pub tool_call_count: u32,
    pub provider_token_count: Option<u64>,
    pub exhaustion_reason: Option<ExecutionWindowExhaustionReason>,
    pub exhausted_limit: Option<u64>,
    pub exhausted_observed: Option<u64>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ExecutionCheckpointToolSummary {
    pub requested_count: u32,
    pub executed_count: u32,
    pub unexecuted_count: u32,
    pub total_count: u32,
    pub succeeded_count: u32,
    pub failed_count: u32,
    pub in_progress_count: u32,
    pub detail_limit: u32,
    pub details_truncated: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub details: Vec<ExecutionCheckpointToolCallSummary>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ExecutionCheckpointToolCallSummary {
    pub item_id: String,
    pub tool_name: String,
    pub item_type: TurnItemType,
    pub status: ToolCallStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub success: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_class: Option<ToolErrorClass>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_error_class: Option<String>,
    pub metadata: ToolMetadata,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ExecutionCheckpointStrictObligation {
    pub obligation_id: String,
    pub kind: String,
    pub description: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub refs: BTreeMap<String, String>,
}

pub trait StrictObligationCollector {
    fn collect_strict_obligations(&self) -> Vec<ExecutionCheckpointStrictObligation>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct EmptyStrictObligationCollector;

impl StrictObligationCollector for EmptyStrictObligationCollector {
    fn collect_strict_obligations(&self) -> Vec<ExecutionCheckpointStrictObligation> {
        Vec::new()
    }
}

#[derive(Debug, Clone)]
pub struct StaticStrictObligationCollector {
    obligations: Vec<ExecutionCheckpointStrictObligation>,
}

impl StaticStrictObligationCollector {
    pub fn new(obligations: Vec<ExecutionCheckpointStrictObligation>) -> Self {
        Self { obligations }
    }
}

impl StrictObligationCollector for StaticStrictObligationCollector {
    fn collect_strict_obligations(&self) -> Vec<ExecutionCheckpointStrictObligation> {
        self.obligations.clone()
    }
}

pub fn build_execution_checkpoint_original_request_summary(
    input: &[UserInput],
) -> ExecutionCheckpointOriginalRequestSummary {
    let mut text = String::new();
    let mut text_truncated = false;
    let mut attachment_kinds = BTreeMap::<String, ()>::new();

    for item in input {
        match item {
            UserInput::Text {
                text: item_text, ..
            } => {
                if !text.is_empty() {
                    push_preview_text(&mut text, "\n", &mut text_truncated);
                }
                push_preview_text(&mut text, item_text, &mut text_truncated);
            }
            _ => {
                attachment_kinds.insert(user_input_kind(item).to_owned(), ());
            }
        }
    }

    ExecutionCheckpointOriginalRequestSummary {
        input_count: u32::try_from(input.len()).unwrap_or(u32::MAX),
        text_preview: if text.is_empty() { None } else { Some(text) },
        text_truncated,
        attachment_count: u32::try_from(
            input
                .iter()
                .filter(|item| !matches!(item, UserInput::Text { .. }))
                .count(),
        )
        .unwrap_or(u32::MAX),
        attachment_kinds: attachment_kinds.into_keys().collect(),
    }
}

pub fn build_execution_checkpoint_provider_budget_summary(
    input: ExecutionCheckpointProviderBudgetInput,
) -> ExecutionCheckpointProviderBudgetSummary {
    ExecutionCheckpointProviderBudgetSummary {
        model: input.model,
        model_provider: input.model_provider,
        agent_round_count: input.agent_round_count,
        tool_call_count: input.tool_call_count,
        provider_token_count: input.provider_token_count,
        provider_usage_available: input.provider_token_count.is_some(),
        exhaustion_reason: input.exhaustion_reason,
        exhausted_limit: input.exhausted_limit,
        exhausted_observed: input.exhausted_observed,
    }
}

pub fn build_execution_checkpoint_tool_summary(
    items: &[TurnItem],
    detail_limit: usize,
) -> ExecutionCheckpointToolSummary {
    let detail_limit = detail_limit.max(1);
    let tool_items = items
        .iter()
        .filter_map(tool_call_summary_from_item)
        .collect::<Vec<_>>();

    let total_count = u32::try_from(tool_items.len()).unwrap_or(u32::MAX);
    let succeeded_count = u32::try_from(
        tool_items
            .iter()
            .filter(|item| item.status == ToolCallStatus::Completed)
            .count(),
    )
    .unwrap_or(u32::MAX);
    let failed_count = u32::try_from(
        tool_items
            .iter()
            .filter(|item| item.status == ToolCallStatus::Failed)
            .count(),
    )
    .unwrap_or(u32::MAX);
    let in_progress_count = u32::try_from(
        tool_items
            .iter()
            .filter(|item| item.status == ToolCallStatus::InProgress)
            .count(),
    )
    .unwrap_or(u32::MAX);
    let details_truncated = tool_items.len() > detail_limit;

    ExecutionCheckpointToolSummary {
        requested_count: total_count,
        executed_count: succeeded_count.saturating_add(failed_count),
        unexecuted_count: 0,
        total_count,
        succeeded_count,
        failed_count,
        in_progress_count,
        detail_limit: u32::try_from(detail_limit).unwrap_or(u32::MAX),
        details_truncated,
        details: tool_items.into_iter().take(detail_limit).collect(),
    }
}

pub fn build_execution_checkpoint_payload(
    workspace_id: impl Into<String>,
    thread_id: impl Into<String>,
    turn_id: impl Into<String>,
    original_request: ExecutionCheckpointOriginalRequestSummary,
    window: ExecutionCheckpointWindowSummary,
    provider_budget: ExecutionCheckpointProviderBudgetSummary,
    tools: ExecutionCheckpointToolSummary,
    obligations: Vec<ExecutionCheckpointStrictObligation>,
) -> ExecutionCheckpointPayload {
    ExecutionCheckpointPayload {
        schema_version: EXECUTION_CHECKPOINT_PAYLOAD_SCHEMA_VERSION,
        workspace_id: workspace_id.into(),
        thread_id: thread_id.into(),
        turn_id: turn_id.into(),
        original_request,
        window,
        provider_budget,
        tools,
        strict_obligations: obligations,
    }
}

pub fn collect_execution_checkpoint_strict_obligations<C: StrictObligationCollector + ?Sized>(
    collector: &C,
) -> Vec<ExecutionCheckpointStrictObligation> {
    collector.collect_strict_obligations()
}

fn push_preview_text(buffer: &mut String, text: &str, truncated: &mut bool) {
    if *truncated {
        return;
    }
    for ch in text.chars() {
        if buffer.chars().count() >= EXECUTION_CHECKPOINT_TEXT_PREVIEW_MAX_CHARS {
            *truncated = true;
            break;
        }
        buffer.push(ch);
    }
}

fn user_input_kind(input: &UserInput) -> &'static str {
    match input {
        UserInput::Text { .. } => "text",
        UserInput::Image { .. } => "image",
        UserInput::LocalImage { .. } => "local_image",
        UserInput::File { .. } => "file",
        UserInput::LocalFile { .. } => "local_file",
        UserInput::Audio { .. } => "audio",
        UserInput::LocalAudio { .. } => "local_audio",
        UserInput::Video { .. } => "video",
        UserInput::LocalVideo { .. } => "local_video",
        UserInput::Artifact { .. } => "artifact",
        UserInput::Mention { .. } => "mention",
    }
}

fn tool_call_summary_from_item(item: &TurnItem) -> Option<ExecutionCheckpointToolCallSummary> {
    match item {
        TurnItem::CommandExecution {
            id,
            tool_name,
            status,
            display,
            storage,
            recovery,
            command,
            cwd,
            success,
            outcome,
            ..
        } => {
            let mut metadata = base_tool_metadata(display, storage);
            insert_json_field(
                &mut metadata,
                "command_arg_count",
                usize_to_u64(command.len()),
            );
            if let Some(cwd) = cwd {
                insert_json_field(&mut metadata, "cwd", cwd.as_str());
            }
            Some(make_tool_call_summary(
                id,
                tool_name,
                item.item_type(),
                *status,
                *success,
                outcome.as_ref(),
                recovery.as_ref(),
                metadata,
            ))
        }
        TurnItem::FileChange {
            id,
            tool_name,
            status,
            display,
            storage,
            recovery,
            changed_files,
            exit_code,
            success,
            outcome,
            ..
        } => {
            let mut metadata = base_tool_metadata(display, storage);
            insert_json_field(
                &mut metadata,
                "changed_file_count",
                usize_to_u64(changed_files.len()),
            );
            if let Some(exit_code) = exit_code {
                insert_json_field(&mut metadata, "exit_code", i64::from(*exit_code));
            }
            Some(make_tool_call_summary(
                id,
                tool_name,
                item.item_type(),
                *status,
                *success,
                outcome.as_ref(),
                recovery.as_ref(),
                metadata,
            ))
        }
        TurnItem::WebSearch {
            id,
            tool_name,
            status,
            display,
            storage,
            recovery,
            query,
            provider,
            took_ms,
            result_count,
            success,
            outcome,
            ..
        } => {
            let mut metadata = base_tool_metadata(display, storage);
            if let Some(query) = query {
                insert_json_field(&mut metadata, "query_preview", truncate_string(query, 120));
            }
            if let Some(provider) = provider {
                insert_json_field(&mut metadata, "provider", provider.as_str());
            }
            if let Some(took_ms) = took_ms {
                insert_json_field(&mut metadata, "took_ms", *took_ms);
            }
            if let Some(result_count) = result_count {
                insert_json_field(&mut metadata, "result_count", usize_to_u64(*result_count));
            }
            Some(make_tool_call_summary(
                id,
                tool_name,
                item.item_type(),
                *status,
                *success,
                outcome.as_ref(),
                recovery.as_ref(),
                metadata,
            ))
        }
        TurnItem::WebFetch {
            id,
            tool_name,
            status,
            display,
            storage,
            recovery,
            url,
            final_url,
            status_code,
            content_type,
            bytes_received,
            elapsed_ms,
            title,
            word_count,
            success,
            outcome,
            ..
        } => {
            let mut metadata = base_tool_metadata(display, storage);
            if let Some(url) = url {
                insert_json_field(&mut metadata, "url", truncate_string(url, 180));
            }
            if let Some(final_url) = final_url {
                insert_json_field(&mut metadata, "final_url", truncate_string(final_url, 180));
            }
            if let Some(status_code) = status_code {
                insert_json_field(&mut metadata, "status_code", u64::from(*status_code));
            }
            if let Some(content_type) = content_type {
                insert_json_field(&mut metadata, "content_type", content_type.as_str());
            }
            if let Some(bytes_received) = bytes_received {
                insert_json_field(
                    &mut metadata,
                    "bytes_received",
                    usize_to_u64(*bytes_received),
                );
            }
            if let Some(elapsed_ms) = elapsed_ms {
                insert_json_field(&mut metadata, "elapsed_ms", *elapsed_ms);
            }
            if let Some(title) = title {
                insert_json_field(&mut metadata, "title_preview", truncate_string(title, 160));
            }
            if let Some(word_count) = word_count {
                insert_json_field(&mut metadata, "word_count", usize_to_u64(*word_count));
            }
            Some(make_tool_call_summary(
                id,
                tool_name,
                item.item_type(),
                *status,
                *success,
                outcome.as_ref(),
                recovery.as_ref(),
                metadata,
            ))
        }
        TurnItem::Download {
            id,
            tool_name,
            status,
            display,
            storage,
            recovery,
            url,
            final_url,
            status_code,
            path,
            bytes_written,
            sha256,
            content_type,
            elapsed_ms,
            success,
            outcome,
            ..
        } => {
            let mut metadata = base_tool_metadata(display, storage);
            if let Some(url) = url {
                insert_json_field(&mut metadata, "url", truncate_string(url, 180));
            }
            if let Some(final_url) = final_url {
                insert_json_field(&mut metadata, "final_url", truncate_string(final_url, 180));
            }
            if let Some(status_code) = status_code {
                insert_json_field(&mut metadata, "status_code", u64::from(*status_code));
            }
            if let Some(path) = path {
                insert_json_field(&mut metadata, "path", truncate_string(path, 180));
            }
            if let Some(bytes_written) = bytes_written {
                insert_json_field(&mut metadata, "bytes_written", *bytes_written);
            }
            if let Some(sha256) = sha256 {
                insert_json_field(&mut metadata, "sha256", sha256.as_str());
            }
            if let Some(content_type) = content_type {
                insert_json_field(&mut metadata, "content_type", content_type.as_str());
            }
            if let Some(elapsed_ms) = elapsed_ms {
                insert_json_field(&mut metadata, "elapsed_ms", *elapsed_ms);
            }
            Some(make_tool_call_summary(
                id,
                tool_name,
                item.item_type(),
                *status,
                *success,
                outcome.as_ref(),
                recovery.as_ref(),
                metadata,
            ))
        }
        TurnItem::DynamicToolCall {
            id,
            tool_name,
            status,
            display,
            storage,
            recovery,
            success,
            outcome,
            ..
        } => Some(make_tool_call_summary(
            id,
            tool_name,
            item.item_type(),
            *status,
            *success,
            outcome.as_ref(),
            recovery.as_ref(),
            base_tool_metadata(display, storage),
        )),
        TurnItem::UserMessage { .. }
        | TurnItem::AgentMessage { .. }
        | TurnItem::Reasoning { .. }
        | TurnItem::SystemEvent { .. }
        | TurnItem::Task { .. } => None,
    }
}

fn make_tool_call_summary(
    item_id: &str,
    tool_name: &str,
    item_type: TurnItemType,
    status: ToolCallStatus,
    success: Option<bool>,
    outcome: Option<&ToolOutcome>,
    recovery: Option<&ToolRecoveryView>,
    metadata: ToolMetadata,
) -> ExecutionCheckpointToolCallSummary {
    ExecutionCheckpointToolCallSummary {
        item_id: item_id.to_owned(),
        tool_name: tool_name.to_owned(),
        item_type,
        status,
        success,
        error_class: outcome.and_then(|outcome| outcome.error_class),
        retry_error_class: recovery.and_then(|recovery| recovery.error_class.clone()),
        metadata: ToolMetadata::from_json(bound_json_value(metadata.to_json(), 0)),
    }
}

fn base_tool_metadata(display: &ToolDisplayPayload, storage: &ToolStoragePayload) -> ToolMetadata {
    let mut metadata = ToolMetadata::empty();
    if let Some(tool_metadata) = safe_tool_metadata(display, storage) {
        metadata.insert(
            "tool_metadata",
            ToolMetadataValue::from_json(tool_metadata.to_json()),
        );
    }
    metadata
}

fn safe_tool_metadata(
    display: &ToolDisplayPayload,
    storage: &ToolStoragePayload,
) -> Option<ToolMetadata> {
    match storage {
        ToolStoragePayload::Summary(summary) => Some(summary.metadata.clone()),
        ToolStoragePayload::Metadata { metadata } => Some(metadata.clone()),
        ToolStoragePayload::Shell { .. } | ToolStoragePayload::None => match display {
            ToolDisplayPayload::Summary(summary) => Some(summary.metadata.clone()),
            ToolDisplayPayload::Progress { metadata, .. } => Some(metadata.clone()),
            ToolDisplayPayload::Shell { .. } | ToolDisplayPayload::Hidden => None,
        },
    }
}

fn insert_json_field<T: Into<JsonValue>>(metadata: &mut ToolMetadata, key: &str, value: T) {
    metadata.insert(key, ToolMetadataValue::from_json(value.into()));
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn bound_json_value(value: JsonValue, depth: usize) -> JsonValue {
    match value {
        JsonValue::String(value) => JsonValue::String(truncate_string(
            &value,
            EXECUTION_CHECKPOINT_METADATA_MAX_CHARS,
        )),
        JsonValue::Array(values) => JsonValue::Array(
            values
                .into_iter()
                .take(EXECUTION_CHECKPOINT_METADATA_MAX_ARRAY_ITEMS)
                .map(|value| bound_json_value(value, depth + 1))
                .collect(),
        ),
        JsonValue::Object(map) if depth < 3 => JsonValue::Object(
            map.into_iter()
                .take(EXECUTION_CHECKPOINT_METADATA_MAX_FIELDS)
                .map(|(key, value)| (key, bound_json_value(value, depth + 1)))
                .collect(),
        ),
        JsonValue::Object(map) => serde_json::json!({
            "truncated": true,
            "objectFieldCount": map.len(),
        }),
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) => value,
    }
}

fn truncate_string(value: &str, max_chars: usize) -> String {
    let mut output = String::new();
    let mut truncated = false;
    for ch in value.chars() {
        if output.chars().count() >= max_chars {
            truncated = true;
            break;
        }
        output.push(ch);
    }
    if truncated {
        output.push_str("...");
    }
    output
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TurnItemType {
    UserMessage,
    AgentMessage,
    Reasoning,
    SystemEvent,
    Task,
    CommandExecution,
    FileChange,
    WebSearch,
    WebFetch,
    Download,
    DynamicToolCall,
}

impl TurnItemType {
    pub const fn is_tool_item(self) -> bool {
        matches!(
            self,
            Self::CommandExecution
                | Self::FileChange
                | Self::WebSearch
                | Self::WebFetch
                | Self::Download
                | Self::DynamicToolCall
        )
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TurnItemAttemptStatus {
    Running,
    Completed,
    Failed,
    TimedOut,
    Cancelled,
    Interrupted,
    Retrying,
    Exhausted,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TurnItemTimeoutReason {
    StartDeadlineExceeded,
    IdleDeadlineExceeded,
    HardDeadlineExceeded,
    LeaseExpired,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryJobStatus {
    Pending,
    Active,
    Succeeded,
    Failed,
    Exhausted,
    Blocked,
    Cancelled,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryTrigger {
    /// Turn-item execution exceeded timeout policy (start/idle/hard/lease).
    Timeout,
    /// Any provider/model/transport failure in LLM interaction path.
    ProviderError,
    /// Turn runtime could not be prepared after the turn was persisted.
    TurnStart,
    /// Turn could not be dispatched to the agent runtime.
    TurnDispatch,
    /// A durable event could not be projected/materialized.
    ProjectionFailure,
    /// A same-turn execution window continuation could not be opened directly.
    ExecutionWindowContinuation,
    /// Final artifact validation/registration failed after model output.
    ArtifactFinalization,
    /// Child/reviewer/revision task turn dispatch failed.
    TaskDispatch,
    /// Generic runtime failure routed through the central terminal decision gate.
    RuntimeFailure,
    /// Forward-compatibility fallback for unknown persisted values.
    Unknown,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryAction {
    RetryAttempt,
    RetryWithBackoff,
    RestartTurn,
    ReplayDurableEvent,
    RehydrateTurnState,
    OpenNextExecutionWindow,
    AdaptProviderRequest,
    RefreshProviderAuth,
    CompactHistory,
    DisableStreaming,
    DisableUnsupportedCapability,
    RepairArtifactFinalization,
    RequeueTaskDispatch,
    BlockResumable,
    Fallback,
    MarkFailed,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolRecoveryRetryClass {
    Never,
    Transient,
    Arguments,
    Session,
    Network,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolRecoveryIdempotencyMode {
    None,
    Safe,
    RequiresKey,
    SessionBound,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ToolRecoveryPolicySnapshot {
    pub retry_class: ToolRecoveryRetryClass,
    pub idempotency_mode: ToolRecoveryIdempotencyMode,
    pub max_attempts: u8,
    pub can_resume: bool,
    pub resolved_action: RecoveryAction,
    pub base_backoff_secs: u64,
    pub max_wall_clock_secs: u64,
    pub no_progress_limit: i64,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ToolOutputPolicySnapshot {
    pub llm: LlmOutputPolicy,
    pub llm_retention: LlmRetentionPolicy,
    pub timeline: TimelineOutputPolicy,
    pub storage: StorageOutputPolicy,
    pub recovery: RecoveryOutputPolicy,
    pub deltas: DeltaOutputPolicy,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "mode")]
pub enum LlmOutputPolicy {
    Full { max_bytes: usize },
    Structured { max_bytes: usize },
    SummaryOnly,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "mode")]
pub enum LlmRetentionPolicy {
    UntilTurnTerminal { max_bytes: usize },
    DoNotRetain,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "mode")]
pub enum TimelineOutputPolicy {
    Full { max_bytes: usize },
    Summary { max_chars: usize },
    MetadataOnly,
    Hidden,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "mode")]
pub enum StorageOutputPolicy {
    Full { max_bytes: usize },
    Summary { max_chars: usize },
    MetadataOnly,
    None,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "mode")]
pub enum RecoveryOutputPolicy {
    Evidence {
        include_exit_status: bool,
        include_error_class: bool,
        include_retry_hint: bool,
        diagnostic_excerpt: DiagnosticExcerptPolicy,
        include_fingerprints: bool,
    },
    MetadataOnly,
    None,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "mode")]
pub enum DiagnosticExcerptPolicy {
    Disabled,
    ErrorsOnly { max_chars: usize },
    Output { max_chars: usize },
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "mode")]
pub enum DeltaOutputPolicy {
    PersistAndDisplay {
        max_chunk_bytes: usize,
        max_total_bytes: usize,
    },
    ProgressOnly,
    Disabled,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ToolOutputSummary {
    pub title: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lines: Vec<String>,
    pub metadata: ToolMetadata,
    pub truncated: bool,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum ToolDisplayPayload {
    Shell {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        stdout: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        stderr: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        aggregated_output: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        exit_code: Option<i32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        duration_ms: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timed_out: Option<bool>,
        truncated: bool,
    },
    Summary(ToolOutputSummary),
    Progress {
        stage: String,
        metadata: ToolMetadata,
    },
    Hidden,
}

impl Default for ToolDisplayPayload {
    fn default() -> Self {
        Self::Hidden
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum ToolStoragePayload {
    Shell {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        stdout: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        stderr: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        aggregated_output: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        exit_code: Option<i32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        duration_ms: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timed_out: Option<bool>,
        truncated: bool,
    },
    Summary(ToolOutputSummary),
    Metadata {
        metadata: ToolMetadata,
    },
    None,
}

impl Default for ToolStoragePayload {
    fn default() -> Self {
        Self::None
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ToolRecoveryView {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_class: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_hint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incomplete_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostic_summary: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostic_excerpt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_fingerprint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_fingerprint: Option<String>,
    pub was_truncated: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation: Option<JsonValue>,
}

impl ToolOutputPolicySnapshot {
    pub fn for_tool_name(tool_name: &str) -> Self {
        match tool_name {
            "exec_command" | "write_stdin" => shell_output_policy_snapshot(),
            "web_fetch" => web_fetch_output_policy_snapshot(),
            "web_search" => web_search_output_policy_snapshot(),
            "download_url" | "download" => download_output_policy_snapshot(),
            "computer_use" => computer_use_output_policy_snapshot(),
            "artifact_prepare" | "artifact_register" | "read_file" | "read_skill" | "list_dir"
            | "grep_files" | "apply_patch" | "write_file" | "edit_file" | "tool_search"
            | "tool_suggest" => model_only_metadata_policy_snapshot(),
            _ => dynamic_unknown_output_policy_snapshot(),
        }
    }

    pub fn for_external_runtime_tool_name(tool_name: &str) -> Self {
        match tool_name {
            "exec_command" | "write_stdin" => shell_output_policy_snapshot(),
            "web_fetch" => web_fetch_output_policy_snapshot(),
            "web_search" => web_search_output_policy_snapshot(),
            "download_url" | "download" => download_output_policy_snapshot(),
            "computer_use" => computer_use_output_policy_snapshot(),
            "artifact_prepare" | "artifact_register" | "read_file" | "list_dir" | "grep_files"
            | "apply_patch" | "write_file" | "edit_file" | "tool_search" | "tool_suggest" => {
                model_only_metadata_policy_snapshot()
            }
            _ => external_runtime_tool_output_policy_snapshot(),
        }
    }
}

const DEFAULT_LLM_MAX_BYTES: usize = 2 * 1024 * 1024;
const DEFAULT_SUMMARY_CHARS: usize = 2_000;
const DEFAULT_SHELL_CHUNK_BYTES: usize = 64 * 1024;
const DEFAULT_SHELL_TOTAL_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_DIAGNOSTIC_CHARS: usize = 4_000;

fn retained_llm_policy() -> LlmRetentionPolicy {
    LlmRetentionPolicy::UntilTurnTerminal {
        max_bytes: DEFAULT_LLM_MAX_BYTES,
    }
}

fn evidence_recovery_policy(diagnostic_excerpt: DiagnosticExcerptPolicy) -> RecoveryOutputPolicy {
    RecoveryOutputPolicy::Evidence {
        include_exit_status: true,
        include_error_class: true,
        include_retry_hint: true,
        diagnostic_excerpt,
        include_fingerprints: true,
    }
}

fn shell_output_policy_snapshot() -> ToolOutputPolicySnapshot {
    ToolOutputPolicySnapshot {
        llm: LlmOutputPolicy::Full {
            max_bytes: DEFAULT_LLM_MAX_BYTES,
        },
        llm_retention: retained_llm_policy(),
        timeline: TimelineOutputPolicy::Full {
            max_bytes: DEFAULT_SHELL_TOTAL_BYTES,
        },
        storage: StorageOutputPolicy::Full {
            max_bytes: DEFAULT_SHELL_TOTAL_BYTES,
        },
        recovery: evidence_recovery_policy(DiagnosticExcerptPolicy::ErrorsOnly {
            max_chars: DEFAULT_DIAGNOSTIC_CHARS,
        }),
        deltas: DeltaOutputPolicy::PersistAndDisplay {
            max_chunk_bytes: DEFAULT_SHELL_CHUNK_BYTES,
            max_total_bytes: DEFAULT_SHELL_TOTAL_BYTES,
        },
    }
}

fn model_only_metadata_policy_snapshot() -> ToolOutputPolicySnapshot {
    ToolOutputPolicySnapshot {
        llm: LlmOutputPolicy::Full {
            max_bytes: DEFAULT_LLM_MAX_BYTES,
        },
        llm_retention: retained_llm_policy(),
        timeline: TimelineOutputPolicy::Summary {
            max_chars: DEFAULT_SUMMARY_CHARS,
        },
        storage: StorageOutputPolicy::MetadataOnly,
        recovery: evidence_recovery_policy(DiagnosticExcerptPolicy::Disabled),
        deltas: DeltaOutputPolicy::Disabled,
    }
}

fn web_fetch_output_policy_snapshot() -> ToolOutputPolicySnapshot {
    ToolOutputPolicySnapshot {
        llm: LlmOutputPolicy::Full {
            max_bytes: DEFAULT_LLM_MAX_BYTES,
        },
        llm_retention: retained_llm_policy(),
        timeline: TimelineOutputPolicy::Summary {
            max_chars: DEFAULT_SUMMARY_CHARS,
        },
        storage: StorageOutputPolicy::MetadataOnly,
        recovery: evidence_recovery_policy(DiagnosticExcerptPolicy::Disabled),
        deltas: DeltaOutputPolicy::ProgressOnly,
    }
}

fn web_search_output_policy_snapshot() -> ToolOutputPolicySnapshot {
    ToolOutputPolicySnapshot {
        llm: LlmOutputPolicy::Structured {
            max_bytes: DEFAULT_LLM_MAX_BYTES,
        },
        llm_retention: retained_llm_policy(),
        timeline: TimelineOutputPolicy::Summary {
            max_chars: DEFAULT_SUMMARY_CHARS,
        },
        storage: StorageOutputPolicy::Summary {
            max_chars: DEFAULT_SUMMARY_CHARS,
        },
        recovery: evidence_recovery_policy(DiagnosticExcerptPolicy::Disabled),
        deltas: DeltaOutputPolicy::ProgressOnly,
    }
}

fn download_output_policy_snapshot() -> ToolOutputPolicySnapshot {
    ToolOutputPolicySnapshot {
        llm: LlmOutputPolicy::Structured {
            max_bytes: DEFAULT_LLM_MAX_BYTES,
        },
        llm_retention: retained_llm_policy(),
        timeline: TimelineOutputPolicy::Summary {
            max_chars: DEFAULT_SUMMARY_CHARS,
        },
        storage: StorageOutputPolicy::MetadataOnly,
        recovery: evidence_recovery_policy(DiagnosticExcerptPolicy::Disabled),
        deltas: DeltaOutputPolicy::ProgressOnly,
    }
}

fn computer_use_output_policy_snapshot() -> ToolOutputPolicySnapshot {
    ToolOutputPolicySnapshot {
        llm: LlmOutputPolicy::Structured {
            max_bytes: DEFAULT_LLM_MAX_BYTES,
        },
        llm_retention: retained_llm_policy(),
        timeline: TimelineOutputPolicy::Summary {
            max_chars: DEFAULT_SUMMARY_CHARS,
        },
        storage: StorageOutputPolicy::MetadataOnly,
        recovery: evidence_recovery_policy(DiagnosticExcerptPolicy::Disabled),
        deltas: DeltaOutputPolicy::ProgressOnly,
    }
}

fn external_runtime_tool_output_policy_snapshot() -> ToolOutputPolicySnapshot {
    ToolOutputPolicySnapshot {
        llm: LlmOutputPolicy::Structured {
            max_bytes: DEFAULT_LLM_MAX_BYTES,
        },
        llm_retention: retained_llm_policy(),
        timeline: TimelineOutputPolicy::Summary {
            max_chars: DEFAULT_SUMMARY_CHARS,
        },
        storage: StorageOutputPolicy::MetadataOnly,
        recovery: RecoveryOutputPolicy::MetadataOnly,
        deltas: DeltaOutputPolicy::ProgressOnly,
    }
}

fn dynamic_unknown_output_policy_snapshot() -> ToolOutputPolicySnapshot {
    ToolOutputPolicySnapshot {
        llm: LlmOutputPolicy::Structured {
            max_bytes: DEFAULT_LLM_MAX_BYTES,
        },
        llm_retention: retained_llm_policy(),
        timeline: TimelineOutputPolicy::MetadataOnly,
        storage: StorageOutputPolicy::MetadataOnly,
        recovery: RecoveryOutputPolicy::MetadataOnly,
        deltas: DeltaOutputPolicy::ProgressOnly,
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ProviderFailureClass {
    NetworkTransient,
    RateLimit,
    #[serde(rename = "provider_5xx")]
    Provider5xx,
    AuthExpired,
    AuthOrPermission,
    ModelNotFound,
    PromptTooLong,
    ContextTooLarge,
    MaxOutputTokens,
    StreamStall,
    StreamTruncated,
    EmptyResponse,
    ProviderRejected,
    UnsupportedParameter,
    UnsupportedCapability,
    UnsupportedImageInput,
    UnsupportedToolCalling,
    UnsupportedStreaming,
    MalformedProviderRequest,
    InvalidRequest,
    PermissionDenied,
    Unknown,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderTransportKind {
    Stream,
    NonStream,
    Ws,
    Sse,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderFailureStage {
    Connect,
    FirstChunk,
    MidStream,
    Finalize,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ProviderFailureDetails {
    pub provider: String,
    pub model: String,
    pub transport: ProviderTransportKind,
    pub class: ProviderFailureClass,
    pub stage: ProviderFailureStage,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_status: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
    pub is_recoverable_hint: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolCallStatus {
    InProgress,
    Completed,
    Failed,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ItemDeltaStream {
    AgentMessage,
    Stdout,
    Stderr,
    ToolProgress,
    FileChange,
    Generic,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SystemEventLevel {
    Info,
    Warning,
    Error,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ToolObservation {
    pub trace_id: String,
    pub turn_id: String,
    pub tool_call_id: String,
    pub attempt_id: u32,
    pub pipeline_stage: String,
    pub ts_unix_ms: i64,
    pub mono_ns: u64,
    pub event_seq: u64,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolOutcomeStatus {
    Ok,
    RecoverableError,
    FatalError,
    PartialSuccess,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolErrorClass {
    InvalidArguments,
    NotFound,
    ToolNotVisible,
    PermissionDenied,
    CommandNotFound,
    Timeout,
    Cancelled,
    ExecutionFailed,
    NeedsNarrowing,
    Internal,
    OutputTruncated,
    Unknown,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ToolRetryErrorClass {
    InvalidArguments,
    NotFound,
    ToolNotVisible,
    PermissionDenied,
    CommandNotFound,
    Timeout,
    Cancelled,
    ExecutionFailed,
    NeedsNarrowing,
    Internal,
    OutputTruncated,
    Unknown,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ToolRetryBudgetKind {
    Episode,
    ErrorClass,
    ToolName,
    FailureSignature,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ToolRetryBudgetUsage {
    pub kind: ToolRetryBudgetKind,
    pub used: u32,
    pub limit: u32,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ToolRetryResolution {
    Succeeded,
    NonRetryable,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ToolRetryExhaustionKind {
    TotalRetryRounds,
    ErrorClass,
    ToolName,
    FailureSignature,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ToolLoopBudgetLimitKind {
    AgentRounds,
    ToolCalls,
    ProviderReturnedToolsAfterToolsDisabled,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ToolLoopBudgetAction {
    ContinueInNextWindow,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ToolOutcome {
    pub status: ToolOutcomeStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_class: Option<ToolErrorClass>,
    pub should_retry: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_hint: Option<String>,
    pub incomplete: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incomplete_reason: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct WebSearchResultItem {
    pub rank: usize,
    pub title: String,
    pub url: String,
    pub snippet: String,
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub published_at: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct WebFetchLink {
    pub text: String,
    pub url: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TextElement {
    pub byte_range: ByteRange,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub placeholder: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum UserInput {
    #[serde(rename_all = "camelCase")]
    Text {
        text: String,
        #[serde(default)]
        text_elements: Vec<TextElement>,
    },
    Image {
        url: String,
    },
    LocalImage {
        path: String,
    },
    File {
        url: String,
    },
    LocalFile {
        path: String,
    },
    Audio {
        url: String,
    },
    LocalAudio {
        path: String,
    },
    Video {
        url: String,
    },
    LocalVideo {
        path: String,
    },
    Artifact {
        #[serde(rename = "artifactId")]
        artifact_id: String,
        #[serde(rename = "versionId", default, skip_serializing_if = "Option::is_none")]
        version_id: Option<String>,
    },
    Mention {
        name: String,
        path: String,
    },
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum UserMessageAttachment {
    Image {
        url: String,
    },
    LocalImage {
        path: String,
    },
    File {
        url: String,
    },
    LocalFile {
        path: String,
    },
    Audio {
        url: String,
    },
    LocalAudio {
        path: String,
    },
    Video {
        url: String,
    },
    LocalVideo {
        path: String,
    },
    Artifact {
        artifact: ArtifactRef,
    },
    Skill {
        capability: TurnSkillCapabilitySummary,
    },
    SkillPack {
        capability: TurnSkillPackCapabilitySummary,
    },
    McpServer {
        capability: TurnMcpServerCapabilitySummary,
    },
    McpTool {
        capability: TurnMcpToolCapabilitySummary,
    },
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TurnSkillCapabilitySummary {
    #[serde(rename = "skillId")]
    pub skill_id: SkillId,
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    pub slug: String,
    #[serde(rename = "sourceKind")]
    pub source_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pack: Option<TurnSkillPackPresentationSummary>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TurnSkillPackCapabilitySummary {
    #[serde(rename = "packId")]
    pub pack_id: SkillPackId,
    pub label: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TurnSkillPackPresentationSummary {
    #[serde(rename = "packId")]
    pub pack_id: SkillPackId,
    pub label: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TurnMcpServerCapabilitySummary {
    pub id: String,
    pub label: String,
    pub name: String,
    #[serde(rename = "scopeKind")]
    pub scope_kind: McpScopeKind,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TurnMcpToolCapabilitySummary {
    pub id: String,
    pub label: String,
    #[serde(rename = "serverName")]
    pub server_name: String,
    #[serde(rename = "rawToolName")]
    pub raw_tool_name: String,
    #[serde(rename = "scopeKind")]
    pub scope_kind: McpScopeKind,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[serde(rename_all = "snake_case")]
pub enum AgentMessagePhase {
    #[default]
    FinalAnswer,
    Commentary,
}

impl AgentMessagePhase {
    pub fn is_final_answer(&self) -> bool {
        matches!(self, Self::FinalAnswer)
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum TurnItem {
    #[serde(rename_all = "camelCase")]
    UserMessage {
        id: String,
        text: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        attachments: Vec<UserMessageAttachment>,
    },
    #[serde(rename_all = "camelCase")]
    AgentMessage {
        id: String,
        text: String,
        #[serde(default, skip_serializing_if = "AgentMessagePhase::is_final_answer")]
        phase: AgentMessagePhase,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        markdown: Option<MarkdownDocument>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        markdown_version: Option<u16>,
    },
    #[serde(rename_all = "camelCase")]
    Reasoning {
        id: String,
        #[serde(default)]
        summary: Vec<String>,
        #[serde(default)]
        content: Vec<String>,
    },
    #[serde(rename_all = "camelCase")]
    SystemEvent {
        id: String,
        level: SystemEventLevel,
        message: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        code: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        details: Option<JsonValue>,
    },
    #[serde(rename_all = "camelCase")]
    Task {
        #[serde(flatten)]
        item: TaskTurnItem,
    },
    #[serde(rename_all = "camelCase")]
    CommandExecution {
        id: String,
        tool_name: String,
        arguments: JsonValue,
        status: ToolCallStatus,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        recovery_policy: Option<ToolRecoveryPolicySnapshot>,
        output_policy: ToolOutputPolicySnapshot,
        display: ToolDisplayPayload,
        storage: ToolStoragePayload,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        recovery: Option<ToolRecoveryView>,
        #[serde(default)]
        command: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cwd: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        success: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        outcome: Option<ToolOutcome>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        observation: Option<ToolObservation>,
    },
    #[serde(rename_all = "camelCase")]
    FileChange {
        id: String,
        tool_name: String,
        arguments: JsonValue,
        status: ToolCallStatus,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        recovery_policy: Option<ToolRecoveryPolicySnapshot>,
        output_policy: ToolOutputPolicySnapshot,
        display: ToolDisplayPayload,
        storage: ToolStoragePayload,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        recovery: Option<ToolRecoveryView>,
        #[serde(default)]
        changed_files: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        exit_code: Option<i32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        stdout: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        stderr: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        success: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        outcome: Option<ToolOutcome>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        observation: Option<ToolObservation>,
    },
    #[serde(rename_all = "camelCase")]
    WebSearch {
        id: String,
        tool_name: String,
        arguments: JsonValue,
        status: ToolCallStatus,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        recovery_policy: Option<ToolRecoveryPolicySnapshot>,
        output_policy: ToolOutputPolicySnapshot,
        display: ToolDisplayPayload,
        storage: ToolStoragePayload,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        recovery: Option<ToolRecoveryView>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        query: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        provider: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        took_ms: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        result_count: Option<usize>,
        #[serde(default)]
        results: Vec<WebSearchResultItem>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        success: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        outcome: Option<ToolOutcome>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        observation: Option<ToolObservation>,
    },
    #[serde(rename_all = "camelCase")]
    WebFetch {
        id: String,
        tool_name: String,
        arguments: JsonValue,
        status: ToolCallStatus,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        recovery_policy: Option<ToolRecoveryPolicySnapshot>,
        output_policy: ToolOutputPolicySnapshot,
        display: ToolDisplayPayload,
        storage: ToolStoragePayload,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        recovery: Option<ToolRecoveryView>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        url: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        final_url: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        status_code: Option<u16>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content_type: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        extract_mode: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        resolved_mode: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        bytes_received: Option<usize>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        elapsed_ms: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        truncated: Option<JsonValue>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        title: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        word_count: Option<usize>,
        #[serde(default)]
        links: Vec<WebFetchLink>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        success: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        outcome: Option<ToolOutcome>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        observation: Option<ToolObservation>,
    },
    #[serde(rename_all = "camelCase")]
    Download {
        id: String,
        tool_name: String,
        arguments: JsonValue,
        status: ToolCallStatus,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        recovery_policy: Option<ToolRecoveryPolicySnapshot>,
        output_policy: ToolOutputPolicySnapshot,
        display: ToolDisplayPayload,
        storage: ToolStoragePayload,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        recovery: Option<ToolRecoveryView>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        url: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        final_url: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        status_code: Option<u16>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        path: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        bytes_written: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sha256: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content_type: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        elapsed_ms: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        truncated: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        success: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        outcome: Option<ToolOutcome>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        observation: Option<ToolObservation>,
    },
    #[serde(rename_all = "camelCase")]
    DynamicToolCall {
        id: String,
        tool_name: String,
        arguments: JsonValue,
        status: ToolCallStatus,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        recovery_policy: Option<ToolRecoveryPolicySnapshot>,
        output_policy: ToolOutputPolicySnapshot,
        display: ToolDisplayPayload,
        storage: ToolStoragePayload,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        recovery: Option<ToolRecoveryView>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        success: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        outcome: Option<ToolOutcome>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        observation: Option<ToolObservation>,
    },
}

impl TurnItem {
    pub fn item_id(&self) -> &str {
        match self {
            Self::UserMessage { id, .. }
            | Self::AgentMessage { id, .. }
            | Self::Reasoning { id, .. }
            | Self::SystemEvent { id, .. }
            | Self::Task {
                item: TaskTurnItem { id, .. },
            }
            | Self::CommandExecution { id, .. }
            | Self::FileChange { id, .. }
            | Self::WebSearch { id, .. }
            | Self::WebFetch { id, .. }
            | Self::Download { id, .. }
            | Self::DynamicToolCall { id, .. } => id.as_str(),
        }
    }

    pub fn item_type(&self) -> TurnItemType {
        match self {
            Self::UserMessage { .. } => TurnItemType::UserMessage,
            Self::AgentMessage { .. } => TurnItemType::AgentMessage,
            Self::Reasoning { .. } => TurnItemType::Reasoning,
            Self::SystemEvent { .. } => TurnItemType::SystemEvent,
            Self::Task { .. } => TurnItemType::Task,
            Self::CommandExecution { .. } => TurnItemType::CommandExecution,
            Self::FileChange { .. } => TurnItemType::FileChange,
            Self::WebSearch { .. } => TurnItemType::WebSearch,
            Self::WebFetch { .. } => TurnItemType::WebFetch,
            Self::Download { .. } => TurnItemType::Download,
            Self::DynamicToolCall { .. } => TurnItemType::DynamicToolCall,
        }
    }

    pub fn is_tool_item(&self) -> bool {
        self.item_type().is_tool_item()
    }

    pub fn recovery_policy(&self) -> Option<&ToolRecoveryPolicySnapshot> {
        match self {
            Self::CommandExecution {
                recovery_policy, ..
            }
            | Self::FileChange {
                recovery_policy, ..
            }
            | Self::WebSearch {
                recovery_policy, ..
            }
            | Self::WebFetch {
                recovery_policy, ..
            }
            | Self::Download {
                recovery_policy, ..
            }
            | Self::DynamicToolCall {
                recovery_policy, ..
            } => recovery_policy.as_ref(),
            Self::UserMessage { .. }
            | Self::AgentMessage { .. }
            | Self::Reasoning { .. }
            | Self::SystemEvent { .. }
            | Self::Task { .. } => None,
        }
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnStartedNotification {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn: Turn,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnCompletedNotification {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn: Turn,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnFailedNotification {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn: Turn,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnBlockedNotification {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn: Turn,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume: Option<TurnBlockedResumeMetadata>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnBlockedResumeMetadata {
    pub reason_class: String,
    pub human_message: String,
    pub resume_requirements: Vec<String>,
    pub resume_command: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocked_recovery_job_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_checkpoint_id: Option<String>,
    pub can_resume_same_turn: bool,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ItemStartedNotification {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub item: TurnItem,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ItemDeltaNotification {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub delta: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream: Option<ItemDeltaStream>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<JsonValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub markdown: Option<MarkdownDocument>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub markdown_version: Option<u16>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ItemCompletedNotification {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub item: TurnItem,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ItemUpdatedNotification {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub item: TurnItem,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ItemTimeoutDetectedNotification {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub item_type: TurnItemType,
    pub attempt_number: u32,
    pub reason: TurnItemTimeoutReason,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_job_id: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ItemRecoveryOpenedNotification {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub item_type: TurnItemType,
    pub recovery_job_id: String,
    pub trigger: RecoveryTrigger,
    pub action: RecoveryAction,
    pub attempt_number: u32,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ItemRecoveryAttachedNotification {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub item_type: TurnItemType,
    pub recovery_job_id: String,
    pub recovery_item_id: String,
    pub recovery_item_type: TurnItemType,
    pub trigger: RecoveryTrigger,
    pub action: RecoveryAction,
    pub existing_status: RecoveryJobStatus,
    pub next_attempt_number: u32,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ItemRetryScheduledNotification {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub item_type: TurnItemType,
    pub recovery_job_id: String,
    pub attempt_number: u32,
    pub next_run_at_unix: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ItemRetryAttemptStartedNotification {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub item_type: TurnItemType,
    pub recovery_job_id: String,
    pub attempt_number: u32,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ItemRecoverySucceededNotification {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub item_type: TurnItemType,
    pub recovery_job_id: String,
    pub attempt_number: u32,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ItemRecoveryExhaustedNotification {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub item_type: TurnItemType,
    pub recovery_job_id: String,
    pub attempt_number: u32,
    pub status: RecoveryJobStatus,
    pub error_message: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ItemToolRetryScheduledNotification {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub item_type: TurnItemType,
    pub tool_retry_episode_id: String,
    pub tool_name: String,
    pub attempt_number: u32,
    pub error_class: ToolRetryErrorClass,
    pub retry_hint: String,
    #[serde(default)]
    pub budgets: Vec<ToolRetryBudgetUsage>,
    pub failure_signature_fingerprint: String,
    pub reason: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ItemToolRetryResolvedNotification {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub item_type: TurnItemType,
    pub tool_retry_episode_id: String,
    pub tool_name: String,
    pub attempt_number: u32,
    pub resolution: ToolRetryResolution,
    #[serde(default)]
    pub budgets: Vec<ToolRetryBudgetUsage>,
    pub reason: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ItemToolRetryExhaustedNotification {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub item_type: TurnItemType,
    pub tool_retry_episode_id: String,
    pub tool_name: String,
    pub attempt_number: u32,
    pub error_class: ToolRetryErrorClass,
    pub exhaustion_kind: ToolRetryExhaustionKind,
    #[serde(default)]
    pub budgets: Vec<ToolRetryBudgetUsage>,
    pub failure_signature_fingerprint: String,
    pub reason: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnToolLoopBudgetExceededNotification {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub limit_kind: ToolLoopBudgetLimitKind,
    pub limit: u32,
    pub observed: u32,
    pub action: ToolLoopBudgetAction,
    pub reason: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnExecutionWindowStartedNotification {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub window_id: String,
    pub window_index: u32,
    pub status: ExecutionWindowStatus,
    pub started_at_unix_ms: i64,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnExecutionWindowExhaustedNotification {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub window_id: String,
    pub window_index: u32,
    pub status: ExecutionWindowStatus,
    pub exhaustion_reason: ExecutionWindowExhaustionReason,
    pub limit: u64,
    pub observed: u64,
    pub agent_round_count: u32,
    pub tool_call_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_token_count: Option<u64>,
    pub started_at_unix_ms: i64,
    pub exhausted_at_unix_ms: i64,
    pub reason: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnExecutionWindowCheckpointedNotification {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub window_id: String,
    pub window_index: u32,
    pub status: ExecutionWindowStatus,
    pub checkpoint_id: String,
    pub checkpoint_kind: String,
    pub payload_bytes: u64,
    pub created_at_unix_ms: i64,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnExecutionWindowContinuedNotification {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub window_id: String,
    pub window_index: u32,
    pub status: ExecutionWindowStatus,
    pub previous_window_id: String,
    pub previous_window_index: u32,
    pub checkpoint_id: String,
    pub continued_at_unix_ms: i64,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnExecutionWindowBlockedNotification {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub window_id: String,
    pub window_index: u32,
    pub status: ExecutionWindowStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exhaustion_reason: Option<ExecutionWindowExhaustionReason>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_id: Option<String>,
    pub total_windows: u32,
    pub total_tool_calls: u32,
    pub reason: String,
    pub blocked_at_unix_ms: i64,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnStatusChangedNotification {
    pub thread_id: String,
    pub turn_id: String,
    pub status: TurnStatus,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ContextCompressingNotification {
    pub thread_id: String,
    pub turn_id: String,
    pub message: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ContextCompressedNotification {
    pub thread_id: String,
    pub turn_id: String,
    pub compressed_tokens: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn turn_permission_mode_uses_snake_case_wire_values() {
        let cases = [
            (TurnPermissionMode::FullAccess, "full_access"),
            (TurnPermissionMode::AutoAcceptEdits, "auto_accept_edits"),
            (TurnPermissionMode::Supervised, "supervised"),
        ];

        for (value, expected) in cases {
            let encoded = serde_json::to_value(value).expect("mode should serialize");
            assert_eq!(encoded, json!(expected));
            assert_eq!(value.as_str(), expected);
            let decoded: TurnPermissionMode =
                serde_json::from_value(encoded).expect("mode should deserialize");
            assert_eq!(decoded, value);
        }

        assert_eq!(
            TurnPermissionMode::default(),
            TurnPermissionMode::FullAccess
        );
    }

    #[test]
    fn permission_behavior_uses_snake_case_wire_values() {
        let cases = [
            (PermissionBehavior::Allow, "allow"),
            (PermissionBehavior::Ask, "ask"),
            (PermissionBehavior::Deny, "deny"),
        ];

        for (value, expected) in cases {
            let encoded = serde_json::to_value(value).expect("behavior should serialize");
            assert_eq!(encoded, json!(expected));
            assert_eq!(value.as_str(), expected);
            let decoded: PermissionBehavior =
                serde_json::from_value(encoded).expect("behavior should deserialize");
            assert_eq!(decoded, value);
        }
    }

    #[test]
    fn turn_permission_profile_source_uses_snake_case_wire_values() {
        let cases = [
            (TurnPermissionProfileSource::Composer, "composer"),
            (TurnPermissionProfileSource::Defaulted, "defaulted"),
            (
                TurnPermissionProfileSource::InheritedFromParentTurn,
                "inherited_from_parent_turn",
            ),
            (
                TurnPermissionProfileSource::TaskPermissionCap,
                "task_permission_cap",
            ),
            (TurnPermissionProfileSource::System, "system"),
        ];

        for (value, expected) in cases {
            let encoded = serde_json::to_value(value).expect("source should serialize");
            assert_eq!(encoded, json!(expected));
            assert_eq!(value.as_str(), expected);
            let decoded: TurnPermissionProfileSource =
                serde_json::from_value(encoded).expect("source should deserialize");
            assert_eq!(decoded, value);
        }
    }

    #[test]
    fn turn_permission_profile_selection_round_trips() {
        let selection: TurnPermissionProfileSelection = serde_json::from_value(json!({
            "mode": "auto_accept_edits"
        }))
        .expect("selection should deserialize");

        assert_eq!(selection.mode, TurnPermissionMode::AutoAcceptEdits);
        assert_eq!(
            serde_json::to_value(selection).expect("selection should serialize"),
            json!({ "mode": "auto_accept_edits" })
        );
    }

    #[test]
    fn turn_permission_profile_snapshot_round_trips_policy_fields() {
        let snapshot = TurnPermissionProfileSnapshot {
            mode: TurnPermissionMode::Supervised,
            source: TurnPermissionProfileSource::Composer,
            effective_policy: ToolPermissionPolicySnapshot {
                default_behavior: PermissionBehavior::Ask,
                file_read: PermissionBehavior::Allow,
                file_write: PermissionBehavior::Ask,
                shell_command: PermissionBehavior::Ask,
                network: PermissionBehavior::Ask,
                mcp_read: PermissionBehavior::Allow,
                mcp_write_or_unknown: PermissionBehavior::Ask,
                dynamic_skill_tool: PermissionBehavior::Ask,
                computer_use: PermissionBehavior::Ask,
                task_subagent: PermissionBehavior::Ask,
                allowed_tools: vec!["read_file".to_owned()],
                denied_tools: vec!["exec_command".to_owned()],
                allowed_paths: vec!["/workspace/src".to_owned()],
            },
        };

        let encoded = serde_json::to_value(&snapshot).expect("snapshot should serialize");
        assert_eq!(encoded["mode"], "supervised");
        assert_eq!(encoded["source"], "composer");
        assert_eq!(encoded["effective_policy"]["file_read"], "allow");
        assert_eq!(encoded["effective_policy"]["shell_command"], "ask");
        assert_eq!(encoded["effective_policy"]["allowed_tools"][0], "read_file");

        let decoded: TurnPermissionProfileSnapshot =
            serde_json::from_value(encoded).expect("snapshot should deserialize");
        assert_eq!(decoded, snapshot);
    }

    #[test]
    fn resolve_turn_permission_profile_defaults_to_full_access() {
        let snapshot = resolve_turn_permission_profile(None);

        assert_eq!(snapshot.mode, TurnPermissionMode::FullAccess);
        assert_eq!(snapshot.source, TurnPermissionProfileSource::Defaulted);
        assert_eq!(
            snapshot.effective_policy,
            ToolPermissionPolicySnapshot::all(PermissionBehavior::Allow)
        );
    }

    #[test]
    fn resolve_turn_permission_profile_uses_composer_source_for_selection() {
        let selection = TurnPermissionProfileSelection {
            mode: TurnPermissionMode::AutoAcceptEdits,
        };
        let snapshot = resolve_turn_permission_profile(Some(&selection));

        assert_eq!(snapshot.mode, TurnPermissionMode::AutoAcceptEdits);
        assert_eq!(snapshot.source, TurnPermissionProfileSource::Composer);
        assert_eq!(
            snapshot.effective_policy.file_write,
            PermissionBehavior::Allow
        );
        assert_eq!(
            snapshot.effective_policy.shell_command,
            PermissionBehavior::Ask
        );
    }

    #[test]
    fn compile_turn_permission_profile_full_access_is_allow_all() {
        let snapshot = crate::compile_turn_permission_profile(
            TurnPermissionMode::FullAccess,
            TurnPermissionProfileSource::Composer,
        );

        assert_eq!(snapshot.mode, TurnPermissionMode::FullAccess);
        assert_eq!(snapshot.source, TurnPermissionProfileSource::Composer);
        assert_eq!(
            snapshot.effective_policy,
            ToolPermissionPolicySnapshot::all(PermissionBehavior::Allow)
        );
    }

    #[test]
    fn default_turn_permission_profile_snapshot_is_full_access_defaulted() {
        let snapshot = crate::default_turn_permission_profile_snapshot();

        assert_eq!(snapshot.mode, TurnPermissionMode::FullAccess);
        assert_eq!(snapshot.source, TurnPermissionProfileSource::Defaulted);
        assert_eq!(
            snapshot.effective_policy,
            ToolPermissionPolicySnapshot::all(PermissionBehavior::Allow)
        );
    }

    #[test]
    fn permission_profile_compilation_is_deterministic() {
        let first = crate::compile_turn_permission_profile(
            TurnPermissionMode::FullAccess,
            TurnPermissionProfileSource::System,
        );
        let second = crate::compile_turn_permission_profile(
            TurnPermissionMode::FullAccess,
            TurnPermissionProfileSource::System,
        );

        assert_eq!(first, second);
    }

    #[test]
    fn permission_profile_snapshot_json_field_order_is_not_semantic() {
        let first: TurnPermissionProfileSnapshot = serde_json::from_value(json!({
            "mode": "full_access",
            "source": "defaulted",
            "effective_policy": {
                "default_behavior": "allow",
                "file_read": "allow",
                "file_write": "allow",
                "shell_command": "allow",
                "network": "allow",
                "mcp_read": "allow",
                "mcp_write_or_unknown": "allow",
                "dynamic_skill_tool": "allow",
                "computer_use": "allow",
                "task_subagent": "allow"
            }
        }))
        .expect("first snapshot should decode");
        let second: TurnPermissionProfileSnapshot = serde_json::from_value(json!({
            "effective_policy": {
                "task_subagent": "allow",
                "computer_use": "allow",
                "dynamic_skill_tool": "allow",
                "mcp_write_or_unknown": "allow",
                "mcp_read": "allow",
                "network": "allow",
                "shell_command": "allow",
                "file_write": "allow",
                "file_read": "allow",
                "default_behavior": "allow"
            },
            "source": "defaulted",
            "mode": "full_access"
        }))
        .expect("second snapshot should decode");

        assert_eq!(first, second);
    }

    #[test]
    fn source_specific_permission_profile_helpers_set_explicit_sources() {
        let selection = TurnPermissionProfileSelection {
            mode: TurnPermissionMode::Supervised,
        };

        assert_eq!(
            crate::composer_turn_permission_profile_snapshot(&selection).source,
            TurnPermissionProfileSource::Composer
        );
        assert_eq!(
            crate::inherited_turn_permission_profile_snapshot(TurnPermissionMode::Supervised)
                .source,
            TurnPermissionProfileSource::InheritedFromParentTurn
        );
        assert_eq!(
            crate::system_turn_permission_profile_snapshot(TurnPermissionMode::Supervised).source,
            TurnPermissionProfileSource::System
        );
        assert_eq!(
            crate::task_permission_cap_snapshot(&crate::task_permission_cap_for_mode(
                TurnPermissionMode::Supervised
            ))
            .source,
            TurnPermissionProfileSource::TaskPermissionCap
        );
    }

    #[test]
    fn permission_policy_tables_match_product_modes() {
        let full_access = crate::permission_policy_for_mode(TurnPermissionMode::FullAccess);
        assert_eq!(
            full_access,
            ToolPermissionPolicySnapshot::all(PermissionBehavior::Allow)
        );

        let auto_accept_edits =
            crate::permission_policy_for_mode(TurnPermissionMode::AutoAcceptEdits);
        assert_eq!(auto_accept_edits.default_behavior, PermissionBehavior::Ask);
        assert_eq!(auto_accept_edits.file_read, PermissionBehavior::Allow);
        assert_eq!(auto_accept_edits.file_write, PermissionBehavior::Allow);
        assert_eq!(auto_accept_edits.shell_command, PermissionBehavior::Ask);
        assert_eq!(auto_accept_edits.network, PermissionBehavior::Ask);
        assert_eq!(auto_accept_edits.mcp_read, PermissionBehavior::Allow);
        assert_eq!(
            auto_accept_edits.mcp_write_or_unknown,
            PermissionBehavior::Ask
        );
        assert_eq!(
            auto_accept_edits.dynamic_skill_tool,
            PermissionBehavior::Ask
        );
        assert_eq!(auto_accept_edits.computer_use, PermissionBehavior::Ask);
        assert_eq!(auto_accept_edits.task_subagent, PermissionBehavior::Ask);

        let supervised = crate::permission_policy_for_mode(TurnPermissionMode::Supervised);
        assert_eq!(supervised.default_behavior, PermissionBehavior::Ask);
        assert_eq!(supervised.file_read, PermissionBehavior::Allow);
        assert_eq!(supervised.file_write, PermissionBehavior::Ask);
        assert_eq!(supervised.shell_command, PermissionBehavior::Ask);
        assert_eq!(supervised.network, PermissionBehavior::Ask);
        assert_eq!(supervised.mcp_read, PermissionBehavior::Allow);
        assert_eq!(supervised.mcp_write_or_unknown, PermissionBehavior::Ask);
        assert_eq!(supervised.dynamic_skill_tool, PermissionBehavior::Ask);
        assert_eq!(supervised.computer_use, PermissionBehavior::Ask);
        assert_eq!(supervised.task_subagent, PermissionBehavior::Ask);
    }

    #[test]
    fn restricted_permission_policy_tables_do_not_deny_by_default() {
        for mode in [
            TurnPermissionMode::AutoAcceptEdits,
            TurnPermissionMode::Supervised,
        ] {
            let policy = crate::permission_policy_for_mode(mode);
            let fields = [
                policy.default_behavior,
                policy.file_read,
                policy.file_write,
                policy.shell_command,
                policy.network,
                policy.mcp_read,
                policy.mcp_write_or_unknown,
                policy.dynamic_skill_tool,
                policy.computer_use,
                policy.task_subagent,
            ];

            assert!(
                fields
                    .into_iter()
                    .all(|behavior| behavior != PermissionBehavior::Deny)
            );
            assert_eq!(policy.default_behavior, PermissionBehavior::Ask);
        }
    }

    #[test]
    fn security_snapshot_unrestricted_full_access_round_trips_with_explicit_sandbox() {
        let snapshot =
            TurnExecutionSecuritySnapshot::unrestricted_full_access("/workspace/project", 1_700);

        let encoded = serde_json::to_value(&snapshot).expect("snapshot should serialize");

        assert_eq!(encoded["schema_version"], 1);
        assert_eq!(encoded["version"], 1);
        assert_eq!(encoded["permission_profile"]["mode"], "full_access");
        assert_eq!(encoded["sandbox"]["mode"], "unrestricted");
        assert_eq!(encoded["sandbox"]["filesystem"]["kind"], "unrestricted");
        assert_eq!(encoded["sandbox"]["network"]["mode"], "enabled");
        assert_eq!(encoded["network"]["mode"], "enabled");
        assert_eq!(encoded["sandbox"]["backend_requirement"], "optional");
        assert_eq!(encoded["process"]["shell"]["enabled"], true);
        assert_eq!(encoded["enforcement"], json!({ "status": "active" }));
        assert!(encoded.get("sandbox").is_some());

        let decoded: TurnExecutionSecuritySnapshot =
            serde_json::from_value(encoded).expect("snapshot should deserialize");
        assert_eq!(decoded, snapshot);
        assert_eq!(decoded.sandbox.mode, TurnSandboxMode::Unrestricted);
        assert_eq!(
            decoded.sandbox.filesystem.kind,
            TurnFilesystemSandboxKind::Unrestricted
        );
    }

    #[test]
    fn security_snapshot_read_only_round_trips_with_restricted_roots() {
        let profile = TurnPermissionProfileSnapshot::from_mode(
            TurnPermissionMode::Supervised,
            TurnPermissionProfileSource::Composer,
        );
        let snapshot = TurnExecutionSecuritySnapshot::read_only(
            profile,
            "/workspace/project",
            vec![TurnFilesystemSandboxEntry::workspace_root(
                TurnFilesystemAccess::Read,
                "/workspace/project",
            )],
            1_701,
        );

        let encoded = serde_json::to_value(&snapshot).expect("snapshot should serialize");

        assert_eq!(encoded["permission_profile"]["mode"], "supervised");
        assert_eq!(encoded["sandbox"]["mode"], "read_only");
        assert_eq!(encoded["sandbox"]["filesystem"]["kind"], "restricted");
        assert_eq!(
            encoded["sandbox"]["filesystem"]["entries"][0]["path"],
            json!({ "kind": "workspace_root" })
        );
        assert_eq!(
            encoded["sandbox"]["filesystem"]["entries"][0]["access"],
            "read"
        );
        assert_eq!(encoded["network"]["mode"], "disabled");
        assert_eq!(encoded["sandbox"]["backend_requirement"], "required");
        assert_eq!(encoded["sandbox"]["backend_preference"], json!(["nono"]));

        let decoded: TurnExecutionSecuritySnapshot =
            serde_json::from_value(encoded).expect("snapshot should deserialize");
        assert_eq!(decoded, snapshot);
        assert_eq!(decoded.sandbox.mode, TurnSandboxMode::ReadOnly);
    }

    #[test]
    fn security_snapshot_workspace_write_round_trips_with_write_roots() {
        let profile = TurnPermissionProfileSnapshot::from_mode(
            TurnPermissionMode::AutoAcceptEdits,
            TurnPermissionProfileSource::Composer,
        );
        let snapshot = TurnExecutionSecuritySnapshot::workspace_write(
            profile,
            "/workspace/project",
            vec![TurnFilesystemSandboxEntry::workspace_root(
                TurnFilesystemAccess::Write,
                "/workspace/project",
            )],
            1_702,
        );

        let encoded = serde_json::to_value(&snapshot).expect("snapshot should serialize");

        assert_eq!(encoded["permission_profile"]["mode"], "auto_accept_edits");
        assert_eq!(encoded["sandbox"]["mode"], "workspace_write");
        assert_eq!(
            encoded["sandbox"]["filesystem"]["entries"][0]["access"],
            "write"
        );
        assert_eq!(encoded["approval"]["allow_for_turn"], true);
        assert_eq!(encoded["approval"]["request_permissions"], true);
        assert_eq!(encoded["backend"]["sandbox_backend"], "nono");

        let decoded: TurnExecutionSecuritySnapshot =
            serde_json::from_value(encoded).expect("snapshot should deserialize");
        assert_eq!(decoded, snapshot);
        assert_eq!(decoded.sandbox.mode, TurnSandboxMode::WorkspaceWrite);
    }

    #[test]
    fn execution_window_status_uses_snake_case_wire_values() {
        let cases = [
            (ExecutionWindowStatus::Running, "running"),
            (ExecutionWindowStatus::Exhausted, "exhausted"),
            (ExecutionWindowStatus::Checkpointed, "checkpointed"),
            (ExecutionWindowStatus::Continued, "continued"),
            (ExecutionWindowStatus::Completed, "completed"),
            (ExecutionWindowStatus::Interrupted, "interrupted"),
            (ExecutionWindowStatus::Blocked, "blocked"),
            (ExecutionWindowStatus::Failed, "failed"),
        ];

        for (value, expected) in cases {
            let encoded = serde_json::to_value(value).expect("status should serialize");
            assert_eq!(encoded, json!(expected));
            let decoded: ExecutionWindowStatus =
                serde_json::from_value(encoded).expect("status should deserialize");
            assert_eq!(decoded, value);
        }
    }

    #[test]
    fn execution_window_exhaustion_reason_uses_window_scoped_wire_values() {
        let cases = [
            (
                ExecutionWindowExhaustionReason::MaxAgentRoundsPerWindow,
                "max_agent_rounds_per_window",
            ),
            (
                ExecutionWindowExhaustionReason::MaxToolCallsPerWindow,
                "max_tool_calls_per_window",
            ),
            (
                ExecutionWindowExhaustionReason::MaxWallClockMsPerWindow,
                "max_wall_clock_ms_per_window",
            ),
            (
                ExecutionWindowExhaustionReason::MaxProviderTokensPerWindow,
                "max_provider_tokens_per_window",
            ),
            (
                ExecutionWindowExhaustionReason::ProviderFailureContinuation,
                "provider_failure_continuation",
            ),
            (
                ExecutionWindowExhaustionReason::RuntimeShutdownContinuation,
                "runtime_shutdown_continuation",
            ),
        ];

        for (value, expected) in cases {
            let encoded = serde_json::to_value(value).expect("reason should serialize");
            assert_eq!(encoded, json!(expected));
            let decoded: ExecutionWindowExhaustionReason =
                serde_json::from_value(encoded).expect("reason should deserialize");
            assert_eq!(decoded, value);
        }
    }

    #[test]
    fn tool_loop_budget_action_includes_continuation_wire_value() {
        let cases = [(
            ToolLoopBudgetAction::ContinueInNextWindow,
            "continue_in_next_window",
        )];

        for (value, expected) in cases {
            let encoded = serde_json::to_value(value).expect("action should serialize");
            assert_eq!(encoded, json!(expected));
            let decoded: ToolLoopBudgetAction =
                serde_json::from_value(encoded).expect("action should deserialize");
            assert_eq!(decoded, value);
        }
    }

    #[test]
    fn execution_window_lifecycle_notifications_serialize_bounded_payloads() {
        let started = TurnExecutionWindowStartedNotification {
            workspace_id: "ws_1".to_owned(),
            thread_id: "thr_1".to_owned(),
            turn_id: "turn_1".to_owned(),
            window_id: "win_1".to_owned(),
            window_index: 1,
            status: ExecutionWindowStatus::Running,
            started_at_unix_ms: 1000,
        };
        let started_json = serde_json::to_value(&started).expect("started should serialize");
        assert_eq!(started_json["workspace_id"], "ws_1");
        assert_eq!(started_json["status"], "running");

        let exhausted = TurnExecutionWindowExhaustedNotification {
            workspace_id: "ws_1".to_owned(),
            thread_id: "thr_1".to_owned(),
            turn_id: "turn_1".to_owned(),
            window_id: "win_1".to_owned(),
            window_index: 1,
            status: ExecutionWindowStatus::Exhausted,
            exhaustion_reason: ExecutionWindowExhaustionReason::MaxToolCallsPerWindow,
            limit: 512,
            observed: 513,
            agent_round_count: 20,
            tool_call_count: 513,
            provider_token_count: Some(42_000),
            started_at_unix_ms: 1000,
            exhausted_at_unix_ms: 2000,
            reason: "tool-call window budget exhausted".to_owned(),
        };
        let exhausted_json = serde_json::to_value(&exhausted).expect("exhausted should serialize");
        assert_eq!(
            exhausted_json["exhaustion_reason"],
            "max_tool_calls_per_window"
        );
        assert_eq!(exhausted_json["tool_call_count"], 513);

        let checkpointed = TurnExecutionWindowCheckpointedNotification {
            workspace_id: "ws_1".to_owned(),
            thread_id: "thr_1".to_owned(),
            turn_id: "turn_1".to_owned(),
            window_id: "win_1".to_owned(),
            window_index: 1,
            status: ExecutionWindowStatus::Checkpointed,
            checkpoint_id: "chk_1".to_owned(),
            checkpoint_kind: "window_exhausted".to_owned(),
            payload_bytes: 1024,
            created_at_unix_ms: 2100,
        };
        let checkpointed_json =
            serde_json::to_value(&checkpointed).expect("checkpointed should serialize");
        assert_eq!(checkpointed_json["checkpoint_id"], "chk_1");
        assert!(checkpointed_json.get("payload").is_none());

        let continued = TurnExecutionWindowContinuedNotification {
            workspace_id: "ws_1".to_owned(),
            thread_id: "thr_1".to_owned(),
            turn_id: "turn_1".to_owned(),
            window_id: "win_2".to_owned(),
            window_index: 2,
            status: ExecutionWindowStatus::Continued,
            previous_window_id: "win_1".to_owned(),
            previous_window_index: 1,
            checkpoint_id: "chk_1".to_owned(),
            continued_at_unix_ms: 2200,
        };
        let continued_json = serde_json::to_value(&continued).expect("continued should serialize");
        assert_eq!(continued_json["window_id"], "win_2");
        assert_eq!(continued_json["previous_window_id"], "win_1");

        let blocked = TurnExecutionWindowBlockedNotification {
            workspace_id: "ws_1".to_owned(),
            thread_id: "thr_1".to_owned(),
            turn_id: "turn_1".to_owned(),
            window_id: "win_3".to_owned(),
            window_index: 3,
            status: ExecutionWindowStatus::Blocked,
            exhaustion_reason: Some(ExecutionWindowExhaustionReason::MaxWallClockMsPerWindow),
            checkpoint_id: Some("chk_3".to_owned()),
            total_windows: 3,
            total_tool_calls: 900,
            reason: "total continuation budget exhausted".to_owned(),
            blocked_at_unix_ms: 3000,
        };
        let blocked_json = serde_json::to_value(&blocked).expect("blocked should serialize");
        assert_eq!(blocked_json["status"], "blocked");
        assert_eq!(
            blocked_json["exhaustion_reason"],
            "max_wall_clock_ms_per_window"
        );
        assert_eq!(blocked_json["checkpoint_id"], "chk_3");
    }

    #[test]
    fn turn_status_blocked_round_trips_as_distinct_status() {
        let encoded = serde_json::to_value(TurnStatus::Blocked).expect("status should serialize");
        assert_eq!(encoded, json!("Blocked"));

        let decoded: TurnStatus =
            serde_json::from_value(encoded).expect("status should deserialize");
        assert_eq!(decoded, TurnStatus::Blocked);
    }

    #[test]
    fn execution_checkpoint_payload_round_trips_with_bounded_original_request() {
        let original_request = build_execution_checkpoint_original_request_summary(&[
            UserInput::Text {
                text: "x".repeat(EXECUTION_CHECKPOINT_TEXT_PREVIEW_MAX_CHARS + 64),
                text_elements: Vec::new(),
            },
            UserInput::LocalFile {
                path: "/tmp/input.md".to_owned(),
            },
        ]);
        assert_eq!(original_request.input_count, 2);
        assert_eq!(original_request.attachment_count, 1);
        assert_eq!(original_request.attachment_kinds, vec!["local_file"]);
        assert_eq!(
            original_request
                .text_preview
                .as_ref()
                .expect("text preview should exist")
                .chars()
                .count(),
            EXECUTION_CHECKPOINT_TEXT_PREVIEW_MAX_CHARS
        );
        assert!(original_request.text_truncated);

        let payload = build_execution_checkpoint_payload(
            "ws_1",
            "thr_1",
            "turn_1",
            original_request,
            ExecutionCheckpointWindowSummary {
                window_id: Some("win_1".to_owned()),
                window_index: 1,
                started_at_unix_ms: Some(1000),
                completed_at_unix_ms: Some(2000),
                agent_round_count: 7,
                tool_call_count: 3,
                provider_token_count: Some(1024),
                exhaustion_reason: Some(ExecutionWindowExhaustionReason::MaxToolCallsPerWindow),
            },
            build_execution_checkpoint_provider_budget_summary(
                ExecutionCheckpointProviderBudgetInput {
                    model: Some("model_a".to_owned()),
                    model_provider: Some("provider_a".to_owned()),
                    agent_round_count: 7,
                    tool_call_count: 3,
                    provider_token_count: Some(1024),
                    exhaustion_reason: Some(ExecutionWindowExhaustionReason::MaxToolCallsPerWindow),
                    exhausted_limit: Some(3),
                    exhausted_observed: Some(4),
                },
            ),
            build_execution_checkpoint_tool_summary(&[], 4),
            Vec::new(),
        );

        let encoded = serde_json::to_vec(&payload).expect("payload should serialize");
        assert!(
            encoded.len() < 10 * 1024,
            "sample checkpoint payload should stay compact"
        );
        let decoded: ExecutionCheckpointPayload =
            serde_json::from_slice(&encoded).expect("payload should deserialize");
        assert_eq!(decoded, payload);
        assert_eq!(
            decoded.schema_version,
            EXECUTION_CHECKPOINT_PAYLOAD_SCHEMA_VERSION
        );
    }

    #[test]
    fn execution_checkpoint_tool_summary_counts_and_omits_raw_shell_output() {
        let items = vec![
            TurnItem::CommandExecution {
                id: "cmd_1".to_owned(),
                tool_name: "exec_command".to_owned(),
                arguments: json!({"cmd": "printf secret"}),
                status: ToolCallStatus::Failed,
                recovery_policy: None,
                output_policy: ToolOutputPolicySnapshot::for_tool_name("exec_command"),
                display: ToolDisplayPayload::Shell {
                    stdout: Some("RAW_STDOUT_DO_NOT_COPY".to_owned()),
                    stderr: Some("RAW_STDERR_DO_NOT_COPY".to_owned()),
                    aggregated_output: Some("RAW_AGGREGATED_DO_NOT_COPY".to_owned()),
                    exit_code: Some(2),
                    duration_ms: Some(50),
                    timed_out: Some(false),
                    truncated: false,
                },
                storage: ToolStoragePayload::Shell {
                    stdout: Some("STORED_STDOUT_DO_NOT_COPY".to_owned()),
                    stderr: Some("STORED_STDERR_DO_NOT_COPY".to_owned()),
                    aggregated_output: Some("STORED_AGGREGATED_DO_NOT_COPY".to_owned()),
                    exit_code: Some(2),
                    duration_ms: Some(50),
                    timed_out: Some(false),
                    truncated: false,
                },
                recovery: Some(ToolRecoveryView {
                    error_class: Some("invalid_arguments".to_owned()),
                    retry_hint: None,
                    incomplete_reason: None,
                    diagnostic_summary: None,
                    diagnostic_excerpt: None,
                    output_fingerprint: Some("out_fp".to_owned()),
                    content_fingerprint: None,
                    was_truncated: false,
                    continuation: None,
                }),
                command: vec!["printf".to_owned(), "secret".to_owned()],
                cwd: Some("/tmp".to_owned()),
                success: Some(false),
                outcome: Some(ToolOutcome {
                    status: ToolOutcomeStatus::FatalError,
                    error_class: Some(ToolErrorClass::InvalidArguments),
                    should_retry: false,
                    retry_hint: None,
                    incomplete: false,
                    incomplete_reason: None,
                }),
                observation: None,
            },
            TurnItem::FileChange {
                id: "file_1".to_owned(),
                tool_name: "apply_patch".to_owned(),
                arguments: json!({}),
                status: ToolCallStatus::Completed,
                recovery_policy: None,
                output_policy: ToolOutputPolicySnapshot::for_tool_name("apply_patch"),
                display: ToolDisplayPayload::default(),
                storage: ToolStoragePayload::default(),
                recovery: None,
                changed_files: vec!["/tmp/a.txt".to_owned()],
                exit_code: Some(0),
                stdout: Some("FILE_STDOUT_DO_NOT_COPY".to_owned()),
                stderr: Some("FILE_STDERR_DO_NOT_COPY".to_owned()),
                success: Some(true),
                outcome: Some(ToolOutcome {
                    status: ToolOutcomeStatus::Ok,
                    error_class: None,
                    should_retry: false,
                    retry_hint: None,
                    incomplete: false,
                    incomplete_reason: None,
                }),
                observation: None,
            },
            TurnItem::AgentMessage {
                id: "agent_1".to_owned(),
                text: "visible".to_owned(),
                phase: Default::default(),
                markdown: None,
                markdown_version: None,
            },
        ];

        let summary = build_execution_checkpoint_tool_summary(&items, 1);
        assert_eq!(summary.requested_count, 2);
        assert_eq!(summary.executed_count, 2);
        assert_eq!(summary.unexecuted_count, 0);
        assert_eq!(summary.total_count, 2);
        assert_eq!(summary.succeeded_count, 1);
        assert_eq!(summary.failed_count, 1);
        assert_eq!(summary.in_progress_count, 0);
        assert_eq!(summary.details.len(), 1);
        assert!(summary.details_truncated);
        assert_eq!(summary.details[0].item_id, "cmd_1");
        assert_eq!(
            summary.details[0].error_class,
            Some(ToolErrorClass::InvalidArguments)
        );
        assert_eq!(
            summary.details[0].retry_error_class.as_deref(),
            Some("invalid_arguments")
        );
        assert_eq!(
            summary.details[0]
                .metadata
                .get("command_arg_count")
                .and_then(ToolMetadataValue::as_u64),
            Some(2)
        );
        assert_eq!(
            summary.details[0]
                .metadata
                .get("cwd")
                .and_then(ToolMetadataValue::as_str),
            Some("/tmp")
        );

        let encoded = serde_json::to_string(&summary).expect("summary should serialize");
        for forbidden in [
            "RAW_STDOUT_DO_NOT_COPY",
            "RAW_STDERR_DO_NOT_COPY",
            "RAW_AGGREGATED_DO_NOT_COPY",
            "STORED_STDOUT_DO_NOT_COPY",
            "STORED_STDERR_DO_NOT_COPY",
            "STORED_AGGREGATED_DO_NOT_COPY",
            "FILE_STDOUT_DO_NOT_COPY",
            "FILE_STDERR_DO_NOT_COPY",
            "visible",
        ] {
            assert!(
                !encoded.contains(forbidden),
                "checkpoint tool summary should not include raw output or non-tool content"
            );
        }
    }

    #[test]
    fn execution_checkpoint_provider_budget_does_not_fabricate_missing_usage() {
        let unavailable = build_execution_checkpoint_provider_budget_summary(
            ExecutionCheckpointProviderBudgetInput {
                model: Some("model_a".to_owned()),
                model_provider: Some("provider_a".to_owned()),
                agent_round_count: 4,
                tool_call_count: 9,
                provider_token_count: None,
                exhaustion_reason: None,
                exhausted_limit: None,
                exhausted_observed: None,
            },
        );
        assert_eq!(unavailable.provider_token_count, None);
        assert!(!unavailable.provider_usage_available);

        let available = build_execution_checkpoint_provider_budget_summary(
            ExecutionCheckpointProviderBudgetInput {
                model: None,
                model_provider: None,
                agent_round_count: 4,
                tool_call_count: 9,
                provider_token_count: Some(0),
                exhaustion_reason: Some(
                    ExecutionWindowExhaustionReason::MaxProviderTokensPerWindow,
                ),
                exhausted_limit: Some(100),
                exhausted_observed: Some(101),
            },
        );
        assert_eq!(available.provider_token_count, Some(0));
        assert!(available.provider_usage_available);
        assert_eq!(available.exhausted_limit, Some(100));
        assert_eq!(available.exhausted_observed, Some(101));
    }

    #[test]
    fn strict_obligation_collectors_only_return_explicit_runtime_obligations() {
        let empty = EmptyStrictObligationCollector;
        assert!(collect_execution_checkpoint_strict_obligations(&empty).is_empty());

        let mut refs = BTreeMap::new();
        refs.insert("artifact_id".to_owned(), "art_1".to_owned());
        let obligation = ExecutionCheckpointStrictObligation {
            obligation_id: "obl_1".to_owned(),
            kind: "artifact_not_registered".to_owned(),
            description: "artifact was prepared but not finalized".to_owned(),
            refs,
        };
        let collector = StaticStrictObligationCollector::new(vec![obligation.clone()]);
        assert_eq!(
            collect_execution_checkpoint_strict_obligations(&collector),
            vec![obligation]
        );
    }

    #[test]
    fn turn_start_params_decode_text_input() {
        let params: TurnStartParams = serde_json::from_value(json!({
            "thread_id": "thr_123",
            "turn_id": "turn_123",
            "input": [
                {
                    "type": "text",
                    "text": "hello"
                }
            ]
        }))
        .expect("params should decode");

        assert_eq!(params.thread_id, "thr_123");
        assert_eq!(params.turn_id, "turn_123");
        assert_eq!(params.input.len(), 1);
        assert!(matches!(
            params.input.first(),
            Some(UserInput::Text { text, .. }) if text == "hello"
        ));
        assert!(params.execution_backend.is_none());
        assert!(params.reasoning.is_none());
        assert!(params.cli_runtime_options.is_none());
    }

    #[test]
    fn turn_start_params_backcompat_accepts_old_payload_without_security_snapshot() {
        let params: TurnStartParams = serde_json::from_value(json!({
            "thread_id": "thr_legacy",
            "turn_id": "turn_legacy",
            "input": [
                {
                    "type": "text",
                    "text": "legacy client payload"
                }
            ],
            "sandbox_policy": {
                "mode": "FullAccess"
            }
        }))
        .expect("old turn/start payload should decode without a security snapshot");

        assert_eq!(params.thread_id, "thr_legacy");
        assert_eq!(params.turn_id, "turn_legacy");
        assert!(params.permission_profile.is_none());
        assert!(params.execution_backend.is_none());
        assert!(params.cli_runtime_options.is_none());
        assert_eq!(
            params.sandbox_policy,
            Some(SandboxPolicy {
                mode: crate::SandboxMode::FullAccess
            })
        );
    }

    #[test]
    fn turn_start_params_round_trips_api_provider_execution_backend() {
        let params: TurnStartParams = serde_json::from_value(json!({
            "thread_id": "thr_123",
            "turn_id": "turn_123",
            "execution_backend": {
                "type": "apiProvider",
                "provider": "openai"
            }
        }))
        .expect("params should decode");

        assert_eq!(
            params.execution_backend,
            Some(AgentExecutionBackend::ApiProvider {
                provider: "openai".to_owned()
            })
        );

        let encoded = serde_json::to_value(params).expect("params should encode");
        assert_eq!(
            encoded["execution_backend"],
            json!({
                "type": "apiProvider",
                "provider": "openai"
            })
        );
    }

    #[test]
    fn turn_start_params_round_trips_reasoning_selection() {
        let params: TurnStartParams = serde_json::from_value(json!({
            "thread_id": "thr_123",
            "turn_id": "turn_123",
            "reasoning": {
                "effort": "high"
            }
        }))
        .expect("params should decode");

        assert_eq!(
            params.reasoning,
            Some(TurnReasoningSelection {
                effort: "high".to_owned()
            })
        );
        assert!(params.permission_profile.is_none());

        let encoded = serde_json::to_value(params).expect("params should encode");
        assert_eq!(
            encoded["reasoning"],
            json!({
                "effort": "high"
            })
        );
    }

    #[test]
    fn reasoning_effort_parse_and_as_str_cover_canonical_values() {
        for (raw, effort, canonical) in [
            ("max", ReasoningEffort::Max, "max"),
            ("xhigh", ReasoningEffort::XHigh, "xhigh"),
            ("high", ReasoningEffort::High, "high"),
            ("medium", ReasoningEffort::Medium, "medium"),
            ("low", ReasoningEffort::Low, "low"),
            ("minimal", ReasoningEffort::Minimal, "minimal"),
            ("none", ReasoningEffort::None, "none"),
        ] {
            assert_eq!(ReasoningEffort::from_str(raw), Some(effort));
            assert_eq!(effort.as_str(), canonical);
        }
    }

    #[test]
    fn reasoning_effort_parse_accepts_documented_aliases_and_rejects_unknown() {
        assert_eq!(
            ReasoningEffort::from_str("extra_high"),
            Some(ReasoningEffort::XHigh)
        );
        assert_eq!(
            ReasoningEffort::from_str("extra-high"),
            Some(ReasoningEffort::XHigh)
        );
        assert_eq!(
            ReasoningEffort::from_str("Extra High"),
            Some(ReasoningEffort::XHigh)
        );
        assert_eq!(
            ReasoningEffort::from_str("maximum"),
            Some(ReasoningEffort::Max)
        );
        assert_eq!(
            ReasoningEffort::from_str("off"),
            Some(ReasoningEffort::None)
        );
        assert_eq!(ReasoningEffort::canonical_value("x-high"), Some("xhigh"));
        assert_eq!(ReasoningEffort::from_str("turbo"), None);
    }

    #[test]
    fn reasoning_effort_metadata_values_preserve_runtime_defined_options() {
        assert_eq!(
            normalize_metadata_reasoning_effort(" Extra High "),
            Some("xhigh".to_owned())
        );
        assert_eq!(
            normalize_metadata_reasoning_effort(" ultra "),
            Some("ultra".to_owned())
        );
        assert_eq!(
            normalize_metadata_reasoning_effort("future-level"),
            Some("future-level".to_owned())
        );
        assert_eq!(
            reasoning_effort_comparison_key("Ultra"),
            Some("ultra".to_owned())
        );
        assert_eq!(normalize_metadata_reasoning_effort("  "), None);
    }

    #[test]
    fn turn_start_params_round_trips_cli_agent_runtime_execution_backend() {
        let params: TurnStartParams = serde_json::from_value(json!({
            "thread_id": "thr_123",
            "turn_id": "turn_123",
            "execution_backend": {
                "type": "cliAgentRuntime",
                "runtime_id": "codex_personal",
                "runtime_kind": "codex"
            },
            "cli_runtime_options": {
                "sandbox": {
                    "type": "workspaceWrite",
                    "networkAccess": false
                },
                "effort": "medium",
                "personality": "friendly",
                "summary": "concise",
                "steer_if_active": true
            }
        }))
        .expect("params should decode");

        assert_eq!(
            params.execution_backend,
            Some(AgentExecutionBackend::CLIAgentRuntime {
                runtime_id: "codex_personal".to_owned(),
                runtime_kind: CLIAgentRuntimeKind::Codex
            })
        );
        let options = params
            .cli_runtime_options
            .as_ref()
            .expect("cli options should decode");
        assert_eq!(options.effort.as_deref(), Some("medium"));
        assert_eq!(options.personality.as_deref(), Some("friendly"));
        assert_eq!(options.summary.as_deref(), Some("concise"));
        assert_eq!(options.steer_if_active, Some(true));

        let encoded = serde_json::to_value(params).expect("params should encode");
        assert_eq!(
            encoded["execution_backend"],
            json!({
                "type": "cliAgentRuntime",
                "runtime_id": "codex_personal",
                "runtime_kind": "codex"
            })
        );
        assert_eq!(
            encoded["cli_runtime_options"]["sandbox"],
            json!({
                "type": "workspaceWrite",
                "networkAccess": false
            })
        );
    }

    #[test]
    fn turn_start_params_round_trips_future_acp_execution_backend() {
        let params: TurnStartParams = serde_json::from_value(json!({
            "thread_id": "thr_123",
            "turn_id": "turn_123",
            "execution_backend": {
                "type": "acpAgentRuntime",
                "runtime_id": "acp_local"
            }
        }))
        .expect("params should decode");

        assert_eq!(
            params.execution_backend,
            Some(AgentExecutionBackend::ACPAgentRuntime {
                runtime_id: "acp_local".to_owned()
            })
        );
    }

    #[test]
    fn turn_start_params_encode_user_input_tagged_enum() {
        let params = TurnStartParams {
            thread_id: "thr_123".to_owned(),
            turn_id: "turn_123".to_owned(),
            input: vec![UserInput::Image {
                url: "https://example.com/image.png".to_owned(),
            }],
            capabilities: Vec::new(),
            model: None,
            model_provider: None,
            sandbox_policy: None,
            mode: None,
            execution_backend: None,
            reasoning: None,
            permission_profile: None,
            cli_runtime_options: None,
        };

        let encoded = serde_json::to_value(params).expect("params should encode");
        assert_eq!(
            encoded,
            json!({
                "thread_id": "thr_123",
                "turn_id": "turn_123",
                "input": [
                    {
                        "type": "image",
                        "url": "https://example.com/image.png"
                    }
                ]
            })
        );
    }

    #[test]
    fn turn_cancel_params_roundtrip_optional_reason() {
        let params: TurnCancelParams = serde_json::from_value(json!({
            "thread_id": "thr_123",
            "turn_id": "turn_123",
            "reason": "user clicked stop"
        }))
        .expect("params should decode");

        assert_eq!(params.thread_id, "thr_123");
        assert_eq!(params.turn_id, "turn_123");
        assert_eq!(params.reason.as_deref(), Some("user clicked stop"));

        let encoded = serde_json::to_value(TurnCancelParams {
            thread_id: "thr_123".to_owned(),
            turn_id: "turn_123".to_owned(),
            reason: None,
        })
        .expect("params should encode");
        assert_eq!(
            encoded,
            json!({
                "thread_id": "thr_123",
                "turn_id": "turn_123"
            })
        );
    }

    #[test]
    fn turn_start_params_encode_extended_attachment_input_variants() {
        let params = TurnStartParams {
            thread_id: "thr_123".to_owned(),
            turn_id: "turn_123".to_owned(),
            input: vec![
                UserInput::File {
                    url: "https://example.com/file.pdf".to_owned(),
                },
                UserInput::LocalFile {
                    path: "/tmp/file.pdf".to_owned(),
                },
                UserInput::Audio {
                    url: "https://example.com/sample.mp3".to_owned(),
                },
                UserInput::LocalAudio {
                    path: "/tmp/sample.wav".to_owned(),
                },
                UserInput::Video {
                    url: "https://example.com/clip.mp4".to_owned(),
                },
                UserInput::LocalVideo {
                    path: "/tmp/clip.mp4".to_owned(),
                },
                UserInput::Artifact {
                    artifact_id: "art_123".to_owned(),
                    version_id: Some("av_123".to_owned()),
                },
            ],
            capabilities: Vec::new(),
            model: None,
            model_provider: None,
            sandbox_policy: None,
            mode: None,
            execution_backend: None,
            reasoning: None,
            permission_profile: None,
            cli_runtime_options: None,
        };

        let encoded = serde_json::to_value(params).expect("params should encode");
        assert_eq!(
            encoded,
            json!({
                "thread_id": "thr_123",
                "turn_id": "turn_123",
                "input": [
                    { "type": "file", "url": "https://example.com/file.pdf" },
                    { "type": "localFile", "path": "/tmp/file.pdf" },
                    { "type": "audio", "url": "https://example.com/sample.mp3" },
                    { "type": "localAudio", "path": "/tmp/sample.wav" },
                    { "type": "video", "url": "https://example.com/clip.mp4" },
                    { "type": "localVideo", "path": "/tmp/clip.mp4" },
                    { "type": "artifact", "artifactId": "art_123", "versionId": "av_123" }
                ]
            })
        );
    }

    #[test]
    fn turn_start_params_round_trips_permission_profile_selection() {
        let params: TurnStartParams = serde_json::from_value(json!({
            "thread_id": "thr_123",
            "turn_id": "turn_123",
            "permission_profile": {
                "mode": "supervised"
            }
        }))
        .expect("params should decode");

        assert_eq!(
            params.permission_profile,
            Some(TurnPermissionProfileSelection {
                mode: TurnPermissionMode::Supervised
            })
        );

        let encoded = serde_json::to_value(params).expect("params should encode");
        assert_eq!(
            encoded["permission_profile"],
            json!({ "mode": "supervised" })
        );
    }

    #[test]
    fn turn_payload_requires_permission_profile_snapshot() {
        let error = serde_json::from_value::<Turn>(json!({
            "id": "turn_123",
            "status": "InProgress"
        }))
        .expect_err("turn payload without permission profile should fail");
        assert!(
            error.to_string().contains("permission_profile"),
            "unexpected error: {error}"
        );

        let turn: Turn = serde_json::from_value(json!({
            "id": "turn_123",
            "status": "InProgress",
            "permission_profile": {
                "mode": "full_access",
                "source": "defaulted",
                "effective_policy": {
                    "default_behavior": "allow",
                    "file_read": "allow",
                    "file_write": "allow",
                    "shell_command": "allow",
                    "network": "allow",
                    "mcp_read": "allow",
                    "mcp_write_or_unknown": "allow",
                    "dynamic_skill_tool": "allow",
                    "computer_use": "allow",
                    "task_subagent": "allow"
                }
            }
        }))
        .expect("turn payload with permission profile should deserialize");
        assert_eq!(turn.id, "turn_123");
        assert_eq!(turn.status, TurnStatus::InProgress);
        assert_eq!(turn.turn_kind, TurnKind::Conversation);
        assert_eq!(turn.origin, TurnOrigin::User);
        assert_eq!(turn.permission_profile.mode, TurnPermissionMode::FullAccess);
        assert_eq!(
            turn.permission_profile.source,
            TurnPermissionProfileSource::Defaulted
        );
    }

    #[test]
    fn turn_start_params_round_trips_capabilities() {
        let params: TurnStartParams = serde_json::from_value(json!({
            "thread_id": "thr_123",
            "turn_id": "turn_123",
            "input": [],
            "capabilities": [
                {
                    "id": "skill:mvg02zVNGWuw5z5C4nYDo",
                    "label": "imagegen",
                    "kind": {
                        "type": "skill",
                        "skillId": "mvg02zVNGWuw5z5C4nYDo"
                    }
                },
                {
                    "id": "mcp-server:workspace:browser",
                    "label": "browser",
                    "kind": {
                        "type": "mcpServer",
                        "name": "browser",
                        "scopeKind": "workspace"
                    }
                },
                {
                    "id": "mcp-tool:workspace:browser:open",
                    "label": "browser/open",
                    "kind": {
                        "type": "mcpTool",
                        "serverName": "browser",
                        "rawToolName": "open",
                        "scopeKind": "workspace"
                    }
                }
            ]
        }))
        .expect("params should decode");

        assert_eq!(params.capabilities.len(), 3);
        assert_eq!(
            params.capabilities[0],
            TurnCapability {
                id: "skill:mvg02zVNGWuw5z5C4nYDo".to_owned(),
                label: Some("imagegen".to_owned()),
                kind: TurnCapabilityKind::Skill {
                    skill_id: "mvg02zVNGWuw5z5C4nYDo".parse().expect("valid skill id"),
                    pack_id: None,
                },
            }
        );
        assert_eq!(
            params.capabilities[1].kind,
            TurnCapabilityKind::McpServer {
                name: "browser".to_owned(),
                scope_kind: McpScopeKind::Workspace,
            }
        );
        assert_eq!(
            params.capabilities[2].kind,
            TurnCapabilityKind::McpTool {
                server_name: "browser".to_owned(),
                raw_tool_name: "open".to_owned(),
                scope_kind: McpScopeKind::Workspace,
            }
        );

        let encoded = serde_json::to_value(params).expect("params should encode");
        assert_eq!(encoded["capabilities"][0]["kind"]["type"], "skill");
        assert_eq!(encoded["capabilities"][1]["kind"]["type"], "mcpServer");
        assert_eq!(encoded["capabilities"][2]["kind"]["type"], "mcpTool");
    }

    #[test]
    fn skill_capability_uses_exact_validated_id_and_canonical_key() {
        let skill_id: SkillId = "mvg02zVNGWuw5z5C4nYDo".parse().expect("valid skill id");
        assert_eq!(
            skill_capability_key(&skill_id),
            "skill:mvg02zVNGWuw5z5C4nYDo"
        );

        let missing_id = serde_json::from_value::<TurnCapabilityKind>(json!({
            "type": "skill"
        }));
        assert!(
            missing_id.is_err(),
            "skill capability must require an exact id"
        );

        let invalid = serde_json::from_value::<TurnCapabilityKind>(json!({
            "type": "skill",
            "skillId": "too-short"
        }));
        assert!(invalid.is_err(), "invalid skill id must be rejected");

        let mismatched_key = serde_json::from_value::<TurnCapability>(json!({
            "id": "skill:AAAAAAAAAAAAAAAAAAAAA",
            "kind": {
                "type": "skill",
                "skillId": "mvg02zVNGWuw5z5C4nYDo"
            }
        }));
        assert!(
            mismatched_key.is_err(),
            "skill capability key must be derived from the exact skill id"
        );

        let old_standalone: TurnCapability = serde_json::from_value(json!({
            "id": "skill:mvg02zVNGWuw5z5C4nYDo",
            "kind": {
                "type": "skill",
                "skillId": "mvg02zVNGWuw5z5C4nYDo"
            }
        }))
        .expect("legacy skill capability without packId should decode");
        assert_eq!(
            old_standalone.kind,
            TurnCapabilityKind::Skill {
                skill_id,
                pack_id: None,
            }
        );

        let capability_schema = serde_json::to_value(schemars::schema_for!(TurnCapabilityKind))
            .expect("capability schema should encode");
        let capability_schema = capability_schema.to_string();
        assert!(capability_schema.contains("skillId"));
        assert!(capability_schema.contains("^[A-Za-z0-9]{21}$"));
        assert!(!capability_schema.contains("sourceKind\":{\"type\":\"string\"}"));
    }

    #[test]
    fn skill_pack_capability_requires_canonical_key_and_preserves_child_context() {
        let pack_id: SkillPackId = "mvg02zVNGWuw5z5C4nYDo".parse().expect("valid pack id");
        assert_eq!(
            skill_pack_capability_key(&pack_id),
            "skill_pack:mvg02zVNGWuw5z5C4nYDo"
        );

        let full_pack: TurnCapability = serde_json::from_value(json!({
            "id": "skill_pack:mvg02zVNGWuw5z5C4nYDo",
            "kind": {
                "type": "skillPack",
                "packId": "mvg02zVNGWuw5z5C4nYDo"
            }
        }))
        .expect("full pack capability should decode");
        assert_eq!(
            full_pack.kind,
            TurnCapabilityKind::SkillPack {
                pack_id: pack_id.clone(),
            }
        );

        let packed_child: TurnCapability = serde_json::from_value(json!({
            "id": "skill:mvg02zVNGWuw5z5C4nYDo",
            "kind": {
                "type": "skill",
                "skillId": "mvg02zVNGWuw5z5C4nYDo",
                "packId": "mvg02zVNGWuw5z5C4nYDo"
            }
        }))
        .expect("packed child capability should decode");
        assert_eq!(packed_child.id, "skill:mvg02zVNGWuw5z5C4nYDo");
        assert_eq!(
            packed_child.kind,
            TurnCapabilityKind::Skill {
                skill_id: "mvg02zVNGWuw5z5C4nYDo".parse().expect("valid skill id"),
                pack_id: Some(pack_id),
            }
        );

        let mismatched_key = serde_json::from_value::<TurnCapability>(json!({
            "id": "skill:mvg02zVNGWuw5z5C4nYDo",
            "kind": {
                "type": "skillPack",
                "packId": "mvg02zVNGWuw5z5C4nYDo"
            }
        }));
        assert!(mismatched_key.is_err());

        let missing_pack_id = serde_json::from_value::<TurnCapabilityKind>(json!({
            "type": "skillPack"
        }));
        assert!(missing_pack_id.is_err());

        let invalid_pack_id = serde_json::from_value::<TurnCapabilityKind>(json!({
            "type": "skillPack",
            "packId": "too-short"
        }));
        assert!(invalid_pack_id.is_err());
    }

    #[test]
    fn user_input_artifact_round_trips_with_optional_version() {
        let input: UserInput = serde_json::from_value(json!({
            "type": "artifact",
            "artifactId": "art_123",
            "versionId": "av_123"
        }))
        .expect("artifact input should decode");

        assert_eq!(
            input,
            UserInput::Artifact {
                artifact_id: "art_123".to_owned(),
                version_id: Some("av_123".to_owned())
            }
        );

        let encoded = serde_json::to_value(UserInput::Artifact {
            artifact_id: "art_123".to_owned(),
            version_id: None,
        })
        .expect("artifact input should encode");

        assert_eq!(
            encoded,
            json!({
                "type": "artifact",
                "artifactId": "art_123"
            })
        );
    }

    #[test]
    fn user_message_attachment_artifact_round_trips() {
        let attachment = UserMessageAttachment::Artifact {
            artifact: ArtifactRef {
                artifact_id: "art_123".to_owned(),
                version_id: Some("av_123".to_owned()),
                display_name: "report.pdf".to_owned(),
                kind: crate::ArtifactKind::Pdf,
                mime_type: Some("application/pdf".to_owned()),
                size_bytes: Some(42),
                sha256: Some("a".repeat(64)),
                status: crate::ArtifactStatus::Ready,
                preview: None,
            },
        };

        let encoded = serde_json::to_value(&attachment).expect("attachment should encode");
        assert_eq!(encoded["type"], json!("artifact"));
        assert_eq!(encoded["artifact"]["artifact_id"], json!("art_123"));
        assert_eq!(encoded["artifact"]["version_id"], json!("av_123"));

        let decoded: UserMessageAttachment =
            serde_json::from_value(encoded).expect("attachment should decode");
        assert_eq!(decoded, attachment);
    }

    #[test]
    fn user_message_attachment_capabilities_round_trip_in_single_attachment_list() {
        let attachments = vec![
            UserMessageAttachment::Skill {
                capability: TurnSkillCapabilitySummary {
                    skill_id: "mvg02zVNGWuw5z5C4nYDo".parse().expect("valid skill id"),
                    label: "docs".to_owned(),
                    owner: Some("pioneer".to_owned()),
                    slug: "docs".to_owned(),
                    source_kind: "user".to_owned(),
                    pack: Some(TurnSkillPackPresentationSummary {
                        pack_id: "mvg02zVNGWuw5z5C4nYDo".parse().expect("valid pack id"),
                        label: "writer-pack".to_owned(),
                    }),
                },
            },
            UserMessageAttachment::SkillPack {
                capability: TurnSkillPackCapabilitySummary {
                    pack_id: "mvg02zVNGWuw5z5C4nYDo".parse().expect("valid pack id"),
                    label: "writer-pack".to_owned(),
                },
            },
            UserMessageAttachment::McpServer {
                capability: TurnMcpServerCapabilitySummary {
                    id: "mcp-server:workspace:resend".to_owned(),
                    label: "resend".to_owned(),
                    name: "resend".to_owned(),
                    scope_kind: McpScopeKind::Workspace,
                },
            },
            UserMessageAttachment::McpTool {
                capability: TurnMcpToolCapabilitySummary {
                    id: "mcp-tool:workspace:resend:send".to_owned(),
                    label: "resend/send".to_owned(),
                    server_name: "resend".to_owned(),
                    raw_tool_name: "send".to_owned(),
                    scope_kind: McpScopeKind::Workspace,
                },
            },
        ];

        let item = TurnItem::UserMessage {
            id: "user_msg_1".to_owned(),
            text: "send it".to_owned(),
            attachments,
        };

        let encoded = serde_json::to_value(&item).expect("user message should encode");
        assert_eq!(encoded["attachments"][0]["type"], json!("skill"));
        assert_eq!(
            encoded["attachments"][0]["capability"]["pack"]["label"],
            json!("writer-pack")
        );
        assert_eq!(encoded["attachments"][1]["type"], json!("skillPack"));
        assert_eq!(encoded["attachments"][2]["type"], json!("mcpServer"));
        assert_eq!(encoded["attachments"][3]["type"], json!("mcpTool"));
        let mut keys = encoded
            .as_object()
            .expect("encoded user message should be an object")
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>();
        keys.sort_unstable();
        assert_eq!(keys, vec!["attachments", "id", "text", "type"]);

        let decoded: TurnItem = serde_json::from_value(encoded).expect("item should decode");
        assert_eq!(decoded, item);
    }

    #[test]
    fn skill_attachment_requires_exact_skill_id() {
        let without_id = serde_json::from_value::<UserMessageAttachment>(json!({
            "type": "skill",
            "capability": {
                "label": "docs",
                "slug": "docs",
                "sourceKind": "user"
            }
        }));
        assert!(
            without_id.is_err(),
            "skill attachment must require an exact id"
        );

        let summary_schema =
            serde_json::to_value(schemars::schema_for!(TurnSkillCapabilitySummary))
                .expect("summary schema should encode")
                .to_string();
        assert!(summary_schema.contains("skillId"));
        assert!(summary_schema.contains("owner"));
        assert!(!summary_schema.contains("\"id\""));

        let old_attachment: UserMessageAttachment = serde_json::from_value(json!({
            "type": "skill",
            "capability": {
                "skillId": "mvg02zVNGWuw5z5C4nYDo",
                "label": "docs",
                "slug": "docs",
                "sourceKind": "user"
            }
        }))
        .expect("historical standalone skill attachment without pack should decode");
        let UserMessageAttachment::Skill { capability } = old_attachment else {
            panic!("expected skill attachment");
        };
        assert_eq!(capability.pack, None);
    }

    #[test]
    fn turn_start_params_round_trip_turn_id() {
        let params: TurnStartParams = serde_json::from_value(json!({
            "thread_id": "thr_123",
            "turn_id": "turn_abc"
        }))
        .expect("params should decode");
        assert_eq!(params.turn_id, "turn_abc");

        let encoded = serde_json::to_value(params).expect("params should encode");
        assert_eq!(
            encoded,
            json!({"thread_id": "thr_123", "turn_id": "turn_abc", "input": []})
        );
    }

    #[test]
    fn turn_start_params_require_turn_id() {
        let error = serde_json::from_value::<TurnStartParams>(json!({
            "thread_id": "thr_123",
            "input": []
        }))
        .expect_err("turn_id is required");
        assert!(error.to_string().contains("turn_id"));
    }

    #[test]
    fn turn_item_type_is_tool_item_matches_expected_variants() {
        assert!(TurnItemType::CommandExecution.is_tool_item());
        assert!(TurnItemType::DynamicToolCall.is_tool_item());
        assert!(!TurnItemType::AgentMessage.is_tool_item());
        assert!(!TurnItemType::SystemEvent.is_tool_item());
    }

    #[test]
    fn tool_recovery_policy_snapshot_uses_snake_case_enum_wire_values() {
        let snapshot = sample_snapshot();
        let value = serde_json::to_value(snapshot).expect("snapshot should serialize");

        assert_eq!(value["retryClass"], "network");
        assert_eq!(value["idempotencyMode"], "requires_key");
        assert_eq!(value["resolvedAction"], "retry_with_backoff");

        let decoded: ToolRecoveryPolicySnapshot =
            serde_json::from_value(value).expect("snapshot should deserialize");
        assert_eq!(decoded, sample_snapshot());
    }

    #[test]
    fn every_tool_turn_item_variant_carries_recovery_policy() {
        for item in sample_tool_items() {
            let value = serde_json::to_value(&item).expect("tool item should serialize");
            assert!(
                value.get("recoveryPolicy").is_some(),
                "missing recoveryPolicy in {value:?}"
            );
            assert!(
                value.get("outputPolicy").is_some(),
                "missing outputPolicy in {value:?}"
            );
            assert!(
                value.get("outputJson").is_none(),
                "tool item must not expose outputJson in {value:?}"
            );
            let decoded: TurnItem = serde_json::from_value(value).expect("tool item should decode");
            assert_eq!(decoded.recovery_policy(), Some(&sample_snapshot()));
        }
    }

    #[test]
    fn tool_output_policy_snapshot_round_trips() {
        let snapshot = ToolOutputPolicySnapshot::for_tool_name("web_fetch");
        let value = serde_json::to_value(&snapshot).expect("policy should serialize");
        assert_eq!(value["storage"]["mode"], "metadata_only");
        assert_eq!(value["deltas"]["mode"], "progress_only");

        let decoded: ToolOutputPolicySnapshot =
            serde_json::from_value(value).expect("policy should deserialize");
        assert_eq!(decoded, snapshot);
    }

    #[test]
    fn external_runtime_tool_policy_keeps_timeline_summary_without_raw_storage() {
        let snapshot =
            ToolOutputPolicySnapshot::for_external_runtime_tool_name("external:custom-action");
        assert!(matches!(
            &snapshot.timeline,
            TimelineOutputPolicy::Summary { .. }
        ));
        assert_eq!(snapshot.storage, StorageOutputPolicy::MetadataOnly);
        assert_eq!(snapshot.recovery, RecoveryOutputPolicy::MetadataOnly);
        assert_eq!(snapshot.deltas, DeltaOutputPolicy::ProgressOnly);

        let unknown_snapshot = ToolOutputPolicySnapshot::for_tool_name("external:custom-action");
        assert_eq!(
            unknown_snapshot.timeline,
            TimelineOutputPolicy::MetadataOnly
        );
        assert_eq!(unknown_snapshot.storage, StorageOutputPolicy::MetadataOnly);
    }

    #[test]
    fn tool_display_and_storage_payloads_round_trip() {
        let display = ToolDisplayPayload::Summary(ToolOutputSummary {
            title: "Read crates/tools/src/runtime.rs".to_owned(),
            lines: vec!["Read lines 240-310".to_owned(), "71 lines".to_owned()],
            metadata: ToolMetadata::from_json(json!({
                "path": "crates/tools/src/runtime.rs",
                "lineStart": 240,
                "lineEnd": 310,
                "bytes": 3890
            })),
            truncated: false,
        });
        let storage = ToolStoragePayload::Metadata {
            metadata: ToolMetadata::from_json(json!({
                "path": "crates/tools/src/runtime.rs",
                "contentHash": "sha256:test"
            })),
        };
        let recovery = ToolRecoveryView {
            error_class: Some("timeout".to_owned()),
            retry_hint: Some("retry".to_owned()),
            incomplete_reason: Some("tool timed out".to_owned()),
            diagnostic_summary: Some("stderr contained timeout".to_owned()),
            diagnostic_excerpt: Some("deadline exceeded".to_owned()),
            output_fingerprint: Some("sha256:output".to_owned()),
            content_fingerprint: Some("sha256:content".to_owned()),
            was_truncated: true,
            continuation: Some(json!({"sessionId": 7})),
        };

        let display_value = serde_json::to_value(&display).expect("display should serialize");
        let storage_value = serde_json::to_value(&storage).expect("storage should serialize");
        let recovery_value = serde_json::to_value(&recovery).expect("recovery should serialize");
        assert_eq!(
            serde_json::from_value::<ToolDisplayPayload>(display_value)
                .expect("display should deserialize"),
            display
        );
        assert_eq!(
            serde_json::from_value::<ToolStoragePayload>(storage_value)
                .expect("storage should deserialize"),
            storage
        );
        assert_eq!(
            serde_json::from_value::<ToolRecoveryView>(recovery_value)
                .expect("recovery should deserialize"),
            recovery
        );
    }

    #[test]
    fn tool_metadata_redacts_raw_like_fields_by_construction() {
        let metadata = ToolMetadata::from_json(json!({
            "url": "https://example.com",
            "body": "SECRET_BODY",
            "nested": {
                "base64": "SECRET_BLOB"
            }
        }));

        assert_eq!(
            metadata.get("url").and_then(ToolMetadataValue::as_str),
            Some("https://example.com")
        );
        assert!(matches!(
            metadata.get("body"),
            Some(ToolMetadataValue::RedactedRaw {
                raw_kind: ToolMetadataRawKind::Body,
                ..
            })
        ));
        let serialized = serde_json::to_string(&metadata).expect("metadata should serialize");
        assert!(!serialized.contains("SECRET_BODY"));
        assert!(!serialized.contains("SECRET_BLOB"));
    }

    #[test]
    fn tool_metadata_to_json_preserves_integer_precision() {
        let value = "9007199254740993";
        let metadata_value = ToolMetadataValue::Number {
            value: value.to_owned(),
        };

        assert_eq!(metadata_value.to_json(), json!(9007199254740993_u64));
    }

    #[test]
    fn generated_schema_documents_cover_typed_tool_output_contract() {
        let documents = crate::protocol_schema_documents();
        let schema_names = documents
            .iter()
            .map(|document| document.file_name)
            .collect::<Vec<_>>();

        for expected in [
            "tool_output_policy_snapshot.json",
            "tool_metadata.json",
            "tool_metadata_value.json",
            "tool_metadata_raw_kind.json",
            "tool_output_summary.json",
            "tool_display_payload.json",
            "tool_storage_payload.json",
            "tool_recovery_view.json",
        ] {
            assert!(
                schema_names.iter().any(|name| *name == expected),
                "missing schema document {expected}"
            );
        }

        let turn_item_schema = documents
            .iter()
            .find(|document| document.file_name == "turn_item.json")
            .expect("turn_item schema should be exported");
        let schema_json = serde_json::to_string(&turn_item_schema.schema)
            .expect("turn_item schema should serialize");
        for expected_field in ["outputPolicy", "display", "storage", "recovery"] {
            assert!(
                schema_json.contains(expected_field),
                "turn_item schema should include {expected_field}"
            );
        }
        assert!(
            !schema_json.contains("outputJson") && !schema_json.contains("output_json"),
            "turn_item schema must not expose generic raw output_json"
        );
    }

    #[test]
    fn generated_schema_documents_cover_execution_security_snapshot_contract() {
        let documents = crate::protocol_schema_documents();
        let schema_names = documents
            .iter()
            .map(|document| document.file_name)
            .collect::<Vec<_>>();

        for expected in [
            "turn_execution_security_snapshot.json",
            "turn_security_snapshot_source.json",
            "turn_sandbox_snapshot.json",
            "turn_sandbox_mode.json",
            "turn_filesystem_sandbox_policy.json",
            "turn_filesystem_sandbox_kind.json",
            "turn_filesystem_sandbox_entry.json",
            "turn_filesystem_sandbox_path.json",
            "turn_filesystem_access.json",
            "turn_network_policy_snapshot.json",
            "turn_network_mode.json",
            "turn_process_policy_snapshot.json",
            "turn_shell_policy.json",
            "turn_environment_policy.json",
            "turn_process_timeout_policy.json",
            "turn_command_risk_policy.json",
            "turn_approval_scope_policy_snapshot.json",
            "turn_security_backend_snapshot.json",
            "turn_security_execution_backend_kind.json",
            "sandbox_backend_kind.json",
            "sandbox_backend_requirement.json",
            "backend_security_capabilities.json",
            "turn_security_enforcement_status.json",
            "turn_security_degradation.json",
            "turn_security_capability_kind.json",
            "turn_security_parent_cap_snapshot.json",
        ] {
            assert!(
                schema_names.iter().any(|name| *name == expected),
                "missing schema document {expected}"
            );
        }

        let snapshot_schema = documents
            .iter()
            .find(|document| document.file_name == "turn_execution_security_snapshot.json")
            .expect("security snapshot schema should be exported");
        let schema_json = serde_json::to_string(&snapshot_schema.schema)
            .expect("security snapshot schema should serialize");
        for expected_field in [
            "schema_version",
            "version",
            "source",
            "permission_profile",
            "sandbox",
            "process",
            "network",
            "approval",
            "backend",
            "enforcement",
            "created_at_unix_ms",
        ] {
            assert!(
                schema_json.contains(expected_field),
                "security snapshot schema should include {expected_field}"
            );
        }
    }

    #[test]
    fn non_tool_turn_items_do_not_require_recovery_policy() {
        let item = TurnItem::AgentMessage {
            id: "agent_1".to_owned(),
            text: "done".to_owned(),
            phase: Default::default(),
            markdown: None,
            markdown_version: None,
        };

        let value = serde_json::to_value(&item).expect("agent item should serialize");
        assert!(value.get("recoveryPolicy").is_none());
        assert!(
            value.get("phase").is_none(),
            "default final_answer phase should preserve the legacy wire shape"
        );
        let decoded: TurnItem = serde_json::from_value(value).expect("agent item should decode");
        assert_eq!(decoded.recovery_policy(), None);
        let TurnItem::AgentMessage { phase, .. } = decoded else {
            panic!("expected agent message");
        };
        assert_eq!(phase, AgentMessagePhase::FinalAnswer);
    }

    fn sample_snapshot() -> ToolRecoveryPolicySnapshot {
        ToolRecoveryPolicySnapshot {
            retry_class: ToolRecoveryRetryClass::Network,
            idempotency_mode: ToolRecoveryIdempotencyMode::RequiresKey,
            max_attempts: 4,
            can_resume: true,
            resolved_action: RecoveryAction::RetryWithBackoff,
            base_backoff_secs: 3,
            max_wall_clock_secs: 240,
            no_progress_limit: 3,
        }
    }

    fn sample_tool_items() -> Vec<TurnItem> {
        let recovery_policy = Some(sample_snapshot());
        let recovery = Some(ToolRecoveryView {
            error_class: None,
            retry_hint: None,
            incomplete_reason: None,
            diagnostic_summary: Some("sample".to_owned()),
            diagnostic_excerpt: None,
            output_fingerprint: None,
            content_fingerprint: None,
            was_truncated: false,
            continuation: None,
        });
        vec![
            TurnItem::CommandExecution {
                id: "cmd_1".to_owned(),
                tool_name: "exec_command".to_owned(),
                arguments: json!({}),
                status: ToolCallStatus::InProgress,
                recovery_policy: recovery_policy.clone(),
                output_policy: ToolOutputPolicySnapshot::for_tool_name("exec_command"),
                display: ToolDisplayPayload::default(),
                storage: ToolStoragePayload::default(),
                recovery: recovery.clone(),
                command: Vec::new(),
                cwd: None,
                success: None,
                outcome: None,
                observation: None,
            },
            TurnItem::FileChange {
                id: "file_1".to_owned(),
                tool_name: "apply_patch".to_owned(),
                arguments: json!({}),
                status: ToolCallStatus::InProgress,
                recovery_policy: recovery_policy.clone(),
                output_policy: ToolOutputPolicySnapshot::for_tool_name("apply_patch"),
                display: ToolDisplayPayload::default(),
                storage: ToolStoragePayload::default(),
                recovery: recovery.clone(),
                changed_files: Vec::new(),
                exit_code: None,
                stdout: None,
                stderr: None,
                success: None,
                outcome: None,
                observation: None,
            },
            TurnItem::WebSearch {
                id: "search_1".to_owned(),
                tool_name: "web_search".to_owned(),
                arguments: json!({}),
                status: ToolCallStatus::InProgress,
                recovery_policy: recovery_policy.clone(),
                output_policy: ToolOutputPolicySnapshot::for_tool_name("web_search"),
                display: ToolDisplayPayload::default(),
                storage: ToolStoragePayload::default(),
                recovery: recovery.clone(),
                query: None,
                provider: None,
                took_ms: None,
                result_count: None,
                results: Vec::new(),
                success: None,
                outcome: None,
                observation: None,
            },
            TurnItem::WebFetch {
                id: "fetch_1".to_owned(),
                tool_name: "web_fetch".to_owned(),
                arguments: json!({}),
                status: ToolCallStatus::InProgress,
                recovery_policy: recovery_policy.clone(),
                output_policy: ToolOutputPolicySnapshot::for_tool_name("web_fetch"),
                display: ToolDisplayPayload::default(),
                storage: ToolStoragePayload::default(),
                recovery: recovery.clone(),
                url: None,
                final_url: None,
                status_code: None,
                content_type: None,
                extract_mode: None,
                resolved_mode: None,
                bytes_received: None,
                elapsed_ms: None,
                truncated: None,
                title: None,
                word_count: None,
                links: Vec::new(),
                success: None,
                outcome: None,
                observation: None,
            },
            TurnItem::Download {
                id: "download_1".to_owned(),
                tool_name: "download".to_owned(),
                arguments: json!({}),
                status: ToolCallStatus::InProgress,
                recovery_policy: recovery_policy.clone(),
                output_policy: ToolOutputPolicySnapshot::for_tool_name("download"),
                display: ToolDisplayPayload::default(),
                storage: ToolStoragePayload::default(),
                recovery: recovery.clone(),
                url: None,
                final_url: None,
                status_code: None,
                path: None,
                bytes_written: None,
                sha256: None,
                content_type: None,
                elapsed_ms: None,
                truncated: None,
                success: None,
                outcome: None,
                observation: None,
            },
            TurnItem::DynamicToolCall {
                id: "dynamic_1".to_owned(),
                tool_name: "dynamic".to_owned(),
                arguments: json!({}),
                status: ToolCallStatus::InProgress,
                recovery_policy,
                output_policy: ToolOutputPolicySnapshot::for_tool_name("dynamic"),
                display: ToolDisplayPayload::default(),
                storage: ToolStoragePayload::default(),
                recovery,
                success: None,
                outcome: None,
                observation: None,
            },
        ]
    }
}
