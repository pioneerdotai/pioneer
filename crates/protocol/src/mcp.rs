use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

fn default_true() -> bool {
    true
}

fn default_false() -> bool {
    false
}

fn default_workspace_scope() -> McpScopeKind {
    McpScopeKind::Workspace
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct McpListParams {
    pub workspace_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct McpListResponse {
    pub snapshot_version: u64,
    pub generated_at: i64,
    #[serde(default)]
    pub servers: Vec<McpListItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct McpInstallParams {
    pub workspace_id: String,
    pub config_json: String,
    #[serde(default = "default_workspace_scope")]
    pub scope_kind: McpScopeKind,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_false")]
    pub allow_implicit_invocation: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct McpInstallResponse {
    pub status: McpInstallStatus,
    #[serde(default)]
    pub servers: Vec<McpInstallResult>,
    pub audit: McpLifecycleAuditSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct McpInstallResult {
    pub name: String,
    pub status: McpInstallResultStatus,
    #[serde(default)]
    pub diagnostics: Vec<McpValidationDiagnostic>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server: Option<McpListItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct McpListItem {
    pub id: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    pub scope: McpScopeKind,
    pub policy: McpPolicyState,
    pub required: bool,
    pub runtime: McpRuntimeStatus,
    pub tools_count: usize,
    pub resources_count: usize,
    pub resource_templates_count: usize,
    pub prompts_count: usize,
    pub status: McpServerStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct McpPolicyState {
    pub enabled: bool,
    pub allow_implicit_invocation: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct McpPolicySetParams {
    pub workspace_id: String,
    pub name: String,
    #[serde(default = "default_workspace_scope")]
    pub scope_kind: McpScopeKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allow_implicit_invocation: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct McpPolicySetResponse {
    pub policy: McpServerPolicy,
    pub server: McpListItem,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct McpServerRestartParams {
    pub workspace_id: String,
    pub name: String,
    #[serde(default = "default_workspace_scope")]
    pub scope_kind: McpScopeKind,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct McpServerRestartResponse {
    pub accepted: bool,
    pub server: McpListItem,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct McpUninstallParams {
    pub workspace_id: String,
    pub name: String,
    #[serde(default = "default_workspace_scope")]
    pub scope_kind: McpScopeKind,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct McpUninstallResponse {
    pub removed: bool,
    pub server_id: String,
    pub name: String,
    pub scope_kind: McpScopeKind,
    pub audit: McpLifecycleAuditSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct McpServerDetailsParams {
    pub workspace_id: String,
    pub server_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct McpServerDetailsResponse {
    pub snapshot_version: u64,
    pub generated_at: i64,
    pub server: McpListItem,
    pub catalog: McpServerCatalogDetails,
    /// Management-only configuration and diagnostics. Omitted unless the
    /// caller may manage the selected MCP installation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub management: Option<McpManagementDetails>,
}

/// Configuration and diagnostics intentionally excluded from the operational
/// MCP capability returned by discovery endpoints.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct McpManagementDetails {
    pub scope: McpScopeKind,
    pub source_kind: McpSourceKind,
    pub transport: McpTransportSummary,
    pub fingerprint: String,
    pub health: McpServerHealthDetails,
    #[serde(default)]
    pub audit: Vec<McpAuditEventSummary>,
    #[serde(default)]
    pub recent_bindings: Vec<McpTurnBindingSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct McpServerCatalogDetails {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generated_at: Option<i64>,
    #[serde(default)]
    pub server_info: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_instructions_hash: Option<String>,
    #[serde(default)]
    pub tools: Vec<McpToolCatalogItem>,
    #[serde(default)]
    pub resources: Vec<McpResourceCatalogItem>,
    #[serde(default)]
    pub resource_templates: Vec<McpResourceTemplateCatalogItem>,
    #[serde(default)]
    pub prompts: Vec<McpPromptCatalogItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct McpToolCatalogItem {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_schema_summary: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub annotations: Option<McpToolAnnotationSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct McpToolAnnotationSummary {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_only_hint: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destructive_hint: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotent_hint: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub open_world_hint: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct McpResourceCatalogItem {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uri: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct McpResourceTemplateCatalogItem {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uri_template: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct McpPromptCatalogItem {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub arguments_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct McpServerHealthDetails {
    pub runtime: McpRuntimeStatus,
    pub status: McpServerStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_attempt: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_retry_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stderr_tail: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct McpAuditEventSummary {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_installation_id: Option<String>,
    pub server_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_tool_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub callable_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog_version: Option<String>,
    pub action: String,
    pub decision: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason_code: Option<String>,
    #[serde(default)]
    pub details: serde_json::Value,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct McpTurnBindingSummary {
    pub server_installation_id: String,
    pub server_name: String,
    pub raw_tool_name: String,
    pub callable_name: String,
    pub catalog_version: String,
    pub fingerprint: String,
    pub selection_reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capability_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct McpServerPolicy {
    pub workspace_id: String,
    pub name: String,
    pub scope_kind: McpScopeKind,
    pub enabled: bool,
    pub allow_implicit_invocation: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum McpTransportSummary {
    Stdio { command: String },
    StreamableHttp { url: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct McpRuntimeStatus {
    pub state: McpRuntimeState,
    pub live: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_seen_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct McpValidationDiagnostic {
    pub code: String,
    pub level: McpDiagnosticLevel,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct McpLifecycleAuditSummary {
    pub events_written: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct McpChangedNotification {
    pub workspace_id: String,
    pub snapshot_version: u64,
    #[serde(default)]
    pub changed: Vec<McpChangedItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct McpChangedItem {
    pub name: String,
    pub source_kind: McpSourceKind,
    pub action: McpChangedAction,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct McpServerStatusChangedNotification {
    pub workspace_id: String,
    pub snapshot_version: u64,
    pub server: McpServerStatusItem,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct McpServerStatusItem {
    pub id: String,
    pub name: String,
    pub scope_kind: McpScopeKind,
    pub runtime: McpRuntimeStatus,
    pub status: McpServerStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct McpServerCatalogChangedNotification {
    pub workspace_id: String,
    pub snapshot_version: u64,
    pub server_id: String,
    pub name: String,
    pub catalog_version: String,
    pub tools_count: usize,
    pub resources_count: usize,
    pub resource_templates_count: usize,
    pub prompts_count: usize,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum McpScopeKind {
    Workspace,
    User,
}

impl McpScopeKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Workspace => "workspace",
            Self::User => "user",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum McpSourceKind {
    Config,
}

impl McpSourceKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Config => "config",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum McpInstallStatus {
    Ok,
    Partial,
    ValidationError,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum McpInstallResultStatus {
    Installed,
    Updated,
    ValidationError,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum McpRuntimeState {
    NotStarted,
    Disabled,
    Starting,
    Ready,
    Degraded,
    AuthRequired,
    Failed,
    Stopping,
    Stopped,
    Restarting,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum McpServerStatus {
    NotStarted,
    Disabled,
    Starting,
    Ready,
    Degraded,
    AuthRequired,
    Failed,
    Stopping,
    Stopped,
    Restarting,
}

impl From<McpRuntimeState> for McpServerStatus {
    fn from(value: McpRuntimeState) -> Self {
        match value {
            McpRuntimeState::NotStarted => Self::NotStarted,
            McpRuntimeState::Disabled => Self::Disabled,
            McpRuntimeState::Starting => Self::Starting,
            McpRuntimeState::Ready => Self::Ready,
            McpRuntimeState::Degraded => Self::Degraded,
            McpRuntimeState::AuthRequired => Self::AuthRequired,
            McpRuntimeState::Failed => Self::Failed,
            McpRuntimeState::Stopping => Self::Stopping,
            McpRuntimeState::Stopped => Self::Stopped,
            McpRuntimeState::Restarting => Self::Restarting,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum McpDiagnosticLevel {
    Error,
    Warning,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum McpChangedAction {
    Install,
    Policy,
    Update,
    Uninstall,
}

impl McpChangedAction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Install => "install",
            Self::Policy => "policy",
            Self::Update => "update",
            Self::Uninstall => "uninstall",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn operational_mcp_item_has_no_management_transport_or_diagnostics() {
        let item = McpListItem {
            id: "mcp-1".to_owned(),
            name: "search".to_owned(),
            display_name: Some("Search".to_owned()),
            scope: McpScopeKind::Workspace,
            policy: McpPolicyState {
                enabled: true,
                allow_implicit_invocation: false,
            },
            required: false,
            runtime: McpRuntimeStatus {
                state: McpRuntimeState::Ready,
                live: true,
                last_seen_at: Some(1),
            },
            tools_count: 1,
            resources_count: 0,
            resource_templates_count: 0,
            prompts_count: 0,
            status: McpServerStatus::Ready,
        };
        let encoded = serde_json::to_string(&item).expect("MCP item serializes");

        for forbidden in [
            "transport",
            "command",
            "url",
            "fingerprint",
            "status_reason",
            "last_error",
            "stderr",
            "recent_bindings",
        ] {
            assert!(
                !encoded.contains(forbidden),
                "operational MCP projection leaked {forbidden}: {encoded}"
            );
        }
    }

    #[test]
    fn install_params_default_to_workspace_scope() {
        let params: McpInstallParams = serde_json::from_value(json!({
            "workspace_id": "ws_000000000000000001",
            "config_json": "{\"mcpServers\":{}}"
        }))
        .expect("mcp install params should decode without scope_kind");

        assert_eq!(params.scope_kind, McpScopeKind::Workspace);
        assert!(params.enabled);
        assert!(!params.allow_implicit_invocation);
    }
}
