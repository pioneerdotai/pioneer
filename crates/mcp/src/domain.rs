use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum McpScopeKind {
    Workspace,
    User,
}

impl McpScopeKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Workspace => "workspace",
            Self::User => "user",
        }
    }
}

impl FromStr for McpScopeKind {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "workspace" => Ok(Self::Workspace),
            "user" => Ok(Self::User),
            other => Err(format!("unsupported MCP scope kind `{other}`")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum McpSourceKind {
    Config,
}

impl McpSourceKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Config => "config",
        }
    }
}

impl FromStr for McpSourceKind {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "config" => Ok(Self::Config),
            other => Err(format!("unsupported MCP source kind `{other}`")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct McpServerInstallation {
    pub scope_kind: McpScopeKind,
    pub scope_key: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    pub source_kind: McpSourceKind,
    pub source_ref: serde_json::Value,
    pub transport: McpTransportConfig,
    pub auth: McpAuthConfig,
    #[serde(default)]
    pub secret_refs: Vec<McpSecretRef>,
    pub enabled: bool,
    pub allow_implicit_invocation: bool,
    pub required: bool,
    pub fingerprint: String,
}

impl McpServerInstallation {
    pub fn transport_kind(&self) -> &'static str {
        self.transport.kind()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum McpTransportConfig {
    Stdio {
        command: String,
        #[serde(default)]
        args: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cwd: Option<String>,
        #[serde(default)]
        env: BTreeMap<String, McpConfigValue>,
        startup_timeout_ms: u64,
        tool_timeout_ms: u64,
    },
    StreamableHttp {
        url: String,
        #[serde(default)]
        headers: BTreeMap<String, McpConfigValue>,
        startup_timeout_ms: u64,
        tool_timeout_ms: u64,
    },
}

impl McpTransportConfig {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Stdio { .. } => "stdio",
            Self::StreamableHttp { .. } => "streamable_http",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq, PartialOrd, Ord)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum McpConfigValue {
    Literal { value: String },
    SecretRef { ref_id: String },
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct McpAuthConfig {
    #[serde(default)]
    pub required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq, PartialOrd, Ord)]
pub struct McpSecretRef {
    pub ref_id: String,
    pub name: String,
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq, PartialOrd, Ord)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum McpDependencyKey {
    Server { server: String },
    Tool { server: String, tool: String },
}

impl McpDependencyKey {
    pub fn parse(value: &str) -> Result<Self, String> {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Err("MCP dependency key must not be empty".to_owned());
        }

        if let Some((server, tool)) = trimmed.split_once('/') {
            let server = server.trim();
            let tool = tool.trim();
            if server.is_empty() || tool.is_empty() {
                return Err("MCP dependency key must use `server/tool`".to_owned());
            }
            return Ok(Self::Tool {
                server: server.to_owned(),
                tool: tool.to_owned(),
            });
        }

        Ok(Self::Server {
            server: trimmed.to_owned(),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum McpUnavailableReason {
    NotInstalled,
    Disabled,
    NotStarted,
    AuthRequired,
    Failed,
    ToolMissing,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct McpAvailabilitySnapshot {
    #[serde(default)]
    pub available: BTreeSet<McpDependencyKey>,
    #[serde(default)]
    pub blocked: BTreeMap<McpDependencyKey, McpUnavailableReason>,
}

impl McpAvailabilitySnapshot {
    pub fn phase1_from_installations<'a>(
        installations: impl IntoIterator<Item = &'a McpServerInstallation>,
    ) -> Self {
        let mut snapshot = Self::default();
        for installation in installations {
            let key = McpDependencyKey::Server {
                server: installation.name.clone(),
            };
            let reason = if installation.enabled {
                McpUnavailableReason::NotStarted
            } else {
                McpUnavailableReason::Disabled
            };
            snapshot.blocked.insert(key, reason);
        }
        snapshot
    }
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

impl McpRuntimeState {
    pub fn live(self) -> bool {
        matches!(self, Self::Ready | Self::Degraded)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct McpServerRuntimeSnapshot {
    pub installation_id: String,
    pub name: String,
    pub scope_kind: McpScopeKind,
    pub scope_key: String,
    pub fingerprint: String,
    pub state: McpRuntimeState,
    pub live: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_seen_at_unix: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub retry_attempt: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_retry_at_unix: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog_version: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dependency_key_parses_server() {
        assert_eq!(
            McpDependencyKey::parse("github").unwrap(),
            McpDependencyKey::Server {
                server: "github".to_owned()
            }
        );
    }

    #[test]
    fn dependency_key_parses_tool() {
        assert_eq!(
            McpDependencyKey::parse("github/search").unwrap(),
            McpDependencyKey::Tool {
                server: "github".to_owned(),
                tool: "search".to_owned()
            }
        );
    }

    #[test]
    fn dependency_key_rejects_empty_segments() {
        assert!(McpDependencyKey::parse("").is_err());
        assert!(McpDependencyKey::parse("github/").is_err());
        assert!(McpDependencyKey::parse("/search").is_err());
    }
}
