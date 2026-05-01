mod backoff;
mod command;
mod stderr;

pub use backoff::McpRetryPolicy;
pub use command::{
    MaterializedHttpTransport, MaterializedStdioTransport, MaterializedTransport,
    materialize_transport,
};
pub use stderr::StderrTail;

use crate::catalog::McpCatalogSnapshot;
use crate::domain::{McpRuntimeState, McpServerInstallation};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpRuntimeError {
    pub state: McpRuntimeState,
    pub message: String,
}

impl std::fmt::Display for McpRuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.state, self.message)
    }
}

impl std::error::Error for McpRuntimeError {}

impl McpRuntimeError {
    pub fn failed(message: impl Into<String>) -> Self {
        Self {
            state: McpRuntimeState::Failed,
            message: message.into(),
        }
    }

    pub fn auth_required(message: impl Into<String>) -> Self {
        Self {
            state: McpRuntimeState::AuthRequired,
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpSessionEvent {
    CatalogChanged,
    Closed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct McpToolCallResult {
    #[serde(default)]
    pub content: JsonValue,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub structured_content: Option<JsonValue>,
    pub is_error: bool,
    pub duration_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<JsonValue>,
}

#[async_trait]
pub trait McpSecretResolver: Send + Sync {
    fn resolve_mcp_secret(&self, ref_id: &str) -> Option<String>;
}

#[async_trait]
pub trait McpRuntimeSession: Send {
    fn initial_catalog(&self) -> &McpCatalogSnapshot;
    fn degraded_reason(&self) -> Option<&str> {
        None
    }
    async fn wait_for_event(&mut self) -> McpSessionEvent;
    async fn refresh_catalog(&mut self) -> Result<McpCatalogSnapshot, McpRuntimeError>;
    async fn call_tool(
        &mut self,
        raw_tool_name: &str,
        arguments: JsonValue,
    ) -> Result<McpToolCallResult, McpRuntimeError>;
    async fn shutdown(&mut self);
}

#[async_trait]
pub trait McpRuntimeConnector: Send + Sync {
    async fn connect(
        &self,
        installation: McpServerInstallation,
        installation_id: String,
        resolver: Arc<dyn McpSecretResolver>,
        now_unix: i64,
    ) -> Result<Box<dyn McpRuntimeSession>, McpRuntimeError>;
}
