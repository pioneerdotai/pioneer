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
use crate::domain::{McpRuntimeState, McpSecretRef, McpServerInstallation};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpRuntimeErrorKind {
    Failed,
    AuthRequired,
    Cancelled,
    TimedOut,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpRuntimeError {
    pub kind: McpRuntimeErrorKind,
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
            kind: McpRuntimeErrorKind::Failed,
            state: McpRuntimeState::Failed,
            message: message.into(),
        }
    }

    pub fn auth_required(message: impl Into<String>) -> Self {
        Self {
            kind: McpRuntimeErrorKind::AuthRequired,
            state: McpRuntimeState::AuthRequired,
            message: message.into(),
        }
    }

    pub fn cancelled(message: impl Into<String>) -> Self {
        Self {
            kind: McpRuntimeErrorKind::Cancelled,
            state: McpRuntimeState::Failed,
            message: message.into(),
        }
    }

    pub fn timed_out(message: impl Into<String>) -> Self {
        Self {
            kind: McpRuntimeErrorKind::TimedOut,
            state: McpRuntimeState::Failed,
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

/// Returns an opaque, process-keyed change token for the effective secret
/// material used by one installation. The token is safe to retain in memory
/// for runtime reuse decisions, but is intentionally not a durable identity.
pub fn effective_secret_material_fingerprint(
    installation: &McpServerInstallation,
    resolver: &dyn McpSecretResolver,
    process_key: &[u8],
) -> Result<String, McpRuntimeError> {
    let mut secret_refs = installation.secret_refs.iter().collect::<Vec<_>>();
    secret_refs.sort_by(|left, right| {
        (&left.ref_id, &left.name, &left.source).cmp(&(&right.ref_id, &right.name, &right.source))
    });

    let mut hasher = Sha256::new();
    hash_secret_field(&mut hasher, b"pioneer.mcp.effective-secret.v1");
    hash_secret_field(&mut hasher, process_key);
    for secret_ref in secret_refs {
        let value = resolver
            .resolve_mcp_secret(secret_ref.ref_id.as_str())
            .ok_or_else(|| {
                McpRuntimeError::auth_required(format!(
                    "MCP secret reference `{}` is unavailable",
                    secret_ref.ref_id
                ))
            })?;
        hash_secret_ref(&mut hasher, secret_ref);
        hash_secret_field(&mut hasher, value.as_bytes());
    }
    Ok(hex::encode(hasher.finalize()))
}

fn hash_secret_ref(hasher: &mut Sha256, secret_ref: &McpSecretRef) {
    hash_secret_field(hasher, secret_ref.ref_id.as_bytes());
    hash_secret_field(hasher, secret_ref.name.as_bytes());
    hash_secret_field(hasher, secret_ref.source.as_bytes());
}

fn hash_secret_field(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
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
        timeout: Duration,
        cancellation: CancellationToken,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{McpAuthConfig, McpScopeKind, McpSourceKind, McpTransportConfig};
    use std::collections::{BTreeMap, HashMap};

    struct TestResolver {
        values: HashMap<String, String>,
    }

    impl McpSecretResolver for TestResolver {
        fn resolve_mcp_secret(&self, ref_id: &str) -> Option<String> {
            self.values.get(ref_id).cloned()
        }
    }

    fn installation_with_secret(ref_id: &str) -> McpServerInstallation {
        McpServerInstallation {
            scope_kind: McpScopeKind::Workspace,
            scope_key: "workspace-test".to_owned(),
            name: "secret-server".to_owned(),
            display_name: None,
            source_kind: McpSourceKind::Config,
            source_ref: serde_json::json!({"kind": "test"}),
            transport: McpTransportConfig::Stdio {
                command: "secret-server".to_owned(),
                args: Vec::new(),
                cwd: None,
                env: BTreeMap::new(),
                startup_timeout_ms: 1_000,
                tool_timeout_ms: 1_000,
            },
            auth: McpAuthConfig::default(),
            secret_refs: vec![McpSecretRef {
                ref_id: ref_id.to_owned(),
                name: "TOKEN".to_owned(),
                source: "env".to_owned(),
            }],
            enabled: true,
            allow_implicit_invocation: false,
            required: false,
            fingerprint: "installation-fingerprint".to_owned(),
        }
    }

    #[test]
    fn runtime_generation_secret_fingerprint_is_stable_until_effective_value_changes() {
        let installation = installation_with_secret("same-ref");
        let process_key = b"test-process-key";
        let first = TestResolver {
            values: HashMap::from([("same-ref".to_owned(), "secret-canary-one".to_owned())]),
        };
        let unchanged = TestResolver {
            values: HashMap::from([("same-ref".to_owned(), "secret-canary-one".to_owned())]),
        };
        let rotated = TestResolver {
            values: HashMap::from([("same-ref".to_owned(), "secret-canary-two".to_owned())]),
        };

        let first_fingerprint =
            effective_secret_material_fingerprint(&installation, &first, process_key).unwrap();
        let unchanged_fingerprint =
            effective_secret_material_fingerprint(&installation, &unchanged, process_key).unwrap();
        let rotated_fingerprint =
            effective_secret_material_fingerprint(&installation, &rotated, process_key).unwrap();

        assert_eq!(first_fingerprint, unchanged_fingerprint);
        assert_ne!(first_fingerprint, rotated_fingerprint);
        assert!(!first_fingerprint.contains("secret-canary-one"));
        assert!(!rotated_fingerprint.contains("secret-canary-two"));
    }

    #[test]
    fn runtime_generation_secret_fingerprint_fails_closed_when_material_is_missing() {
        let installation = installation_with_secret("missing-ref");
        let error = effective_secret_material_fingerprint(
            &installation,
            &TestResolver {
                values: HashMap::new(),
            },
            b"test-process-key",
        )
        .unwrap_err();

        assert_eq!(error.state, McpRuntimeState::AuthRequired);
        assert!(error.message.contains("missing-ref"));
    }
}
