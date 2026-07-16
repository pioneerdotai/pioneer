mod catalog;
mod client;
mod config;
mod domain;
mod error;
mod fingerprint;
mod policy;
mod redaction;
mod runtime;
mod secrets;
mod validation;

pub use catalog::McpCatalogSnapshot;
pub use client::rmcp_adapter::RmcpRuntimeConnector;
pub use config::{InstallParseContext, McpInstallPlan, McpInstallPlanItem, parse_install_config};
pub use domain::{
    McpAuthConfig, McpAvailabilitySnapshot, McpConfigValue, McpDependencyKey, McpRuntimeState,
    McpScopeKind, McpSecretRef, McpServerInstallation, McpServerRuntimeSnapshot, McpSourceKind,
    McpTransportConfig, McpUnavailableReason,
};
pub use error::{McpConfigDocumentError, McpDiagnosticLevel, McpValidationDiagnostic};
pub use fingerprint::fingerprint_installation;
pub use policy::{
    McpToolPermissionClass, McpToolPolicyClassification, McpToolSafetyHints,
    McpToolSideEffectClass, classify_mcp_tool_policy,
};
pub use redaction::{REDACTED_VALUE, bounded_text, redact_text};
pub use runtime::{
    McpRetryPolicy, McpRuntimeConnector, McpRuntimeError, McpRuntimeErrorKind, McpRuntimeSession,
    McpSecretResolver, McpSessionEvent, McpToolCallResult, effective_secret_material_fingerprint,
};
pub use secrets::McpSecretMaterialization;
