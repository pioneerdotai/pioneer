//! UI-neutral turn security presentation models.

use pioneer_protocol::{
    SandboxBackendKind, TurnExecutionSecuritySnapshot, TurnNetworkMode, TurnPermissionMode,
    TurnSandboxMode, TurnSecurityCapabilityKind, TurnSecurityEnforcementStatus,
    TurnSecurityExecutionBackendKind,
};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ClientTurnSecuritySummary {
    pub permission_mode: TurnPermissionMode,
    pub sandbox_mode: TurnSandboxMode,
    pub filesystem_access: ClientSecurityFilesystemAccess,
    pub network_mode: TurnNetworkMode,
    pub execution_backend: TurnSecurityExecutionBackendKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sandbox_backend: Option<SandboxBackendKind>,
    pub enforcement: ClientSecurityEnforcementStatus,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<ClientSecurityDiagnostic>,
}

impl ClientTurnSecuritySummary {
    pub fn from_execution_snapshot(snapshot: &TurnExecutionSecuritySnapshot) -> Self {
        let (enforcement, diagnostics) =
            client_enforcement_status_and_diagnostics(&snapshot.enforcement);

        Self {
            permission_mode: snapshot.permission_profile.mode,
            sandbox_mode: snapshot.sandbox.mode,
            filesystem_access: ClientSecurityFilesystemAccess::from_sandbox_mode(
                snapshot.sandbox.mode,
            ),
            network_mode: snapshot.network.mode,
            execution_backend: snapshot.backend.execution_backend,
            sandbox_backend: snapshot.backend.sandbox_backend,
            enforcement,
            diagnostics,
        }
    }

    pub fn is_unrestricted(&self) -> bool {
        self.permission_mode == TurnPermissionMode::FullAccess
            && self.sandbox_mode == TurnSandboxMode::Unrestricted
            && self.filesystem_access == ClientSecurityFilesystemAccess::Unrestricted
            && self.network_mode == TurnNetworkMode::Enabled
            && self.enforcement == ClientSecurityEnforcementStatus::Active
    }

    pub fn has_visible_diagnostics(&self) -> bool {
        !self.diagnostics.is_empty()
            || matches!(
                self.enforcement,
                ClientSecurityEnforcementStatus::Degraded
                    | ClientSecurityEnforcementStatus::Unavailable
            )
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ClientSecurityFilesystemAccess {
    Unrestricted,
    ReadOnly,
    WorkspaceWrite,
}

impl ClientSecurityFilesystemAccess {
    pub fn from_sandbox_mode(mode: TurnSandboxMode) -> Self {
        match mode {
            TurnSandboxMode::Unrestricted => Self::Unrestricted,
            TurnSandboxMode::ReadOnly => Self::ReadOnly,
            TurnSandboxMode::WorkspaceWrite => Self::WorkspaceWrite,
        }
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ClientSecurityEnforcementStatus {
    Active,
    Degraded,
    Unavailable,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ClientSecurityDiagnostic {
    pub capability: TurnSecurityCapabilityKind,
    pub status: ClientSecurityEnforcementStatus,
    pub message: String,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ClientSecurityDiagnosticRow {
    pub capability: TurnSecurityCapabilityKind,
    pub label: String,
    pub message: String,
}

pub fn security_summary_label(summary: &ClientTurnSecuritySummary) -> &'static str {
    match summary.enforcement {
        ClientSecurityEnforcementStatus::Unavailable => "Sandbox unavailable",
        ClientSecurityEnforcementStatus::Degraded => "Sandbox degraded",
        ClientSecurityEnforcementStatus::Active => match summary.filesystem_access {
            ClientSecurityFilesystemAccess::Unrestricted => "Unrestricted",
            ClientSecurityFilesystemAccess::ReadOnly => "Read-only sandbox",
            ClientSecurityFilesystemAccess::WorkspaceWrite => "Workspace sandbox",
        },
    }
}

pub fn security_diagnostic_rows(
    summary: &ClientTurnSecuritySummary,
) -> Vec<ClientSecurityDiagnosticRow> {
    summary
        .diagnostics
        .iter()
        .map(|diagnostic| ClientSecurityDiagnosticRow {
            capability: diagnostic.capability,
            label: security_capability_label(diagnostic.capability).to_owned(),
            message: sanitize_security_diagnostic_message(diagnostic.message.as_str()),
        })
        .collect()
}

fn security_capability_label(capability: TurnSecurityCapabilityKind) -> &'static str {
    match capability {
        TurnSecurityCapabilityKind::Filesystem => "Filesystem sandbox",
        TurnSecurityCapabilityKind::Network => "Network sandbox",
        TurnSecurityCapabilityKind::Process => "Process sandbox",
        TurnSecurityCapabilityKind::Approval => "Approval policy",
        TurnSecurityCapabilityKind::SandboxBackend => "Sandbox backend",
    }
}

fn sanitize_security_diagnostic_message(message: &str) -> String {
    let mut sanitized = message.split_whitespace().collect::<Vec<_>>().join(" ");
    if sanitized.is_empty() {
        sanitized = "Capability is not fully enforced.".to_owned();
    }

    const MAX_CHARS: usize = 180;
    if sanitized.chars().count() <= MAX_CHARS {
        return sanitized;
    }

    let truncated = sanitized.chars().take(MAX_CHARS).collect::<String>();
    format!("{truncated}...")
}

fn client_enforcement_status_and_diagnostics(
    status: &TurnSecurityEnforcementStatus,
) -> (
    ClientSecurityEnforcementStatus,
    Vec<ClientSecurityDiagnostic>,
) {
    match status {
        TurnSecurityEnforcementStatus::Active => {
            (ClientSecurityEnforcementStatus::Active, Vec::new())
        }
        TurnSecurityEnforcementStatus::PartiallyActive { degraded } => (
            ClientSecurityEnforcementStatus::Degraded,
            degraded
                .iter()
                .map(|degradation| ClientSecurityDiagnostic {
                    capability: degradation.capability,
                    status: ClientSecurityEnforcementStatus::Degraded,
                    message: degradation.reason.clone(),
                })
                .collect(),
        ),
        TurnSecurityEnforcementStatus::Unavailable { reason } => (
            ClientSecurityEnforcementStatus::Unavailable,
            vec![ClientSecurityDiagnostic {
                capability: TurnSecurityCapabilityKind::SandboxBackend,
                status: ClientSecurityEnforcementStatus::Unavailable,
                message: reason.clone(),
            }],
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        BackendSecurityCapabilities, SandboxBackendRequirement, TurnFilesystemAccess,
        TurnFilesystemSandboxEntry, TurnPermissionProfileSnapshot, TurnPermissionProfileSource,
        TurnSecurityBackendSnapshot, TurnSecurityDegradation,
    };

    #[test]
    fn contracts_native_full_access_summary_is_unrestricted() {
        let snapshot = TurnExecutionSecuritySnapshot::unrestricted_full_access("/repo", 1);

        let summary = ClientTurnSecuritySummary::from_execution_snapshot(&snapshot);

        assert!(summary.is_unrestricted());
        assert_eq!(security_summary_label(&summary), "Unrestricted");
        assert_eq!(
            summary.execution_backend,
            TurnSecurityExecutionBackendKind::Native
        );
        assert_eq!(summary.sandbox_backend, None);
        assert!(!summary.has_visible_diagnostics());
    }

    #[test]
    fn contracts_codex_summary_uses_normalized_provider_fields() {
        let mut snapshot = TurnExecutionSecuritySnapshot::workspace_write(
            TurnPermissionProfileSnapshot::from_mode(
                TurnPermissionMode::AutoAcceptEdits,
                TurnPermissionProfileSource::Composer,
            ),
            "/repo",
            vec![TurnFilesystemSandboxEntry::workspace_root(
                TurnFilesystemAccess::Write,
                "/repo",
            )],
            1,
        );
        snapshot.backend = TurnSecurityBackendSnapshot {
            execution_backend: TurnSecurityExecutionBackendKind::CodexCli,
            sandbox_backend: Some(SandboxBackendKind::ProviderNative),
            provider: Some("codex".to_owned()),
            capabilities: BackendSecurityCapabilities::native_sandboxed(),
        };

        let summary = ClientTurnSecuritySummary::from_execution_snapshot(&snapshot);
        let encoded = serde_json::to_string(&summary).expect("summary serializes");

        assert_eq!(
            summary.filesystem_access,
            ClientSecurityFilesystemAccess::WorkspaceWrite
        );
        assert_eq!(security_summary_label(&summary), "Workspace sandbox");
        assert_eq!(
            summary.execution_backend,
            TurnSecurityExecutionBackendKind::CodexCli
        );
        assert!(!encoded.contains("danger"));
        assert!(!encoded.contains("bypass"));
    }

    #[test]
    fn contracts_claude_degraded_summary_preserves_diagnostics() {
        let mut snapshot = TurnExecutionSecuritySnapshot::read_only(
            TurnPermissionProfileSnapshot::from_mode(
                TurnPermissionMode::Supervised,
                TurnPermissionProfileSource::Composer,
            ),
            "/repo",
            vec![TurnFilesystemSandboxEntry::workspace_root(
                TurnFilesystemAccess::Read,
                "/repo",
            )],
            1,
        );
        snapshot.backend = TurnSecurityBackendSnapshot {
            execution_backend: TurnSecurityExecutionBackendKind::ClaudeCli,
            sandbox_backend: None,
            provider: Some("claude".to_owned()),
            capabilities: BackendSecurityCapabilities {
                can_enforce_filesystem: false,
                can_enforce_network: false,
                can_enforce_process: false,
                supports_turn_scope_approval: true,
                supports_session_scope_approval: false,
                supports_request_permissions: true,
            },
        };
        snapshot.sandbox.backend_requirement = SandboxBackendRequirement::Optional;
        snapshot.enforcement = TurnSecurityEnforcementStatus::PartiallyActive {
            degraded: vec![TurnSecurityDegradation {
                capability: TurnSecurityCapabilityKind::Filesystem,
                reason: "detailed filesystem sandbox is not provider-enforced".to_owned(),
            }],
        };

        let summary = ClientTurnSecuritySummary::from_execution_snapshot(&snapshot);

        assert_eq!(
            summary.enforcement,
            ClientSecurityEnforcementStatus::Degraded
        );
        assert_eq!(security_summary_label(&summary), "Sandbox degraded");
        assert_eq!(summary.diagnostics.len(), 1);
        let rows = security_diagnostic_rows(&summary);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].label, "Filesystem sandbox");
        assert_eq!(
            rows[0].message,
            "detailed filesystem sandbox is not provider-enforced"
        );
        assert_eq!(
            summary.diagnostics[0].capability,
            TurnSecurityCapabilityKind::Filesystem
        );
    }

    #[test]
    fn unavailable_native_sandbox_summary_has_safe_diagnostic_row() {
        let mut snapshot = TurnExecutionSecuritySnapshot::read_only(
            TurnPermissionProfileSnapshot::from_mode(
                TurnPermissionMode::Supervised,
                TurnPermissionProfileSource::Composer,
            ),
            "/repo",
            vec![TurnFilesystemSandboxEntry::workspace_root(
                TurnFilesystemAccess::Read,
                "/repo",
            )],
            1,
        );
        snapshot.enforcement = TurnSecurityEnforcementStatus::Unavailable {
            reason: "native sandbox backend unavailable\nfor current host".to_owned(),
        };

        let summary = ClientTurnSecuritySummary::from_execution_snapshot(&snapshot);
        let rows = security_diagnostic_rows(&summary);

        assert_eq!(
            summary.enforcement,
            ClientSecurityEnforcementStatus::Unavailable
        );
        assert_eq!(security_summary_label(&summary), "Sandbox unavailable");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].label, "Sandbox backend");
        assert_eq!(
            rows[0].message,
            "native sandbox backend unavailable for current host"
        );
    }
}
