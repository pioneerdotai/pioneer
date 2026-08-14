//! Capability descriptors for local CLI agent runtime providers.

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CLIAgentRuntimeProviderKind {
    Codex,
    Claude,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CLIRuntimeCapabilitySupport {
    Supported,
    Unsupported { reason: &'static str },
}

impl CLIRuntimeCapabilitySupport {
    pub const fn is_supported(self) -> bool {
        matches!(self, Self::Supported)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CLIRuntimeApprovalCapabilities {
    pub permission_mode_mapping: CLIRuntimeCapabilitySupport,
    pub request_permissions: CLIRuntimeCapabilitySupport,
    pub turn_scope_approval: CLIRuntimeCapabilitySupport,
    pub session_scope_approval: CLIRuntimeCapabilitySupport,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CLIRuntimeSandboxCapabilities {
    pub provider_sandbox_policy: CLIRuntimeCapabilitySupport,
    pub detailed_filesystem_sandbox: CLIRuntimeCapabilitySupport,
    pub detailed_network_sandbox: CLIRuntimeCapabilitySupport,
    pub detailed_process_sandbox: CLIRuntimeCapabilitySupport,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CLIRuntimeProviderCapabilities {
    pub provider: CLIAgentRuntimeProviderKind,
    pub approval: CLIRuntimeApprovalCapabilities,
    pub sandbox: CLIRuntimeSandboxCapabilities,
}

pub fn cli_runtime_provider_capabilities(
    provider: CLIAgentRuntimeProviderKind,
) -> &'static CLIRuntimeProviderCapabilities {
    match provider {
        CLIAgentRuntimeProviderKind::Codex => &CODEX_CAPABILITIES,
        CLIAgentRuntimeProviderKind::Claude => &CLAUDE_CAPABILITIES,
    }
}

const UNSUPPORTED_SESSION_SCOPE_APPROVAL: CLIRuntimeCapabilitySupport =
    CLIRuntimeCapabilitySupport::Unsupported {
        reason: "provider adapter does not expose a durable session-scoped approval mapping",
    };

const UNSUPPORTED_TURN_SCOPE_APPROVAL: CLIRuntimeCapabilitySupport =
    CLIRuntimeCapabilitySupport::Unsupported {
        reason: "provider adapter exposes only one-shot and session-scoped approval responses",
    };

const CODEX_CAPABILITIES: CLIRuntimeProviderCapabilities = CLIRuntimeProviderCapabilities {
    provider: CLIAgentRuntimeProviderKind::Codex,
    approval: CLIRuntimeApprovalCapabilities {
        permission_mode_mapping: CLIRuntimeCapabilitySupport::Supported,
        request_permissions: CLIRuntimeCapabilitySupport::Supported,
        turn_scope_approval: UNSUPPORTED_TURN_SCOPE_APPROVAL,
        session_scope_approval: UNSUPPORTED_SESSION_SCOPE_APPROVAL,
    },
    sandbox: CLIRuntimeSandboxCapabilities {
        provider_sandbox_policy: CLIRuntimeCapabilitySupport::Supported,
        detailed_filesystem_sandbox: CLIRuntimeCapabilitySupport::Supported,
        detailed_network_sandbox: CLIRuntimeCapabilitySupport::Supported,
        detailed_process_sandbox: CLIRuntimeCapabilitySupport::Supported,
    },
};

const CLAUDE_DETAILED_SANDBOX_UNSUPPORTED: CLIRuntimeCapabilitySupport =
    CLIRuntimeCapabilitySupport::Unsupported {
        reason: "Claude CLI adapter currently maps permission mode only; no proven detailed sandbox knob is available",
    };

const CLAUDE_CAPABILITIES: CLIRuntimeProviderCapabilities = CLIRuntimeProviderCapabilities {
    provider: CLIAgentRuntimeProviderKind::Claude,
    approval: CLIRuntimeApprovalCapabilities {
        permission_mode_mapping: CLIRuntimeCapabilitySupport::Supported,
        request_permissions: CLIRuntimeCapabilitySupport::Supported,
        turn_scope_approval: UNSUPPORTED_TURN_SCOPE_APPROVAL,
        session_scope_approval: UNSUPPORTED_SESSION_SCOPE_APPROVAL,
    },
    sandbox: CLIRuntimeSandboxCapabilities {
        provider_sandbox_policy: CLAUDE_DETAILED_SANDBOX_UNSUPPORTED,
        detailed_filesystem_sandbox: CLAUDE_DETAILED_SANDBOX_UNSUPPORTED,
        detailed_network_sandbox: CLAUDE_DETAILED_SANDBOX_UNSUPPORTED,
        detailed_process_sandbox: CLAUDE_DETAILED_SANDBOX_UNSUPPORTED,
    },
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capabilities_codex_reports_exact_approval_and_provider_sandbox_knobs() {
        let descriptor = cli_runtime_provider_capabilities(CLIAgentRuntimeProviderKind::Codex);

        assert_eq!(descriptor.provider, CLIAgentRuntimeProviderKind::Codex);
        assert!(descriptor.approval.permission_mode_mapping.is_supported());
        assert!(descriptor.approval.request_permissions.is_supported());
        assert!(!descriptor.approval.turn_scope_approval.is_supported());
        assert!(!descriptor.approval.session_scope_approval.is_supported());
        assert!(descriptor.sandbox.provider_sandbox_policy.is_supported());
        assert!(
            descriptor
                .sandbox
                .detailed_filesystem_sandbox
                .is_supported()
        );
        assert!(descriptor.sandbox.detailed_network_sandbox.is_supported());
        assert!(descriptor.sandbox.detailed_process_sandbox.is_supported());
    }

    #[test]
    fn capabilities_claude_does_not_claim_detailed_sandbox_support() {
        let descriptor = cli_runtime_provider_capabilities(CLIAgentRuntimeProviderKind::Claude);

        assert_eq!(descriptor.provider, CLIAgentRuntimeProviderKind::Claude);
        assert!(descriptor.approval.permission_mode_mapping.is_supported());
        assert!(descriptor.approval.request_permissions.is_supported());
        assert!(!descriptor.approval.turn_scope_approval.is_supported());
        assert!(!descriptor.sandbox.provider_sandbox_policy.is_supported());
        assert!(
            !descriptor
                .sandbox
                .detailed_filesystem_sandbox
                .is_supported()
        );
        assert!(!descriptor.sandbox.detailed_network_sandbox.is_supported());
        assert!(!descriptor.sandbox.detailed_process_sandbox.is_supported());
    }

    #[test]
    fn claude_security_descriptor_supports_permission_mode_not_detailed_sandbox() {
        let descriptor = cli_runtime_provider_capabilities(CLIAgentRuntimeProviderKind::Claude);

        assert!(descriptor.approval.permission_mode_mapping.is_supported());
        assert!(descriptor.approval.request_permissions.is_supported());
        assert!(matches!(
            descriptor.sandbox.detailed_filesystem_sandbox,
            CLIRuntimeCapabilitySupport::Unsupported { reason }
                if reason.contains("permission mode only")
        ));
        assert!(matches!(
            descriptor.sandbox.detailed_network_sandbox,
            CLIRuntimeCapabilitySupport::Unsupported { reason }
                if reason.contains("permission mode only")
        ));
    }
}
