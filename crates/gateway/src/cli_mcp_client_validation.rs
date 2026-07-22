//! Authoritative, provider-neutral validation for MCP claims submitted by clients.
//!
//! Client-side filtering is intentionally not trusted. The turn-start path
//! supplies fresh Gateway evidence to this gate before projection persistence
//! or CLI provider acquisition.

use pioneer_protocol::TurnCapabilityRejectedReason;

/// Non-secret identity needed to persist a failed client claim before any
/// projection, binding, or provider-session side effect is allowed.
#[derive(Debug, Clone, Copy)]
pub struct CliMcpClientValidationAuditContext<'a> {
    pub workspace_id: Option<&'a str>,
    pub thread_id: &'a str,
    pub turn_id: &'a str,
    pub runtime_id: &'a str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CliMcpClientTarget {
    Api,
    Codex,
    Claude,
}

impl CliMcpClientTarget {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Api => "api",
            Self::Codex => "codex",
            Self::Claude => "claude",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CliMcpClientValidationEvidence {
    pub target: CliMcpClientTarget,
    pub has_mcp_projection: bool,
    pub provider_claim_matches: bool,
    pub runtime_snapshot_current: bool,
    pub runtime_supports_mcp_tools: bool,
    pub projection_workspace_matches: bool,
    pub explicit_capabilities_resolved: bool,
}

impl CliMcpClientValidationEvidence {
    pub const fn api(has_mcp_projection: bool) -> Self {
        Self {
            target: CliMcpClientTarget::Api,
            has_mcp_projection,
            provider_claim_matches: true,
            runtime_snapshot_current: true,
            runtime_supports_mcp_tools: true,
            projection_workspace_matches: true,
            explicit_capabilities_resolved: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CliMcpClientValidationRejectionCode {
    ProviderSwitchRace,
    StaleRuntimeSnapshot,
    RuntimeMcpUnsupported,
    CrossWorkspaceProjection,
    ExplicitCapabilityUnresolved,
}

impl CliMcpClientValidationRejectionCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ProviderSwitchRace => "cli_runtime.mcp.provider_switch_race",
            Self::StaleRuntimeSnapshot => "cli_runtime.mcp.stale_runtime_snapshot",
            Self::RuntimeMcpUnsupported => "cli_runtime.mcp.runtime_unsupported",
            Self::CrossWorkspaceProjection => "cli_runtime.mcp.cross_workspace_projection",
            Self::ExplicitCapabilityUnresolved => "cli_runtime.mcp.explicit_capability_unresolved",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliMcpClientValidationRejection {
    pub code: CliMcpClientValidationRejectionCode,
    pub reason: TurnCapabilityRejectedReason,
    pub message: &'static str,
}

impl std::fmt::Display for CliMcpClientValidationRejection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.code.as_str(), self.message)
    }
}

impl std::error::Error for CliMcpClientValidationRejection {}

/// Validate fresh Gateway evidence before any active projection/binding commit
/// or provider acquisition. Native API turns are a regression control and keep
/// their existing authoritative MCP path.
pub fn validate_cli_mcp_client_request(
    evidence: CliMcpClientValidationEvidence,
) -> Result<(), CliMcpClientValidationRejection> {
    if evidence.target == CliMcpClientTarget::Api {
        return Ok(());
    }

    if !evidence.provider_claim_matches {
        return Err(rejection(
            CliMcpClientValidationRejectionCode::ProviderSwitchRace,
            TurnCapabilityRejectedReason::ProviderUnsupported,
            "the selected CLI provider changed before authoritative turn validation",
        ));
    }
    if evidence.has_mcp_projection && !evidence.runtime_snapshot_current {
        return Err(rejection(
            CliMcpClientValidationRejectionCode::StaleRuntimeSnapshot,
            TurnCapabilityRejectedReason::Unavailable,
            "the CLI runtime readiness snapshot is not current",
        ));
    }
    if evidence.has_mcp_projection && !evidence.runtime_supports_mcp_tools {
        return Err(rejection(
            CliMcpClientValidationRejectionCode::RuntimeMcpUnsupported,
            TurnCapabilityRejectedReason::ProviderUnsupported,
            "the selected CLI runtime does not currently support MCP tools",
        ));
    }
    if evidence.has_mcp_projection && !evidence.projection_workspace_matches {
        return Err(rejection(
            CliMcpClientValidationRejectionCode::CrossWorkspaceProjection,
            TurnCapabilityRejectedReason::SecurityBlocked,
            "the resolved MCP projection does not belong to the turn workspace",
        ));
    }
    if evidence.has_mcp_projection && !evidence.explicit_capabilities_resolved {
        return Err(rejection(
            CliMcpClientValidationRejectionCode::ExplicitCapabilityUnresolved,
            TurnCapabilityRejectedReason::ValidationRejected,
            "one or more explicit MCP capabilities were not authoritatively resolved",
        ));
    }

    Ok(())
}

/// Apply the authoritative client-claim gate and durably record a typed safe
/// diagnostic when it rejects. The audit row deliberately contains no tool
/// arguments, provider config, bootstrap material, grants, or secrets.
pub async fn validate_cli_mcp_client_request_durably(
    crud_store: &pioneer_crud::CrudStore,
    context: CliMcpClientValidationAuditContext<'_>,
    evidence: CliMcpClientValidationEvidence,
) -> Result<(), CliMcpClientValidationRejection> {
    match validate_cli_mcp_client_request(evidence) {
        Ok(()) => Ok(()),
        Err(rejection) => {
            persist_cli_mcp_preflight_rejection(
                crud_store,
                context,
                evidence.target,
                rejection.code.as_str(),
                rejection.reason,
                None,
                None,
            )
            .await;
            Err(rejection)
        }
    }
}

/// Persist resolver-produced typed capability rejections before returning the
/// terminal preflight error. If an infrastructure failure has no individual
/// capability row, one generic typed diagnostic is retained instead.
pub async fn persist_cli_mcp_materialization_rejections(
    crud_store: &pioneer_crud::CrudStore,
    context: CliMcpClientValidationAuditContext<'_>,
    target: CliMcpClientTarget,
    diagnostic_code: &str,
    rejected: &[pioneer_protocol::TurnRejectedCapability],
) {
    if rejected.is_empty() {
        persist_cli_mcp_preflight_rejection(
            crud_store,
            context,
            target,
            diagnostic_code,
            TurnCapabilityRejectedReason::MaterializationFailed,
            None,
            None,
        )
        .await;
        return;
    }

    for capability in rejected {
        let (server_name, raw_tool_name) = match &capability.kind {
            pioneer_protocol::TurnCapabilityKind::McpServer { name, .. } => {
                (Some(name.as_str()), None)
            }
            pioneer_protocol::TurnCapabilityKind::McpTool {
                server_name,
                raw_tool_name,
                ..
            } => (Some(server_name.as_str()), Some(raw_tool_name.as_str())),
            pioneer_protocol::TurnCapabilityKind::Skill { .. }
            | pioneer_protocol::TurnCapabilityKind::SkillPack { .. } => (None, None),
        };
        persist_cli_mcp_preflight_rejection(
            crud_store,
            context,
            target,
            diagnostic_code,
            capability.reason,
            server_name,
            raw_tool_name,
        )
        .await;
    }
}

async fn persist_cli_mcp_preflight_rejection(
    crud_store: &pioneer_crud::CrudStore,
    context: CliMcpClientValidationAuditContext<'_>,
    target: CliMcpClientTarget,
    diagnostic_code: &str,
    reason: TurnCapabilityRejectedReason,
    server_name: Option<&str>,
    raw_tool_name: Option<&str>,
) {
    let record = pioneer_crud::McpAuditEventRecord {
        turn_id: Some(context.turn_id.to_owned()),
        server_installation_id: None,
        server_name: server_name.unwrap_or(context.runtime_id).to_owned(),
        raw_tool_name: raw_tool_name.map(str::to_owned),
        callable_name: None,
        catalog_version: None,
        action: "cli_mcp_client_preflight".to_owned(),
        decision: "rejected".to_owned(),
        reason_code: Some(diagnostic_code.to_owned()),
        details_json: serde_json::json!({
            "workspace_id": context.workspace_id,
            "thread_id": context.thread_id,
            "runtime_id": context.runtime_id,
            "target": target.as_str(),
            "rejection_reason": turn_capability_rejected_reason_code(reason),
        })
        .to_string(),
        created_at_unix: chrono::Utc::now().timestamp(),
    };
    if let Err(error) = crud_store.insert_mcp_audit_event_record(&record).await {
        tracing::error!(
            runtime_id = context.runtime_id,
            thread_id = context.thread_id,
            turn_id = context.turn_id,
            diagnostic_code,
            error = %format!("{error:#}"),
            "failed to persist CLI MCP client preflight rejection"
        );
    }
}

const fn turn_capability_rejected_reason_code(
    reason: TurnCapabilityRejectedReason,
) -> &'static str {
    match reason {
        TurnCapabilityRejectedReason::InvalidInput => "invalid_input",
        TurnCapabilityRejectedReason::Duplicate => "duplicate",
        TurnCapabilityRejectedReason::NotFound => "not_found",
        TurnCapabilityRejectedReason::DisabledByPolicy => "disabled_by_policy",
        TurnCapabilityRejectedReason::ValidationRejected => "validation_rejected",
        TurnCapabilityRejectedReason::SecurityBlocked => "security_blocked",
        TurnCapabilityRejectedReason::DependencyMissing => "dependency_missing",
        TurnCapabilityRejectedReason::Unavailable => "unavailable",
        TurnCapabilityRejectedReason::CatalogMissing => "catalog_missing",
        TurnCapabilityRejectedReason::ToolMissing => "tool_missing",
        TurnCapabilityRejectedReason::ProviderUnsupported => "provider_unsupported",
        TurnCapabilityRejectedReason::MaterializationFailed => "materialization_failed",
    }
}

fn rejection(
    code: CliMcpClientValidationRejectionCode,
    reason: TurnCapabilityRejectedReason,
    message: &'static str,
) -> CliMcpClientValidationRejection {
    CliMcpClientValidationRejection {
        code,
        reason,
        message,
    }
}
