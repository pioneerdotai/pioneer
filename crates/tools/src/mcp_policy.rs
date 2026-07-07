use crate::error::ToolError;
use pioneer_mcp::{McpToolPolicyClassification, McpToolSideEffectClass};
use pioneer_protocol::{TurnExecutionSecuritySnapshot, TurnNetworkMode};

pub fn enforce_mcp_network_policy(
    snapshot: Option<&TurnExecutionSecuritySnapshot>,
    classification: &McpToolPolicyClassification,
    server_name: &str,
    raw_tool_name: &str,
) -> Result<(), ToolError> {
    if !classification.requires_network {
        return Ok(());
    }

    let Some(snapshot) = snapshot else {
        return Err(ToolError::Rejected(format!(
            "MCP tool `{server_name}/{raw_tool_name}` requires network access, but the turn is missing an execution security snapshot"
        )));
    };

    match snapshot.network.mode {
        TurnNetworkMode::Enabled => Ok(()),
        TurnNetworkMode::Disabled => Err(ToolError::Rejected(format!(
            "MCP tool `{server_name}/{raw_tool_name}` requires network access, but network is disabled for this turn"
        ))),
        TurnNetworkMode::Restricted => Err(ToolError::Rejected(format!(
            "MCP tool `{server_name}/{raw_tool_name}` requires network access, but its target host cannot be checked against the turn allowlist"
        ))),
    }
}

pub fn mcp_policy_classification_metadata(
    classification: &McpToolPolicyClassification,
) -> serde_json::Value {
    serde_json::json!({
        "sideEffectClass": classification.side_effect_class.as_str(),
        "permissionClass": classification.permission_class.as_str(),
        "requiresNetwork": classification.requires_network,
        "unknownCapability": classification.side_effect_class == McpToolSideEffectClass::Unknown,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_mcp::McpToolPermissionClass;
    use pioneer_protocol::{
        TurnExecutionSecuritySnapshot, TurnNetworkPolicySnapshot, TurnPermissionMode,
        TurnPermissionProfileSnapshot, TurnPermissionProfileSource,
    };

    fn snapshot_with_network(network: TurnNetworkPolicySnapshot) -> TurnExecutionSecuritySnapshot {
        let mut snapshot = TurnExecutionSecuritySnapshot::unrestricted_full_access(
            "/tmp/workspace",
            1_700_000_000_000,
        );
        snapshot.permission_profile = TurnPermissionProfileSnapshot::from_mode(
            TurnPermissionMode::FullAccess,
            TurnPermissionProfileSource::Composer,
        );
        snapshot.network = network.clone();
        snapshot.sandbox.network = network;
        snapshot
    }

    fn network_classification() -> McpToolPolicyClassification {
        McpToolPolicyClassification {
            side_effect_class: McpToolSideEffectClass::NetworkLike,
            permission_class: McpToolPermissionClass::WriteOrUnknown,
            requires_network: true,
        }
    }

    #[test]
    fn mcp_policy_network_disabled_blocks_network_side_effect() {
        let snapshot = snapshot_with_network(TurnNetworkPolicySnapshot::disabled());

        let error = enforce_mcp_network_policy(
            Some(&snapshot),
            &network_classification(),
            "server",
            "tool",
        )
        .expect_err("disabled network must reject MCP network side effects");

        assert!(
            matches!(error, ToolError::Rejected(ref message) if message.contains("network is disabled")),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn mcp_policy_enabled_network_allows_network_side_effect() {
        let snapshot = snapshot_with_network(TurnNetworkPolicySnapshot::enabled());

        enforce_mcp_network_policy(Some(&snapshot), &network_classification(), "server", "tool")
            .expect("enabled network should allow MCP network side effects");
    }

    #[test]
    fn mcp_policy_rejects_missing_snapshot_for_network_side_effect() {
        let error = enforce_mcp_network_policy(None, &network_classification(), "server", "tool")
            .expect_err("missing security snapshot should fail closed");

        assert!(
            matches!(error, ToolError::Rejected(ref message) if message.contains("missing an execution security snapshot")),
            "unexpected error: {error}"
        );
    }
}
