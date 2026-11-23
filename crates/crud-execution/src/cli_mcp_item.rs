use pioneer_protocol::{ToolStoragePayload, TurnItem};

pub fn cli_mcp_item_metadata(item: &TurnItem) -> Option<&pioneer_protocol::ToolMetadata> {
    let TurnItem::DynamicToolCall { storage, .. } = item else {
        return None;
    };
    match storage {
        ToolStoragePayload::Summary(summary) => Some(&summary.metadata),
        ToolStoragePayload::Metadata { metadata } => Some(metadata),
        _ => None,
    }
}

/// Recognizes host-enriched MCP lifecycle items, not tool names or arguments.
pub fn is_managed_cli_mcp_item(item: &TurnItem) -> bool {
    let TurnItem::DynamicToolCall { id, tool_name, .. } = item else {
        return false;
    };
    let Some(metadata) = cli_mcp_item_metadata(item) else {
        return false;
    };
    metadata
        .get("canonicalCallableName")
        .and_then(|v| v.as_str())
        == Some(tool_name.as_str())
        && metadata.get("providerCallId").and_then(|v| v.as_str()) == Some(id.as_str())
        && metadata
            .get("sessionGeneration")
            .and_then(|v| v.as_u64())
            .is_some()
        && metadata
            .get("manifestHash")
            .and_then(|v| v.as_str())
            .is_some()
        && metadata
            .get("invocationCorrelationId")
            .and_then(|v| v.as_str())
            .is_some()
}
