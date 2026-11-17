use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct McpToolSafetyHints {
    pub read_only_hint: Option<bool>,
    pub destructive_hint: Option<bool>,
    pub open_world_hint: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpToolSideEffectClass {
    ReadOnly,
    WriteLike,
    NetworkLike,
    Unknown,
}

impl McpToolSideEffectClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReadOnly => "read_only",
            Self::WriteLike => "write_like",
            Self::NetworkLike => "network_like",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpToolPermissionClass {
    Read,
    WriteOrUnknown,
}

impl McpToolPermissionClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::WriteOrUnknown => "write_or_unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct McpToolPolicyClassification {
    pub side_effect_class: McpToolSideEffectClass,
    pub permission_class: McpToolPermissionClass,
    pub requires_network: bool,
}

pub fn classify_mcp_tool_policy(hints: McpToolSafetyHints) -> McpToolPolicyClassification {
    // MCP annotations are supplied by the server itself. Until Pioneer has a
    // separate, server-owned trust policy, they may only make the displayed
    // classification more conservative; they must never remove consent or
    // network enforcement. In particular, `readOnlyHint=true` and
    // `openWorldHint=false` are not security assertions.
    let side_effect_class = if hints.destructive_hint == Some(true) {
        McpToolSideEffectClass::WriteLike
    } else if hints.open_world_hint == Some(true) {
        McpToolSideEffectClass::NetworkLike
    } else {
        McpToolSideEffectClass::Unknown
    };

    McpToolPolicyClassification {
        side_effect_class,
        permission_class: McpToolPermissionClass::WriteOrUnknown,
        // An out-of-process MCP server can perform network effects regardless
        // of what it advertises in tools/list.
        requires_network: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policy_classification_preserves_untrusted_hint_boundaries() {
        for (case, hints, side_effect, permission) in [
            (
                "policy_does_not_trust_server_declared_read_only_tool",
                McpToolSafetyHints {
                    read_only_hint: Some(true),
                    destructive_hint: Some(false),
                    open_world_hint: Some(false),
                },
                McpToolSideEffectClass::Unknown,
                McpToolPermissionClass::WriteOrUnknown,
            ),
            (
                "policy_classifies_destructive_tool_as_write_like",
                McpToolSafetyHints {
                    read_only_hint: Some(false),
                    destructive_hint: Some(true),
                    open_world_hint: Some(false),
                },
                McpToolSideEffectClass::WriteLike,
                McpToolPermissionClass::WriteOrUnknown,
            ),
            (
                "policy_classifies_open_world_tool_as_network_like",
                McpToolSafetyHints {
                    read_only_hint: Some(false),
                    destructive_hint: Some(false),
                    open_world_hint: Some(true),
                },
                McpToolSideEffectClass::NetworkLike,
                McpToolPermissionClass::WriteOrUnknown,
            ),
            (
                "policy_classifies_missing_hints_as_unknown",
                McpToolSafetyHints::default(),
                McpToolSideEffectClass::Unknown,
                McpToolPermissionClass::WriteOrUnknown,
            ),
        ] {
            let classification = classify_mcp_tool_policy(hints);
            assert_eq!(
                classification.side_effect_class, side_effect,
                "{case}: side effect"
            );
            assert_eq!(
                classification.permission_class, permission,
                "{case}: permission"
            );
            assert!(classification.requires_network, "{case}: requires network");
        }
    }
}
