use pioneer_protocol::{
    CLIAgentRuntimeKind, TurnCLIRuntimeOptions, TurnExecutionSecuritySnapshot, TurnPermissionMode,
    TurnPermissionProfileSelection, TurnPermissionProfileSnapshot, TurnSandboxMode,
    default_turn_permission_profile_snapshot, resolve_turn_permission_profile,
};
use serde_json::{Value as JsonValue, json};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ClaudeMcpPermissionFallbackDecision {
    AllowExact,
    Deny { reason: String },
}

pub(crate) fn claude_mcp_permission_fallback_response(
    decision: ClaudeMcpPermissionFallbackDecision,
    original_input: &JsonValue,
) -> JsonValue {
    match decision {
        ClaudeMcpPermissionFallbackDecision::AllowExact => json!({
            "behavior": "allow",
            "updatedInput": original_input,
        }),
        ClaudeMcpPermissionFallbackDecision::Deny { reason } => json!({
            "behavior": "deny",
            "message": reason,
        }),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CLIRuntimePermissionMappingQuality {
    Exact,
    StricterFallback,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CLIRuntimePermissionAdapterOutput {
    pub runtime_kind: CLIAgentRuntimeKind,
    pub profile: TurnPermissionProfileSnapshot,
    pub approval_policy: String,
    pub provider_mode_label: String,
    pub mapping_quality: CLIRuntimePermissionMappingQuality,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CLIRuntimePermissionAdapterResult {
    pub output: CLIRuntimePermissionAdapterOutput,
    pub options: TurnCLIRuntimeOptions,
}

pub(crate) fn adapt_cli_runtime_permissions_for_turn(
    runtime_kind: CLIAgentRuntimeKind,
    selection: Option<&TurnPermissionProfileSelection>,
    runtime_options: Option<TurnCLIRuntimeOptions>,
) -> CLIRuntimePermissionAdapterResult {
    let profile = selection
        .map(|selection| resolve_turn_permission_profile(Some(selection)))
        .unwrap_or_else(default_turn_permission_profile_snapshot);
    let base = adapter_base_mapping(runtime_kind, profile.mode);
    let mapping_quality = base.mapping_quality;
    let notes = base.notes;
    let approval_policy = base.approval_policy.to_owned();

    let mut options = runtime_options.unwrap_or_else(empty_cli_runtime_options);
    options.sandbox = None;

    CLIRuntimePermissionAdapterResult {
        output: CLIRuntimePermissionAdapterOutput {
            runtime_kind,
            profile,
            approval_policy: approval_policy.clone(),
            provider_mode_label: approval_policy,
            mapping_quality,
            notes,
        },
        options,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BaseMapping {
    approval_policy: &'static str,
    mapping_quality: CLIRuntimePermissionMappingQuality,
    notes: Vec<String>,
}

fn adapter_base_mapping(
    runtime_kind: CLIAgentRuntimeKind,
    mode: TurnPermissionMode,
) -> BaseMapping {
    match runtime_kind {
        CLIAgentRuntimeKind::Codex => codex_base_mapping(mode),
        CLIAgentRuntimeKind::Claude => claude_base_mapping(mode),
    }
}

fn codex_base_mapping(mode: TurnPermissionMode) -> BaseMapping {
    match mode {
        TurnPermissionMode::FullAccess => BaseMapping {
            approval_policy: "never",
            mapping_quality: CLIRuntimePermissionMappingQuality::Exact,
            notes: Vec::new(),
        },
        TurnPermissionMode::AutoAcceptEdits => BaseMapping {
            approval_policy: "on-request",
            mapping_quality: CLIRuntimePermissionMappingQuality::StricterFallback,
            notes: vec![
                "Codex does not expose a distinct Pioneer auto_accept_edits policy; on-request is the supported stricter fallback".to_owned(),
            ],
        },
        TurnPermissionMode::Supervised => BaseMapping {
            approval_policy: "on-request",
            mapping_quality: CLIRuntimePermissionMappingQuality::Exact,
            notes: Vec::new(),
        },
    }
}

fn claude_base_mapping(mode: TurnPermissionMode) -> BaseMapping {
    match mode {
        TurnPermissionMode::FullAccess => BaseMapping {
            approval_policy: "bypassPermissions",
            mapping_quality: CLIRuntimePermissionMappingQuality::Exact,
            notes: Vec::new(),
        },
        TurnPermissionMode::AutoAcceptEdits => BaseMapping {
            approval_policy: "acceptEdits",
            mapping_quality: CLIRuntimePermissionMappingQuality::Exact,
            notes: Vec::new(),
        },
        TurnPermissionMode::Supervised => BaseMapping {
            approval_policy: "default",
            mapping_quality: CLIRuntimePermissionMappingQuality::Exact,
            notes: Vec::new(),
        },
    }
}

fn empty_cli_runtime_options() -> TurnCLIRuntimeOptions {
    TurnCLIRuntimeOptions {
        sandbox: None,
        effort: None,
        personality: None,
        summary: None,
        steer_if_active: None,
    }
}

pub(crate) fn codex_permissions_profile_for_security_snapshot(
    snapshot: &TurnExecutionSecuritySnapshot,
) -> &'static str {
    match snapshot.sandbox.mode {
        TurnSandboxMode::Unrestricted => ":danger-full-access",
        TurnSandboxMode::ReadOnly => ":read-only",
        TurnSandboxMode::WorkspaceWrite => ":workspace",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        CLIAgentRuntimeSandboxPolicy, TurnExecutionSecuritySnapshot,
        TurnPermissionProfileSelection, TurnPermissionProfileSource,
    };

    fn selection(mode: TurnPermissionMode) -> TurnPermissionProfileSelection {
        TurnPermissionProfileSelection { mode }
    }

    #[test]
    fn omitted_profile_defaults_to_full_access_provider_policy() {
        let result = adapt_cli_runtime_permissions_for_turn(
            CLIAgentRuntimeKind::Codex,
            None,
            Some(TurnCLIRuntimeOptions {
                sandbox: None,
                effort: Some("high".to_owned()),
                personality: None,
                summary: None,
                steer_if_active: None,
            }),
        );

        assert_eq!(result.output.profile.mode, TurnPermissionMode::FullAccess);
        assert_eq!(result.output.approval_policy, "never");
        assert_eq!(result.options.sandbox, None);
        assert_eq!(
            result.output.mapping_quality,
            CLIRuntimePermissionMappingQuality::Exact
        );
        assert_eq!(result.options.effort.as_deref(), Some("high"));
    }

    #[test]
    fn restricted_profile_maps_to_provider_policy() {
        let result = adapt_cli_runtime_permissions_for_turn(
            CLIAgentRuntimeKind::Codex,
            Some(&selection(TurnPermissionMode::Supervised)),
            Some(TurnCLIRuntimeOptions {
                sandbox: None,
                effort: None,
                personality: None,
                summary: None,
                steer_if_active: None,
            }),
        );

        assert_eq!(result.output.approval_policy, "on-request");
        assert_eq!(result.options.sandbox, None);
    }

    #[test]
    fn codex_security_supervised_profile_does_not_emit_pre_snapshot_sandbox() {
        let result = adapt_cli_runtime_permissions_for_turn(
            CLIAgentRuntimeKind::Codex,
            Some(&selection(TurnPermissionMode::Supervised)),
            None,
        );

        assert_eq!(result.output.approval_policy, "on-request");
        assert_eq!(result.options.sandbox, None);
    }

    #[test]
    fn codex_profiles_map_to_supported_approval_policies() {
        let cases = [
            (
                TurnPermissionMode::FullAccess,
                "never",
                CLIRuntimePermissionMappingQuality::Exact,
            ),
            (
                TurnPermissionMode::AutoAcceptEdits,
                "on-request",
                CLIRuntimePermissionMappingQuality::StricterFallback,
            ),
            (
                TurnPermissionMode::Supervised,
                "on-request",
                CLIRuntimePermissionMappingQuality::Exact,
            ),
        ];

        for (mode, expected_policy, expected_quality) in cases {
            let result = adapt_cli_runtime_permissions_for_turn(
                CLIAgentRuntimeKind::Codex,
                Some(&selection(mode)),
                None,
            );

            assert_eq!(result.output.approval_policy, expected_policy);
            assert_eq!(result.output.mapping_quality, expected_quality);
        }
    }

    #[test]
    fn profile_mapping_preserves_non_permission_runtime_options() {
        let result = adapt_cli_runtime_permissions_for_turn(
            CLIAgentRuntimeKind::Claude,
            Some(&selection(TurnPermissionMode::AutoAcceptEdits)),
            Some(TurnCLIRuntimeOptions {
                sandbox: None,
                effort: None,
                personality: Some("concise".to_owned()),
                summary: None,
                steer_if_active: None,
            }),
        );

        assert_eq!(result.output.approval_policy, "acceptEdits");
        assert_eq!(
            result.output.mapping_quality,
            CLIRuntimePermissionMappingQuality::Exact
        );
        assert_eq!(result.options.personality.as_deref(), Some("concise"));
    }

    #[test]
    fn claude_profiles_map_to_supported_permission_modes() {
        let cases = [
            (
                TurnPermissionMode::FullAccess,
                "bypassPermissions",
                CLIRuntimePermissionMappingQuality::Exact,
            ),
            (
                TurnPermissionMode::AutoAcceptEdits,
                "acceptEdits",
                CLIRuntimePermissionMappingQuality::Exact,
            ),
            (
                TurnPermissionMode::Supervised,
                "default",
                CLIRuntimePermissionMappingQuality::Exact,
            ),
        ];

        for (mode, expected_policy, expected_quality) in cases {
            let result = adapt_cli_runtime_permissions_for_turn(
                CLIAgentRuntimeKind::Claude,
                Some(&selection(mode)),
                None,
            );

            assert_eq!(result.output.approval_policy, expected_policy);
            assert_eq!(result.output.mapping_quality, expected_quality);
        }
    }

    #[test]
    fn claude_cli_runtime_full_access_permission_mapping_uses_bypass_permissions() {
        let result = adapt_cli_runtime_permissions_for_turn(
            CLIAgentRuntimeKind::Claude,
            Some(&selection(TurnPermissionMode::FullAccess)),
            None,
        );

        assert_eq!(result.output.approval_policy, "bypassPermissions");
        assert_eq!(
            result.output.mapping_quality,
            CLIRuntimePermissionMappingQuality::Exact
        );
        assert_eq!(result.options.sandbox, None);
    }

    #[test]
    fn adapter_preserves_non_permission_cli_options() {
        let result = adapt_cli_runtime_permissions_for_turn(
            CLIAgentRuntimeKind::Codex,
            Some(&selection(TurnPermissionMode::FullAccess)),
            Some(TurnCLIRuntimeOptions {
                sandbox: None,
                effort: Some("low".to_owned()),
                personality: Some("direct".to_owned()),
                summary: Some("brief".to_owned()),
                steer_if_active: Some(true),
            }),
        );

        assert_eq!(result.options.effort.as_deref(), Some("low"));
        assert_eq!(result.options.personality.as_deref(), Some("direct"));
        assert_eq!(result.options.summary.as_deref(), Some("brief"));
        assert_eq!(result.options.steer_if_active, Some(true));
        assert_eq!(result.options.sandbox, None);
    }

    #[test]
    fn codex_permission_adapter_ignores_legacy_sandbox_option() {
        let result = adapt_cli_runtime_permissions_for_turn(
            CLIAgentRuntimeKind::Codex,
            Some(&selection(TurnPermissionMode::FullAccess)),
            Some(TurnCLIRuntimeOptions {
                sandbox: Some(CLIAgentRuntimeSandboxPolicy(serde_json::json!({
                    "type": "workspaceWrite",
                    "networkAccess": false
                }))),
                effort: None,
                personality: None,
                summary: None,
                steer_if_active: None,
            }),
        );

        assert_eq!(result.output.approval_policy, "never");
        assert_eq!(result.options.sandbox, None);
    }

    #[test]
    fn codex_security_snapshot_read_only_uses_permissions_profile() {
        let snapshot = TurnExecutionSecuritySnapshot::read_only(
            pioneer_protocol::TurnPermissionProfileSnapshot::from_mode(
                TurnPermissionMode::Supervised,
                TurnPermissionProfileSource::Composer,
            ),
            "/tmp/workspace",
            Vec::new(),
            1_700_000_000_000,
        );

        assert_eq!(
            codex_permissions_profile_for_security_snapshot(&snapshot),
            ":read-only"
        );
    }

    #[test]
    fn codex_security_snapshot_workspace_write_uses_permissions_profile() {
        let snapshot = TurnExecutionSecuritySnapshot::workspace_write(
            pioneer_protocol::TurnPermissionProfileSnapshot::from_mode(
                TurnPermissionMode::AutoAcceptEdits,
                TurnPermissionProfileSource::Composer,
            ),
            "/tmp/workspace",
            Vec::new(),
            1_700_000_000_000,
        );

        assert_eq!(
            codex_permissions_profile_for_security_snapshot(&snapshot),
            ":workspace"
        );
    }
}
