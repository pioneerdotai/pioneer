use pioneer_protocol::{
    CLIAgentRuntimeApprovalPolicy, CLIAgentRuntimeKind, TurnCLIRuntimeOptions, TurnPermissionMode,
    TurnPermissionProfileSelection, TurnPermissionProfileSnapshot,
    default_turn_permission_profile_snapshot, resolve_turn_permission_profile,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CLIRuntimePermissionMappingQuality {
    Exact,
    StricterFallback,
    LegacyNarrowed,
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
    legacy_options: Option<TurnCLIRuntimeOptions>,
) -> CLIRuntimePermissionAdapterResult {
    let profile = selection
        .map(|selection| resolve_turn_permission_profile(Some(selection)))
        .unwrap_or_else(default_turn_permission_profile_snapshot);
    let base = adapter_base_mapping(runtime_kind, profile.mode);
    let legacy_policy = legacy_options
        .as_ref()
        .and_then(|options| options.approval_policy.as_ref())
        .map(|policy| policy.0.trim())
        .filter(|policy| !policy.is_empty());
    let legacy = legacy_policy.and_then(|policy| classify_provider_policy(runtime_kind, policy));
    let mut mapping_quality = base.mapping_quality;
    let mut notes = base.notes;
    let approval_policy = match legacy {
        Some(legacy) if legacy.rank > base.rank => {
            mapping_quality = CLIRuntimePermissionMappingQuality::LegacyNarrowed;
            notes.push(format!(
                "legacy CLI approval policy `{}` is stricter than generated `{}` and was preserved",
                legacy.value, base.approval_policy
            ));
            legacy.value.to_owned()
        }
        Some(legacy) if legacy.rank == base.rank => legacy.value.to_owned(),
        Some(legacy) => {
            notes.push(format!(
                "legacy CLI approval policy `{}` would broaden Pioneer `{}` profile and was ignored",
                legacy.value,
                profile.mode.as_str()
            ));
            base.approval_policy.to_owned()
        }
        None => {
            if let Some(policy) = legacy_policy {
                if profile.mode == TurnPermissionMode::FullAccess {
                    notes.push(format!(
                        "unclassified legacy CLI approval policy `{policy}` preserved because Pioneer profile is full_access"
                    ));
                    policy.to_owned()
                } else {
                    notes.push(format!(
                        "unclassified legacy CLI approval policy `{policy}` ignored for restricted Pioneer profile"
                    ));
                    base.approval_policy.to_owned()
                }
            } else {
                base.approval_policy.to_owned()
            }
        }
    };

    let mut options = legacy_options.unwrap_or_else(empty_cli_runtime_options);
    options.approval_policy = Some(CLIAgentRuntimeApprovalPolicy(approval_policy.clone()));

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProviderPolicy<'a> {
    value: &'a str,
    rank: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BaseMapping {
    approval_policy: &'static str,
    rank: u8,
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
            rank: 0,
            mapping_quality: CLIRuntimePermissionMappingQuality::Exact,
            notes: Vec::new(),
        },
        TurnPermissionMode::AutoAcceptEdits => BaseMapping {
            approval_policy: "on-request",
            rank: 1,
            mapping_quality: CLIRuntimePermissionMappingQuality::StricterFallback,
            notes: vec![
                "Codex does not expose a distinct Pioneer auto_accept_edits policy; on-request is the supported stricter fallback".to_owned(),
            ],
        },
        TurnPermissionMode::Supervised => BaseMapping {
            approval_policy: "on-request",
            rank: 1,
            mapping_quality: CLIRuntimePermissionMappingQuality::Exact,
            notes: Vec::new(),
        },
    }
}

fn claude_base_mapping(mode: TurnPermissionMode) -> BaseMapping {
    match mode {
        TurnPermissionMode::FullAccess => BaseMapping {
            approval_policy: "bypassPermissions",
            rank: 0,
            mapping_quality: CLIRuntimePermissionMappingQuality::Exact,
            notes: Vec::new(),
        },
        TurnPermissionMode::AutoAcceptEdits => BaseMapping {
            approval_policy: "acceptEdits",
            rank: 1,
            mapping_quality: CLIRuntimePermissionMappingQuality::Exact,
            notes: Vec::new(),
        },
        TurnPermissionMode::Supervised => BaseMapping {
            approval_policy: "default",
            rank: 2,
            mapping_quality: CLIRuntimePermissionMappingQuality::Exact,
            notes: Vec::new(),
        },
    }
}

fn classify_provider_policy(
    runtime_kind: CLIAgentRuntimeKind,
    policy: &str,
) -> Option<ProviderPolicy<'_>> {
    match runtime_kind {
        CLIAgentRuntimeKind::Codex => classify_codex_policy(policy),
        CLIAgentRuntimeKind::Claude => classify_claude_policy(policy),
    }
}

fn classify_codex_policy(policy: &str) -> Option<ProviderPolicy<'_>> {
    let normalized = normalize_policy_token(policy);
    let rank = match normalized.as_str() {
        "never" => 0,
        "onrequest" | "unlesstrusted" | "onfailure" => 1,
        "always" | "ask" => 2,
        _ => return None,
    };
    Some(ProviderPolicy {
        value: policy.trim(),
        rank,
    })
}

fn classify_claude_policy(policy: &str) -> Option<ProviderPolicy<'_>> {
    let normalized = normalize_policy_token(policy);
    let rank = match normalized.as_str() {
        "bypasspermissions" => 0,
        "acceptedits" => 1,
        "default" | "ask" => 2,
        _ => return None,
    };
    Some(ProviderPolicy {
        value: policy.trim(),
        rank,
    })
}

fn normalize_policy_token(policy: &str) -> String {
    policy
        .trim()
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

fn empty_cli_runtime_options() -> TurnCLIRuntimeOptions {
    TurnCLIRuntimeOptions {
        approval_policy: None,
        sandbox: None,
        effort: None,
        personality: None,
        summary: None,
        steer_if_active: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::TurnPermissionProfileSelection;

    fn selection(mode: TurnPermissionMode) -> TurnPermissionProfileSelection {
        TurnPermissionProfileSelection { mode }
    }

    #[test]
    fn omitted_profile_defaults_to_full_access_provider_policy() {
        let result = adapt_cli_runtime_permissions_for_turn(
            CLIAgentRuntimeKind::Codex,
            None,
            Some(TurnCLIRuntimeOptions {
                approval_policy: None,
                sandbox: None,
                effort: Some("high".to_owned()),
                personality: None,
                summary: None,
                steer_if_active: None,
            }),
        );

        assert_eq!(result.output.profile.mode, TurnPermissionMode::FullAccess);
        assert_eq!(result.output.approval_policy, "never");
        assert_eq!(
            result.output.mapping_quality,
            CLIRuntimePermissionMappingQuality::Exact
        );
        assert_eq!(
            result
                .options
                .approval_policy
                .as_ref()
                .map(|policy| policy.0.as_str()),
            Some("never")
        );
        assert_eq!(result.options.effort.as_deref(), Some("high"));
    }

    #[test]
    fn restricted_profile_ignores_legacy_broader_policy() {
        let result = adapt_cli_runtime_permissions_for_turn(
            CLIAgentRuntimeKind::Codex,
            Some(&selection(TurnPermissionMode::Supervised)),
            Some(TurnCLIRuntimeOptions {
                approval_policy: Some(CLIAgentRuntimeApprovalPolicy("never".to_owned())),
                sandbox: None,
                effort: None,
                personality: None,
                summary: None,
                steer_if_active: None,
            }),
        );

        assert_eq!(result.output.approval_policy, "on-request");
        assert_eq!(
            result
                .options
                .approval_policy
                .as_ref()
                .map(|policy| policy.0.as_str()),
            Some("on-request")
        );
        assert!(
            result
                .output
                .notes
                .iter()
                .any(|note| note.contains("would broaden"))
        );
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
            assert_eq!(
                result
                    .options
                    .approval_policy
                    .as_ref()
                    .map(|policy| policy.0.as_str()),
                Some(expected_policy)
            );
        }
    }

    #[test]
    fn legacy_stricter_policy_may_narrow_generated_profile() {
        let result = adapt_cli_runtime_permissions_for_turn(
            CLIAgentRuntimeKind::Claude,
            Some(&selection(TurnPermissionMode::AutoAcceptEdits)),
            Some(TurnCLIRuntimeOptions {
                approval_policy: Some(CLIAgentRuntimeApprovalPolicy("default".to_owned())),
                sandbox: None,
                effort: None,
                personality: Some("concise".to_owned()),
                summary: None,
                steer_if_active: None,
            }),
        );

        assert_eq!(result.output.approval_policy, "default");
        assert_eq!(
            result.output.mapping_quality,
            CLIRuntimePermissionMappingQuality::LegacyNarrowed
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
            assert_eq!(
                result
                    .options
                    .approval_policy
                    .as_ref()
                    .map(|policy| policy.0.as_str()),
                Some(expected_policy)
            );
        }
    }

    #[test]
    fn adapter_preserves_non_permission_cli_options() {
        let result = adapt_cli_runtime_permissions_for_turn(
            CLIAgentRuntimeKind::Codex,
            Some(&selection(TurnPermissionMode::FullAccess)),
            Some(TurnCLIRuntimeOptions {
                approval_policy: None,
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
    }
}
