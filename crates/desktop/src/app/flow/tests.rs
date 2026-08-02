use super::{
    default_user_command_bin_dir_label, popover_view::gateway_endpoint_subtitle,
    warning_notification_messages,
};
use crate::app::root::{
    composer_capability_target_for_provider, composer_submission_plan_for_provider,
};
use crate::gateway::{GatewayInstallWarning, GatewayRuntime};
use pioneer_client::composer::capabilities::{
    COMPOSER_CAPABILITY_MATRIX, ComposerCapability, ComposerCapabilityKind,
    ComposerCapabilityPolicy, ComposerCapabilityTarget, filter_composer_capabilities_for_target,
};
use pioneer_client::gateway::{
    endpoint::GatewayBaseUrl,
    types::{GatewayEndpoint, GatewayEndpointKind},
};
use pioneer_protocol::{
    CLIAgentRuntimeKind, McpScopeKind, RuntimeCapabilities, RuntimeStatus, RuntimeSummary, SkillId,
    skill_capability_key,
};
use std::time::Duration;

fn desktop_skill_capability(
    seed: char,
    owner: Option<&str>,
    slug: &str,
    source_kind: &str,
    label: &str,
) -> ComposerCapability {
    let skill_id = SkillId::new(seed.to_string().repeat(21)).expect("valid test skill id");
    ComposerCapability {
        id: skill_capability_key(&skill_id),
        label: label.to_owned(),
        kind: ComposerCapabilityKind::Skill {
            skill_id,
            owner: owner.map(str::to_owned),
            slug: slug.to_owned(),
            source_kind: source_kind.to_owned(),
        },
    }
}

#[test]
fn local_ws_connect_spec_keeps_configured_timeout() {
    let runtime = GatewayRuntime::for_ws_spec_tests();
    assert_eq!(
        runtime.ws_timings().connect_timeout,
        Duration::from_millis(300)
    );
}

#[test]
fn gateway_subtitles_interpolate_canonical_address() {
    rust_i18n::set_locale("en");

    let endpoint = |kind, address: &str| GatewayEndpoint {
        id: "gateway-id".to_owned(),
        name: "Gateway".to_owned(),
        gateway_base_url: GatewayBaseUrl::parse_presentation(address)
            .expect("valid gateway base URL"),
        kind,
        session_ref: None,
        server_gateway_id: None,
        workspace_id: None,
        service_name: None,
    };

    let local = gateway_endpoint_subtitle(&endpoint(
        GatewayEndpointKind::Local,
        "http://127.0.0.1:17878/",
    ));
    let remote = gateway_endpoint_subtitle(&endpoint(
        GatewayEndpointKind::Remote,
        "https://relay.example.com/pioneer/",
    ));

    assert_eq!(local, "Local - http://127.0.0.1:17878/");
    assert_eq!(remote, "Remote - https://relay.example.com/pioneer/");
    assert!(!local.contains("%{"));
    assert!(!remote.contains("%{"));
}

#[test]
fn warning_notification_uses_friendly_path_message_for_path_update_warning() {
    let warnings = vec![GatewayInstallWarning {
        code: "path_update_skipped".to_owned(),
        message: "failed to update profile".to_owned(),
    }];

    let messages = warning_notification_messages(&warnings);
    assert_eq!(messages.len(), 1);
    assert!(messages[0].contains(default_user_command_bin_dir_label()));
    assert!(!messages[0].contains("failed to update profile"));
}

#[test]
fn warning_notification_keeps_one_message_per_warning() {
    let warnings = vec![
        GatewayInstallWarning {
            code: "path_update_skipped".to_owned(),
            message: "first".to_owned(),
        },
        GatewayInstallWarning {
            code: "path_update_skipped".to_owned(),
            message: "second".to_owned(),
        },
        GatewayInstallWarning {
            code: "other_warning".to_owned(),
            message: "third".to_owned(),
        },
    ];

    let messages = warning_notification_messages(&warnings);
    assert_eq!(messages.len(), 3);
    assert!(messages[0].contains(default_user_command_bin_dir_label()));
    assert!(messages[1].contains(default_user_command_bin_dir_label()));
    assert_eq!(messages[2], "third");
}

#[test]
fn desktop_composer_cli_target_and_capability_matrix_match_runtime_summary() {
    let runtime = RuntimeSummary {
        runtime_id: "codex".to_owned(),
        kind: CLIAgentRuntimeKind::Codex,
        display_name: "Codex".to_owned(),
        enabled: true,
        status: RuntimeStatus::Ready,
        capabilities: RuntimeCapabilities {
            supports_skills: true,
            supports_mcp_tools: true,
            ..Default::default()
        },
        account: None,
        version: None,
        binary_path: None,
        home_path: None,
        shadow_home_path: None,
        proxy_url: None,
        debug_native_events_enabled: false,
        models_refreshed_at_unix_ms: None,
        diagnostics: Vec::new(),
        recent_stderr: Vec::new(),
    };
    let capabilities = vec![
        desktop_skill_capability('U', None, "user", "user", "User"),
        desktop_skill_capability('S', Some("pioneer"), "system", "system", "System"),
        ComposerCapability {
            id: "server".to_owned(),
            label: "Docs".to_owned(),
            kind: ComposerCapabilityKind::McpServer {
                name: "docs".to_owned(),
                scope_kind: McpScopeKind::Workspace,
            },
        },
        ComposerCapability {
            id: "tool".to_owned(),
            label: "Issues / search".to_owned(),
            kind: ComposerCapabilityKind::McpTool {
                server_name: "issues".to_owned(),
                raw_tool_name: "search".to_owned(),
                scope_kind: McpScopeKind::Workspace,
            },
        },
    ];

    let target = composer_capability_target_for_provider(
        Some("cli_runtime:codex"),
        std::slice::from_ref(&runtime),
    );
    assert_eq!(target.policy().supports_skills, true);
    assert_eq!(target.policy().supports_mcp_tools, true);
    assert_eq!(
        filter_composer_capabilities_for_target(capabilities.as_slice(), target)
            .iter()
            .map(|capability| capability.id.as_str())
            .collect::<Vec<_>>(),
        vec![
            "skill:UUUUUUUUUUUUUUUUUUUUU",
            "skill:SSSSSSSSSSSSSSSSSSSSS",
            "server",
            "tool",
        ]
    );
    assert_eq!(
        composer_capability_target_for_provider(Some("cli_runtime:missing"), &[runtime]),
        ComposerCapabilityTarget::cli(ComposerCapabilityPolicy::unsupported_cli())
    );
    assert_eq!(
        composer_capability_target_for_provider(Some("openai"), &[]),
        ComposerCapabilityTarget::native()
    );
}

#[test]
fn desktop_composer_keeps_duplicate_skill_labels_as_exact_id_rows() {
    let first = desktop_skill_capability('A', Some("alex"), "humanizer", "user", "alex/humanizer");
    let second = desktop_skill_capability('B', Some("alex"), "humanizer", "user", "alex/humanizer");

    let rows = filter_composer_capabilities_for_target(
        &[first.clone(), second.clone()],
        ComposerCapabilityTarget::native(),
    );

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].label, rows[1].label);
    assert_ne!(rows[0].id, rows[1].id);
    assert_eq!(rows[0].id, first.id);
    assert_eq!(rows[1].id, second.id);
    assert!(matches!(
        (&rows[0].kind, &rows[1].kind),
        (
            ComposerCapabilityKind::Skill {
                skill_id: first_id,
                ..
            },
            ComposerCapabilityKind::Skill {
                skill_id: second_id,
                ..
            }
        ) if first_id != second_id
    ));
}

#[test]
fn desktop_submission_preserves_explicit_mcp_when_presentation_catalog_is_stale() {
    let capabilities = vec![
        ComposerCapability {
            id: "server".to_owned(),
            label: "App Store Connect".to_owned(),
            kind: ComposerCapabilityKind::McpServer {
                name: "appstoreconnect".to_owned(),
                scope_kind: McpScopeKind::Workspace,
            },
        },
        desktop_skill_capability('M', Some("pioneer"), "memory", "system", "Memory"),
    ];

    assert_eq!(
        composer_capability_target_for_provider(Some("cli_runtime:codex"), &[]),
        ComposerCapabilityTarget::cli(ComposerCapabilityPolicy::unsupported_cli())
    );
    let text = composer_submission_plan_for_provider(
        Some("cli_runtime:codex"),
        "inspect releases",
        false,
        capabilities.as_slice(),
    );
    let voice = composer_submission_plan_for_provider(
        Some("cli_runtime:codex"),
        "",
        true,
        capabilities.as_slice(),
    );

    assert_eq!(text.capabilities, voice.capabilities);
    assert_eq!(text.removed, voice.removed);
    assert_eq!(text.capabilities.len(), 2);
    assert_eq!(text.capabilities[0].id, "server");
    assert_eq!(text.capabilities[1].id, "skill:MMMMMMMMMMMMMMMMMMMMM");
    assert!(text.has_composer_payload);
    assert!(voice.has_composer_payload);
}

#[test]
fn desktop_composer_cli_policy_filters_skills_and_mcp_independently() {
    assert_eq!(
        COMPOSER_CAPABILITY_MATRIX
            .iter()
            .map(|case| case.id)
            .collect::<Vec<_>>(),
        vec![
            "native",
            "cli_neither",
            "cli_skills_only",
            "cli_mcp_only",
            "cli_both",
        ]
    );
    let runtime =
        |runtime_id: &str, supports_skills: bool, supports_mcp_tools: bool| -> RuntimeSummary {
            RuntimeSummary {
                runtime_id: runtime_id.to_owned(),
                kind: CLIAgentRuntimeKind::Codex,
                display_name: runtime_id.to_owned(),
                enabled: true,
                status: RuntimeStatus::Ready,
                capabilities: RuntimeCapabilities {
                    supports_skills,
                    supports_mcp_tools,
                    ..Default::default()
                },
                account: None,
                version: None,
                binary_path: None,
                home_path: None,
                shadow_home_path: None,
                proxy_url: None,
                debug_native_events_enabled: false,
                models_refreshed_at_unix_ms: None,
                diagnostics: Vec::new(),
                recent_stderr: Vec::new(),
            }
        };
    let runtimes = vec![
        runtime("neither", false, false),
        runtime("skills", true, false),
        runtime("mcp", false, true),
        runtime("both", true, true),
    ];

    let policy = |runtime_id: &str| {
        composer_capability_target_for_provider(
            Some(format!("cli_runtime:{runtime_id}").as_str()),
            runtimes.as_slice(),
        )
        .policy()
    };

    assert_eq!(policy("neither").supports_skills, false);
    assert_eq!(policy("neither").supports_mcp_tools, false);
    assert_eq!(policy("skills").supports_skills, true);
    assert_eq!(policy("skills").supports_mcp_tools, false);
    assert_eq!(policy("mcp").supports_skills, false);
    assert_eq!(policy("mcp").supports_mcp_tools, true);
    assert_eq!(policy("both").supports_skills, true);
    assert_eq!(policy("both").supports_mcp_tools, true);
}

#[test]
fn desktop_composer_cli_target_fails_closed_for_unavailable_runtime() {
    let runtime = RuntimeSummary {
        runtime_id: "codex".to_owned(),
        kind: CLIAgentRuntimeKind::Codex,
        display_name: "Codex".to_owned(),
        enabled: true,
        status: RuntimeStatus::NeedsAuth,
        capabilities: RuntimeCapabilities {
            supports_skills: true,
            ..Default::default()
        },
        account: None,
        version: None,
        binary_path: None,
        home_path: None,
        shadow_home_path: None,
        proxy_url: None,
        debug_native_events_enabled: false,
        models_refreshed_at_unix_ms: None,
        diagnostics: Vec::new(),
        recent_stderr: Vec::new(),
    };

    assert_eq!(
        composer_capability_target_for_provider(Some("cli_runtime:codex"), &[runtime]),
        ComposerCapabilityTarget::cli(ComposerCapabilityPolicy::unsupported_cli())
    );
}
