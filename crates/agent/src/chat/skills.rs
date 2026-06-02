use crate::{AgentMcpAvailability, SkillsLoopConfig, WorkspaceSkillPolicy};
use pioneer_protocol::{TurnSkillBinding, UserInput};
use pioneer_skills::{
    ResolvedSkill, SkillAuditEvent, SkillCatalogLoadParams, SkillExplicitRef, SkillPolicy,
    SkillPolicyKey, SkillPolicySet, SkillPromptBudget, SkillResolutionInput, SkillResolutionResult,
    SkillRuntimePlan, SkillValidationPolicy, build_skill_prompt, build_skill_runtime_plan,
    load_catalog, qualified_skill_slug, resolve_skills,
};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

const WORKSPACE_ID_TOKEN: &str = "{workspaceId}";

#[derive(Debug, Clone)]
pub(super) struct TurnSkillResolution {
    pub prompt: String,
    pub result: SkillResolutionResult,
    pub runtime_plan: SkillRuntimePlan,
    pub audit_events: Vec<SkillAuditEvent>,
}

fn expand_home(path: &str) -> String {
    if path == "~" {
        return dirs::home_dir()
            .map(|home| home.display().to_string())
            .unwrap_or_else(|| path.to_owned());
    }

    if let Some(suffix) = path.strip_prefix("~/")
        && let Some(home) = dirs::home_dir()
    {
        return home.join(suffix).display().to_string();
    }

    path.to_owned()
}

fn sanitize_workspace_id_component(workspace_id: &str) -> String {
    let mut sanitized = String::with_capacity(workspace_id.len());
    for ch in workspace_id.trim().chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
            sanitized.push(ch);
        } else {
            sanitized.push('_');
        }
    }

    if sanitized.is_empty() || sanitized == "." || sanitized == ".." {
        "workspace".to_owned()
    } else {
        sanitized
    }
}

fn expand_workspace_id_token(path: &str, workspace_id: &str) -> String {
    if !path.contains(WORKSPACE_ID_TOKEN) {
        return path.to_owned();
    }

    let sanitized_workspace_id = sanitize_workspace_id_component(workspace_id);
    path.replace(WORKSPACE_ID_TOKEN, sanitized_workspace_id.as_str())
}

fn resolve_root_path(raw: &str, workdir: &Path, workspace_id: &str) -> PathBuf {
    let expanded_workspace = expand_workspace_id_token(raw, workspace_id);
    let expanded = expand_home(expanded_workspace.as_str());
    let candidate = PathBuf::from(expanded);
    if candidate.is_absolute() {
        candidate
    } else {
        workdir.join(candidate)
    }
}

fn collect_touched_paths(input: &[UserInput]) -> Vec<String> {
    let mut values = HashSet::new();

    for item in input {
        match item {
            UserInput::Text {
                text,
                text_elements,
            } => {
                for element in text_elements {
                    if let Some(placeholder) = element.placeholder.as_deref()
                        && !placeholder.trim().is_empty()
                    {
                        values.insert(placeholder.trim().to_owned());
                    }
                }

                for token in text.split_whitespace() {
                    let trimmed = token.trim_matches(|ch: char| {
                        matches!(
                            ch,
                            ',' | ';' | ':' | '(' | ')' | '[' | ']' | '{' | '}' | '"' | '\''
                        )
                    });
                    if trimmed.contains('/') || trimmed.contains('\\') {
                        values.insert(trimmed.to_owned());
                    }
                }
            }
            UserInput::LocalImage { path }
            | UserInput::LocalFile { path }
            | UserInput::LocalAudio { path }
            | UserInput::LocalVideo { path }
            | UserInput::Mention { path, .. } => {
                if !path.trim().is_empty() {
                    values.insert(path.trim().to_owned());
                }
            }
            UserInput::Image { .. }
            | UserInput::File { .. }
            | UserInput::Audio { .. }
            | UserInput::Video { .. }
            | UserInput::Artifact { .. } => {}
        }
    }

    let mut sorted = values.into_iter().collect::<Vec<_>>();
    sorted.sort();
    sorted
}

pub(super) fn resolve_turn_skills_with_explicit_refs(
    workdir: &Path,
    workspace_id: &str,
    input: &[UserInput],
    explicit_refs: &[SkillExplicitRef],
    skills: &SkillsLoopConfig,
    workspace_skill_policies: &HashMap<SkillPolicyKey, WorkspaceSkillPolicy>,
    mcp_availability: &AgentMcpAvailability,
) -> anyhow::Result<TurnSkillResolution> {
    if !skills.enabled {
        return Ok(TurnSkillResolution {
            prompt: String::new(),
            result: SkillResolutionResult {
                active: Vec::new(),
                excluded: Vec::new(),
            },
            runtime_plan: SkillRuntimePlan {
                tools: Vec::new(),
                read_skill_index: HashMap::new(),
                excluded_tools: Vec::new(),
            },
            audit_events: Vec::new(),
        });
    }

    let catalog = load_catalog(&SkillCatalogLoadParams {
        system_roots: skills
            .system_roots
            .iter()
            .map(|raw| resolve_root_path(raw, workdir, workspace_id))
            .collect(),
        user_roots: skills
            .user_roots
            .iter()
            .map(|raw| resolve_root_path(raw, workdir, workspace_id))
            .collect(),
        registry_roots: skills
            .registry_roots
            .iter()
            .map(|raw| resolve_root_path(raw, workdir, workspace_id))
            .collect(),
        max_skills_per_source: skills.max_skills_per_source,
        max_file_bytes: skills.max_skill_file_bytes,
    })?;

    let mut global_by_key = HashMap::new();
    for skill in &catalog.skills {
        global_by_key.insert(
            SkillPolicyKey::new(
                qualified_skill_slug(skill.identity.owner.as_str(), skill.identity.slug.as_str()),
                skill.identity.source_kind.as_db_value().to_owned(),
            ),
            SkillPolicy {
                enabled: Some(skills.enabled),
                allow_implicit_invocation: Some(skills.allow_implicit_invocation),
            },
        );
    }

    let workspace_by_key = workspace_skill_policies
        .iter()
        .map(|(key, policy)| {
            (
                key.clone(),
                SkillPolicy {
                    enabled: policy.enabled,
                    allow_implicit_invocation: policy.allow_implicit_invocation,
                },
            )
        })
        .collect::<HashMap<_, _>>();

    let touched_paths = collect_touched_paths(input);
    let dependency_input = pioneer_skills::DependencyCheckInput {
        available_mcp: mcp_availability.available_mcp.clone(),
        blocked_mcp: mcp_availability.blocked_mcp.clone(),
        ..pioneer_skills::DependencyCheckInput::baseline()
    };

    let result = resolve_skills(SkillResolutionInput {
        explicit_refs,
        touched_paths: touched_paths.as_slice(),
        catalog: &catalog,
        policy_set: &SkillPolicySet {
            global_by_key,
            workspace_by_key,
        },
        validation_policy: SkillValidationPolicy {
            strict_agentskills: skills.validation.strict_agentskills,
            accept_openclaw_profile: skills.validation.accept_openclaw_profile,
            preflight_on_resolve: skills.dependencies.preflight_on_resolve,
            allow_untrusted_install: skills.security.allow_untrusted_install,
            security_scan_on_resolve: true,
            max_security_scan_file_bytes: skills.security.max_install_file_bytes,
        },
        dependency_input: &dependency_input,
    });

    let now_unix = match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_secs()).unwrap_or(i64::MAX),
        Err(_) => 0,
    };

    let mut audit_events = Vec::new();

    for active in &result.active {
        audit_events.push(SkillAuditEvent::resolution_allowed(
            active.slug.clone(),
            active
                .definition
                .identity
                .source_kind
                .as_db_value()
                .to_owned(),
            serde_json::json!({
                "resolved_reason": active.reason.as_db_value(),
                "trust_level": active.definition.runtime.trust_level,
            }),
            now_unix,
        ));
    }

    for excluded in &result.excluded {
        audit_events.push(SkillAuditEvent::resolution_blocked(
            excluded.slug.clone(),
            excluded.source_kind.clone(),
            format!("resolve.{}", excluded.reason.as_db_value()),
            serde_json::json!({
                "reason": excluded.reason.as_db_value(),
                "dependency_diagnostics": excluded.dependency_diagnostics,
                "security_findings": excluded.security_findings,
            }),
            now_unix,
        ));
    }

    audit_events.sort_by(|left, right| {
        left.skill_slug
            .as_str()
            .cmp(right.skill_slug.as_str())
            .then_with(|| left.action.cmp(&right.action))
    });

    let runtime_plan = build_skill_runtime_plan(
        result.active.as_slice(),
        pioneer_skills::SkillRuntimeBudget {
            enable_dynamic_tools: skills.runtime.enable_dynamic_tools,
            max_dynamic_tools_per_skill: skills.runtime.max_dynamic_tools_per_skill,
            allow_shell_tools: skills.runtime.allow_shell_tools,
            allow_http_tools: skills.runtime.allow_http_tools,
            allow_function_proxy_tools: skills.runtime.allow_function_proxy_tools,
            allow_untrusted_install: skills.security.allow_untrusted_install,
            min_trust_for_shell_tools: skills.security.min_trust_for_shell_tools.clone(),
            min_trust_for_http_tools: skills.security.min_trust_for_http_tools.clone(),
            min_trust_for_function_proxy_tools: skills
                .security
                .min_trust_for_function_proxy_tools
                .clone(),
        },
    );

    let prompt = build_skill_prompt(
        result.active.as_slice(),
        SkillPromptBudget {
            max_chars: skills.prompt_max_chars,
            compact_mode_threshold: skills.runtime.compact_mode_threshold,
            include_read_skill_hint: skills.runtime.enable_read_skill,
        },
    )
    .text;

    Ok(TurnSkillResolution {
        prompt,
        result,
        runtime_plan,
        audit_events,
    })
}

pub(super) fn to_turn_skill_bindings(active: &[ResolvedSkill]) -> Vec<TurnSkillBinding> {
    active
        .iter()
        .map(|skill| TurnSkillBinding {
            skill_slug: skill.slug.clone(),
            skill_version: skill.definition.identity.version_hint.clone(),
            fingerprint: skill.definition.identity.fingerprint.clone(),
            source_kind: skill
                .definition
                .identity
                .source_kind
                .as_db_value()
                .to_owned(),
            resolved_reason: skill.reason.as_db_value().to_owned(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        expand_workspace_id_token, resolve_root_path, resolve_turn_skills_with_explicit_refs,
    };
    use crate::{
        SkillsDependenciesLoopConfig, SkillsLoopConfig, SkillsRuntimeLoopConfig,
        SkillsSecurityLoopConfig, SkillsValidationLoopConfig,
    };
    use pioneer_skills::{SkillExplicitRef, SkillPolicyKey, SkillTrustLevel};
    use std::collections::HashMap;
    use std::fs;
    use std::path::Path;
    use std::path::PathBuf;

    fn temp_case(name: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("unix timestamp")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("pioneer-agent-skills-{name}-{nanos}"));
        fs::create_dir_all(&path).expect("create temp dir");
        path
    }

    fn test_skills_config(root: &std::path::Path) -> SkillsLoopConfig {
        SkillsLoopConfig {
            enabled: true,
            max_skills_per_source: 64,
            max_skill_file_bytes: 1024 * 1024,
            prompt_max_chars: 24_000,
            allow_implicit_invocation: false,
            system_roots: Vec::new(),
            user_roots: vec![root.display().to_string()],
            registry_roots: Vec::new(),
            validation: SkillsValidationLoopConfig {
                strict_agentskills: true,
                accept_openclaw_profile: true,
            },
            security: SkillsSecurityLoopConfig {
                allow_untrusted_install: false,
                min_trust_for_shell_tools: SkillTrustLevel::Verified,
                min_trust_for_http_tools: SkillTrustLevel::Community,
                min_trust_for_function_proxy_tools: SkillTrustLevel::Community,
                max_install_archive_bytes: 10 * 1024 * 1024,
                max_install_archive_compressed_bytes: 10 * 1024 * 1024,
                max_install_archive_uncompressed_bytes: 50 * 1024 * 1024,
                max_install_archive_entries: 2048,
                max_install_file_bytes: 1024 * 1024,
                upload_ttl_secs: 3600,
                upload_recommended_chunk_size_bytes: 256 * 1024,
                upload_max_chunk_size_bytes: 1024 * 1024,
            },
            dependencies: SkillsDependenciesLoopConfig {
                preflight_on_resolve: true,
                runtime_recheck_on_tool_call: true,
            },
            runtime: SkillsRuntimeLoopConfig {
                enable_dynamic_tools: true,
                enable_read_skill: true,
                max_dynamic_tools_per_skill: 64,
                read_skill_max_chars: 24_000,
                compact_mode_threshold: 6,
                allow_shell_tools: true,
                allow_http_tools: true,
                allow_function_proxy_tools: true,
            },
        }
    }

    fn explicit_skill_ref(name: &str) -> SkillExplicitRef {
        SkillExplicitRef {
            capability_id: format!("skill:user:{name}"),
            label: Some(name.to_owned()),
            slug: name.to_owned(),
            source_kind: "user".to_owned(),
        }
    }

    #[test]
    fn workspace_id_token_expands_into_relative_root() {
        let workdir = Path::new("/tmp/workdir");
        let resolved = resolve_root_path(
            "{homeDirectory}/skills/workspace/{workspaceId}",
            workdir,
            "ws_000000000000000001",
        );
        assert_eq!(
            resolved.to_string_lossy(),
            "/tmp/workdir/{homeDirectory}/skills/workspace/ws_000000000000000001"
        );
    }

    #[test]
    fn workspace_id_token_sanitizes_unsafe_chars() {
        let expanded = expand_workspace_id_token(
            "{homeDirectory}/skills/workspace/{workspaceId}",
            "../workspace?01",
        );
        assert_eq!(expanded, "{homeDirectory}/skills/workspace/.._workspace_01");
    }

    #[test]
    fn workspace_id_token_blocks_parent_dir_component() {
        let expanded =
            expand_workspace_id_token("{homeDirectory}/skills/workspace/{workspaceId}", "..");
        assert_eq!(expanded, "{homeDirectory}/skills/workspace/workspace");
    }

    #[test]
    fn skill_activates_when_dependency_exists_only_in_metadata() {
        let root = temp_case("metadata-pass");
        let skill_dir = root.join("tests").join("agent-browser");
        fs::create_dir_all(&skill_dir).expect("create skill dir");
        fs::write(
            skill_dir.join("SKILL.md"),
            r#"---
name: Agent Browser
slug: agent-browser
description: Metadata-only dependency test
metadata: {"clawdbot":{"requires":{"commands":["cargo"]}}}
---
Instructions
"#,
        )
        .expect("write skill");

        let explicit_refs = [explicit_skill_ref("agent-browser")];
        let result = resolve_turn_skills_with_explicit_refs(
            root.as_path(),
            "ws_000000000000000001",
            &[],
            &explicit_refs,
            &test_skills_config(root.as_path()),
            &HashMap::<SkillPolicyKey, crate::WorkspaceSkillPolicy>::new(),
            &crate::AgentMcpAvailability::default(),
        )
        .expect("resolve skills");

        assert_eq!(result.result.active.len(), 1);
        assert_eq!(result.result.active[0].slug, "tests/agent-browser");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn explicit_skill_capability_does_not_expand_body_into_prompt() {
        let root = temp_case("explicit-compact-prompt");
        let skill_dir = root.join("tests").join("weather");
        fs::create_dir_all(&skill_dir).expect("create skill dir");
        fs::write(
            skill_dir.join("SKILL.md"),
            r#"---
name: weather
slug: weather
description: Get weather forecasts.
---
SELECTED SKILL BODY SHOULD ONLY BE AVAILABLE THROUGH read_skill.
"#,
        )
        .expect("write skill");

        let explicit_refs = [explicit_skill_ref("weather")];
        let result = resolve_turn_skills_with_explicit_refs(
            root.as_path(),
            "ws_000000000000000001",
            &[],
            &explicit_refs,
            &test_skills_config(root.as_path()),
            &HashMap::<SkillPolicyKey, crate::WorkspaceSkillPolicy>::new(),
            &crate::AgentMcpAvailability::default(),
        )
        .expect("resolve skills");

        assert_eq!(result.result.active.len(), 1);
        assert_eq!(result.result.active[0].slug, "tests/weather");
        assert!(
            result
                .prompt
                .contains("Skill slug for read_skill: `user:tests/weather`")
        );
        assert!(
            !result
                .prompt
                .contains("SELECTED SKILL BODY SHOULD ONLY BE AVAILABLE")
        );
        assert_eq!(
            result
                .runtime_plan
                .read_skill_index
                .get("user:tests/weather")
                .expect("read_skill entry")
                .body
                .trim(),
            "SELECTED SKILL BODY SHOULD ONLY BE AVAILABLE THROUGH read_skill."
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn catalog_hidden_skill_stays_active_but_is_omitted_from_prompt() {
        let root = temp_case("catalog-hidden-prompt");
        let skill_dir = root.join("tests").join("hidden");
        fs::create_dir_all(&skill_dir).expect("create skill dir");
        fs::write(
            skill_dir.join("SKILL.md"),
            r#"---
name: hidden
slug: hidden
description: Hidden skill prompt catalog entry.
catalog-hide: true
---
HIDDEN SKILL BODY SHOULD ONLY BE AVAILABLE THROUGH read_skill.
"#,
        )
        .expect("write skill");

        let explicit_refs = [explicit_skill_ref("hidden")];
        let result = resolve_turn_skills_with_explicit_refs(
            root.as_path(),
            "ws_000000000000000001",
            &[],
            &explicit_refs,
            &test_skills_config(root.as_path()),
            &HashMap::<SkillPolicyKey, crate::WorkspaceSkillPolicy>::new(),
            &crate::AgentMcpAvailability::default(),
        )
        .expect("resolve skills");

        assert_eq!(result.result.active.len(), 1);
        assert_eq!(result.result.active[0].slug, "tests/hidden");
        assert!(result.prompt.is_empty());
        assert_eq!(
            result
                .runtime_plan
                .read_skill_index
                .get("user:tests/hidden")
                .expect("read_skill entry")
                .body
                .trim(),
            "HIDDEN SKILL BODY SHOULD ONLY BE AVAILABLE THROUGH read_skill."
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn skill_is_excluded_when_metadata_command_dependency_is_missing() {
        let root = temp_case("metadata-missing");
        let skill_dir = root.join("tests").join("agent-browser");
        fs::create_dir_all(&skill_dir).expect("create skill dir");
        fs::write(
            skill_dir.join("SKILL.md"),
            r#"---
name: Agent Browser
slug: agent-browser
description: Metadata-only dependency test
metadata: {"clawdbot":{"requires":{"commands":["definitely-missing-binary-phase25-agent"]}}}
---
Instructions
"#,
        )
        .expect("write skill");

        let explicit_refs = [explicit_skill_ref("agent-browser")];
        let result = resolve_turn_skills_with_explicit_refs(
            root.as_path(),
            "ws_000000000000000001",
            &[],
            &explicit_refs,
            &test_skills_config(root.as_path()),
            &HashMap::<SkillPolicyKey, crate::WorkspaceSkillPolicy>::new(),
            &crate::AgentMcpAvailability::default(),
        )
        .expect("resolve skills");

        assert!(result.result.active.is_empty());
        assert_eq!(result.result.excluded.len(), 1);
        assert_eq!(
            result.result.excluded[0].reason,
            pioneer_skills::SkillExcludedReason::DependencyMissing
        );

        let _ = fs::remove_dir_all(root);
    }
}
