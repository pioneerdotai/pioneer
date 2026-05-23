use pioneer_skills::{
    DependencyCheckInput, SkillExcludedReason, SkillExplicitRef, SkillPolicySet,
    SkillResolutionInput, SkillSourceKind, SkillTrustLevel, SkillValidationPolicy,
    parse_skill_from_file, resolve_skills,
};
use serde_json::json;
use std::path::{Path, PathBuf};

fn fixtures_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
}

fn parse_fixture(dir_name: &str) -> pioneer_skills::SkillDefinition {
    let root = fixtures_root();
    let skill_file = root.join(dir_name).join("SKILL.md");
    parse_skill_from_file(
        skill_file.as_path(),
        SkillSourceKind::User,
        root.as_path(),
        1024 * 1024,
    )
    .expect("fixture must parse")
}

#[test]
fn parses_official_agentskills_fixture_in_strict_mode() {
    let skill = parse_fixture("pdf-processing");

    assert!(skill.conformance.agentskills_strict.compliant);
    assert_eq!(
        skill.runtime.allowed_tools,
        vec![
            "Bash(git:*)".to_owned(),
            "Bash(jq:*)".to_owned(),
            "Read".to_owned()
        ]
    );
}

#[test]
fn strict_fail_fixture_emits_expected_issue_codes() {
    let skill = parse_fixture("strict-invalid");

    assert!(!skill.conformance.agentskills_strict.compliant);
    assert!(
        skill
            .conformance
            .agentskills_strict
            .issues
            .iter()
            .any(|issue| issue.code == "strict.allowed_tools.format")
    );
}

#[test]
fn parses_openclaw_metadata_reference_fields() {
    let skill = parse_fixture("openclaw-browser");

    assert!(skill.conformance.openclaw_compat.compliant);
    assert!(skill.conformance.agentskills_strict.compliant);
    assert_eq!(
        skill.dependencies.bins,
        vec!["node".to_owned(), "npm".to_owned()]
    );
    assert_eq!(
        skill.dependencies.config,
        vec!["skills.browser.enabled".to_owned()]
    );

    let openclaw = skill
        .metadata_known
        .openclaw
        .expect("openclaw namespace must be normalized");
    assert_eq!(openclaw.skill_key.as_deref(), Some("openclaw/browser"));
}

#[test]
fn skill_definition_is_stable_for_same_input() {
    let first = parse_fixture("openclaw-browser");
    let second = parse_fixture("openclaw-browser");

    let mut first_json = serde_json::to_value(first).expect("serialize first manifest");
    let mut second_json = serde_json::to_value(second).expect("serialize second manifest");

    // Paths vary by machine; normalize them before snapshot-like comparison.
    for value in [&mut first_json, &mut second_json] {
        value["identity"]["source_root"] = json!("<root>");
        value["identity"]["skill_dir"] = json!("<skill_dir>");
        value["identity"]["skill_file"] = json!("<skill_file>");
    }

    assert_eq!(first_json, second_json);
}

#[test]
fn parses_agent_browser_metadata_json_and_normalizes_command_dependency() {
    let skill = parse_fixture("agent-browser");

    assert!(skill.metadata_raw.get("clawdbot").is_some());
    assert_eq!(
        skill.dependencies.commands,
        vec!["agent-browser".to_owned()]
    );
    assert!(skill.conformance.openclaw_compat.compliant);
}

#[test]
fn skill_definition_retains_metadata_raw_and_known_fields() {
    let skill = parse_fixture("agent-browser");

    assert_eq!(
        skill.metadata_raw,
        json!({
            "clawdbot": {
                "emoji": "🌐",
                "requires": {
                    "commands": ["agent-browser"]
                },
                "homepage": "https://github.com/vercel-labs/agent-browser"
            }
        })
    );

    let clawdbot = skill
        .metadata_known
        .clawdbot
        .expect("clawdbot namespace must be present");
    assert_eq!(clawdbot.emoji.as_deref(), Some("🌐"));
    assert_eq!(clawdbot.requires_commands, vec!["agent-browser".to_owned()]);
}

#[test]
fn metadata_command_dependency_can_block_resolution_when_binary_missing() {
    let mut skill = parse_fixture("agent-browser");
    skill.dependencies.commands = vec!["definitely-missing-binary-phase25".to_owned()];

    let catalog = pioneer_skills::SkillCatalogSnapshot {
        version: 1,
        generated_at_unix: 1,
        skills: vec![skill],
    };

    let result = resolve_skills(SkillResolutionInput {
        explicit_refs: &[SkillExplicitRef {
            capability_id: "skill:user:agent-browser".to_owned(),
            label: Some("agent-browser".to_owned()),
            slug: "agent-browser".to_owned(),
            source_kind: "user".to_owned(),
        }],
        touched_paths: &[],
        catalog: &catalog,
        policy_set: &SkillPolicySet::default(),
        validation_policy: SkillValidationPolicy {
            strict_agentskills: false,
            ..SkillValidationPolicy::default()
        },
        dependency_input: &DependencyCheckInput::baseline(),
    });

    assert!(result.active.is_empty());
    assert_eq!(result.excluded.len(), 1);
    assert_eq!(
        result.excluded[0].reason,
        SkillExcludedReason::DependencyMissing
    );
}

#[test]
fn local_and_registry_sources_compile_to_equivalent_runtime_manifest() {
    let root = fixtures_root();
    let skill_file = root.join("agent-browser").join("SKILL.md");

    let local = parse_skill_from_file(
        skill_file.as_path(),
        SkillSourceKind::User,
        root.as_path(),
        1024 * 1024,
    )
    .expect("local skill should parse");
    let registry = parse_skill_from_file(
        skill_file.as_path(),
        SkillSourceKind::Registry,
        root.as_path(),
        1024 * 1024,
    )
    .expect("registry skill should parse");

    assert_eq!(local.identity.slug, registry.identity.slug);
    assert_eq!(local.identity.name, registry.identity.name);
    assert_eq!(local.identity.display_name, registry.identity.display_name);
    assert_eq!(local.runtime.trust_level, SkillTrustLevel::Community);
    assert_eq!(registry.runtime.trust_level, SkillTrustLevel::Community);
    assert_eq!(local.runtime, registry.runtime);
    assert_eq!(local.dependencies, registry.dependencies);
    assert_eq!(local.metadata_known, registry.metadata_known);
    assert_eq!(local.metadata_raw, registry.metadata_raw);
    assert_eq!(local.conformance, registry.conformance);
}
