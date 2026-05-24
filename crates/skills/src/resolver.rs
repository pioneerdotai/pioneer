use crate::compile::SkillDefinition;
use crate::contract::{SkillCatalogSnapshot, qualified_skill_slug};
use crate::dependencies::{
    DependencyCheckInput, DependencyDiagnostic, evaluate_skill_dependencies,
};
use crate::path_match::path_matches_any_pattern;
use crate::policy::{SkillPolicySet, merge_policy};
use crate::security::{SecurityFinding, scan_skill_directory};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillValidationPolicy {
    pub strict_agentskills: bool,
    pub accept_openclaw_profile: bool,
    pub preflight_on_resolve: bool,
    pub allow_untrusted_install: bool,
    pub security_scan_on_resolve: bool,
    pub max_security_scan_file_bytes: usize,
}

impl Default for SkillValidationPolicy {
    fn default() -> Self {
        Self {
            strict_agentskills: true,
            accept_openclaw_profile: true,
            preflight_on_resolve: true,
            allow_untrusted_install: true,
            security_scan_on_resolve: true,
            max_security_scan_file_bytes: 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SkillResolvedReason {
    ExplicitCapability,
    PathMatch,
    Implicit,
}

impl SkillResolvedReason {
    pub fn as_db_value(&self) -> &'static str {
        match self {
            Self::ExplicitCapability => "explicit_composer_capability",
            Self::PathMatch => "path_match",
            Self::Implicit => "implicit",
        }
    }

    fn rank(&self) -> u8 {
        match self {
            Self::ExplicitCapability => 0,
            Self::PathMatch => 1,
            Self::Implicit => 2,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SkillExcludedReason {
    DisabledByPolicy,
    DisabledModelInvocation,
    ValidationRejected,
    InvalidMetadata,
    TrustBlocked,
    SecurityBlocked,
    DependencyMissing,
    NotMatched,
}

impl SkillExcludedReason {
    pub fn as_db_value(&self) -> &'static str {
        match self {
            Self::DisabledByPolicy => "disabled_by_policy",
            Self::DisabledModelInvocation => "disabled_model_invocation",
            Self::ValidationRejected => "validation_rejected",
            Self::InvalidMetadata => "invalid_metadata",
            Self::TrustBlocked => "trust_blocked",
            Self::SecurityBlocked => "security_blocked",
            Self::DependencyMissing => "dependency_missing",
            Self::NotMatched => "not_matched",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResolvedSkill {
    pub slug: String,
    pub reason: SkillResolvedReason,
    pub definition: SkillDefinition,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExcludedSkill {
    pub slug: String,
    pub source_kind: String,
    pub reason: SkillExcludedReason,
    #[serde(default)]
    pub dependency_diagnostics: Vec<DependencyDiagnostic>,
    #[serde(default)]
    pub security_findings: Vec<SecurityFinding>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SkillResolutionResult {
    pub active: Vec<ResolvedSkill>,
    pub excluded: Vec<ExcludedSkill>,
}

#[derive(Debug, Clone)]
pub struct SkillResolutionInput<'a> {
    pub explicit_refs: &'a [SkillExplicitRef],
    pub touched_paths: &'a [String],
    pub catalog: &'a SkillCatalogSnapshot,
    pub policy_set: &'a SkillPolicySet,
    pub validation_policy: SkillValidationPolicy,
    pub dependency_input: &'a DependencyCheckInput,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillExplicitRef {
    pub capability_id: String,
    pub label: Option<String>,
    pub slug: String,
    pub source_kind: String,
}

fn normalize_key(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>()
        .split('-')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>()
        .join("-")
}

fn explicit_ref_matches_skill(input: &SkillExplicitRef, skill: &SkillDefinition) -> bool {
    if !input.source_kind.trim().is_empty()
        && input.source_kind.as_str() != skill.identity.source_kind.as_db_value()
    {
        return false;
    }

    let normalized_ref = normalize_key(input.slug.as_str());
    if normalized_ref.is_empty() {
        return false;
    }

    let normalized_slug = normalize_key(skill.identity.slug.as_str());
    let normalized_qualified_slug = normalize_key(
        qualified_skill_slug(skill.identity.owner.as_str(), skill.identity.slug.as_str()).as_str(),
    );
    let normalized_name = normalize_key(skill.identity.name.as_str());
    let normalized_display_name = normalize_key(skill.identity.display_name.as_str());

    normalized_ref == normalized_slug
        || normalized_ref == normalized_qualified_slug
        || normalized_ref == normalized_name
        || normalized_ref == normalized_display_name
}

fn dependency_failures(
    skill: &SkillDefinition,
    policy: SkillValidationPolicy,
    input: &DependencyCheckInput,
) -> Vec<DependencyDiagnostic> {
    if !policy.preflight_on_resolve {
        return Vec::new();
    }

    evaluate_skill_dependencies(skill, input).failing_diagnostics()
}

fn security_blocking_findings(
    skill: &SkillDefinition,
    policy: SkillValidationPolicy,
) -> Vec<SecurityFinding> {
    if !policy.security_scan_on_resolve {
        return Vec::new();
    }

    let source_root = Path::new(skill.identity.source_root.as_str());
    let skill_dir = Path::new(skill.identity.skill_dir.as_str());
    if !source_root.exists() || !skill_dir.exists() {
        return Vec::new();
    }
    let report = scan_skill_directory(
        source_root,
        skill_dir,
        policy.max_security_scan_file_bytes.max(1),
    );
    if matches!(report.decision, crate::security::SecurityDecision::Block) {
        report.findings
    } else {
        Vec::new()
    }
}

fn trust_blocked(skill: &SkillDefinition, policy: SkillValidationPolicy) -> bool {
    !policy.allow_untrusted_install
        && matches!(
            skill.runtime.trust_level,
            crate::contract::SkillTrustLevel::Untrusted
        )
}

fn resolve_reason(
    skill: &SkillDefinition,
    explicit_refs: &[SkillExplicitRef],
    touched_paths: &[String],
    allow_implicit_invocation: bool,
) -> Option<SkillResolvedReason> {
    if explicit_refs
        .iter()
        .any(|explicit_ref| explicit_ref_matches_skill(explicit_ref, skill))
    {
        return Some(SkillResolvedReason::ExplicitCapability);
    }

    if !skill.runtime.paths.is_empty()
        && touched_paths.iter().any(|path| {
            path_matches_any_pattern(
                skill.runtime.paths.iter().map(String::as_str),
                path.replace('\\', "/").as_str(),
            )
        })
    {
        return Some(SkillResolvedReason::PathMatch);
    }

    if allow_implicit_invocation && skill.runtime.user_invocable {
        return Some(SkillResolvedReason::Implicit);
    }

    None
}

fn passes_validation_policy(skill: &SkillDefinition, policy: SkillValidationPolicy) -> bool {
    if skill.conformance.agentskills_strict.compliant {
        return true;
    }

    if policy.strict_agentskills {
        return false;
    }

    if policy.accept_openclaw_profile && skill.conformance.openclaw_compat.compliant {
        return true;
    }

    false
}

fn has_critical_metadata_issues(skill: &SkillDefinition) -> bool {
    skill.policy_hints.activation_blocked
}

pub fn resolve_skills(input: SkillResolutionInput<'_>) -> SkillResolutionResult {
    let mut active = Vec::new();
    let mut excluded = Vec::new();

    for skill in &input.catalog.skills {
        let skill_slug =
            qualified_skill_slug(skill.identity.owner.as_str(), skill.identity.slug.as_str());

        let effective_policy = merge_policy(
            skill_slug.as_str(),
            skill.identity.source_kind.as_db_value(),
            input.policy_set,
        );

        if !effective_policy.enabled {
            excluded.push(ExcludedSkill {
                slug: skill_slug.clone(),
                source_kind: skill.identity.source_kind.as_db_value().to_owned(),
                reason: SkillExcludedReason::DisabledByPolicy,
                dependency_diagnostics: Vec::new(),
                security_findings: Vec::new(),
            });
            continue;
        }

        let Some(reason) = resolve_reason(
            skill,
            input.explicit_refs,
            input.touched_paths,
            effective_policy.allow_implicit_invocation,
        ) else {
            excluded.push(ExcludedSkill {
                slug: skill_slug.clone(),
                source_kind: skill.identity.source_kind.as_db_value().to_owned(),
                reason: SkillExcludedReason::NotMatched,
                dependency_diagnostics: Vec::new(),
                security_findings: Vec::new(),
            });
            continue;
        };

        if !matches!(reason, SkillResolvedReason::ExplicitCapability)
            && skill.runtime.disable_model_invocation
        {
            excluded.push(ExcludedSkill {
                slug: skill_slug.clone(),
                source_kind: skill.identity.source_kind.as_db_value().to_owned(),
                reason: SkillExcludedReason::DisabledModelInvocation,
                dependency_diagnostics: Vec::new(),
                security_findings: Vec::new(),
            });
            continue;
        }

        if !passes_validation_policy(skill, input.validation_policy) {
            excluded.push(ExcludedSkill {
                slug: skill_slug.clone(),
                source_kind: skill.identity.source_kind.as_db_value().to_owned(),
                reason: SkillExcludedReason::ValidationRejected,
                dependency_diagnostics: Vec::new(),
                security_findings: Vec::new(),
            });
            continue;
        }

        if has_critical_metadata_issues(skill) {
            excluded.push(ExcludedSkill {
                slug: skill_slug.clone(),
                source_kind: skill.identity.source_kind.as_db_value().to_owned(),
                reason: SkillExcludedReason::InvalidMetadata,
                dependency_diagnostics: Vec::new(),
                security_findings: Vec::new(),
            });
            continue;
        }

        if trust_blocked(skill, input.validation_policy) {
            excluded.push(ExcludedSkill {
                slug: skill_slug.clone(),
                source_kind: skill.identity.source_kind.as_db_value().to_owned(),
                reason: SkillExcludedReason::TrustBlocked,
                dependency_diagnostics: Vec::new(),
                security_findings: Vec::new(),
            });
            continue;
        }

        let security_findings = security_blocking_findings(skill, input.validation_policy);
        if !security_findings.is_empty() {
            excluded.push(ExcludedSkill {
                slug: skill_slug.clone(),
                source_kind: skill.identity.source_kind.as_db_value().to_owned(),
                reason: SkillExcludedReason::SecurityBlocked,
                dependency_diagnostics: Vec::new(),
                security_findings,
            });
            continue;
        }

        let dependency_diagnostics =
            dependency_failures(skill, input.validation_policy, input.dependency_input);
        if !dependency_diagnostics.is_empty() {
            excluded.push(ExcludedSkill {
                slug: skill_slug.clone(),
                source_kind: skill.identity.source_kind.as_db_value().to_owned(),
                reason: SkillExcludedReason::DependencyMissing,
                dependency_diagnostics,
                security_findings: Vec::new(),
            });
            continue;
        }

        active.push(ResolvedSkill {
            slug: skill_slug,
            reason,
            definition: skill.clone(),
        });
    }

    active.sort_by(|left, right| {
        left.reason
            .rank()
            .cmp(&right.reason.rank())
            .then_with(|| left.slug.cmp(&right.slug))
    });

    excluded.sort_by(|left, right| left.slug.cmp(&right.slug));

    SkillResolutionResult { active, excluded }
}

#[cfg(test)]
mod tests {
    use super::{
        SkillExcludedReason, SkillExplicitRef, SkillResolutionInput, SkillResolvedReason,
        SkillValidationPolicy, resolve_skills,
    };
    use crate::compile::{CompileSkillInput, SkillDefinition, compile_skill_definition};
    use crate::contract::{
        SkillCatalogSnapshot, SkillDependencies, SkillSourceKind, SkillTrustLevel,
        default_skill_conformance,
    };
    use crate::dependencies::DependencyCheckInput;
    use crate::policy::{SkillPolicy, SkillPolicyKey, SkillPolicySet};

    fn explicit_ref(name: &str) -> SkillExplicitRef {
        explicit_ref_with_source_kind(name, SkillSourceKind::User)
    }

    fn explicit_ref_with_source_kind(name: &str, source_kind: SkillSourceKind) -> SkillExplicitRef {
        let source_kind = source_kind.as_db_value().to_owned();
        SkillExplicitRef {
            capability_id: format!("skill:{source_kind}:{name}"),
            label: Some(name.to_owned()),
            slug: name.to_owned(),
            source_kind,
        }
    }

    fn skill(slug: &str, paths: &[&str], source_kind: SkillSourceKind) -> SkillDefinition {
        let conformance = default_skill_conformance();
        let definition = compile_skill_definition(CompileSkillInput {
            owner: "workspace".to_owned(),
            slug: slug.to_owned(),
            name: slug.to_owned(),
            display_name: slug.to_owned(),
            description: "desc".to_owned(),
            body: "body".to_owned(),
            source_kind: source_kind.clone(),
            source_root: "/tmp".to_owned(),
            skill_dir: format!("/tmp/{slug}"),
            skill_file: format!("/tmp/{slug}/SKILL.md"),
            version_hint: None,
            fingerprint: "abc".to_owned(),
            user_invocable: true,
            disable_model_invocation: false,
            paths: paths.iter().map(|value| value.to_string()).collect(),
            allowed_tools: Vec::new(),
            runtime_tools: Vec::new(),
            trust_level: SkillTrustLevel::Community,
            dependencies: SkillDependencies::default(),
            license: None,
            compatibility: None,
            metadata_raw: serde_json::json!({}),
            conformance: conformance.clone(),
        });

        definition
    }

    #[test]
    fn explicit_capabilities_rank_before_path_matches() {
        let catalog = SkillCatalogSnapshot {
            version: 1,
            generated_at_unix: 1,
            skills: vec![
                skill("path-skill", &["src/**"], SkillSourceKind::User),
                skill("explicit-skill", &[], SkillSourceKind::User),
            ],
        };

        let result = resolve_skills(SkillResolutionInput {
            explicit_refs: &[explicit_ref("explicit-skill")],
            touched_paths: &["src/main.rs".to_owned()],
            catalog: &catalog,
            policy_set: &SkillPolicySet::default(),
            validation_policy: SkillValidationPolicy::default(),
            dependency_input: &DependencyCheckInput::baseline(),
        });

        assert_eq!(result.active.len(), 2);
        assert_eq!(result.active[0].slug, "workspace/explicit-skill");
        assert_eq!(
            result.active[0].reason,
            SkillResolvedReason::ExplicitCapability
        );
        assert_eq!(result.active[1].slug, "workspace/path-skill");
        assert_eq!(result.active[1].reason, SkillResolvedReason::PathMatch);
    }

    #[test]
    fn policy_can_disable_skill() {
        let catalog = SkillCatalogSnapshot {
            version: 1,
            generated_at_unix: 1,
            skills: vec![skill("explicit-skill", &[], SkillSourceKind::User)],
        };

        let mut policy = SkillPolicySet::default();
        policy.workspace_by_key.insert(
            SkillPolicyKey::new("workspace/explicit-skill", "user"),
            SkillPolicy {
                enabled: Some(false),
                allow_implicit_invocation: None,
            },
        );

        let result = resolve_skills(SkillResolutionInput {
            explicit_refs: &[explicit_ref("explicit-skill")],
            touched_paths: &[],
            catalog: &catalog,
            policy_set: &policy,
            validation_policy: SkillValidationPolicy::default(),
            dependency_input: &DependencyCheckInput::baseline(),
        });

        assert!(result.active.is_empty());
        assert_eq!(result.excluded.len(), 1);
        assert_eq!(
            result.excluded[0].reason,
            SkillExcludedReason::DisabledByPolicy
        );
    }

    #[test]
    fn metadata_command_dependency_missing_excludes_skill() {
        let mut skill = skill("agent-browser", &[], SkillSourceKind::User);
        skill.dependencies.commands = vec!["non-existent-pioneer-test-binary".to_owned()];

        let catalog = SkillCatalogSnapshot {
            version: 1,
            generated_at_unix: 1,
            skills: vec![skill],
        };

        let result = resolve_skills(SkillResolutionInput {
            explicit_refs: &[explicit_ref("agent-browser")],
            touched_paths: &[],
            catalog: &catalog,
            policy_set: &SkillPolicySet::default(),
            validation_policy: SkillValidationPolicy::default(),
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
    fn non_strict_skill_is_rejected_when_strict_validation_enabled() {
        let mut non_strict = skill("agent-browser", &[], SkillSourceKind::Registry);
        non_strict.conformance.agentskills_strict.compliant = false;

        let catalog = SkillCatalogSnapshot {
            version: 1,
            generated_at_unix: 1,
            skills: vec![non_strict],
        };

        let result = resolve_skills(SkillResolutionInput {
            explicit_refs: &[explicit_ref_with_source_kind(
                "agent-browser",
                SkillSourceKind::Registry,
            )],
            touched_paths: &[],
            catalog: &catalog,
            policy_set: &SkillPolicySet::default(),
            validation_policy: SkillValidationPolicy::default(),
            dependency_input: &DependencyCheckInput::baseline(),
        });

        assert!(result.active.is_empty());
        assert_eq!(
            result.excluded[0].reason,
            SkillExcludedReason::ValidationRejected
        );
    }

    #[test]
    fn openclaw_compatible_skill_can_pass_when_strict_validation_disabled() {
        let mut non_strict_system = skill("system-browser", &[], SkillSourceKind::System);
        non_strict_system.conformance.agentskills_strict.compliant = false;
        non_strict_system.conformance.openclaw_compat.compliant = true;

        let catalog = SkillCatalogSnapshot {
            version: 1,
            generated_at_unix: 1,
            skills: vec![non_strict_system],
        };

        let result = resolve_skills(SkillResolutionInput {
            explicit_refs: &[explicit_ref_with_source_kind(
                "system-browser",
                SkillSourceKind::System,
            )],
            touched_paths: &[],
            catalog: &catalog,
            policy_set: &SkillPolicySet::default(),
            validation_policy: SkillValidationPolicy {
                strict_agentskills: false,
                ..SkillValidationPolicy::default()
            },
            dependency_input: &DependencyCheckInput::baseline(),
        });

        assert_eq!(result.active.len(), 1);
        assert!(result.excluded.is_empty());
    }

    #[test]
    fn strict_validation_is_source_kind_agnostic() {
        let mut registry = skill("registry-skill", &[], SkillSourceKind::Registry);
        registry.conformance.agentskills_strict.compliant = false;

        let mut user = skill("user-skill", &[], SkillSourceKind::User);
        user.conformance.agentskills_strict.compliant = false;

        let catalog = SkillCatalogSnapshot {
            version: 1,
            generated_at_unix: 1,
            skills: vec![registry, user],
        };

        let result = resolve_skills(SkillResolutionInput {
            explicit_refs: &[
                explicit_ref_with_source_kind("registry-skill", SkillSourceKind::Registry),
                explicit_ref("user-skill"),
            ],
            touched_paths: &[],
            catalog: &catalog,
            policy_set: &SkillPolicySet::default(),
            validation_policy: SkillValidationPolicy::default(),
            dependency_input: &DependencyCheckInput::baseline(),
        });

        assert!(result.active.is_empty());
        assert_eq!(result.excluded.len(), 2);
        assert!(
            result
                .excluded
                .iter()
                .all(|skill| skill.reason == SkillExcludedReason::ValidationRejected)
        );
    }

    #[test]
    fn untrusted_skill_is_blocked_when_policy_disallows_untrusted_install() {
        let mut untrusted = skill("untrusted", &[], SkillSourceKind::User);
        untrusted.runtime.trust_level = SkillTrustLevel::Untrusted;

        let catalog = SkillCatalogSnapshot {
            version: 1,
            generated_at_unix: 1,
            skills: vec![untrusted],
        };

        let result = resolve_skills(SkillResolutionInput {
            explicit_refs: &[explicit_ref("untrusted")],
            touched_paths: &[],
            catalog: &catalog,
            policy_set: &SkillPolicySet::default(),
            validation_policy: SkillValidationPolicy {
                allow_untrusted_install: false,
                ..SkillValidationPolicy::default()
            },
            dependency_input: &DependencyCheckInput::baseline(),
        });

        assert!(result.active.is_empty());
        assert_eq!(result.excluded.len(), 1);
        assert_eq!(result.excluded[0].reason, SkillExcludedReason::TrustBlocked);
    }

    #[test]
    fn metadata_shape_errors_block_activation() {
        let mut skill = skill("agent-browser", &[], SkillSourceKind::User);
        skill.policy_hints.activation_blocked = true;
        skill.policy_hints.block_issue_codes = vec!["openclaw.metadata.clawdbot.type".to_owned()];

        let catalog = SkillCatalogSnapshot {
            version: 1,
            generated_at_unix: 1,
            skills: vec![skill],
        };

        let result = resolve_skills(SkillResolutionInput {
            explicit_refs: &[explicit_ref("agent-browser")],
            touched_paths: &[],
            catalog: &catalog,
            policy_set: &SkillPolicySet::default(),
            validation_policy: SkillValidationPolicy::default(),
            dependency_input: &DependencyCheckInput::baseline(),
        });

        assert!(result.active.is_empty());
        assert_eq!(result.excluded.len(), 1);
        assert_eq!(
            result.excluded[0].reason,
            SkillExcludedReason::InvalidMetadata
        );
    }

    #[test]
    fn deterministic_for_same_inputs() {
        let catalog = SkillCatalogSnapshot {
            version: 1,
            generated_at_unix: 1,
            skills: vec![
                skill("zeta", &["docs/**"], SkillSourceKind::User),
                skill("alpha", &[], SkillSourceKind::User),
                skill("beta", &["src/**"], SkillSourceKind::User),
            ],
        };

        let explicit_refs = vec![explicit_ref("alpha")];
        let touched_paths = vec!["src/main.rs".to_owned(), "docs/spec.md".to_owned()];
        let policy_set = SkillPolicySet::default();

        let first = resolve_skills(SkillResolutionInput {
            explicit_refs: explicit_refs.as_slice(),
            touched_paths: touched_paths.as_slice(),
            catalog: &catalog,
            policy_set: &policy_set,
            validation_policy: SkillValidationPolicy::default(),
            dependency_input: &DependencyCheckInput::baseline(),
        });
        let second = resolve_skills(SkillResolutionInput {
            explicit_refs: explicit_refs.as_slice(),
            touched_paths: touched_paths.as_slice(),
            catalog: &catalog,
            policy_set: &policy_set,
            validation_policy: SkillValidationPolicy::default(),
            dependency_input: &DependencyCheckInput::baseline(),
        });

        assert_eq!(
            first
                .active
                .iter()
                .map(|skill| (skill.slug.clone(), skill.reason.clone()))
                .collect::<Vec<_>>(),
            second
                .active
                .iter()
                .map(|skill| (skill.slug.clone(), skill.reason.clone()))
                .collect::<Vec<_>>()
        );
        assert_eq!(first.excluded, second.excluded);
    }

    #[test]
    fn implicit_resolution_activates_skill_when_policy_enabled() {
        let catalog = SkillCatalogSnapshot {
            version: 1,
            generated_at_unix: 1,
            skills: vec![skill("implicit-skill", &[], SkillSourceKind::User)],
        };

        let mut policy = SkillPolicySet::default();
        policy.workspace_by_key.insert(
            SkillPolicyKey::new("workspace/implicit-skill", "user"),
            SkillPolicy {
                enabled: Some(true),
                allow_implicit_invocation: Some(true),
            },
        );

        let result = resolve_skills(SkillResolutionInput {
            explicit_refs: &[],
            touched_paths: &[],
            catalog: &catalog,
            policy_set: &policy,
            validation_policy: SkillValidationPolicy::default(),
            dependency_input: &DependencyCheckInput::baseline(),
        });

        assert_eq!(result.active.len(), 1);
        assert_eq!(result.active[0].slug, "workspace/implicit-skill");
        assert_eq!(result.active[0].reason, SkillResolvedReason::Implicit);
    }

    #[test]
    fn implicit_resolution_respects_disable_model_invocation() {
        let mut implicit_blocked = skill("implicit-blocked", &[], SkillSourceKind::User);
        implicit_blocked.runtime.disable_model_invocation = true;

        let catalog = SkillCatalogSnapshot {
            version: 1,
            generated_at_unix: 1,
            skills: vec![implicit_blocked],
        };

        let mut policy = SkillPolicySet::default();
        policy.workspace_by_key.insert(
            SkillPolicyKey::new("workspace/implicit-blocked", "user"),
            SkillPolicy {
                enabled: Some(true),
                allow_implicit_invocation: Some(true),
            },
        );

        let result = resolve_skills(SkillResolutionInput {
            explicit_refs: &[],
            touched_paths: &[],
            catalog: &catalog,
            policy_set: &policy,
            validation_policy: SkillValidationPolicy::default(),
            dependency_input: &DependencyCheckInput::baseline(),
        });

        assert!(result.active.is_empty());
        assert_eq!(result.excluded.len(), 1);
        assert_eq!(
            result.excluded[0].reason,
            SkillExcludedReason::DisabledModelInvocation
        );
    }
}
