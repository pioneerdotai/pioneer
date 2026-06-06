//! Skill health selectors.

use super::catalog;
use pioneer_protocol::{SkillHealthItem, SkillHealthTarget, SkillListItem, SkillsHealthParams};
use std::collections::HashMap;

pub const DEFAULT_SKILLS_HEALTH_AUDIT_LIMIT: u64 = 16;

pub fn skill_health_targets(skills: &[SkillListItem]) -> Vec<SkillHealthTarget> {
    skills
        .iter()
        .map(|skill| SkillHealthTarget {
            slug: skill.slug.clone(),
            source_kind: skill.source_kind.clone(),
        })
        .collect()
}

pub fn skills_health_params(
    workspace_id: impl Into<String>,
    skills: Vec<SkillHealthTarget>,
) -> SkillsHealthParams {
    SkillsHealthParams {
        workspace_id: workspace_id.into(),
        skills,
        audit_limit: DEFAULT_SKILLS_HEALTH_AUDIT_LIMIT,
    }
}

pub fn health_details_map(health_items: Vec<SkillHealthItem>) -> HashMap<String, SkillHealthItem> {
    health_items
        .into_iter()
        .map(|item| {
            (
                catalog::skill_key(item.slug.as_str(), item.source_kind.as_str()),
                item,
            )
        })
        .collect()
}

pub fn skill_health_detail<'a>(
    health_details: &'a HashMap<String, SkillHealthItem>,
    slug: &str,
    source_kind: &str,
) -> Option<&'a SkillHealthItem> {
    health_details.get(catalog::skill_key(slug, source_kind).as_str())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{SkillHealthSummary, SkillInstallState, SkillPolicyState};

    fn skill(slug: &str, source_kind: &str) -> SkillListItem {
        SkillListItem {
            slug: slug.to_owned(),
            source_kind: source_kind.to_owned(),
            display_name: slug.to_owned(),
            description: String::new(),
            version: None,
            fingerprint: format!("{slug}:{source_kind}:fingerprint"),
            trust_level: "community".to_owned(),
            install: SkillInstallState {
                managed: true,
                installed: true,
                lifecycle_editable: true,
                install_path: None,
                updated_at: None,
            },
            policy: SkillPolicyState {
                enabled: true,
                allow_implicit_invocation: true,
                allow_implicit_invocation_editable: true,
            },
            health: SkillHealthSummary {
                status: "ok".to_owned(),
                dependency_failures: Vec::new(),
                security_blocks: Vec::new(),
                validation_issues: Vec::new(),
            },
            status: "active".to_owned(),
            status_reason: None,
        }
    }

    fn health(slug: &str, source_kind: &str) -> SkillHealthItem {
        SkillHealthItem {
            slug: slug.to_owned(),
            source_kind: source_kind.to_owned(),
            trust_level: "community".to_owned(),
            dependency_diagnostics: Vec::new(),
            security_findings: Vec::new(),
            validation_issues: Vec::new(),
            trust_gate: Vec::new(),
            recent_audit: Vec::new(),
        }
    }

    #[test]
    fn health_targets_and_params_preserve_skill_identity() {
        let targets = skill_health_targets(&[skill("alpha", "user")]);
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].slug, "alpha");
        assert_eq!(targets[0].source_kind, "user");

        let params = skills_health_params("workspace", targets);
        assert_eq!(params.workspace_id, "workspace");
        assert_eq!(params.audit_limit, DEFAULT_SKILLS_HEALTH_AUDIT_LIMIT);
        assert_eq!(params.skills.len(), 1);
    }

    #[test]
    fn health_details_are_keyed_by_skill_target() {
        let details = health_details_map(vec![health("alpha", "user")]);

        assert!(details.contains_key("alpha::user"));
        assert!(skill_health_detail(&details, "alpha", "user").is_some());
        assert!(skill_health_detail(&details, "missing", "user").is_none());
    }
}
