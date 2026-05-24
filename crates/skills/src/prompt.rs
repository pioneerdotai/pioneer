use crate::resolver::{ResolvedSkill, SkillResolvedReason};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillPromptBuild {
    pub text: String,
    pub omitted_slugs: Vec<String>,
    pub truncated: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SkillPromptBudget {
    pub max_chars: usize,
    pub compact_mode_threshold: usize,
    pub include_read_skill_hint: bool,
}

fn compact_skill_block(skill: &ResolvedSkill) -> String {
    format!(
        "- {}\n  Skill slug for read_skill: `{}`\n  Use when: {}\n",
        skill.definition.identity.display_name,
        skill.slug,
        skill.definition.instructions.description,
    )
}

fn full_skill_block(skill: &ResolvedSkill) -> String {
    format!(
        "\n[Skill Body: ${}]\n{}\n",
        skill.slug, skill.definition.instructions.body
    )
}

pub fn build_skill_prompt(active: &[ResolvedSkill], budget: SkillPromptBudget) -> SkillPromptBuild {
    if active.is_empty() {
        return SkillPromptBuild {
            text: String::new(),
            omitted_slugs: Vec::new(),
            truncated: false,
        };
    }

    let max_chars = budget.max_chars.max(1);
    let compact_mode_threshold = budget.compact_mode_threshold;

    let mut text = String::from("[Skills]\nThe following skills are available for this turn:\n");
    let mut omitted_slugs = std::collections::BTreeSet::new();

    for skill in active {
        let block = compact_skill_block(skill);
        if text.len() + block.len() > max_chars {
            omitted_slugs.insert(skill.slug.clone());
            continue;
        }
        text.push_str(block.as_str());
    }

    let can_expand_full = active.len() <= compact_mode_threshold;

    if can_expand_full {
        let mut full_body_candidates = active
            .iter()
            .filter(|skill| matches!(skill.reason, SkillResolvedReason::PathMatch))
            .collect::<Vec<_>>();

        full_body_candidates.sort_by(|left, right| left.slug.as_str().cmp(right.slug.as_str()));

        for skill in full_body_candidates {
            let block = full_skill_block(skill);
            if text.len() + block.len() > max_chars {
                omitted_slugs.insert(skill.slug.clone());
                continue;
            }
            text.push_str(block.as_str());
        }
    } else {
        for skill in active {
            omitted_slugs.insert(skill.slug.clone());
        }
    }

    if text.len() < max_chars {
        let footer = "\nWhen a skill is relevant, call `read_skill` with the exact `Skill slug for read_skill` value before executing specialized actions. Do not use the display name alone. Then follow its instructions";
        if text.len() + footer.len() <= max_chars {
            text.push_str(footer);
        }
    }

    let truncated = !omitted_slugs.is_empty();

    if truncated {
        let note = "\nSkills list truncated due to prompt budget.";
        if text.len() + note.len() <= max_chars {
            text.push_str(note);
        }

        if budget.include_read_skill_hint {
            let hint = "\nUse `read_skill` with the exact `Skill slug for read_skill` value, not the display name.";
            if text.len() + hint.len() <= max_chars {
                text.push_str(hint);
            } else if hint.len() < max_chars {
                let mut keep = max_chars.saturating_sub(hint.len());
                while keep > 0 && !text.is_char_boundary(keep) {
                    keep = keep.saturating_sub(1);
                }
                text.truncate(keep);
                text.push_str(hint);
            }
        }
    }

    SkillPromptBuild {
        text,
        omitted_slugs: omitted_slugs.into_iter().collect(),
        truncated,
    }
}

#[cfg(test)]
mod tests {
    use super::{SkillPromptBudget, build_skill_prompt};
    use crate::compile::{CompileSkillInput, compile_skill_definition};
    use crate::contract::{
        SkillDependencies, SkillSourceKind, SkillTrustLevel, default_skill_conformance,
    };
    use crate::resolver::{ResolvedSkill, SkillResolvedReason};
    use serde_json::json;

    fn resolved(slug: &str, description: &str) -> ResolvedSkill {
        resolved_with_reason(slug, description, SkillResolvedReason::ExplicitCapability)
    }

    fn resolved_with_reason(
        slug: &str,
        description: &str,
        reason: SkillResolvedReason,
    ) -> ResolvedSkill {
        let owner = "workspace";
        let conformance = default_skill_conformance();
        let definition = compile_skill_definition(CompileSkillInput {
            owner: owner.to_owned(),
            slug: slug.to_owned(),
            name: slug.to_owned(),
            display_name: slug.to_owned(),
            description: description.to_owned(),
            body: "body".to_owned(),
            source_kind: SkillSourceKind::User,
            source_root: "/tmp".to_owned(),
            skill_dir: format!("/tmp/{slug}"),
            skill_file: format!("/tmp/{slug}/SKILL.md"),
            version_hint: None,
            fingerprint: "abc".to_owned(),
            user_invocable: true,
            disable_model_invocation: false,
            paths: Vec::new(),
            allowed_tools: Vec::new(),
            runtime_tools: Vec::new(),
            trust_level: SkillTrustLevel::Community,
            dependencies: SkillDependencies::default(),
            license: None,
            compatibility: None,
            metadata_raw: json!({}),
            conformance: conformance.clone(),
        });

        ResolvedSkill {
            slug: format!("{owner}/{slug}"),
            reason,
            definition,
        }
    }

    #[test]
    fn truncates_by_budget() {
        let active = vec![
            resolved("one", "desc"),
            resolved("two", "desc"),
            resolved("three", "desc"),
        ];

        let built = build_skill_prompt(
            active.as_slice(),
            SkillPromptBudget {
                max_chars: 120,
                compact_mode_threshold: 6,
                include_read_skill_hint: true,
            },
        );
        assert!(built.truncated);
        assert!(!built.omitted_slugs.is_empty());
        assert!(built.text.contains("Use `read_skill`"));
    }

    #[test]
    fn truncation_is_stable_for_same_input() {
        let active = vec![
            resolved("alpha", "desc"),
            resolved("beta", "desc"),
            resolved("gamma", "desc"),
        ];
        let budget = SkillPromptBudget {
            max_chars: 120,
            compact_mode_threshold: 6,
            include_read_skill_hint: true,
        };

        let first = build_skill_prompt(active.as_slice(), budget);
        let second = build_skill_prompt(active.as_slice(), budget);

        assert_eq!(first, second);
    }

    #[test]
    fn compact_mode_omits_full_body_and_inserts_hint() {
        let active = vec![resolved("one", "desc"), resolved("two", "desc")];

        let built = build_skill_prompt(
            active.as_slice(),
            SkillPromptBudget {
                max_chars: 2_000,
                compact_mode_threshold: 1,
                include_read_skill_hint: true,
            },
        );

        assert!(built.truncated);
        assert!(built.text.contains("Use `read_skill`"));
        assert!(!built.text.contains("[Skill Body: $workspace/one]"));
    }

    #[test]
    fn compact_prompt_uses_exact_read_skill_slug_and_omits_file_location() {
        let active = vec![resolved("weather", "Get weather forecasts.")];

        let built = build_skill_prompt(
            active.as_slice(),
            SkillPromptBudget {
                max_chars: 2_000,
                compact_mode_threshold: 6,
                include_read_skill_hint: true,
            },
        );

        assert!(
            built
                .text
                .contains("Skill slug for read_skill: `workspace/weather`")
        );
        assert!(
            built
                .text
                .contains("call `read_skill` with the exact `Skill slug for read_skill` value")
        );
        assert!(!built.text.contains("Location:"));
        assert!(!built.text.contains("/tmp/weather/SKILL.md"));
    }

    #[test]
    fn explicit_capability_stays_compact_without_full_body() {
        let active = vec![resolved("weather", "Get weather forecasts.")];

        let built = build_skill_prompt(
            active.as_slice(),
            SkillPromptBudget {
                max_chars: 2_000,
                compact_mode_threshold: 6,
                include_read_skill_hint: true,
            },
        );

        assert!(
            built
                .text
                .contains("Skill slug for read_skill: `workspace/weather`")
        );
        assert!(!built.text.contains("[Skill Body: $workspace/weather]"));
        assert!(!built.text.contains("\nbody\n"));
    }

    #[test]
    fn path_match_can_expand_full_body() {
        let active = vec![resolved_with_reason(
            "weather",
            "Get weather forecasts.",
            SkillResolvedReason::PathMatch,
        )];

        let built = build_skill_prompt(
            active.as_slice(),
            SkillPromptBudget {
                max_chars: 2_000,
                compact_mode_threshold: 6,
                include_read_skill_hint: true,
            },
        );

        assert!(built.text.contains("[Skill Body: $workspace/weather]"));
        assert!(built.text.contains("\nbody\n"));
    }
}
