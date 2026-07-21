use crate::compact_skill_label;
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
    let label = compact_skill_label(
        skill.definition.identity.owner.as_deref(),
        skill.definition.identity.slug.as_str(),
    );
    let machine_ref = format!("skill:{}", skill.skill_id);
    format!(
        "- {} ({})\n  Exact skill reference for read_skill: `{}`\n  Skill asset root: `{}`\n  Use when: {}\n",
        skill.definition.identity.display_name,
        label,
        machine_ref,
        skill.definition.identity.skill_dir,
        skill.definition.instructions.description,
    )
}

fn full_skill_block(skill: &ResolvedSkill) -> String {
    let machine_ref = format!("skill:{}", skill.skill_id);
    format!(
        "\nSkill asset root for ${}: `{}`\nResolve relative paths mentioned by this skill under Skill asset root. Prefer absolute paths built from Skill asset root for commands and file operations.\n[Skill Body: ${}]\n{}\n",
        machine_ref,
        skill.definition.identity.skill_dir,
        machine_ref,
        skill.definition.instructions.body
    )
}

fn internal_skill_reference_block(skills: &[&ResolvedSkill]) -> String {
    let mut skills = skills.to_vec();
    skills.sort_by(|left, right| {
        let left_label = compact_skill_label(
            left.definition.identity.owner.as_deref(),
            left.definition.identity.slug.as_str(),
        );
        let right_label = compact_skill_label(
            right.definition.identity.owner.as_deref(),
            right.definition.identity.slug.as_str(),
        );
        left_label
            .cmp(&right_label)
            .then_with(|| left.skill_id.cmp(&right.skill_id))
    });

    let mut block = String::from(
        "[Internal Skill References]\nUse these exact references when a system policy names a catalog-hidden skill:\n",
    );
    for skill in skills {
        let label = compact_skill_label(
            skill.definition.identity.owner.as_deref(),
            skill.definition.identity.slug.as_str(),
        );
        block.push_str(format!("- {label}: `skill:{}`\n", skill.skill_id).as_str());
    }
    block
}

pub fn build_skill_prompt(active: &[ResolvedSkill], budget: SkillPromptBudget) -> SkillPromptBuild {
    let hidden_skills = active
        .iter()
        .filter(|skill| skill.definition.policy_hints.catalog_hidden)
        .collect::<Vec<_>>();
    let catalog_skills = active
        .iter()
        .filter(|skill| !skill.definition.policy_hints.catalog_hidden)
        .collect::<Vec<_>>();

    if catalog_skills.is_empty() && hidden_skills.is_empty() {
        return SkillPromptBuild {
            text: String::new(),
            omitted_slugs: Vec::new(),
            truncated: false,
        };
    }

    let max_chars = budget.max_chars.max(1);
    let compact_mode_threshold = budget.compact_mode_threshold;
    let mut text = String::new();
    let mut omitted_slugs = std::collections::BTreeSet::new();

    if !hidden_skills.is_empty() {
        let block = internal_skill_reference_block(hidden_skills.as_slice());
        if block.len() <= max_chars {
            text.push_str(block.as_str());
        } else {
            omitted_slugs.extend(hidden_skills.iter().map(|skill| skill.slug.clone()));
        }
    }

    let mut catalog_header_added = false;
    if !catalog_skills.is_empty() {
        if !text.is_empty() {
            text.push('\n');
        }
        let header = "[Skills]\nThe following skills are available for this turn:\n";
        if text.len() + header.len() <= max_chars {
            text.push_str(header);
            catalog_header_added = true;
        } else {
            omitted_slugs.extend(catalog_skills.iter().map(|skill| skill.slug.clone()));
        }
    }

    for skill in catalog_skills
        .iter()
        .copied()
        .filter(|_| catalog_header_added)
    {
        let block = compact_skill_block(skill);
        if text.len() + block.len() > max_chars {
            omitted_slugs.insert(skill.slug.clone());
            continue;
        }
        text.push_str(block.as_str());
    }

    let can_expand_full = catalog_header_added && catalog_skills.len() <= compact_mode_threshold;

    if can_expand_full {
        let mut full_body_candidates = catalog_skills
            .iter()
            .copied()
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
        for skill in catalog_skills.iter().copied() {
            omitted_slugs.insert(skill.slug.clone());
        }
    }

    if catalog_header_added && text.len() < max_chars {
        let footer = "\nWhen a skill is relevant, call `read_skill` with its exact `skill:<skill_id>` reference before executing specialized actions. Never reconstruct the reference from the readable label. `read_skill` returns `skill_asset_root`; resolve relative file paths from the skill body under that directory and prefer absolute paths built from `skill_asset_root`. Skill runtime tools remain subject to the current turn permissions and sandbox. Then follow its instructions";
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
            let hint = "\nUse `read_skill` with the exact `skill:<skill_id>` reference, not the readable label.";
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
    use pioneer_protocol::SkillId;
    use serde_json::json;

    fn resolved(slug: &str, description: &str) -> ResolvedSkill {
        resolved_with_reason(slug, description, SkillResolvedReason::ExplicitCapability)
    }

    fn resolved_with_reason(
        slug: &str,
        description: &str,
        reason: SkillResolvedReason,
    ) -> ResolvedSkill {
        resolved_with_reason_and_source(slug, description, reason, SkillSourceKind::User)
    }

    fn resolved_with_reason_and_source(
        slug: &str,
        description: &str,
        reason: SkillResolvedReason,
        source_kind: SkillSourceKind,
    ) -> ResolvedSkill {
        let owner = "workspace";
        let skill_id = test_skill_id(slug, source_kind);
        let conformance = default_skill_conformance();
        let definition = compile_skill_definition(CompileSkillInput {
            skill_id: skill_id.clone(),
            owner: Some(owner.to_owned()),
            slug: slug.to_owned(),
            name: slug.to_owned(),
            display_name: slug.to_owned(),
            description: description.to_owned(),
            body: "body".to_owned(),
            source_kind,
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
            skill_id,
            slug: slug.to_owned(),
            reason,
            definition,
        }
    }

    fn test_skill_id(value: &str, source_kind: SkillSourceKind) -> SkillId {
        let suffix = match source_kind {
            SkillSourceKind::System => 'S',
            SkillSourceKind::User => 'U',
            SkillSourceKind::Registry => 'R',
        };
        let mut value = value
            .chars()
            .filter(char::is_ascii_alphanumeric)
            .collect::<String>();
        value.truncate(20);
        while value.len() < 20 {
            value.push(suffix);
        }
        value.push(suffix);
        SkillId::new(value).unwrap()
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
        assert!(!built.text.contains("[Skill Body:"));
    }

    #[test]
    fn compact_prompt_uses_exact_read_skill_id_and_asset_root() {
        let active = vec![resolved("weather", "Get weather forecasts.")];

        let built = build_skill_prompt(
            active.as_slice(),
            SkillPromptBudget {
                max_chars: 2_000,
                compact_mode_threshold: 6,
                include_read_skill_hint: true,
            },
        );

        assert!(built.text.contains(&format!(
            "Exact skill reference for read_skill: `skill:{}`",
            active[0].skill_id
        )));
        assert!(built.text.contains("workspace/weather"));
        assert!(
            built
                .text
                .contains("call `read_skill` with its exact `skill:<skill_id>` reference")
        );
        assert!(
            built
                .text
                .contains("`read_skill` returns `skill_asset_root`")
        );
        assert!(built.text.contains("Skill asset root: `/tmp/weather`"));
        assert!(!built.text.contains("/tmp/weather/SKILL.md"));
    }

    #[test]
    fn skill_prompt_policy_footer_mentions_current_turn_permissions() {
        let active = vec![resolved("weather", "Get weather forecasts.")];

        let built = build_skill_prompt(
            active.as_slice(),
            SkillPromptBudget {
                max_chars: 2_000,
                compact_mode_threshold: 6,
                include_read_skill_hint: true,
            },
        );

        assert!(built.text.contains(
            "Skill runtime tools remain subject to the current turn permissions and sandbox"
        ));
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
                .contains(&format!("skill:{}", active[0].skill_id))
        );
        assert!(!built.text.contains("[Skill Body:"));
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

        assert!(
            built
                .text
                .contains(&format!("[Skill Body: $skill:{}]", active[0].skill_id))
        );
        assert!(built.text.contains(&format!(
            "Skill asset root for $skill:{}: `/tmp/weather`",
            active[0].skill_id
        )));
        assert!(
            built
                .text
                .contains("Resolve relative paths mentioned by this skill under Skill asset root")
        );
        assert!(built.text.contains("\nbody\n"));
    }

    #[test]
    fn same_readable_label_gets_distinct_exact_skill_references() {
        let active = vec![
            resolved_with_reason_and_source(
                "browser",
                "System browser.",
                SkillResolvedReason::ExplicitCapability,
                SkillSourceKind::System,
            ),
            resolved_with_reason_and_source(
                "browser",
                "User browser.",
                SkillResolvedReason::ExplicitCapability,
                SkillSourceKind::User,
            ),
        ];

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
                .contains(&format!("skill:{}", active[0].skill_id))
        );
        assert!(
            built
                .text
                .contains(&format!("skill:{}", active[1].skill_id))
        );
    }

    #[test]
    fn catalog_hidden_skill_is_omitted_from_public_catalog_but_keeps_exact_internal_reference() {
        let mut hidden = resolved("hidden", "Hidden from prompt catalog.");
        hidden.definition.policy_hints.catalog_hidden = true;
        let hidden_id = hidden.skill_id.clone();
        let visible = resolved("visible", "Visible in prompt catalog.");

        let built = build_skill_prompt(
            &[hidden, visible],
            SkillPromptBudget {
                max_chars: 2_000,
                compact_mode_threshold: 6,
                include_read_skill_hint: true,
            },
        );

        assert!(!built.truncated);
        assert!(built.omitted_slugs.is_empty());
        assert!(built.text.contains("workspace/visible"));
        assert!(built.text.contains("[Internal Skill References]"));
        assert!(built.text.contains(&format!("skill:{hidden_id}")));
        assert!(!built.text.contains("- hidden (workspace/hidden)"));
        assert!(!built.text.contains("Hidden from prompt catalog."));
    }

    #[test]
    fn catalog_hidden_path_match_does_not_expand_body() {
        let mut hidden = resolved_with_reason(
            "hidden",
            "Hidden from prompt catalog.",
            SkillResolvedReason::PathMatch,
        );
        hidden.definition.policy_hints.catalog_hidden = true;
        let hidden_id = hidden.skill_id.clone();

        let built = build_skill_prompt(
            &[hidden],
            SkillPromptBudget {
                max_chars: 2_000,
                compact_mode_threshold: 6,
                include_read_skill_hint: true,
            },
        );

        assert!(built.text.contains("[Internal Skill References]"));
        assert!(built.text.contains(&format!("skill:{hidden_id}")));
        assert!(!built.text.contains("Hidden from prompt catalog."));
        assert!(!built.truncated);
        assert!(built.omitted_slugs.is_empty());
    }
}
