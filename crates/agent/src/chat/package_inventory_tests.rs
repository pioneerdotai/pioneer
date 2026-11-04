//! Regression at the package parser -> card -> persisted inventory boundary.
use pioneer_skills::{
    ResolvedSkill, SkillId, SkillMarkdownParseContext, SkillPromptBudget, SkillResolvedReason,
    SkillSourceKind, build_skill_prompt, parse_skill_markdown,
};

fn package(body: &str, version: Option<&str>, hidden: bool) -> ResolvedSkill {
    let skill_id = SkillId::new("EEEEEEEEEEEEEEEEEEEEE").unwrap();
    // Version override is the existing _meta.json path: it can change while
    // SKILL.md bytes (and therefore the content fingerprint) stay identical.
    let definition = parse_skill_markdown(
        &format!("---\nname: package\nslug: package\ndescription: Same description\ncatalog-hide: {hidden}\n---\n{body}\n"),
        SkillMarkdownParseContext {
            skill_id: skill_id.clone(),
            source_kind: SkillSourceKind::User,
            source_root: "/packages".to_owned(),
            skill_dir: "/packages/package".to_owned(),
            skill_file: "/packages/package/SKILL.md".to_owned(),
            parent_directory_name: "package".to_owned(),
            identity_owner_override: None,
            identity_slug_override: None,
            version_hint_override: version.map(str::to_owned),
            display_name_override: None,
        },
    ).unwrap();
    ResolvedSkill {
        skill_id,
        slug: "package".to_owned(),
        reason: SkillResolvedReason::ExplicitCapability,
        definition,
    }
}

#[test]
fn package_body_and_version_changes_refresh_actual_inventory_without_rewriting_saved_input() {
    for hidden in [false, true] {
        let root = std::env::temp_dir().join(format!(
            "pioneer-package-known-state-{}-{hidden}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let initial = package("OLD_BODY_SENTINEL", Some("1"), hidden);
        let body_changed = package("NEW_BODY_SENTINEL", Some("1"), hidden);
        let version_changed = package("OLD_BODY_SENTINEL", Some("2"), hidden);
        assert_ne!(
            initial.definition.identity.fingerprint,
            body_changed.definition.identity.fingerprint
        );
        assert_eq!(
            initial.definition.identity.fingerprint,
            version_changed.definition.identity.fingerprint
        );
        assert_eq!(initial.definition.policy_hints.catalog_hidden, hidden);
        let compile = |skill: &ResolvedSkill| {
            let card = build_skill_prompt(
                std::slice::from_ref(skill),
                SkillPromptBudget {
                    max_chars: 8_000,
                    compact_mode_threshold: 6,
                    include_read_skill_hint: true,
                },
            );
            assert!(!card.truncated);
            assert!(!card.text.contains("OLD_BODY_SENTINEL"));
            assert!(!card.text.contains("NEW_BODY_SENTINEL"));
            super::super::compile_agent_instruction_delivery_plan_with_prompt_root(
                &root,
                Some(card.text),
                None,
                &[],
                &pioneer_protocol::default_turn_permission_profile_snapshot(),
                false,
                false,
                false,
                true,
                false,
                false,
                None,
                "thread",
                "turn",
            )
            .unwrap()
        };
        let original_plan = compile(&initial);
        let (context, metadata) =
            super::super::prepare_context_metadata(&original_plan, &[], "Date: 2026-11-02");
        let mut user = pioneer_provider::ChatMessage::user("original user input");
        user.application_context = Some(metadata);
        super::super::prepend_turn_context(&mut user, &context);
        let durable = serde_json::to_string(&user).unwrap();
        let saved = serde_json::from_str::<pioneer_provider::ChatMessage>(&durable).unwrap();
        let (unchanged, _) = super::super::prepare_context_metadata(
            &compile(&initial),
            std::slice::from_ref(&saved),
            "next date",
        );
        assert!(!unchanged.contains("Package fingerprint:"));
        for changed in [&body_changed, &version_changed] {
            let binding =
                super::to_turn_skill_bindings(std::slice::from_ref(changed), &[]).remove(0);
            assert_eq!(binding.fingerprint, changed.definition.identity.fingerprint);
            assert_eq!(
                binding.skill_version,
                changed.definition.identity.version_hint
            );
            let (announcement, _) = super::super::prepare_context_metadata(
                &compile(changed),
                std::slice::from_ref(&saved),
                "next date",
            );
            assert!(announcement.contains("replaces the previously shown slice"));
            assert!(announcement.contains(&changed.definition.identity.fingerprint));
            assert!(announcement.contains(&format!(
                "version: `{:?}`",
                changed.definition.identity.version_hint
            )));
            assert_eq!(initial.skill_id, changed.skill_id);
            assert_eq!(
                initial.definition.identity.skill_dir,
                changed.definition.identity.skill_dir
            );
            assert_eq!(
                initial.definition.instructions.description,
                changed.definition.instructions.description
            );
        }
        // Absence must not collide with a valid literal version string.
        let absent = package("SAME_BODY", None, hidden);
        let literal = package("SAME_BODY", Some("unspecified"), hidden);
        assert_eq!(
            absent.definition.identity.fingerprint,
            literal.definition.identity.fingerprint
        );
        let (context, metadata) =
            super::super::prepare_context_metadata(&compile(&absent), &[], "date");
        let mut absent_user = pioneer_provider::ChatMessage::user("old input");
        absent_user.application_context = Some(metadata);
        super::super::prepend_turn_context(&mut absent_user, &context);
        let (updated, _) =
            super::super::prepare_context_metadata(&compile(&literal), &[absent_user], "date");
        assert!(updated.contains("replaces the previously shown slice"));
        assert!(updated.contains("version: `Some(\"unspecified\")`"));
        assert_eq!(serde_json::to_string(&saved).unwrap(), durable);
        assert_eq!(
            super::to_turn_skill_bindings(&[initial], &[])[0]
                .skill_version
                .as_deref(),
            Some("1")
        );
    }
}
